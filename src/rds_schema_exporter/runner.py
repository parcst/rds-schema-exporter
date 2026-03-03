"""Orchestrator: connect -> detect metadata -> extract -> write -> report -> notify."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from pathlib import Path

import click
import pymysql

from rds_schema_exporter.config import Config
from rds_schema_exporter.extraction import extract_all
from rds_schema_exporter.extraction.databases import list_databases
from rds_schema_exporter.metadata import detect_metadata
from rds_schema_exporter.models import RunReport
from rds_schema_exporter.notifications.slack import send_slack_notification
from rds_schema_exporter.reporting.summary import format_summary
from rds_schema_exporter.writers.base import Writer
from rds_schema_exporter.writers.local import LocalWriter
from rds_schema_exporter.writers.s3 import S3Writer

logger = logging.getLogger(__name__)


def _create_writer(config: Config) -> Writer:
    if config.output.target == "s3":
        if not config.output.s3_bucket:
            raise ValueError("S3 bucket is required when output target is 's3'.")
        return S3Writer(
            bucket=config.output.s3_bucket,
            prefix=config.output.s3_prefix,
        )
    return LocalWriter(base_path=config.output.local_path)


def _ensure_output_dir(config: Config) -> None:
    """Prompt to create the local output directory if it doesn't exist."""
    if config.output.target != "local":
        return
    base = Path(config.output.local_path).expanduser()
    if base.exists():
        return
    if click.confirm(f"Output directory '{base}' does not exist. Create it?"):
        base.mkdir(parents=True)
        config.output.local_path = str(base)
    else:
        raise ValueError(f"Output directory '{base}' does not exist.")


def _build_connect_kwargs(config: Config) -> dict:
    """Build pymysql.connect keyword arguments from config."""
    kwargs: dict = {
        "host": config.connection.host,
        "port": config.connection.port,
    }
    if config.connection.user:
        kwargs["user"] = config.connection.user
    if config.connection.password:
        kwargs["password"] = config.connection.password
    return kwargs


def _run_pipeline(
    conn: pymysql.connections.Connection,
    config: Config,
    report: RunReport,
) -> None:
    """Detect metadata, list schemas, extract objects, and write them out.

    Updates *report* in place with ``database_info``, ``object_counts``,
    ``total_objects``, and ``databases_processed``.
    """
    logger.info("Detecting AWS metadata...")
    db_info = detect_metadata(
        conn,
        override_account_id=config.metadata.account_id,
        override_connection_name=config.metadata.connection_name,
        override_region=config.metadata.region,
        override_instance_id=config.metadata.instance_id,
    )
    report.database_info = db_info
    logger.info(
        "Metadata: account=%s region=%s instance=%s",
        db_info.account_id,
        db_info.region,
        db_info.instance_id,
    )

    writer = _create_writer(config)

    databases = list_databases(
        conn, exclude=config.filtering.exclude_databases
    )
    logger.info("Found %d databases: %s", len(databases), databases)

    for database in databases:
        logger.info("Processing database: %s", database)
        objects = extract_all(
            conn,
            database,
            strip_auto_increment=config.behavior.strip_auto_increment,
        )

        for obj in objects:
            path = writer.write(obj, db_info)
            logger.debug("Wrote: %s", path)

            key = obj.object_type.value
            report.object_counts[key] = report.object_counts.get(key, 0) + 1
            report.total_objects += 1

        report.databases_processed.append(database)


def run_export(config: Config) -> RunReport:
    """Run the full schema export pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    _ensure_output_dir(config)

    report = RunReport(
        database_info=None,  # type: ignore[arg-type]
        started_at=datetime.now(timezone.utc),
    )

    tunnel = None
    tsh = None

    try:
        # Connect to MySQL
        connect_kwargs = _build_connect_kwargs(config)

        logger.info(
            "Connecting to MySQL at %s:%d",
            config.connection.host,
            config.connection.port,
        )

        try:
            conn = pymysql.connect(**connect_kwargs)
        except pymysql.err.OperationalError as e:
            if "Connection refused" not in str(e):
                raise

            # Attempt Teleport interactive fallback
            logger.info("Connection refused — attempting Teleport interactive flow...")
            from rds_schema_exporter.teleport import (
                ALL_SENTINEL,
                find_tsh,
                get_clusters,
                get_logged_in_user,
                interactive_select,
                list_mysql_databases,
                start_tunnel,
                stop_tunnel,
            )

            tsh = find_tsh(config.teleport.tsh_path)
            clusters = get_clusters(tsh)
            cluster = interactive_select(clusters, "Select a Teleport cluster")
            databases = list_mysql_databases(tsh, cluster)
            db_names = [d["name"] for d in databases]
            choice = interactive_select(
                db_names, "Select an RDS instance", allow_all=True
            )

            db_user = get_logged_in_user(tsh, cluster)

            if choice == ALL_SENTINEL:
                # --- Export every database sequentially ---
                failed_instances: list[str] = []
                for idx, selected_db in enumerate(databases, 1):
                    name = selected_db["name"]
                    logger.info(
                        "Exporting %d/%d: %s ...", idx, len(databases), name
                    )
                    try:
                        tunnel = start_tunnel(tsh, name, db_user, cluster=cluster)
                        config.connection.host = tunnel.host
                        config.connection.port = tunnel.port
                        config.connection.user = db_user

                        config.metadata.account_id = cluster
                        config.metadata.connection_name = name
                        config.metadata.region = selected_db.get("region", "")
                        config.metadata.instance_id = selected_db.get("instance_id", "")

                        connect_kwargs = _build_connect_kwargs(config)
                        conn = pymysql.connect(**connect_kwargs)
                        try:
                            _run_pipeline(conn, config, report)
                        finally:
                            conn.close()
                    except Exception as exc:
                        logger.error(
                            "Failed to export %s: %s", name, exc, exc_info=True
                        )
                        failed_instances.append(f"{name}: {exc}")
                    finally:
                        if tunnel is not None:
                            stop_tunnel(tsh, tunnel)
                            tunnel = None

                if failed_instances:
                    report.success = False
                    report.error = (
                        f"{len(failed_instances)} instance(s) failed:\n"
                        + "\n".join(f"  - {f}" for f in failed_instances)
                    )
                else:
                    report.success = True
                # Skip the normal single-db path below
                raise _AllDone()

            # --- Single database selected ---
            selected_db = next(d for d in databases if d["name"] == choice)
            tunnel = start_tunnel(tsh, choice, db_user, cluster=cluster)

            # Override connection config with tunnel details
            config.connection.host = tunnel.host
            config.connection.port = tunnel.port
            config.connection.user = db_user

            # Auto-populate metadata from Teleport DB info
            if not config.metadata.account_id:
                config.metadata.account_id = cluster
            config.metadata.connection_name = choice
            if not config.metadata.region:
                config.metadata.region = selected_db.get("region", "")
            if not config.metadata.instance_id:
                config.metadata.instance_id = selected_db.get("instance_id", "")

            connect_kwargs = _build_connect_kwargs(config)
            conn = pymysql.connect(**connect_kwargs)

        try:
            _run_pipeline(conn, config, report)
            report.success = True

        finally:
            conn.close()

    except _AllDone:
        pass  # loop path already set report.success

    except Exception as e:
        report.error = str(e)
        report.success = False
        logger.error("Export failed: %s", e, exc_info=True)

    finally:
        # Always clean up Teleport tunnel if one was started
        if tunnel is not None:
            from rds_schema_exporter.teleport import stop_tunnel

            assert tsh is not None
            stop_tunnel(tsh, tunnel)

    report.finished_at = datetime.now(timezone.utc)

    # Print summary
    summary = format_summary(report)
    logger.info("\n%s", summary)

    # Send Slack notification
    if config.notifications.slack_webhook_url:
        send_slack_notification(config.notifications.slack_webhook_url, report)

    if not report.success:
        raise RuntimeError(f"Schema export failed: {report.error}")

    return report


class _AllDone(Exception):
    """Internal signal: the 'export all' loop has finished."""
