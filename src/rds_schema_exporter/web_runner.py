"""Streaming export runner for the web UI.

Reuses the same building blocks as ``runner.py`` (extractors, writers,
metadata detection) but emits :class:`ExportEvent` callbacks instead of
logging — suitable for SSE streaming.

The two public entry points are blocking functions designed to be called
from a thread executor so the async SSE generator can poll for events.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import TYPE_CHECKING, Callable

import pymysql

from rds_schema_exporter.config import Config
from rds_schema_exporter.extraction.databases import list_databases
from rds_schema_exporter.extraction.events import extract_events
from rds_schema_exporter.extraction.functions import extract_functions
from rds_schema_exporter.extraction.indexes import extract_indexes
from rds_schema_exporter.extraction.procedures import extract_procedures
from rds_schema_exporter.extraction.tables import extract_tables
from rds_schema_exporter.extraction.triggers import extract_triggers
from rds_schema_exporter.extraction.views import extract_views
from rds_schema_exporter.metadata import detect_metadata
from rds_schema_exporter.models import ExportEvent, ExportEventType
from rds_schema_exporter.teleport import (
    find_tsh,
    list_mysql_databases,
    start_tunnel,
    stop_tunnel,
)
from rds_schema_exporter.writers.local import LocalWriter
from rds_schema_exporter.writers.s3 import S3Writer

if TYPE_CHECKING:
    from rds_schema_exporter.writers.base import Writer

logger = logging.getLogger(__name__)

OnEvent = Callable[[ExportEvent], None]


def _create_writer(config: Config) -> Writer:
    if config.output.target == "s3":
        if not config.output.s3_bucket:
            raise ValueError("S3 bucket is required when output target is 's3'.")
        return S3Writer(bucket=config.output.s3_bucket, prefix=config.output.s3_prefix)
    return LocalWriter(base_path=config.output.local_path)


def _ensure_output_dir(config: Config) -> None:
    """Auto-create the local output directory (no interactive prompt in web mode)."""
    if config.output.target != "local":
        return
    base = Path(config.output.local_path).expanduser()
    if not base.exists():
        base.mkdir(parents=True)
        config.output.local_path = str(base)


def _run_streaming_pipeline(
    conn: pymysql.connections.Connection,
    config: Config,
    instance_name: str,
    on_event: OnEvent,
    *,
    db_mode: str = "user",
    include_databases: list[str] | None = None,
    cumulative_objects: int = 0,
    cumulative_counts: dict[str, int] | None = None,
) -> tuple[int, dict[str, int]]:
    """Inner pipeline: metadata -> list databases -> extract -> write.

    *db_mode* controls which databases are exported:
    - ``"user"`` — all non-system databases (default)
    - ``"all"`` — every database including system ones
    - ``"specific"`` — only *include_databases*

    Returns ``(total_objects, object_counts)`` accumulated across calls.
    """
    total_objects = cumulative_objects
    object_counts = dict(cumulative_counts or {})

    on_event(ExportEvent(
        event_type=ExportEventType.STEP,
        message=f"Detecting metadata for {instance_name}...",
        instance_name=instance_name,
    ))

    db_info = detect_metadata(
        conn,
        override_account_id=config.metadata.account_id,
        override_connection_name=config.metadata.connection_name,
        override_region=config.metadata.region,
        override_instance_id=config.metadata.instance_id,
    )

    writer = _create_writer(config)

    on_event(ExportEvent(
        event_type=ExportEventType.STEP,
        message=f"Listing databases on {instance_name}...",
        instance_name=instance_name,
    ))

    if db_mode == "all":
        databases = list_databases(conn, exclude=[])
    elif db_mode == "specific" and include_databases:
        databases = include_databases
    else:
        databases = list_databases(conn, exclude=config.filtering.exclude_databases)

    # Individual extractors called one at a time so we can emit
    # step events between SQL queries (extract_all batches them).
    extractors = [
        ("tables", lambda db: extract_tables(conn, db, do_strip_auto_increment=config.behavior.strip_auto_increment)),
        ("views", lambda db: extract_views(conn, db)),
        ("procedures", lambda db: extract_procedures(conn, db)),
        ("functions", lambda db: extract_functions(conn, db)),
        ("triggers", lambda db: extract_triggers(conn, db)),
        ("events", lambda db: extract_events(conn, db)),
        ("indexes", lambda db: extract_indexes(conn, db)),
    ]

    total_types = len(extractors)

    for db_idx, database in enumerate(databases, 1):
        on_event(ExportEvent(
            event_type=ExportEventType.DATABASE_START,
            instance_name=instance_name,
            database_name=database,
            current_database=db_idx,
            total_databases=len(databases),
            total_objects=total_objects,
            object_counts=dict(object_counts),
        ))

        # Single pass: extract + write each type, emit one batch event per type.
        # Each SQL query provides a natural pause so the browser can repaint
        # between batches (7 updates per database instead of 1000+ instant ones).
        db_objects_written = 0

        for type_idx, (type_label, extract_fn) in enumerate(extractors, 1):
            on_event(ExportEvent(
                event_type=ExportEventType.STEP,
                message=f"{database} — extracting {type_label} ({type_idx}/{total_types})...",
                instance_name=instance_name,
                database_name=database,
            ))

            batch = extract_fn(database)

            # Write immediately and collect results for the batch event
            written: list[dict[str, str]] = []
            for obj in batch:
                path = writer.write(obj, db_info)
                key = obj.object_type.value
                object_counts[key] = object_counts.get(key, 0) + 1
                total_objects += 1
                db_objects_written += 1
                written.append({"name": obj.name, "path": path})

            # Emit one batch event per type (even if empty, for progress tracking)
            on_event(ExportEvent(
                event_type=ExportEventType.OBJECT_EXTRACTED,
                instance_name=instance_name,
                database_name=database,
                object_type=type_label,
                total_objects=total_objects,
                current_object_in_db=type_idx,
                total_objects_in_db=total_types,
                object_counts=dict(object_counts),
                batch_objects=written,
            ))

        on_event(ExportEvent(
            event_type=ExportEventType.DATABASE_DONE,
            instance_name=instance_name,
            database_name=database,
            current_database=db_idx,
            total_databases=len(databases),
            total_objects=total_objects,
            object_counts=dict(object_counts),
        ))

    return total_objects, object_counts


def run_single_instance_export(
    config: Config,
    instance_name: str,
    db_user: str,
    cluster: str,
    on_event: OnEvent,
    *,
    db_mode: str = "user",
    include_databases: list[str] | None = None,
) -> None:
    """Export a single Teleport instance. Blocking — run in an executor."""
    start = time.monotonic()
    tsh = find_tsh(config.teleport.tsh_path)
    tunnel = None

    try:
        _ensure_output_dir(config)

        # Look up instance metadata from Teleport
        databases = list_mysql_databases(tsh, cluster)
        selected = next((d for d in databases if d["name"] == instance_name), None)
        if not selected:
            raise ValueError(f"Instance '{instance_name}' not found on cluster '{cluster}'")

        on_event(ExportEvent(
            event_type=ExportEventType.INSTANCE_START,
            instance_name=instance_name,
            message=f"Starting tunnel for {instance_name}...",
            current_instance=1,
            total_instances=1,
        ))

        tunnel = start_tunnel(tsh, instance_name, db_user, cluster=cluster)

        config.connection.host = tunnel.host
        config.connection.port = tunnel.port
        config.connection.user = db_user
        config.metadata.account_id = config.metadata.account_id or cluster
        config.metadata.connection_name = instance_name
        config.metadata.region = config.metadata.region or selected.get("region", "")
        config.metadata.instance_id = config.metadata.instance_id or selected.get("instance_id", "")

        on_event(ExportEvent(
            event_type=ExportEventType.STEP,
            message=f"Connecting to {instance_name}...",
            instance_name=instance_name,
        ))

        conn = pymysql.connect(
            host=tunnel.host,
            port=tunnel.port,
            user=db_user,
        )

        try:
            total_objects, object_counts = _run_streaming_pipeline(
                conn, config, instance_name, on_event,
                db_mode=db_mode,
                include_databases=include_databases,
            )
        finally:
            conn.close()

        on_event(ExportEvent(
            event_type=ExportEventType.INSTANCE_DONE,
            instance_name=instance_name,
            total_objects=total_objects,
            object_counts=object_counts,
        ))

        duration = time.monotonic() - start
        on_event(ExportEvent(
            event_type=ExportEventType.DONE,
            success=True,
            total_objects=total_objects,
            object_counts=object_counts,
            duration=duration,
        ))

    except Exception as e:
        logger.exception("Export failed for %s", instance_name)
        duration = time.monotonic() - start
        on_event(ExportEvent(
            event_type=ExportEventType.ERROR,
            message=str(e),
            instance_name=instance_name,
            duration=duration,
        ))

    finally:
        if tunnel is not None:
            stop_tunnel(tsh, tunnel)


def run_all_instances_export(
    config: Config,
    db_user: str,
    cluster: str,
    on_event: OnEvent,
    *,
    db_mode: str = "user",
    include_databases: list[str] | None = None,
) -> None:
    """Export all MySQL instances on a cluster. Blocking — run in an executor."""
    start = time.monotonic()
    tsh = find_tsh(config.teleport.tsh_path)

    try:
        _ensure_output_dir(config)

        on_event(ExportEvent(
            event_type=ExportEventType.STEP,
            message="Discovering MySQL instances...",
        ))

        all_databases = list_mysql_databases(tsh, cluster)
        total_instances = len(all_databases)

        on_event(ExportEvent(
            event_type=ExportEventType.STEP,
            message=f"Found {total_instances} instance{'s' if total_instances != 1 else ''}. Starting export...",
        ))

        cumulative_objects = 0
        cumulative_counts: dict[str, int] = {}
        errors: list[str] = []

        for idx, entry in enumerate(all_databases, 1):
            name = entry["name"]
            tunnel = None

            try:
                on_event(ExportEvent(
                    event_type=ExportEventType.INSTANCE_START,
                    instance_name=name,
                    message=f"Starting tunnel for {name}...",
                    current_instance=idx,
                    total_instances=total_instances,
                    total_objects=cumulative_objects,
                    object_counts=dict(cumulative_counts),
                ))

                tunnel = start_tunnel(tsh, name, db_user, cluster=cluster)

                # Configure for this instance
                cfg_copy_meta_account = config.metadata.account_id or cluster
                config.metadata.account_id = cfg_copy_meta_account
                config.metadata.connection_name = name
                config.metadata.region = entry.get("region", "")
                config.metadata.instance_id = entry.get("instance_id", "")

                on_event(ExportEvent(
                    event_type=ExportEventType.STEP,
                    message=f"Connecting to {name}...",
                    instance_name=name,
                ))

                conn = pymysql.connect(
                    host=tunnel.host,
                    port=tunnel.port,
                    user=db_user,
                )

                try:
                    cumulative_objects, cumulative_counts = _run_streaming_pipeline(
                        conn, config, name, on_event,
                        db_mode=db_mode,
                        include_databases=include_databases,
                        cumulative_objects=cumulative_objects,
                        cumulative_counts=cumulative_counts,
                    )
                finally:
                    conn.close()

                on_event(ExportEvent(
                    event_type=ExportEventType.INSTANCE_DONE,
                    instance_name=name,
                    current_instance=idx,
                    total_instances=total_instances,
                    total_objects=cumulative_objects,
                    object_counts=dict(cumulative_counts),
                ))

            except Exception as e:
                logger.exception("Failed to export %s", name)
                errors.append(f"{name}: {e}")
                on_event(ExportEvent(
                    event_type=ExportEventType.INSTANCE_ERROR,
                    instance_name=name,
                    message=str(e),
                    current_instance=idx,
                    total_instances=total_instances,
                    total_objects=cumulative_objects,
                    object_counts=dict(cumulative_counts),
                ))

            finally:
                if tunnel is not None:
                    stop_tunnel(tsh, tunnel)

        duration = time.monotonic() - start
        on_event(ExportEvent(
            event_type=ExportEventType.DONE,
            success=len(errors) == 0,
            total_objects=cumulative_objects,
            object_counts=cumulative_counts,
            duration=duration,
            errors=errors,
        ))

    except Exception as e:
        logger.exception("Export failed")
        duration = time.monotonic() - start
        on_event(ExportEvent(
            event_type=ExportEventType.ERROR,
            message=str(e),
            duration=duration,
        ))
