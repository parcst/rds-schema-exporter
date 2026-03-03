"""CLI interface for rds-schema-exporter."""

from __future__ import annotations

import sys

import click

from rds_schema_exporter.config import load_config
from rds_schema_exporter.runner import run_export


@click.group()
def cli() -> None:
    """RDS Schema Exporter — export MySQL schema DDL from RDS instances."""


@cli.command()
@click.option("--config", "config_path", type=click.Path(exists=True), help="Path to YAML config file.")
@click.option("--host", default=None, help="MySQL host (default: localhost).")
@click.option("--port", default=None, type=int, help="MySQL port (default: 3306).")
@click.option("--user", default=None, help="MySQL user.")
@click.option("--password", default=None, help="MySQL password.")
@click.option("--output", "output_target", type=click.Choice(["local", "s3"]), default=None, help="Output target.")
@click.option("--output-path", default=None, help="Local output directory.")
@click.option("--bucket", default=None, help="S3 bucket name.")
@click.option("--s3-prefix", default=None, help="S3 key prefix.")
@click.option("--account-id", default=None, help="AWS account ID (overrides auto-detection).")
@click.option("--region", default=None, help="AWS region (overrides auto-detection).")
@click.option("--instance-id", default=None, help="RDS instance ID (overrides auto-detection).")
@click.option("--exclude-db", multiple=True, help="Database names to exclude (repeatable).")
@click.option("--slack-webhook-url", default=None, help="Slack webhook URL for notifications.")
def run(
    config_path: str | None,
    host: str | None,
    port: int | None,
    user: str | None,
    password: str | None,
    output_target: str | None,
    output_path: str | None,
    bucket: str | None,
    s3_prefix: str | None,
    account_id: str | None,
    region: str | None,
    instance_id: str | None,
    exclude_db: tuple[str, ...],
    slack_webhook_url: str | None,
) -> None:
    """Run the schema export."""
    config = load_config(config_path)

    # CLI flags override config file values
    if host is not None:
        config.connection.host = host
    if port is not None:
        config.connection.port = port
    if user is not None:
        config.connection.user = user
    if password is not None:
        config.connection.password = password

    if output_target is not None:
        config.output.target = output_target
    if output_path is not None:
        config.output.local_path = output_path
    if bucket is not None:
        config.output.s3_bucket = bucket
    if s3_prefix is not None:
        config.output.s3_prefix = s3_prefix

    if account_id is not None:
        config.metadata.account_id = account_id
    if region is not None:
        config.metadata.region = region
    if instance_id is not None:
        config.metadata.instance_id = instance_id

    if exclude_db:
        config.filtering.exclude_databases = list(exclude_db)

    if slack_webhook_url is not None:
        config.notifications.slack_webhook_url = slack_webhook_url

    try:
        run_export(config)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)
