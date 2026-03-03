"""Configuration loading for rds-schema-exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml


@dataclass
class ConnectionConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""


@dataclass
class OutputConfig:
    target: str = "local"  # "local" or "s3"
    local_path: str = str(Path.home() / "SchemaDump")
    s3_bucket: str = ""
    s3_prefix: str = ""


@dataclass
class MetadataConfig:
    account_id: str = ""
    connection_name: str = ""
    region: str = ""
    instance_id: str = ""


@dataclass
class FilteringConfig:
    exclude_databases: list[str] = field(default_factory=list)


@dataclass
class NotificationsConfig:
    slack_webhook_url: str = ""


@dataclass
class BehaviorConfig:
    strip_auto_increment: bool = True
    strip_definer: bool = False


@dataclass
class TeleportConfig:
    tsh_path: str = ""  # auto-detected if blank


@dataclass
class Config:
    connection: ConnectionConfig = field(default_factory=ConnectionConfig)
    output: OutputConfig = field(default_factory=OutputConfig)
    metadata: MetadataConfig = field(default_factory=MetadataConfig)
    filtering: FilteringConfig = field(default_factory=FilteringConfig)
    notifications: NotificationsConfig = field(default_factory=NotificationsConfig)
    behavior: BehaviorConfig = field(default_factory=BehaviorConfig)
    teleport: TeleportConfig = field(default_factory=TeleportConfig)


def load_config(config_path: str | Path | None = None) -> Config:
    """Load config from a YAML file, returning defaults if no path given."""
    if config_path is None:
        return Config()

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f) or {}

    return _parse_config(raw)


def _parse_config(raw: dict) -> Config:
    """Parse a raw dict into a Config object."""
    config = Config()

    if conn := raw.get("connection"):
        config.connection = ConnectionConfig(
            host=conn.get("host", config.connection.host),
            port=int(conn.get("port", config.connection.port)),
            user=conn.get("user", config.connection.user),
            password=conn.get("password", config.connection.password),
        )

    if out := raw.get("output"):
        config.output = OutputConfig(
            target=out.get("target", config.output.target),
            local_path=out.get("local_path", config.output.local_path),
            s3_bucket=out.get("s3_bucket", config.output.s3_bucket),
            s3_prefix=out.get("s3_prefix", config.output.s3_prefix),
        )

    if meta := raw.get("metadata"):
        config.metadata = MetadataConfig(
            account_id=meta.get("account_id", config.metadata.account_id),
            connection_name=meta.get("connection_name", config.metadata.connection_name),
            region=meta.get("region", config.metadata.region),
            instance_id=meta.get("instance_id", config.metadata.instance_id),
        )

    if filt := raw.get("filtering"):
        config.filtering = FilteringConfig(
            exclude_databases=filt.get(
                "exclude_databases", config.filtering.exclude_databases
            ),
        )

    if notif := raw.get("notifications"):
        config.notifications = NotificationsConfig(
            slack_webhook_url=notif.get(
                "slack_webhook_url", config.notifications.slack_webhook_url
            ),
        )

    if behav := raw.get("behavior"):
        config.behavior = BehaviorConfig(
            strip_auto_increment=behav.get(
                "strip_auto_increment", config.behavior.strip_auto_increment
            ),
            strip_definer=behav.get(
                "strip_definer", config.behavior.strip_definer
            ),
        )

    if tp := raw.get("teleport"):
        config.teleport = TeleportConfig(
            tsh_path=tp.get("tsh_path", config.teleport.tsh_path),
        )

    return config
