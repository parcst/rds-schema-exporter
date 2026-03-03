"""Data models for rds-schema-exporter."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum


class ObjectType(str, Enum):
    TABLE = "tables"
    VIEW = "views"
    PROCEDURE = "procedures"
    FUNCTION = "functions"
    TRIGGER = "triggers"
    EVENT = "events"
    INDEX = "indexes"


@dataclass
class SchemaObject:
    """A single schema object with its DDL."""

    database: str
    object_type: ObjectType
    name: str
    ddl: str


@dataclass
class DatabaseInfo:
    """Auto-detected or provided AWS metadata for an RDS instance."""

    account_id: str
    region: str
    instance_id: str
    connection_name: str = ""


@dataclass
class RunReport:
    """Summary of a schema export run."""

    database_info: DatabaseInfo
    started_at: datetime
    finished_at: datetime | None = None
    databases_processed: list[str] = field(default_factory=list)
    object_counts: dict[str, int] = field(default_factory=dict)
    total_objects: int = 0
    success: bool = False
    error: str | None = None

    @property
    def duration_seconds(self) -> float:
        if self.finished_at is None:
            return 0.0
        return (self.finished_at - self.started_at).total_seconds()
