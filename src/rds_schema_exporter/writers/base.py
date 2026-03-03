"""Base writer interface."""

from __future__ import annotations

from abc import ABC, abstractmethod

from rds_schema_exporter.models import DatabaseInfo, SchemaObject


class Writer(ABC):
    """Abstract base class for schema output writers."""

    @abstractmethod
    def write(self, obj: SchemaObject, db_info: DatabaseInfo) -> str:
        """Write a schema object and return the output path/key."""

    def build_path(self, obj: SchemaObject, db_info: DatabaseInfo) -> str:
        """Build the standard output path.

        Without connection_name (direct connect):
            ``{account}/{region}/{instance}/{db}/{type}/{name}.sql``

        With connection_name (Teleport):
            ``{account}/{connection}/{region}/{instance}/{db}/{type}/{name}.sql``
        """
        parts = [db_info.account_id]
        if db_info.connection_name:
            parts.append(db_info.connection_name)
        parts.extend([
            db_info.region,
            db_info.instance_id,
            obj.database,
            obj.object_type.value,
            f"{obj.name}.sql",
        ])
        return "/".join(parts)
