"""Local filesystem writer."""

from __future__ import annotations

from pathlib import Path

from rds_schema_exporter.models import DatabaseInfo, SchemaObject

from .base import Writer


class LocalWriter(Writer):
    """Write schema objects to the local filesystem."""

    def __init__(self, base_path: str) -> None:
        self.base_path = Path(base_path)

    def write(self, obj: SchemaObject, db_info: DatabaseInfo) -> str:
        relative = self.build_path(obj, db_info)
        full_path = self.base_path / relative

        full_path.parent.mkdir(parents=True, exist_ok=True)
        full_path.write_text(obj.ddl, encoding="utf-8")

        return str(full_path)
