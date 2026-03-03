"""Schema extraction modules."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import SchemaObject

from .events import extract_events
from .functions import extract_functions
from .indexes import extract_indexes
from .procedures import extract_procedures
from .tables import extract_tables
from .triggers import extract_triggers
from .views import extract_views

if TYPE_CHECKING:
    import pymysql


def extract_all(
    conn: pymysql.Connection,
    database: str,
    *,
    strip_auto_increment: bool = True,
) -> list[SchemaObject]:
    """Run all extractors for a database and return combined results."""
    objects: list[SchemaObject] = []

    objects.extend(extract_tables(conn, database, do_strip_auto_increment=strip_auto_increment))
    objects.extend(extract_views(conn, database))
    objects.extend(extract_procedures(conn, database))
    objects.extend(extract_functions(conn, database))
    objects.extend(extract_triggers(conn, database))
    objects.extend(extract_events(conn, database))
    objects.extend(extract_indexes(conn, database))

    return objects
