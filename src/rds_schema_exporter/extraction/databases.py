"""List databases from a MySQL connection."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pymysql

SYSTEM_DATABASES = {"information_schema", "mysql", "performance_schema", "sys"}


def list_databases(
    conn: pymysql.Connection,
    exclude: list[str] | None = None,
) -> list[str]:
    """Return non-system database names, minus any in the exclude list."""
    exclude_set = SYSTEM_DATABASES | set(exclude or [])

    with conn.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        rows = cursor.fetchall()

    return sorted(
        row[0] for row in rows if row[0] not in exclude_set
    )
