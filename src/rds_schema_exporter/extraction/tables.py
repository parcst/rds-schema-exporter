"""Extract table DDL from MySQL."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql

AUTO_INCREMENT_RE = re.compile(r"\s*AUTO_INCREMENT=\d+", re.IGNORECASE)


def strip_auto_increment(ddl: str) -> str:
    """Remove AUTO_INCREMENT=N from CREATE TABLE DDL."""
    return AUTO_INCREMENT_RE.sub("", ddl)


def extract_tables(
    conn: pymysql.Connection,
    database: str,
    *,
    do_strip_auto_increment: bool = True,
) -> list[SchemaObject]:
    """Extract CREATE TABLE statements for all base tables in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(f"USE `{database}`")
        cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
        table_rows = cursor.fetchall()

        for row in table_rows:
            table_name = row[0]
            cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[1]

            if do_strip_auto_increment:
                ddl = strip_auto_increment(ddl)

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.TABLE,
                    name=table_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
