"""Extract view DDL from MySQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_views(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Extract CREATE VIEW statements for all views in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(f"USE `{database}`")
        cursor.execute("SHOW FULL TABLES WHERE Table_type = 'VIEW'")
        view_rows = cursor.fetchall()

        for row in view_rows:
            view_name = row[0]
            cursor.execute(f"SHOW CREATE VIEW `{view_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[1]

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.VIEW,
                    name=view_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
