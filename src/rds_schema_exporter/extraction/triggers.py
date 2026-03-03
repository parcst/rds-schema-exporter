"""Extract trigger DDL from MySQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_triggers(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Extract CREATE TRIGGER statements for all triggers in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT TRIGGER_NAME FROM information_schema.TRIGGERS "
            "WHERE TRIGGER_SCHEMA = %s",
            (database,),
        )
        trigger_rows = cursor.fetchall()

        cursor.execute(f"USE `{database}`")

        for row in trigger_rows:
            trigger_name = row[0]
            cursor.execute(f"SHOW CREATE TRIGGER `{trigger_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[2]

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.TRIGGER,
                    name=trigger_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
