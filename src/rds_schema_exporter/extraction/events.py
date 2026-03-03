"""Extract event DDL from MySQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_events(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Extract CREATE EVENT statements for all events in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT EVENT_NAME FROM information_schema.EVENTS "
            "WHERE EVENT_SCHEMA = %s",
            (database,),
        )
        event_rows = cursor.fetchall()

        cursor.execute(f"USE `{database}`")

        for row in event_rows:
            event_name = row[0]
            cursor.execute(f"SHOW CREATE EVENT `{event_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[3]

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.EVENT,
                    name=event_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
