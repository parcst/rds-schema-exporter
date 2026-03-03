"""Extract stored procedure DDL from MySQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_procedures(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Extract CREATE PROCEDURE statements for all procedures in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT ROUTINE_NAME FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'PROCEDURE'",
            (database,),
        )
        proc_rows = cursor.fetchall()

        cursor.execute(f"USE `{database}`")

        for row in proc_rows:
            proc_name = row[0]
            cursor.execute(f"SHOW CREATE PROCEDURE `{proc_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[2]

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.PROCEDURE,
                    name=proc_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
