"""Extract function DDL from MySQL."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_functions(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Extract CREATE FUNCTION statements for all functions in a database."""
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(
            "SELECT ROUTINE_NAME FROM information_schema.ROUTINES "
            "WHERE ROUTINE_SCHEMA = %s AND ROUTINE_TYPE = 'FUNCTION'",
            (database,),
        )
        func_rows = cursor.fetchall()

        cursor.execute(f"USE `{database}`")

        for row in func_rows:
            func_name = row[0]
            cursor.execute(f"SHOW CREATE FUNCTION `{func_name}`")
            create_row = cursor.fetchone()
            ddl = create_row[2]

            objects.append(
                SchemaObject(
                    database=database,
                    object_type=ObjectType.FUNCTION,
                    name=func_name,
                    ddl=ddl + ";\n",
                )
            )

    return objects
