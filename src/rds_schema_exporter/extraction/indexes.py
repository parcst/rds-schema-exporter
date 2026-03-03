"""Extract non-primary index DDL from MySQL information_schema."""

from __future__ import annotations

from typing import TYPE_CHECKING

from rds_schema_exporter.models import ObjectType, SchemaObject

if TYPE_CHECKING:
    import pymysql


def extract_indexes(
    conn: pymysql.Connection,
    database: str,
) -> list[SchemaObject]:
    """Synthesize CREATE INDEX statements from information_schema.STATISTICS.

    Excludes PRIMARY KEY indexes since those are part of CREATE TABLE.
    """
    objects: list[SchemaObject] = []

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                INDEX_NAME,
                TABLE_NAME,
                NON_UNIQUE,
                COLUMN_NAME,
                SEQ_IN_INDEX,
                SUB_PART
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = %s
              AND INDEX_NAME != 'PRIMARY'
            ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
            """,
            (database,),
        )
        rows = cursor.fetchall()

    # Group columns by (table_name, index_name)
    indexes: dict[tuple[str, str], dict] = {}
    for index_name, table_name, non_unique, column_name, seq, sub_part in rows:
        key = (table_name, index_name)
        if key not in indexes:
            indexes[key] = {
                "table_name": table_name,
                "index_name": index_name,
                "unique": not non_unique,
                "columns": [],
            }
        col_expr = f"`{column_name}`"
        if sub_part is not None:
            col_expr += f"({sub_part})"
        indexes[key]["columns"].append(col_expr)

    for (table_name, index_name), info in indexes.items():
        unique = "UNIQUE " if info["unique"] else ""
        columns = ", ".join(info["columns"])
        ddl = (
            f"CREATE {unique}INDEX `{index_name}` "
            f"ON `{database}`.`{table_name}` ({columns});\n"
        )
        # Use table__index as the file name to avoid collisions
        objects.append(
            SchemaObject(
                database=database,
                object_type=ObjectType.INDEX,
                name=f"{table_name}__{index_name}",
                ddl=ddl,
            )
        )

    return objects
