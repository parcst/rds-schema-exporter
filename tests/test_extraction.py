"""Tests for all extraction modules."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from rds_schema_exporter.extraction.databases import list_databases
from rds_schema_exporter.extraction.events import extract_events
from rds_schema_exporter.extraction.functions import extract_functions
from rds_schema_exporter.extraction.indexes import extract_indexes
from rds_schema_exporter.extraction.procedures import extract_procedures
from rds_schema_exporter.extraction.tables import extract_tables, strip_auto_increment
from rds_schema_exporter.extraction.triggers import extract_triggers
from rds_schema_exporter.extraction.views import extract_views
from rds_schema_exporter.models import ObjectType


# ---------------------------------------------------------------------------
# Tables
# ---------------------------------------------------------------------------


def test_extract_tables(mock_connection):
    """Extract tables: mock cursor returns table list, then CREATE TABLE DDL."""
    cursor = mock_connection.cursor.return_value

    # First execute: USE `testdb`
    # Second execute: SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'
    # Third execute: SHOW CREATE TABLE `users`
    cursor.fetchall.return_value = [("users", "BASE TABLE")]
    cursor.fetchone.return_value = (
        "users",
        "CREATE TABLE `users` (\n  `id` int NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`id`)\n) ENGINE=InnoDB AUTO_INCREMENT=42",
    )

    objects = extract_tables(mock_connection, "testdb", do_strip_auto_increment=True)

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.TABLE
    assert obj.name == "users"
    # Table-level AUTO_INCREMENT=42 should be stripped, column-level stays
    assert "AUTO_INCREMENT=42" not in obj.ddl
    assert obj.ddl.endswith(";\n")


def test_extract_tables_no_strip(mock_connection):
    """When strip_auto_increment is disabled, AUTO_INCREMENT is preserved."""
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [("orders", "BASE TABLE")]
    cursor.fetchone.return_value = (
        "orders",
        "CREATE TABLE `orders` (\n  `id` int NOT NULL\n) ENGINE=InnoDB AUTO_INCREMENT=100",
    )

    objects = extract_tables(mock_connection, "testdb", do_strip_auto_increment=False)

    assert len(objects) == 1
    assert "AUTO_INCREMENT=100" in objects[0].ddl


# ---------------------------------------------------------------------------
# strip_auto_increment helper
# ---------------------------------------------------------------------------


def test_strip_auto_increment_basic():
    ddl = "CREATE TABLE t (id int) ENGINE=InnoDB AUTO_INCREMENT=123"
    assert "AUTO_INCREMENT" not in strip_auto_increment(ddl)


def test_strip_auto_increment_large_number():
    ddl = "CREATE TABLE t (id int) ENGINE=InnoDB AUTO_INCREMENT=99999999"
    assert "AUTO_INCREMENT" not in strip_auto_increment(ddl)


def test_strip_auto_increment_no_match():
    ddl = "CREATE TABLE t (id int) ENGINE=InnoDB"
    assert strip_auto_increment(ddl) == ddl


def test_strip_auto_increment_case_insensitive():
    ddl = "CREATE TABLE t (id int) ENGINE=InnoDB auto_increment=5"
    assert "auto_increment" not in strip_auto_increment(ddl)


# ---------------------------------------------------------------------------
# Views
# ---------------------------------------------------------------------------


def test_extract_views(mock_connection):
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [("active_users", "VIEW")]
    cursor.fetchone.return_value = (
        "active_users",
        "CREATE VIEW `active_users` AS SELECT * FROM `users` WHERE active = 1",
        "utf8mb4",
        "utf8mb4_general_ci",
    )

    objects = extract_views(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.VIEW
    assert obj.name == "active_users"
    assert "CREATE VIEW" in obj.ddl
    assert obj.ddl.endswith(";\n")


# ---------------------------------------------------------------------------
# Procedures
# ---------------------------------------------------------------------------


def test_extract_procedures(mock_connection):
    cursor = mock_connection.cursor.return_value

    # First call: information_schema query returns procedure names
    # Second call after USE: SHOW CREATE PROCEDURE
    cursor.fetchall.return_value = [("cleanup_old_rows",)]
    cursor.fetchone.return_value = (
        "cleanup_old_rows",
        "NO_AUTO_VALUE_ON_ZERO",
        "CREATE PROCEDURE `cleanup_old_rows`() BEGIN DELETE FROM logs WHERE created < NOW() - INTERVAL 30 DAY; END",
        "utf8mb4",
        "utf8mb4_general_ci",
        "utf8mb4_general_ci",
    )

    objects = extract_procedures(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.PROCEDURE
    assert obj.name == "cleanup_old_rows"
    assert "CREATE PROCEDURE" in obj.ddl
    assert obj.ddl.endswith(";\n")


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------


def test_extract_functions(mock_connection):
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [("full_name",)]
    cursor.fetchone.return_value = (
        "full_name",
        "NO_AUTO_VALUE_ON_ZERO",
        "CREATE FUNCTION `full_name`(first VARCHAR(50), last VARCHAR(50)) RETURNS varchar(101) RETURN CONCAT(first, ' ', last)",
        "utf8mb4",
        "utf8mb4_general_ci",
        "utf8mb4_general_ci",
    )

    objects = extract_functions(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.FUNCTION
    assert obj.name == "full_name"
    assert "CREATE FUNCTION" in obj.ddl
    assert obj.ddl.endswith(";\n")


# ---------------------------------------------------------------------------
# Triggers
# ---------------------------------------------------------------------------


def test_extract_triggers(mock_connection):
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [("before_insert_users",)]
    cursor.fetchone.return_value = (
        "before_insert_users",
        "NO_AUTO_VALUE_ON_ZERO",
        "CREATE TRIGGER `before_insert_users` BEFORE INSERT ON `users` FOR EACH ROW SET NEW.created = NOW()",
        "utf8mb4",
        "utf8mb4_general_ci",
        "utf8mb4_general_ci",
        "2024-01-01 00:00:00",
    )

    objects = extract_triggers(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.TRIGGER
    assert obj.name == "before_insert_users"
    assert "CREATE TRIGGER" in obj.ddl
    assert obj.ddl.endswith(";\n")


# ---------------------------------------------------------------------------
# Events
# ---------------------------------------------------------------------------


def test_extract_events(mock_connection):
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [("daily_cleanup",)]
    cursor.fetchone.return_value = (
        "daily_cleanup",
        "NO_AUTO_VALUE_ON_ZERO",
        "SYSTEM",
        "CREATE EVENT `daily_cleanup` ON SCHEDULE EVERY 1 DAY DO DELETE FROM logs WHERE ts < NOW() - INTERVAL 7 DAY",
        "utf8mb4",
        "utf8mb4_general_ci",
        "utf8mb4_general_ci",
    )

    objects = extract_events(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.EVENT
    assert obj.name == "daily_cleanup"
    assert "CREATE EVENT" in obj.ddl
    assert obj.ddl.endswith(";\n")


# ---------------------------------------------------------------------------
# Indexes
# ---------------------------------------------------------------------------


def test_extract_indexes(mock_connection):
    """STATISTICS rows are grouped into synthesized CREATE INDEX DDL."""
    cursor = mock_connection.cursor.return_value

    # Simulate two rows for the same composite index on (email, status)
    cursor.fetchall.return_value = [
        ("idx_email_status", "users", 0, "email", 1, None),
        ("idx_email_status", "users", 0, "status", 2, None),
    ]

    objects = extract_indexes(mock_connection, "testdb")

    assert len(objects) == 1
    obj = objects[0]
    assert obj.database == "testdb"
    assert obj.object_type == ObjectType.INDEX
    assert obj.name == "users__idx_email_status"
    assert "CREATE UNIQUE INDEX" in obj.ddl
    assert "`email`, `status`" in obj.ddl
    assert "ON `testdb`.`users`" in obj.ddl


def test_extract_indexes_non_unique(mock_connection):
    """Non-unique index should not contain the UNIQUE keyword."""
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [
        ("idx_name", "users", 1, "name", 1, None),
    ]

    objects = extract_indexes(mock_connection, "testdb")

    assert len(objects) == 1
    assert "UNIQUE" not in objects[0].ddl
    assert "CREATE INDEX" in objects[0].ddl


def test_extract_indexes_with_sub_part(mock_connection):
    """Sub-part (prefix length) should appear in the column expression."""
    cursor = mock_connection.cursor.return_value

    cursor.fetchall.return_value = [
        ("idx_body", "posts", 1, "body", 1, 100),
    ]

    objects = extract_indexes(mock_connection, "testdb")

    assert len(objects) == 1
    assert "`body`(100)" in objects[0].ddl


# ---------------------------------------------------------------------------
# list_databases
# ---------------------------------------------------------------------------


def test_list_databases(mock_connection):
    """System databases are excluded by default."""
    cursor = mock_connection.cursor.return_value
    cursor.fetchall.return_value = [
        ("information_schema",),
        ("mysql",),
        ("performance_schema",),
        ("sys",),
        ("myapp",),
        ("analytics",),
    ]

    databases = list_databases(mock_connection)

    assert databases == ["analytics", "myapp"]


def test_list_databases_with_exclusions(mock_connection):
    """Custom exclusions are merged with system database exclusions."""
    cursor = mock_connection.cursor.return_value
    cursor.fetchall.return_value = [
        ("information_schema",),
        ("mysql",),
        ("performance_schema",),
        ("sys",),
        ("myapp",),
        ("analytics",),
        ("staging",),
    ]

    databases = list_databases(mock_connection, exclude=["staging"])

    assert "staging" not in databases
    assert "myapp" in databases
    assert "analytics" in databases
    # system dbs should still be excluded
    assert "mysql" not in databases
