"""Tests for local and S3 writers."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from rds_schema_exporter.models import DatabaseInfo, ObjectType, SchemaObject
from rds_schema_exporter.writers.base import Writer
from rds_schema_exporter.writers.local import LocalWriter
from rds_schema_exporter.writers.s3 import S3Writer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_schema_object(
    database: str = "myapp",
    object_type: ObjectType = ObjectType.TABLE,
    name: str = "users",
    ddl: str = "CREATE TABLE `users` (id int);\n",
) -> SchemaObject:
    return SchemaObject(
        database=database,
        object_type=object_type,
        name=name,
        ddl=ddl,
    )


def _make_db_info() -> DatabaseInfo:
    return DatabaseInfo(
        account_id="123456789012",
        region="us-east-1",
        instance_id="prod-mysql-01",
    )


# ---------------------------------------------------------------------------
# build_path (from base Writer)
# ---------------------------------------------------------------------------


def test_build_path():
    """Verify the standard path format: {account}/{region}/{instance}/{db}/{type}/{name}.sql"""
    obj = _make_schema_object()
    db_info = _make_db_info()

    # LocalWriter inherits build_path from Writer
    writer = LocalWriter(base_path="/tmp/schemas")
    path = writer.build_path(obj, db_info)

    assert path == "123456789012/us-east-1/prod-mysql-01/myapp/tables/users.sql"


def test_build_path_various_types():
    """build_path works for every ObjectType."""
    db_info = _make_db_info()
    writer = LocalWriter(base_path="/tmp/schemas")

    for obj_type in ObjectType:
        obj = _make_schema_object(object_type=obj_type, name="test_obj")
        path = writer.build_path(obj, db_info)
        assert f"/{obj_type.value}/" in path
        assert path.endswith("test_obj.sql")


# ---------------------------------------------------------------------------
# LocalWriter
# ---------------------------------------------------------------------------


def test_local_writer(tmp_output_dir: Path):
    """Write a SchemaObject and verify the file exists with correct content."""
    obj = _make_schema_object()
    db_info = _make_db_info()
    writer = LocalWriter(base_path=str(tmp_output_dir))

    result_path = writer.write(obj, db_info)

    written = Path(result_path)
    assert written.exists()
    assert written.read_text(encoding="utf-8") == obj.ddl
    assert written.name == "users.sql"


def test_local_writer_nested_dirs(tmp_output_dir: Path):
    """Parent directories are created automatically."""
    obj = _make_schema_object(database="deep_db", name="nested_table")
    db_info = _make_db_info()
    writer = LocalWriter(base_path=str(tmp_output_dir))

    result_path = writer.write(obj, db_info)

    written = Path(result_path)
    assert written.exists()
    # The parent tree should include the full hierarchy
    assert "deep_db" in str(written)
    assert "tables" in str(written)


# ---------------------------------------------------------------------------
# S3Writer
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.writers.s3.boto3")
def test_s3_writer(mock_boto3):
    """Verify put_object called with correct bucket, key, and body."""
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client

    obj = _make_schema_object()
    db_info = _make_db_info()
    writer = S3Writer(bucket="my-schema-bucket")

    result = writer.write(obj, db_info)

    mock_client.put_object.assert_called_once_with(
        Bucket="my-schema-bucket",
        Key="123456789012/us-east-1/prod-mysql-01/myapp/tables/users.sql",
        Body=obj.ddl.encode("utf-8"),
        ContentType="text/plain",
    )
    assert result == "s3://my-schema-bucket/123456789012/us-east-1/prod-mysql-01/myapp/tables/users.sql"


@patch("rds_schema_exporter.writers.s3.boto3")
def test_s3_writer_with_prefix(mock_boto3):
    """Verify prefix is prepended to the S3 key."""
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client

    obj = _make_schema_object()
    db_info = _make_db_info()
    writer = S3Writer(bucket="my-bucket", prefix="exports/v2/")

    result = writer.write(obj, db_info)

    expected_key = "exports/v2/123456789012/us-east-1/prod-mysql-01/myapp/tables/users.sql"
    mock_client.put_object.assert_called_once()
    actual_key = mock_client.put_object.call_args.kwargs["Key"]
    assert actual_key == expected_key
    assert result == f"s3://my-bucket/{expected_key}"


@patch("rds_schema_exporter.writers.s3.boto3")
def test_s3_writer_prefix_stripped(mock_boto3):
    """Leading/trailing slashes in prefix are normalised."""
    mock_client = MagicMock()
    mock_boto3.client.return_value = mock_client

    writer = S3Writer(bucket="b", prefix="/leading/trailing/")
    # The prefix should have slashes stripped
    assert writer.prefix == "leading/trailing"
