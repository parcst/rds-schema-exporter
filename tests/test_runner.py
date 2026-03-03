"""Tests for the runner orchestrator."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rds_schema_exporter.config import Config, ConnectionConfig, MetadataConfig, OutputConfig
from rds_schema_exporter.models import DatabaseInfo, ObjectType, SchemaObject
from rds_schema_exporter.runner import run_export


def _make_config(tmp_path) -> Config:
    """Build a Config pointing to a temporary local directory."""
    out = tmp_path / "schemas"
    out.mkdir(exist_ok=True)
    return Config(
        connection=ConnectionConfig(
            host="localhost",
            port=3306,
            user="testuser",
            password="testpass",
        ),
        output=OutputConfig(
            target="local",
            local_path=str(out),
        ),
        metadata=MetadataConfig(
            account_id="123456789012",
            region="us-east-1",
            instance_id="prod-mysql-01",
        ),
    )


# ---------------------------------------------------------------------------
# Successful export
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.runner.send_slack_notification")
@patch("rds_schema_exporter.runner.extract_all")
@patch("rds_schema_exporter.runner.list_databases")
@patch("rds_schema_exporter.runner.detect_metadata")
@patch("rds_schema_exporter.runner.pymysql")
def test_run_export_success(
    mock_pymysql,
    mock_detect_metadata,
    mock_list_databases,
    mock_extract_all,
    mock_slack,
    tmp_path,
):
    """Full happy path: connect, detect metadata, extract, write, report."""
    # Setup mocks
    mock_conn = MagicMock()
    mock_pymysql.connect.return_value = mock_conn

    db_info = DatabaseInfo(
        account_id="123456789012",
        region="us-east-1",
        instance_id="prod-mysql-01",
    )
    mock_detect_metadata.return_value = db_info
    mock_list_databases.return_value = ["myapp"]

    mock_extract_all.return_value = [
        SchemaObject(
            database="myapp",
            object_type=ObjectType.TABLE,
            name="users",
            ddl="CREATE TABLE `users` (id int);\n",
        ),
        SchemaObject(
            database="myapp",
            object_type=ObjectType.VIEW,
            name="active_users",
            ddl="CREATE VIEW `active_users` AS SELECT * FROM users;\n",
        ),
    ]

    config = _make_config(tmp_path)
    report = run_export(config)

    # Verify report fields
    assert report.success is True
    assert report.error is None
    assert report.databases_processed == ["myapp"]
    assert report.total_objects == 2
    assert report.object_counts["tables"] == 1
    assert report.object_counts["views"] == 1
    assert report.database_info == db_info
    assert report.finished_at is not None
    assert report.duration_seconds >= 0.0

    # Connection was opened and closed
    mock_pymysql.connect.assert_called_once()
    mock_conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# Connection failure
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.runner.pymysql")
def test_run_export_connection_failure(mock_pymysql, tmp_path):
    """When pymysql.connect raises, run_export should raise RuntimeError."""
    mock_pymysql.connect.side_effect = Exception("Connection refused")

    config = _make_config(tmp_path)

    with pytest.raises(RuntimeError, match="Schema export failed"):
        run_export(config)


# ---------------------------------------------------------------------------
# Metadata detection failure
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.runner.detect_metadata")
@patch("rds_schema_exporter.runner.pymysql")
def test_run_export_metadata_failure(mock_pymysql, mock_detect_metadata, tmp_path):
    """When detect_metadata raises ValueError, export fails with RuntimeError."""
    mock_conn = MagicMock()
    mock_pymysql.connect.return_value = mock_conn
    mock_detect_metadata.side_effect = ValueError("Could not detect region")

    config = _make_config(tmp_path)

    with pytest.raises(RuntimeError, match="Schema export failed"):
        run_export(config)

    mock_conn.close.assert_called_once()
