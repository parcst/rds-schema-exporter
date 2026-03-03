"""Shared pytest fixtures for rds-schema-exporter tests."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from rds_schema_exporter.config import Config, ConnectionConfig, OutputConfig
from rds_schema_exporter.models import DatabaseInfo


@pytest.fixture()
def mock_connection():
    """Return a MagicMock pymysql connection with a mock cursor.

    The cursor supports the context-manager protocol so that
    ``with conn.cursor() as cur:`` works in production code.
    """
    conn = MagicMock()
    cursor = MagicMock()

    # Make the cursor usable as a context manager
    cursor.__enter__ = MagicMock(return_value=cursor)
    cursor.__exit__ = MagicMock(return_value=False)

    conn.cursor.return_value = cursor
    return conn


@pytest.fixture()
def sample_db_info() -> DatabaseInfo:
    """A representative DatabaseInfo for testing."""
    return DatabaseInfo(
        account_id="123456789012",
        region="us-east-1",
        instance_id="prod-mysql-01",
    )


@pytest.fixture()
def sample_config() -> Config:
    """A minimal Config with local output defaults."""
    return Config(
        connection=ConnectionConfig(
            host="localhost",
            port=3306,
            user="testuser",
            password="testpass",
        ),
        output=OutputConfig(
            target="local",
            local_path="./schemas",
        ),
    )


@pytest.fixture()
def tmp_output_dir(tmp_path: Path) -> Path:
    """Return a temporary directory suitable for writer output."""
    out = tmp_path / "output"
    out.mkdir()
    return out
