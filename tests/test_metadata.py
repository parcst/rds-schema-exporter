"""Tests for metadata detection."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rds_schema_exporter.metadata import detect_metadata, parse_rds_endpoint


# ---------------------------------------------------------------------------
# parse_rds_endpoint
# ---------------------------------------------------------------------------


def test_parse_rds_endpoint_valid():
    """A well-formed RDS endpoint yields (instance_id, region)."""
    instance_id, region = parse_rds_endpoint(
        "mydb.abc123xyz.us-east-1.rds.amazonaws.com"
    )

    assert instance_id == "mydb"
    assert region == "us-east-1"


def test_parse_rds_endpoint_valid_other_region():
    instance_id, region = parse_rds_endpoint(
        "prod-db.x9y8z7.eu-west-2.rds.amazonaws.com"
    )

    assert instance_id == "prod-db"
    assert region == "eu-west-2"


def test_parse_rds_endpoint_invalid():
    """Non-RDS hostnames return (None, None)."""
    instance_id, region = parse_rds_endpoint("some-random-hostname.local")

    assert instance_id is None
    assert region is None


def test_parse_rds_endpoint_invalid_localhost():
    instance_id, region = parse_rds_endpoint("localhost")

    assert instance_id is None
    assert region is None


# ---------------------------------------------------------------------------
# detect_metadata — overrides provided
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.metadata.detect_rds_endpoint", return_value=None)
def test_detect_metadata_with_overrides(_mock_endpoint, mock_connection):
    """When all three overrides are provided, auto-detection is not needed."""
    db_info = detect_metadata(
        mock_connection,
        override_account_id="111111111111",
        override_region="eu-west-1",
        override_instance_id="my-instance",
    )

    assert db_info.account_id == "111111111111"
    assert db_info.region == "eu-west-1"
    assert db_info.instance_id == "my-instance"


# ---------------------------------------------------------------------------
# detect_metadata — missing info raises ValueError
# ---------------------------------------------------------------------------


@patch("rds_schema_exporter.metadata.detect_account_id", return_value=None)
@patch("rds_schema_exporter.metadata.detect_rds_endpoint", return_value=None)
def test_detect_metadata_missing_account_raises(
    _mock_endpoint, _mock_account, mock_connection
):
    """When account ID cannot be detected and no override, raise ValueError."""
    with pytest.raises(ValueError, match="account ID"):
        detect_metadata(mock_connection)


@patch("rds_schema_exporter.metadata.detect_account_id", return_value="123456789012")
@patch("rds_schema_exporter.metadata.detect_rds_endpoint", return_value=None)
def test_detect_metadata_missing_region_raises(
    _mock_endpoint, _mock_account, mock_connection
):
    """When region cannot be detected and no override, raise ValueError."""
    with pytest.raises(ValueError, match="region"):
        detect_metadata(mock_connection)


@patch("rds_schema_exporter.metadata.detect_account_id", return_value="123456789012")
@patch(
    "rds_schema_exporter.metadata.detect_rds_endpoint",
    return_value="mydb.abc123.us-east-1.rds.amazonaws.com",
)
def test_detect_metadata_auto_detection_succeeds(
    _mock_endpoint, _mock_account, mock_connection
):
    """When auto-detection works, no overrides are needed."""
    db_info = detect_metadata(mock_connection)

    assert db_info.account_id == "123456789012"
    assert db_info.region == "us-east-1"
    assert db_info.instance_id == "mydb"


@patch("rds_schema_exporter.metadata.detect_account_id", return_value=None)
@patch("rds_schema_exporter.metadata.detect_rds_endpoint", return_value=None)
def test_detect_metadata_missing_raises(
    _mock_endpoint, _mock_account, mock_connection
):
    """No overrides and all auto-detection fails: should raise ValueError."""
    with pytest.raises(ValueError):
        detect_metadata(mock_connection)
