"""Tests for Slack notification module."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from rds_schema_exporter.models import DatabaseInfo, RunReport
from rds_schema_exporter.notifications.slack import build_slack_message, send_slack_notification


def _make_report(
    success: bool = True,
    error: str | None = None,
) -> RunReport:
    """Create a sample RunReport for testing."""
    db_info = DatabaseInfo(
        account_id="123456789012",
        region="us-east-1",
        instance_id="prod-mysql-01",
    )
    return RunReport(
        database_info=db_info,
        started_at=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        finished_at=datetime(2025, 1, 1, 12, 1, 30, tzinfo=timezone.utc),
        databases_processed=["myapp", "analytics"],
        object_counts={"tables": 10, "views": 3, "indexes": 5},
        total_objects=18,
        success=success,
        error=error,
    )


# ---------------------------------------------------------------------------
# build_slack_message
# ---------------------------------------------------------------------------


def test_build_slack_message_success():
    """Successful report message includes account, region, instance, status."""
    report = _make_report(success=True)
    payload = build_slack_message(report)

    text = payload["text"]
    assert "SUCCESS" in text
    assert "123456789012" in text
    assert "us-east-1" in text
    assert "prod-mysql-01" in text
    assert "18" in text  # total objects
    assert "tables: 10" in text
    assert "views: 3" in text
    assert "indexes: 5" in text


def test_build_slack_message_failure():
    """Failed report message includes error details."""
    report = _make_report(success=False, error="Connection refused")
    payload = build_slack_message(report)

    text = payload["text"]
    assert "FAILED" in text
    assert "Connection refused" in text
    assert "123456789012" in text
    assert "us-east-1" in text
    assert "prod-mysql-01" in text


def test_build_slack_message_no_objects():
    """Message is valid even when there are no objects."""
    report = _make_report(success=True)
    report.object_counts = {}
    report.total_objects = 0
    payload = build_slack_message(report)

    text = payload["text"]
    assert "SUCCESS" in text
    assert "0" in text


# ---------------------------------------------------------------------------
# send_slack_notification
# ---------------------------------------------------------------------------


def test_send_slack_notification_empty_url():
    """Empty webhook URL should be a no-op (no exception raised)."""
    report = _make_report(success=True)
    # Should not raise any exception
    send_slack_notification("", report)


@patch("rds_schema_exporter.notifications.slack.urllib.request.urlopen")
@patch("rds_schema_exporter.notifications.slack.urllib.request.Request")
def test_send_slack_notification_calls_webhook(mock_request_cls, mock_urlopen):
    """When a webhook URL is provided, an HTTP request is made."""
    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)
    mock_urlopen.return_value = mock_response

    report = _make_report(success=True)
    send_slack_notification("https://hooks.slack.com/services/XXX", report)

    mock_request_cls.assert_called_once()
    call_kwargs = mock_request_cls.call_args
    assert call_kwargs.args[0] == "https://hooks.slack.com/services/XXX"
    mock_urlopen.assert_called_once()


@patch("rds_schema_exporter.notifications.slack.urllib.request.urlopen")
@patch("rds_schema_exporter.notifications.slack.urllib.request.Request")
def test_send_slack_notification_handles_error(mock_request_cls, mock_urlopen):
    """Network errors are caught and logged, not re-raised."""
    mock_urlopen.side_effect = Exception("Network error")

    report = _make_report(success=True)
    # Should not raise
    send_slack_notification("https://hooks.slack.com/services/XXX", report)
