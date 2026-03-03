"""Slack webhook notifications."""

from __future__ import annotations

import json
import logging
import urllib.request

from rds_schema_exporter.models import RunReport

logger = logging.getLogger(__name__)


def build_slack_message(report: RunReport) -> dict:
    """Build a Slack message payload from a RunReport."""
    status_emoji = ":white_check_mark:" if report.success else ":x:"
    status_text = "SUCCESS" if report.success else "FAILED"

    object_lines = "\n".join(
        f"  {obj_type}: {count}"
        for obj_type, count in sorted(report.object_counts.items())
    )

    text = (
        f"{status_emoji} *RDS Schema Export — {status_text}*\n"
        f"*Account:* `{report.database_info.account_id}`\n"
        f"*Region:* `{report.database_info.region}`\n"
        f"*Instance:* `{report.database_info.instance_id}`\n"
        f"*Duration:* {report.duration_seconds:.1f}s\n"
        f"*Databases:* {len(report.databases_processed)}\n"
        f"*Total objects:* {report.total_objects}\n"
    )

    if object_lines:
        text += f"\n*Breakdown:*\n```\n{object_lines}\n```\n"

    if report.error:
        text += f"\n*Error:* ```{report.error}```\n"

    return {"text": text}


def send_slack_notification(webhook_url: str, report: RunReport) -> None:
    """Send a run report to Slack via webhook."""
    if not webhook_url:
        return

    payload = build_slack_message(report)
    data = json.dumps(payload).encode("utf-8")

    req = urllib.request.Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            logger.info("Slack notification sent (status %d)", resp.status)
    except Exception:
        logger.warning("Failed to send Slack notification", exc_info=True)
