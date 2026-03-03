"""Auto-detect AWS metadata (account ID, region, instance ID) from the database connection."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING

import boto3

from rds_schema_exporter.models import DatabaseInfo

if TYPE_CHECKING:
    import pymysql

logger = logging.getLogger(__name__)

# Pattern: {instance-id}.{hash}.{region}.rds.amazonaws.com
RDS_ENDPOINT_RE = re.compile(
    r"^(?P<instance_id>[^.]+)\.[a-z0-9]+\.(?P<region>[a-z0-9-]+)\.rds\.amazonaws\.com$"
)


def detect_account_id() -> str | None:
    """Get AWS account ID via STS."""
    try:
        sts = boto3.client("sts")
        return sts.get_caller_identity()["Account"]
    except Exception:
        logger.debug("Failed to detect account ID via STS", exc_info=True)
        return None


def detect_rds_endpoint(conn: pymysql.Connection) -> str | None:
    """Try to get the RDS endpoint hostname from MySQL variables."""
    with conn.cursor() as cursor:
        # Try @@report_host first (some RDS instances set this)
        try:
            cursor.execute("SELECT @@report_host")
            row = cursor.fetchone()
            if row and row[0]:
                return row[0]
        except Exception:
            pass

        # Fall back to hostname variable
        try:
            cursor.execute("SHOW VARIABLES WHERE Variable_name = 'hostname'")
            row = cursor.fetchone()
            if row and row[1]:
                return row[1]
        except Exception:
            pass

    return None


def parse_rds_endpoint(endpoint: str) -> tuple[str | None, str | None]:
    """Parse instance_id and region from an RDS endpoint hostname.

    Returns (instance_id, region) or (None, None) if not an RDS endpoint.
    """
    match = RDS_ENDPOINT_RE.match(endpoint)
    if match:
        return match.group("instance_id"), match.group("region")
    return None, None


def detect_metadata(
    conn: pymysql.Connection,
    *,
    override_account_id: str = "",
    override_connection_name: str = "",
    override_region: str = "",
    override_instance_id: str = "",
) -> DatabaseInfo:
    """Auto-detect AWS metadata, using overrides where provided.

    Raises ValueError if any required field cannot be determined.
    """
    # Account ID
    account_id = override_account_id or detect_account_id()
    if not account_id:
        raise ValueError(
            "Could not auto-detect AWS account ID. "
            "Provide --account-id or ensure AWS credentials are configured."
        )

    # Region + Instance ID from RDS endpoint
    auto_instance_id = None
    auto_region = None

    endpoint = detect_rds_endpoint(conn)
    if endpoint:
        logger.info("Detected RDS endpoint: %s", endpoint)
        auto_instance_id, auto_region = parse_rds_endpoint(endpoint)

    region = override_region or auto_region
    instance_id = override_instance_id or auto_instance_id

    if not region:
        raise ValueError(
            "Could not auto-detect AWS region from RDS endpoint. "
            "Provide --region."
        )

    if not instance_id:
        raise ValueError(
            "Could not auto-detect RDS instance ID from endpoint. "
            "Provide --instance-id."
        )

    return DatabaseInfo(
        account_id=account_id,
        region=region,
        instance_id=instance_id,
        connection_name=override_connection_name,
    )
