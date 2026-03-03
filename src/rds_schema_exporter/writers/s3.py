"""S3 writer."""

from __future__ import annotations

import boto3

from rds_schema_exporter.models import DatabaseInfo, SchemaObject

from .base import Writer


class S3Writer(Writer):
    """Write schema objects to an S3 bucket."""

    def __init__(self, bucket: str, prefix: str = "") -> None:
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self.s3 = boto3.client("s3")

    def write(self, obj: SchemaObject, db_info: DatabaseInfo) -> str:
        relative = self.build_path(obj, db_info)
        key = f"{self.prefix}/{relative}" if self.prefix else relative

        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=obj.ddl.encode("utf-8"),
            ContentType="text/plain",
        )

        return f"s3://{self.bucket}/{key}"
