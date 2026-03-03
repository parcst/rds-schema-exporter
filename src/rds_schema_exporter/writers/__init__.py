"""Output writers."""

from .local import LocalWriter
from .s3 import S3Writer

__all__ = ["LocalWriter", "S3Writer"]
