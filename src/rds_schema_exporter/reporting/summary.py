"""Run summary formatting."""

from __future__ import annotations

from rds_schema_exporter.models import RunReport


def format_summary(report: RunReport) -> str:
    """Format a RunReport as a human-readable summary string."""
    db = report.database_info
    lines = [
        "=" * 60,
        "RDS Schema Export — Run Summary",
        "=" * 60,
        f"Status:      {'SUCCESS' if report.success else 'FAILED'}",
        f"Account:     {db.account_id if db else 'N/A'}",
        f"Region:      {db.region if db else 'N/A'}",
        f"Instance:    {db.instance_id if db else 'N/A'}",
        f"Duration:    {report.duration_seconds:.1f}s",
        f"Databases:   {len(report.databases_processed)}",
        f"Total objects exported: {report.total_objects}",
    ]

    if report.object_counts:
        lines.append("")
        lines.append("Object counts:")
        for obj_type, count in sorted(report.object_counts.items()):
            lines.append(f"  {obj_type}: {count}")

    if report.error:
        lines.append("")
        lines.append(f"Error: {report.error}")

    lines.append("=" * 60)
    return "\n".join(lines)
