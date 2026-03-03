# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (dev mode)
pip install -e ".[dev]"

# Run tests
.venv/bin/python -m pytest tests/ -v

# Run a single test file or test
.venv/bin/python -m pytest tests/test_teleport.py -v
.venv/bin/python -m pytest tests/test_teleport.py::TestInteractiveSelect::test_allow_all_returns_sentinel -v

# Run the CLI
.venv/bin/rds-schema-export run --output local
.venv/bin/rds-schema-export run --config config.yaml
```

## Architecture

**Pipeline** (`runner.py:run_export`): Connect to MySQL → detect AWS metadata → list databases → extract DDL for all object types → write via LocalWriter or S3Writer → report summary → optional Slack notification.

**Teleport fallback**: When direct MySQL connection is refused, the runner falls back to an interactive Teleport flow: select cluster → select RDS instance (or "All") → start tsh tunnel → run pipeline through tunnel → tear down. The "All" option loops through every MySQL instance on the cluster sequentially, aggregating results into a single report.

**`_run_pipeline` helper** (`runner.py`): Extracted inner pipeline (metadata → list schemas → extract → write) reused by both single-instance and batch-export paths.

**Config layering** (`config.py`): Dataclass-based config loaded from YAML, then overridden by CLI flags. Seven sections: connection, output, metadata, filtering, notifications, behavior, teleport.

**Writer path structure** (`writers/base.py:build_path`): Direct connect produces `{account_id}/{region}/{instance_id}/{database}/{object_type}/{name}.sql`. Teleport adds a `connection_name` level: `{cluster}/{connection_name}/{region}/{instance_id}/{database}/{object_type}/{name}.sql`.

**Extraction modules** (`extraction/`): Each extractor queries MySQL `INFORMATION_SCHEMA` and returns `SchemaObject` instances with DDL. `extract_all()` in `extraction/__init__.py` aggregates all extractors.

## Key Types

- `Config` — full configuration tree
- `DatabaseInfo` — account_id, connection_name, region, instance_id
- `SchemaObject` — database, object_type (ObjectType enum), name, ddl
- `RunReport` — aggregated result with counts, timing, success/error
- `TeleportTunnel` — running tsh proxy process with host/port

## Testing

Tests use `pytest` with `unittest.mock` for patching subprocess calls, pymysql connections, and boto3 clients. `moto` is used for S3/STS mocking. Shared fixtures are in `tests/conftest.py`.
