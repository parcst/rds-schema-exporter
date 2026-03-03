# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install (dev mode)
pip install -e ".[dev]"

# Install with web UI
pip install -e ".[web,dev]"

# Run tests
.venv/bin/python -m pytest tests/ -v

# Run a single test file or test
.venv/bin/python -m pytest tests/test_teleport.py -v
.venv/bin/python -m pytest tests/test_teleport.py::TestInteractiveSelect::test_allow_all_returns_sentinel -v

# Run the CLI
.venv/bin/rds-schema-export run --output local
.venv/bin/rds-schema-export run --config config.yaml

# Start the web UI (http://127.0.0.1:8002)
.venv/bin/rds-schema-export web
.venv/bin/rds-schema-export web --port 8002 --reload
```

## Architecture

### CLI Pipeline

**Pipeline** (`runner.py:run_export`): Connect to MySQL → detect AWS metadata → list databases → extract DDL for all object types → write via LocalWriter or S3Writer → report summary → optional Slack notification.

**Teleport fallback**: When direct MySQL connection is refused, the runner falls back to an interactive Teleport flow: select cluster → select RDS instance (or "All") → start tsh tunnel → run pipeline through tunnel → tear down. The "All" option loops through every MySQL instance on the cluster sequentially, aggregating results into a single report.

### Web UI

**Tech stack**: FastAPI, Jinja2 templates, HTMX (CDN), vanilla JS for SSE, sse-starlette. Dark theme matching PII Sentinel (gray-950 background, red accent).

**Key principle**: `runner.py` is untouched. `web_runner.py` reuses the same building blocks (extractors, writers, metadata) but yields `ExportEvent` callbacks via `on_event()` instead of logging.

**Layout**: Sidebar (320px) + main area. Sidebar has cluster/login, instance selector, output config, options, export button. Main area shows live progress, results tree, and summary.

#### Web Routes

| Method | Path | Purpose |
|--------|------|---------|
| GET | `/` | Main page with cluster dropdown pre-loaded |
| POST | `/api/login` | Trigger SSO login, returns HTMX login_status partial |
| GET | `/api/login-status?cluster=X` | HTMX polled every 2s, returns login_status partial |
| GET | `/api/instances?cluster=X` | JSON list of MySQL instances on cluster |
| GET | `/api/databases?cluster&instance` | JSON list of databases on an instance (opens tunnel) |
| GET | `/api/browse-dirs?path=X` | JSON list of subdirectories for directory picker |
| POST | `/api/create-dir` | Create a new directory (directory picker) |
| GET | `/api/export?cluster&instance&...` | SSE stream of export events |
| POST | `/api/shutdown` | Tunnel cleanup on page unload (sendBeacon) |

#### SSE Event Protocol

| Event | Key Data | UI Update |
|-------|----------|-----------|
| `step` | `message` | Update spinner text |
| `instance_start` | `instance_name`, `current/total` | Add instance node to tree |
| `database_start` | `database_name`, `current/total` | Add database sub-node |
| `object_extracted` | `object_type`, `batch_objects[]`, counts | Add type batch to tree, update progress (1 event per type per database) |
| `database_done` | `database_name` | Mark database complete |
| `instance_done` | `instance_name`, `object_counts` | Mark instance complete |
| `instance_error` | `instance_name`, `message` | Show error badge |
| `done` | `success`, `summary_html`, `duration` | Render summary bar |
| `error` | `message` | Show error banner |

#### Key Web Files

- `app.py` — FastAPI app, routes, SSE event stream. Module-level state for login process/username (single-user local tool). Runs export in thread executor, polls event queue every 100ms.
- `web_runner.py` — Streaming export runner with two entry points: `run_single_instance_export()` and `run_all_instances_export()`. Both blocking, designed for `run_in_executor`. Call `on_event(ExportEvent)` callback at each step. Inner `_run_streaming_pipeline()` uses single-pass-per-type (extract+write each type, emit one batch event) for real-time progress — 7 updates per database with natural SQL pauses between them.
- `templates/base.html` — Dark header with "SE" brand icon.
- `templates/index.html` — Sidebar + main area with JS SSE handling (~200 lines). Tree view for results.
- `templates/partials/login_status.html` — HTMX-polled login badge (pending/success/error states).
- `templates/partials/summary.html` — Final export summary bar with stats.
- `static/style.css` — PII Sentinel dark theme CSS variables, sidebar, forms, tree view, progress bar, summary bar.

### Shared Architecture

**Teleport implementation notes** (`teleport.py`):
- `tsh status` returns exit code 1 even when logged in — never use `check=True` with it
- Uses `--proxy=<cluster>` (not `--cluster=`) for `tsh db ls`, `tsh db login`, and `tsh proxy db` — required for non-active profiles
- `get_logged_in_user()` checks both `active` and `profiles[]` in `tsh status` JSON for cluster-aware login lookup
- Port allocation uses `--port 0` (OS picks free port), parsed from stdout via regex `127\.0\.0\.1:(\d+)`
- SSO email from `tsh status` is used as `--db-user` automatically
- `login_to_cluster()` spawns `tsh login <cluster>` for web SSO flow
- `check_cluster_login()` validates login status for a specific cluster

**`_run_pipeline` helper** (`runner.py`): Extracted inner pipeline (metadata → list schemas → extract → write) reused by both single-instance and batch-export paths.

**Config layering** (`config.py`): Dataclass-based config loaded from YAML, then overridden by CLI flags. Seven sections: connection, output, metadata, filtering, notifications, behavior, teleport.

**Writer path structure** (`writers/base.py:build_path`): Direct connect produces `{account_id}/{region}/{instance_id}/{database}/{object_type}/{name}.sql`. Teleport adds a `connection_name` level: `{cluster}/{connection_name}/{region}/{instance_id}/{database}/{object_type}/{name}.sql`.

**Extraction modules** (`extraction/`): Each extractor queries MySQL `INFORMATION_SCHEMA` and returns `SchemaObject` instances with DDL. `extract_all()` in `extraction/__init__.py` aggregates all extractors.

## Key Types

- `Config` — full configuration tree
- `DatabaseInfo` — account_id, connection_name, region, instance_id
- `SchemaObject` — database, object_type (ObjectType enum), name, ddl
- `RunReport` — aggregated result with counts, timing, success/error (CLI only)
- `ExportEvent` — streaming event with type, message, counters, object info (web only)
- `ExportEventType` — enum: step, instance_start, database_start, object_extracted, database_done, instance_done, instance_error, done, error
- `TeleportTunnel` — running tsh proxy process with host/port

## Testing

Tests use `pytest` with `unittest.mock` for patching subprocess calls, pymysql connections, and boto3 clients. `moto` is used for S3/STS mocking. Shared fixtures are in `tests/conftest.py`.
