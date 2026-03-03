"""FastAPI application with SSE-powered schema export streaming."""

from __future__ import annotations

import asyncio
import json
import logging
import queue as thread_queue
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sse_starlette.sse import EventSourceResponse

from .config import Config
from .models import ExportEvent, ExportEventType
from .teleport import (
    check_cluster_login,
    find_tsh,
    get_clusters,
    login_to_cluster,
)
from .web_runner import run_all_instances_export, run_single_instance_export

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
TEMPLATES_DIR = PROJECT_ROOT / "templates"
STATIC_DIR = PROJECT_ROOT / "static"

app = FastAPI(title="RDS Schema Exporter")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Module-level state (single-user local tool)
_login_process = None
_logged_in_cluster: str = ""
_logged_in_username: str = ""
_active_tunnels: list = []


def _resolve_username(cluster: str = "") -> str:
    """Return the logged-in Teleport username for *cluster*."""
    global _logged_in_username, _logged_in_cluster
    if _logged_in_username and _logged_in_cluster == cluster:
        return _logged_in_username
    try:
        tsh = find_tsh()
        ok, username = check_cluster_login(tsh, cluster)
        if ok:
            _logged_in_username = username
            _logged_in_cluster = cluster
            return username
    except Exception:
        pass
    return ""


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    try:
        clusters = get_clusters(find_tsh())
    except Exception:
        clusters = []
    username = _resolve_username()
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "clusters": clusters,
            "username": username,
        },
    )


@app.post("/api/login", response_class=HTMLResponse)
async def api_login(request: Request):
    global _login_process
    form = await request.form()
    cluster = str(form.get("cluster", ""))
    if not cluster:
        return HTMLResponse('<div class="error">No cluster selected</div>')

    try:
        tsh = find_tsh()
        _login_process = login_to_cluster(tsh, cluster)
        return templates.TemplateResponse(
            "partials/login_status.html",
            {"request": request, "status": "pending", "cluster": cluster},
        )
    except Exception as e:
        return HTMLResponse(f'<div class="error">{e}</div>')


@app.get("/api/login-status", response_class=HTMLResponse)
async def api_login_status(request: Request):
    global _logged_in_username, _logged_in_cluster
    cluster = request.query_params.get("cluster", "")
    try:
        tsh = find_tsh()
        logged_in, username = check_cluster_login(tsh, cluster)
        if logged_in:
            _logged_in_username = username
            _logged_in_cluster = cluster
            return templates.TemplateResponse(
                "partials/login_status.html",
                {
                    "request": request,
                    "status": "success",
                    "username": username,
                    "cluster": cluster,
                },
            )
        else:
            return templates.TemplateResponse(
                "partials/login_status.html",
                {"request": request, "status": "pending", "cluster": cluster},
            )
    except Exception:
        return templates.TemplateResponse(
            "partials/login_status.html",
            {"request": request, "status": "pending", "cluster": cluster},
        )


@app.get("/api/instances")
async def api_instances(request: Request):
    cluster = request.query_params.get("cluster", "")
    if not cluster:
        return {"instances": []}

    try:
        from .teleport import list_mysql_databases

        tsh = find_tsh()
        databases = await asyncio.to_thread(list_mysql_databases, tsh, cluster)
        return {"instances": [d["name"] for d in databases]}
    except Exception as e:
        logger.exception("Failed to list instances")
        return {"instances": [], "error": str(e)}


@app.get("/api/databases")
async def api_databases(request: Request):
    """List MySQL databases on a specific instance (requires tunnel)."""
    cluster = request.query_params.get("cluster", "")
    instance = request.query_params.get("instance", "")
    if not cluster or not instance:
        return {"databases": []}

    db_user = _resolve_username(cluster)
    if not db_user:
        return {"databases": [], "error": f"Not logged in to {cluster}"}

    def _fetch():
        import pymysql
        from .teleport import list_mysql_databases, start_tunnel, stop_tunnel

        tsh = find_tsh()
        tunnel = start_tunnel(tsh, instance, db_user, cluster=cluster)
        try:
            conn = pymysql.connect(host=tunnel.host, port=tunnel.port, user=db_user)
            try:
                from .extraction.databases import list_databases
                return list_databases(conn)
            finally:
                conn.close()
        finally:
            stop_tunnel(tsh, tunnel)

    try:
        dbs = await asyncio.to_thread(_fetch)
        return {"databases": dbs}
    except Exception as e:
        logger.exception("Failed to list databases on %s", instance)
        return {"databases": [], "error": str(e)}


@app.get("/api/export")
async def api_export(request: Request):
    cluster = request.query_params.get("cluster", "")
    instance = request.query_params.get("instance", "")
    all_instances = request.query_params.get("all", "") == "true"
    output_target = request.query_params.get("output", "local")
    output_path = request.query_params.get("output_path", "")
    bucket = request.query_params.get("bucket", "")
    s3_prefix = request.query_params.get("s3_prefix", "")
    exclude_dbs = request.query_params.get("exclude_dbs", "")
    strip_auto_increment = request.query_params.get("strip_auto_increment", "true") == "true"
    db_mode = request.query_params.get("db_mode", "user")  # "user", "all", "specific"
    specific_dbs = request.query_params.get("databases", "")

    if not cluster:
        async def error_stream():
            yield {"event": "error", "data": "No cluster selected"}
        return EventSourceResponse(error_stream())

    db_user = _resolve_username(cluster)
    if not db_user:
        async def error_stream():
            yield {"event": "error", "data": f"Not logged in to {cluster}. Click Login first."}
        return EventSourceResponse(error_stream())

    if not all_instances and not instance:
        async def error_stream():
            yield {"event": "error", "data": "No instance selected"}
        return EventSourceResponse(error_stream())

    # Build config
    config = Config()
    config.output.target = output_target
    if output_path:
        config.output.local_path = output_path
    if bucket:
        config.output.s3_bucket = bucket
    if s3_prefix:
        config.output.s3_prefix = s3_prefix
    if exclude_dbs:
        config.filtering.exclude_databases = [
            db.strip() for db in exclude_dbs.split(",") if db.strip()
        ]
    config.behavior.strip_auto_increment = strip_auto_increment

    include_databases: list[str] | None = None
    if db_mode == "specific" and specific_dbs:
        include_databases = [db.strip() for db in specific_dbs.split(",") if db.strip()]
        if not include_databases:
            db_mode = "user"  # fallback if no valid names

    event_queue: thread_queue.Queue[ExportEvent] = thread_queue.Queue()

    def on_event(evt: ExportEvent) -> None:
        event_queue.put(evt)

    async def event_stream():
        loop = asyncio.get_event_loop()

        if all_instances:
            future = loop.run_in_executor(
                None,
                lambda: run_all_instances_export(
                    config, db_user, cluster, on_event,
                    db_mode=db_mode, include_databases=include_databases,
                ),
            )
        else:
            future = loop.run_in_executor(
                None,
                lambda: run_single_instance_export(
                    config, instance, db_user, cluster, on_event,
                    db_mode=db_mode, include_databases=include_databases,
                ),
            )

        try:
            while not future.done():
                # Drain all queued events
                while True:
                    try:
                        evt = event_queue.get_nowait()
                        yield _format_sse_event(evt)
                    except thread_queue.Empty:
                        break
                await asyncio.sleep(0.1)

            # Drain remaining events after future completes
            while True:
                try:
                    evt = event_queue.get_nowait()
                    yield _format_sse_event(evt)
                except thread_queue.Empty:
                    break

            # Ensure future exceptions are raised
            future.result()

        except asyncio.CancelledError:
            future.cancel()
            raise
        except Exception as e:
            logger.exception("SSE stream error")
            yield {"event": "error", "data": str(e)}

    return EventSourceResponse(event_stream())


@app.post("/api/shutdown")
async def api_shutdown():
    """Kill any active tunnels on page unload."""
    return {"ok": True}


def _format_sse_event(evt: ExportEvent) -> dict:
    """Convert an ExportEvent to an SSE dict for EventSourceResponse."""
    data = {
        "message": evt.message,
        "instance_name": evt.instance_name,
        "database_name": evt.database_name,
        "object_type": evt.object_type,
        "object_name": evt.object_name,
        "file_path": evt.file_path,
        "current_instance": evt.current_instance,
        "total_instances": evt.total_instances,
        "current_database": evt.current_database,
        "total_databases": evt.total_databases,
        "total_objects": evt.total_objects,
        "current_object_in_db": evt.current_object_in_db,
        "total_objects_in_db": evt.total_objects_in_db,
        "object_counts": evt.object_counts,
        "batch_objects": evt.batch_objects,
        "success": evt.success,
        "summary_html": evt.summary_html,
        "duration": evt.duration,
        "errors": evt.errors,
    }

    if evt.event_type == ExportEventType.DONE:
        data["summary_html"] = templates.get_template(
            "partials/summary.html"
        ).render(
            success=evt.success,
            total_objects=evt.total_objects,
            object_counts=evt.object_counts,
            duration=evt.duration,
            errors=evt.errors,
        )

    return {
        "event": evt.event_type.value,
        "data": json.dumps(data),
    }
