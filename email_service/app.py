import json
import logging
import threading
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import psycopg
from pydantic import BaseModel, Field
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response

if __package__:
    from .config import CHART_LOOKBACK_HOURS, EMAIL_FROM, REPORT_LOOKBACK_HOURS, SMTP_HOST, SMTP_PORT, SMTP_USER, YIELD_ALERT_THRESHOLD, validate_config
    from .db import create_system, get_conn, get_scheduler_state, get_system, get_system_recipients, get_system_report_profile, get_systems, save_scheduler_state
    from .main import load_system_context, send_report_for_system
else:
    from config import CHART_LOOKBACK_HOURS, EMAIL_FROM, REPORT_LOOKBACK_HOURS, SMTP_HOST, SMTP_PORT, SMTP_USER, YIELD_ALERT_THRESHOLD, validate_config
    from db import create_system, get_conn, get_scheduler_state, get_system, get_system_recipients, get_system_report_profile, get_systems, save_scheduler_state
    from main import load_system_context, send_report_for_system

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Email Reporting Hub", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://localhost:5175",
        "http://127.0.0.1:5175",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

_run_locks: dict[str, threading.Lock] = defaultdict(threading.Lock)
_scheduler_threads: dict[str, threading.Thread] = {}
_scheduler_stop_events: dict[str, threading.Event] = {}
_max_run_history = 500


class CreateSystemRequest(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    code: str = Field(min_length=1, max_length=60)
    timezone: str = Field(default="UTC", min_length=1, max_length=100)
    status: str = Field(default="inactive", pattern="^(active|inactive)$")
    description: str | None = Field(default=None, max_length=500)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _is_scheduler_running(system_id: str) -> bool:
    thread = _scheduler_threads.get(system_id)
    return bool(thread and thread.is_alive())


def _system_health_summary(system_id: str) -> dict:
    system = get_system(system_id)
    if not system:
        raise HTTPException(status_code=404, detail="System not found")

    scheduler_state = get_scheduler_state(system_id)
    recipients = get_system_recipients(system_id)
    report_profile = get_system_report_profile(system_id)
    last_run = _latest_run_log(system_id)

    config_errors = []
    try:
        validate_config()
    except Exception as exc:
        config_errors.append(str(exc))

    if not recipients:
        config_errors.append("No active recipients configured")
    if not report_profile:
        config_errors.append("No report profile configured")

    return {
        "system_id": system_id,
        "status": "ok" if not config_errors else "degraded",
        "config_ok": len(config_errors) == 0,
        "config_errors": config_errors,
        "scheduler_running": _is_scheduler_running(system_id) or bool(scheduler_state["running"]),
        "scheduler_interval_minutes": scheduler_state["interval_minutes"],
        "last_run": {
            "status": last_run["status"] if last_run else "never",
            "started_at": last_run["started_at"].isoformat() if last_run and last_run.get("started_at") else None,
            "finished_at": last_run["finished_at"].isoformat() if last_run and last_run.get("finished_at") else None,
            "error": last_run["error"] if last_run else None,
        },
    }


def _latest_run_log(system_id: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, started_at, finished_at, status, recipient, subject, template_used, device_count, error
                FROM public.run_logs
                WHERE system_id = %s
                ORDER BY id DESC
                LIMIT 1
                """,
                (system_id,),
            )
            return cur.fetchone()


def _record_run_history(system_id: str, payload: dict | None, *, status: str, started_at: datetime, finished_at: datetime, error: str | None, trigger_type: str) -> int | None:
    payload = payload or {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.run_logs (
                    system_id,
                    started_at,
                    finished_at,
                    status,
                    trigger_type,
                    recipient,
                    subject,
                    template_used,
                    device_count,
                    error
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
                """,
                (
                    system_id,
                    started_at,
                    finished_at,
                    status,
                    trigger_type,
                    payload.get("recipient"),
                    payload.get("subject"),
                    payload.get("template_used"),
                    len(payload.get("rows", [])) if payload.get("rows") is not None else None,
                    error,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return int(row["id"]) if row and row.get("id") is not None else None


def _record_generated_report(system_id: str, run_log_id: int | None, payload: dict | None, *, status: str, error: str | None) -> None:
    payload = payload or {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.generated_reports (
                    system_id,
                    run_log_id,
                    report_period_start,
                    report_period_end,
                    status,
                    recipient,
                    subject,
                    template_used,
                    report_generated_at,
                    report_hours,
                    report_totals_json,
                    report_rows_json,
                    html_body,
                    text_body,
                    pdf_filename,
                    pdf_bytes,
                    delivery_status,
                    error
                )
                VALUES (%s, %s, NOW() - (%s || ' hours')::interval, NOW(), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    system_id,
                    run_log_id,
                    payload.get("hours") or 0,
                    status,
                    payload.get("recipient"),
                    payload.get("subject"),
                    payload.get("template_used"),
                    payload.get("generated_at"),
                    payload.get("hours"),
                    json.dumps(payload.get("totals")) if payload.get("totals") is not None else None,
                    json.dumps(payload.get("rows")) if payload.get("rows") is not None else None,
                    payload.get("html"),
                    payload.get("text"),
                    payload.get("pdf_filename"),
                    payload.get("pdf_bytes"),
                    "sent" if status == "success" else "failed",
                    error,
                ),
            )
            conn.commit()


def _execute_report(system_id: str, trigger_type: str = "manual") -> dict:
    lock = _run_locks[system_id]
    if not lock.acquire(blocking=False):
        raise RuntimeError("A report run is already in progress for this system")

    started_at = _utc_now()
    scheduler_state = get_scheduler_state(system_id)
    interval_minutes = int(scheduler_state["interval_minutes"] or 60)
    save_scheduler_state(
        system_id,
        enabled=bool(scheduler_state["enabled"]),
        interval_minutes=interval_minutes,
        running=True,
        locked_at=started_at,
        lock_owner="api",
        last_error=None,
    )

    payload = None
    status = "failed"
    error = None
    try:
        payload = send_report_for_system(system_id, trigger_type=trigger_type)
        status = "success"
        return payload
    except Exception as exc:
        error = str(exc)
        raise
    finally:
        finished_at = _utc_now()
        run_log_id = _record_run_history(
            system_id,
            payload,
            status=status,
            started_at=started_at,
            finished_at=finished_at,
            error=error,
            trigger_type=trigger_type,
        )
        if payload or status == "failed":
            _record_generated_report(system_id, run_log_id, payload, status=status, error=error)
        save_scheduler_state(
            system_id,
            enabled=bool(scheduler_state["enabled"]),
            interval_minutes=interval_minutes,
            running=False,
            last_run_at=finished_at,
            next_run_at=finished_at + timedelta(minutes=interval_minutes) if scheduler_state["enabled"] else None,
            last_status=status,
            last_error=error,
            locked_at=None,
            lock_owner=None,
        )
        lock.release()


def _public_report_payload(payload: dict) -> dict:
    public_payload = dict(payload)
    pdf_bytes = public_payload.pop("pdf_bytes", None)
    public_payload["pdf_available"] = pdf_bytes is not None
    return public_payload


def _start_scheduler_thread(system_id: str, interval_minutes: int) -> None:
    if _is_scheduler_running(system_id):
        return

    stop_event = threading.Event()
    thread = threading.Thread(
        target=_scheduler_loop,
        args=(system_id, interval_minutes, stop_event),
        daemon=True,
        name=f"scheduler-{system_id}",
    )
    _scheduler_stop_events[system_id] = stop_event
    _scheduler_threads[system_id] = thread
    thread.start()


def _scheduler_loop(system_id: str, interval_minutes: int, stop_event: threading.Event) -> None:
    logger.info("Scheduler started for %s: interval=%sm", system_id, interval_minutes)
    while not stop_event.is_set():
        scheduler_state = get_scheduler_state(system_id)
        next_run_at = scheduler_state.get("next_run_at")
        now = _utc_now()
        if next_run_at is None:
            next_run_at = now + timedelta(minutes=interval_minutes)
            save_scheduler_state(
                system_id,
                enabled=True,
                interval_minutes=interval_minutes,
                running=False,
                next_run_at=next_run_at,
                last_error=scheduler_state.get("last_error"),
            )

        wait_seconds = max(0, (next_run_at - now).total_seconds())
        if wait_seconds > 0 and stop_event.wait(wait_seconds):
            break

        try:
            _execute_report(system_id, trigger_type="scheduler")
        except Exception:
            logger.exception("Scheduler run failed for %s", system_id)

    logger.info("Scheduler stopped for %s", system_id)


@app.get("/")
def root():
    return {
        "service": "email-reporting-hub",
        "status": "ok",
        "docs": "/docs",
        "systems": "/systems",
    }


@app.on_event("startup")
def startup_resume_enabled_schedulers() -> None:
    for system in get_systems():
        system_id = str(system["id"])
        scheduler_state = get_scheduler_state(system_id)
        if scheduler_state.get("enabled"):
            interval_minutes = int(scheduler_state.get("interval_minutes") or 60)
            _start_scheduler_thread(system_id, interval_minutes)


@app.get("/systems")
def systems_list() -> list[dict]:
    items = []
    for system in get_systems():
        scheduler_state = get_scheduler_state(str(system["id"]))
        last_run = _latest_run_log(str(system["id"]))
        items.append(
            {
                "id": str(system["id"]),
                "name": system["name"],
                "code": system["code"],
                "timezone": system["timezone"],
                "status": system["status"],
                "connectionStatus": "connected",
                "dataSource": {"host": "", "database": "", "schema": "public", "username": ""},
                "lastRun": last_run["finished_at"].isoformat() if last_run and last_run.get("finished_at") else None,
                "nextRun": scheduler_state["next_run_at"].isoformat() if scheduler_state.get("next_run_at") else None,
                "schedulerState": "running" if _is_scheduler_running(str(system["id"])) or scheduler_state["running"] or scheduler_state["enabled"] else ("stopped" if not scheduler_state["last_error"] else "error"),
                "createdAt": system["created_at"].isoformat() if system.get("created_at") else None,
            }
        )
    return items


@app.post("/systems", status_code=201)
def create_system_route(payload: CreateSystemRequest) -> dict:
    normalized_code = payload.code.strip().upper()
    normalized_name = payload.name.strip()
    normalized_timezone = payload.timezone.strip()
    normalized_description = payload.description.strip() if payload.description else None

    if not normalized_code:
        raise HTTPException(status_code=400, detail="System code is required")
    if not normalized_name:
        raise HTTPException(status_code=400, detail="System name is required")
    if not normalized_timezone:
        raise HTTPException(status_code=400, detail="Timezone is required")

    try:
        system = create_system(
            code=normalized_code,
            name=normalized_name,
            timezone=normalized_timezone,
            status=payload.status,
            description=normalized_description,
        )
    except psycopg.errors.UniqueViolation as exc:
        raise HTTPException(status_code=409, detail="A system with this code already exists") from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to create system: {exc}") from exc

    return {
        "id": str(system["id"]),
        "name": system["name"],
        "code": system["code"],
        "timezone": system["timezone"],
        "status": system["status"],
        "connectionStatus": "connected",
        "dataSource": {"host": "", "database": "", "schema": "public", "username": ""},
        "lastRun": None,
        "nextRun": None,
        "schedulerState": "stopped",
        "createdAt": system["created_at"].isoformat() if system.get("created_at") else None,
    }


@app.get("/systems/{system_id}")
def get_system_details(system_id: str) -> dict:
    system = get_system(system_id)
    if not system:
        raise HTTPException(status_code=404, detail="System not found")
    scheduler_state = get_scheduler_state(system_id)
    return {
        "system": system,
        "scheduler": scheduler_state,
        "recipients": get_system_recipients(system_id),
        "report_profile": get_system_report_profile(system_id),
    }


@app.get("/systems/{system_id}/health")
def system_health(system_id: str) -> dict:
    return _system_health_summary(system_id)


@app.get("/systems/{system_id}/config")
def system_config(system_id: str) -> dict:
    system = get_system(system_id)
    if not system:
        raise HTTPException(status_code=404, detail="System not found")
    return {
        "system": system,
        "email": {
            "smtp_host": SMTP_HOST,
            "smtp_port": SMTP_PORT,
            "smtp_user": SMTP_USER,
            "from_email": EMAIL_FROM,
            "to_emails": get_system_recipients(system_id),
        },
        "report": get_system_report_profile(system_id),
        "scheduler": get_scheduler_state(system_id),
        "defaults": {
            "lookback_hours": REPORT_LOOKBACK_HOURS,
            "chart_lookback_hours": CHART_LOOKBACK_HOURS,
            "yield_alert_threshold": YIELD_ALERT_THRESHOLD,
        },
    }


@app.get("/systems/{system_id}/logs")
def system_logs(system_id: str, limit: int = 100) -> dict:
    if not get_system(system_id):
        raise HTTPException(status_code=404, detail="System not found")

    safe_limit = max(1, min(limit, _max_run_history))
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    l.id,
                    l.started_at,
                    l.finished_at,
                    l.status,
                    l.recipient,
                    l.subject,
                    l.template_used,
                    l.device_count,
                    l.error,
                    gr.id AS generated_report_id
                FROM public.run_logs l
                LEFT JOIN public.generated_reports gr
                  ON gr.run_log_id = l.id
                WHERE l.system_id = %s
                ORDER BY l.id DESC
                LIMIT %s
                """,
                (system_id, safe_limit),
            )
            rows = cur.fetchall()

    items = [
        {
            "id": str(row["id"]),
            "timestamp": (row["finished_at"] or row["started_at"]).isoformat() if (row.get("finished_at") or row.get("started_at")) else None,
            "started_at": row["started_at"].isoformat() if row.get("started_at") else None,
            "finished_at": row["finished_at"].isoformat() if row.get("finished_at") else None,
            "status": row["status"],
            "recipient": row["recipient"],
            "subject": row["subject"],
            "template_used": row["template_used"],
            "device_count": row["device_count"],
            "error": row["error"],
            "generated_report_id": str(row["generated_report_id"]) if row.get("generated_report_id") is not None else None,
        }
        for row in rows
    ]

    return {
        "logs": items,
        "summary": {
            "total": len(items),
            "success": sum(1 for item in items if item["status"] == "success"),
            "failed": sum(1 for item in items if item["status"] == "failed"),
            "pending": sum(1 for item in items if item["status"] in {"pending", "running"}),
        },
    }


@app.post("/systems/{system_id}/run-report")
def run_report_once(system_id: str) -> dict:
    if not get_system(system_id):
        raise HTTPException(status_code=404, detail="System not found")
    try:
        payload = _execute_report(system_id, trigger_type="manual")
        return {"message": "Report email sent", "report": _public_report_payload(payload)}
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Report run failed: {exc}") from exc


@app.get("/systems/{system_id}/reports/{report_id}/pdf")
def download_generated_report_pdf(system_id: str, report_id: int) -> Response:
    if not get_system(system_id):
        raise HTTPException(status_code=404, detail="System not found")
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT pdf_filename, pdf_bytes
                FROM public.generated_reports
                WHERE id = %s
                  AND system_id = %s
                """,
                (report_id, system_id),
            )
            row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Generated report not found")
    if row.get("pdf_bytes") is None:
        raise HTTPException(status_code=404, detail="PDF not available for this generated report")

    pdf_bytes = bytes(row["pdf_bytes"])
    filename = row.get("pdf_filename") or f"generated_report_{report_id}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.post("/systems/{system_id}/scheduler/start")
def start_scheduler(system_id: str, interval_minutes: int = 60) -> dict:
    if not get_system(system_id):
        raise HTTPException(status_code=404, detail="System not found")
    if _is_scheduler_running(system_id):
        state = get_scheduler_state(system_id)
        return {
            "message": "Scheduler already running",
            "scheduler_running": True,
            "scheduler_interval_minutes": state["interval_minutes"],
        }

    save_scheduler_state(
        system_id,
        enabled=True,
        interval_minutes=interval_minutes,
        running=False,
        next_run_at=_utc_now() + timedelta(minutes=interval_minutes),
        last_error=None,
    )
    _start_scheduler_thread(system_id, interval_minutes)
    return {
        "message": "Scheduler started",
        "scheduler_running": True,
        "scheduler_interval_minutes": interval_minutes,
    }


@app.post("/systems/{system_id}/scheduler/stop")
def stop_scheduler(system_id: str) -> dict:
    if not get_system(system_id):
        raise HTTPException(status_code=404, detail="System not found")
    if not _is_scheduler_running(system_id):
        save_scheduler_state(
            system_id,
            enabled=False,
            interval_minutes=get_scheduler_state(system_id)["interval_minutes"],
            running=False,
            next_run_at=None,
        )
        return {"message": "Scheduler already stopped", "scheduler_running": False}

    stop_event = _scheduler_stop_events.get(system_id)
    if stop_event is not None:
        stop_event.set()

    save_scheduler_state(
        system_id,
        enabled=False,
        interval_minutes=get_scheduler_state(system_id)["interval_minutes"],
        running=False,
        next_run_at=None,
    )
    return {"message": "Scheduler stop requested", "scheduler_running": False}
