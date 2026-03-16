import logging
import threading
import json
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import Response
from fastapi.middleware.cors import CORSMiddleware

if __package__:
    from .config import validate_config
else:
    from config import validate_config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(title="Email Service App", version="1.0.0")

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

_run_lock = threading.Lock()
_last_run = {
    "status": "never",
    "started_at": None,
    "finished_at": None,
    "error": None,
}
_scheduler_thread = None
_scheduler_stop = None
_scheduler_interval_minutes = 60
_max_run_history = 500


def _is_scheduler_running() -> bool:
    return bool(_scheduler_thread and _scheduler_thread.is_alive())


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ensure_logs_table() -> None:
    if __package__:
        from .db import get_conn
    else:
        from db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS email_run_logs (
                    id BIGSERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL,
                    status TEXT NOT NULL,
                    recipient TEXT,
                    subject TEXT,
                    template_used TEXT,
                    device_count INTEGER,
                    error TEXT
                )
                """
            )
            conn.commit()


def _ensure_generated_reports_table() -> None:
    if __package__:
        from .db import get_conn
    else:
        from db import get_conn

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS email_generated_reports (
                    id BIGSERIAL PRIMARY KEY,
                    run_log_id BIGINT REFERENCES email_run_logs(id) ON DELETE SET NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    run_finished_at TIMESTAMPTZ,
                    status TEXT NOT NULL,
                    recipient TEXT,
                    subject TEXT,
                    template_used TEXT,
                    report_generated_at TEXT,
                    report_hours INTEGER,
                    report_totals_json TEXT,
                    report_rows_json TEXT,
                    html_body TEXT,
                    text_body TEXT,
                    pdf_filename TEXT,
                    pdf_bytes BYTEA,
                    error TEXT
                )
                """
            )
            # Keep old installations compatible: add missing columns when table already exists.
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS run_log_id BIGINT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS run_finished_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS status TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS recipient TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS subject TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS template_used TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS report_generated_at TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS report_hours INTEGER")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS report_totals_json TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS report_rows_json TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS html_body TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS text_body TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS pdf_filename TEXT")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS pdf_bytes BYTEA")
            cur.execute("ALTER TABLE email_generated_reports ADD COLUMN IF NOT EXISTS error TEXT")
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                        FROM pg_constraint
                        WHERE conname = 'email_generated_reports_run_log_id_fkey'
                    ) THEN
                        ALTER TABLE email_generated_reports
                        ADD CONSTRAINT email_generated_reports_run_log_id_fkey
                        FOREIGN KEY (run_log_id)
                        REFERENCES email_run_logs(id)
                        ON DELETE SET NULL;
                    END IF;
                END
                $$;
                """
            )
            conn.commit()


def _record_run_history() -> int | None:
    if __package__:
        from .config import EMAIL_TO
        from .db import get_conn
    else:
        from config import EMAIL_TO
        from db import get_conn

    timestamp_raw = _last_run["finished_at"] or _last_run["started_at"] or _utc_now()
    timestamp = datetime.fromisoformat(timestamp_raw)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO email_run_logs
                        (timestamp, status, recipient, subject, template_used, device_count, error)
                    VALUES
                        (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        timestamp,
                        _last_run["status"],
                        ", ".join(EMAIL_TO),
                        "Hourly IoT Device Report",
                        "IoT Summary Report",
                        None,
                        _last_run["error"],
                    ),
                )
                row = cur.fetchone()
                conn.commit()
                return int(row["id"]) if row and row.get("id") is not None else None
    except Exception:
        logger.exception("Failed to persist run history in database")
        return None


def _record_generated_report(report_payload: dict | None, run_log_id: int | None) -> None:
    if __package__:
        from .config import EMAIL_TO
        from .db import get_conn
    else:
        from config import EMAIL_TO
        from db import get_conn

    recipient = ", ".join(EMAIL_TO)
    report_payload = report_payload or {}
    run_finished_raw = _last_run["finished_at"] or _utc_now()
    run_finished_at = datetime.fromisoformat(run_finished_raw)

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO email_generated_reports (
                        run_log_id,
                        run_finished_at,
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
                        error
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        run_log_id,
                        run_finished_at,
                        _last_run["status"],
                        recipient,
                        report_payload.get("subject") or "Hourly IoT Device Report",
                        report_payload.get("template_used") or "IoT Summary Report",
                        report_payload.get("generated_at"),
                        report_payload.get("hours"),
                        json.dumps(report_payload.get("totals")) if report_payload.get("totals") is not None else None,
                        json.dumps(report_payload.get("rows")) if report_payload.get("rows") is not None else None,
                        report_payload.get("html"),
                        report_payload.get("text"),
                        report_payload.get("pdf_filename"),
                        report_payload.get("pdf_bytes"),
                        _last_run["error"],
                    ),
                )
                conn.commit()
    except Exception:
        logger.exception("Failed to persist generated report in database")


def _send_report_once() -> None:
    if __package__:
        from .main import send_report
    else:
        from main import send_report

    if not _run_lock.acquire(blocking=False):
        raise RuntimeError("A report run is already in progress")

    report_payload = None

    _last_run["status"] = "running"
    _last_run["started_at"] = _utc_now()
    _last_run["finished_at"] = None
    _last_run["error"] = None

    try:
        report_payload = send_report()
        _last_run["status"] = "success"
    except Exception as exc:
        _last_run["status"] = "failed"
        _last_run["error"] = str(exc)
        raise
    finally:
        _last_run["finished_at"] = _utc_now()
        if _last_run["status"] in {"success", "failed"}:
            run_log_id = _record_run_history()
            _record_generated_report(report_payload, run_log_id)
        _run_lock.release()


def _scheduler_loop(interval_minutes: int, stop_event: threading.Event) -> None:
    logger.info("App scheduler started: interval=%sm", interval_minutes)
    while not stop_event.is_set():
        try:
            _send_report_once()
            logger.info("Scheduler run completed")
        except Exception:
            logger.exception("Scheduler run failed")

        wait_seconds = max(1, interval_minutes * 60)
        if stop_event.wait(wait_seconds):
            break

    logger.info("App scheduler stopped")


@app.get("/")
def root():
    return {
        "service": "email-service",
        "status": "ok",
        "docs": "/docs",
        "health": "/health",
    }


@app.get("/health")
def health() -> dict:
    try:
        validate_config()
        return {
            "status": "ok",
            "config_ok": True,
            "last_run": _last_run,
            "scheduler_running": _is_scheduler_running(),
            "scheduler_interval_minutes": _scheduler_interval_minutes,
        }
    except Exception as exc:
        return {
            "status": "degraded",
            "config_ok": False,
            "config_error": str(exc),
            "last_run": _last_run,
            "scheduler_running": _is_scheduler_running(),
            "scheduler_interval_minutes": _scheduler_interval_minutes,
        }


@app.get("/logs")
def logs(limit: int = 100) -> dict:
    safe_limit = max(1, min(limit, _max_run_history))

    if __package__:
        from .db import get_conn
    else:
        from db import get_conn

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT
                        l.id,
                        l.timestamp,
                        l.status,
                        l.recipient,
                        l.subject,
                        l.template_used,
                        l.device_count,
                        l.error,
                        gr.id AS generated_report_id
                    FROM email_run_logs l
                    LEFT JOIN email_generated_reports gr
                      ON gr.run_log_id = l.id
                    ORDER BY l.id DESC
                    LIMIT %s
                    """,
                    (safe_limit,),
                )
                rows = cur.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load logs: {exc}") from exc

    items = [
        {
            "id": str(row["id"]),
            "timestamp": row["timestamp"].isoformat() if row["timestamp"] else None,
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

    success = sum(1 for item in items if item["status"] == "success")
    failed = sum(1 for item in items if item["status"] == "failed")

    return {
        "logs": items,
        "summary": {
            "total": len(items),
            "success": success,
            "failed": failed,
            "pending": 0,
        },
    }


@app.on_event("startup")
def on_startup() -> None:
    _ensure_logs_table()
    _ensure_generated_reports_table()


@app.post("/run-report")
def run_report_once() -> dict:
    try:
        _send_report_once()
        return {"message": "Report email sent", "last_run": _last_run}
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Report run failed: {exc}") from exc


@app.get("/generated-reports/{report_id}/pdf")
def download_generated_report_pdf(report_id: int) -> Response:
    if __package__:
        from .db import get_conn
    else:
        from db import get_conn

    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT pdf_filename, pdf_bytes
                    FROM email_generated_reports
                    WHERE id = %s
                    """,
                    (report_id,),
                )
                row = cur.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Failed to load generated report PDF: {exc}") from exc

    if not row:
        raise HTTPException(status_code=404, detail="Generated report not found")

    pdf_blob = row.get("pdf_bytes")
    if pdf_blob is None:
        raise HTTPException(status_code=404, detail="PDF not available for this generated report")

    pdf_bytes = bytes(pdf_blob)
    filename = row.get("pdf_filename") or f"generated_report_{report_id}.pdf"

    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{filename}"'},
    )


@app.post("/scheduler/start")
def start_scheduler(interval_minutes: int = 60) -> dict:
    global _scheduler_thread, _scheduler_stop, _scheduler_interval_minutes

    if _is_scheduler_running():
        return {
            "message": "Scheduler already running",
            "scheduler_running": True,
            "scheduler_interval_minutes": _scheduler_interval_minutes,
        }

    _scheduler_interval_minutes = interval_minutes
    _scheduler_stop = threading.Event()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(interval_minutes, _scheduler_stop),
        daemon=True,
        name="app-email-scheduler",
    )
    _scheduler_thread.start()

    return {
        "message": "Scheduler started",
        "scheduler_running": True,
        "scheduler_interval_minutes": _scheduler_interval_minutes,
    }


@app.post("/scheduler/stop")
def stop_scheduler() -> dict:
    global _scheduler_stop

    if not _is_scheduler_running():
        return {"message": "Scheduler already stopped", "scheduler_running": False}

    if _scheduler_stop is not None:
        _scheduler_stop.set()

    return {"message": "Scheduler stop requested", "scheduler_running": False}

