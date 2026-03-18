import uuid

import psycopg
from psycopg.rows import dict_row
if __package__:
    from .config import (
        CHART_LOOKBACK_HOURS,
        PGHOST,
        PGPORT,
        PGDATABASE,
        PGPASSWORD,
        PGSSLMODE,
        PGUSER,
        REPORT_LOOKBACK_HOURS,
        YIELD_ALERT_THRESHOLD,
    )
else:
    from config import (
        CHART_LOOKBACK_HOURS,
        PGHOST,
        PGPORT,
        PGDATABASE,
        PGPASSWORD,
        PGSSLMODE,
        PGUSER,
        REPORT_LOOKBACK_HOURS,
        YIELD_ALERT_THRESHOLD,
    )

def get_conn():
    return psycopg.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE,
        row_factory=dict_row,
    )

def fetch_all(sql: str, params: dict | None = None) -> list[dict]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or {})
            return cur.fetchall()


def fetch_one(sql: str, params: dict | None = None) -> dict | None:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def get_system(system_id: str) -> dict | None:
    return fetch_one(
        """
        SELECT id, code, name, description, timezone, status, source_type, is_archived, created_at, updated_at
        FROM public.systems
        WHERE id = %(system_id)s AND NOT is_archived
        """,
        {"system_id": system_id},
    )


def get_systems() -> list[dict]:
    return fetch_all(
        """
        SELECT id, code, name, description, timezone, status, source_type, is_archived, created_at, updated_at
        FROM public.systems
        WHERE NOT is_archived
        ORDER BY name
        """
    )


def create_system(*, code: str, name: str, timezone: str, status: str, description: str | None = None) -> dict:
    system_id = str(uuid.uuid4())
    report_profile_id = str(uuid.uuid4())
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.systems (
                    id,
                    code,
                    name,
                    description,
                    timezone,
                    status,
                    source_type,
                    is_archived,
                    created_at,
                    updated_at
                )
                VALUES (%(id)s, %(code)s, %(name)s, %(description)s, %(timezone)s, %(status)s, 'centralized', FALSE, NOW(), NOW())
                RETURNING id, code, name, description, timezone, status, source_type, is_archived, created_at, updated_at
                """,
                {
                    "id": system_id,
                    "code": code,
                    "name": name,
                    "description": description,
                    "timezone": timezone,
                    "status": status,
                },
            )
            system = cur.fetchone()
            system_id = str(system["id"])
            cur.execute(
                """
                INSERT INTO public.scheduler_state (
                    system_id,
                    enabled,
                    schedule_type,
                    interval_minutes,
                    cron_expression,
                    next_run_at,
                    last_run_at,
                    last_status,
                    running,
                    locked_at,
                    lock_owner,
                    last_error,
                    updated_at
                )
                VALUES (%s, FALSE, 'hourly', 60, NULL, NULL, NULL, NULL, FALSE, NULL, NULL, NULL, NOW())
                ON CONFLICT (system_id) DO NOTHING
                """,
                (system_id,),
            )
            cur.execute(
                """
                INSERT INTO public.system_report_profiles (
                    id,
                    system_id,
                    profile_name,
                    template_name,
                    report_lookback_hours,
                    chart_lookback_hours,
                    yield_alert_threshold,
                    data_mapping_json,
                    chart_options_json,
                    pdf_enabled,
                    is_active,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, 'default', 'report.html', %s, %s, %s, '{}'::jsonb, '{}'::jsonb, TRUE, TRUE, NOW(), NOW())
                """,
                (report_profile_id, system_id, REPORT_LOOKBACK_HOURS, CHART_LOOKBACK_HOURS, YIELD_ALERT_THRESHOLD),
            )
            conn.commit()
            return system


def get_system_recipients(system_id: str) -> list[str]:
    rows = fetch_all(
        """
        SELECT email
        FROM public.system_recipients
        WHERE system_id = %(system_id)s
          AND is_active = TRUE
          AND recipient_type = 'to'
        ORDER BY email
        """,
        {"system_id": system_id},
    )
    return [row["email"] for row in rows]


def get_system_report_profile(system_id: str) -> dict:
    row = fetch_one(
        """
        SELECT
            profile_name,
            template_name,
            report_lookback_hours,
            chart_lookback_hours,
            yield_alert_threshold,
            data_mapping_json,
            chart_options_json,
            pdf_enabled
        FROM public.system_report_profiles
        WHERE system_id = %(system_id)s
          AND is_active = TRUE
        ORDER BY updated_at DESC
        LIMIT 1
        """,
        {"system_id": system_id},
    )

    return row or {
        "profile_name": "default",
        "template_name": "report.html",
        "report_lookback_hours": REPORT_LOOKBACK_HOURS,
        "chart_lookback_hours": CHART_LOOKBACK_HOURS,
        "yield_alert_threshold": YIELD_ALERT_THRESHOLD,
        "data_mapping_json": {},
        "chart_options_json": {},
        "pdf_enabled": True,
    }


def get_scheduler_state(system_id: str) -> dict:
    row = fetch_one(
        """
        SELECT
            system_id,
            enabled,
            schedule_type,
            interval_minutes,
            cron_expression,
            next_run_at,
            last_run_at,
            last_status,
            running,
            locked_at,
            lock_owner,
            last_error,
            updated_at
        FROM public.scheduler_state
        WHERE system_id = %(system_id)s
        """,
        {"system_id": system_id},
    )

    return row or {
        "system_id": system_id,
        "enabled": False,
        "schedule_type": "hourly",
        "interval_minutes": 60,
        "cron_expression": None,
        "next_run_at": None,
        "last_run_at": None,
        "last_status": None,
        "running": False,
        "locked_at": None,
        "lock_owner": None,
        "last_error": None,
        "updated_at": None,
    }


def save_scheduler_state(system_id: str, *, enabled: bool, interval_minutes: int, running: bool | None = None, last_run_at=None, next_run_at=None, last_status: str | None = None, last_error: str | None = None, lock_owner: str | None = None, locked_at=None) -> None:
    current = get_scheduler_state(system_id)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.scheduler_state (
                    system_id,
                    enabled,
                    schedule_type,
                    interval_minutes,
                    cron_expression,
                    next_run_at,
                    last_run_at,
                    last_status,
                    running,
                    locked_at,
                    lock_owner,
                    last_error,
                    updated_at
                )
                VALUES (
                    %(system_id)s,
                    %(enabled)s,
                    %(schedule_type)s,
                    %(interval_minutes)s,
                    %(cron_expression)s,
                    %(next_run_at)s,
                    %(last_run_at)s,
                    %(last_status)s,
                    %(running)s,
                    %(locked_at)s,
                    %(lock_owner)s,
                    %(last_error)s,
                    NOW()
                )
                ON CONFLICT (system_id) DO UPDATE SET
                    enabled = EXCLUDED.enabled,
                    schedule_type = EXCLUDED.schedule_type,
                    interval_minutes = EXCLUDED.interval_minutes,
                    cron_expression = EXCLUDED.cron_expression,
                    next_run_at = EXCLUDED.next_run_at,
                    last_run_at = EXCLUDED.last_run_at,
                    last_status = EXCLUDED.last_status,
                    running = EXCLUDED.running,
                    locked_at = EXCLUDED.locked_at,
                    lock_owner = EXCLUDED.lock_owner,
                    last_error = EXCLUDED.last_error,
                    updated_at = NOW()
                """,
                {
                    "system_id": system_id,
                    "enabled": enabled,
                    "schedule_type": current["schedule_type"] or "hourly",
                    "interval_minutes": interval_minutes,
                    "cron_expression": current["cron_expression"],
                    "next_run_at": next_run_at,
                    "last_run_at": last_run_at,
                    "last_status": last_status,
                    "running": current["running"] if running is None else running,
                    "locked_at": locked_at,
                    "lock_owner": lock_owner,
                    "last_error": last_error,
                },
            )
            conn.commit()
