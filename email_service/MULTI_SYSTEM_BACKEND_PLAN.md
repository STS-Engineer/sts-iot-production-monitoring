# Multi-System Backend Plan

This document translates the current `email_service` codebase into the target multi-system architecture.

## 1. Target Schema

Use one application database. Make every operational table system-aware.

```sql
CREATE TABLE IF NOT EXISTS systems (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    code TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    description TEXT,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    status TEXT NOT NULL DEFAULT 'inactive',
    source_type TEXT NOT NULL DEFAULT 'postgresql',
    is_archived BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS system_data_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    source_type TEXT NOT NULL,
    host TEXT,
    port INTEGER,
    database_name TEXT,
    schema_name TEXT DEFAULT 'public',
    username TEXT,
    password_encrypted TEXT,
    ssl_mode TEXT,
    options_json JSONB,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (system_id, is_active) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE IF NOT EXISTS system_email_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    profile_name TEXT NOT NULL DEFAULT 'default',
    smtp_host TEXT NOT NULL,
    smtp_port INTEGER NOT NULL,
    smtp_security TEXT NOT NULL DEFAULT 'starttls',
    smtp_username TEXT,
    smtp_password_encrypted TEXT,
    from_email TEXT NOT NULL,
    from_name TEXT,
    reply_to_email TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (system_id, profile_name)
);

CREATE TABLE IF NOT EXISTS system_recipients (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    email_profile_id UUID REFERENCES system_email_profiles(id) ON DELETE SET NULL,
    email TEXT NOT NULL,
    recipient_type TEXT NOT NULL DEFAULT 'to',
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (system_id, email, recipient_type)
);

CREATE TABLE IF NOT EXISTS system_report_profiles (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    profile_name TEXT NOT NULL DEFAULT 'default',
    template_name TEXT NOT NULL DEFAULT 'report.html',
    report_lookback_hours INTEGER NOT NULL DEFAULT 1,
    chart_lookback_hours INTEGER NOT NULL DEFAULT 8,
    yield_alert_threshold NUMERIC(5,2) NOT NULL DEFAULT 90,
    data_mapping_json JSONB,
    chart_options_json JSONB,
    pdf_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (system_id, profile_name)
);

CREATE TABLE IF NOT EXISTS scheduler_state (
    system_id UUID PRIMARY KEY REFERENCES systems(id) ON DELETE CASCADE,
    enabled BOOLEAN NOT NULL DEFAULT FALSE,
    schedule_type TEXT NOT NULL DEFAULT 'hourly',
    interval_minutes INTEGER NOT NULL DEFAULT 60,
    cron_expression TEXT,
    next_run_at TIMESTAMPTZ,
    last_run_at TIMESTAMPTZ,
    last_status TEXT,
    running BOOLEAN NOT NULL DEFAULT FALSE,
    locked_at TIMESTAMPTZ,
    lock_owner TEXT,
    last_error TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_logs (
    id BIGSERIAL PRIMARY KEY,
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    trigger_type TEXT NOT NULL DEFAULT 'manual',
    recipient TEXT,
    subject TEXT,
    template_used TEXT,
    device_count INTEGER,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_run_logs_system_started_at
    ON run_logs(system_id, started_at DESC);

CREATE TABLE IF NOT EXISTS generated_reports (
    id BIGSERIAL PRIMARY KEY,
    system_id UUID NOT NULL REFERENCES systems(id) ON DELETE CASCADE,
    run_log_id BIGINT REFERENCES run_logs(id) ON DELETE SET NULL,
    report_period_start TIMESTAMPTZ,
    report_period_end TIMESTAMPTZ,
    status TEXT NOT NULL,
    recipient TEXT,
    subject TEXT,
    template_used TEXT,
    report_generated_at TEXT,
    report_hours INTEGER,
    report_totals_json JSONB,
    report_rows_json JSONB,
    html_body TEXT,
    text_body TEXT,
    pdf_filename TEXT,
    pdf_bytes BYTEA,
    delivery_status TEXT,
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_generated_reports_system_created_at
    ON generated_reports(system_id, created_at DESC);

CREATE TABLE IF NOT EXISTS audit_log (
    id BIGSERIAL PRIMARY KEY,
    system_id UUID REFERENCES systems(id) ON DELETE CASCADE,
    actor TEXT,
    action TEXT NOT NULL,
    entity_type TEXT NOT NULL,
    entity_id TEXT,
    before_json JSONB,
    after_json JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

## 2. Minimal Bootstrap Migration

Use the current `.env` values to create one default system and associated profiles.

Suggested bootstrap records:

- `systems`
  - `code = 'DEFAULT'`
  - `name = 'Default System'`
  - `status = 'active'`
  - `source_type = 'postgresql'`
- `system_data_sources`
  - from `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGSSLMODE`
- `system_email_profiles`
  - from `SMTP_HOST`, `SMTP_PORT`, `SMTP_USER`, `SMTP_PASS`, `EMAIL_FROM`
- `system_recipients`
  - one row per `EMAIL_TO`
- `system_report_profiles`
  - from `REPORT_LOOKBACK_HOURS`, `CHART_LOOKBACK_HOURS`, `YIELD_ALERT_THRESHOLD`
- `scheduler_state`
  - initialize from current in-memory defaults

## 3. Required Runtime Model

Replace global config assumptions with a runtime context object.

```python
from dataclasses import dataclass

@dataclass
class DataSourceConfig:
    source_type: str
    host: str | None
    port: int | None
    database_name: str | None
    schema_name: str | None
    username: str | None
    password: str | None
    ssl_mode: str | None
    options: dict

@dataclass
class EmailProfile:
    smtp_host: str
    smtp_port: int
    smtp_security: str
    smtp_username: str | None
    smtp_password: str | None
    from_email: str
    from_name: str | None
    recipients: list[str]

@dataclass
class ReportProfile:
    template_name: str
    report_lookback_hours: int
    chart_lookback_hours: int
    yield_alert_threshold: float
    data_mapping: dict
    chart_options: dict
    pdf_enabled: bool

@dataclass
class SystemContext:
    system_id: str
    system_code: str
    system_name: str
    timezone: str
    data_source: DataSourceConfig
    email_profile: EmailProfile
    report_profile: ReportProfile
```

Every major service should accept `SystemContext`.

## 4. Backend Route Shape

Recommended route model:

```text
GET    /systems
POST   /systems
GET    /systems/{system_id}
PATCH  /systems/{system_id}
GET    /systems/{system_id}/health
GET    /systems/{system_id}/logs
GET    /systems/{system_id}/config
PATCH  /systems/{system_id}/config/data-source
PATCH  /systems/{system_id}/config/email
PATCH  /systems/{system_id}/config/report
GET    /systems/{system_id}/schedule
PATCH  /systems/{system_id}/schedule
POST   /systems/{system_id}/run-report
POST   /systems/{system_id}/scheduler/start
POST   /systems/{system_id}/scheduler/stop
GET    /systems/{system_id}/reports
GET    /systems/{system_id}/reports/{report_id}/pdf
POST   /systems/{system_id}/test/data-source
POST   /systems/{system_id}/test/email
```

Use path-scoped routes. This keeps isolation explicit and frontend wiring simpler.

## 5. Refactor Map Against Current Files

### `email_service/config.py`

Current role:
- Global environment-backed runtime config.

Target role:
- Keep only bootstrap settings and platform-level settings.
- Remove per-system runtime authority from this file.

Refactor:
- Keep DB connection to the platform database.
- Keep app-level secret and encryption settings.
- Move `SMTP_*`, `EMAIL_TO`, `PGHOST`, `REPORT_LOOKBACK_HOURS`, and similar values out of active runtime use after bootstrap.
- Add platform settings such as:
  - `APP_ENCRYPTION_KEY`
  - `SCHEDULER_POLL_SECONDS`
  - `DEFAULT_TIMEZONE`

### `email_service/db.py`

Current role:
- Opens one connection using global source DB credentials.

Target role:
- Connect only to the platform database.
- Add repository helpers for system-aware persistence.

Refactor:
- Keep `get_conn()` but point it only at the platform DB.
- Add repository functions or repository modules for:
  - system registry
  - profiles
  - scheduler state
  - run logs
  - generated reports
- Introduce separate source connectors; do not use `db.py` for plant/source data access directly.

### `email_service/report.py`

Current role:
- Queries one PostgreSQL source directly.
- Reads lookback and threshold settings from global config.

Target role:
- Pure report service operating on `SystemContext`.

Refactor:
- Change `get_report_data()` to `get_report_data(ctx: SystemContext)`.
- Replace imports from `config.py` with `ctx.report_profile`.
- Replace direct `fetch_all()` calls with a connector object:
  - `connector.fetch_totals(ctx, ...)`
  - `connector.fetch_machine_rows(ctx, ...)`
  - `connector.fetch_hourly_series(ctx, ...)`
- Keep KPI calculation logic and merge/trend helpers.
- Remove hardcoded `public.production_events` and `public.quality_events` assumptions from this layer.

### `email_service/mailer.py`

Current role:
- Sends through one global SMTP profile.

Target role:
- Notification service driven by `SystemContext`.

Refactor:
- Change `send_html_email(...)` to accept `email_profile` or `ctx`.
- Stop importing `SMTP_*` and `EMAIL_FROM` from `config.py`.
- Build the message from `ctx.email_profile`.
- Keep MIME assembly logic.
- Later add retry policy and delivery status capture.

### `email_service/main.py`

Current role:
- Single-system orchestration and optional local scheduler.

Target role:
- System-aware run pipeline, reusable by API and scheduler.

Refactor:
- Change `send_report()` to `send_report_for_system(ctx: SystemContext, trigger_type: str = "manual")`.
- Keep template rendering and PDF generation.
- Replace `EMAIL_TO` references with `ctx.email_profile.recipients`.
- Replace global `validate_config()` use with:
  - `load_system_context(system_id)`
  - `validate_system_context(ctx)`
- Move local scheduler logic out or mark it as development-only.

Suggested split:
- orchestration stays in `main.py` temporarily
- later move to `services/run_service.py`

### `email_service/app.py`

Current role:
- Single-system FastAPI wrapper with global state:
  - `_last_run`
  - `_run_lock`
  - `_scheduler_thread`
  - `_scheduler_interval_minutes`

Target role:
- Multi-system API entry point.

Refactor:
- Remove global single-instance runtime state.
- Replace `_last_run` with persisted `run_logs` and `scheduler_state`.
- Replace one global lock with per-system lock strategy.
- Replace `/health`, `/logs`, `/config`, `/run-report`, `/scheduler/*` with system-scoped routes.
- Keep startup migration/bootstrap hooks, but move DDL creation into dedicated migration/bootstrap code.

Immediate structural split recommended:
- `routers/systems.py`
- `routers/runs.py`
- `routers/scheduler.py`
- `routers/configuration.py`

### `email_service/charts.py`

Current role:
- Shared rendering helper.

Target role:
- Mostly unchanged.

Refactor:
- Keep chart rendering functions.
- Accept series/rows already prepared by report service.
- No system-specific logic should live here.

### `email_service/pdf_report.py`

Current role:
- Shared PDF builder.

Target role:
- Mostly unchanged.

Refactor:
- Keep as shared rendering utility.
- Ensure it only consumes report payload, not global config.

### `email_service/templates/report.html`

Current role:
- Shared report template.

Target role:
- Remains reusable.

Refactor:
- No immediate change required.
- Later support template selection per report profile.

### `email_service/start_local_app.ps1` and `run_app.cmd`

Current role:
- Starts current single FastAPI app or local CLI flow.

Target role:
- Still useful for local development.

Refactor:
- Keep them.
- Update docs after route changes.
- Local CLI mode should accept `system_id` when manual runs are needed.

## 6. New Modules To Introduce

Recommended additions:

```text
email_service/
  models/
    system_context.py
  repositories/
    systems.py
    profiles.py
    scheduler_state.py
    run_logs.py
    reports.py
  services/
    system_context_service.py
    run_service.py
    scheduler_service.py
    health_service.py
  connectors/
    base.py
    postgresql.py
  routers/
    systems.py
    configuration.py
    scheduler.py
    runs.py
```

## 7. First Safe Implementation Sequence

1. Add new schema tables without removing current ones.
2. Bootstrap one default system from current `.env`.
3. Add `system_id` to current logging/report writes.
4. Introduce `SystemContext` and load it from DB.
5. Convert `report.py`, `mailer.py`, and `main.py` to consume `SystemContext`.
6. Add new `/systems/...` routes beside current legacy routes.
7. Migrate frontend to the new routes.
8. Remove legacy single-system endpoints after parity is confirmed.

## 8. Exact Risks In The Current Code

These are the blockers to multi-system behavior:

- `app.py` stores runtime state in global variables, which makes all systems share one state machine.
- `db.py` assumes the source data DB and the application DB are the same connection.
- `report.py` hardcodes source schema/table names and reads thresholds from global config.
- `mailer.py` imports one SMTP profile globally.
- `main.py` imports one recipient list globally.
- Current logs and reports tables are not system-scoped.

## 9. Recommendation On Legacy Compatibility

During migration, keep legacy endpoints only as compatibility shims:

- `/health`
- `/logs`
- `/config`
- `/run-report`
- `/scheduler/start`
- `/scheduler/stop`

Back them with the default system internally until the frontend is fully switched.
