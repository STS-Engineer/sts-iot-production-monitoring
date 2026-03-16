# Run Locally (No Web Server)

This runs the email service as a local Python application (CLI), not as a web server.

## 1) Install dependencies

From project root:

```powershell
& ".venv\Scripts\python.exe" -m pip install -r ".\email_service\requirements.txt"
```

## 2) Run once (send one report now)

From project root:

```powershell
& ".\email_service\start_local_app.ps1" -Mode once
```

Equivalent direct command:

```powershell
& ".venv\Scripts\python.exe" ".\email_service\main.py" --once
```

## 3) Run scheduler (continuous local app)

Every 60 minutes:

```powershell
& ".\email_service\start_local_app.ps1" -Mode scheduler -Interval 60
```

Shortcut (default is hourly scheduler):

```powershell
& ".\email_service\start_local_app.ps1"
```

With first run aligned to top of hour:

```powershell
& ".\email_service\start_local_app.ps1" -Mode scheduler -Interval 60 -Align
```

Stop with `Ctrl + C`.

## Notes

- Ensure `.env` contains all DB and SMTP settings.
- This project uses one PostgreSQL connection (`PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, `PGSSLMODE`).
- On API startup, table `email_run_logs` is auto-created in the same database if it does not exist.
- This mode does not use FastAPI or `uvicorn`.
- The FastAPI wrapper is available for API endpoints (`/health`, `/run-report`, `/scheduler/start`, `/scheduler/stop`).
