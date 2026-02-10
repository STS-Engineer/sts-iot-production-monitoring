import os
from dotenv import load_dotenv

# Load .env locally if present. In Azure later, real env vars will be used.
load_dotenv(override=False)

# ---- PostgreSQL ----
PGHOST = os.getenv("PGHOST")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE")
PGUSER = os.getenv("PGUSER")
PGPASSWORD = os.getenv("PGPASSWORD")
PGSSLMODE = os.getenv("PGSSLMODE", "require")  # require for Azure

# ---- Email / SMTP ----
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.office365.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

EMAIL_FROM = os.getenv("EMAIL_FROM", SMTP_USER)
EMAIL_TO = [x.strip() for x in (os.getenv("EMAIL_TO", "")).split(",") if x.strip()]

# ---- Report options ----
REPORT_LOOKBACK_HOURS = int(os.getenv("REPORT_LOOKBACK_HOURS", "1"))

def validate_config() -> None:
    missing = []
    for k in ["PGHOST", "PGDATABASE", "PGUSER", "PGPASSWORD"]:
        if not globals().get(k):
            missing.append(k)

    for k in ["SMTP_HOST", "SMTP_PORT", "SMTP_USER", "SMTP_PASS", "EMAIL_FROM"]:
        if not globals().get(k):
            missing.append(k)

    if not EMAIL_TO:
        missing.append("EMAIL_TO")

    if missing:
        raise RuntimeError(f"Missing config values: {', '.join(missing)}")
