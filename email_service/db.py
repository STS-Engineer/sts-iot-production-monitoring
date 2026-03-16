import psycopg
from psycopg.rows import dict_row
if __package__:
    from .config import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE
else:
    from config import PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD, PGSSLMODE

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
