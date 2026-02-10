from datetime import datetime
from db import fetch_all
from config import REPORT_LOOKBACK_HOURS

def get_report_data():
    sql_prod = """
        SELECT machine_id,
               COALESCE(SUM(COALESCE(qty, 1)), 0)::int AS pieces
        FROM public.production_events
        WHERE event_time >= now() - (%(hours)s || ' hours')::interval
        GROUP BY machine_id
        ORDER BY machine_id;
    """

    sql_qual = """
        SELECT machine_id,
               COUNT(*) FILTER (WHERE result='OK')::int  AS ok_count,
               COUNT(*) FILTER (WHERE result='NOK')::int AS nok_count
        FROM public.quality_events
        WHERE event_time >= now() - (%(hours)s || ' hours')::interval
        GROUP BY machine_id
        ORDER BY machine_id;
    """

    sql_totals = """
        SELECT
          COALESCE((SELECT SUM(COALESCE(qty,1)) FROM public.production_events
                    WHERE event_time >= now() - (%(hours)s || ' hours')::interval), 0)::int AS total_pieces,
          COALESCE((SELECT COUNT(*) FROM public.quality_events
                    WHERE event_time >= now() - (%(hours)s || ' hours')::interval AND result='OK'), 0)::int AS total_ok,
          COALESCE((SELECT COUNT(*) FROM public.quality_events
                    WHERE event_time >= now() - (%(hours)s || ' hours')::interval AND result='NOK'), 0)::int AS total_nok;
    """

    params = {"hours": REPORT_LOOKBACK_HOURS}

    prod = fetch_all(sql_prod, params)
    qual = fetch_all(sql_qual, params)
    totals_rows = fetch_all(sql_totals, params)
    totals = totals_rows[0] if totals_rows else {"total_pieces": 0, "total_ok": 0, "total_nok": 0}

    merged = merge_by_machine(prod, qual)

    total_quality = totals["total_ok"] + totals["total_nok"]
    yield_pct = (totals["total_ok"] * 100.0 / total_quality) if total_quality > 0 else 0.0

    return {
        "hours": REPORT_LOOKBACK_HOURS,
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "totals": totals,
        "yield_pct": round(yield_pct, 1),
        "rows": merged,
    }

def merge_by_machine(prod_rows: list[dict], qual_rows: list[dict]) -> list[dict]:
    m: dict[str, dict] = {}

    for r in prod_rows:
        mid = r["machine_id"]
        m[mid] = {
            "machine_id": mid,
            "pieces": int(r.get("pieces") or 0),
            "ok": 0,
            "nok": 0,
            "yield_pct": 0.0,
        }

    for r in qual_rows:
        mid = r["machine_id"]
        cur = m.get(mid) or {
            "machine_id": mid,
            "pieces": 0,
            "ok": 0,
            "nok": 0,
            "yield_pct": 0.0,
        }
        cur["ok"] = int(r.get("ok_count") or 0)
        cur["nok"] = int(r.get("nok_count") or 0)
        m[mid] = cur

    out = []
    for mid in sorted(m.keys()):
        row = m[mid]
        q_total = row["ok"] + row["nok"]
        row["yield_pct"] = round((row["ok"] * 100.0 / q_total), 1) if q_total > 0 else 0.0
        out.append(row)

    return out
