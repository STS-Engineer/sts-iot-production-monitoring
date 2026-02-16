from datetime import datetime
from db import fetch_all
from config import REPORT_LOOKBACK_HOURS, CHART_LOOKBACK_HOURS, YIELD_ALERT_THRESHOLD

def _window_clause(offset_hours: int, duration_hours: int) -> str:
    # window: [now - (offset+duration), now - offset]
    # offset=0 => last duration
    return f"""
      event_time >= now() - ({offset_hours + duration_hours} || ' hours')::interval
      AND event_time <  now() - ({offset_hours} || ' hours')::interval
    """

def get_report_data():
    dur = REPORT_LOOKBACK_HOURS
    params = {"dur": dur, "chart_hours": CHART_LOOKBACK_HOURS}

    # -------- CURRENT WINDOW (last dur hours) --------
    sql_totals_cur = f"""
      SELECT
        COALESCE((SELECT SUM(COALESCE(qty,1)) FROM public.production_events
                  WHERE {_window_clause(0, dur)}), 0)::int AS total_pieces,
        COALESCE((SELECT COUNT(*) FROM public.quality_events
                  WHERE {_window_clause(0, dur)} AND result='OK'), 0)::int AS total_ok,
        COALESCE((SELECT COUNT(*) FROM public.quality_events
                  WHERE {_window_clause(0, dur)} AND result='NOK'), 0)::int AS total_nok;
    """

    sql_prod_cur = f"""
      SELECT machine_id,
             COALESCE(SUM(COALESCE(qty, 1)), 0)::int AS pieces
      FROM public.production_events
      WHERE {_window_clause(0, dur)}
      GROUP BY machine_id
      ORDER BY machine_id;
    """

    sql_qual_cur = f"""
      SELECT machine_id,
             COUNT(*) FILTER (WHERE result='OK')::int  AS ok_count,
             COUNT(*) FILTER (WHERE result='NOK')::int AS nok_count
      FROM public.quality_events
      WHERE {_window_clause(0, dur)}
      GROUP BY machine_id
      ORDER BY machine_id;
    """

    # -------- PREVIOUS WINDOW (the period before) --------
    sql_totals_prev = f"""
      SELECT
        COALESCE((SELECT SUM(COALESCE(qty,1)) FROM public.production_events
                  WHERE {_window_clause(dur, dur)}), 0)::int AS total_pieces,
        COALESCE((SELECT COUNT(*) FROM public.quality_events
                  WHERE {_window_clause(dur, dur)} AND result='OK'), 0)::int AS total_ok,
        COALESCE((SELECT COUNT(*) FROM public.quality_events
                  WHERE {_window_clause(dur, dur)} AND result='NOK'), 0)::int AS total_nok;
    """

    # -------- CHART DATA (last CHART_LOOKBACK_HOURS hours) --------
    sql_hourly = """
      SELECT date_trunc('hour', event_time) AS hour_bucket,
             COALESCE(SUM(COALESCE(qty,1)),0)::int AS pieces
      FROM public.production_events
      WHERE event_time >= now() - (%(chart_hours)s || ' hours')::interval
      GROUP BY 1
      ORDER BY 1;
    """

    totals = fetch_all(sql_totals_cur)[0]
    totals_prev = fetch_all(sql_totals_prev)[0]
    prod = fetch_all(sql_prod_cur)
    qual = fetch_all(sql_qual_cur)
    hourly = fetch_all(sql_hourly, {"chart_hours": CHART_LOOKBACK_HOURS})

    rows = merge_by_machine(prod, qual)

    # Global yield
    total_quality = totals["total_ok"] + totals["total_nok"]
    yield_pct = (totals["total_ok"] * 100.0 / total_quality) if total_quality > 0 else None

    # Previous yield
    total_quality_prev = totals_prev["total_ok"] + totals_prev["total_nok"]
    yield_prev = (totals_prev["total_ok"] * 100.0 / total_quality_prev) if total_quality_prev > 0 else None

    # Parts per minute (PPM)
    minutes = dur * 60.0
    ppm = (totals["total_pieces"] / minutes) if minutes > 0 else 0.0
    ppm_prev = (totals_prev["total_pieces"] / minutes) if minutes > 0 else 0.0

    # Trends (delta & %)
    trends = compute_trends(
        cur=totals, prev=totals_prev,
        yield_cur=yield_pct, yield_prev=yield_prev,
        ppm_cur=ppm, ppm_prev=ppm_prev
    )

    # Ranking best->worst by yield, then pieces
    ranked = sorted(
        rows,
        key=lambda r: ((r["yield_pct"] if r["yield_pct"] is not None else -1), r["pieces"]),
        reverse=True
    )

    # Alerts
    alerts = build_alerts(yield_pct, rows, YIELD_ALERT_THRESHOLD)

    result = {
        "hours": dur,
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "totals": totals,
        "yield_pct": None if yield_pct is None else round(yield_pct, 1),
        "ppm": round(ppm, 2),
        "rows": rows,
        "ranked_rows": ranked,
        "trends": trends,
        "alerts": alerts,
        "alert_threshold": YIELD_ALERT_THRESHOLD,
        "hourly_series": [
            {"label": h["hour_bucket"].strftime("%H:%M"), "pieces": int(h["pieces"])}
            for h in hourly
        ],
    }
    return result

def compute_trends(cur: dict, prev: dict, yield_cur, yield_prev, ppm_cur: float, ppm_prev: float) -> dict:
    def delta(a, b):
        return (a - b)

    def pct(a, b):
        if b == 0:
            return None
        return (a - b) * 100.0 / b

    out = {}
    out["pieces_delta"] = delta(cur["total_pieces"], prev["total_pieces"])
    out["pieces_pct"] = pct(cur["total_pieces"], prev["total_pieces"])

    out["ok_delta"] = delta(cur["total_ok"], prev["total_ok"])
    out["ok_pct"] = pct(cur["total_ok"], prev["total_ok"])

    out["nok_delta"] = delta(cur["total_nok"], prev["total_nok"])
    out["nok_pct"] = pct(cur["total_nok"], prev["total_nok"])

    out["yield_delta"] = None if (yield_cur is None or yield_prev is None) else round(yield_cur - yield_prev, 1)
    out["ppm_delta"] = round(ppm_cur - ppm_prev, 2)
    out["ppm_pct"] = None if ppm_prev == 0 else round((ppm_cur - ppm_prev) * 100.0 / ppm_prev, 1)
    return out

def build_alerts(yield_pct, rows: list[dict], threshold: float) -> list[str]:
    alerts = []
    if yield_pct is not None and yield_pct < threshold:
        alerts.append(f"Global yield is below threshold: {yield_pct:.1f}% < {threshold:.1f}%")

    low = [r for r in rows if (r["yield_pct"] is not None and r["yield_pct"] < threshold)]
    if low:
        worst = sorted(low, key=lambda r: r["yield_pct"])[0]
        alerts.append(f"Machine with lowest yield: {worst['machine_id']} ({worst['yield_pct']:.1f}%)")

    return alerts

def merge_by_machine(prod_rows: list[dict], qual_rows: list[dict]) -> list[dict]:
    m = {}
    for r in prod_rows:
        mid = r["machine_id"]
        m[mid] = {
            "machine_id": mid,
            "pieces": int(r.get("pieces") or 0),
            "ok": 0,
            "nok": 0,
            "yield_pct": None,
        }

    for r in qual_rows:
        mid = r["machine_id"]
        cur = m.get(mid) or {
            "machine_id": mid,
            "pieces": 0,
            "ok": 0,
            "nok": 0,
            "yield_pct": None,
        }
        cur["ok"] = int(r.get("ok_count") or 0)
        cur["nok"] = int(r.get("nok_count") or 0)
        m[mid] = cur

    out = []
    for mid, row in sorted(m.items()):
        q_total = row["ok"] + row["nok"]
        row["yield_pct"] = (row["ok"] * 100.0 / q_total) if q_total > 0 else None
        if row["yield_pct"] is not None:
            row["yield_pct"] = round(row["yield_pct"], 1)
        out.append(row)
    return out
