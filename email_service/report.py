from datetime import datetime

if __package__:
    from .db import fetch_all
else:
    from db import fetch_all


def _window_clause(offset_hours: int, duration_hours: int) -> str:
    return f"""
      event_time >= now() - ({offset_hours + duration_hours} || ' hours')::interval
      AND event_time <  now() - ({offset_hours} || ' hours')::interval
    """


def get_report_data(system_id: str, profile: dict) -> dict:
    dur = int(profile["report_lookback_hours"])
    chart_hours = int(profile["chart_lookback_hours"])
    threshold = float(profile["yield_alert_threshold"])

    sql_totals_cur = f"""
      SELECT
        COALESCE((
            SELECT SUM(COALESCE(qty, 1))
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'PRODUCTION'
              AND {_window_clause(0, dur)}
        ), 0)::int AS total_pieces,
        COALESCE((
            SELECT COUNT(*)
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'QUALITY'
              AND result = 'OK'
              AND {_window_clause(0, dur)}
        ), 0)::int AS total_ok,
        COALESCE((
            SELECT COUNT(*)
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'QUALITY'
              AND result = 'NOK'
              AND {_window_clause(0, dur)}
        ), 0)::int AS total_nok;
    """

    sql_prod_cur = f"""
      SELECT machine_id,
             COALESCE(SUM(COALESCE(qty, 1)), 0)::int AS pieces,
             ROUND(AVG(NULLIF(cycle_time_ms, 0))::numeric, 1) AS avg_cycle_ms,
             MAX(event_time) AS last_event
      FROM public.events
      WHERE system_id = %(system_id)s
        AND event_type = 'PRODUCTION'
        AND {_window_clause(0, dur)}
      GROUP BY machine_id
      ORDER BY machine_id;
    """

    sql_qual_cur = f"""
      SELECT machine_id,
             COUNT(*) FILTER (WHERE result = 'OK')::int  AS ok_count,
             COUNT(*) FILTER (WHERE result = 'NOK')::int AS nok_count
      FROM public.events
      WHERE system_id = %(system_id)s
        AND event_type = 'QUALITY'
        AND {_window_clause(0, dur)}
      GROUP BY machine_id
      ORDER BY machine_id;
    """

    sql_totals_prev = f"""
      SELECT
        COALESCE((
            SELECT SUM(COALESCE(qty, 1))
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'PRODUCTION'
              AND {_window_clause(dur, dur)}
        ), 0)::int AS total_pieces,
        COALESCE((
            SELECT COUNT(*)
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'QUALITY'
              AND result = 'OK'
              AND {_window_clause(dur, dur)}
        ), 0)::int AS total_ok,
        COALESCE((
            SELECT COUNT(*)
            FROM public.events
            WHERE system_id = %(system_id)s
              AND event_type = 'QUALITY'
              AND result = 'NOK'
              AND {_window_clause(dur, dur)}
        ), 0)::int AS total_nok;
    """

    sql_hourly = """
      SELECT date_trunc('hour', event_time) AS hour_bucket,
             COALESCE(SUM(COALESCE(qty, 1)), 0)::int AS pieces
      FROM public.events
      WHERE system_id = %(system_id)s
        AND event_type = 'PRODUCTION'
        AND event_time >= now() - (%(chart_hours)s || ' hours')::interval
      GROUP BY 1
      ORDER BY 1;
    """

    params = {"system_id": system_id, "chart_hours": chart_hours}
    totals = fetch_all(sql_totals_cur, params)[0]
    totals_prev = fetch_all(sql_totals_prev, params)[0]
    prod = fetch_all(sql_prod_cur, params)
    qual = fetch_all(sql_qual_cur, params)
    hourly = fetch_all(sql_hourly, params)

    rows = merge_by_machine(prod, qual, dur)
    total_quality = totals["total_ok"] + totals["total_nok"]
    yield_pct = (totals["total_ok"] * 100.0 / total_quality) if total_quality > 0 else None
    total_quality_prev = totals_prev["total_ok"] + totals_prev["total_nok"]
    yield_prev = (totals_prev["total_ok"] * 100.0 / total_quality_prev) if total_quality_prev > 0 else None

    minutes = dur * 60.0
    ppm = (totals["total_pieces"] / minutes) if minutes > 0 else 0.0
    ppm_prev = (totals_prev["total_pieces"] / minutes) if minutes > 0 else 0.0

    trends = compute_trends(
        cur=totals,
        prev=totals_prev,
        yield_cur=yield_pct,
        yield_prev=yield_prev,
        ppm_cur=ppm,
        ppm_prev=ppm_prev,
    )

    ranked = sorted(
        rows,
        key=lambda row: ((row["yield_pct"] if row["yield_pct"] is not None else -1), row["pieces"]),
        reverse=True,
    )

    alerts = build_alerts(yield_pct, rows, threshold)

    return {
        "system_id": system_id,
        "hours": dur,
        "generated_at": datetime.now().astimezone().strftime("%Y-%m-%d %H:%M:%S %Z"),
        "totals": totals,
        "yield_pct": None if yield_pct is None else round(yield_pct, 1),
        "ppm": round(ppm, 2),
        "rows": rows,
        "ranked_rows": ranked,
        "trends": trends,
        "alerts": alerts,
        "alert_threshold": threshold,
        "hourly_series": [
            {"label": bucket["hour_bucket"].strftime("%H:%M"), "pieces": int(bucket["pieces"])}
            for bucket in hourly
        ],
    }


def compute_trends(cur: dict, prev: dict, yield_cur, yield_prev, ppm_cur: float, ppm_prev: float) -> dict:
    def delta(a, b):
        return a - b

    def pct(a, b):
        if b == 0:
            return None
        return (a - b) * 100.0 / b

    return {
        "pieces_delta": delta(cur["total_pieces"], prev["total_pieces"]),
        "pieces_pct": pct(cur["total_pieces"], prev["total_pieces"]),
        "ok_delta": delta(cur["total_ok"], prev["total_ok"]),
        "ok_pct": pct(cur["total_ok"], prev["total_ok"]),
        "nok_delta": delta(cur["total_nok"], prev["total_nok"]),
        "nok_pct": pct(cur["total_nok"], prev["total_nok"]),
        "yield_delta": None if (yield_cur is None or yield_prev is None) else round(yield_cur - yield_prev, 1),
        "ppm_delta": round(ppm_cur - ppm_prev, 2),
        "ppm_pct": None if ppm_prev == 0 else round((ppm_cur - ppm_prev) * 100.0 / ppm_prev, 1),
    }


def build_alerts(yield_pct, rows: list[dict], threshold: float) -> list[str]:
    alerts = []
    if yield_pct is not None and yield_pct < threshold:
        alerts.append(f"Global quality rate is below threshold: {yield_pct:.1f}% < {threshold:.1f}%")

    low_rows = [row for row in rows if row["yield_pct"] is not None and row["yield_pct"] < threshold]
    if low_rows:
        worst = sorted(low_rows, key=lambda row: row["yield_pct"])[0]
        alerts.append(f"Machine with lowest quality rate: {worst['machine_id']} ({worst['yield_pct']:.1f}%)")

    return alerts


def merge_by_machine(prod_rows: list[dict], qual_rows: list[dict], report_lookback_hours: int) -> list[dict]:
    merged: dict[str, dict] = {}
    for row in prod_rows:
        machine_id = row["machine_id"]
        merged[machine_id] = {
            "machine_id": machine_id,
            "pieces": int(row.get("pieces") or 0),
            "ok": 0,
            "nok": 0,
            "yield_pct": None,
            "avg_cycle_ms": None if row.get("avg_cycle_ms") is None else float(row.get("avg_cycle_ms")),
            "last_event": row.get("last_event"),
            "ppm": 0.0,
        }

    for row in qual_rows:
        machine_id = row["machine_id"]
        current = merged.get(machine_id) or {
            "machine_id": machine_id,
            "pieces": 0,
            "ok": 0,
            "nok": 0,
            "yield_pct": None,
            "avg_cycle_ms": None,
            "last_event": None,
            "ppm": 0.0,
        }
        current["ok"] = int(row.get("ok_count") or 0)
        current["nok"] = int(row.get("nok_count") or 0)
        merged[machine_id] = current

    out = []
    for machine_id, row in sorted(merged.items()):
        q_total = row["ok"] + row["nok"]
        row["yield_pct"] = round((row["ok"] * 100.0 / q_total), 1) if q_total > 0 else None
        minutes = float(report_lookback_hours) * 60.0
        row["ppm"] = round((row.get("pieces", 0) / minutes), 2) if minutes > 0 else 0.0
        last_event = row.get("last_event")
        if last_event is not None:
            row["last_event"] = last_event.strftime("%Y-%m-%d %H:%M:%S") if hasattr(last_event, "strftime") else str(last_event)
        out.append(row)

    return out
