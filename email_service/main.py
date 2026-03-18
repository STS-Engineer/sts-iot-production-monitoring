import argparse
import datetime
import logging
import signal
import sys
import threading
import time
from pathlib import Path

from jinja2 import Environment, FileSystemLoader, select_autoescape

if __package__:
    from .charts import defected_pieces_per_machine_bar_chart_base64, hourly_bar_chart_base64
    from .config import validate_config
    from .db import get_scheduler_state, get_system, get_system_recipients, get_system_report_profile, get_systems
    from .mailer import send_html_email
    from .pdf_report import build_pdf_bytes
    from .report import get_report_data
else:
    from charts import defected_pieces_per_machine_bar_chart_base64, hourly_bar_chart_base64
    from config import validate_config
    from db import get_scheduler_state, get_system, get_system_recipients, get_system_report_profile, get_systems
    from mailer import send_html_email
    from pdf_report import build_pdf_bytes
    from report import get_report_data

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def render_template(template_name: str, context: dict) -> str:
    templates_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    return env.get_template(template_name).render(**context)


def load_system_context(system_id: str) -> dict:
    system = get_system(system_id)
    if not system:
        raise RuntimeError(f"System not found: {system_id}")

    recipients = get_system_recipients(system_id)
    if not recipients:
        raise RuntimeError(f"No active recipients configured for system {system['name']}")

    report_profile = get_system_report_profile(system_id)
    scheduler_state = get_scheduler_state(system_id)

    return {
        "system": system,
        "recipients": recipients,
        "report_profile": report_profile,
        "scheduler_state": scheduler_state,
    }


def send_report_for_system(system_id: str, trigger_type: str = "manual") -> dict:
    validate_config()
    ctx = load_system_context(system_id)
    system = ctx["system"]
    recipients = ctx["recipients"]
    report_profile = ctx["report_profile"]

    data = get_report_data(system_id, report_profile)
    data["system_name"] = system["name"]
    data["system_code"] = system["code"]

    chart_b64 = hourly_bar_chart_base64(data["hourly_series"])
    data["chart_b64"] = chart_b64

    defected_chart_b64 = defected_pieces_per_machine_bar_chart_base64(data["rows"])
    data["defected_chart_b64"] = defected_chart_b64

    chart_bytes = None
    defected_chart_bytes = None
    if chart_b64:
        import base64
        chart_bytes = base64.b64decode(chart_b64)
        data["chart_cid"] = "chart_image"
    if defected_chart_b64:
        import base64
        defected_chart_bytes = base64.b64decode(defected_chart_b64)
        data["defected_chart_cid"] = "defected_chart_image"

    html = render_template(report_profile["template_name"], data)

    totals = data["totals"]
    text_lines = [
        f"System: {system['name']} ({system['code']})",
        f"Hourly report (last {data['hours']}h)",
        f"Generated at: {data['generated_at']}",
        f"Total pieces: {totals['total_pieces']}",
        f"OK: {totals['total_ok']} | NOK: {totals['total_nok']}",
        f"Quality rate: {data['yield_pct'] or 'N/A'}%",
        f"PPM: {data['ppm']}",
        "",
        "Machine details:",
    ]
    for row in data.get("rows", []):
        text_lines.append(
            f"{row['machine_id']}: pieces={row.get('pieces', 0)} ok={row.get('ok', 0)} nok={row.get('nok', 0)} "
            f"quality_rate={row.get('yield_pct') or 'N/A'}% ppm={row.get('ppm', 0.0)} "
            f"avg_cycle_ms={row.get('avg_cycle_ms') or 'N/A'} last_event={row.get('last_event') or 'N/A'}"
        )
    text = "\n".join(text_lines) + "\n"

    pdf_bytes = build_pdf_bytes(data, chart_b64) if report_profile.get("pdf_enabled", True) else None
    subject = f"[{system['code']}] Hourly Report (last {data['hours']}h) - {data['generated_at']}"

    inline_images = {}
    if chart_bytes:
        inline_images["chart_image"] = chart_bytes
    if defected_chart_bytes:
        inline_images["defected_chart_image"] = defected_chart_bytes

    send_html_email(
        recipients,
        subject,
        html,
        text_body=text,
        pdf_bytes=pdf_bytes,
        pdf_filename=f"{system['code'].lower()}_hourly_report.pdf",
        inline_images=inline_images if inline_images else None,
    )

    logger.info("Email sent for system %s to: %s", system["code"], ", ".join(recipients))

    return {
        "system_id": system_id,
        "system_name": system["name"],
        "recipient": ", ".join(recipients),
        "subject": subject,
        "template_used": report_profile["template_name"],
        "generated_at": data.get("generated_at"),
        "hours": data.get("hours"),
        "totals": data.get("totals"),
        "rows": data.get("rows"),
        "html": html,
        "text": text,
        "pdf_filename": f"{system['code'].lower()}_hourly_report.pdf",
        "pdf_bytes": pdf_bytes,
        "trigger_type": trigger_type,
    }


def run_scheduler(system_id: str, interval_minutes: int = 60, align_to_hour: bool = False) -> None:
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logger.info("Received signal %s - stopping scheduler...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if align_to_hour:
        now = datetime.datetime.now()
        secs = (60 - now.minute - 1) * 60 + (60 - now.second)
        if secs > 0:
            logger.info("Aligning to top of hour - waiting %s seconds", secs)
            stop_event.wait(secs)

    logger.info("Starting scheduler: system=%s interval=%sm align_to_hour=%s", system_id, interval_minutes, align_to_hour)
    while not stop_event.is_set():
        start_ts = time.time()
        try:
            send_report_for_system(system_id, trigger_type="scheduler")
        except Exception as exc:
            logger.exception("Scheduled run failed for %s", system_id)
            save_scheduler_state(
                system_id,
                enabled=True,
                interval_minutes=interval_minutes,
                running=False,
                last_status="failed",
                last_error=str(exc),
            )

        elapsed = time.time() - start_ts
        wait_sec = max(0, interval_minutes * 60 - elapsed)
        logger.info("Next run for %s in %s seconds", system_id, int(wait_sec))
        stop_event.wait(wait_sec)


def main() -> None:
    parser = argparse.ArgumentParser(description="Multi-system email report generator")
    parser.add_argument("--once", action="store_true", help="Run a single report and exit")
    parser.add_argument("--system-id", required=False, help="System id to execute")
    parser.add_argument("--interval", type=int, default=60, help="Interval in minutes between runs")
    parser.add_argument("--align", action="store_true", help="Align first run to the top of the hour")
    args = parser.parse_args()

    system_id = args.system_id
    if not system_id:
        systems = get_systems()
        if len(systems) != 1:
            raise RuntimeError("Please provide --system-id when multiple systems exist")
        system_id = str(systems[0]["id"])

    if args.once:
        send_report_for_system(system_id, trigger_type="manual")
    else:
        validate_config()
        run_scheduler(system_id=system_id, interval_minutes=args.interval, align_to_hour=args.align)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.exception("Failed: %s", exc)
        sys.exit(1)
