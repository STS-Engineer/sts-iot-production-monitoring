import sys
import logging
import argparse
import threading
import time
import signal
import datetime
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import validate_config, EMAIL_TO
from report import get_report_data
from charts import hourly_bar_chart_base64
from pdf_report import build_pdf_bytes
from mailer import send_html_email

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def render_template(template_name: str, context: dict) -> str:
    templates_dir = Path(__file__).parent / "templates"
    env = Environment(
        loader=FileSystemLoader(str(templates_dir)),
        autoescape=select_autoescape(["html", "xml"]),
    )
    tpl = env.get_template(template_name)
    return tpl.render(**context)


def send_report() -> None:
    """Generate the report and send one email (single run)."""
    validate_config()

    data = get_report_data()

    # chart image
    chart_b64 = hourly_bar_chart_base64(data["hourly_series"])
    data["chart_b64"] = chart_b64

    # defected pieces chart
    from charts import defected_pieces_per_machine_bar_chart_base64
    defected_chart_b64 = defected_pieces_per_machine_bar_chart_base64(data["rows"])
    data["defected_chart_b64"] = defected_chart_b64
    defected_chart_bytes = None
    defected_chart_cid = None
    if defected_chart_b64:
        import base64
        try:
            defected_chart_bytes = base64.b64decode(defected_chart_b64)
            defected_chart_cid = "defected_chart_image"
            data["defected_chart_cid"] = defected_chart_cid
        except Exception:
            defected_chart_bytes = None
            defected_chart_cid = None

    # prefer inline CID image for desktop email clients (Outlook doesn't support data: URIs)
    chart_bytes = None
    chart_cid = None
    if chart_b64:
        import base64
        try:
            chart_bytes = base64.b64decode(chart_b64)
            chart_cid = "chart_image"
            data["chart_cid"] = chart_cid
        except Exception:
            chart_bytes = None
            chart_cid = None

    # html
    html = render_template("report.html", data)

    # plain text
    totals = data["totals"]
    text_lines = [
        f"Hourly report (last {data['hours']}h)",
        f"Generated at: {data['generated_at']}",
        f"Total pieces: {totals['total_pieces']}",
        f"OK: {totals['total_ok']} | NOK: {totals['total_nok']}",
        f"Quality rate: {data['yield_pct'] or 'N/A'}%",
        f"PPM: {data['ppm']}",
        "",
        "Machine details:",
    ]

    for r in data.get('rows', []):
        text_lines.append(
            f"{r['machine_id']}: pieces={r.get('pieces',0)} ok={r.get('ok',0)} nok={r.get('nok',0)} "
            f"quality_rate={r.get('yield_pct') or 'N/A'}% ppm={r.get('ppm',0.0)} avg_cycle_ms={r.get('avg_cycle_ms') or 'N/A'} last_event={r.get('last_event') or 'N/A'}"
        )

    text = "\n".join(text_lines) + "\n"

    # pdf attachment
    pdf_bytes = build_pdf_bytes(data, chart_b64)

    subject = f"[PM] Hourly Report (last {data['hours']}h) — {data['generated_at']}"
    inline_images = {}
    if chart_bytes:
        inline_images["chart_image"] = chart_bytes
    if defected_chart_bytes:
        inline_images["defected_chart_image"] = defected_chart_bytes
    send_html_email(
        EMAIL_TO,
        subject,
        html,
        text_body=text,
        pdf_bytes=pdf_bytes,
        pdf_filename="hourly_report.pdf",
        inline_images=inline_images if inline_images else None,
    )

    logging.info("✅ Email sent to: %s", ", ".join(EMAIL_TO))


def run_scheduler(interval_minutes: int = 60, align_to_hour: bool = False) -> None:
    """Run send_report() repeatedly every `interval_minutes` until terminated.

    - align_to_hour: if True wait until the next top-of-hour before first run.
    - graceful shutdown on SIGINT/SIGTERM.
    """
    stop_event = threading.Event()

    def _handle_signal(signum, frame):
        logging.info("Received signal %s — stopping scheduler...", signum)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    if align_to_hour:
        now = datetime.datetime.now()
        secs = (60 - now.minute - 1) * 60 + (60 - now.second)
        if secs > 0:
            logging.info("Aligning to top of hour — waiting %s seconds", secs)
            stop_event.wait(secs)

    logging.info("Starting scheduler: interval=%sm, align_to_hour=%s", interval_minutes, align_to_hour)
    while not stop_event.is_set():
        start_ts = time.time()
        try:
            send_report()
        except Exception:
            logging.exception("Scheduled run failed — will retry after interval")

        elapsed = time.time() - start_ts
        wait_sec = max(0, interval_minutes * 60 - elapsed)
        logging.info("Next run in %s seconds", int(wait_sec))
        stop_event.wait(wait_sec)

    logging.info("Scheduler stopped")


def main() -> None:
    parser = argparse.ArgumentParser(description="Email report generator — scheduler runs by default")
    parser.add_argument("--once", action="store_true", help="Run a single report and exit (opposite of default)")
    parser.add_argument("--interval", type=int, default=60, help="Interval in minutes between runs (default: 60)")
    parser.add_argument("--align", action="store_true", help="Align first run to the top of the hour")
    args = parser.parse_args()

    # Default behaviour: run the scheduler in-process so the app sends automatically every `interval` minutes.
    if args.once:
        send_report()
    else:
        validate_config()
        run_scheduler(interval_minutes=args.interval, align_to_hour=args.align)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("❌ Failed: %s", e)
        sys.exit(1)
