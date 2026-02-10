import sys
import logging
from pathlib import Path
from jinja2 import Environment, FileSystemLoader, select_autoescape

from config import validate_config, EMAIL_TO
from report import get_report_data
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

def main():
    validate_config()

    data = get_report_data()
    subject = f"[PM] Hourly Report (last {data['hours']}h) — {data['generated_at']}"

    html = render_template("report.html", data)

    text = (
        f"Hourly report (last {data['hours']}h)\n"
        f"Generated at: {data['generated_at']}\n"
        f"Total pieces: {data['totals']['total_pieces']}\n"
        f"OK: {data['totals']['total_ok']} | NOK: {data['totals']['total_nok']}\n"
        f"Yield: {data['yield_pct']}%\n"
    )

    send_html_email(EMAIL_TO, subject, html, text_body=text)
    logging.info("✅ Email sent to: %s", ", ".join(EMAIL_TO))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("❌ Failed: %s", e)
        sys.exit(1)
