import sys
import logging
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

def main():
    validate_config()

    data = get_report_data()

    # chart image
    chart_b64 = hourly_bar_chart_base64(data["hourly_series"])
    data["chart_b64"] = chart_b64

    # html
    html = render_template("report.html", data)

    # plain text
    totals = data["totals"]
    text = (
        f"Hourly report (last {data['hours']}h)\n"
        f"Generated at: {data['generated_at']}\n"
        f"Total pieces: {totals['total_pieces']}\n"
        f"OK: {totals['total_ok']} | NOK: {totals['total_nok']}\n"
        f"Yield: {data['yield_pct'] or 'N/A'}%\n"
        f"PPM: {data['ppm']}\n"
    )

    # pdf attachment
    pdf_bytes = build_pdf_bytes(data, chart_b64)

    subject = f"[PM] Hourly Report (last {data['hours']}h) — {data['generated_at']}"
    send_html_email(
        EMAIL_TO, subject, html,
        text_body=text,
        pdf_bytes=pdf_bytes,
        pdf_filename="hourly_report.pdf",
    )

    logging.info("✅ Email sent to: %s", ", ".join(EMAIL_TO))

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception("❌ Failed: %s", e)
        sys.exit(1)
