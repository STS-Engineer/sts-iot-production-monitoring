import smtplib
import ssl
import logging
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.image import MIMEImage

if __package__:
    from .config import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM
else:
    from config import SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, EMAIL_FROM

logger = logging.getLogger(__name__)

def send_html_email(
    to_list: list[str],
    subject: str,
    html_body: str,
    text_body: str | None = None,
    pdf_bytes: bytes | None = None,
    pdf_filename: str = "hourly_report.pdf",
    inline_images: dict[str, bytes] | None = None,
):
    """Send an HTML email with optional inline images (CID) and PDF attachment.

    inline_images: mapping from content-id (without <>) to raw bytes (PNG/JPEG). The template
    should reference them as <img src="cid:<id>">. This improves compatibility with Outlook.
    """
    msg = MIMEMultipart("mixed")
    msg["From"] = EMAIL_FROM
    msg["To"] = ", ".join(to_list)
    msg["Subject"] = subject

    # multipart/related -> contains the HTML + inline images
    related = MIMEMultipart("related")
    alt = MIMEMultipart("alternative")

    if not text_body:
        text_body = "Automated report. Please view in an HTML-capable email client."
    alt.attach(MIMEText(text_body, "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))
    related.attach(alt)

    # Attach inline images (CID)
    if inline_images:
        for cid, data in inline_images.items():
            if not data:
                continue
            try:
                img = MIMEImage(data)
                img.add_header("Content-ID", f"<{cid}>")
                img.add_header("Content-Disposition", "inline", filename=cid)
                related.attach(img)
            except Exception:
                logging.getLogger(__name__).exception("Failed to attach inline image %s", cid)

    # attach the related part to the main message
    msg.attach(related)

    if pdf_bytes:
        part = MIMEApplication(pdf_bytes, _subtype="pdf")
        part.add_header("Content-Disposition", "attachment", filename=pdf_filename)
        msg.attach(part)

    logger.info(f"Connecting to SMTP server: {SMTP_HOST}:{SMTP_PORT}")
    
    try:
        # Try with TLS (secure connection)
        _send_with_tls(msg, to_list)
        logger.info("Email sent successfully via TLS")
    except smtplib.SMTPNotSupportedError:
        # If TLS fails, try without authentication first
        logger.warning("TLS not supported, trying without authentication...")
        try:
            _send_without_auth(msg, to_list)
            logger.info("Email sent successfully without authentication")
        except Exception as e:
            logger.error(f"Failed to send without auth: {e}")
            raise
    except Exception as e:
        logger.error(f"SMTP error: {e}")
        raise


def _send_with_tls(msg, to_list):
    """Send email with TLS encryption and authentication."""
    context = ssl.create_default_context()
    
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def _send_without_auth(msg, to_list):
    """Send email without authentication (relay server)."""
    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=30) as server:
        server.ehlo()
        server.send_message(msg)

