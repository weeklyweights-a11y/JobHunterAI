"""Email delivery via Gmail SMTP (Phase 5)."""

from __future__ import annotations

import logging
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

_GMAIL_HOST = "smtp.gmail.com"
_GMAIL_PORT = 465

_SOURCE_ORDER = ("linkedin", "indeed", "yc", "career_page")
_SOURCE_LABELS = {
    "linkedin": "LinkedIn",
    "indeed": "Indeed",
    "yc": "YC",
    "career_page": "Career Pages",
}


def _build_body(n: int, by_source: dict[str, int]) -> str:
    lines = [
        f"JobHunter AI found {n} new jobs for you today!",
        "",
        "Breakdown:",
    ]
    for key in _SOURCE_ORDER:
        label = _SOURCE_LABELS[key]
        count = int(by_source.get(key, 0))
        lines.append(f"- {label}: {count} jobs")
    lines.extend(
        [
            "",
            "Open the attached Excel file to see all jobs with direct apply links.",
            "",
            "Happy hunting!",
        ]
    )
    return "\n".join(lines)


def send_jobs_report_sync(
    recipient: str,
    app_password: str,
    excel_path: Path,
    summary: dict[str, Any],
) -> None:
    """
    Send Gmail message with .xlsx attachment. Blocking — call via asyncio.to_thread.
    summary: total (int), by_source (dict str -> int) for new jobs this run only.
    """
    total = int(summary.get("total") or 0)
    by_src = summary.get("by_source") or {}
    if not isinstance(by_src, dict):
        by_src = {}
    by_src_int = {str(k): int(v) for k, v in by_src.items()}

    date_part = datetime.now().strftime("%Y-%m-%d")
    subject = f"JobHunter AI — {total} New Jobs Found ({date_part})"
    body = _build_body(total, by_src_int)

    msg = MIMEMultipart()
    msg["Subject"] = subject
    msg["From"] = recipient
    msg["To"] = recipient
    msg.attach(MIMEText(body, "plain", "utf-8"))

    path = Path(excel_path)
    with open(path, "rb") as f:
        part = MIMEBase(
            "application",
            "vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        part.set_payload(f.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=path.name,
    )
    msg.attach(part)

    with smtplib.SMTP_SSL(_GMAIL_HOST, _GMAIL_PORT) as smtp:
        smtp.login(recipient, app_password)
        smtp.sendmail(recipient, recipient, msg.as_string())

    logger.info("Sent jobs report email to %s", recipient)
