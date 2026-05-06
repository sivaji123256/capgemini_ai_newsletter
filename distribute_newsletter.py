"""
Distribute generated newsletter issues to recipients and log deliveries.

Usage examples:
    .venv\\Scripts\\python.exe distribute_newsletter.py --dry-run
    .venv\\Scripts\\python.exe distribute_newsletter.py --html newsletter_output\\ai_business_newsletter_20260506_155930_refined.html --dry-run
    .venv\\Scripts\\python.exe distribute_newsletter.py --send

Environment variables for Microsoft Graph sending:
    DATABASE_URL=postgresql://...
    NEWSLETTER_FROM_EMAIL=sender@company.com
    NEWSLETTER_FROM_NAME=Capgemini Weekly AI Pulse
    GRAPH_TENANT_ID=your-entra-tenant-id
    GRAPH_CLIENT_ID=your-app-registration-client-id
    GRAPH_CLIENT_SECRET=your-app-registration-client-secret

Optional SMTP fallback:
    DATABASE_URL=postgresql://...
    NEWSLETTER_FROM_EMAIL=sender@example.com
    NEWSLETTER_FROM_NAME=Capgemini Weekly AI Pulse
    SMTP_HOST=smtp.example.com
    SMTP_PORT=587
    SMTP_USERNAME=sender@example.com
    SMTP_PASSWORD=app-password-or-smtp-password
    SMTP_USE_TLS=true
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import smtplib
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import parse as urlparse
from urllib import request as urlrequest

import psycopg
from psycopg.rows import dict_row
from bs4 import BeautifulSoup

from collect_ai_news_to_postgres import load_local_env


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "newsletter_output"

DEFAULT_RECIPIENTS = [
    {"email": "sivajiretta171@gmail.com", "name": "Sivaji Retta", "organization": "Personal", "segment": "primary"},
    {"email": "sivaji.retta@capgemini.com", "name": "Sivaji Retta", "organization": "Capgemini", "segment": "primary"},
]


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS newsletter_issues (
    id BIGSERIAL PRIMARY KEY,
    issue_slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    issue_date DATE NOT NULL,
    html_path TEXT NOT NULL,
    html_content TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS newsletter_recipients (
    id BIGSERIAL PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    name TEXT,
    organization TEXT,
    segment TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_sent_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS newsletter_deliveries (
    id BIGSERIAL PRIMARY KEY,
    issue_id BIGINT NOT NULL REFERENCES newsletter_issues(id) ON DELETE CASCADE,
    recipient_id BIGINT NOT NULL REFERENCES newsletter_recipients(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    status TEXT NOT NULL,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    provider_message_id TEXT,
    error_message TEXT,
    sent_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (issue_id, recipient_id, dry_run)
);

CREATE INDEX IF NOT EXISTS idx_newsletter_issues_issue_date
    ON newsletter_issues (issue_date DESC);

CREATE INDEX IF NOT EXISTS idx_newsletter_deliveries_issue_id
    ON newsletter_deliveries (issue_id);

CREATE INDEX IF NOT EXISTS idx_newsletter_deliveries_recipient_id
    ON newsletter_deliveries (recipient_id);
"""


def connect() -> psycopg.Connection:
    load_local_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not configured. Set it in .env or the environment.")
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Distribute generated AI newsletter issues.")
    parser.add_argument("--html", help="Path to newsletter HTML file. Defaults to latest refined or latest HTML.")
    parser.add_argument("--subject", help="Override email subject line.")
    parser.add_argument("--dry-run", action="store_true", help="Log the issue and deliveries without sending email.")
    parser.add_argument("--send", action="store_true", help="Send the email through configured SMTP.")
    parser.add_argument("--approve", action="store_true", help="Mark the issue approved before delivery.")
    parser.add_argument(
        "--provider",
        choices=("graph", "smtp"),
        default="graph",
        help="Email provider to use for delivery. Defaults to Microsoft Graph.",
    )
    return parser.parse_args()


def latest_newsletter_html() -> Path:
    refined = sorted(OUTPUT_DIR.glob("*_refined.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    if refined:
        return refined[0]
    html_files = sorted(OUTPUT_DIR.glob("*.html"), key=lambda p: p.stat().st_mtime, reverse=True)
    if not html_files:
        raise SystemExit("No newsletter HTML files found in newsletter_output.")
    return html_files[0]


def load_html(path_arg: str | None) -> tuple[Path, str]:
    path = Path(path_arg).resolve() if path_arg else latest_newsletter_html()
    if not path.exists():
        raise SystemExit(f"Newsletter HTML file not found: {path}")
    return path, path.read_text(encoding="utf-8")


def text_of(node: Any) -> str:
    if not node:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())


def escape_html(text: str) -> str:
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def inner_html(node: Any) -> str:
    if not node:
        return ""
    return "".join(str(child) for child in node.contents)


def trim_sentences(text: str, max_sentences: int = 2) -> str:
    text = " ".join(text.split())
    if not text:
        return ""
    parts = [part.strip() for part in __import__("re").split(r"(?<=[.!?])\s+", text) if part.strip()]
    if not parts:
        return text
    return " ".join(parts[:max_sentences])


def choose_story_brief(one_sentence: str, intro_parts: list[str]) -> str:
    one_sentence = " ".join(one_sentence.split())
    intro_text = " ".join(intro_parts[0].split()) if intro_parts else ""
    if intro_text:
        return trim_sentences(intro_text, 1)
    return trim_sentences(one_sentence, 1)


def normalized_compare(text: str) -> str:
    return " ".join(text.lower().split())


def render_insight_block(node: Any) -> str:
    label_node = node.find("strong")
    label = text_of(label_node).rstrip(":")

    list_items = [text_of(li) for li in node.find_all("li") if text_of(li)]
    paragraphs = []
    for part in node.contents:
        if getattr(part, "name", None) == "strong":
            continue
        if getattr(part, "name", None) == "ul":
            continue
        text = ""
        if hasattr(part, "get_text"):
            text = text_of(part)
        else:
            text = " ".join(str(part).split())
        if text:
            paragraphs.append(text)

    body_html = ""
    if paragraphs:
        body_html += "".join(
            f'<span>{escape_html(trim_sentences(text, 2))}</span>'
            for text in paragraphs[:1]
        )
    if list_items:
        shown_items = list_items[:2]
        if body_html:
            body_html += '<div style="height:4px;line-height:4px;font-size:0;">&nbsp;</div>'
        body_html += (
            '<ul style="margin:0;padding:0 0 0 18px;font-size:13px;line-height:1.55;color:#31465c;">'
            + "".join(f'<li style="margin:0 0 3px 0;">{escape_html(item)}</li>' for item in shown_items)
            + "</ul>"
        )

    if not body_html:
        body_html = '<span>-</span>'

    label_color = "#4c647d"
    if "recommended action" in label.lower():
        label_color = "#1f4f7a"
    elif "risk" in label.lower():
        label_color = "#6f572d"
    return f"""
    <tr>
      <td style="padding:0 0 4px 0;">
        <div style="font-size:13px;line-height:1.58;color:#31465c;">
          <span style="font-weight:700;color:{label_color};">{escape_html(label)}:</span>
          <span> </span>
          {body_html}
        </div>
      </td>
    </tr>
    """


def render_email_safe_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    page_title = text_of(soup.find("h1")) or "Capgemini Weekly AI Pulse"
    issue_time = text_of(soup.find("time"))

    exec_brief_items = [
        text_of(li)
        for li in soup.select(".exec-brief li")
        if text_of(li)
    ]

    section_blocks: list[str] = []
    for section in soup.select("section.editorial-section"):
        section_title = text_of(section.find("h2"))
        story_blocks: list[str] = []
        for story in section.select("article.story-card"):
            title_node = story.select_one(".story-title")
            badge_node = story.select_one(".priority-badge")
            one_sentence = text_of(story.select_one(".story-one-sentence"))
            details = story.select_one(".story-details")

            source_links: list[str] = []
            why_block = ""
            risk_block = ""
            action_block = ""
            intro_parts: list[str] = []

            if details:
                for child in details.children:
                    if getattr(child, "name", None) == "summary":
                        continue
                    if getattr(child, "name", None) == "p":
                        links = child.find_all("a")
                        if links and text_of(child).lower().startswith("source:"):
                            for link in links:
                                href = link.get("href", "").strip()
                                label = (text_of(link) or href).removeprefix("Source: ").strip()
                                if href:
                                    source_links.append(
                                        f'<a href="{escape_html(href)}" style="color:#0a66c2;text-decoration:none;">{escape_html(label)}</a>'
                                    )
                        else:
                            content = text_of(child).strip()
                            if content:
                                intro_parts.append(content)
                    elif getattr(child, "name", None) == "div" and "insights-block" in (child.get("class") or []):
                        label = text_of(child.find("strong")).rstrip(":").lower()
                        block_html = render_insight_block(child)
                        if "why it matters" in label and not why_block:
                            why_block = block_html
                        elif "recommended action" in label and not action_block:
                            action_block = block_html
                        elif "risk" in label and not risk_block:
                            risk_block = block_html

            title_link = title_node.find("a") if title_node else None
            title_href = title_link.get("href", "").strip() if title_link else ""
            title_text = text_of(title_node)
            badge_text = text_of(badge_node)

            source_html = ""
            if source_links:
                joined = " | ".join(source_links)
                source_html = f"""
                <tr>
                  <td style="padding:6px 0 0 0;font-size:13px;line-height:1.5;color:#5a6b7d;">
                    <strong style="color:#183b63;">Sources:</strong> {joined}
                  </td>
                </tr>
                """

            badge_html = ""
            if badge_text:
                badge_color = "#0a66c2"
                if "medium" in badge_text.lower():
                    badge_color = "#5f7ea3"
                elif "low" in badge_text.lower():
                    badge_color = "#89aecd"
                badge_html = (
                    '<table role="presentation" cellspacing="0" cellpadding="0" border="0" align="right">'
                    f'<tr><td bgcolor="{badge_color}" style="background:{badge_color};color:#ffffff;'
                    'font-size:12px;font-weight:700;line-height:1;padding:8px 12px;white-space:nowrap;">'
                    f"{escape_html(badge_text)}</td></tr></table>"
                )

            title_html = escape_html(title_text)
            if title_href:
                title_html = (
                    f'<a href="{escape_html(title_href)}" style="color:#0b3d78;text-decoration:none;">'
                    f"{escape_html(title_text)}</a>"
                )

            story_brief = choose_story_brief(one_sentence, intro_parts)
            combined_parts: list[str] = []
            first_line = trim_sentences(one_sentence, 1)
            if first_line:
                combined_parts.append(first_line)
            if story_brief and normalized_compare(story_brief) != normalized_compare(first_line):
                combined_parts.append(story_brief)
            combined_copy = " ".join(part.strip() for part in combined_parts if part.strip())
            combined_html = ""
            if combined_copy:
                combined_html = (
                    f'<div style="font-size:14px;line-height:1.6;color:#2a3d52;text-align:left;padding:0 0 10px 0;">'
                    f"{escape_html(combined_copy)}"
                    "</div>"
                )

            story_sections = why_block
            if action_block:
                story_sections += action_block
            if risk_block:
                story_sections += risk_block

            story_blocks.append(
                f"""
                <tr>
                  <td style="padding:0 0 20px 0;">
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0"
                      style="width:100%;background:#ffffff;border:1px solid #dbe5ef;">
                      <tr>
                        <td style="padding:0 28px;">
                          <table role="presentation" cellspacing="0" cellpadding="0" border="0" style="width:96px;">
                            <tr><td height="4" bgcolor="#0a66c2" style="height:4px;font-size:0;line-height:0;">&nbsp;</td></tr>
                          </table>
                        </td>
                      </tr>
                      <tr>
                        <td style="padding:22px 28px 24px 28px;">
                          <div style="padding:0 0 8px 0;">{badge_html}</div>
                          <div style="font-size:23px;line-height:1.3;font-weight:700;color:#0b3d78;text-align:left;padding:0 0 10px 0;">
                            {title_html}
                          </div>
                          {combined_html}
                          <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                            {story_sections}
                            {source_html}
                          </table>
                        </td>
                      </tr>
                    </table>
                  </td>
                </tr>
                """
            )

        section_blocks.append(
            f"""
            <tr>
              <td style="padding:0 0 24px 0;">
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;margin:0 0 18px 0;">
                  <tr>
                    <td bgcolor="#0b3d78" style="background:#0b3d78;padding:12px 16px;">
                      <div style="font-size:22px;line-height:1.25;font-weight:700;color:#ffffff;">
                        {escape_html(section_title)}
                      </div>
                    </td>
                  </tr>
                </table>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                  {''.join(story_blocks)}
                </table>
              </td>
            </tr>
            """
        )

    boardroom_items = [text_of(li) for li in soup.select(".boardroom-questions li") if text_of(li)]
    boardroom_html = ""
    if boardroom_items:
        boardroom_items = boardroom_items[:4]
        items = "".join(
            f'<li style="margin:0 0 12px 0;">{escape_html(item)}</li>'
            for item in boardroom_items
        )
        boardroom_html = f"""
        <tr>
          <td style="padding:0 0 8px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
              <tr>
                <td bgcolor="#0b3d78" style="background:#0b3d78;padding:12px 16px;">
                  <div style="font-size:21px;line-height:1.25;font-weight:700;color:#ffffff;">Boardroom Questions</div>
                </td>
              </tr>
              <tr>
                <td bgcolor="#eef4fb" style="background:#eef4fb;border:1px solid #cfdaea;padding:14px 20px;">
                  <ol style="margin:0;padding-left:22px;font-size:14px;line-height:1.6;color:#27415d;">
                    {items}
                  </ol>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    brief_html = ""
    if exec_brief_items:
        items = "".join(
            f'<li style="margin:0 0 10px 0;">{escape_html(item)}</li>'
            for item in exec_brief_items
        )
        brief_html = f"""
        <tr>
          <td style="padding:0 0 28px 0;">
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
              <tr>
                <td bgcolor="#0a66c2" style="background:#0a66c2;padding:12px 16px;">
                  <div style="font-size:21px;line-height:1.25;font-weight:700;color:#ffffff;">Executive Brief</div>
                </td>
              </tr>
              <tr>
                <td bgcolor="#eef6ff" style="background:#eef6ff;border:1px solid #cfe0f3;padding:14px 22px;">
                  <ul style="margin:0;padding-left:20px;font-size:14px;line-height:1.6;color:#174d82;">
                    {items}
                  </ul>
                </td>
              </tr>
            </table>
          </td>
        </tr>
        """

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{escape_html(page_title)}</title>
</head>
<body style="margin:0;padding:0;background:#e9eef5;font-family:Segoe UI,Arial,sans-serif;color:#1f2d3d;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;background:#e9eef5;">
    <tr>
      <td align="center" style="padding:20px 12px 28px 12px;">
        <table role="presentation" width="760" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:760px;max-width:760px;background:#ffffff;border:1px solid #ccd7e3;">
          <tr>
            <td bgcolor="#0b2f57" style="padding:28px 28px 18px 28px;text-align:center;background:#0b2f57;">
              <div style="font-size:34px;line-height:1.2;font-weight:700;color:#ffffff;letter-spacing:0.02em;">{escape_html(page_title)}</div>
              <div style="padding-top:8px;font-size:14px;line-height:1.4;color:#d5e4f5;">{escape_html(issue_time)}</div>
            </td>
          </tr>
          <tr>
            <td style="padding:20px 22px 22px 22px;background:#ffffff;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                {brief_html}
                {''.join(section_blocks)}
                {boardroom_html}
              </table>
            </td>
          </tr>
        </table>
      </td>
    </tr>
  </table>
</body>
</html>
"""


def slug_from_path(path: Path) -> str:
    return path.stem.lower().replace(" ", "-")


def subject_from_html(html: str, fallback_date: dt.date) -> str:
    marker = "<title>"
    end_marker = "</title>"
    start = html.find(marker)
    end = html.find(end_marker)
    if start != -1 and end != -1 and end > start:
        value = html[start + len(marker) : end].strip()
        if value:
            return value
    return f"Capgemini Weekly AI Pulse - {fallback_date.strftime('%B %d, %Y')}"


def email_variant_path(path: Path) -> Path:
    return path.with_name(f"{path.stem}_email.html")


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def upsert_default_recipients(conn: psycopg.Connection) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with conn.cursor() as cur:
        for recipient in DEFAULT_RECIPIENTS:
            cur.execute(
                """
                INSERT INTO newsletter_recipients (email, name, organization, segment, is_active)
                VALUES (%(email)s, %(name)s, %(organization)s, %(segment)s, TRUE)
                ON CONFLICT (email) DO UPDATE
                SET
                    name = EXCLUDED.name,
                    organization = EXCLUDED.organization,
                    segment = EXCLUDED.segment,
                    is_active = TRUE
                RETURNING id, email, name, organization, segment, is_active
                """,
                recipient,
            )
            rows.append(cur.fetchone())
    conn.commit()
    return rows


def upsert_issue(
    conn: psycopg.Connection,
    html_path: Path,
    html_content: str,
    subject: str,
    approve: bool,
) -> dict[str, Any]:
    slug = slug_from_path(html_path)
    issue_date = dt.date.today()
    approved_at = dt.datetime.now(dt.timezone.utc) if approve else None
    status = "approved" if approve else "draft"
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO newsletter_issues (
                issue_slug, title, issue_date, html_path, html_content, status, approved_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (issue_slug) DO UPDATE
            SET
                title = EXCLUDED.title,
                issue_date = EXCLUDED.issue_date,
                html_path = EXCLUDED.html_path,
                html_content = EXCLUDED.html_content,
                status = CASE
                    WHEN newsletter_issues.status = 'sent' THEN newsletter_issues.status
                    ELSE EXCLUDED.status
                END,
                approved_at = COALESCE(newsletter_issues.approved_at, EXCLUDED.approved_at)
            RETURNING *
            """,
            (slug, subject, issue_date, str(html_path), html_content, status, approved_at),
        )
        row = cur.fetchone()
    conn.commit()
    return row


def smtp_settings() -> dict[str, Any]:
    return {
        "host": os.environ.get("SMTP_HOST", "").strip(),
        "port": int(os.environ.get("SMTP_PORT", "587").strip() or "587"),
        "username": os.environ.get("SMTP_USERNAME", "").strip(),
        "password": os.environ.get("SMTP_PASSWORD", "").strip(),
        "use_tls": os.environ.get("SMTP_USE_TLS", "true").strip().lower() in {"1", "true", "yes", "y"},
        "from_email": os.environ.get("NEWSLETTER_FROM_EMAIL", "").strip(),
        "from_name": os.environ.get("NEWSLETTER_FROM_NAME", "Capgemini Weekly AI Pulse").strip(),
    }


def validate_send_config(settings: dict[str, Any]) -> None:
    missing = [key for key in ("host", "port", "username", "password", "from_email") if not settings.get(key)]
    if missing:
        raise SystemExit(
            "SMTP sending is not configured. Missing: "
            + ", ".join(missing)
            + ". Set SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD, and NEWSLETTER_FROM_EMAIL in .env."
        )


def graph_settings() -> dict[str, Any]:
    return {
        "tenant_id": os.environ.get("GRAPH_TENANT_ID", "").strip(),
        "client_id": os.environ.get("GRAPH_CLIENT_ID", "").strip(),
        "client_secret": os.environ.get("GRAPH_CLIENT_SECRET", "").strip(),
        "from_email": os.environ.get("NEWSLETTER_FROM_EMAIL", "").strip(),
        "from_name": os.environ.get("NEWSLETTER_FROM_NAME", "Capgemini Weekly AI Pulse").strip(),
    }


def validate_graph_config(settings: dict[str, Any]) -> None:
    missing = [key for key in ("tenant_id", "client_id", "client_secret", "from_email") if not settings.get(key)]
    if missing:
        raise SystemExit(
            "Microsoft Graph sending is not configured. Missing: "
            + ", ".join(missing)
            + ". Set GRAPH_TENANT_ID, GRAPH_CLIENT_ID, GRAPH_CLIENT_SECRET, and NEWSLETTER_FROM_EMAIL in .env."
        )


def graph_access_token(settings: dict[str, Any]) -> str:
    token_url = f"https://login.microsoftonline.com/{settings['tenant_id']}/oauth2/v2.0/token"
    body = urlparse.urlencode(
        {
            "client_id": settings["client_id"],
            "client_secret": settings["client_secret"],
            "scope": "https://graph.microsoft.com/.default",
            "grant_type": "client_credentials",
        }
    ).encode("utf-8")
    req = urlrequest.Request(
        token_url,
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"Microsoft Graph token request failed: HTTP {exc.code} {detail}") from exc
    except urlerror.URLError as exc:
        raise SystemExit(f"Microsoft Graph token request failed: {exc}") from exc

    import json

    data = json.loads(payload)
    token = data.get("access_token")
    if not token:
        raise SystemExit(f"Microsoft Graph token response missing access_token: {data}")
    return token


def graph_send_mail(subject: str, html: str, recipient_email: str, settings: dict[str, Any]) -> str | None:
    access_token = graph_access_token(settings)
    import json

    payload = {
        "message": {
            "subject": subject,
            "body": {
                "contentType": "HTML",
                "content": html,
            },
            "toRecipients": [
                {
                    "emailAddress": {
                        "address": recipient_email,
                    }
                }
            ],
        },
        "saveToSentItems": True,
    }
    sender = urlparse.quote(settings["from_email"])
    send_url = f"https://graph.microsoft.com/v1.0/users/{sender}/sendMail"
    req = urlrequest.Request(
        send_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlrequest.urlopen(req, timeout=60) as resp:
            request_id = resp.headers.get("request-id") or resp.headers.get("client-request-id")
            return request_id
    except urlerror.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Microsoft Graph sendMail failed: HTTP {exc.code} {detail}") from exc
    except urlerror.URLError as exc:
        raise RuntimeError(f"Microsoft Graph sendMail failed: {exc}") from exc


def build_message(subject: str, html: str, recipient_email: str, settings: dict[str, Any]) -> EmailMessage:
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = f"{settings['from_name']} <{settings['from_email']}>"
    msg["To"] = recipient_email
    msg.set_content("This newsletter contains HTML content. Please view it in an HTML-capable email client.")
    msg.add_alternative(html, subtype="html")
    return msg


def send_message(message: EmailMessage, settings: dict[str, Any]) -> str | None:
    with smtplib.SMTP(settings["host"], settings["port"], timeout=30) as server:
        if settings["use_tls"]:
            server.starttls()
        server.login(settings["username"], settings["password"])
        response = server.send_message(message)
    if response:
        return str(response)
    return None


def log_delivery(
    conn: psycopg.Connection,
    issue_id: int,
    recipient_id: int,
    provider: str,
    status: str,
    dry_run: bool,
    provider_message_id: str | None = None,
    error_message: str | None = None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO newsletter_deliveries (
                issue_id, recipient_id, provider, status, dry_run, provider_message_id, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (issue_id, recipient_id, dry_run) DO UPDATE
            SET
                provider = EXCLUDED.provider,
                status = EXCLUDED.status,
                provider_message_id = EXCLUDED.provider_message_id,
                error_message = EXCLUDED.error_message,
                sent_at = now()
            """,
            (issue_id, recipient_id, provider, status, dry_run, provider_message_id, error_message),
        )
        if status == "sent":
            cur.execute(
                "UPDATE newsletter_recipients SET last_sent_at = now() WHERE id = %s",
                (recipient_id,),
            )
        if status == "sent" and not dry_run:
            cur.execute(
                "UPDATE newsletter_issues SET status = 'sent', sent_at = COALESCE(sent_at, now()) WHERE id = %s",
                (issue_id,),
            )
    conn.commit()


def active_recipients(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, email, name, organization, segment
            FROM newsletter_recipients
            WHERE is_active = TRUE
            ORDER BY email
            """
        )
        return list(cur.fetchall())


def main() -> int:
    args = parse_args()
    if not args.dry_run and not args.send:
        args.dry_run = True

    with connect() as conn:
        ensure_schema(conn)
        upsert_default_recipients(conn)
        recipients = active_recipients(conn)

        html_path, source_html = load_html(args.html)
        email_html = render_email_safe_html(source_html)
        email_path = email_variant_path(html_path)
        email_path.write_text(email_html, encoding="utf-8")
        subject = args.subject or subject_from_html(source_html, dt.date.today())
        issue = upsert_issue(conn, email_path, email_html, subject, approve=args.approve)

        print(f"Issue ready: {issue['title']}")
        print(f"Source HTML: {html_path}")
        print(f"Email HTML: {email_path}")
        print(f"Recipients: {len(recipients)}")

        if args.dry_run:
            for recipient in recipients:
                log_delivery(
                    conn,
                    issue_id=issue["id"],
                    recipient_id=recipient["id"],
                    provider=args.provider,
                    status="dry_run",
                    dry_run=True,
                )
            print("Dry run complete. Delivery rows logged without sending email.")
            return 0

        if args.provider == "graph":
            settings = graph_settings()
            validate_graph_config(settings)
        else:
            settings = smtp_settings()
            validate_send_config(settings)

        sent = 0
        failed = 0
        for recipient in recipients:
            try:
                if args.provider == "graph":
                    provider_message_id = graph_send_mail(subject, email_html, recipient["email"], settings)
                else:
                    msg = build_message(subject, email_html, recipient["email"], settings)
                    provider_message_id = send_message(msg, settings)
                log_delivery(
                    conn,
                    issue_id=issue["id"],
                    recipient_id=recipient["id"],
                    provider=args.provider,
                    status="sent",
                    dry_run=False,
                    provider_message_id=provider_message_id,
                )
                sent += 1
                print(f"Sent: {recipient['email']}")
            except Exception as exc:
                failed += 1
                log_delivery(
                    conn,
                    issue_id=issue["id"],
                    recipient_id=recipient["id"],
                    provider="smtp",
                    status="failed",
                    dry_run=False,
                    error_message=str(exc),
                )
                print(f"Failed: {recipient['email']} -> {exc}")

        print(f"Delivery complete. Sent={sent}, Failed={failed}")
        return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
