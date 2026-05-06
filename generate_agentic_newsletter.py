"""
Generate a business-ready AI intelligence newsletter from PostgreSQL.

Agent flow:
    1. Summarizer agent: turns deduplicated weekly story clusters into
       business-oriented intelligence briefs.
    2. Critic agent: judges relevance, clarity, risk, and usefulness.
    3. Editor agent: produces a polished HTML newsletter with source links.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row

from collect_ai_news_to_postgres import load_local_env

try:
    from openai import OpenAI
except Exception:  # pragma: no cover
    OpenAI = None


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "newsletter_output"


AI_KEYWORDS = {
    "ai",
    "artificial",
    "intelligence",
    "llm",
    "model",
    "models",
    "agent",
    "agents",
    "rag",
    "machine",
    "learning",
    "neural",
    "openai",
    "anthropic",
    "claude",
    "gemini",
    "nvidia",
    "gpu",
    "inference",
    "training",
    "robot",
    "automation",
    "benchmark",
    "dataset",
}


def connect() -> psycopg.Connection:
    load_local_env()
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is not configured. Set it in .env or the environment.")
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10)


def keyword_relevance(title: str, summary: str) -> float:
    text = re.sub(r"[^a-z0-9 ]+", " ", f"{title} {summary}".lower())
    tokens = set(text.split())
    hits = len(tokens & AI_KEYWORDS)
    return min(1.0, hits / 3)


def fetch_weekly_clusters(conn: psycopg.Connection, days: int, limit: int) -> list[dict[str, Any]]:
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=days)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                sc.id,
                sc.canonical_title,
                sc.canonical_summary,
                sc.event_at_ts,
                sc.freshness_status,
                sc.primary_source,
                sc.source_count,
                sc.item_count,
                sc.is_ai_relevant,
                sc.ai_relevance_score,
                sc.ai_relevance_reason,
                array_agg(
                    DISTINCT jsonb_build_object(
                        'source', ani.source_name,
                        'title', ani.title,
                        'url', ani.url,
                        'published_at', ani.published_at
                    )
                ) AS sources
            FROM story_clusters sc
            JOIN story_cluster_items sci ON sci.cluster_id = sc.id
            JOIN ai_news_items ani ON ani.id = sci.item_id
            WHERE ani.published_at_ts >= %s
              AND COALESCE(sc.freshness_status, ani.freshness_status, 'current_week_event') IN
                    ('current_week_event', 'unknown_event_date')
            GROUP BY sc.id
            ORDER BY
                COALESCE(sc.ai_relevance_score, 0) DESC,
                sc.source_count DESC,
                sc.item_count DESC,
                sc.event_at_ts DESC NULLS LAST,
                sc.updated_at DESC
            LIMIT %s
            """,
            (cutoff, limit * 3),
        )
        rows = list(cur.fetchall())

    filtered: list[dict[str, Any]] = []
    for row in rows:
        score = row["ai_relevance_score"]
        if score is None:
            score = keyword_relevance(row["canonical_title"], row.get("canonical_summary") or "")
        if row["is_ai_relevant"] is False and score < 0.55:
            continue
        if score < 0.25:
            continue
        row["computed_ai_relevance_score"] = float(score)
        filtered.append(row)
        if len(filtered) >= limit:
            break
    return filtered


def call_json_agent(client: OpenAI, model: str, system: str, payload: dict[str, Any]) -> dict[str, Any]:
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": system},
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
        ],
    )
    text = response.output_text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return json.loads(text)


def summarizer_agent(client: OpenAI, model: str, clusters: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "task": "Summarize this week's deduplicated AI news for business leaders.",
        "requirements": [
            "Do not simply restate headlines.",
            "Explain business meaning: adoption, cost, risk, competitive impact, operating model impact.",
            "Keep each story concise but substantive.",
            "Return JSON only.",
        ],
        "clusters": clusters,
        "output_schema": {
            "briefs": [
                {
                    "cluster_id": 1,
                    "headline": "business-focused headline",
                    "one_sentence": "one sentence summary",
                    "deep_summary": "4-6 sentence explanation",
                    "business_implications": ["implication"],
                    "risk_or_watchout": "risk",
                    "recommended_action": "action",
                    "audience": "CEO/CIO/CTO/Data leaders/etc",
                    "importance": "critical|high|medium|low",
                }
            ]
        },
    }
    return call_json_agent(
        client,
        model,
        "You are the Summarizer Agent for an executive AI intelligence newsletter. Return only valid JSON.",
        payload,
    )


def critic_agent(client: OpenAI, model: str, summaries: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "task": "Critique the summarizer output for business newsletter quality.",
        "evaluation_dimensions": [
            "Is the story relevant to businesses?",
            "Is the summary specific, useful, and non-generic?",
            "Does it avoid hype?",
            "Does it identify risk or action clearly?",
            "Should it be included, downgraded, merged, or removed?",
        ],
        "summaries": summaries,
        "output_schema": {
            "verdicts": [
                {
                    "cluster_id": 1,
                    "include": True,
                    "score": 0.0,
                    "critique": "focused critique",
                    "required_edits": ["edit"],
                    "business_relevance": "critical|high|medium|low|irrelevant",
                }
            ],
            "overall_editor_guidance": "guidance",
        },
    }
    return call_json_agent(
        client,
        model,
        "You are the Critic Agent. Be strict, practical, and business-focused. Return only valid JSON.",
        payload,
    )


def editor_agent(
    client: OpenAI,
    model: str,
    clusters: list[dict[str, Any]],
    summaries: dict[str, Any],
    critique: dict[str, Any],
) -> str:
    payload = {
        "task": "Create a polished production-ready weekly AI business intelligence newsletter.",
        "today_date": dt.datetime.now().strftime("%B %d, %Y"),
        "editorial_rules": [
            "Use the critic feedback to decide what to include.",
            "Brand the newsletter as 'Capgemini Weekly AI Pulse'.",
            "The main title must be exactly 'Capgemini Weekly AI Pulse'.",
            "The issue date should use today_date only. Do not show a date range such as May 4-10.",
            "Do not use generic headings like 'Weekly AI Business Intelligence Newsletter' or 'Business Newsletter'.",
            "Write for a mixed professional readership: consultants, corporate strategy teams, technology leaders, product teams, data/AI leaders, risk leaders, and operators.",
            "The writing must feel proprietary and editorially curated, not like generic Google-search news snippets.",
            "Translate each story into why it matters for enterprise transformation, client advisory, investment priorities, operating models, or technology roadmaps.",
            "Use a professional, concise, insight-led voice.",
            "Include source hyperlinks inside each story card only.",
            "Do not create a bottom source references section.",
            "Do not include target audience labels.",
            "Do not create a left-side panel, sidebar, sticky table of contents, or navigation rail.",
            "Make the HTML self-contained with CSS.",
            "Use interactive-feeling HTML/CSS patterns that work without JavaScript, such as details/summary expanders, hover states, badges, score chips, and reveal panels.",
            "No JavaScript and no external assets.",
            "Use tasteful inline visual elements: SVG-style icons, CSS mini charts, signal meters, infographic rows, priority badges, timeline chips, impact/risk matrices, and a compact weekly pulse dashboard.",
            "Replace any large KPI-card grid with a section titled 'AI Signal Radar'.",
            "The AI Signal Radar must include an Impact x Risk Matrix and compact executive signal cards underneath.",
            "Do not use oversized cards with huge blue headline text for this section.",
            "Each signal card should show category, headline, impact chip, risk chip, horizon chip, why it matters, business move, and source link.",
            "The matrix should classify stories into practical business quadrants such as High Impact / High Risk, High Impact / Lower Risk, Watchlist, and Emerging Bet.",
            "Make the first viewport feel like an executive infographic, not a standard text newsletter.",
            "Do not use decorative blobs or generic gradients as the main visual idea.",
            "Include a very short executive brief: maximum 3 bullets, each under 18 words.",
            "Include a signal dashboard, editorial sections, visual story cards, implications, watchouts, and boardroom questions.",
            "Avoid generic AI hype. Be specific and decision-oriented.",
            "Make it visually premium: careful spacing, refined color palette, elegant cards, strong hierarchy, and mobile-friendly layout.",
            "Use this weekly date framing, not monthly framing.",
        ],
        "clusters": clusters,
        "summaries": summaries,
        "critique": critique,
        "output": "Return only complete HTML document.",
    }
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "You are the Editor Agent for a premium AI intelligence newsletter. Return only HTML.",
            },
            {"role": "user", "content": json.dumps(payload, ensure_ascii=False, default=str)},
        ],
    )
    html = response.output_text.strip()
    html = re.sub(r"^```html\s*", "", html)
    html = re.sub(r"\s*```$", "", html)
    return html


def save_outputs(html: str, summaries: dict[str, Any], critique: dict[str, Any]) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = OUTPUT_DIR / f"ai_business_newsletter_{stamp}.html"
    json_path = OUTPUT_DIR / f"ai_business_newsletter_agents_{stamp}.json"
    html = normalize_html_text(html)
    html_path.write_text(html, encoding="utf-8")
    json_path.write_text(
        json.dumps({"summaries": summaries, "critique": critique}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    return {"html": html_path, "json": json_path}


def normalize_html_text(html: str) -> str:
    replacements = {
        "\u2013": "-",
        "\u2014": "-",
        "\u2018": "'",
        "\u2019": "'",
        "\u201c": '"',
        "\u201d": '"',
        "\u2026": "...",
        "\u00a0": " ",
    }
    for old, new in replacements.items():
        html = html.replace(old, new)
    html = remove_bottom_references(html)
    html = remove_sidebar_navigation(html)
    html = enforce_issue_branding(html)
    html = re.sub(r"(?i)<p>\s*<strong>\s*Target Audience\s*:?\s*</strong>.*?</p>", "", html, flags=re.S)
    html = re.sub(r"(?i)<[^>]*>\s*Target Audience\s*:?\s*</[^>]+>", "", html)
    return html


def enforce_issue_branding(html: str) -> str:
    today = dt.datetime.now().strftime("%B %d, %Y")
    html = re.sub(
        r"(?is)<title>.*?</title>",
        f"<title>Capgemini Weekly AI Pulse - {today}</title>",
        html,
        count=1,
    )
    html = re.sub(
        r"(?is)<h1[^>]*>.*?</h1>",
        '<h1>Capgemini Weekly AI Pulse</h1>',
        html,
        count=1,
    )
    html = re.sub(r"(?i)\bMay\s+\d{1,2}\s*[-–—]\s*\d{1,2},?\s*2026\b", today, html)
    html = re.sub(r"(?i)\bWeek of\s+May\s+\d{1,2},?\s*2026\b", today, html)
    html = re.sub(r"(?i)Weekly AI Business Intelligence Newsletter", "", html)
    html = re.sub(r"(?i)Weekly Business Newsletter", "", html)
    return html


def remove_bottom_references(html: str) -> str:
    patterns = [
        r"(?is)<section[^>]*>\s*<h2[^>]*>\s*(?:source references|references|sources)\s*</h2>.*?</section>",
        r"(?is)<footer[^>]*>.*?(?:source references|references|sources).*?</footer>",
    ]
    for pattern in patterns:
        html = re.sub(pattern, "", html)
    return html


def remove_sidebar_navigation(html: str) -> str:
    patterns = [
        r"(?is)<aside[^>]*>.*?</aside>",
        r"(?is)<nav[^>]*(?:toc|side|sidebar|rail)[^>]*>.*?</nav>",
        r"(?is)<div[^>]*(?:toc|side-panel|sidebar|nav-rail)[^>]*>.*?</div>",
    ]
    for pattern in patterns:
        html = re.sub(pattern, "", html)
    html = re.sub(r"(?is)\.sidebar\s*\{.*?\}", "", html)
    html = re.sub(r"(?is)\.toc\s*\{.*?\}", "", html)
    html = re.sub(r"(?is)\.side-panel\s*\{.*?\}", "", html)
    return html


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate an agentic AI business newsletter from PostgreSQL.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--limit", type=int, default=12)
    parser.add_argument("--model", default="gpt-4.1-mini")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_local_env()
    if OpenAI is None:
        raise SystemExit("OpenAI package is not installed.")
    if not os.environ.get("OPENAI_API_KEY"):
        raise SystemExit("OPENAI_API_KEY is not configured in .env or environment.")

    with connect() as conn:
        clusters = fetch_weekly_clusters(conn, days=args.days, limit=args.limit)
    if not clusters:
        raise SystemExit("No relevant current-week AI story clusters found.")

    print(f"Loaded {len(clusters)} story clusters for newsletter generation.")
    client = OpenAI()
    summaries = summarizer_agent(client, args.model, clusters)
    print(f"Summarizer agent produced {len(summaries.get('briefs', []))} briefs.")
    critique = critic_agent(client, args.model, summaries)
    print(f"Critic agent produced {len(critique.get('verdicts', []))} verdicts.")
    html = editor_agent(client, args.model, clusters, summaries, critique)
    paths = save_outputs(html, summaries, critique)
    print(f"HTML newsletter: {paths['html']}")
    print(f"Agent JSON: {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
