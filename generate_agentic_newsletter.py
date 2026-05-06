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


def escape_html(text: str) -> str:
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


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
                        'published_at', ani.published_at,
                        'source_summary', COALESCE(
                            ani.raw_item->>'summary',
                            ani.raw_item->>'description',
                            ani.raw_item->>'content',
                            ani.content_markdown,
                            ''
                        )
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


def pick_primary_source(cluster: dict[str, Any]) -> dict[str, Any]:
    sources = cluster.get("sources") or []
    primary_source_name = (cluster.get("primary_source") or "").strip().lower()
    for source in sources:
        if (source.get("source") or "").strip().lower() == primary_source_name:
            return source
    return sources[0] if sources else {}


def build_grounded_clusters(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grounded: list[dict[str, Any]] = []
    for cluster in clusters:
        primary = pick_primary_source(cluster)
        primary_title = (primary.get("title") or "").strip()
        primary_summary = " ".join((primary.get("source_summary") or "").split())
        canonical_title = (cluster.get("canonical_title") or "").strip()
        canonical_summary = " ".join((cluster.get("canonical_summary") or "").split())
        factual_headline = primary_title or canonical_title
        factual_summary = primary_summary or canonical_summary or factual_headline
        source_titles = []
        for source in cluster.get("sources") or []:
            title = " ".join((source.get("title") or "").split())
            if title and title not in source_titles:
                source_titles.append(title)
        grounded.append(
            {
                **cluster,
                "factual_headline": factual_headline,
                "factual_summary": factual_summary,
                "primary_url": (primary.get("url") or "").strip(),
                "primary_source_title": primary_title,
                "primary_source_name": (primary.get("source") or cluster.get("primary_source") or "").strip(),
                "primary_source_summary": primary_summary,
                "source_titles": source_titles[:5],
            }
        )
    return grounded


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
        "task": "Create business interpretation for this week's source-grounded AI news without changing the factual story.",
        "requirements": [
            "Use the provided factual_headline and factual_summary as the source of truth.",
            "Do not replace companies, model names, products, versions, numbers, or technical claims with different ones.",
            "Do not add facts not clearly supported by factual_headline, factual_summary, or source_titles.",
            "If evidence is ambiguous, stay conservative and say less.",
            "Focus on business meaning: adoption, cost, risk, competitive impact, and operating model impact.",
            "Keep each story concise and specific.",
            "Return JSON only.",
        ],
        "clusters": clusters,
        "output_schema": {
            "briefs": [
                {
                    "cluster_id": 1,
                    "executive_bullet": "short executive bullet under 18 words",
                    "why_it_matters": ["1-2 grounded bullets"],
                    "risk_or_watchout": "risk",
                    "recommended_action": "action",
                    "boardroom_question": "one boardroom-level question",
                    "importance": "high|medium|low",
                    "impact": "high|medium|low",
                    "risk_level": "high|medium|low",
                    "horizon": "now|mid|long",
                    "category": "short category label",
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
        "task": "Critique the interpretation output for business newsletter quality and factual discipline.",
        "evaluation_dimensions": [
            "Is the story relevant to businesses?",
            "Is the summary specific, useful, and non-generic?",
            "Does it stay within the facts provided by the source-grounded cluster input?",
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
                    "factuality_ok": True,
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


def render_html_newsletter(clusters: list[dict[str, Any]], summaries: dict[str, Any], critique: dict[str, Any]) -> str:
    today = dt.datetime.now().strftime("%B %d, %Y")
    brief_by_id = {item["cluster_id"]: item for item in summaries.get("briefs", []) if item.get("cluster_id") is not None}
    verdict_by_id = {item["cluster_id"]: item for item in critique.get("verdicts", []) if item.get("cluster_id") is not None}

    selected: list[dict[str, Any]] = []
    for cluster in clusters:
        cid = cluster["id"]
        brief = brief_by_id.get(cid)
        verdict = verdict_by_id.get(cid, {})
        if not brief:
            continue
        if verdict.get("include") is False or verdict.get("factuality_ok") is False:
            continue
        selected.append({"cluster": cluster, "brief": brief, "verdict": verdict})

    def section_name(item: dict[str, Any]) -> str:
        cluster = item["cluster"]
        brief = item["brief"]
        text = " ".join(
            [
                (cluster.get("factual_headline") or ""),
                (cluster.get("factual_summary") or ""),
                (brief.get("category") or ""),
            ]
        ).lower()
        if any(token in text for token in ["code", "coding", "developer", "open swe", "framework", "agent"]):
            return "Coding Agents and Governance"
        if any(token in text for token in ["security", "cyber", "hack", "regulatory", "liability", "compliance", "inference", "gemma", "qwen", "chrome"]):
            return "Inference Economics and Security"
        return "Operating Model and Edge Signals"

    section_order = [
        "Coding Agents and Governance",
        "Inference Economics and Security",
        "Operating Model and Edge Signals",
    ]
    grouped: dict[str, list[dict[str, Any]]] = {name: [] for name in section_order}
    for item in selected:
        grouped[section_name(item)].append(item)

    exec_items = [
        item["brief"].get("executive_bullet", "").strip()
        for item in selected[:3]
        if item["brief"].get("executive_bullet")
    ]
    exec_html = "".join(f"<li>{escape_html(text)}</li>" for text in exec_items)

    section_blocks: list[str] = []
    for section in section_order:
        stories_html: list[str] = []
        for item in grouped.get(section, []):
            cluster = item["cluster"]
            brief = item["brief"]
            importance = (brief.get("importance") or "high").strip().title()
            badge = f"{importance} Priority"
            headline = cluster.get("factual_headline") or cluster.get("canonical_title") or "AI Story"
            summary = cluster.get("factual_summary") or ""
            url = cluster.get("primary_url") or ""
            source_name = cluster.get("primary_source_name") or cluster.get("primary_source") or "Source"

            why_items = brief.get("why_it_matters") or []
            if isinstance(why_items, str):
                why_items = [why_items]
            why_html = "".join(f"<li>{escape_html(text)}</li>" for text in why_items[:2] if text)
            source_html = ""
            if url:
                source_html = f'<p class="source-line"><strong>Sources:</strong> <a href="{escape_html(url)}">{escape_html(source_name)}</a></p>'

            title_html = escape_html(headline)
            if url:
                title_html = f'<a href="{escape_html(url)}">{escape_html(headline)}</a>'

            stories_html.append(
                f"""
                <article class="story-card">
                  <div class="story-card-header">
                    <h3 class="story-title">{title_html}</h3>
                    <span class="priority-badge">{escape_html(badge)}</span>
                  </div>
                  <p class="story-one-sentence">{escape_html(summary)}</p>
                  <details class="story-details" open>
                    <summary>Read analysis and business implications</summary>
                    <div class="insights-block">
                      <strong>Why it matters:</strong>
                      <ul>{why_html}</ul>
                    </div>
                    <div class="insights-block">
                      <strong>Recommended action:</strong>
                      <p>{escape_html((brief.get("recommended_action") or "").strip())}</p>
                    </div>
                    <div class="insights-block">
                      <strong>Risk / Watchout:</strong>
                      <p>{escape_html((brief.get("risk_or_watchout") or "").strip())}</p>
                    </div>
                    {source_html}
                  </details>
                </article>
                """
            )
        if stories_html:
            section_blocks.append(
                f"""
                <section class="editorial-section">
                  <h2 class="section-title">{escape_html(section)}</h2>
                  <div class="stories">
                    {''.join(stories_html)}
                  </div>
                </section>
                """
            )

    boardroom_items = [
        item["brief"].get("boardroom_question", "").strip()
        for item in selected[:6]
        if item["brief"].get("boardroom_question")
    ]
    boardroom_html = "".join(f"<li>{escape_html(text)}</li>" for text in boardroom_items)

    guidance = escape_html((critique.get("overall_editor_guidance") or "").strip())
    guidance_html = f"<p class=\"editor-note\">{guidance}</p>" if guidance else ""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8" />
<meta name="viewport" content="width=device-width, initial-scale=1" />
<title>Capgemini Weekly AI Pulse - {today}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0;
    background: #f4f7fb;
    color: #1f2d3d;
    font-family: 'Segoe UI', Arial, sans-serif;
    line-height: 1.58;
  }}
  a {{ color: #0a66c2; text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  main {{
    max-width: 980px;
    margin: 0 auto;
    padding: 24px 18px 48px;
  }}
  header {{
    background: #fff;
    border: 1px solid #d9e3ef;
    border-radius: 8px;
    padding: 28px 30px;
    box-shadow: 0 10px 24px rgba(10, 39, 76, 0.06);
  }}
  h1 {{
    margin: 0;
    font-size: 2rem;
    color: #0b2f57;
  }}
  .issue-date {{
    margin-top: 8px;
    color: #61758a;
    font-size: 0.98rem;
  }}
  .exec-brief, .editorial-section, .boardroom-questions {{
    margin-top: 24px;
    background: #fff;
    border: 1px solid #d9e3ef;
    border-radius: 8px;
    padding: 24px 26px;
    box-shadow: 0 10px 24px rgba(10, 39, 76, 0.05);
  }}
  .section-title {{
    margin: 0 0 16px;
    font-size: 1.35rem;
    color: #0b3d78;
  }}
  .exec-brief ul, .boardroom-questions ol {{
    margin: 0;
    padding-left: 20px;
  }}
  .stories {{
    display: grid;
    gap: 18px;
  }}
  .story-card {{
    border-top: 1px solid #dbe5ef;
    padding-top: 18px;
  }}
  .story-card:first-child {{
    border-top: 0;
    padding-top: 0;
  }}
  .story-card-header {{
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 16px;
    align-items: start;
  }}
  .story-title {{
    margin: 0;
    font-size: 1.32rem;
    line-height: 1.35;
    color: #0b3d78;
  }}
  .priority-badge {{
    background: #0a66c2;
    color: #fff;
    padding: 8px 14px;
    border-radius: 999px;
    font-size: 0.86rem;
    font-weight: 700;
    white-space: nowrap;
  }}
  .story-one-sentence {{
    margin: 12px 0 0;
    font-size: 1rem;
    color: #24425f;
  }}
  .story-details {{
    margin-top: 10px;
  }}
  .story-details summary {{
    cursor: pointer;
    font-weight: 700;
    color: #0a66c2;
    margin-bottom: 10px;
  }}
  .insights-block {{
    margin-top: 10px;
  }}
  .insights-block strong {{
    color: #24425f;
  }}
  .insights-block p, .insights-block ul {{
    margin: 8px 0 0;
  }}
  .source-line {{
    margin: 12px 0 0;
    color: #516579;
  }}
  .editor-note {{
    margin: 0 0 16px;
    color: #5d6f82;
    font-size: 0.95rem;
  }}
  @media (max-width: 720px) {{
    .story-card-header {{ grid-template-columns: 1fr; }}
    .priority-badge {{ justify-self: start; }}
  }}
</style>
</head>
<body>
<main>
  <header>
    <h1>Capgemini Weekly AI Pulse</h1>
    <div class="issue-date">Issue Date: {today}</div>
  </header>

  <section class="exec-brief">
    <h2 class="section-title">Executive Brief</h2>
    <ul>{exec_html}</ul>
  </section>

  {guidance_html}
  {''.join(section_blocks)}

  <section class="boardroom-questions">
    <h2 class="section-title">Boardroom Questions</h2>
    <ol>{boardroom_html}</ol>
  </section>
</main>
</body>
</html>
"""


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
    grounded_clusters = build_grounded_clusters(clusters)

    print(f"Loaded {len(grounded_clusters)} story clusters for newsletter generation.")
    client = OpenAI()
    summaries = summarizer_agent(client, args.model, grounded_clusters)
    print(f"Summarizer agent produced {len(summaries.get('briefs', []))} briefs.")
    critique = critic_agent(client, args.model, summaries)
    print(f"Critic agent produced {len(critique.get('verdicts', []))} verdicts.")
    html = render_html_newsletter(grounded_clusters, summaries, critique)
    paths = save_outputs(html, summaries, critique)
    print(f"HTML newsletter: {paths['html']}")
    print(f"Agent JSON: {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
