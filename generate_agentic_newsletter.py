"""
Generate a business-ready AI intelligence newsletter from PostgreSQL.

Agent flow:
    1. Source grounder: builds immutable factual fields from weekly clusters.
    2. Summarizer agent: adds business interpretation only.
    3. Critic agent: judges relevance, clarity, and factual discipline.
    4. Reviser agent: applies required edits where the critic found issues.
    5. Editor agent: composes newsletter structure without changing facts.
    6. Deterministic renderer: converts the structured issue into HTML.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

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

AGGREGATOR_SOURCES = {
    "hacker news",
    "reddit r/machinelearning",
    "reddit localllama",
    "reddit",
    "lobsters",
}

AGGREGATOR_DOMAINS = {
    "news.ycombinator.com",
    "reddit.com",
    "www.reddit.com",
}

BUSINESS_RELEVANCE_WEIGHT = {
    "critical": 4,
    "high": 3,
    "medium": 2,
    "low": 1,
    "irrelevant": 0,
}

IMPORTANCE_WEIGHT = {
    "high": 3,
    "medium": 2,
    "low": 1,
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


def normalize_ws(text: Any) -> str:
    return " ".join(str(text or "").split()).strip()


def normalize_compare(text: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", normalize_ws(text).lower()).strip()


def sentence_split(text: str) -> list[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[.!?])\s+", text)
    return [normalize_ws(part) for part in parts if normalize_ws(part)]


def strip_leading_title_from_summary(title: str, summary: str) -> str:
    clean_title = normalize_compare(title)
    clean_summary = normalize_ws(summary)
    if not clean_title or not clean_summary:
        return clean_summary
    summary_compare = normalize_compare(clean_summary)
    if summary_compare == clean_title:
        return ""
    title_words = normalize_ws(title)
    if clean_summary.lower().startswith(title_words.lower() + ":"):
        clean_summary = normalize_ws(clean_summary[len(title_words) + 1 :])
    if normalize_compare(clean_summary) == clean_title:
        return ""
    return clean_summary


def clean_source_summary(title: str, summary: str) -> str:
    clean_summary = strip_leading_title_from_summary(title, summary)
    if not clean_summary:
        return ""
    if len(clean_summary) > 900:
        clean_summary = clean_summary[:900].rsplit(" ", 1)[0].strip()
    sentences = sentence_split(clean_summary)
    if not sentences:
        return ""
    kept: list[str] = []
    seen: set[str] = set()
    title_compare = normalize_compare(title)
    for sentence in sentences:
        comp = normalize_compare(sentence)
        if not comp or comp in seen:
            continue
        if comp == title_compare:
            continue
        if len(comp.split()) < 6:
            continue
        seen.add(comp)
        kept.append(sentence)
        if len(kept) >= 2:
            break
    if kept:
        return " ".join(kept)
    return clean_summary


def source_domain(url: str) -> str:
    return urlparse(url or "").netloc.lower().replace("www.", "")


def is_aggregator_source(source_name: str, url: str) -> bool:
    return source_name.strip().lower() in AGGREGATOR_SOURCES or source_domain(url) in AGGREGATOR_DOMAINS


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


def choose_best_source_evidence(cluster: dict[str, Any]) -> tuple[dict[str, Any], str]:
    sources = cluster.get("sources") or []
    scored: list[tuple[int, dict[str, Any], str]] = []
    for source in sources:
        title = normalize_ws(source.get("title") or "")
        raw_summary = normalize_ws(source.get("source_summary") or "")
        cleaned_summary = clean_source_summary(title, raw_summary)
        source_name = normalize_ws(source.get("source") or "")
        url = normalize_ws(source.get("url") or "")
        aggregator_penalty = 20 if is_aggregator_source(source_name, url) else 0
        summary_score = min(len(cleaned_summary), 300)
        title_penalty = 80 if normalize_compare(cleaned_summary) == normalize_compare(title) else 0
        thin_penalty = 60 if len(cleaned_summary) < 70 else 0
        score = summary_score - aggregator_penalty - title_penalty - thin_penalty
        if normalize_compare(title) == normalize_compare(cluster.get("canonical_title") or ""):
            score += 5
        scored.append((score, source, cleaned_summary))
    if scored:
        scored.sort(key=lambda item: item[0], reverse=True)
        best_score, best_source, best_summary = scored[0]
        if best_score > 0:
            return best_source, best_summary
    primary = pick_primary_source(cluster)
    primary_title = normalize_ws(primary.get("title") or "")
    fallback_summary = clean_source_summary(primary_title, normalize_ws(primary.get("source_summary") or ""))
    return primary, fallback_summary


def build_grounded_clusters(clusters: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grounded: list[dict[str, Any]] = []
    for cluster in clusters:
        primary, best_summary = choose_best_source_evidence(cluster)
        primary_title = normalize_ws(primary.get("title") or "")
        primary_summary = normalize_ws(primary.get("source_summary") or "")
        canonical_title = normalize_ws(cluster.get("canonical_title") or "")
        canonical_summary = normalize_ws(cluster.get("canonical_summary") or "")
        factual_headline = primary_title or canonical_title
        factual_summary = (
            best_summary
            or clean_source_summary(primary_title, primary_summary)
            or clean_source_summary(canonical_title, canonical_summary)
            or canonical_summary
            or factual_headline
        )
        source_titles = []
        supporting_sources = []
        for source in cluster.get("sources") or []:
            title = normalize_ws(source.get("title") or "")
            if title and title not in source_titles:
                source_titles.append(title)
            source_name = normalize_ws(source.get("source") or "")
            source_url = normalize_ws(source.get("url") or "")
            if source_name and source_url:
                supporting_sources.append({"source": source_name, "url": source_url, "title": title})
        grounded.append(
            {
                **cluster,
                "factual_headline": factual_headline,
                "factual_summary": factual_summary,
                "primary_url": (primary.get("url") or "").strip(),
                "primary_source_title": primary_title,
                "primary_source_name": (primary.get("source") or cluster.get("primary_source") or "").strip(),
                "primary_source_summary": clean_source_summary(primary_title, primary_summary),
                "source_titles": source_titles[:5],
                "supporting_sources": supporting_sources[:5],
            }
        )
    return grounded


def grounder_agent(client: OpenAI, model: str, clusters: list[dict[str, Any]]) -> dict[str, Any]:
    payload = {
        "task": "Create a short factual lede for each source-grounded story using only the provided evidence.",
        "requirements": [
            "Preserve the factual meaning of the evidence.",
            "Do not change company names, product names, model names, versions, numbers, or technical claims.",
            "You may paraphrase wording for readability, but only if the meaning stays the same.",
            "If the evidence is thin, write one conservative sentence rather than embellishing.",
            "Do not add interpretation, business value, risk, or recommendation.",
            "Return JSON only.",
        ],
        "clusters": [
            {
                "cluster_id": cluster["id"],
                "factual_headline": cluster["factual_headline"],
                "factual_summary": cluster["factual_summary"],
                "primary_source_name": cluster["primary_source_name"],
                "source_titles": cluster.get("source_titles", []),
            }
            for cluster in clusters
        ],
        "output_schema": {
            "grounded_stories": [
                {
                    "cluster_id": 1,
                    "factual_lede": "One short factual paragraph grounded in the provided evidence.",
                }
            ]
        },
    }
    return call_json_agent(
        client,
        model,
        "You are the Source Grounder Agent for an executive AI newsletter. Produce only factual paraphrases grounded in the supplied evidence. Return only valid JSON.",
        payload,
    )


def source_label_from_cluster(cluster: dict[str, Any]) -> str:
    raw_name = (cluster.get("primary_source_name") or cluster.get("primary_source") or "").strip()
    url = (cluster.get("primary_url") or "").strip()
    if url:
        domain = urlparse(url).netloc.lower().replace("www.", "")
        mapping = {
            "towardsdatascience.com": "Towards Data Science",
            "langchain.com": "LangChain Blog",
            "developer.nvidia.com": "NVIDIA Technical Blog",
            "computing.co.uk": "Computing",
            "semafor.com": "Semafor",
            "marktechpost.com": "MarkTechPost",
            "rishgupta.com": "Rish Gupta",
            "technologyreview.com": "MIT Technology Review",
            "techcrunch.com": "TechCrunch",
            "zdnet.com": "ZDNet",
        }
        if domain in mapping:
            return mapping[domain]
        parts = [part.capitalize() for part in domain.split(".") if part not in {"com", "co", "uk", "org", "net", "io"}]
        if parts:
            return " ".join(parts)
    return raw_name or "Source"


def apply_grounded_ledes(clusters: list[dict[str, Any]], grounded_output: dict[str, Any]) -> list[dict[str, Any]]:
    lede_by_id = {
        item.get("cluster_id"): normalize_ws(item.get("factual_lede") or "")
        for item in grounded_output.get("grounded_stories", [])
        if item.get("cluster_id") is not None
    }
    updated: list[dict[str, Any]] = []
    for cluster in clusters:
        lede = lede_by_id.get(cluster["id"], "")
        if lede:
            cluster = {**cluster, "factual_summary": lede}
        updated.append(cluster)
    return updated


def merge_briefs(base: dict[str, Any], extra: dict[str, Any]) -> dict[str, Any]:
    merged: dict[int, dict[str, Any]] = {}
    for item in base.get("briefs", []):
        cid = item.get("cluster_id")
        if cid is not None:
            merged[cid] = item
    for item in extra.get("briefs", []):
        cid = item.get("cluster_id")
        if cid is not None:
            merged[cid] = item
    return {"briefs": [merged[cid] for cid in sorted(merged)]}


def section_name_for_item(cluster: dict[str, Any], brief: dict[str, Any]) -> str:
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
            "Do not rewrite the factual story text. The factual headline and factual summary are immutable and will be rendered separately.",
            "Create a newsletter_title that is sharper and more engaging than the source title while preserving the exact factual meaning.",
            "newsletter_title may improve phrasing, but must not change the company, model, product, version, number, or technical claim.",
            "Focus on business meaning: adoption, cost, risk, competitive impact, operating model impact, integration impact, and governance impact.",
            "Write from the perspective of a chief AI architect at Capgemini advising enterprise leaders.",
            "Recommended action must be a concrete enterprise move, not a generic suggestion.",
            "Risk / Watchout must describe the implementation, governance, data, security, operating model, or vendor risk if the story is acted on poorly or ignored.",
            "Avoid repeating the headline inside executive_bullet.",
            "Avoid generic consultant language. Be concrete, enterprise-focused, and concise.",
            "Return exactly one brief for every cluster_id provided.",
            "Keep each story concise and specific.",
            "Return JSON only.",
        ],
        "clusters": clusters,
        "output_schema": {
            "briefs": [
                {
                    "cluster_id": 1,
                    "newsletter_title": "improved but fact-preserving story title",
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
            "Is the newsletter_title sharper than the source title without changing the factual meaning?",
            "Does it stay within the facts provided by the source-grounded cluster input?",
            "Does it avoid hype?",
            "Does it identify enterprise risk or enterprise action clearly?",
            "Is recommended_action concrete enough for enterprise architecture, operating model, governance, or rollout planning?",
            "Is risk_or_watchout specific rather than generic?",
            "Should it be included, downgraded, or removed?",
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


def reviser_agent(
    client: OpenAI,
    model: str,
    clusters: list[dict[str, Any]],
    summaries: dict[str, Any],
    critique: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "task": "Revise only the interpretation fields for stories where the critic requested edits.",
        "requirements": [
            "Do not change cluster_id values.",
            "Do not rewrite factual_headline, factual_summary, companies, model names, versions, numbers, or technical claims.",
            "Revise only newsletter_title, executive_bullet, why_it_matters, risk_or_watchout, recommended_action, boardroom_question, importance, impact, risk_level, horizon, and category.",
            "Apply the critic required_edits precisely.",
            "If a story needs no edits, keep its existing interpretation unchanged.",
            "Keep the enterprise-architecture lens strong and avoid generic action or risk phrasing.",
            "Return JSON only.",
        ],
        "clusters": clusters,
        "current_briefs": summaries.get("briefs", []),
        "critic_verdicts": critique.get("verdicts", []),
        "output_schema": {
            "briefs": [
                {
                    "cluster_id": 1,
                    "newsletter_title": "improved but fact-preserving story title",
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
        "You are the Reviser Agent for an executive AI newsletter. Repair interpretation quality without changing facts. Return only valid JSON.",
        payload,
    )


def editor_agent(
    client: OpenAI,
    model: str,
    items: list[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "task": "Compose the newsletter structure using immutable factual stories and approved interpretations.",
        "requirements": [
            "Do not rewrite factual headlines, factual summaries, companies, model names, versions, or technical claims.",
            "You are selecting and arranging stories, not rewriting the story facts.",
            "Choose up to 3 executive brief items from the provided executive_bullet options.",
            "Assign each story to one of the provided section names.",
            "Preserve all cluster_ids exactly.",
            "Return JSON only.",
        ],
        "available_sections": [
            "Coding Agents and Governance",
            "Inference Economics and Security",
            "Operating Model and Edge Signals",
        ],
        "items": [
            {
                "cluster_id": item["cluster"]["id"],
                "factual_headline": item["cluster"]["factual_headline"],
                "factual_summary": item["cluster"]["factual_summary"],
                "newsletter_title": item["brief"].get("newsletter_title", ""),
                "source_label": item["cluster"]["source_label"],
                "executive_bullet": item["brief"].get("executive_bullet", ""),
                "category": item["brief"].get("category", ""),
                "importance": item["brief"].get("importance", ""),
                "critic_score": item["verdict"].get("score", 0),
                "business_relevance": item["verdict"].get("business_relevance", ""),
                "boardroom_question": item["brief"].get("boardroom_question", ""),
                "default_section": item["default_section"],
            }
            for item in items
        ],
        "output_schema": {
            "executive_cluster_ids": [1, 2, 3],
            "sections": [
                {
                    "name": "Coding Agents and Governance",
                    "story_cluster_ids": [1, 2],
                }
            ],
            "boardroom_cluster_ids": [1, 2, 3, 4],
        },
    }
    return call_json_agent(
        client,
        model,
        "You are the Editor Agent for an executive AI newsletter. Compose structure only. Return only valid JSON.",
        payload,
    )


def render_html_newsletter(
    items: list[dict[str, Any]],
    editor_plan: dict[str, Any],
) -> str:
    today = dt.datetime.now().strftime("%B %d, %Y")
    items_by_id = {item["cluster"]["id"]: item for item in items}
    section_order = [
        "Coding Agents and Governance",
        "Inference Economics and Security",
        "Operating Model and Edge Signals",
    ]
    exec_ids = [cid for cid in editor_plan.get("executive_cluster_ids", []) if cid in items_by_id][:3]
    if not exec_ids:
        exec_ids = [item["cluster"]["id"] for item in items[:3]]
    exec_html = "".join(
        f"<li>{escape_html(items_by_id[cid]['brief'].get('executive_bullet', ''))}</li>"
        for cid in exec_ids
        if items_by_id[cid]["brief"].get("executive_bullet")
    )

    section_blocks: list[str] = []
    editor_sections = {section.get("name"): section.get("story_cluster_ids", []) for section in editor_plan.get("sections", [])}
    for section in section_order:
        stories_html: list[str] = []
        story_ids = [cid for cid in editor_sections.get(section, []) if cid in items_by_id]
        default_ids = [item["cluster"]["id"] for item in items if item["default_section"] == section]
        for cid in default_ids:
            if cid not in story_ids:
                story_ids.append(cid)
        for cid in story_ids:
            item = items_by_id[cid]
            cluster = item["cluster"]
            brief = item["brief"]
            importance = (brief.get("importance") or "high").strip().title()
            badge = f"{importance} Priority"
            headline = (
                brief.get("newsletter_title")
                or cluster.get("factual_headline")
                or cluster.get("canonical_title")
                or "AI Story"
            )
            summary = " ".join(str(cluster.get("primary_source_summary") or "").split())
            if not summary:
                summary = " ".join(str(cluster.get("factual_summary") or "").split())
            if summary.strip().lower() == headline.strip().lower():
                summary = " ".join(str(cluster.get("canonical_summary") or "").split())
            if not summary:
                summary = " ".join(str(cluster.get("factual_summary") or "").split())
            url = cluster.get("primary_url") or ""
            source_name = cluster.get("source_label") or "Source"

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

    boardroom_ids = [cid for cid in editor_plan.get("boardroom_cluster_ids", []) if cid in items_by_id][:6]
    if not boardroom_ids:
        boardroom_ids = [item["cluster"]["id"] for item in items[:6]]
    boardroom_items = [
        items_by_id[cid]["brief"].get("boardroom_question", "").strip()
        for cid in boardroom_ids
        if items_by_id[cid]["brief"].get("boardroom_question")
    ]
    boardroom_html = "".join(f"<li>{escape_html(text)}</li>" for text in boardroom_items)

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

  {''.join(section_blocks)}

  <section class="boardroom-questions">
    <h2 class="section-title">Boardroom Questions</h2>
    <ol>{boardroom_html}</ol>
  </section>
</main>
</body>
</html>
"""


def render_email_newsletter(
    items: list[dict[str, Any]],
    editor_plan: dict[str, Any],
) -> str:
    today = dt.datetime.now().strftime("%B %d, %Y")
    items_by_id = {item["cluster"]["id"]: item for item in items}
    section_order = [
        "Coding Agents and Governance",
        "Inference Economics and Security",
        "Operating Model and Edge Signals",
    ]

    exec_ids = [cid for cid in editor_plan.get("executive_cluster_ids", []) if cid in items_by_id][:3]
    if not exec_ids:
        exec_ids = [item["cluster"]["id"] for item in items[:3]]
    exec_html = "".join(
        f'<li style="margin:0 0 10px 0;">{escape_html(items_by_id[cid]["brief"].get("executive_bullet", ""))}</li>'
        for cid in exec_ids
        if items_by_id[cid]["brief"].get("executive_bullet")
    )

    section_blocks: list[str] = []
    editor_sections = {section.get("name"): section.get("story_cluster_ids", []) for section in editor_plan.get("sections", [])}
    for section in section_order:
        story_ids = [cid for cid in editor_sections.get(section, []) if cid in items_by_id]
        default_ids = [item["cluster"]["id"] for item in items if item["default_section"] == section]
        for cid in default_ids:
            if cid not in story_ids:
                story_ids.append(cid)
        if not story_ids:
            continue

        story_blocks: list[str] = []
        for cid in story_ids:
            item = items_by_id[cid]
            cluster = item["cluster"]
            brief = item["brief"]
            importance = (brief.get("importance") or "high").strip().title()
            badge_color = "#0a66c2" if importance == "High" else "#5f7ea3" if importance == "Medium" else "#89aecd"
            badge = f"{importance} Priority"
            headline = (
                brief.get("newsletter_title")
                or cluster.get("factual_headline")
                or cluster.get("canonical_title")
                or "AI Story"
            )
            summary = " ".join(str(cluster.get("primary_source_summary") or "").split())
            if not summary:
                summary = " ".join(str(cluster.get("factual_summary") or "").split())
            if summary.strip().lower() == headline.strip().lower():
                summary = " ".join(str(cluster.get("canonical_summary") or "").split())
            if not summary:
                summary = " ".join(str(cluster.get("factual_summary") or "").split())
            url = cluster.get("primary_url") or ""
            source_name = cluster.get("source_label") or "Source"
            title_html = escape_html(headline)
            if url:
                title_html = f'<a href="{escape_html(url)}" style="color:#0b3d78;text-decoration:none;">{escape_html(headline)}</a>'

            why_items = brief.get("why_it_matters") or []
            if isinstance(why_items, str):
                why_items = [why_items]
            why_html = "".join(
                f'<li style="margin:0 0 3px 0;">{escape_html(text)}</li>' for text in why_items[:2] if text
            )
            source_html = ""
            if url:
                source_html = (
                    f'<div style="font-size:13px;line-height:1.5;color:#5a6b7d;padding-top:6px;">'
                    f'<strong style="color:#183b63;">Sources:</strong> '
                    f'<a href="{escape_html(url)}" style="color:#0a66c2;text-decoration:none;">{escape_html(source_name)}</a>'
                    "</div>"
                )

            story_blocks.append(
                f"""
                <tr>
                  <td style="padding:0 0 20px 0;">
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;background:#ffffff;border:1px solid #dbe5ef;">
                      <tr><td style="padding:0 28px;"><table role="presentation" cellspacing="0" cellpadding="0" border="0" style="width:96px;"><tr><td height="4" bgcolor="#0a66c2" style="height:4px;font-size:0;line-height:0;">&nbsp;</td></tr></table></td></tr>
                      <tr>
                        <td style="padding:22px 28px 24px 28px;">
                          <div style="padding:0 0 8px 0;"><table role="presentation" cellspacing="0" cellpadding="0" border="0" align="right"><tr><td bgcolor="{badge_color}" style="background:{badge_color};color:#ffffff;font-size:12px;font-weight:700;line-height:1;padding:8px 12px;white-space:nowrap;">{escape_html(badge)}</td></tr></table></div>
                          <div style="font-size:23px;line-height:1.3;font-weight:700;color:#0b3d78;text-align:left;padding:0 0 10px 0;">{title_html}</div>
                          <div style="font-size:14px;line-height:1.6;color:#2a3d52;text-align:left;padding:0 0 10px 0;">{escape_html(summary)}</div>
                          <div style="font-size:13px;line-height:1.58;color:#31465c;padding:0 0 4px 0;"><span style="font-weight:700;color:#4c647d;">Why it matters:</span><ul style="margin:0;padding:0 0 0 18px;font-size:13px;line-height:1.55;color:#31465c;">{why_html}</ul></div>
                          <div style="font-size:13px;line-height:1.58;color:#31465c;padding:0 0 4px 0;"><span style="font-weight:700;color:#1f4f7a;">Recommended action:</span> {escape_html((brief.get("recommended_action") or "").strip())}</div>
                          <div style="font-size:13px;line-height:1.58;color:#31465c;padding:0 0 4px 0;"><span style="font-weight:700;color:#6f572d;">Risk / Watchout:</span> {escape_html((brief.get("risk_or_watchout") or "").strip())}</div>
                          {source_html}
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
                  <tr><td bgcolor="#0b3d78" style="background:#0b3d78;padding:12px 16px;"><div style="font-size:22px;line-height:1.25;font-weight:700;color:#ffffff;">{escape_html(section)}</div></td></tr>
                </table>
                <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                  {''.join(story_blocks)}
                </table>
              </td>
            </tr>
            """
        )

    boardroom_ids = [cid for cid in editor_plan.get("boardroom_cluster_ids", []) if cid in items_by_id][:6]
    if not boardroom_ids:
        boardroom_ids = [item["cluster"]["id"] for item in items[:6]]
    boardroom_html = "".join(
        f'<li style="margin:0 0 12px 0;">{escape_html(items_by_id[cid]["brief"].get("boardroom_question", "").strip())}</li>'
        for cid in boardroom_ids
        if items_by_id[cid]["brief"].get("boardroom_question")
    )

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Capgemini Weekly AI Pulse</title>
</head>
<body style="margin:0;padding:0;background:#e9eef5;font-family:Segoe UI,Arial,sans-serif;color:#1f2d3d;">
  <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;background:#e9eef5;">
    <tr>
      <td align="center" style="padding:20px 12px 28px 12px;">
        <table role="presentation" width="760" cellspacing="0" cellpadding="0" border="0" bgcolor="#ffffff" style="width:760px;max-width:760px;background:#ffffff;border:1px solid #ccd7e3;">
          <tr>
            <td bgcolor="#0b2f57" style="padding:28px 28px 18px 28px;text-align:center;background:#0b2f57;">
              <div style="font-size:34px;line-height:1.2;font-weight:700;color:#ffffff;letter-spacing:0.02em;">Capgemini Weekly AI Pulse</div>
              <div style="padding-top:8px;font-size:14px;line-height:1.4;color:#d5e4f5;">Issue Date: {escape_html(today)}</div>
            </td>
          </tr>
          <tr>
            <td style="padding:20px 22px 22px 22px;background:#ffffff;">
              <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                <tr>
                  <td style="padding:0 0 28px 0;">
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                      <tr><td bgcolor="#0a66c2" style="background:#0a66c2;padding:12px 16px;"><div style="font-size:21px;line-height:1.25;font-weight:700;color:#ffffff;">Executive Brief</div></td></tr>
                      <tr><td bgcolor="#eef6ff" style="background:#eef6ff;border:1px solid #cfe0f3;padding:14px 22px;"><ul style="margin:0;padding-left:20px;font-size:14px;line-height:1.6;color:#174d82;">{exec_html}</ul></td></tr>
                    </table>
                  </td>
                </tr>
                {''.join(section_blocks)}
                <tr>
                  <td style="padding:0 0 8px 0;">
                    <table role="presentation" width="100%" cellspacing="0" cellpadding="0" border="0" style="width:100%;">
                      <tr><td bgcolor="#0b3d78" style="background:#0b3d78;padding:12px 16px;"><div style="font-size:21px;line-height:1.25;font-weight:700;color:#ffffff;">Boardroom Questions</div></td></tr>
                      <tr><td bgcolor="#eef4fb" style="background:#eef4fb;border:1px solid #cfdaea;padding:14px 20px;"><ol style="margin:0;padding-left:22px;font-size:14px;line-height:1.6;color:#27415d;">{boardroom_html}</ol></td></tr>
                    </table>
                  </td>
                </tr>
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


def save_outputs(
    html: str,
    email_html: str,
    grounded_output: dict[str, Any],
    summaries: dict[str, Any],
    critique: dict[str, Any],
    revised_summaries: dict[str, Any],
    editor_plan: dict[str, Any],
) -> dict[str, Path]:
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    html_path = OUTPUT_DIR / f"ai_business_newsletter_{stamp}.html"
    email_path = OUTPUT_DIR / f"ai_business_newsletter_{stamp}_email.html"
    json_path = OUTPUT_DIR / f"ai_business_newsletter_agents_{stamp}.json"
    html = normalize_html_text(html)
    email_html = normalize_html_text(email_html)
    html_path.write_text(html, encoding="utf-8")
    email_path.write_text(email_html, encoding="utf-8")
    json_path.write_text(
        json.dumps(
            {
                "grounded_output": grounded_output,
                "summaries": summaries,
                "critique": critique,
                "revised_summaries": revised_summaries,
                "editor_plan": editor_plan,
            },
            indent=2,
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return {"html": html_path, "email_html": email_path, "json": json_path}


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


def ensure_complete_summaries(client: OpenAI, model: str, clusters: list[dict[str, Any]], summaries: dict[str, Any]) -> dict[str, Any]:
    present_ids = {item.get("cluster_id") for item in summaries.get("briefs", []) if item.get("cluster_id") is not None}
    missing = [cluster for cluster in clusters if cluster["id"] not in present_ids]
    if not missing:
        return summaries
    extra = summarizer_agent(client, model, missing)
    return merge_briefs(summaries, extra)


def rank_selected_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
        cluster = item["cluster"]
        brief = item["brief"]
        verdict = item["verdict"]
        return (
            -BUSINESS_RELEVANCE_WEIGHT.get(str(verdict.get("business_relevance") or "").lower(), 0),
            -float(verdict.get("score") or 0.0),
            -IMPORTANCE_WEIGHT.get(str(brief.get("importance") or "").lower(), 0),
            -float(cluster.get("computed_ai_relevance_score") or 0.0),
            -int(cluster.get("source_count") or 0),
            int(cluster.get("id") or 0),
        )

    return sorted(items, key=sort_key)


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
    grounded_output = grounder_agent(client, args.model, grounded_clusters)
    grounded_clusters = apply_grounded_ledes(grounded_clusters, grounded_output)
    print(f"Grounder agent produced {len(grounded_output.get('grounded_stories', []))} factual ledes.")
    summaries = summarizer_agent(client, args.model, grounded_clusters)
    summaries = ensure_complete_summaries(client, args.model, grounded_clusters, summaries)
    print(f"Summarizer agent produced {len(summaries.get('briefs', []))} briefs.")
    critique = critic_agent(client, args.model, summaries)
    print(f"Critic agent produced {len(critique.get('verdicts', []))} verdicts.")
    needs_revision = any(verdict.get("required_edits") for verdict in critique.get("verdicts", []))
    revised_summaries = summaries
    if needs_revision:
        revised_summaries = reviser_agent(client, args.model, grounded_clusters, summaries, critique)
        print(f"Reviser agent produced {len(revised_summaries.get('briefs', []))} final briefs.")

    brief_by_id = {
        item["cluster_id"]: item
        for item in revised_summaries.get("briefs", [])
        if item.get("cluster_id") is not None
    }
    verdict_by_id = {item["cluster_id"]: item for item in critique.get("verdicts", []) if item.get("cluster_id") is not None}

    selected: list[dict[str, Any]] = []
    for cluster in grounded_clusters:
        cid = cluster["id"]
        brief = brief_by_id.get(cid)
        verdict = verdict_by_id.get(cid, {})
        if not brief:
            continue
        if verdict.get("include") is False or verdict.get("factuality_ok") is False:
            continue
        headline = (cluster.get("factual_headline") or "").strip()
        source_name = (cluster.get("primary_source_name") or "").strip().lower()
        source_summary = (cluster.get("primary_source_summary") or cluster.get("factual_summary") or "").strip()
        if headline.lower().startswith("show hn:"):
            continue
        if source_name in {"hacker news", "reddit r/machinelearning", "reddit localllama"} and len(source_summary) < 80:
            continue
        cluster["source_label"] = source_label_from_cluster(cluster)
        selected.append(
            {
                "cluster": cluster,
                "brief": brief,
                "verdict": verdict,
                "default_section": section_name_for_item(cluster, brief),
            }
        )

    selected = rank_selected_items(selected)
    editor_plan = editor_agent(client, args.model, selected)
    html = render_html_newsletter(selected, editor_plan)
    email_html = render_email_newsletter(selected, editor_plan)
    paths = save_outputs(html, email_html, grounded_output, summaries, critique, revised_summaries, editor_plan)
    print(f"HTML newsletter: {paths['html']}")
    print(f"Email HTML: {paths['email_html']}")
    print(f"Agent JSON: {paths['json']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
