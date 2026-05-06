"""
Collect AI intelligence source entries and store them in PostgreSQL.

Usage:
    $env:DATABASE_URL="postgresql://user:password@localhost:5432/ai_news"
    .venv\\Scripts\\python.exe collect_ai_news_to_postgres.py --limit 10

The script creates two tables if needed:
    ai_sources
    ai_news_items
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import re
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from dateutil import parser as date_parser

from unified_ai_news_collector import installed_frameworks, load_sources, test_source

try:
    from openai import OpenAI
except Exception:  # pragma: no cover - optional LLM path
    OpenAI = None


def load_local_env() -> None:
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path, "r", encoding="utf-8") as handle:
        for line in handle:
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS ai_sources (
    id BIGSERIAL PRIMARY KEY,
    source_name TEXT NOT NULL UNIQUE,
    category TEXT,
    subcategory TEXT,
    website TEXT NOT NULL,
    access_model TEXT,
    priority TEXT,
    selected_framework TEXT,
    last_accessible BOOLEAN,
    last_success_mode TEXT,
    last_entries_found INTEGER,
    last_diagnosis TEXT,
    last_checked_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS ai_news_items (
    id BIGSERIAL PRIMARY KEY,
    source_id BIGINT NOT NULL REFERENCES ai_sources(id) ON DELETE CASCADE,
    source_name TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT NOT NULL,
    published_at TEXT,
    published_at_ts TIMESTAMPTZ,
    collected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    success_mode TEXT,
    extraction_framework TEXT,
    content_markdown TEXT,
    raw_item JSONB,
    content_hash TEXT NOT NULL,
    duplicate_key TEXT,
    is_duplicate BOOLEAN NOT NULL DEFAULT FALSE,
    duplicate_of_item_id BIGINT REFERENCES ai_news_items(id),
    event_at_ts TIMESTAMPTZ,
    event_date_confidence TEXT,
    freshness_status TEXT,
    story_cluster_id BIGINT,
    UNIQUE (source_name, url)
);

CREATE TABLE IF NOT EXISTS story_clusters (
    id BIGSERIAL PRIMARY KEY,
    cluster_key TEXT NOT NULL UNIQUE,
    canonical_title TEXT NOT NULL,
    canonical_summary TEXT,
    event_at_ts TIMESTAMPTZ,
    event_date_confidence TEXT,
    freshness_status TEXT,
    primary_source TEXT,
    source_count INTEGER NOT NULL DEFAULT 0,
    item_count INTEGER NOT NULL DEFAULT 0,
    llm_model TEXT,
    llm_confidence NUMERIC,
    llm_reason TEXT,
    is_ai_relevant BOOLEAN,
    ai_relevance_score NUMERIC,
    ai_relevance_reason TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS story_cluster_items (
    cluster_id BIGINT NOT NULL REFERENCES story_clusters(id) ON DELETE CASCADE,
    item_id BIGINT NOT NULL REFERENCES ai_news_items(id) ON DELETE CASCADE,
    relationship TEXT NOT NULL DEFAULT 'supporting',
    PRIMARY KEY (cluster_id, item_id)
);

CREATE INDEX IF NOT EXISTS idx_ai_news_items_collected_at
    ON ai_news_items (collected_at DESC);

CREATE INDEX IF NOT EXISTS idx_ai_news_items_source_name
    ON ai_news_items (source_name);

CREATE INDEX IF NOT EXISTS idx_ai_sources_last_checked_at
    ON ai_sources (last_checked_at DESC);

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS published_at_ts TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_ai_news_items_published_at_ts
    ON ai_news_items (published_at_ts DESC);

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS duplicate_key TEXT;

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS is_duplicate BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS duplicate_of_item_id BIGINT REFERENCES ai_news_items(id);

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS event_at_ts TIMESTAMPTZ;

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS event_date_confidence TEXT;

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS freshness_status TEXT;

ALTER TABLE ai_news_items
    ADD COLUMN IF NOT EXISTS story_cluster_id BIGINT;

CREATE INDEX IF NOT EXISTS idx_ai_news_items_duplicate_key
    ON ai_news_items (duplicate_key);

CREATE INDEX IF NOT EXISTS idx_ai_news_items_event_at_ts
    ON ai_news_items (event_at_ts DESC);

CREATE INDEX IF NOT EXISTS idx_ai_news_items_story_cluster_id
    ON ai_news_items (story_cluster_id);

CREATE INDEX IF NOT EXISTS idx_story_clusters_freshness_status
    ON story_clusters (freshness_status);

ALTER TABLE story_clusters
    ADD COLUMN IF NOT EXISTS is_ai_relevant BOOLEAN;

ALTER TABLE story_clusters
    ADD COLUMN IF NOT EXISTS ai_relevance_score NUMERIC;

ALTER TABLE story_clusters
    ADD COLUMN IF NOT EXISTS ai_relevance_reason TEXT;
"""


def content_hash(source_name: str, title: str, url: str) -> str:
    payload = f"{source_name}\n{title}\n{url}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()


def normalize_title(title: str) -> str:
    lowered = title.lower()
    lowered = re.sub(r"https?://\S+", " ", lowered)
    lowered = re.sub(r"[^a-z0-9 ]+", " ", lowered)
    words = [
        word
        for word in lowered.split()
        if word
        and word
        not in {
            "the",
            "a",
            "an",
            "and",
            "or",
            "to",
            "of",
            "for",
            "in",
            "on",
            "with",
            "by",
            "from",
            "new",
            "latest",
        }
    ]
    return " ".join(words[:18])


def duplicate_key(title: str, url: str) -> str:
    normalized = normalize_title(title)
    if not normalized:
        normalized = url.strip().lower()
    return hashlib.sha256(normalized.encode("utf-8", errors="replace")).hexdigest()


def parse_published_at(value: str) -> dt.datetime | None:
    if not value:
        return None
    value = str(value).strip()
    if not value:
        return None
    if re.fullmatch(r"\d+(\.\d+)?", value):
        try:
            timestamp = float(value)
            if timestamp > 10_000_000_000:
                timestamp = timestamp / 1000
            return dt.datetime.fromtimestamp(timestamp, tz=dt.timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    try:
        parsed = date_parser.parse(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def is_recent_item(item: dict[str, Any], cutoff: dt.datetime, include_undated: bool) -> tuple[bool, dt.datetime | None]:
    parsed = parse_published_at(item.get("published", ""))
    if parsed is None:
        return include_undated, None
    return parsed >= cutoff, parsed


def item_text_for_event_date(title: str, item: dict[str, Any]) -> str:
    parts = [title]
    for key in ("summary", "description", "content", "published"):
        value = item.get(key)
        if isinstance(value, str):
            parts.append(value)
    return " ".join(parts)


def infer_event_date(
    title: str,
    item: dict[str, Any],
    published_at: dt.datetime | None,
    cutoff: dt.datetime,
) -> tuple[dt.datetime | None, str, str]:
    text = item_text_for_event_date(title, item)
    lowered = text.lower()
    reference = published_at or dt.datetime.now(dt.timezone.utc)

    if re.search(r"\b(today|announced today|released today|launches today)\b", lowered):
        event_at = reference
        confidence = "medium"
    elif re.search(r"\b(yesterday|announced yesterday|released yesterday)\b", lowered):
        event_at = reference - dt.timedelta(days=1)
        confidence = "medium"
    elif re.search(r"\b(last week|this week|weekly)\b", lowered):
        event_at = reference
        confidence = "low"
    else:
        explicit_date = None
        for pattern in (
            r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)[a-z]*\.?\s+\d{1,2},?\s+\d{4}\b",
            r"\b\d{4}-\d{1,2}-\d{1,2}\b",
        ):
            match = re.search(pattern, text, flags=re.I)
            if match:
                explicit_date = parse_published_at(match.group(0))
                break
        if explicit_date:
            event_at = explicit_date
            confidence = "medium"
        else:
            event_at = published_at
            confidence = "low" if event_at else "unknown"

    if event_at is None:
        status = "unknown_event_date"
    elif event_at >= cutoff:
        status = "current_week_event"
    elif published_at and published_at >= cutoff:
        status = "recent_analysis_of_old_event"
    else:
        status = "historical_context"
    return event_at, confidence, status


def title_tokens(title: str) -> set[str]:
    return set(normalize_title(title).split())


def jaccard(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def blocking_key(title: str) -> str:
    tokens = sorted(title_tokens(title))
    return " ".join(tokens[:4]) if tokens else normalize_title(title)[:24]


def connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(database_url, row_factory=dict_row, connect_timeout=10)


def ensure_schema(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(SCHEMA_SQL)
    conn.commit()


def backfill_event_fields(conn: psycopg.Connection, cutoff: dt.datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE ai_news_items
            SET event_at_ts = COALESCE(event_at_ts, published_at_ts),
                event_date_confidence = COALESCE(event_date_confidence, 'low'),
                freshness_status = CASE
                    WHEN (freshness_status IS NULL OR freshness_status = 'unknown_event_date')
                         AND COALESCE(event_at_ts, published_at_ts) >= %s THEN 'current_week_event'
                    WHEN freshness_status IS NULL AND published_at_ts IS NULL THEN 'unknown_event_date'
                    WHEN freshness_status IS NULL THEN 'historical_context'
                    ELSE freshness_status
                END
            WHERE event_at_ts IS NULL
               OR event_date_confidence IS NULL
               OR freshness_status IS NULL
               OR (freshness_status = 'unknown_event_date' AND COALESCE(event_at_ts, published_at_ts) >= %s)
            """,
            (cutoff, cutoff),
        )
        count = cur.rowcount
    conn.commit()
    return int(count)


def upsert_source(conn: psycopg.Connection, source: dict[str, Any], result: dict[str, Any]) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ai_sources (
                source_name, category, subcategory, website, access_model, priority,
                selected_framework, last_accessible, last_success_mode, last_entries_found,
                last_diagnosis, last_checked_at
            )
            VALUES (
                %(source_name)s, %(category)s, %(subcategory)s, %(website)s, %(access_model)s,
                %(priority)s, %(selected_framework)s, %(last_accessible)s, %(last_success_mode)s,
                %(last_entries_found)s, %(last_diagnosis)s, %(last_checked_at)s
            )
            ON CONFLICT (source_name) DO UPDATE SET
                category = EXCLUDED.category,
                subcategory = EXCLUDED.subcategory,
                website = EXCLUDED.website,
                access_model = EXCLUDED.access_model,
                priority = EXCLUDED.priority,
                selected_framework = EXCLUDED.selected_framework,
                last_accessible = EXCLUDED.last_accessible,
                last_success_mode = EXCLUDED.last_success_mode,
                last_entries_found = EXCLUDED.last_entries_found,
                last_diagnosis = EXCLUDED.last_diagnosis,
                last_checked_at = EXCLUDED.last_checked_at
            RETURNING id
            """,
            {
                "source_name": source["name"],
                "category": source.get("category", ""),
                "subcategory": source.get("subcategory", ""),
                "website": source.get("website", ""),
                "access_model": source.get("access", ""),
                "priority": source.get("priority", ""),
                "selected_framework": source.get("framework", ""),
                "last_accessible": result.get("accessible", False),
                "last_success_mode": result.get("success_mode", ""),
                "last_entries_found": result.get("entries_found", 0),
                "last_diagnosis": result.get("diagnosis", ""),
                "last_checked_at": dt.datetime.now(dt.timezone.utc),
            },
        )
        row = cur.fetchone()
    return int(row["id"])


def find_duplicate(conn: psycopg.Connection, key: str, source_name: str, url: str) -> int | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM ai_news_items
            WHERE duplicate_key = %s
              AND NOT (source_name = %s AND url = %s)
            ORDER BY collected_at ASC
            LIMIT 1
            """,
            (key, source_name, url),
        )
        row = cur.fetchone()
    return int(row["id"]) if row else None


def upsert_items(
    conn: psycopg.Connection,
    source_id: int,
    result: dict[str, Any],
    cutoff: dt.datetime,
    include_undated: bool,
) -> tuple[int, int, int]:
    inserted_or_updated = 0
    skipped_old = 0
    duplicates = 0
    extraction = result.get("page_extraction") or {}
    extraction_framework = extraction.get("framework_used")
    content_markdown = extraction.get("sample_markdown") if extraction.get("ok") else None

    with conn.cursor() as cur:
        for item in result.get("entries", result.get("sample_entries", [])):
            title = (item.get("title") or "").strip()
            url = (item.get("link") or "").strip()
            if not title or not url:
                continue
            keep, parsed_published = is_recent_item(item, cutoff, include_undated)
            if not keep:
                skipped_old += 1
                continue
            event_at, event_confidence, freshness_status = infer_event_date(title, item, parsed_published, cutoff)
            key = duplicate_key(title, url)
            duplicate_of = find_duplicate(conn, key, result["source"], url)
            is_duplicate = duplicate_of is not None
            cur.execute(
                """
                INSERT INTO ai_news_items (
                    source_id, source_name, title, url, published_at, published_at_ts, success_mode,
                    extraction_framework, content_markdown, raw_item, content_hash
                    , duplicate_key, is_duplicate, duplicate_of_item_id,
                    event_at_ts, event_date_confidence, freshness_status
                )
                VALUES (
                    %(source_id)s, %(source_name)s, %(title)s, %(url)s, %(published_at)s,
                    %(published_at_ts)s,
                    %(success_mode)s, %(extraction_framework)s, %(content_markdown)s,
                    %(raw_item)s, %(content_hash)s, %(duplicate_key)s, %(is_duplicate)s,
                    %(duplicate_of_item_id)s, %(event_at_ts)s, %(event_date_confidence)s,
                    %(freshness_status)s
                )
                ON CONFLICT (source_name, url) DO UPDATE SET
                    title = EXCLUDED.title,
                    published_at = EXCLUDED.published_at,
                    published_at_ts = EXCLUDED.published_at_ts,
                    collected_at = now(),
                    success_mode = EXCLUDED.success_mode,
                    extraction_framework = EXCLUDED.extraction_framework,
                    content_markdown = COALESCE(EXCLUDED.content_markdown, ai_news_items.content_markdown),
                    raw_item = EXCLUDED.raw_item,
                    content_hash = EXCLUDED.content_hash,
                    duplicate_key = EXCLUDED.duplicate_key,
                    is_duplicate = EXCLUDED.is_duplicate,
                    duplicate_of_item_id = EXCLUDED.duplicate_of_item_id,
                    event_at_ts = EXCLUDED.event_at_ts,
                    event_date_confidence = EXCLUDED.event_date_confidence,
                    freshness_status = EXCLUDED.freshness_status
                """,
                {
                    "source_id": source_id,
                    "source_name": result["source"],
                    "title": title,
                    "url": url,
                    "published_at": parsed_published.isoformat() if parsed_published else "",
                    "published_at_ts": parsed_published,
                    "success_mode": result.get("success_mode", ""),
                    "extraction_framework": extraction_framework,
                    "content_markdown": content_markdown,
                    "raw_item": Jsonb(item),
                    "content_hash": content_hash(result["source"], title, url),
                    "duplicate_key": key,
                    "is_duplicate": is_duplicate,
                    "duplicate_of_item_id": duplicate_of,
                    "event_at_ts": event_at,
                    "event_date_confidence": event_confidence,
                    "freshness_status": freshness_status,
                },
            )
            inserted_or_updated += 1
            duplicates += int(is_duplicate)
    return inserted_or_updated, skipped_old, duplicates


def write_run_summary(conn: psycopg.Connection) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) AS count FROM ai_sources")
        sources = int(cur.fetchone()["count"])
        cur.execute("SELECT COUNT(*) AS count FROM ai_news_items")
        items = int(cur.fetchone()["count"])
        cur.execute("SELECT COUNT(*) AS count FROM ai_news_items WHERE is_duplicate")
        duplicate_items = int(cur.fetchone()["count"])
        cur.execute("SELECT COUNT(*) AS count FROM ai_sources WHERE last_accessible")
        accessible = int(cur.fetchone()["count"])
        cur.execute("SELECT COUNT(*) AS count FROM story_clusters")
        clusters = int(cur.fetchone()["count"])
    return {
        "sources": sources,
        "items": items,
        "duplicate_items": duplicate_items,
        "accessible_sources": accessible,
        "clusters": clusters,
    }


def fetch_recent_items(conn: psycopg.Connection, cutoff: dt.datetime) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, source_name, title, url, published_at_ts, event_at_ts,
                   event_date_confidence, freshness_status
            FROM ai_news_items
            WHERE published_at_ts >= %s
            ORDER BY published_at_ts DESC NULLS LAST, id DESC
            """,
            (cutoff,),
        )
        return list(cur.fetchall())


def build_deterministic_clusters(items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    clusters: list[list[dict[str, Any]]] = []
    cluster_tokens: list[set[str]] = []
    for item in items:
        tokens = title_tokens(item["title"])
        placed = False
        for index, existing_tokens in enumerate(cluster_tokens):
            if item["source_name"] == clusters[index][0]["source_name"] and item["url"] == clusters[index][0]["url"]:
                continue
            if jaccard(tokens, existing_tokens) >= 0.58 and len(tokens & existing_tokens) >= 3:
                clusters[index].append(item)
                cluster_tokens[index] = cluster_tokens[index] | tokens
                placed = True
                break
        if not placed:
            clusters.append([item])
            cluster_tokens.append(tokens)
    return clusters


def choose_cluster_event(cluster: list[dict[str, Any]]) -> tuple[dt.datetime | None, str, str]:
    dated = [item for item in cluster if item.get("event_at_ts")]
    if dated:
        dated.sort(key=lambda item: item["event_at_ts"], reverse=True)
        item = dated[0]
        return item["event_at_ts"], item.get("event_date_confidence") or "low", item.get("freshness_status") or "unknown_event_date"
    return None, "unknown", "unknown_event_date"


def llm_refine_cluster(cluster: list[dict[str, Any]], model: str, cutoff: dt.datetime) -> dict[str, Any] | None:
    if OpenAI is None or not os.environ.get("OPENAI_API_KEY"):
        return None
    client = OpenAI()
    payload = [
        {
            "id": item["id"],
            "source": item["source_name"],
            "title": item["title"],
            "url": item["url"],
            "published_at": item["published_at_ts"].isoformat() if item.get("published_at_ts") else None,
            "event_at": item["event_at_ts"].isoformat() if item.get("event_at_ts") else None,
        }
        for item in cluster
    ]
    prompt = {
        "task": "Deduplicate and refine one AI news story cluster.",
        "rules": [
            "Decide whether these items are the same underlying news event.",
            "Produce a canonical title and short summary.",
            "Infer the actual event date if possible, not just publication date.",
            "freshness_status must be one of current_week_event, recent_analysis_of_old_event, historical_context, unknown_event_date.",
            "Use current_week_event only when the event itself appears to be within the cutoff window.",
        ],
        "cutoff_utc": cutoff.isoformat(),
        "items": payload,
        "output_json_schema": {
            "same_story": True,
            "canonical_title": "string",
            "canonical_summary": "string",
            "event_at": "ISO date or null",
            "event_date_confidence": "high|medium|low|unknown",
            "freshness_status": "current_week_event|recent_analysis_of_old_event|historical_context|unknown_event_date",
            "primary_source": "string",
            "confidence": 0.0,
            "reason": "string",
        },
    }
    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": "You are an AI news intelligence deduplication analyst. Return only valid JSON.",
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
        )
        text = response.output_text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```(?:json)?\s*", "", text)
            text = re.sub(r"\s*```$", "", text)
        return json.loads(text)
    except Exception as exc:  # noqa: BLE001
        print(f"  LLM cluster refinement skipped after error: {type(exc).__name__}: {exc}")
        return None


def clean_json_text(text: str) -> str:
    text = text.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    return text.strip()


def llm_refine_cluster_batch(
    clusters: list[list[dict[str, Any]]],
    model: str,
    cutoff: dt.datetime,
    start_index: int,
) -> dict[int, dict[str, Any]]:
    if OpenAI is None or not os.environ.get("OPENAI_API_KEY") or not clusters:
        return {}
    client = OpenAI()
    payload = []
    for offset, cluster in enumerate(clusters):
        cluster_index = start_index + offset
        payload.append(
            {
                "cluster_index": cluster_index,
                "items": [
                    {
                        "id": item["id"],
                        "source": item["source_name"],
                        "title": item["title"],
                        "url": item["url"],
                        "published_at": item["published_at_ts"].isoformat() if item.get("published_at_ts") else None,
                        "event_at": item["event_at_ts"].isoformat() if item.get("event_at_ts") else None,
                    }
                    for item in cluster
                ],
            }
        )
    prompt = {
        "task": "Refine AI news story clusters in one batch.",
        "instructions": [
            "Each cluster is a deterministic candidate group from a news database.",
            "For every cluster, decide canonical title, summary, actual event date, freshness, duplicate confidence, and AI relevance.",
            "Do not merge clusters across cluster_index values in this response; only refine each candidate cluster.",
            "is_ai_relevant should be false for generic consumer tech, TV reviews, non-AI product launches, or unrelated stories.",
            "freshness_status must be current_week_event, recent_analysis_of_old_event, historical_context, or unknown_event_date.",
            "Return only valid JSON.",
        ],
        "cutoff_utc": cutoff.isoformat(),
        "clusters": payload,
        "output_shape": {
            "clusters": [
                {
                    "cluster_index": 0,
                    "same_story": True,
                    "canonical_title": "string",
                    "canonical_summary": "string",
                    "event_at": "ISO date or null",
                    "event_date_confidence": "high|medium|low|unknown",
                    "freshness_status": "current_week_event|recent_analysis_of_old_event|historical_context|unknown_event_date",
                    "primary_source": "string",
                    "confidence": 0.0,
                    "reason": "string",
                    "is_ai_relevant": True,
                    "ai_relevance_score": 0.0,
                    "ai_relevance_reason": "string",
                }
            ]
        },
    }
    try:
        response = client.responses.create(
            model=model,
            input=[
                {
                    "role": "system",
                    "content": "You are an AI intelligence deduplication and relevance analyst. Return only valid JSON.",
                },
                {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
            ],
        )
        parsed = json.loads(clean_json_text(response.output_text))
    except Exception as exc:  # noqa: BLE001
        print(f"  LLM batch refinement skipped after error: {type(exc).__name__}: {exc}")
        return {}
    refined: dict[int, dict[str, Any]] = {}
    for item in parsed.get("clusters", []):
        try:
            refined[int(item["cluster_index"])] = item
        except (KeyError, TypeError, ValueError):
            continue
    return refined


def fetch_weekly_cluster_summaries(conn: psycopg.Connection, cutoff: dt.datetime) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT sc.id, sc.canonical_title, sc.canonical_summary, sc.event_at_ts,
                   sc.freshness_status, sc.primary_source, sc.source_count,
                   sc.item_count, sc.is_ai_relevant, sc.ai_relevance_score,
                   array_agg(DISTINCT ani.source_name ORDER BY ani.source_name) AS sources,
                   array_agg(ani.title ORDER BY ani.published_at_ts DESC NULLS LAST) AS item_titles
            FROM story_clusters sc
            JOIN story_cluster_items sci ON sci.cluster_id = sc.id
            JOIN ai_news_items ani ON ani.id = sci.item_id
            WHERE ani.published_at_ts >= %s
            GROUP BY sc.id
            ORDER BY sc.updated_at DESC
            """,
            (cutoff,),
        )
        return list(cur.fetchall())


def deterministic_global_merge_groups(clusters: list[dict[str, Any]]) -> list[list[int]]:
    groups: list[list[int]] = []
    used: set[int] = set()
    tokens_by_id = {cluster["id"]: title_tokens(cluster["canonical_title"]) for cluster in clusters}
    block_by_id = {cluster["id"]: blocking_key(cluster["canonical_title"]) for cluster in clusters}
    for cluster in clusters:
        cluster_id = int(cluster["id"])
        if cluster_id in used:
            continue
        group = [cluster_id]
        used.add(cluster_id)
        for other in clusters:
            other_id = int(other["id"])
            if other_id in used:
                continue
            same_block = block_by_id[cluster_id] == block_by_id[other_id]
            similarity = jaccard(tokens_by_id[cluster_id], tokens_by_id[other_id])
            if same_block or (similarity >= 0.62 and len(tokens_by_id[cluster_id] & tokens_by_id[other_id]) >= 3):
                group.append(other_id)
                used.add(other_id)
        groups.append(group)
    return groups


def llm_global_merge_groups(
    clusters: list[dict[str, Any]],
    model: str,
    batch_size: int,
) -> list[list[int]]:
    if OpenAI is None or not os.environ.get("OPENAI_API_KEY") or not clusters:
        return []
    client = OpenAI()
    all_groups: list[list[int]] = []
    for start in range(0, len(clusters), max(1, batch_size)):
        batch = clusters[start : start + max(1, batch_size)]
        payload = [
            {
                "cluster_id": int(cluster["id"]),
                "canonical_title": cluster["canonical_title"],
                "summary": cluster.get("canonical_summary") or "",
                "event_at": cluster["event_at_ts"].isoformat() if cluster.get("event_at_ts") else None,
                "freshness_status": cluster.get("freshness_status"),
                "sources": cluster.get("sources") or [],
                "item_titles": (cluster.get("item_titles") or [])[:5],
                "is_ai_relevant": cluster.get("is_ai_relevant"),
                "ai_relevance_score": float(cluster["ai_relevance_score"]) if cluster.get("ai_relevance_score") is not None else None,
            }
            for cluster in batch
        ]
        prompt = {
            "task": "Find duplicate AI news story clusters within this batch.",
            "instructions": [
                "Return groups of cluster_ids that describe the same underlying news event.",
                "Only merge when clearly the same event, product launch, paper, model release, acquisition, funding, policy decision, outage, or benchmark result.",
                "Do not merge broad trend articles merely because they share entities like OpenAI or AI.",
                "Return singleton groups only if no duplicates are found for that cluster.",
                "Return only valid JSON.",
            ],
            "clusters": payload,
            "output_shape": {"merge_groups": [[1, 2], [3]]},
        }
        try:
            response = client.responses.create(
                model=model,
                input=[
                    {
                        "role": "system",
                        "content": "You are a cost-conscious AI news deduplication analyst. Return only valid JSON.",
                    },
                    {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
                ],
            )
            parsed = json.loads(clean_json_text(response.output_text))
        except Exception as exc:  # noqa: BLE001
            print(f"  LLM global merge batch skipped after error: {type(exc).__name__}: {exc}")
            continue
        merge_groups = parsed.get("merge_groups", []) if isinstance(parsed, dict) else parsed
        for group in merge_groups:
            ids = []
            for value in group:
                try:
                    ids.append(int(value))
                except (TypeError, ValueError):
                    continue
            if ids:
                all_groups.append(ids)
    return all_groups


def apply_global_merge_groups(conn: psycopg.Connection, groups: list[list[int]]) -> tuple[int, int]:
    merged_groups = 0
    duplicate_clusters = 0
    with conn.cursor() as cur:
        for group in groups:
            unique_ids = sorted(set(int(cluster_id) for cluster_id in group))
            if len(unique_ids) < 2:
                continue
            canonical_id = unique_ids[0]
            duplicate_ids = unique_ids[1:]
            cur.execute(
                """
                INSERT INTO story_cluster_items (cluster_id, item_id, relationship)
                SELECT %s, item_id, 'supporting'
                FROM story_cluster_items
                WHERE cluster_id = ANY(%s)
                ON CONFLICT (cluster_id, item_id) DO UPDATE SET relationship = EXCLUDED.relationship
                """,
                (canonical_id, duplicate_ids),
            )
            cur.execute(
                """
                UPDATE ai_news_items
                SET story_cluster_id = %s,
                    is_duplicate = CASE WHEN duplicate_of_item_id IS NULL THEN TRUE ELSE is_duplicate END,
                    duplicate_of_item_id = COALESCE(
                        duplicate_of_item_id,
                        (
                            SELECT MIN(item_id)
                            FROM story_cluster_items
                            WHERE cluster_id = %s
                        )
                    )
                WHERE story_cluster_id = ANY(%s)
                """,
                (canonical_id, canonical_id, duplicate_ids),
            )
            cur.execute(
                """
                UPDATE story_clusters
                SET source_count = sub.source_count,
                    item_count = sub.item_count,
                    updated_at = now()
                FROM (
                    SELECT COUNT(DISTINCT ani.source_name) AS source_count,
                           COUNT(*) AS item_count
                    FROM story_cluster_items sci
                    JOIN ai_news_items ani ON ani.id = sci.item_id
                    WHERE sci.cluster_id = %s
                ) sub
                WHERE id = %s
                """,
                (canonical_id, canonical_id),
            )
            cur.execute(
                """
                DELETE FROM story_cluster_items
                WHERE cluster_id = ANY(%s)
                """,
                (duplicate_ids,),
            )
            cur.execute("DELETE FROM story_clusters WHERE id = ANY(%s)", (duplicate_ids,))
            merged_groups += 1
            duplicate_clusters += len(duplicate_ids)
    conn.commit()
    return merged_groups, duplicate_clusters


def global_weekly_dedupe(
    conn: psycopg.Connection,
    cutoff: dt.datetime,
    use_llm: bool,
    llm_model: str,
    llm_batch_size: int,
) -> tuple[int, int]:
    clusters = fetch_weekly_cluster_summaries(conn, cutoff)
    groups = deterministic_global_merge_groups(clusters)
    if use_llm:
        # LLM sees compact cluster summaries, not raw source pages. This keeps
        # cost bounded while catching semantic duplicates missed by title rules.
        llm_groups = llm_global_merge_groups(clusters, llm_model, llm_batch_size)
        groups.extend(llm_groups)
    return apply_global_merge_groups(conn, groups)


def upsert_story_clusters(
    conn: psycopg.Connection,
    cutoff: dt.datetime,
    use_llm: bool,
    llm_model: str,
    cluster_limit: int = 0,
    llm_batch_size: int = 20,
    llm_refine_singletons: bool = False,
) -> tuple[int, int]:
    items = fetch_recent_items(conn, cutoff)
    clusters = build_deterministic_clusters(items)
    if cluster_limit:
        clusters = clusters[:cluster_limit]
    llm_refinements: dict[int, dict[str, Any]] = {}
    if use_llm:
        candidate_indexes = [
            index
            for index, cluster in enumerate(clusters)
            if llm_refine_singletons or len(cluster) > 1
        ]
        for batch_start in range(0, len(candidate_indexes), max(1, llm_batch_size)):
            batch_indexes = candidate_indexes[batch_start : batch_start + max(1, llm_batch_size)]
            batch_clusters = [clusters[index] for index in batch_indexes]
            batch_refinements = llm_refine_cluster_batch(
                batch_clusters,
                model=llm_model,
                cutoff=cutoff,
                start_index=batch_indexes[0] if batch_indexes else 0,
            )
            # The batch helper uses consecutive indexes from start_index. Re-map
            # defensively in case candidate indexes are sparse.
            if batch_refinements and batch_indexes != list(range(batch_indexes[0], batch_indexes[0] + len(batch_indexes))):
                remapped: dict[int, dict[str, Any]] = {}
                for offset, original_index in enumerate(batch_indexes):
                    candidate_key = batch_indexes[0] + offset
                    if candidate_key in batch_refinements:
                        remapped[original_index] = batch_refinements[candidate_key]
                batch_refinements = remapped
            llm_refinements.update(batch_refinements)
    cluster_count = 0
    duplicate_count = 0
    with conn.cursor() as cur:
        for cluster in clusters:
            if not cluster:
                continue
            cluster_hash_input = "|".join(sorted(str(item["id"]) for item in cluster))
            cluster_key = hashlib.sha256(cluster_hash_input.encode("utf-8")).hexdigest()
            canonical_item = cluster[0]
            event_at, event_confidence, freshness_status = choose_cluster_event(cluster)
            primary_source = canonical_item["source_name"]
            summary = ""
            llm_confidence = None
            llm_reason = ""
            llm_model_used = None
            is_ai_relevant = None
            ai_relevance_score = None
            ai_relevance_reason = ""
            if use_llm:
                refined = llm_refinements.get(cluster_count)
                if refined:
                    canonical_title = refined.get("canonical_title") or canonical_item["title"]
                    summary = refined.get("canonical_summary") or ""
                    parsed_event = parse_published_at(refined.get("event_at") or "")
                    event_at = parsed_event or event_at
                    event_confidence = refined.get("event_date_confidence") or event_confidence
                    freshness_status = refined.get("freshness_status") or freshness_status
                    primary_source = refined.get("primary_source") or primary_source
                    llm_confidence = refined.get("confidence")
                    llm_reason = refined.get("reason") or ""
                    llm_model_used = llm_model
                    is_ai_relevant = refined.get("is_ai_relevant")
                    ai_relevance_score = refined.get("ai_relevance_score")
                    ai_relevance_reason = refined.get("ai_relevance_reason") or ""
                else:
                    canonical_title = canonical_item["title"]
            else:
                canonical_title = canonical_item["title"]
            source_count = len({item["source_name"] for item in cluster})
            cur.execute(
                """
                INSERT INTO story_clusters (
                    cluster_key, canonical_title, canonical_summary, event_at_ts,
                    event_date_confidence, freshness_status, primary_source,
                    source_count, item_count, llm_model, llm_confidence, llm_reason,
                    is_ai_relevant, ai_relevance_score, ai_relevance_reason, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                ON CONFLICT (cluster_key) DO UPDATE SET
                    canonical_title = EXCLUDED.canonical_title,
                    canonical_summary = EXCLUDED.canonical_summary,
                    event_at_ts = EXCLUDED.event_at_ts,
                    event_date_confidence = EXCLUDED.event_date_confidence,
                    freshness_status = EXCLUDED.freshness_status,
                    primary_source = EXCLUDED.primary_source,
                    source_count = EXCLUDED.source_count,
                    item_count = EXCLUDED.item_count,
                    llm_model = EXCLUDED.llm_model,
                    llm_confidence = EXCLUDED.llm_confidence,
                    llm_reason = EXCLUDED.llm_reason,
                    is_ai_relevant = EXCLUDED.is_ai_relevant,
                    ai_relevance_score = EXCLUDED.ai_relevance_score,
                    ai_relevance_reason = EXCLUDED.ai_relevance_reason,
                    updated_at = now()
                RETURNING id
                """,
                (
                    cluster_key,
                    canonical_title,
                    summary,
                    event_at,
                    event_confidence,
                    freshness_status,
                    primary_source,
                    source_count,
                    len(cluster),
                    llm_model_used,
                    llm_confidence,
                    llm_reason,
                    is_ai_relevant,
                    ai_relevance_score,
                    ai_relevance_reason,
                ),
            )
            cluster_id = int(cur.fetchone()["id"])
            canonical_id = int(canonical_item["id"])
            for index, item in enumerate(cluster):
                relationship = "primary" if index == 0 else "supporting"
                is_duplicate = index > 0
                duplicate_of = canonical_id if is_duplicate else None
                cur.execute(
                    """
                    INSERT INTO story_cluster_items (cluster_id, item_id, relationship)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (cluster_id, item_id) DO UPDATE SET
                        relationship = EXCLUDED.relationship
                    """,
                    (cluster_id, item["id"], relationship),
                )
                cur.execute(
                    """
                    UPDATE ai_news_items
                    SET story_cluster_id = %s,
                        is_duplicate = %s,
                        duplicate_of_item_id = %s,
                        event_at_ts = COALESCE(event_at_ts, %s),
                        event_date_confidence = COALESCE(event_date_confidence, %s),
                        freshness_status = COALESCE(freshness_status, %s)
                    WHERE id = %s
                    """,
                    (
                        cluster_id,
                        is_duplicate,
                        duplicate_of,
                        event_at,
                        event_confidence,
                        freshness_status,
                        item["id"],
                    ),
                )
                duplicate_count += int(is_duplicate)
            cluster_count += 1
    conn.commit()
    return cluster_count, duplicate_count


def parse_args() -> argparse.Namespace:
    load_local_env()
    parser = argparse.ArgumentParser(description="Collect AI news and store results in PostgreSQL.")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL"), help="PostgreSQL connection URL.")
    parser.add_argument("--limit", type=int, default=0, help="Limit number of sources for test runs.")
    parser.add_argument("--source", action="append", default=[], help="Only run matching source name. Can be repeated.")
    parser.add_argument("--dry-run", action="store_true", help="Collect and print results without writing to PostgreSQL.")
    parser.add_argument("--days", type=int, default=7, help="Only store items published within this many days.")
    parser.add_argument(
        "--include-undated",
        action="store_true",
        help="Also store items whose source did not expose a publish date. Default is strict: undated items are skipped.",
    )
    parser.add_argument(
        "--skip-clustering",
        action="store_true",
        help="Skip story cluster generation after ingestion.",
    )
    parser.add_argument(
        "--llm-cluster",
        action="store_true",
        help="Use OpenAI to batch-refine story clusters when OPENAI_API_KEY is configured.",
    )
    parser.add_argument("--llm-model", default="gpt-4.1-mini", help="OpenAI model for optional cluster refinement.")
    parser.add_argument("--cluster-limit", type=int, default=0, help="Limit clusters processed during cluster rebuild/testing.")
    parser.add_argument("--llm-batch-size", type=int, default=20, help="Number of clusters per OpenAI refinement call.")
    parser.add_argument(
        "--llm-refine-singletons",
        action="store_true",
        help="Also send one-item clusters to the LLM. By default, only multi-item duplicate candidates are sent.",
    )
    parser.add_argument(
        "--global-dedupe",
        action="store_true",
        help="Run a weekly global merge pass across all current-week story clusters.",
    )
    parser.add_argument(
        "--llm-global-dedupe",
        action="store_true",
        help="Use OpenAI on compact weekly cluster summaries to catch semantic duplicates missed by deterministic rules.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sources = load_sources()
    if args.source:
        wanted = {name.lower() for name in args.source}
        sources = [source for source in sources if source["name"].lower() in wanted]
    if args.limit:
        sources = sources[: args.limit]

    print("Installed framework availability:")
    for name, available in installed_frameworks().items():
        print(f"  {name}: {'yes' if available else 'no'}")
    print(f"Sources selected: {len(sources)}")
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    print(f"Storage date filter: published_at >= {cutoff.isoformat()}")
    print(f"Include undated items: {'yes' if args.include_undated else 'no'}")

    conn = None
    if not args.dry_run:
        if not args.database_url:
            raise SystemExit("DATABASE_URL is not set. Provide --database-url or set the DATABASE_URL environment variable.")
        conn = connect(args.database_url)
        ensure_schema(conn)
        backfilled = backfill_event_fields(conn, cutoff)
        print("PostgreSQL connection and schema: OK")
        if backfilled:
            print(f"Backfilled event freshness fields for {backfilled} existing items.")

    total_items = 0
    total_skipped_old = 0
    total_duplicates = 0
    accessible = 0
    for index, source in enumerate(sources, start=1):
        print(f"[{index:02d}/{len(sources)}] Collecting {source['name']} ...", flush=True)
        result = test_source(source)
        accessible += int(bool(result.get("accessible")))
        print(
            f"  -> {'OK' if result.get('accessible') else 'FAIL'} via {result.get('success_mode')} "
            f"({result.get('entries_found', 0)} entries)"
        )
        if conn is not None:
            source_id = upsert_source(conn, source, result)
            kept, skipped_old, duplicates = upsert_items(
                conn,
                source_id,
                result,
                cutoff=cutoff,
                include_undated=args.include_undated,
            )
            total_items += kept
            total_skipped_old += skipped_old
            total_duplicates += duplicates
            conn.commit()

    if conn is not None:
        cluster_count = 0
        cluster_duplicates = 0
        if not args.skip_clustering:
            use_llm = bool(args.llm_cluster and os.environ.get("OPENAI_API_KEY"))
            if args.llm_cluster and not use_llm:
                print("LLM clustering requested, but OPENAI_API_KEY is not configured. Using deterministic clustering.")
            cluster_count, cluster_duplicates = upsert_story_clusters(
                conn,
                cutoff=cutoff,
                use_llm=use_llm,
                llm_model=args.llm_model,
                cluster_limit=args.cluster_limit,
                llm_batch_size=args.llm_batch_size,
                llm_refine_singletons=args.llm_refine_singletons,
            )
            global_groups = 0
            global_duplicate_clusters = 0
            if args.global_dedupe or args.llm_global_dedupe:
                use_global_llm = bool(args.llm_global_dedupe and os.environ.get("OPENAI_API_KEY"))
                if args.llm_global_dedupe and not use_global_llm:
                    print("LLM global dedupe requested, but OPENAI_API_KEY is not configured. Using deterministic global dedupe.")
                global_groups, global_duplicate_clusters = global_weekly_dedupe(
                    conn,
                    cutoff=cutoff,
                    use_llm=use_global_llm,
                    llm_model=args.llm_model,
                    llm_batch_size=args.llm_batch_size,
                )
        summary = write_run_summary(conn)
        conn.close()
        print(
            "Stored in PostgreSQL: "
            f"{summary['sources']} sources, {summary['items']} news items, "
            f"{summary['duplicate_items']} duplicate-linked items, "
            f"{summary['clusters']} story clusters, "
            f"{summary['accessible_sources']} accessible sources last run."
        )
        if not args.skip_clustering:
            print(f"Story clusters updated this run: {cluster_count}")
            print(f"Cluster duplicate-linked items this run: {cluster_duplicates}")
            if args.global_dedupe or args.llm_global_dedupe:
                print(f"Global duplicate groups merged this run: {global_groups}")
                print(f"Global duplicate clusters removed this run: {global_duplicate_clusters}")
    else:
        print(f"Dry run completed: {accessible}/{len(sources)} sources accessible.")
    print(f"Items inserted/updated this run: {total_items}")
    print(f"Items skipped by date filter: {total_skipped_old}")
    print(f"Duplicate-linked items this run: {total_duplicates}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
