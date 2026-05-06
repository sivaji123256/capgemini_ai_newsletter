"""
Unified AI news source access tester and lightweight collector.

This script uses only Python standard-library modules so it can run in a bare
environment. It follows the framework choices in
python_scraping_framework_recommendations.md and reports where optional
frameworks such as Crawl4AI, Scrapling, ScrapeGraphAI, Scrapy, or Playwright
would be the better next step.
"""

from __future__ import annotations

import csv
import asyncio
import datetime as dt
import importlib.util
import json
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from html import unescape
from pathlib import Path
from typing import Any

try:
    import feedparser
except Exception:  # pragma: no cover - optional runtime path
    feedparser = None

try:
    import httpx
except Exception:  # pragma: no cover - optional runtime path
    httpx = None

try:
    from parsel import Selector as ParselSelector
except Exception:  # pragma: no cover - optional runtime path
    ParselSelector = None

try:
    from scrapy.selector import Selector as ScrapySelector
except Exception:  # pragma: no cover - optional runtime path
    ScrapySelector = None

try:
    from scrapling.fetchers import Fetcher as ScraplingFetcher
except Exception:  # pragma: no cover - optional runtime path
    ScraplingFetcher = None

try:
    from playwright.sync_api import sync_playwright
except Exception:  # pragma: no cover - optional runtime path
    sync_playwright = None

try:
    from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
except Exception:  # pragma: no cover - optional runtime path
    AsyncWebCrawler = BrowserConfig = CrawlerRunConfig = None


ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = ROOT / "ai_news_collection_output"
COMPLETE_SOURCE_WORKBOOK = ROOT / "complete_ai_intelligence_master_database.xlsx"
TIMEOUT_SECONDS = 20
MAX_BYTES = 1_500_000

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36 "
    "AI-news-source-access-test/1.0"
)


SOURCES: list[dict[str, Any]] = [
    {
        "name": "OpenAI Blog",
        "category": "Frontier AI",
        "website": "https://openai.com/blog",
        "access": "RSS",
        "priority": "Critical",
        "framework": "feedparser/httpx -> Crawl4AI",
        "rss_candidates": ["https://openai.com/blog/rss.xml", "https://openai.com/news/rss.xml"],
    },
    {
        "name": "Google DeepMind Blog",
        "category": "Frontier AI",
        "website": "https://deepmind.google/discover/blog/",
        "access": "RSS",
        "priority": "Critical",
        "framework": "feedparser/httpx -> Crawl4AI",
        "rss_candidates": [
            "https://deepmind.google/discover/blog/rss.xml",
            "https://deepmind.google/blog/rss.xml",
            "https://deepmind.google/discover/blog/feed/basic/",
        ],
    },
    {
        "name": "Anthropic News",
        "category": "Frontier AI",
        "website": "https://www.anthropic.com/news",
        "access": "RSS",
        "priority": "Critical",
        "framework": "feedparser/httpx -> Crawl4AI",
        "rss_candidates": ["https://www.anthropic.com/news/rss.xml", "https://www.anthropic.com/rss.xml"],
    },
    {
        "name": "Meta AI Blog",
        "category": "Frontier AI",
        "website": "https://ai.meta.com/blog/",
        "access": "RSS",
        "priority": "Critical",
        "framework": "feedparser/httpx -> Crawl4AI",
        "rss_candidates": ["https://ai.meta.com/blog/rss/", "https://ai.meta.com/blog/rss.xml"],
    },
    {
        "name": "NVIDIA Technical Blog",
        "category": "AI Infrastructure",
        "website": "https://developer.nvidia.com/blog/",
        "access": "RSS",
        "priority": "Critical",
        "framework": "feedparser/Scrapy -> Crawl4AI",
        "rss_candidates": ["https://developer.nvidia.com/blog/feed/"],
    },
    {
        "name": "Hugging Face Blog",
        "category": "Open Source AI",
        "website": "https://huggingface.co/blog",
        "access": "RSS/API",
        "priority": "Critical",
        "framework": "httpx/feed -> Crawl4AI",
        "rss_candidates": ["https://huggingface.co/blog/feed.xml", "https://huggingface.co/blog/rss.xml"],
    },
    {
        "name": "arXiv cs.LG",
        "category": "Research",
        "website": "https://arxiv.org/list/cs.LG/recent",
        "access": "RSS/API",
        "priority": "Critical",
        "framework": "httpx/arXiv API",
        "rss_candidates": ["https://export.arxiv.org/rss/cs.LG"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.LG&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    {
        "name": "arXiv cs.CL",
        "category": "Research",
        "website": "https://arxiv.org/list/cs.CL/recent",
        "access": "RSS/API",
        "priority": "Critical",
        "framework": "httpx/arXiv API",
        "rss_candidates": ["https://export.arxiv.org/rss/cs.CL"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.CL&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    {
        "name": "arXiv cs.CV",
        "category": "Research",
        "website": "https://arxiv.org/list/cs.CV/recent",
        "access": "RSS/API",
        "priority": "Critical",
        "framework": "httpx/arXiv API",
        "rss_candidates": ["https://export.arxiv.org/rss/cs.CV"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.CV&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    {
        "name": "Papers with Code",
        "category": "Research",
        "website": "https://paperswithcode.com/",
        "access": "API",
        "priority": "Critical",
        "framework": "httpx API -> Scrapy optional",
        "api_url": "https://paperswithcode.com/api/v1/papers/?page=1",
        "rss_candidates": [],
    },
    {
        "name": "GitHub Trending AI",
        "category": "OSS Ecosystem",
        "website": "https://github.com/trending?spoken_language_code=en",
        "access": "API/HTML",
        "priority": "Critical",
        "framework": "GitHub API/httpx -> Scrapling optional",
        "api_url": "https://api.github.com/search/repositories?q=topic:artificial-intelligence&sort=updated&order=desc&per_page=5",
        "rss_candidates": [],
    },
    {
        "name": "Hacker News AI",
        "category": "Engineering Signal",
        "website": "https://news.ycombinator.com/",
        "access": "API",
        "priority": "High",
        "framework": "HN API/httpx",
        "api_url": "https://hacker-news.firebaseio.com/v0/topstories.json",
        "rss_candidates": ["https://hnrss.org/newest?q=AI"],
    },
    {
        "name": "TechCrunch AI",
        "category": "Media",
        "website": "https://techcrunch.com/category/artificial-intelligence/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI -> Playwright fallback",
        "rss_candidates": ["https://techcrunch.com/category/artificial-intelligence/feed/"],
    },
    {
        "name": "VentureBeat AI",
        "category": "Enterprise Media",
        "website": "https://venturebeat.com/ai/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://venturebeat.com/category/ai/feed/", "https://venturebeat.com/ai/feed/"],
    },
    {
        "name": "Reuters Tech",
        "category": "Business Intelligence",
        "website": "https://www.reuters.com/technology/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser/httpx metadata",
        "rss_candidates": ["https://www.reutersagency.com/feed/?best-topics=tech&post_type=best"],
    },
    {
        "name": "MIT Technology Review AI",
        "category": "AI Journalism",
        "website": "https://www.technologyreview.com/topic/artificial-intelligence/",
        "access": "RSS/paywall risk",
        "priority": "High",
        "framework": "feedparser metadata -> Crawl4AI accessible pages",
        "rss_candidates": ["https://www.technologyreview.com/feed/"],
    },
    {
        "name": "The Rundown AI",
        "category": "Newsletter",
        "website": "https://www.therundown.ai/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://www.therundown.ai/feed", "https://www.therundown.ai/rss"],
    },
    {
        "name": "Last Week in AI",
        "category": "Newsletter",
        "website": "https://lastweekin.ai/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://lastweekin.ai/feed", "https://lastweekin.ai/feed.xml"],
    },
    {
        "name": "Interconnects",
        "category": "Technical Newsletter",
        "website": "https://www.interconnects.ai/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://www.interconnects.ai/feed", "https://www.interconnects.ai/feed.xml"],
    },
    {
        "name": "Latent Space",
        "category": "AI Engineering",
        "website": "https://www.latent.space/",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://www.latent.space/feed", "https://www.latent.space/feed.xml"],
    },
    {
        "name": "LangChain Blog",
        "category": "Agents/RAG",
        "website": "https://www.langchain.com/blog",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://blog.langchain.com/rss/", "https://blog.langchain.dev/rss/"],
    },
    {
        "name": "LlamaIndex Blog",
        "category": "RAG Systems",
        "website": "https://www.llamaindex.ai/blog",
        "access": "RSS",
        "priority": "High",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://www.llamaindex.ai/blog/rss.xml", "https://www.llamaindex.ai/rss.xml"],
    },
    {
        "name": "Weights & Biases",
        "category": "MLOps",
        "website": "https://wandb.ai/fully-connected",
        "access": "RSS",
        "priority": "Medium",
        "framework": "feedparser -> Crawl4AI",
        "rss_candidates": ["https://wandb.ai/fully-connected/rss.xml", "https://wandb.ai/site/articles/rss.xml"],
    },
    {
        "name": "SemiAnalysis",
        "category": "AI Chips & Infra",
        "website": "https://www.semianalysis.com/",
        "access": "RSS/paywall risk",
        "priority": "High",
        "framework": "feedparser metadata only",
        "rss_candidates": ["https://www.semianalysis.com/feed", "https://www.semianalysis.com/feed.xml"],
    },
    {
        "name": "Reddit LocalLLaMA",
        "category": "Community",
        "website": "https://www.reddit.com/r/LocalLLaMA/",
        "access": "API",
        "priority": "Medium",
        "framework": "Reddit API/httpx",
        "api_url": "https://www.reddit.com/r/LocalLLaMA/top.json?t=week&limit=5",
        "rss_candidates": ["https://www.reddit.com/r/LocalLLaMA/.rss"],
    },
    {
        "name": "X/Twitter AI",
        "category": "Social Signal",
        "website": "https://x.com/",
        "access": "Paid API",
        "priority": "Medium",
        "framework": "Official X API only",
        "rss_candidates": [],
    },
]


SOURCE_OVERRIDES: dict[str, dict[str, Any]] = {
    "OpenAI Blog": {"rss_candidates": ["https://openai.com/blog/rss.xml", "https://openai.com/news/rss.xml"]},
    "Google DeepMind": {
        "rss_candidates": [
            "https://deepmind.google/discover/blog/rss.xml",
            "https://deepmind.google/blog/rss.xml",
            "https://deepmind.google/discover/blog/feed/basic/",
        ]
    },
    "Google DeepMind Blog": {
        "rss_candidates": [
            "https://deepmind.google/discover/blog/rss.xml",
            "https://deepmind.google/blog/rss.xml",
            "https://deepmind.google/discover/blog/feed/basic/",
        ]
    },
    "Anthropic News": {"rss_candidates": ["https://www.anthropic.com/news/rss.xml", "https://www.anthropic.com/rss.xml"]},
    "Meta AI Blog": {"rss_candidates": ["https://ai.meta.com/blog/rss/", "https://ai.meta.com/blog/rss.xml"]},
    "NVIDIA Technical Blog": {"rss_candidates": ["https://developer.nvidia.com/blog/feed/"]},
    "Hugging Face Blog": {"rss_candidates": ["https://huggingface.co/blog/feed.xml", "https://huggingface.co/blog/rss.xml"]},
    "TechCrunch AI": {"rss_candidates": ["https://techcrunch.com/category/artificial-intelligence/feed/"]},
    "VentureBeat AI": {"rss_candidates": ["https://venturebeat.com/category/ai/feed/", "https://venturebeat.com/ai/feed/"]},
    "Reuters Tech": {"rss_candidates": ["https://www.reutersagency.com/feed/?best-topics=tech&post_type=best"]},
    "MIT Technology Review": {"rss_candidates": ["https://www.technologyreview.com/feed/"]},
    "MIT Technology Review AI": {"rss_candidates": ["https://www.technologyreview.com/feed/"]},
    "SemiAnalysis": {"rss_candidates": ["https://www.semianalysis.com/feed", "https://www.semianalysis.com/feed.xml"]},
    "The Rundown AI": {"rss_candidates": ["https://www.therundown.ai/feed", "https://www.therundown.ai/rss"]},
    "Last Week in AI": {"rss_candidates": ["https://lastweekin.ai/feed", "https://lastweekin.ai/feed.xml"]},
    "Interconnects": {"rss_candidates": ["https://www.interconnects.ai/feed", "https://www.interconnects.ai/feed.xml"]},
    "Latent Space": {"rss_candidates": ["https://www.latent.space/feed", "https://www.latent.space/feed.xml"]},
    "TheSequence": {"rss_candidates": ["https://thesequence.substack.com/feed"]},
    "Ahead of AI": {"rss_candidates": ["https://magazine.sebastianraschka.com/feed"]},
    "Ben's Bites": {"rss_candidates": ["https://www.bensbites.com/feed", "https://www.bensbites.com/rss"]},
    "Superhuman AI": {"rss_candidates": ["https://www.superhuman.ai/feed", "https://www.superhuman.ai/rss"]},
    "arXiv cs.LG": {
        "rss_candidates": ["https://export.arxiv.org/rss/cs.LG"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.LG&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    "arXiv cs.CL": {
        "rss_candidates": ["https://export.arxiv.org/rss/cs.CL"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.CL&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    "arXiv cs.CV": {
        "rss_candidates": ["https://export.arxiv.org/rss/cs.CV"],
        "api_url": "https://export.arxiv.org/api/query?search_query=cat:cs.CV&sortBy=submittedDate&sortOrder=descending&max_results=5",
    },
    "Papers with Code": {
        "api_url": "https://paperswithcode.com/api/v1/papers/?page=1",
        "rss_candidates": [],
    },
    "Stanford CRFM": {"rss_candidates": ["https://crfm.stanford.edu/feed.xml", "https://crfm.stanford.edu/blog/feed.xml"]},
    "EleutherAI Blog": {"rss_candidates": ["https://blog.eleuther.ai/feed", "https://blog.eleuther.ai/rss/"]},
    "BAIR Blog": {"rss_candidates": ["https://bair.berkeley.edu/blog/feed.xml", "https://bair.berkeley.edu/blog/feed"]},
    "Google AI Blog": {"rss_candidates": ["https://blog.google/technology/ai/rss/"]},
    "Machine Learning Mastery": {"rss_candidates": ["https://machinelearningmastery.com/blog/feed/"]},
    "PyImageSearch": {"rss_candidates": ["https://pyimagesearch.com/feed/"]},
    "LangChain Blog": {
        "website": "https://www.langchain.com/blog",
        "rss_candidates": ["https://blog.langchain.com/rss/", "https://blog.langchain.dev/rss/"],
    },
    "LlamaIndex Blog": {"rss_candidates": ["https://www.llamaindex.ai/blog/rss.xml", "https://www.llamaindex.ai/rss.xml"]},
    "Weights & Biases": {"rss_candidates": ["https://wandb.ai/fully-connected/rss.xml", "https://wandb.ai/site/articles/rss.xml"]},
    "Neptune.ai": {"rss_candidates": ["https://neptune.ai/blog/feed"]},
    "Databricks Blog": {"rss_candidates": ["https://www.databricks.com/feed"]},
    "Replicate Blog": {"rss_candidates": ["https://replicate.com/blog/rss.xml", "https://replicate.com/blog/feed.xml"]},
    "AssemblyAI Blog": {"rss_candidates": ["https://www.assemblyai.com/blog/rss.xml", "https://www.assemblyai.com/blog/feed.xml"]},
    "Netflix Tech Blog": {"rss_candidates": ["https://netflixtechblog.com/feed"]},
    "Uber Engineering AI": {"rss_candidates": ["https://www.uber.com/blog/engineering/ai/feed/", "https://eng.uber.com/feed/"]},
    "GitHub Trending": {
        "api_url": "https://api.github.com/search/repositories?q=topic:artificial-intelligence&sort=updated&order=desc&per_page=5",
        "rss_candidates": [],
    },
    "GitHub Trending AI": {
        "api_url": "https://api.github.com/search/repositories?q=topic:artificial-intelligence&sort=updated&order=desc&per_page=5",
        "rss_candidates": [],
    },
    "Hacker News": {
        "api_url": "https://hacker-news.firebaseio.com/v0/topstories.json",
        "rss_candidates": ["https://hnrss.org/newest?q=AI"],
    },
    "Hacker News AI": {
        "api_url": "https://hacker-news.firebaseio.com/v0/topstories.json",
        "rss_candidates": ["https://hnrss.org/newest?q=AI"],
    },
    "Reddit r/MachineLearning": {
        "api_url": "https://www.reddit.com/r/MachineLearning/top.json?t=week&limit=5",
        "rss_candidates": ["https://www.reddit.com/r/MachineLearning/.rss"],
    },
    "Reddit LocalLLaMA": {
        "api_url": "https://www.reddit.com/r/LocalLLaMA/top.json?t=week&limit=5",
        "rss_candidates": ["https://www.reddit.com/r/LocalLLaMA/.rss"],
    },
    "Product Hunt": {
        "rss_candidates": ["https://www.producthunt.com/feed"],
    },
    "Towards Data Science": {"rss_candidates": ["https://towardsdatascience.com/feed"]},
    "Towards AI": {"rss_candidates": ["https://pub.towardsai.net/feed"]},
    "Analytics India Magazine": {"rss_candidates": ["https://analyticsindiamag.com/feed/"]},
    "MarkTechPost": {"rss_candidates": ["https://www.marktechpost.com/feed/"]},
    "The Decoder": {"rss_candidates": ["https://the-decoder.com/feed/"]},
    "InfoQ AI": {"rss_candidates": ["https://feed.infoq.com/ai-ml-data-eng"]},
    "IEEE Spectrum AI": {"rss_candidates": ["https://spectrum.ieee.org/feeds/topic/artificial-intelligence.rss"]},
    "ZDNet AI": {"rss_candidates": ["https://www.zdnet.com/topic/artificial-intelligence/rss.xml"]},
    "The Verge AI": {"rss_candidates": ["https://www.theverge.com/rss/ai-artificial-intelligence/index.xml"]},
    "Wired AI": {"rss_candidates": ["https://www.wired.com/feed/tag/ai/latest/rss"]},
}


def installed_frameworks() -> dict[str, bool]:
    modules = [
        "requests",
        "httpx",
        "feedparser",
        "scrapy",
        "crawl4ai",
        "scrapling",
        "scrapegraphai",
        "playwright",
        "bs4",
        "lxml",
    ]
    return {name: importlib.util.find_spec(name) is not None for name in modules}


def xlsx_rows(path: Path, sheet_name: str) -> list[list[str]]:
    ns = {"a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
    rel_id = "{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id"

    def colnum(ref: str) -> int:
        match = re.match(r"([A-Z]+)", ref)
        if not match:
            return 0
        number = 0
        for char in match.group(1):
            number = number * 26 + ord(char) - 64
        return number - 1

    def cell_text(cell: ET.Element, shared_strings: list[str]) -> str:
        if cell.attrib.get("t") == "inlineStr":
            return "".join(node.text or "" for node in cell.findall(".//a:t", ns)).strip()
        value_node = cell.find("a:v", ns)
        value = "" if value_node is None else value_node.text or ""
        if cell.attrib.get("t") == "s" and value:
            return shared_strings[int(value)].strip()
        return value.strip()

    import zipfile

    with zipfile.ZipFile(path) as archive:
        workbook = ET.fromstring(archive.read("xl/workbook.xml"))
        relationships = ET.fromstring(archive.read("xl/_rels/workbook.xml.rels"))
        rel_map = {node.attrib["Id"]: node.attrib["Target"] for node in relationships}
        shared_strings: list[str] = []
        if "xl/sharedStrings.xml" in archive.namelist():
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            shared_strings = [
                "".join(text_node.text or "" for text_node in item.findall(".//a:t", ns))
                for item in root.findall("a:si", ns)
            ]

        sheet = next(
            node for node in workbook.find("a:sheets", ns) if node.attrib["name"] == sheet_name
        )
        target = rel_map[sheet.attrib[rel_id]].lstrip("/")
        sheet_path = f"xl/{target}" if not target.startswith("xl/") else target
        root = ET.fromstring(archive.read(sheet_path))
        rows: list[list[str]] = []
        for row in root.findall(".//a:sheetData/a:row", ns):
            values = {
                colnum(cell.attrib["r"]): cell_text(cell, shared_strings)
                for cell in row.findall("a:c", ns)
            }
            if not values:
                continue
            result = [values.get(index, "") for index in range(max(values) + 1)]
            if any(value.strip() for value in result):
                rows.append(result)
        return rows


def generic_rss_candidates(website: str) -> list[str]:
    if not website:
        return []
    url = website.rstrip("/")
    parsed = urllib.parse.urlparse(url)
    candidates = [
        f"{url}/feed",
        f"{url}/feed/",
        f"{url}/rss",
        f"{url}/rss.xml",
        f"{url}/feed.xml",
    ]
    if "substack.com" in parsed.netloc or "medium.com" in parsed.netloc:
        candidates.insert(0, f"{url}/feed")
    if "youtube.com" in parsed.netloc:
        candidates = []
    return list(dict.fromkeys(candidates))


def choose_framework_for_source(source: dict[str, str]) -> str:
    access = f"{source.get('access_model', '')} {source.get('rss_api', '')}".lower()
    category = f"{source.get('category', '')} {source.get('subcategory', '')}".lower()
    website = source.get("website", "").lower()
    if "x.com" in website or "twitter" in source.get("name", "").lower():
        return "Official API only"
    if "api" in access and "rss" not in access:
        return "httpx API -> source-specific parser"
    if "rss" in access or "youtube" in category:
        return "feedparser/httpx -> Crawl4AI for selected article pages"
    if "paywall" in access:
        return "feedparser metadata/licensed API -> Crawl4AI only for accessible pages"
    return "httpx/Parsel -> Crawl4AI -> Scrapling/Playwright fallback"


def load_sources() -> list[dict[str, Any]]:
    if not COMPLETE_SOURCE_WORKBOOK.exists():
        return SOURCES

    rows = xlsx_rows(COMPLETE_SOURCE_WORKBOOK, "Master_AI_Intelligence_DB")
    if not rows:
        return SOURCES
    header = [column.strip().lower().replace(" ", "_").replace("/", "_") for column in rows[0]]
    loaded: list[dict[str, Any]] = []
    for raw_row in rows[1:]:
        row = {header[index]: raw_row[index] if index < len(raw_row) else "" for index in range(len(header))}
        name = row.get("source_name", "").strip()
        website = row.get("website", "").strip()
        if not name or not website:
            continue
        source = {
            "name": name,
            "category": row.get("category", ""),
            "subcategory": row.get("subcategory", ""),
            "website": website,
            "access": f"{row.get('access_model', '')} / {row.get('rss_api', '')}".strip(" /"),
            "priority": row.get("recommended_priority", ""),
            "framework": choose_framework_for_source(
                {
                    "name": name,
                    "website": website,
                    "access_model": row.get("access_model", ""),
                    "rss_api": row.get("rss_api", ""),
                    "category": row.get("category", ""),
                    "subcategory": row.get("subcategory", ""),
                }
            ),
            "rss_candidates": generic_rss_candidates(website),
        }
        override = SOURCE_OVERRIDES.get(name, {})
        source.update(override)
        source["rss_candidates"] = list(
            dict.fromkeys(source.get("rss_candidates", []) + generic_rss_candidates(source.get("website", website)))
        )
        loaded.append(source)
    return loaded or SOURCES


def fetch_url(url: str, method: str = "GET") -> dict[str, Any]:
    if httpx is not None:
        return fetch_url_httpx(url, method)

    request = urllib.request.Request(
        url,
        method=method,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/rss+xml, application/atom+xml, application/json, text/html;q=0.9, */*;q=0.8",
            "Accept-Language": "en-US,en;q=0.8",
        },
    )
    context = ssl.create_default_context()
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(request, timeout=TIMEOUT_SECONDS, context=context) as response:
            body = response.read(MAX_BYTES)
            elapsed_ms = round((time.perf_counter() - started) * 1000)
            final_url = response.geturl()
            headers = dict(response.headers.items())
            return {
                "ok": 200 <= response.status < 400,
                "status": response.status,
                "url": url,
                "final_url": final_url,
                "elapsed_ms": elapsed_ms,
                "content_type": headers.get("Content-Type", ""),
                "body": body,
                "error": "",
            }
    except urllib.error.HTTPError as exc:
        body = exc.read(min(MAX_BYTES, 200_000)) if exc.fp else b""
        return {
            "ok": False,
            "status": exc.code,
            "url": url,
            "final_url": exc.geturl(),
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": exc.headers.get("Content-Type", "") if exc.headers else "",
            "body": body,
            "error": f"HTTPError: {exc.reason}",
        }
    except Exception as exc:  # noqa: BLE001 - access test should capture all failure classes.
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "",
            "body": b"",
            "error": f"{type(exc).__name__}: {exc}",
        }


def fetch_url_httpx(url: str, method: str = "GET") -> dict[str, Any]:
    started = time.perf_counter()
    try:
        with httpx.Client(
            timeout=TIMEOUT_SECONDS,
            follow_redirects=True,
            headers={
                "User-Agent": USER_AGENT,
                "Accept": "application/rss+xml, application/atom+xml, application/json, text/html;q=0.9, */*;q=0.8",
                "Accept-Language": "en-US,en;q=0.8",
            },
        ) as client:
            response = client.request(method, url)
            body = response.content[:MAX_BYTES]
            return {
                "ok": 200 <= response.status_code < 400,
                "status": response.status_code,
                "url": url,
                "final_url": str(response.url),
                "elapsed_ms": round((time.perf_counter() - started) * 1000),
                "content_type": response.headers.get("Content-Type", ""),
                "body": body,
                "error": "" if response.is_success else f"HTTPStatusError: {response.reason_phrase}",
                "framework_used": "httpx",
            }
    except Exception as exc:  # noqa: BLE001 - access test should capture all failure classes.
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "",
            "body": b"",
            "error": f"{type(exc).__name__}: {exc}",
            "framework_used": "httpx",
        }


def fetch_url_scrapling(url: str) -> dict[str, Any]:
    if ScraplingFetcher is None:
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": 0,
            "content_type": "",
            "body": b"",
            "error": "Scrapling is not installed.",
            "framework_used": "scrapling",
        }
    started = time.perf_counter()
    try:
        response = ScraplingFetcher.get(url, timeout=TIMEOUT_SECONDS, retries=1)
        text = getattr(response, "text", None)
        if text is None:
            text = str(response)
        status = getattr(response, "status", None) or getattr(response, "status_code", None)
        final_url = str(getattr(response, "url", url))
        body = text.encode("utf-8", errors="replace")[:MAX_BYTES]
        return {
            "ok": status is None or 200 <= int(status) < 400,
            "status": int(status) if status is not None else None,
            "url": url,
            "final_url": final_url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "text/html",
            "body": body,
            "error": "",
            "framework_used": "scrapling",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "",
            "body": b"",
            "error": f"{type(exc).__name__}: {exc}",
            "framework_used": "scrapling",
        }


def fetch_url_playwright(url: str) -> dict[str, Any]:
    if sync_playwright is None:
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": 0,
            "content_type": "",
            "body": b"",
            "error": "Playwright is not installed.",
            "framework_used": "playwright",
        }
    started = time.perf_counter()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page(user_agent=USER_AGENT)
            response = page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_SECONDS * 1000)
            page.wait_for_timeout(1200)
            html = page.content()
            final_url = page.url
            status = response.status if response else None
            browser.close()
        return {
            "ok": status is None or 200 <= int(status) < 400,
            "status": int(status) if status is not None else None,
            "url": url,
            "final_url": final_url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "text/html",
            "body": html.encode("utf-8", errors="replace")[:MAX_BYTES],
            "error": "",
            "framework_used": "playwright",
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "status": None,
            "url": url,
            "final_url": url,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "content_type": "",
            "body": b"",
            "error": f"{type(exc).__name__}: {exc}",
            "framework_used": "playwright",
        }


def text_from_body(body: bytes, content_type: str) -> str:
    encoding = "utf-8"
    match = re.search(r"charset=([\w.-]+)", content_type or "", re.I)
    if match:
        encoding = match.group(1)
    try:
        return body.decode(encoding, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def looks_like_feed(text: str) -> bool:
    sample = text.lstrip()[:500].lower()
    return sample.startswith("<?xml") or "<rss" in sample or "<feed" in sample or "<rdf:rdf" in sample


def parse_feed_entries(text: str, limit: int = 5) -> list[dict[str, str]]:
    if feedparser is not None:
        parsed = feedparser.parse(text)
        entries = []
        for entry in parsed.entries[:limit]:
            entries.append(
                {
                    "title": getattr(entry, "title", "") or entry.get("title", ""),
                    "link": getattr(entry, "link", "") or entry.get("link", ""),
                    "published": getattr(entry, "published", "") or entry.get("updated", ""),
                }
            )
        if entries:
            return entries

    try:
        root = ET.fromstring(text.encode("utf-8"))
    except ET.ParseError:
        return []

    entries: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        entries.append(
            {
                "title": first_child_text(item, "title"),
                "link": first_child_text(item, "link"),
                "published": first_child_text(item, "pubDate") or first_child_text(item, "date"),
            }
        )
        if len(entries) >= limit:
            return entries

    ns = {"atom": "http://www.w3.org/2005/Atom"}
    for entry in root.findall(".//atom:entry", ns):
        link = ""
        link_el = entry.find("atom:link", ns)
        if link_el is not None:
            link = link_el.attrib.get("href", "")
        entries.append(
            {
                "title": first_child_text(entry, "{http://www.w3.org/2005/Atom}title"),
                "link": link,
                "published": first_child_text(entry, "{http://www.w3.org/2005/Atom}updated")
                or first_child_text(entry, "{http://www.w3.org/2005/Atom}published"),
            }
        )
        if len(entries) >= limit:
            break
    return entries


def first_child_text(node: ET.Element, tag: str) -> str:
    child = node.find(tag)
    return (child.text or "").strip() if child is not None else ""


def html_title(text: str) -> str:
    match = re.search(r"<title[^>]*>(.*?)</title>", text, flags=re.I | re.S)
    if not match:
        return ""
    return re.sub(r"\s+", " ", unescape(match.group(1))).strip()


def discover_feed_links(text: str, base_url: str) -> list[str]:
    selector_cls = ScrapySelector or ParselSelector
    if selector_cls is not None:
        selector = selector_cls(text=text)
        hrefs = selector.css(
            'link[type="application/rss+xml"]::attr(href), '
            'link[type="application/atom+xml"]::attr(href), '
            'link[type="text/xml"]::attr(href)'
        ).getall()
        return list(dict.fromkeys(urllib.parse.urljoin(base_url, unescape(href)) for href in hrefs))

    links: list[str] = []
    for match in re.finditer(r"<link\b[^>]+>", text, flags=re.I):
        tag = match.group(0)
        if not re.search(r'type=["\']?(application/(rss|atom)\+xml|text/xml)', tag, flags=re.I):
            continue
        href_match = re.search(r'href=["\']([^"\']+)["\']', tag, flags=re.I)
        if href_match:
            links.append(urllib.parse.urljoin(base_url, unescape(href_match.group(1))))
    return list(dict.fromkeys(links))


def extract_article_links(text: str, base_url: str, limit: int = 5) -> list[dict[str, str]]:
    selector_cls = ScrapySelector or ParselSelector
    if selector_cls is not None:
        base = urllib.parse.urlparse(base_url)
        selector = selector_cls(text=text)
        entries: list[dict[str, str]] = []
        seen: set[str] = set()
        for anchor in selector.css("a"):
            href = anchor.css("::attr(href)").get()
            if not href:
                continue
            url = urllib.parse.urljoin(base_url, unescape(href))
            parsed = urllib.parse.urlparse(url)
            if parsed.netloc and parsed.netloc != base.netloc:
                continue
            path = parsed.path.lower()
            if not any(token in path for token in ["/blog", "/news", "/post", "/p/", "/article", "/research", "/papers/"]):
                continue
            title = " ".join(part.strip() for part in anchor.css("::text").getall() if part.strip())
            title = re.sub(r"\s+", " ", unescape(title)).strip()
            if len(title) < 12:
                continue
            clean_url = urllib.parse.urlunparse(parsed._replace(fragment=""))
            if clean_url in seen:
                continue
            seen.add(clean_url)
            entries.append({"title": title[:180], "link": clean_url, "published": ""})
            if len(entries) >= limit:
                return entries
        return entries

    base = urllib.parse.urlparse(base_url)
    entries: list[dict[str, str]] = []
    seen: set[str] = set()
    anchor_re = re.compile(r"<a\b[^>]*href=[\"']([^\"']+)[\"'][^>]*>(.*?)</a>", re.I | re.S)
    for href, inner_html in anchor_re.findall(text):
        url = urllib.parse.urljoin(base_url, unescape(href))
        parsed = urllib.parse.urlparse(url)
        if parsed.netloc and parsed.netloc != base.netloc:
            continue
        path = parsed.path.lower()
        if not any(token in path for token in ["/blog", "/news", "/post", "/p/", "/article", "/research"]):
            continue
        title = re.sub(r"<[^>]+>", " ", inner_html)
        title = re.sub(r"\s+", " ", unescape(title)).strip()
        if len(title) < 12:
            continue
        clean_url = urllib.parse.urlunparse(parsed._replace(fragment=""))
        if clean_url in seen:
            continue
        seen.add(clean_url)
        entries.append({"title": title[:180], "link": clean_url, "published": ""})
        if len(entries) >= limit:
            break
    return entries


async def crawl4ai_extract(url: str) -> dict[str, Any]:
    if AsyncWebCrawler is None:
        return {"ok": False, "error": "Crawl4AI is not installed.", "markdown_chars": 0, "framework_used": "crawl4ai"}
    started = time.perf_counter()
    try:
        browser_config = BrowserConfig(
            browser_type="chromium",
            headless=True,
            verbose=False,
            user_agent=USER_AGENT,
            enable_stealth=True,
        )
        run_config = CrawlerRunConfig(
            word_count_threshold=10,
            page_timeout=TIMEOUT_SECONDS * 1000,
            wait_until="domcontentloaded",
            remove_overlay_elements=True,
            process_iframes=False,
            magic=True,
            verbose=False,
        )
        async with AsyncWebCrawler(config=browser_config, base_directory=str(ROOT)) as crawler:
            result = await crawler.arun(url=url, config=run_config)
        markdown = getattr(result, "markdown", "") or ""
        success = bool(getattr(result, "success", False))
        error = getattr(result, "error_message", "") or ""
        return {
            "ok": success and bool(markdown.strip()),
            "error": error,
            "markdown_chars": len(markdown),
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "framework_used": "crawl4ai",
            "sample_markdown": markdown[:500],
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "error": f"{type(exc).__name__}: {exc}",
            "markdown_chars": 0,
            "elapsed_ms": round((time.perf_counter() - started) * 1000),
            "framework_used": "crawl4ai",
        }


def run_crawl4ai_extract(url: str) -> dict[str, Any]:
    try:
        return asyncio.run(crawl4ai_extract(url))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(crawl4ai_extract(url))
        finally:
            loop.close()


def extract_api_preview(source: dict[str, Any], text: str) -> list[dict[str, Any]]:
    if not text.strip():
        return []
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        if looks_like_feed(text):
            return parse_feed_entries(text)
        return []

    name = source["name"]
    if name == "GitHub Trending AI":
        return [
            {
                "title": item.get("full_name", ""),
                "link": item.get("html_url", ""),
                "published": item.get("updated_at", ""),
            }
            for item in data.get("items", [])[:5]
        ]
    if name == "Papers with Code":
        results = data.get("results", data if isinstance(data, list) else [])
        return [
            {
                "title": item.get("title", ""),
                "link": item.get("url_abs") or item.get("paper_url") or item.get("url", ""),
                "published": item.get("published", ""),
            }
            for item in results[:5]
        ]
    if name == "Hacker News AI" and isinstance(data, list):
        return [{"title": f"Top story id {story_id}", "link": f"https://news.ycombinator.com/item?id={story_id}", "published": ""} for story_id in data[:5]]
    if name == "Reddit LocalLLaMA":
        children = data.get("data", {}).get("children", [])
        return [
            {
                "title": child.get("data", {}).get("title", ""),
                "link": "https://www.reddit.com" + child.get("data", {}).get("permalink", ""),
                "published": str(child.get("data", {}).get("created_utc", "")),
            }
            for child in children[:5]
        ]
    return []


def diagnose(source: dict[str, Any], attempts: list[dict[str, Any]], success_mode: str) -> str:
    if success_mode:
        if success_mode.startswith("playwright"):
            return "Accessible through Playwright browser rendering after HTTP/static access failed."
        if success_mode.startswith("scrapling"):
            return "Accessible through Scrapling adaptive fetching after HTTP/static access failed."
        if success_mode == "website_html":
            return "Website is reachable, but no structured feed/API entries were extracted. Use Crawl4AI or add a site-specific parser for full article extraction."
        if success_mode == "html_links":
            return "Website is reachable and recent links were extracted from HTML. A feed/API or Crawl4AI parser would be more robust."
        return "Accessible with selected baseline path."

    statuses = [a.get("status") for a in attempts if a.get("status") is not None]
    errors = " | ".join(a.get("error", "") for a in attempts if a.get("error"))
    content_types = " | ".join(a.get("content_type", "") for a in attempts if a.get("content_type"))

    if source["name"] == "X/Twitter AI":
        return "Expected limitation: X/Twitter should use the official paid API, not page scraping."
    if any(status in {401, 403, 429} for status in statuses):
        return "Blocked, unauthorized, or rate-limited. Use official API/auth, reduce rate, or consider Scrapling/Playwright only if allowed by ToS."
    if any(status == 404 for status in statuses):
        return "Candidate feed/API URL not found. Discover the current feed link from the website HTML or use site-specific API documentation."
    if any(status and 500 <= status < 600 for status in statuses):
        return "Remote server error. Retry later with backoff before changing frameworks."
    if "timed out" in errors.lower() or "TimeoutError" in errors:
        return "Network timeout. Retry with longer timeout/backoff; if persistent, test from a server region closer to the source."
    if "json" in content_types.lower() or "xml" in content_types.lower():
        return "Response arrived but parser did not extract entries. Inspect schema and add a source-specific parser."
    if attempts:
        return "Website reachable path did not expose structured entries. Use Crawl4AI for page extraction, or Playwright if content is rendered by JavaScript."
    return "No usable access path configured."


def optimal_fallback(source: dict[str, Any], success_mode: str) -> str:
    if success_mode in {"rss", "api", "api/feed"}:
        return "Keep current selected framework."
    if success_mode.startswith("playwright"):
        return "Keep Playwright for this source and use Crawl4AI on the rendered/article URLs."
    if success_mode.startswith("scrapling"):
        return "Keep Scrapling for this source and add source-specific parsing rules."
    if success_mode in {"website_html", "html_links"}:
        return "Use Crawl4AI for robust article extraction; add source-specific feed discovery if available."
    access = source.get("access", "").lower()
    framework = source.get("framework", "")
    if "paid api" in access:
        return "Use official API/export only."
    if "api" in access and not source.get("api_url"):
        return "Find official API endpoint and authenticate if required."
    if "paywall" in access:
        return "Use RSS metadata and licensed/user-authorized access; avoid paywall scraping."
    if "Crawl4AI" in framework:
        return "Install/use Crawl4AI for accessible article pages; add Playwright only for JS-rendered pages."
    if "Scrapling" in framework:
        return "Install/use Scrapling for adaptive selectors or anti-bot friction."
    return "Try Crawl4AI for clean article extraction; Scrapy if this becomes a scheduled crawler."


def test_source(source: dict[str, Any]) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    success_mode = ""
    crawl4ai_result: dict[str, Any] | None = None

    for rss_url in source.get("rss_candidates", []):
        result = fetch_url(rss_url)
        attempts.append({k: v for k, v in result.items() if k != "body"})
        text = text_from_body(result["body"], result["content_type"])
        if result["ok"] and looks_like_feed(text):
            entries = parse_feed_entries(text)
            if entries:
                success_mode = "rss"
                break

    if not success_mode and source.get("api_url"):
        result = fetch_url(source["api_url"])
        attempts.append({k: v for k, v in result.items() if k != "body"})
        text = text_from_body(result["body"], result["content_type"])
        api_entries = extract_api_preview(source, text)
        if result["ok"] and api_entries:
            entries = api_entries
            success_mode = "api"
        elif result["ok"] and looks_like_feed(text):
            entries = parse_feed_entries(text)
            if entries:
                success_mode = "api/feed"

    website_result: dict[str, Any] | None = None
    website_text = ""
    if not success_mode and source.get("website") and source["name"] != "X/Twitter AI":
        result = fetch_url(source["website"])
        website_result = result
        attempts.append({k: v for k, v in result.items() if k != "body"})
        text = text_from_body(result["body"], result["content_type"])
        website_text = text
        if result["ok"]:
            for feed_url in discover_feed_links(text, result["final_url"]):
                feed_result = fetch_url(feed_url)
                attempts.append({k: v for k, v in feed_result.items() if k != "body"})
                feed_text = text_from_body(feed_result["body"], feed_result["content_type"])
                if feed_result["ok"] and looks_like_feed(feed_text):
                    entries = parse_feed_entries(feed_text)
                    if entries:
                        success_mode = "discovered_rss"
                        break

    if not success_mode and website_result and website_result["ok"]:
        html_entries = extract_article_links(website_text, website_result["final_url"])
        if html_entries:
            success_mode = "html_links"
            entries = html_entries
        else:
            title = html_title(website_text)
            if title:
                success_mode = "website_html"
                entries = [{"title": title, "link": website_result["final_url"], "published": ""}]

    if not success_mode and source.get("website") and source["name"] != "X/Twitter AI":
        for framework_name, fetcher in [("scrapling", fetch_url_scrapling), ("playwright", fetch_url_playwright)]:
            result = fetcher(source["website"])
            attempts.append({k: v for k, v in result.items() if k != "body"})
            text = text_from_body(result["body"], result["content_type"])
            if result["ok"]:
                html_entries = extract_article_links(text, result["final_url"])
                if html_entries:
                    success_mode = f"{framework_name}_html_links"
                    entries = html_entries
                    break
                title = html_title(text)
                if title:
                    success_mode = f"{framework_name}_html"
                    entries = [{"title": title, "link": result["final_url"], "published": ""}]
                    break

    if not success_mode and source["name"] == "X/Twitter AI":
        attempts.append(
            {
                "ok": False,
                "status": None,
                "url": source["website"],
                "final_url": source["website"],
                "elapsed_ms": 0,
                "content_type": "",
                "error": "Skipped by design: official paid API required.",
                "framework_used": "official_api_required",
            }
        )

    needs_page_extraction = (
        bool(success_mode)
        and "Crawl4AI" in source.get("framework", "")
        and success_mode not in {"rss", "api", "api/feed", "discovered_rss"}
    )
    if needs_page_extraction:
        target_url = entries[0].get("link") if entries else source.get("website")
        if target_url:
            crawl4ai_result = run_crawl4ai_extract(target_url)

    return {
        "source": source["name"],
        "category": source["category"],
        "priority": source["priority"],
        "selected_framework": source["framework"],
        "accessible": bool(success_mode),
        "success_mode": success_mode or "none",
        "entries_found": len(entries),
        "entries": entries,
        "sample_entries": entries[:3],
        "page_extraction": crawl4ai_result,
        "attempts": attempts,
        "diagnosis": diagnose(source, attempts, success_mode),
        "optimal_next_solution": optimal_fallback(source, success_mode),
    }


def write_reports(results: list[dict[str, Any]], frameworks: dict[str, bool]) -> dict[str, str]:
    OUTPUT_DIR.mkdir(exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
    json_path = OUTPUT_DIR / f"ai_news_access_report_{stamp}.json"
    csv_path = OUTPUT_DIR / f"ai_news_access_report_{stamp}.csv"
    md_path = OUTPUT_DIR / f"ai_news_access_report_{stamp}.md"

    summary = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "python": sys.version,
        "installed_frameworks": frameworks,
        "total_sources": len(results),
        "accessible_sources": sum(1 for r in results if r["accessible"]),
        "inaccessible_sources": sum(1 for r in results if not r["accessible"]),
        "results": results,
    }
    json_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "source",
                "category",
                "priority",
                "selected_framework",
                "accessible",
                "success_mode",
                "entries_found",
                "page_extraction_status",
                "diagnosis",
                "optimal_next_solution",
            ],
        )
        writer.writeheader()
        for row in results:
            csv_row = {key: row.get(key, "") for key in writer.fieldnames}
            extraction = row.get("page_extraction") or {}
            if extraction:
                csv_row["page_extraction_status"] = (
                    f"{extraction.get('framework_used')} ok={extraction.get('ok')} "
                    f"markdown_chars={extraction.get('markdown_chars')} error={extraction.get('error', '')[:120]}"
                )
            writer.writerow(csv_row)

    lines = [
        "# AI news source access report",
        "",
        f"Generated: {summary['generated_at']}",
        "",
        f"Accessible sources: {summary['accessible_sources']} / {summary['total_sources']}",
        "",
        "## Installed framework check",
        "",
    ]
    for name, available in frameworks.items():
        lines.append(f"- `{name}`: {'installed' if available else 'not installed'}")
    lines.extend(["", "## Source results", ""])
    for row in results:
        status = "OK" if row["accessible"] else "FAIL"
        lines.extend(
            [
                f"### {row['source']} - {status}",
                "",
                f"- Selected framework: `{row['selected_framework']}`",
                f"- Success mode: `{row['success_mode']}`",
                f"- Entries found: `{row['entries_found']}`",
                f"- Diagnosis: {row['diagnosis']}",
                f"- Optimal next solution: {row['optimal_next_solution']}",
            ]
        )
        extraction = row.get("page_extraction")
        if extraction:
            lines.append(
                f"- Crawl4AI extraction: ok=`{extraction.get('ok')}`, "
                f"markdown chars=`{extraction.get('markdown_chars')}`, "
                f"error=`{extraction.get('error', '')[:160]}`"
            )
        if row["sample_entries"]:
            sample = row["sample_entries"][0]
            lines.append(f"- Sample: {sample.get('title', '')} ({sample.get('link', '')})")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")

    return {"json": str(json_path), "csv": str(csv_path), "md": str(md_path)}


def main() -> int:
    frameworks = installed_frameworks()
    sources = load_sources()
    print("Installed framework availability:")
    for name, available in frameworks.items():
        print(f"  {name}: {'yes' if available else 'no'}")
    print()
    print(f"Loaded sources: {len(sources)}")
    print(f"Source workbook: {COMPLETE_SOURCE_WORKBOOK if COMPLETE_SOURCE_WORKBOOK.exists() else 'fallback hardcoded list'}")
    print()

    results = []
    for index, source in enumerate(sources, start=1):
        print(f"[{index:02d}/{len(sources)}] Testing {source['name']} ...", flush=True)
        result = test_source(source)
        results.append(result)
        print(
            f"  -> {'OK' if result['accessible'] else 'FAIL'} via {result['success_mode']} "
            f"({result['entries_found']} entries)"
        )

    paths = write_reports(results, frameworks)
    accessible = sum(1 for r in results if r["accessible"])
    print()
    print(f"Accessible sources: {accessible}/{len(results)}")
    print("Reports written:")
    for kind, path in paths.items():
        print(f"  {kind}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
