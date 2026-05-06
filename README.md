# AI Newsletter Pipeline

This repo collects AI news from a curated source database, stores it in PostgreSQL, deduplicates and clusters stories, generates a business newsletter, and distributes an email-safe version.

## Main scripts

- `collect_ai_news_to_postgres.py`
  - Scrapes and stores recent AI news into PostgreSQL.
- `generate_agentic_newsletter.py`
  - Builds the HTML newsletter from weekly story clusters.
- `distribute_newsletter.py`
  - Converts the newsletter into an email-safe format and distributes it.
- `run_weekly_pipeline.py`
  - Orchestrates the full weekly pipeline.

## Setup

1. Create a virtual environment.
2. Install dependencies:

```powershell
python -m pip install -r requirements.txt
python -m playwright install chromium
```

3. Copy `.env.example` to `.env` and fill in values.

## Important production note

If you want the database to retain week-to-week history from GitHub Actions, use a persistent external PostgreSQL database and store its connection string in the `DATABASE_URL` GitHub secret.

GitHub-hosted runners are ephemeral, so a local runner database will not persist between scheduled runs.

## Local run

Dry-run email:

```powershell
.venv\Scripts\python.exe run_weekly_pipeline.py --dry-run-email
```

Real send:

```powershell
.venv\Scripts\python.exe run_weekly_pipeline.py --provider smtp
```

## GitHub Actions

The workflow file is:

- `.github/workflows/weekly-ai-newsletter.yml`

It supports:

- manual trigger via `workflow_dispatch`
- weekly run every Monday at 07:00 Asia/Dubai (`03:00 UTC`)

## Recommended GitHub secrets

Required:

- `DATABASE_URL`
- `OPENAI_API_KEY`

Email sending, choose one path:

SMTP:

- `NEWSLETTER_FROM_EMAIL`
- `NEWSLETTER_FROM_NAME`
- `SMTP_HOST`
- `SMTP_PORT`
- `SMTP_USERNAME`
- `SMTP_PASSWORD`
- `SMTP_USE_TLS`

Microsoft Graph:

- `NEWSLETTER_FROM_EMAIL`
- `NEWSLETTER_FROM_NAME`
- `GRAPH_TENANT_ID`
- `GRAPH_CLIENT_ID`
- `GRAPH_CLIENT_SECRET`
