from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parent


def run_step(args: list[str], env: dict[str, str]) -> None:
    print(f"Running: {' '.join(args)}", flush=True)
    subprocess.run(args, cwd=ROOT, env=env, check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the weekly AI newsletter pipeline end to end.")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--newsletter-limit", type=int, default=12)
    parser.add_argument("--llm-model", default="gpt-4.1-mini")
    parser.add_argument("--provider", choices=("graph", "smtp"), default="smtp")
    parser.add_argument("--dry-run-email", action="store_true")
    parser.add_argument("--skip-send", action="store_true")
    parser.add_argument("--source-limit", type=int, default=0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    python = sys.executable
    env = os.environ.copy()
    env.setdefault("OPENBLAS_NUM_THREADS", "1")

    collect_cmd = [
        python,
        "collect_ai_news_to_postgres.py",
        "--days",
        str(args.days),
        "--llm-cluster",
        "--global-dedupe",
        "--llm-global-dedupe",
        "--llm-model",
        args.llm_model,
    ]
    if args.source_limit:
        collect_cmd.extend(["--limit", str(args.source_limit)])

    generate_cmd = [
        python,
        "generate_agentic_newsletter.py",
        "--days",
        str(args.days),
        "--limit",
        str(args.newsletter_limit),
        "--model",
        args.llm_model,
    ]

    distribute_cmd = [
        python,
        "distribute_newsletter.py",
        "--provider",
        args.provider,
        "--approve",
    ]
    if args.skip_send or args.dry_run_email:
        distribute_cmd.append("--dry-run")
    else:
        distribute_cmd.append("--send")

    run_step(collect_cmd, env)
    run_step(generate_cmd, env)
    run_step(distribute_cmd, env)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
