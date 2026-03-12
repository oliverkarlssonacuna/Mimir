"""Configuration – loaded from .env or environment variables."""

import os
import sys

from dotenv import load_dotenv

load_dotenv()


def _require(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(f"FATAL: required env var '{name}' is not set. Copy .env.example to .env and fill it in.")
        sys.exit(1)
    return value


def _optional(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


class Config:
    GCP_PROJECT_ID: str = _require("GCP_PROJECT_ID")
    GCP_VERTEXAI_REGION: str = _optional("GCP_VERTEXAI_REGION", "us-central1")
    GEMINI_MODEL: str = _optional("GEMINI_MODEL", "gemini-2.0-flash-001")

    DISCORD_BOT_TOKEN: str = _optional("DISCORD_BOT_TOKEN")
    DISCORD_ALERT_CHANNEL_ID: str = _optional("DISCORD_ALERT_CHANNEL_ID")

    # BQ snapshot table for Steep metric snapshots
    BQ_SNAPSHOT_TABLE: str = (
        "lia-project-sandbox-deletable.anomaly_checks_demo.steep_metric_snapshots"
    )
    MAX_QUERY_ROWS: int = 200

    # Monitor interval – 4 hours (matches Steep cache TTL)
    MONITOR_INTERVAL_SECONDS: int = int(_optional("MONITOR_INTERVAL_SECONDS", "14400"))

    # Jira integration for release context
    JIRA_BASE_URL: str = _optional("JIRA_BASE_URL", "")
    JIRA_EMAIL: str = _optional("JIRA_EMAIL", "")
    JIRA_API_TOKEN: str = _optional("JIRA_API_TOKEN", "")
    JIRA_PROJECT_KEY: str = _optional("JIRA_PROJECT_KEY", "")

    # Steep integration
    STEEP_API_TOKEN: str = _require("STEEP_API_TOKEN")

    # Beta launch date – data before this is unreliable
    BASELINE_START_DATE: str = "2026-03-10"


# ── Monitored metrics ────────────────────────────────────────────────────────

MONITORED_METRICS: list[dict] = [
    {
        "id": "xbkYiyTivpfp",
        "label": "First Opens Game",
        "direction": "down_is_bad",
    },
    {
        "id": "08O_4SH2zpzO",
        "label": "Active Users Game",
        "direction": "down_is_bad",
    },
    {
        "id": "2o79cTggQf3m",
        "label": "Matches 1v1",
        "direction": "down_is_bad",
    },
    {
        "id": "zefdZQHmk2y6",
        "label": "MM Waiting Time For Match",
        "direction": "up_is_bad",
    },
    {
        "id": "vgacJieCzuuo",
        "label": "Crash ratio",
        "direction": "up_is_bad",
    },
]


# ── Thresholds (symmetric – absolute percentage change) ──────────────────────

THRESHOLDS = {
    "pace": {"warning": 0.10, "critical": 0.25},   # ±10% / ±25%
    "dod":  {"warning": 0.08, "critical": 0.20},   # ±8%  / ±20%
    "wow":  {"warning": 0.05, "critical": 0.15},   # ±5%  / ±15%
}
