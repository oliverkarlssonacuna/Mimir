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
    # BQ config table for Steep metric alert rules
    BQ_METRIC_CONFIGS_TABLE: str = (
        "lia-project-sandbox-deletable.anomaly_checks_demo.steep_metrics_configs"
    )
    # BQ config table for BQ-sourced metrics (tables/queries)
    BQ_METRICS_CONFIGS_TABLE: str = (
        "lia-project-sandbox-deletable.anomaly_checks_demo.bq_metrics_configs"
    )
    MAX_QUERY_ROWS: int = 200

    # Monitor interval – 4 hours (matches Steep cache TTL)
    MONITOR_INTERVAL_SECONDS: int = int(_optional("MONITOR_INTERVAL_SECONDS", "3600"))

    # Jira integration for release context
    JIRA_BASE_URL: str = _optional("JIRA_BASE_URL", "")
    JIRA_EMAIL: str = _optional("JIRA_EMAIL", "")
    JIRA_API_TOKEN: str = _optional("JIRA_API_TOKEN", "")
    JIRA_PROJECT_KEY: str = _optional("JIRA_PROJECT_KEY", "")

    # Steep integration
    STEEP_API_TOKEN: str = _require("STEEP_API_TOKEN")

    # Beta launch date – data before this is unreliable
    BASELINE_START_DATE: str = "2026-03-09"

    # Admin web UI URL (used in /admin Discord command)
    ADMIN_URL: str = _optional("ADMIN_URL", "http://localhost:8080")

    # Internal HTTP port for bot — web server posts here to trigger config reload
    BOT_INTERNAL_PORT: int = int(_optional("BOT_INTERNAL_PORT", "8081"))


# ── Thresholds (symmetric – absolute percentage change) ────────────────────── 
# Kept as fallback defaults only. Per-metric thresholds are stored in BQ.

THRESHOLDS = {
    "pace": 0.15,
    "dod":  0.10,
    "wow":  0.10,
}
