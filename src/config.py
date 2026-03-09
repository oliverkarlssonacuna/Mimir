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
    DISCORD_WEBHOOK_URL: str = _optional("DISCORD_WEBHOOK_URL")

    # The one table this bot knows about
    BQ_TABLE: str = "lia-project-sandbox-deletable.anomaly_checks_demo.daily_anomaly_check_results"

    # The dataset where the real source tables live (used for deep analysis)
    BQ_SOURCE_DATASET: str = _optional("BQ_SOURCE_DATASET", "goals-analytics.prod_event_classes")

    # Maximum rows returned from any single query
    MAX_QUERY_ROWS: int = 200

    # How often the monitor checks BQ (in seconds), default 1 hour
    MONITOR_INTERVAL_SECONDS: int = int(_optional("MONITOR_INTERVAL_SECONDS", "3600"))

    # Jira integration for release context
    JIRA_BASE_URL: str = _optional("JIRA_BASE_URL", "")
    JIRA_EMAIL: str = _optional("JIRA_EMAIL", "")
    JIRA_API_TOKEN: str = _optional("JIRA_API_TOKEN", "")
    JIRA_PROJECT_KEY: str = _optional("JIRA_PROJECT_KEY", "")
