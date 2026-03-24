"""Insert all non-TEST Steep metrics into BQ metric_configs table.
Skips metrics already in BQ. Determines direction and display_format from name.
"""
import os
from dotenv import load_dotenv
load_dotenv()

from src.steep_client import SteepClient
from src.bq_client import BQClient
from src.config import Config

# Already in BQ — skip these
EXISTING_IDS = {
    "xbkYiyTivpfp",  # First Opens Game
    "08O_4SH2zpzO",  # Active Users Game
    "2o79cTggQf3m",  # Matches 1v1
    "zefdZQHmk2y6",  # MM Waiting Time For Match
    "vgacJieCzuuo",  # Crash ratio
}

# Metrics where an increase is bad
ALERT_ON_RISE_KEYWORDS = [
    "waiting time", "cancel ratio", "cancel", "ping", "cost", "dbt",
    "crash", "bug", "error", "churn",
]

# Metrics that return 0–1 decimal (displayed as %)
PERCENT_KEYWORDS = [
    "ratio", "coverage", "rate", "crash",
]

# TEST and irrelevant
SKIP_KEYWORDS = ["test ", "game dev"]


def get_direction(label: str) -> str:
    lower = label.lower()
    for kw in ALERT_ON_RISE_KEYWORDS:
        if kw in lower:
            return "alert_on_rise"
    return "alert_on_drop"


def get_display_format(label: str) -> str:
    lower = label.lower()
    for kw in PERCENT_KEYWORDS:
        if kw in lower:
            return "percent"
    return "number"


def main():
    steep = SteepClient(os.environ["STEEP_API_TOKEN"])
    bq = BQClient(Config.GCP_PROJECT_ID)

    metrics = steep.list_metrics(expand=True)

    to_insert = []
    for m in metrics:
        label = m.get("label") or ""
        mid = m["id"]

        # Skip existing
        if mid in EXISTING_IDS:
            print(f"SKIP (existing): {label}")
            continue

        # Skip TEST and Dev metrics
        if any(kw in label.lower() for kw in SKIP_KEYWORDS):
            print(f"SKIP (test/dev): {label}")
            continue

        to_insert.append((mid, label, m.get("link", "")))

    print(f"\n→ Inserting {len(to_insert)} metrics...\n")

    from google.cloud import bigquery as bq_lib

    inserted = 0
    skipped = 0
    for mid, label, link in sorted(to_insert, key=lambda x: x[1]):
        direction = get_direction(label)
        display_format = get_display_format(label)

        sql = (
            f"INSERT INTO `{Config.BQ_METRIC_CONFIGS_TABLE}` "
            "(metric_id, metric_label, direction, steep_url, "
            "pace_threshold, dod_threshold, wow_threshold, "
            "enabled, updated_at, display_format) "
            f"SELECT @metric_id, @label, @direction, @steep_url, "
            "0.25, 0.20, 0.15, TRUE, CURRENT_TIMESTAMP(), @display_format "
            f"FROM (SELECT 1) WHERE NOT EXISTS "
            f"(SELECT 1 FROM `{Config.BQ_METRIC_CONFIGS_TABLE}` WHERE metric_id = @metric_id)"
        )
        params = [
            bq_lib.ScalarQueryParameter("metric_id", "STRING", mid),
            bq_lib.ScalarQueryParameter("label", "STRING", label),
            bq_lib.ScalarQueryParameter("direction", "STRING", direction),
            bq_lib.ScalarQueryParameter("steep_url", "STRING", link),
            bq_lib.ScalarQueryParameter("display_format", "STRING", display_format),
        ]
        rows_affected = bq.run_update(sql, params)
        if rows_affected:
            print(f"  ✓ {label} [{direction}, {display_format}]")
            inserted += 1
        else:
            print(f"  ~ {label} (already exists, skipped)")
            skipped += 1

    print(f"\nDone. Inserted: {inserted}, Skipped (already existed): {skipped}")


if __name__ == "__main__":
    main()
