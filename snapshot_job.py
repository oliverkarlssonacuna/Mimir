"""
Cloud Run Job – collect Steep snapshots and save to BQ.

Runs every hour via Cloud Scheduler.
Only writes to BQ when Steep has new data (refreshed_at changed).

Flags:
  --finalize        Fetch and upsert finalized daily values (run at 03:00 UTC daily).
  --backfill N      Backfill N days of finalized daily values from Steep (one-off).
"""

import logging
import sys

from src.config import Config
from src.bq_client import BQClient
from src.steep_client import SteepClient
from src.detector import Detector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    args = sys.argv[1:]
    bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
    steep = SteepClient(api_key=Config.STEEP_API_TOKEN)
    detector = Detector(steep=steep, bq=bq)

    # ── Backfill mode: python snapshot_job.py --backfill [days] ──────────
    if "--backfill" in args:
        idx = args.index("--backfill")
        days = int(args[idx + 1]) if idx + 1 < len(args) and args[idx + 1].isdigit() else 60
        logger.info("Backfill mode: fetching %d days of finalized values from Steep.", days)
        try:
            detector.reload_configs(enabled_only=True)
            total = detector.finalize_daily_values(days=days)
            logger.info("Backfill complete: %d rows upserted.", total)
        except Exception as e:
            logger.error("Backfill failed: %s", e)
            sys.exit(1)
        return

    # ── Finalize mode: python snapshot_job.py --finalize ─────────────────
    if "--finalize" in args:
        logger.info("Finalize mode: upserting yesterday's final values from Steep.")
        try:
            detector.reload_configs(enabled_only=True)
            total = detector.finalize_daily_values(days=2)  # yesterday + day before as safety
            logger.info("Finalize complete: %d rows upserted.", total)
        except Exception as e:
            logger.error("Finalize failed: %s", e)
            sys.exit(1)
        return

    # ── Normal hourly snapshot mode ───────────────────────────────────────
    logger.info("Snapshot job starting.")
    try:
        detector.reload_configs(collect_data_only=True)
        anomalies = detector.collect_and_check()
        logger.info("Snapshot job complete. %d anomalies detected (not alerted).", len(anomalies))
    except Exception as e:
        logger.error("Snapshot job failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
