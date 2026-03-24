"""
Cloud Run Job – collect Steep snapshots and save to BQ.

Runs every hour via Cloud Scheduler.
Only writes to BQ when Steep has new data (refreshed_at changed).
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
    logger.info("Snapshot job starting.")
    bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
    steep = SteepClient(api_key=Config.STEEP_API_TOKEN)
    detector = Detector(steep=steep, bq=bq)

    try:
        anomalies = detector.collect_and_check()
        logger.info("Snapshot job complete. %d anomalies detected (not alerted).", len(anomalies))
    except Exception as e:
        logger.error("Snapshot job failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()
