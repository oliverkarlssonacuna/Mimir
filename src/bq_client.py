"""Thin BigQuery client – execute queries, return rows as plain dicts."""

import datetime
import logging
from decimal import Decimal
from typing import Any

from google.cloud import bigquery
from config import Config

logger = logging.getLogger(__name__)


class BQClient:
    def __init__(self, project_id: str, max_rows: int = 200):
        self.project_id = project_id
        self.max_rows = max_rows
        self.client = bigquery.Client(project=project_id)

    def run_query(self, sql: str, params: list | None = None) -> list[dict[str, Any]]:
        """Execute SQL and return up to max_rows rows as plain dicts."""
        logger.info("Running query: %s", sql[:300])
        job_config = None
        if params:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
        job = self.client.query(sql, job_config=job_config)
        rows = job.result()

        result = []
        for i, row in enumerate(rows):
            if i >= self.max_rows:
                logger.warning("Result truncated at %d rows", self.max_rows)
                break
            result.append({k: self._serialize(v) for k, v in row.items()})
        return result

    def run_update(self, sql: str, params: list | None = None) -> int:
        """Execute a DML statement (UPDATE/INSERT) and return the number of affected rows.
        
        Args:
            sql: SQL with optional @param placeholders.
            params: list of bigquery.ScalarQueryParameter for parameterised queries.
        """
        logger.info("Running DML: %s", sql[:300])
        job_config = None
        if params:
            job_config = bigquery.QueryJobConfig(query_parameters=params)
        job = self.client.query(sql, job_config=job_config)
        job.result()  # wait for completion
        return job.num_dml_affected_rows or 0

    def load_metric_configs(
        self,
        table: str,
        enabled_only: bool = True,
        collect_data_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Load metric configs from BQ. Returns list of dicts."""
        conditions = []
        if enabled_only:
            conditions.append("enabled = TRUE")
        if collect_data_only:
            conditions.append("collect_data = TRUE")
        where = ("WHERE " + " AND ".join(conditions) + " ") if conditions else ""
        sql = f"SELECT * FROM `{table}` {where}ORDER BY metric_label"
        return self.run_query(sql)

    def update_threshold(
        self,
        table: str,
        metric_id: str,
        comparison: str,
        threshold: float,
    ) -> int:
        """Update a single threshold for a metric. comparison: pace|dod|wow."""
        col = f"{comparison}_threshold"
        from google.cloud import bigquery as _bq
        sql = (
            f"UPDATE `{table}` "
            f"SET {col} = @threshold, updated_at = CURRENT_TIMESTAMP() "
            "WHERE metric_id = @metric_id"
        )
        params = [
            _bq.ScalarQueryParameter("threshold", "FLOAT64", threshold),
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
        ]
        return self.run_update(sql, params)

    def get_correlated_metrics(
        self,
        exclude_metric_id: str,
        baseline_date: str,
        anomaly_date: str,
        min_pct: float = 20.0,
        top_n: int = 3,
    ) -> list[dict]:
        """Return up to top_n metrics (excluding the analysed one) that moved
        ≥ min_pct in the same direction between baseline_date and anomaly_date.

        Returns list of dicts with keys: metric_id, metric_label, baseline_val,
        anomaly_val, pct_change.
        """
        from google.cloud import bigquery as _bq
        sql = f"""
            WITH
              vals AS (
                SELECT
                  s.metric_id,
                  c.metric_label,
                  MAX(CASE WHEN s.snapshot_date = @baseline_date THEN s.cumulative_value END) AS baseline_val,
                  MAX(CASE WHEN s.snapshot_date = @anomaly_date  THEN s.cumulative_value END) AS anomaly_val
                FROM `{Config.BQ_SNAPSHOT_TABLE}` s
                JOIN `{Config.BQ_METRIC_CONFIGS_TABLE}` c USING (metric_id)
                WHERE s.snapshot_date IN (@baseline_date, @anomaly_date)
                  AND s.metric_id != @exclude_id
                  AND c.enabled = TRUE
                GROUP BY s.metric_id, c.metric_label
              )
            SELECT
              metric_id,
              metric_label,
              baseline_val,
              anomaly_val,
              SAFE_DIVIDE(anomaly_val - baseline_val, ABS(baseline_val)) * 100 AS pct_change
            FROM vals
            WHERE baseline_val IS NOT NULL AND anomaly_val IS NOT NULL
              AND baseline_val != 0
              AND ABS(SAFE_DIVIDE(anomaly_val - baseline_val, ABS(baseline_val))) >= @min_pct_dec
            ORDER BY ABS(SAFE_DIVIDE(anomaly_val - baseline_val, ABS(baseline_val))) DESC
            LIMIT @top_n
        """
        params = [
            _bq.ScalarQueryParameter("baseline_date", "STRING", baseline_date),
            _bq.ScalarQueryParameter("anomaly_date",  "STRING", anomaly_date),
            _bq.ScalarQueryParameter("exclude_id",    "STRING", exclude_metric_id),
            _bq.ScalarQueryParameter("min_pct_dec",   "FLOAT64", min_pct / 100.0),
            _bq.ScalarQueryParameter("top_n",         "INT64",   top_n),
        ]
        try:
            return self.run_query(sql, params)
        except Exception:
            return []

    # ── Context notes (team-provided release/event dates) ─────────────────
    NOTES_TABLE = "lia-project-sandbox-deletable.anomaly_checks_demo.context_notes"

    def ensure_notes_table(self) -> None:
        """Create the context_notes table if it doesn't exist."""
        self.client.query(
            f"""CREATE TABLE IF NOT EXISTS `{self.NOTES_TABLE}` (
                note       STRING    NOT NULL,
                added_by   STRING,
                created_at TIMESTAMP
            )"""
        ).result()

    def add_note(self, note: str, added_by: str = "") -> None:
        from google.cloud import bigquery as _bq
        sql = (
            f"INSERT INTO `{self.NOTES_TABLE}` (note, added_by, created_at) "
            "VALUES (@note, @added_by, CURRENT_TIMESTAMP())"
        )
        params = [
            _bq.ScalarQueryParameter("note", "STRING", note),
            _bq.ScalarQueryParameter("added_by", "STRING", added_by),
        ]
        self.run_update(sql, params)

    def get_notes(self) -> list[dict]:
        return self.run_query(
            f"SELECT note, added_by, created_at FROM `{self.NOTES_TABLE}` "
            "ORDER BY created_at DESC LIMIT 100"
        )

    def clear_notes(self) -> int:
        return self.run_update(f"DELETE FROM `{self.NOTES_TABLE}` WHERE TRUE")

    @staticmethod
    def _serialize(v: Any) -> Any:
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, (datetime.date, datetime.datetime)):
            return v.isoformat()
        return v
