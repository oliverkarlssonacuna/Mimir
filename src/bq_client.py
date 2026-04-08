"""Thin BigQuery client – execute queries, return rows as plain dicts."""

import datetime
import logging
from decimal import Decimal
from typing import Any

from google.cloud import bigquery

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

    @staticmethod
    def _serialize(v: Any) -> Any:
        if isinstance(v, Decimal):
            return float(v)
        if isinstance(v, (datetime.date, datetime.datetime)):
            return v.isoformat()
        return v
