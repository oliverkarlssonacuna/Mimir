"""Steep API client – fetch metrics metadata and query metric data."""

import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.steep.app"


class SteepClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        self.session.headers.update({
            "Authorization": f"ApiKey {api_key}",
            "Content-Type": "application/json",
        })

    # ── Metrics catalog ───────────────────────────────────────────────────

    def list_metrics(self, expand: bool = False) -> list[dict[str, Any]]:
        """Return all metrics in the workspace."""
        resp = self.session.get(
            f"{BASE_URL}/v1/metrics",
            params={"expand": str(expand).lower()},
        )
        resp.raise_for_status()
        return resp.json().get("data", [])

    def get_metric(self, metric_id: str) -> dict[str, Any] | None:
        """Get a single metric by ID (via expanded list, since no /metrics/{id} GET)."""
        for m in self.list_metrics(expand=True):
            if m["id"] == metric_id:
                return m
        return None

    # ── Query metric data ─────────────────────────────────────────────────

    def query_metric(
        self,
        metric_id: str,
        from_date: datetime | str,
        to_date: datetime | str,
        time_grain: str = "daily",
        breakdown_dimension_ids: list[str] | None = None,
        filters: list[dict] | None = None,
        slice_id: str | None = None,
    ) -> dict[str, Any]:
        """Query a metric and return the full response (data, sql, total, etc.)."""
        if isinstance(from_date, datetime):
            from_date = from_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        if isinstance(to_date, datetime):
            to_date = to_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        body: dict[str, Any] = {
            "fromDate": from_date,
            "toDate": to_date,
            "timeGrain": time_grain,
        }
        if breakdown_dimension_ids:
            body["breakdownDimensionIds"] = breakdown_dimension_ids
        if filters:
            body["filters"] = filters
        if slice_id:
            body["sliceId"] = slice_id

        for attempt in range(6):
            resp = self.session.post(
                f"{BASE_URL}/v1/metrics/{metric_id}/query",
                json=body,
            )
            if resp.status_code == 429:
                wait = 2 ** attempt
                logger.warning("Steep rate limited (429), retrying in %ds...", wait)
                time.sleep(wait)
                continue
            resp.raise_for_status()
            time.sleep(0.3)  # throttle to avoid 429 rate limiting
            return resp.json()
        resp.raise_for_status()
        return resp.json()

    def query_metric_recent(
        self,
        metric_id: str,
        days: int = 60,
        time_grain: str = "daily",
    ) -> dict[str, Any]:
        """Convenience: query last N days for a metric."""
        now = datetime.now(timezone.utc)
        return self.query_metric(
            metric_id=metric_id,
            from_date=now - timedelta(days=days),
            to_date=now,
            time_grain=time_grain,
        )

    # ── Modules & entities ────────────────────────────────────────────────

    def list_modules(self) -> list[dict[str, Any]]:
        resp = self.session.get(f"{BASE_URL}/v1/modules")
        resp.raise_for_status()
        return resp.json().get("data", [])

    def list_entities(self) -> list[dict[str, Any]]:
        resp = self.session.get(f"{BASE_URL}/v1/entities")
        resp.raise_for_status()
        return resp.json().get("data", [])
