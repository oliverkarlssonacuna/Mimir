"""Anomaly detector – collect Steep snapshots, compare, and flag anomalies."""

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from bq_client import BQClient
from config import Config, MONITORED_METRICS, THRESHOLDS
from steep_client import SteepClient

logger = logging.getLogger(__name__)


@dataclass
class Anomaly:
    metric_id: str
    metric_label: str
    direction: str          # "down_is_bad" or "up_is_bad"
    severity: str           # "warning" or "critical"
    comparison: str         # "pace", "dod", or "wow"
    change_pct: float       # e.g. -0.32 means -32%
    current_value: float
    baseline_value: float
    description: str        # human-readable summary


class Detector:
    def __init__(self, steep: SteepClient, bq: BQClient):
        self.steep = steep
        self.bq = bq

    # ── Main entry point ──────────────────────────────────────────────────

    def collect_and_check(self) -> list[Anomaly]:
        """Collect snapshots from Steep, save to BQ, run comparisons.

        Returns a list of Anomaly objects (empty if all is well).
        """
        now = datetime.now(timezone.utc)
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        anomalies: list[Anomaly] = []

        for metric in MONITORED_METRICS:
            metric_id = metric["id"]
            label = metric["label"]
            direction = metric["direction"]

            # 1. Fetch today's cumulative value from Steep
            try:
                value, refreshed_at = self._fetch_today_value(metric_id)
            except Exception as e:
                logger.error("Failed to fetch %s from Steep: %s", label, e)
                continue

            if value is None:
                logger.warning("No data for %s today, skipping.", label)
                continue

            # 2. Save snapshot if data is new
            if not self._already_captured(metric_id, today_str, current_hour, refreshed_at):
                self._save_snapshot(metric_id, label, today_str, current_hour, value, refreshed_at)
            else:
                logger.info("%s: data unchanged (same refreshed_at), skipping save.", label)

            # 3. Run comparisons (always, even if snapshot already existed)
            anomalies.extend(
                self._check_metric(metric_id, label, direction, today_str, current_hour, value)
            )

        return anomalies

    # ── Fetch from Steep ──────────────────────────────────────────────────

    def _fetch_today_value(self, metric_id: str) -> tuple[float | None, str]:
        """Query Steep for today's daily value. Returns (value, refreshed_at)."""
        now = datetime.now(timezone.utc)
        from_date = now.strftime("%Y-%m-%dT00:00:00Z")
        to_date = now.strftime("%Y-%m-%dT23:59:59Z")

        resp = self.steep.query_metric(
            metric_id=metric_id,
            from_date=from_date,
            to_date=to_date,
            time_grain="daily",
        )

        refreshed_at = resp.get("refreshedAt", "")
        data = resp.get("data", [])

        if not data:
            return None, refreshed_at

        # Today's value is the last data point
        today_point = data[-1]
        value = today_point.get("metric")
        return value, refreshed_at

    # ── BQ snapshot operations ────────────────────────────────────────────

    def _already_captured(self, metric_id: str, date_str: str, hour: int, refreshed_at: str) -> bool:
        """Check if we already have a snapshot with the same refreshed_at (truncated to minute)."""
        sql = (
            f"SELECT 1 FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "AND snapshot_hour = @snapshot_hour "
            "AND TIMESTAMP_TRUNC(refreshed_at, MINUTE) = TIMESTAMP_TRUNC(@refreshed_at, MINUTE) "
            "LIMIT 1"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("snapshot_hour", "INT64", hour),
            _bq.ScalarQueryParameter("refreshed_at", "TIMESTAMP", refreshed_at),
        ]
        rows = self.bq.run_query(sql, params=params)
        return len(rows) > 0

    def _save_snapshot(
        self, metric_id: str, label: str, date_str: str, hour: int,
        value: float, refreshed_at: str,
    ) -> None:
        """Insert a snapshot row into BQ."""
        sql = (
            f"INSERT INTO `{Config.BQ_SNAPSHOT_TABLE}` "
            "(metric_id, metric_label, snapshot_date, snapshot_hour, "
            "cumulative_value, refreshed_at, captured_at) "
            "VALUES (@metric_id, @label, @snapshot_date, @snapshot_hour, "
            "@value, @refreshed_at, CURRENT_TIMESTAMP())"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("label", "STRING", label),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("snapshot_hour", "INT64", hour),
            _bq.ScalarQueryParameter("value", "FLOAT64", value),
            _bq.ScalarQueryParameter("refreshed_at", "TIMESTAMP", refreshed_at),
        ]
        try:
            self.bq.run_update(sql, params=params)
            logger.info("Saved snapshot: %s date=%s hour=%d value=%.2f", label, date_str, hour, value)
        except Exception as e:
            logger.error("Failed to save snapshot for %s: %s", label, e)

    def _get_snapshot(self, metric_id: str, date_str: str, hour: int) -> float | None:
        """Get the cumulative_value for a specific metric/date/hour."""
        sql = (
            f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "AND snapshot_hour = @snapshot_hour "
            "ORDER BY captured_at DESC LIMIT 1"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("snapshot_hour", "INT64", hour),
        ]
        rows = self.bq.run_query(sql, params=params)
        if rows:
            return rows[0].get("cumulative_value")
        return None

    def _get_nearest_snapshot(self, metric_id: str, date_str: str, max_hour: int) -> float | None:
        """Get the latest snapshot for a date at or before max_hour."""
        sql = (
            f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "AND snapshot_hour <= @max_hour "
            "ORDER BY snapshot_hour DESC, captured_at DESC LIMIT 1"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("max_hour", "INT64", max_hour),
        ]
        rows = self.bq.run_query(sql, params=params)
        if rows:
            return rows[0].get("cumulative_value")
        return None

    def _get_day_final_value(self, metric_id: str, date_str: str) -> float | None:
        """Get the last snapshot of a given day (highest hour = closest to end-of-day)."""
        sql = (
            f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "ORDER BY snapshot_hour DESC LIMIT 1"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
        ]
        rows = self.bq.run_query(sql, params=params)
        if rows:
            return rows[0].get("cumulative_value")
        return None

    # ── Comparison logic ──────────────────────────────────────────────────

    def _check_metric(
        self, metric_id: str, label: str, direction: str,
        today_str: str, current_hour: int, current_value: float,
    ) -> list[Anomaly]:
        """Run pace, DoD, and WoW checks for one metric."""
        from datetime import timedelta

        today = datetime.strptime(today_str, "%Y-%m-%d").date()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)
        same_weekday_last_week = today - timedelta(days=7)

        anomalies: list[Anomaly] = []

        # Pace check: today vs yesterday at nearest available hour
        yesterday_same_hour = self._get_nearest_snapshot(metric_id, yesterday.isoformat(), current_hour)
        if yesterday_same_hour is not None and yesterday_same_hour != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "pace",
                current_value, yesterday_same_hour,
            )
            if anomaly:
                anomalies.append(anomaly)

        # DoD check: yesterday final vs day-before final
        yesterday_final = self._get_day_final_value(metric_id, yesterday.isoformat())
        day_before_final = self._get_day_final_value(metric_id, day_before.isoformat())
        if yesterday_final is not None and day_before_final is not None and day_before_final != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "dod",
                yesterday_final, day_before_final,
            )
            if anomaly:
                anomalies.append(anomaly)

        # WoW check: yesterday final vs same weekday last week final
        last_week_final = self._get_day_final_value(metric_id, same_weekday_last_week.isoformat())
        if yesterday_final is not None and last_week_final is not None and last_week_final != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "wow",
                yesterday_final, last_week_final,
            )
            if anomaly:
                anomalies.append(anomaly)

        return anomalies

    def _evaluate(
        self, metric_id: str, label: str, direction: str,
        comparison: str, current: float, baseline: float,
    ) -> Anomaly | None:
        """Compare current vs baseline. Return Anomaly if threshold exceeded."""
        change_pct = (current - baseline) / abs(baseline)
        abs_change = abs(change_pct)

        thresholds = THRESHOLDS[comparison]

        # Determine severity
        severity = None
        if abs_change >= thresholds["critical"]:
            severity = "critical"
        elif abs_change >= thresholds["warning"]:
            severity = "warning"

        if severity is None:
            return None

        # Build description
        direction_symbol = "↓" if change_pct < 0 else "↑"
        pct_str = f"{change_pct:+.1%}"
        comp_label = {"pace": "Pace (intradag)", "dod": "Dag-över-dag", "wow": "Vecka-över-vecka"}[comparison]

        # Determine if this change is in the "bad" direction
        is_bad_direction = (
            (direction == "down_is_bad" and change_pct < 0)
            or (direction == "up_is_bad" and change_pct > 0)
        )
        tone = "⚠️" if is_bad_direction else "📋"

        description = (
            f"{tone} {label}: {direction_symbol} {pct_str} ({comp_label})\n"
            f"Nuvarande: {current:,.1f} → Baseline: {baseline:,.1f}"
        )

        return Anomaly(
            metric_id=metric_id,
            metric_label=label,
            direction=direction,
            severity=severity,
            comparison=comparison,
            change_pct=change_pct,
            current_value=current,
            baseline_value=baseline,
            description=description,
        )
