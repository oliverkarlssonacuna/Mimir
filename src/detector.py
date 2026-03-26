"""Anomaly detector – collect Steep snapshots, compare, and flag anomalies."""

import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone

from bq_client import BQClient
from config import Config, THRESHOLDS
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
    steep_url: str | None
    description: str        # human-readable summary
    reference_date: str     # the date the anomaly refers to (YYYY-MM-DD)
    baseline_date: str = ""  # the date of the baseline value (YYYY-MM-DD)
    display_format: str = "number"  # "number" or "percent"


class Detector:
    def __init__(self, steep: SteepClient, bq: BQClient):
        self.steep = steep
        self.bq = bq
        self._metric_configs: list[dict] = []
        self.reload_configs()

    def reload_configs(self) -> None:
        """Load metric configs from BQ into memory cache."""
        self._metric_configs = self.bq.load_metric_configs(Config.BQ_METRIC_CONFIGS_TABLE)
        logger.info("Loaded %d metric configs from BQ.", len(self._metric_configs))

    # ── Main entry point ──────────────────────────────────────────────────    

    def collect_and_check(self, progress_callback=None) -> tuple[list[Anomaly], int]:
        """Collect snapshots from Steep, save to BQ, run comparisons.

        Returns (anomalies, failed_count) where failed_count is the number of metrics
        that could not be fetched from Steep.
        progress_callback: optional callable(current, total, label) called after each metric.
        """
        now = datetime.now(timezone.utc)
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        anomalies: list[Anomaly] = []
        total = len(self._metric_configs)
        _lock = threading.Lock()
        _counter = [0]
        _failed_labels: list[tuple[str, str]] = []  # (label, error_message)

        def _process_one(metric: dict) -> list[Anomaly]:
            metric_id = metric["metric_id"]
            label = metric["metric_label"]
            direction = metric.get("direction", "down_is_bad")
            result: list[Anomaly] = []

            try:
                value, refreshed_at, historical = self._fetch_values(metric_id)
            except Exception as e:
                logger.error("Failed to fetch %s from Steep: %s", label, e, exc_info=True)
                with _lock:
                    _counter[0] += 1
                    _failed_labels.append((label, type(e).__name__))
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result

            if value is None:
                logger.warning("No data for %s today, skipping.", label)
                with _lock:
                    _counter[0] += 1
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result

            if not self._already_captured(metric_id, today_str, value):
                self._save_snapshot(metric_id, label, today_str, current_hour, value, refreshed_at)
            else:
                logger.info("%s: data unchanged (same value), skipping save.", label)

            metric_thresholds = {
                comp: float(metric.get(f"{comp}_threshold", THRESHOLDS[comp]))
                for comp in ("pace", "dod", "wow")
            }
            display_format = metric.get("display_format") or "number"
            result.extend(
                self._check_metric(metric_id, label, direction, today_str, current_hour, value, metric_thresholds, historical, display_format)
            )

            with _lock:
                _counter[0] += 1
                if progress_callback:
                    progress_callback(_counter[0], total, label)

            return result

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(_process_one, m) for m in self._metric_configs]
            for future in as_completed(futures):
                try:
                    anomalies.extend(future.result())
                except Exception as e:
                    logger.error("Unhandled error in metric worker: %s", e, exc_info=True)

        if _failed_labels:
            logger.warning(
                "Steep fetch: %d/%d metrics failed: %s",
                len(_failed_labels),
                total,
                ", ".join(f"{lbl} ({err})" for lbl, err in _failed_labels),
            )
        return anomalies, _failed_labels

    # ── Fetch from Steep ──────────────────────────────────────────────────

    def _fetch_values(self, metric_id: str) -> tuple[float | None, str, dict[str, float]]:
        """Fetch last 9 days of daily values from Steep in one call.

        Returns (today_value, refreshed_at, historical) where historical is
        a dict of {date_str: value} for all returned data points.
        """
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        today_str = now.strftime("%Y-%m-%d")
        from_date = (now - timedelta(days=8)).strftime("%Y-%m-%dT00:00:00Z")
        to_date = now.strftime("%Y-%m-%dT23:59:59Z")

        resp = self.steep.query_metric(
            metric_id=metric_id,
            from_date=from_date,
            to_date=to_date,
            time_grain="daily",
        )

        refreshed_at = resp.get("refreshedAt", "")
        data = resp.get("data", [])

        historical: dict[str, float] = {}
        today_value = None
        for point in data:
            raw_date = point.get("time", "")
            date_str = raw_date[:10] if raw_date else ""
            value = point.get("metric")
            if date_str and value is not None:
                historical[date_str] = value
                if date_str == today_str:
                    today_value = value

        # Fallback: if time field missing, take last point as today
        if today_value is None and data:
            today_value = data[-1].get("metric")

        return today_value, refreshed_at, historical

    def _fetch_today_value(self, metric_id: str) -> tuple[float | None, str]:
        """Fetch today's current value from Steep. Returns (value, refreshed_at)."""
        value, refreshed_at, _ = self._fetch_values(metric_id)
        return value, refreshed_at

    # ── BQ snapshot operations ────────────────────────────────────────────

    def _already_captured(self, metric_id: str, date_str: str, value: float) -> bool:
        """Check if we already have a snapshot with the same cumulative_value for this metric and date.

        Only saves a new snapshot when the metric value has actually changed.
        snapshot_hour reflects when Steep updated its data.
        """
        sql = (
            f"SELECT 1 FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "AND cumulative_value = @value "
            "LIMIT 1"
        )
        from google.cloud import bigquery as _bq
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("value", "FLOAT64", value),
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

    def _get_nearest_snapshot(self, metric_id: str, date_str: str, max_hour: int, strict: bool = False) -> float | None:
        """Get the snapshot for a date at exactly max_hour (strict=True) or at or before max_hour (strict=False).

        If strict=True, only returns a snapshot at exactly max_hour – ensures apples-to-apples
        pace comparison (same hour yesterday vs same hour today).
        """
        from google.cloud import bigquery as _bq
        if strict:
            sql = (
                f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
                "WHERE metric_id = @metric_id "
                "AND snapshot_date = @snapshot_date "
                "AND snapshot_hour = @max_hour "
                "ORDER BY captured_at DESC LIMIT 1"
            )
        else:
            sql = (
                f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
                "WHERE metric_id = @metric_id "
                "AND snapshot_date = @snapshot_date "
                "AND snapshot_hour <= @max_hour "
                "ORDER BY snapshot_hour DESC, captured_at DESC LIMIT 1"
            )
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("snapshot_date", "DATE", date_str),
            _bq.ScalarQueryParameter("max_hour", "INT64", max_hour),
        ]
        rows = self.bq.run_query(sql, params=params)
        if rows:
            return rows[0].get("cumulative_value")

        if strict:
            return None

        # Fallback: take the closest snapshot for that day regardless of hour
        sql_fallback = (
            f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date = @snapshot_date "
            "ORDER BY ABS(snapshot_hour - @max_hour) ASC, captured_at DESC LIMIT 1"
        )
        rows = self.bq.run_query(sql_fallback, params=params)
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
        metric_thresholds: dict,
        historical: dict[str, float] | None = None,
        display_format: str = "number",
    ) -> list[Anomaly]:
        """Run pace, DoD, and WoW checks for one metric.

        Pace uses BQ snapshots: today at current hour vs same weekday last week at same hour.
        DoD and WoW uses Steep historical data direkt (alltid korrekt slutvärde).
        """
        from datetime import timedelta

        today = datetime.strptime(today_str, "%Y-%m-%d").date()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)
        same_weekday_last_week = today - timedelta(days=7)
        historical = historical or {}

        anomalies: list[Anomaly] = []

        # Pace check: today vs same weekday last week at same hour (BQ snapshots)
        last_week_same_hour = self._get_nearest_snapshot(metric_id, same_weekday_last_week.isoformat(), current_hour, strict=True)
        if last_week_same_hour is not None and last_week_same_hour != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "pace",
                current_value, last_week_same_hour,
                metric_thresholds["pace"],
                reference_date=today_str,
                display_format=display_format,
                baseline_date=same_weekday_last_week.isoformat(),
            )
            if anomaly:
                anomalies.append(anomaly)

        # DoD check: yesterday vs day-before (Steep – alltid korrekt slutvärde)
        yesterday_val = historical.get(yesterday.isoformat())
        day_before_val = historical.get(day_before.isoformat())
        if yesterday_val is not None and day_before_val is not None and day_before_val != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "dod",
                yesterday_val, day_before_val,
                metric_thresholds["dod"],
                reference_date=yesterday.isoformat(),
                display_format=display_format,
                baseline_date=day_before.isoformat(),
            )
            if anomaly:
                anomalies.append(anomaly)

        # WoW check: yesterday vs samma veckodag förra veckan (Steep)
        # Skippa om baseline-veckan faller på eller innan BASELINE_START_DATE
        baseline_start = datetime.strptime(Config.BASELINE_START_DATE, "%Y-%m-%d").date()
        last_week_val = historical.get(same_weekday_last_week.isoformat())
        if (same_weekday_last_week > baseline_start
                and yesterday_val is not None and last_week_val is not None and last_week_val != 0):
            anomaly = self._evaluate(
                metric_id, label, direction, "wow",
                yesterday_val, last_week_val,
                metric_thresholds["wow"],
                reference_date=yesterday.isoformat(),
                display_format=display_format,
                baseline_date=same_weekday_last_week.isoformat(),
            )
            if anomaly:
                anomalies.append(anomaly)

        return anomalies

    def _evaluate(
        self, metric_id: str, label: str, direction: str,
        comparison: str, current: float, baseline: float,
        thresholds: dict | None = None,
        reference_date: str = "",
        display_format: str = "number",
        baseline_date: str = "",
    ) -> Anomaly | None:
        """Compare current vs baseline. Return Anomaly if threshold exceeded."""
        change_pct = (current - baseline) / abs(baseline)
        abs_change = abs(change_pct)

        if thresholds is None:
            thresholds = THRESHOLDS[comparison]

        if abs_change < thresholds:
            return None

        severity = "warning"

        # Build description
        direction_symbol = "↓" if change_pct < 0 else "↑"
        pct_str = f"{change_pct:+.1%}"
        comp_label = {"pace": "Pace (intradag)", "dod": "Dag-över-dag", "wow": "Vecka-över-vecka"}[comparison]

        # Only alert if the change is in the "bad" direction
        is_bad_direction = (
            (direction in ("down_is_bad", "alert_on_drop") and change_pct < 0)
            or (direction in ("up_is_bad", "alert_on_rise") and change_pct > 0)
        )
        if not is_bad_direction:
            return None
        tone = "⚠️"

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
            steep_url=None,
            description=description,
            reference_date=reference_date,
            baseline_date=baseline_date,
            display_format=display_format,
        )
