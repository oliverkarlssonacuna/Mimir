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
class FieldAlert:
    monitor_id: str
    label: str
    bq_table: str
    field_name: str
    new_values: list[str]   # values seen today but not in the past 7 days
    today_date: str
    known_value_count: int = 0  # number of distinct values seen in the past 7 days
    field_type: str = ""        # BQ column type e.g. STRING, INTEGER


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

    def reload_configs(self, *, enabled_only: bool = True, collect_data_only: bool = False) -> None:
        """Load metric configs from BQ into memory cache."""
        self._metric_configs = self.bq.load_metric_configs(
            Config.BQ_METRIC_CONFIGS_TABLE,
            enabled_only=enabled_only,
            collect_data_only=collect_data_only,
        )
        logger.info("Loaded %d metric configs from BQ.", len(self._metric_configs))
        self._bq_metric_configs = self._load_bq_metric_configs(
            enabled_only=enabled_only, collect_data_only=collect_data_only
        )
        logger.info("Loaded %d BQ metric configs.", len(self._bq_metric_configs))
        self._field_monitor_configs = self._load_field_monitor_configs()
        logger.info("Loaded %d field monitor configs.", len(self._field_monitor_configs))

    def _load_field_monitor_configs(self) -> list[dict]:
        """Load event field monitor configs from BQ."""
        return self.bq.load_field_monitor_configs()

    def check_field_monitors(self) -> list["FieldAlert"]:
        """For each configured field monitor, detect values seen today but not in the past 7 days."""
        configs = getattr(self, "_field_monitor_configs", [])
        if not configs:
            return []

        alerts: list[FieldAlert] = []
        for cfg in configs:
            monitor_id = cfg["monitor_id"]
            label = cfg["label"]
            bq_table = cfg["bq_table"]
            field_name = cfg["field_name"]
            date_field = cfg.get("date_field") or "partition_date"
            filter_sql = (cfg.get("filter_sql") or "").strip()
            extra_filter = f"AND ({filter_sql})" if filter_sql else ""

            try:
                past_filter = (
                    f">= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) "
                    f"AND {date_field} < CURRENT_DATE()"
                )
                past_values = self.bq.get_distinct_field_values(
                    bq_table, field_name, date_field, past_filter, extra_filter,
                )
                today_values = self.bq.get_distinct_field_values(
                    bq_table, field_name, date_field, "= CURRENT_DATE()", extra_filter,
                )
                new_values = sorted(today_values - past_values)
                if new_values:
                    from datetime import datetime as _dt, timezone as _tz
                    field_type = self.bq.get_field_type(bq_table, field_name)
                    alerts.append(FieldAlert(
                        monitor_id=monitor_id,
                        label=label,
                        bq_table=bq_table,
                        field_name=field_name,
                        new_values=new_values,
                        today_date=_dt.now(_tz.utc).strftime("%Y-%m-%d"),
                        known_value_count=len(past_values),
                        field_type=field_type,
                    ))
            except Exception as e:
                logger.error("Field monitor check failed for '%s': %s", label, e)

        return alerts

    def _load_bq_metric_configs(self, enabled_only: bool = True, collect_data_only: bool = False) -> list[dict]:
        """Load BQ metric configs (those with a sql_query)."""
        try:
            conditions = ["sql_query IS NOT NULL", "sql_query != ''"]
            if enabled_only:
                conditions.append("enabled = TRUE")
            if collect_data_only:
                conditions.append("collect_data = TRUE")
            where = "WHERE " + " AND ".join(conditions)
            sql = f"SELECT * FROM `{Config.BQ_METRICS_CONFIGS_TABLE}` {where}"
            return self.bq.run_query(sql)
        except Exception as e:
            logger.warning("Could not load BQ metric configs: %s", e)
            return []

    # ── Main entry point ──────────────────────────────────────────────────    

    def collect_and_check(self, progress_callback=None, force_pace: bool = False) -> tuple[list[Anomaly], list[tuple[str, str]]]:
        """Collect snapshots from Steep, save to BQ, run comparisons.

        Returns (anomalies, failed_labels) where failed_labels is a list of (label, error_str).
        progress_callback: optional callable(current, total, label) called after each metric.
        force_pace: if True, always include a pace anomaly (scanning back in BQ if needed) — for debug runs only.
        """
        now = datetime.now(timezone.utc)
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")

        anomalies: list[Anomaly] = []
        failed_labels: list[tuple[str, str]] = []
        total = len(self._metric_configs)
        _lock = threading.Lock()
        _counter = [0]

        def _process_one(metric: dict) -> tuple[list[Anomaly], list[tuple[str, str]]]:
            metric_id = metric["metric_id"]
            label = metric["metric_label"]
            direction = metric.get("direction", "down_is_bad")
            result: list[Anomaly] = []

            failed: list[tuple[str, str]] = []
            try:
                value, refreshed_at, historical = self._fetch_values(metric_id)
            except Exception as e:
                logger.error("Failed to fetch %s from Steep: %s", label, e)
                failed.append((label, type(e).__name__))
                with _lock:
                    _counter[0] += 1
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result, failed

            if value is None:
                logger.warning("No data for %s today, skipping.", label)
                failed.append((label, "No data"))
                with _lock:
                    _counter[0] += 1
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result, failed

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
                self._check_metric(metric_id, label, direction, today_str, current_hour, value, metric_thresholds, historical, display_format, force_pace=force_pace)
            )

            with _lock:
                _counter[0] += 1
                if progress_callback:
                    progress_callback(_counter[0], total, label)

            return result, failed

        with ThreadPoolExecutor(max_workers=8) as executor:
            steep_futures = [executor.submit(_process_one, m) for m in self._metric_configs]
            bq_future = executor.submit(self.check_bq_metrics)

            for future in as_completed(steep_futures):
                try:
                    res, fl = future.result()
                    anomalies.extend(res)
                    failed_labels.extend(fl)
                except Exception as e:
                    logger.error("Unhandled error in metric worker: %s", e)

            try:
                anomalies.extend(bq_future.result())
            except Exception as e:
                logger.error("BQ metric check failed: %s", e)

        return anomalies, failed_labels

    def check_only(self, progress_callback=None) -> tuple[list[Anomaly], list[tuple[str, str]]]:
        """Run anomaly checks using only BQ snapshot data — no Steep API calls.

        Fetches all snapshot data in 2 batch queries, then compares in memory.
        Used by the monitor loop; snapshot collection is handled by the snapshot job.
        Returns (anomalies, failed_labels).
        """
        from datetime import timedelta
        from google.cloud import bigquery as _bq

        now = datetime.now(timezone.utc)
        current_hour = now.hour
        today_str = now.strftime("%Y-%m-%d")
        from_date = (now.date() - timedelta(days=9)).isoformat()

        metric_ids = [m["metric_id"] for m in self._metric_configs]
        if not metric_ids:
            return []

        id_list = ", ".join(f"'{mid}'" for mid in metric_ids)

        # ── Batch query 1: today's current snapshot per metric ──────────────
        sql_today = (
            f"SELECT metric_id, cumulative_value, snapshot_hour "
            f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            f"WHERE metric_id IN ({id_list}) "
            f"AND snapshot_date = '{today_str}' "
            f"AND snapshot_hour <= {current_hour} "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY metric_id ORDER BY snapshot_hour DESC, captured_at DESC) = 1"
        )

        # ── Batch query 2: last 9 days final values per metric ───────────────
        sql_history = (
            f"SELECT metric_id, snapshot_date, MAX(cumulative_value) as value "
            f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            f"WHERE metric_id IN ({id_list}) "
            f"AND snapshot_date >= '{from_date}' "
            f"AND snapshot_date < '{today_str}' "
            f"GROUP BY metric_id, snapshot_date"
        )

        # ── Batch query 3: last week same hour (pace) per metric ─────────────
        last_week_str = (now.date() - timedelta(days=7)).isoformat()
        sql_pace = (
            f"SELECT metric_id, cumulative_value "
            f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            f"WHERE metric_id IN ({id_list}) "
            f"AND snapshot_date = '{last_week_str}' "
            f"AND snapshot_hour = {current_hour} "
            f"QUALIFY ROW_NUMBER() OVER (PARTITION BY metric_id ORDER BY captured_at DESC) = 1"
        )

        failed_labels: list[tuple[str, str]] = []
        try:
            today_rows = self.bq.run_query(sql_today)
            history_rows = self.bq.run_query(sql_history)
            pace_rows = self.bq.run_query(sql_pace)
        except Exception as e:
            logger.error("check_only batch query failed: %s", e)
            return [], []

        # Build lookup dicts
        today_values: dict[str, float] = {r["metric_id"]: r["cumulative_value"] for r in today_rows}
        pace_values: dict[str, float] = {r["metric_id"]: r["cumulative_value"] for r in pace_rows}
        # historical[metric_id][date_str] = value
        historical: dict[str, dict[str, float]] = {}
        for r in history_rows:
            mid = r["metric_id"]
            historical.setdefault(mid, {})[str(r["snapshot_date"])] = r["value"]

        # ── Fallback: metrics missing today's snapshot ───────────────────────
        missing_ids = [m["metric_id"] for m in self._metric_configs if m["metric_id"] not in today_values]
        if missing_ids:
            # Find latest snapshot date per missing metric
            missing_list = ", ".join(f"'{mid}'" for mid in missing_ids)
            sql_latest = (
                f"SELECT metric_id, MAX(snapshot_date) as latest_date "
                f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
                f"WHERE metric_id IN ({missing_list}) "
                f"GROUP BY metric_id"
            )
            try:
                latest_rows = self.bq.run_query(sql_latest)
                latest_dates = {r["metric_id"]: r["latest_date"] for r in latest_rows}
            except Exception as e:
                logger.warning("check_only fallback query failed: %s", e)
                latest_dates = {}

            stale_threshold_hours = 5
            for mid in missing_ids:
                latest = latest_dates.get(mid)
                # Calculate hours since latest snapshot
                if latest is not None:
                    if isinstance(latest, str):
                        latest = datetime.strptime(latest, "%Y-%m-%d").date()
                    latest_dt = datetime.combine(latest, datetime.min.time()).replace(tzinfo=timezone.utc)
                    hours_old = (now - latest_dt).total_seconds() / 3600
                else:
                    hours_old = 999  # no snapshot ever → always fetch

                if hours_old > stale_threshold_hours:
                    # Steep hasn't updated in >5h — fetch directly
                    label = next((m["metric_label"] for m in self._metric_configs if m["metric_id"] == mid), mid)
                    logger.info("%s: no snapshot for %dh, falling back to Steep API.", label, int(hours_old))
                    try:
                        value, refreshed_at, hist = self._fetch_values(mid)
                        if value is not None:
                            today_values[mid] = value
                            # Merge fetched history into our historical dict
                            for date_str, v in hist.items():
                                if date_str != today_str:
                                    historical.setdefault(mid, {})[date_str] = v
                            # Save snapshot so we don't fetch again next hour
                            if not self._already_captured(mid, today_str, value):
                                self._save_snapshot(mid, label, today_str, current_hour, value, refreshed_at)
                    except Exception as e:
                        logger.warning("%s: Steep fallback failed: %s", label, e)
                        failed_labels.append((label, f"Steep {type(e).__name__}"))
                else:
                    # Latest snapshot is fresh enough — use it
                    label = next((m["metric_label"] for m in self._metric_configs if m["metric_id"] == mid), mid)
                    logger.info("%s: using latest BQ snapshot from %s (%.1fh old).", label, latest, hours_old)
                    sql_use_latest = (
                        f"SELECT cumulative_value FROM `{Config.BQ_SNAPSHOT_TABLE}` "
                        f"WHERE metric_id = '{mid}' AND snapshot_date = '{latest}' "
                        f"ORDER BY snapshot_hour DESC LIMIT 1"
                    )
                    try:
                        rows = self.bq.run_query(sql_use_latest)
                        if rows:
                            today_values[mid] = rows[0]["cumulative_value"]
                    except Exception as e:
                        logger.warning("%s: latest snapshot fetch failed: %s", label, e)

        anomalies: list[Anomaly] = []
        total = len(self._metric_configs)

        for i, metric in enumerate(self._metric_configs):
            metric_id = metric["metric_id"]
            label = metric["metric_label"]
            direction = metric.get("direction", "down_is_bad")
            display_format = metric.get("display_format") or "number"

            current_value = today_values.get(metric_id)
            if current_value is None:
                logger.info("%s: no BQ snapshot for today, skipping.", label)
                failed_labels.append((label, "No snapshot"))
                if progress_callback:
                    progress_callback(i + 1, total, label)
                continue

            metric_hist = historical.get(metric_id, {})
            metric_thresholds = {
                comp: float(metric.get(f"{comp}_threshold", THRESHOLDS[comp]))
                for comp in ("pace", "dod", "wow")
            }

            # Run comparisons in memory — override pace baseline with batch value
            result = self._check_metric_from_cache(
                metric_id, label, direction, today_str, current_hour,
                current_value, metric_thresholds, metric_hist,
                pace_baseline=pace_values.get(metric_id),
                display_format=display_format,
            )
            anomalies.extend(result)

            if progress_callback:
                progress_callback(i + 1, total, label)

        # BQ metrics (SQL-based) still run as before
        try:
            anomalies.extend(self.check_bq_metrics())
        except Exception as e:
            logger.error("BQ metric check failed: %s", e)

        return anomalies, failed_labels

    def _check_metric_from_cache(
        self,
        metric_id: str, label: str, direction: str,
        today_str: str, current_hour: int, current_value: float,
        metric_thresholds: dict,
        historical: dict[str, float],
        pace_baseline: float | None,
        display_format: str = "number",
    ) -> list[Anomaly]:
        """Like _check_metric but uses pre-fetched pace_baseline instead of BQ query."""
        from datetime import timedelta
        today = datetime.strptime(today_str, "%Y-%m-%d").date()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)
        same_weekday_last_week = today - timedelta(days=7)

        anomalies: list[Anomaly] = []

        # Pace check — use pre-fetched baseline
        if pace_baseline is not None and pace_baseline != 0:
            anomaly = self._evaluate(
                metric_id, label, direction, "pace",
                current_value, pace_baseline,
                metric_thresholds["pace"],
                reference_date=today_str,
                display_format=display_format,
                baseline_date=same_weekday_last_week.isoformat(),
            )
            if anomaly:
                anomalies.append(anomaly)

        # DoD check
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

        # WoW check
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

    def finalize_daily_values(self, days: int = 60) -> int:
        """Fetch finalized daily values from Steep for all enabled metrics and upsert to BQ.

        Only stores dates up to yesterday — today's data is not yet final.
        Returns total number of rows upserted.
        Intended to run once per day at ~03:00 UTC when Steep has finalized previous day.
        """
        from datetime import timedelta
        now = datetime.now(timezone.utc)
        yesterday = (now.date() - timedelta(days=1)).isoformat()
        from_date = (now.date() - timedelta(days=days)).isoformat()

        metrics = self._metric_configs
        if not metrics:
            logger.warning("finalize_daily_values: no metric configs loaded.")
            return 0

        self.bq.ensure_daily_values_table()

        total_upserted = 0
        for metric in metrics:
            metric_id = metric["metric_id"]
            label = metric["metric_label"]
            try:
                resp = self.steep.query_metric(
                    metric_id=metric_id,
                    from_date=f"{from_date}T00:00:00Z",
                    to_date=f"{yesterday}T23:59:59Z",
                    time_grain="daily",
                )
                data = resp.get("data", [])
                rows = []
                for point in data:
                    date_str = (point.get("time") or "")[:10]
                    value = point.get("metric")
                    if date_str and value is not None and date_str <= yesterday:
                        rows.append({
                            "metric_id": metric_id,
                            "metric_label": label,
                            "value_date": date_str,
                            "final_value": float(value),
                        })
                if rows:
                    upserted = self.bq.upsert_daily_values(rows)
                    total_upserted += len(rows)
                    logger.info("finalize_daily_values: %s → %d rows upserted.", label, len(rows))
            except Exception as e:
                logger.error("finalize_daily_values failed for %s: %s", label, e)

        logger.info("finalize_daily_values complete: %d total rows upserted.", total_upserted)
        return total_upserted

    def _check_metric(
        self, metric_id: str, label: str, direction: str,
        today_str: str, current_hour: int, current_value: float,
        metric_thresholds: dict,
        historical: dict[str, float] | None = None,
        display_format: str = "number",
        force_pace: bool = False,
    ) -> list[Anomaly]:
        """Run pace, DoD, and WoW checks for one metric.

        Pace uses BQ snapshots: today at current hour vs same weekday last week at same hour.
        DoD and WoW uses Steep historical data direkt (alltid korrekt slutvärde).
        force_pace: if True and no pace anomaly found now, scan back 30 days for the most recent one.
        """
        from datetime import timedelta

        today = datetime.strptime(today_str, "%Y-%m-%d").date()
        yesterday = today - timedelta(days=1)
        day_before = today - timedelta(days=2)
        same_weekday_last_week = today - timedelta(days=7)
        historical = historical or {}

        anomalies: list[Anomaly] = []

        # Pace check: today vs same weekday last week at same hour (BQ snapshots)
        # strict=False: använd närmaste snapshot om exakt timme saknas (t.ex. vid manuell run)
        last_week_same_hour = self._get_nearest_snapshot(metric_id, same_weekday_last_week.isoformat(), current_hour, strict=False)
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

        # force_pace: if still no pace anomaly, scan back for the most recent one (debug runs only)
        if force_pace and not any(a.comparison == "pace" for a in anomalies):
            fallback = self._scan_for_pace_anomaly(
                metric_id, label, direction, today, current_hour, metric_thresholds, display_format
            )
            if fallback:
                anomalies.append(fallback)

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

    def _scan_for_pace_anomaly(
        self, metric_id: str, label: str, direction: str,
        today, current_hour: int,
        metric_thresholds: dict, display_format: str,
    ) -> "Anomaly | None":
        """Scan back up to 30 days to find the most recent pace anomaly in BQ snapshots.

        Used only by force_pace=True (debug/test runs) when the current pace check finds nothing.
        Returns the most recent past day where the pace threshold was exceeded.
        """
        from datetime import timedelta
        from google.cloud import bigquery as _bq

        lookback_start = today - timedelta(days=37)
        sql = (
            f"SELECT snapshot_date, snapshot_hour, cumulative_value "
            f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
            "WHERE metric_id = @metric_id "
            "AND snapshot_date >= @start_date "
            "AND snapshot_date <= @today "
            "ORDER BY snapshot_date DESC, snapshot_hour DESC"
        )
        params = [
            _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
            _bq.ScalarQueryParameter("start_date", "DATE", lookback_start.isoformat()),
            _bq.ScalarQueryParameter("today", "DATE", today.isoformat()),
        ]
        rows = self.bq.run_query(sql, params=params)

        # Build: date_str -> list of (hour, value)
        by_date: dict[str, list[tuple[int, float]]] = {}
        for r in rows:
            d = str(r["snapshot_date"])
            by_date.setdefault(d, []).append((int(r["snapshot_hour"]), float(r["cumulative_value"])))

        def _nearest(date_str: str, hour: int) -> "float | None":
            entries = by_date.get(date_str)
            if not entries:
                return None
            candidates = [(h, v) for h, v in entries if h <= hour]
            if candidates:
                return max(candidates, key=lambda x: x[0])[1]
            return min(entries, key=lambda x: abs(x[0] - hour))[1]

        for offset in range(0, 30):
            ref_date = today - timedelta(days=offset)
            base_date = ref_date - timedelta(days=7)
            ref_val = _nearest(ref_date.isoformat(), current_hour)
            base_val = _nearest(base_date.isoformat(), current_hour)
            if ref_val is None or base_val is None or base_val == 0:
                continue
            anomaly = self._evaluate(
                metric_id, label, direction, "pace",
                ref_val, base_val,
                metric_thresholds["pace"],
                reference_date=ref_date.isoformat(),
                display_format=display_format,
                baseline_date=base_date.isoformat(),
            )
            if anomaly:
                return anomaly
        return None

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

    # ── BQ metric checking ────────────────────────────────────────────────

    def check_bq_metrics(self, progress_callback=None) -> list["Anomaly"]:
        """Fetch and check all enabled BQ metrics. Returns anomalies."""
        configs = getattr(self, "_bq_metric_configs", [])
        if not configs:
            return []

        anomalies: list[Anomaly] = []
        total = len(configs)
        _lock = threading.Lock()
        _counter = [0]

        def _process_one(metric: dict) -> list[Anomaly]:
            metric_id = metric["metric_id"]
            label = metric["metric_label"]
            direction = metric.get("direction", "alert_on_drop")
            display_format = metric.get("display_format") or "number"
            sql_query = metric.get("sql_query", "")
            result: list[Anomaly] = []

            try:
                rows = self.bq.run_query(sql_query)
            except Exception as e:
                logger.error("BQ metric query failed for %s: %s", label, e)
                with _lock:
                    _counter[0] += 1
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result

            # Build date → value dict from query results
            historical: dict[str, float] = {}
            for row in rows:
                date_val = row.get("date")
                value = row.get("value")
                if date_val is not None and value is not None:
                    date_str = date_val if isinstance(date_val, str) else str(date_val)
                    historical[date_str] = float(value)

            if not historical:
                logger.warning("No data returned for BQ metric %s", label)
                with _lock:
                    _counter[0] += 1
                    if progress_callback:
                        progress_callback(_counter[0], total, label)
                return result

            metric_thresholds = {
                comp: float(metric.get(f"{comp}_threshold", THRESHOLDS[comp]))
                for comp in ("pace", "dod", "wow")
            }

            from datetime import timezone, timedelta
            now = datetime.now(timezone.utc)
            today_str = now.strftime("%Y-%m-%d")

            # BQ metrics have no intraday snapshots — skip pace, run DoD + WoW only
            today = datetime.strptime(today_str, "%Y-%m-%d").date()
            yesterday = today - timedelta(days=1)
            day_before = today - timedelta(days=2)
            same_weekday_last_week = today - timedelta(days=7)
            baseline_start = datetime.strptime(Config.BASELINE_START_DATE, "%Y-%m-%d").date()

            yesterday_val = historical.get(yesterday.isoformat())
            day_before_val = historical.get(day_before.isoformat())
            last_week_val = historical.get(same_weekday_last_week.isoformat())

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
                    result.append(anomaly)

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
                    result.append(anomaly)

            with _lock:
                _counter[0] += 1
                if progress_callback:
                    progress_callback(_counter[0], total, label)

            return result

        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(_process_one, m) for m in configs]
            for future in as_completed(futures):
                try:
                    anomalies.extend(future.result())
                except Exception as e:
                    logger.error("Unhandled error in BQ metric worker: %s", e)

        return anomalies
