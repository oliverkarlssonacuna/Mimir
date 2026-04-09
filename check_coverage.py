"""
Analyze BQ snapshot coverage for the past 7 days and compare a sample of
metrics against live Steep API values.

Run from repo root:
    python check_coverage.py
"""
import os, sys
from datetime import datetime, timedelta, timezone
from collections import defaultdict

# Make src importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from dotenv import load_dotenv
load_dotenv()

from config import Config
from bq_client import BQClient
from steep_client import SteepClient

bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=5000)
steep = SteepClient(api_key=Config.STEEP_API_TOKEN)

now = datetime.now(timezone.utc)
today_str = now.strftime("%Y-%m-%d")
dates = [(now - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]
oldest = dates[-1]

print("=" * 70)
print(f"BQ SNAPSHOT COVERAGE  ({oldest} → {today_str})")
print("=" * 70)

# ── 1. Load all enabled metrics from BQ ──────────────────────────────────
configs = bq.load_metric_configs(Config.BQ_METRIC_CONFIGS_TABLE)
enabled_ids = {c["metric_id"]: c["metric_label"] for c in configs}
print(f"\nEnabled metrics in BQ config: {len(enabled_ids)}")

# ── 2. Bulk-fetch snapshots for last 8 days ───────────────────────────────
sql = f"""
SELECT metric_id, metric_label, snapshot_date, snapshot_hour, cumulative_value, captured_at
FROM `{Config.BQ_SNAPSHOT_TABLE}`
WHERE snapshot_date >= '{oldest}'
ORDER BY metric_id, snapshot_date, snapshot_hour
"""
rows = bq.run_query(sql)
print(f"Total snapshot rows fetched: {len(rows)}")

# Build: metric_id -> set of dates with at least one snapshot
coverage: dict[str, set] = defaultdict(set)
latest: dict[tuple, float] = {}  # (metric_id, date) -> latest cumulative_value by hour

for r in rows:
    mid = r["metric_id"]
    d = str(r["snapshot_date"])[:10]
    h = r["snapshot_hour"]
    v = r["cumulative_value"]
    coverage[mid].add(d)
    key = (mid, d)
    # Keep value from the highest hour (closest to end of day)
    existing_h = latest.get(("H_" + mid, d), -1)
    if h >= existing_h:
        latest[key] = v
        latest[("H_" + mid, d)] = h

# ── 3. Coverage report ────────────────────────────────────────────────────
dates_set = set(dates)
missing_any = []
for mid, label in sorted(enabled_ids.items(), key=lambda x: x[1]):
    covered = coverage.get(mid, set())
    missing = [d for d in dates if d not in covered]
    if missing:
        missing_any.append((label, mid, missing))

if not missing_any:
    print(f"\n✅ All {len(enabled_ids)} metrics have snapshots for all 8 days.\n")
else:
    print(f"\n⚠️  {len(missing_any)} metrics missing data on some days:\n")
    for label, mid, missing in missing_any:
        print(f"  {label} ({mid}): missing {missing}")

# Per-day summary
print("\nSnapshots per day:")
day_counts: dict[str, int] = defaultdict(int)
for r in rows:
    d = str(r["snapshot_date"])[:10]
    if r["metric_id"] in enabled_ids:
        day_counts[d] += 1

for d in sorted(day_counts):
    # Count distinct metrics for this day
    metrics_on_day = len({r["metric_id"] for r in rows if str(r["snapshot_date"])[:10] == d and r["metric_id"] in enabled_ids})
    print(f"  {d}: {metrics_on_day}/{len(enabled_ids)} metrics have at least one snapshot")

# ── 4. Steep vs BQ comparison for 5 sample metrics ───────────────────────
print("\n" + "=" * 70)
print("STEEP vs BQ VALUE COMPARISON (5 sample metrics, yesterday)")
print("=" * 70)

yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
sample = list(configs)[:5]

for c in sample:
    mid = c["metric_id"]
    label = c["metric_label"]

    # BQ: highest-hour value for yesterday
    bq_val = latest.get((mid, yesterday))
    bq_hour = latest.get(("H_" + mid, yesterday), None)

    # Steep: daily value for yesterday
    try:
        from_date = (now - timedelta(days=3)).strftime("%Y-%m-%dT00:00:00Z")
        to_date = now.strftime("%Y-%m-%dT23:59:59Z")
        resp = steep.query_metric(mid, from_date=from_date, to_date=to_date, time_grain="daily")
        steep_val = None
        for pt in resp.get("data", []):
            if pt.get("time", "")[:10] == yesterday:
                steep_val = pt.get("metric")
                break
    except Exception as e:
        steep_val = f"ERROR: {e}"

    match = ""
    if isinstance(steep_val, float) and isinstance(bq_val, float):
        diff = abs(steep_val - bq_val)
        pct = diff / steep_val * 100 if steep_val else 0
        match = f"  ✅ match" if pct < 1 else f"  ⚠️  diff {pct:.1f}%"

    print(f"\n  {label}")
    print(f"    BQ (hour {bq_hour}): {bq_val}")
    print(f"    Steep (daily):    {steep_val}{match}")

print("\nDone.")
