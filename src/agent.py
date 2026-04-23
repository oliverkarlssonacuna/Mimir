"""
Agent – agentic loop with Gemini function calling for Steep metric analysis.

Tools exposed to the LLM:
  query_steep_metric(metric_id, days, time_grain)  – fetch metric data from Steep
  get_snapshot_history(metric_id, days)             – fetch BQ snapshots
  plot_results(data, chart_type, x_col, y_col, title)  – draw a chart, return file path
  get_jira_releases(date)                          – Jira release context
"""

import json
import logging
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from typing import Any

from google import genai
from google.genai import types

import jira_client

logger = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a data analyst helping to analyse anomalies in game metrics from Steep. You have access to four tools: `query_steep_metric`, `get_snapshot_history`, `plot_results` and `get_jira_releases`.

CRITICAL INSTRUCTIONS:
- You MUST use function calls, NEVER write code.
- Do NOT use `print(...)`, `import`, `pd.DataFrame` or similar.
- Do NOT use HTML tags (e.g. `<img>`, `<b>`, `<a>`). Use Markdown only.
- Do NOT reference chart file paths or embed images in text. Charts are sent as Discord file attachments automatically.

## Language
- Default to English.
- If the user writes in Swedish, respond in Swedish.
- Match the language of the user's message.

## Context
We monitor 80+ metrics from a mobile game application via Steep Analytics. Metrics include active users, FTUE (first-time user experience) funnels, matchmaking, playtime, retention (D7, D30), coverage per game mode, and infrastructure costs.

Beta launched March 9, 2026. Data before that is unreliable.
Strong day-of-week effect: Thursday/Friday ~30% lower, weekends ~40% higher.

## Tools
- `query_steep_metric(metric_id, days, time_grain)` – fetch daily/weekly data from Steep API
- `get_snapshot_history(metric_id, days)` – fetch saved snapshots (cumulative 4h values) from BQ
- `plot_results(data, chart_type, x_col, y_col, title, anomaly_date, anomaly_value, baseline_date, baseline_value, baseline_date_2, baseline_value_2, pace_date, pace_value)` – draw a chart. Always pass anomaly_date + anomaly_value, baseline_date + baseline_value (WoW), baseline_date_2 + baseline_value_2 (DoD), and pace_date + pace_value (intraday) when available — these are needed to draw the comparison arrows correctly.
- `get_jira_releases(date)` – fetch Jira releases near a date

## Tone
- Professional and data-driven. Be direct and confident.
- Avoid filler phrases like "Great question!" or "Of course!".
- Use **bold** formatting for key numbers in text (e.g. **-20.6%**, **18,320 users**).

## Rules
- Be concise but include important numbers.
- When analysing an anomaly: 1) fetch recent data, 2) draw a graph, 3) provide a summary with specific explanation.
- If the anomaly coincides with a release or game milestone (±7 days), name it explicitly as the likely cause.
- Do NOT say "investigate further" — commit to the most likely explanation based on the data and context.
- If a metric's data includes `"unit": "%"`, its values are already in percentage scale (e.g. 13.4 means 13.4%). Always append `%` when showing the raw values.
"""

THREAD_SYSTEM_PROMPT = """You are a data analyst helping users explore game metrics from Steep in a Discord thread. You have access to four tools: `query_steep_metric`, `get_snapshot_history`, `plot_results` and `get_jira_releases`.

CRITICAL INSTRUCTIONS:
- You MUST use function calls, NEVER write code.
- Do NOT use HTML tags (e.g. `<img>`, `<b>`, `<a>`). Use Markdown only.
- Do NOT reference chart file paths or embed images in text. Charts are sent as Discord file attachments automatically.

## Language
- Default to English.
- If the user writes in Swedish, respond in Swedish.
- Match the language of the user's message.

## Context
We monitor game metrics via Steep Analytics. Beta launched March 9, 2026.
Strong day-of-week effect: Thursday/Friday ~30% lower, weekends ~40% higher.

## Tone
- Professional and data-driven. Be direct and confident.
- Avoid filler phrases like "Great question!" or "Of course!".
- Use **bold** formatting for key numbers in text (e.g. **-20.6%**, **18,320 users**).

## Rules
- Be concise but include important numbers.
- When creating charts, ALWAYS describe what the graph shows in text too.
- ALWAYS call `query_steep_metric` to fetch fresh data before calling `plot_results`. Never reconstruct or reuse data from previous text responses — always fetch from the API.
- When calling `plot_results`, always pass `anomaly_date` + `anomaly_value`, `baseline_date` + `baseline_value` (WoW), `baseline_date_2` + `baseline_value_2` (DoD), and `pace_date` + `pace_value` (intraday) whenever those values are known — these are required to draw comparison arrows correctly.
- Only answer questions about metrics and data. Politely decline if the user asks about other topics.
- If a metric's data includes `"unit": "%"`, its values are already in percentage scale (e.g. 13.4 means 13.4%). Always append `%` when showing the raw values.
- When the user mentions a date or time period (e.g. "from the beginning of the year", "since January", "last 30 days", "the past month"), calculate the number of days from that start date to today (today's date is provided at the top of the prompt) and use that as the `days` parameter in `query_steep_metric`. Never guess — always derive `days` from the dates.
"""

# ── Tool definitions ──────────────────────────────────────────────────────────

def _get_tools() -> types.Tool:
    return types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="query_steep_metric",
            description="Fetch time-series data for a Steep metric. Returns daily/weekly data points.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "metric_id": types.Schema(
                        type=types.Type.STRING,
                        description="The Steep metric ID to query.",
                    ),
                    "days": types.Schema(
                        type=types.Type.INTEGER,
                        description="Number of days of history to fetch (default 14).",
                    ),
                    "time_grain": types.Schema(
                        type=types.Type.STRING,
                        description="Time grain: 'daily', 'weekly', or 'monthly'. Default 'daily'.",
                    ),
                },
                required=["metric_id"],
            ),
        ),
        types.FunctionDeclaration(
            name="get_snapshot_history",
            description="Fetch saved intraday snapshots (4-hour cumulative values) from BQ for a metric.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "metric_id": types.Schema(
                        type=types.Type.STRING,
                        description="The Steep metric ID.",
                    ),
                    "days": types.Schema(
                        type=types.Type.INTEGER,
                        description="Number of days of snapshot history (default 7).",
                    ),
                },
                required=["metric_id"],
            ),
        ),
        types.FunctionDeclaration(
            name="plot_results",
            description=(
                "Draw a chart from data and save it as a PNG file. "
                "Supports line, bar, and pie charts. "
                "Returns the file path to the saved PNG."
            ),
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "data": types.Schema(
                        type=types.Type.STRING,
                        description="JSON-encoded list of row dicts (the data to plot).",
                    ),
                    "chart_type": types.Schema(
                        type=types.Type.STRING,
                        description="One of: 'bar', 'line', 'pie'.",
                    ),
                    "x_col": types.Schema(
                        type=types.Type.STRING,
                        description="Column name to use for the X-axis.",
                    ),
                    "y_col": types.Schema(
                        type=types.Type.STRING,
                        description="Column name to use for the Y-axis.",
                    ),
                    "title": types.Schema(
                        type=types.Type.STRING,
                        description="Chart title.",
                    ),
                    "group_col": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Column name to group by for multi-line charts.",
                    ),
                    "anomaly_date": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Date (YYYY-MM-DD) to draw a vertical marker line at.",
                    ),
                    "anomaly_change_pct": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Change percentage to annotate on the anomaly point, e.g. '+28.7%'.",
                    ),
                    "baseline_date": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Date (YYYY-MM-DD) to mark as the WoW baseline comparison point (shown in yellow).",
                    ),
                    "baseline_date_2": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Date (YYYY-MM-DD) for a second baseline marker (e.g. DoD baseline, shown in green).",
                    ),
                    "pace_date": types.Schema(
                        type=types.Type.STRING,
                        description="Optional. Date (YYYY-MM-DD) for today's intraday pace marker (shown in orange).",
                    ),
                    "anomaly_value": types.Schema(
                        type=types.Type.NUMBER,
                        description="Optional. Exact numeric value at the anomaly date (the current/trigger value). Used to place the dot accurately.",
                    ),
                    "baseline_value": types.Schema(
                        type=types.Type.NUMBER,
                        description="Optional. Exact numeric value at the WoW baseline date. Used to place the yellow dot accurately.",
                    ),
                    "baseline_value_2": types.Schema(
                        type=types.Type.NUMBER,
                        description="Optional. Exact numeric value at the DoD baseline date. Used to place the green dot accurately.",
                    ),
                    "pace_value": types.Schema(
                        type=types.Type.NUMBER,
                        description="Optional. Exact numeric value for today's intraday pace. Used to place the orange dot accurately.",
                    ),
                },

                required=["data", "chart_type", "x_col", "y_col", "title"],
            ),
        ),
        types.FunctionDeclaration(
            name="get_jira_releases",
            description="Look up Jira releases near a specific date (±7 days).",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "date": types.Schema(
                        type=types.Type.STRING,
                        description="ISO date string (YYYY-MM-DD) to search around.",
                    ),
                },
                required=["date"],
            ),
        ),
    ])


# ── Tool implementations ──────────────────────────────────────────────────────

def _query_steep_metric(steep_client: Any, metric_id: str, days: int = 14, time_grain: str = "daily") -> list[dict]:
    """Fetch metric data from Steep API, filling in missing days with 0."""
    try:
        resp = steep_client.query_metric_recent(metric_id, days=days, time_grain=time_grain)
        data = resp.get("data", [])
        points = {p["time"][:10]: p["metric"] for p in data}

        if time_grain == "daily" and points:
            # Fill missing dates with 0, always extending to yesterday
            # so the anomaly date is always included even if Steep hasn't published it yet
            from datetime import date as _date
            all_dates = sorted(points.keys())
            start = datetime.strptime(all_dates[0], "%Y-%m-%d").date()
            end = max(
                datetime.strptime(all_dates[-1], "%Y-%m-%d").date(),
                _date.today() - timedelta(days=1),
            )
            filled = []
            current = start
            while current <= end:
                d = current.isoformat()
                filled.append({"date": d, "value": points.get(d, 0)})
                current += timedelta(days=1)
            return filled

        return [{"date": p["time"][:10], "value": p["metric"]} for p in data]
    except Exception as e:
        logger.error("Steep query failed: %s", e)
        return [{"error": str(e)}]


def _get_snapshot_history(bq_client: Any, metric_id: str, days: int = 7) -> list[dict]:
    """Fetch snapshot history from BQ."""
    from config import Config
    sql = (
        f"SELECT snapshot_date, snapshot_hour, cumulative_value, "
        f"FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', captured_at) as captured "
        f"FROM `{Config.BQ_SNAPSHOT_TABLE}` "
        f"WHERE metric_id = @metric_id "
        f"AND snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @days DAY) "
        f"ORDER BY snapshot_date, snapshot_hour"
    )
    from google.cloud import bigquery as _bq
    params = [
        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),
        _bq.ScalarQueryParameter("days", "INT64", days),
    ]
    try:
        return bq_client.run_query(sql, params=params)
    except Exception as e:
        logger.error("Snapshot query failed: %s", e)
        return [{"error": str(e)}]


def _pretty_label(text: str) -> str:
    """Turn 'game_format_arena' into 'Game Format Arena'."""
    return text.replace("_", " ").title()


def _thin_ticks(ax, xs: list[str], max_ticks: int = 15):
    """Show at most max_ticks evenly spaced x-axis labels."""
    n = len(xs)
    if n <= max_ticks:
        ax.set_xticks(range(n))
        ax.set_xticklabels(xs, rotation=40, ha="right", fontsize=9)
    else:
        step = max(1, n // max_ticks)
        tick_positions = list(range(0, n, step))
        ax.set_xticks(tick_positions)
        ax.set_xticklabels([xs[i] for i in tick_positions], rotation=40, ha="right", fontsize=9)


def _plot_results(data_json: str, chart_type: str, x_col: str, y_col: str, title: str, group_col: str = "", highlight_values: str = "", anomaly_date: str = "", anomaly_change_pct: str = "", baseline_date: str = "", baseline_date_2: str = "", pace_date: str = "", anomaly_value: float | None = None, baseline_value: float | None = None, baseline_value_2: float | None = None, pace_value: float | None = None) -> str:
    """Render a chart and save to a temp PNG. Returns the file path."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker
    import matplotlib.patheffects as pe

    try:
        if isinstance(data_json, str):
            data = json.loads(data_json)
        elif isinstance(data_json, (list, dict)):
            data = data_json
        else:
            data = json.loads(str(data_json))
    except (json.JSONDecodeError, TypeError) as e:
        return f"error: could not parse data JSON – {e}"

    if not data:
        return "error: no data to plot"

    # Ensure data is a list of dicts
    if isinstance(data, dict):
        data = [data]
    parsed_rows = []
    for row in data:
        if isinstance(row, str):
            try:
                row = json.loads(row)
            except (json.JSONDecodeError, TypeError):
                continue
        if isinstance(row, dict):
            parsed_rows.append(row)
    if not parsed_rows:
        return "error: data could not be parsed into row dicts"
    data = parsed_rows

    # Sort data by x_col if values look like dates
    import re as _re
    _date_pattern = _re.compile(r"^\d{4}-\d{2}-\d{2}")
    sample_x = str(data[0].get(x_col, ""))
    if _date_pattern.match(sample_x):
        data.sort(key=lambda r: str(r.get(x_col, "")))

    highlighted = {v.strip() for v in highlight_values.split(",") if v.strip()} if highlight_values else set()

    # ── Mimir color palette ─────────────────────────────────────────────
    BG = "#0b0d11"
    SURFACE = "#111318"
    BORDER = "#1e2028"
    GRID = "#1a1c24"
    TEXT = "#f1f5f9"
    TEXT_MUTED = "#cbd5e1"
    TEXT_DIM = "#94a3b8"
    ACCENT = "#818cf8"       # indigo-400 — primary line
    ACCENT_GLOW = "#6366f1"  # indigo-500 — fill
    RED = "#fb7185"          # rose-400 — anomaly
    YELLOW = "#fbbf24"       # amber-400 — WoW baseline
    GREEN = "#34d399"        # emerald-400 — DoD baseline
    ACCENT_COLORS = [ACCENT, RED, GREEN, YELLOW, "#f472b6", "#22d3ee"]

    # ── Figure setup ──────────────────────────────────────────────────────
    fig, ax = plt.subplots(figsize=(14, 6.5))
    fig.patch.set_facecolor(BG)
    ax.set_facecolor(SURFACE)

    # No spines at all — clean edge
    for spine in ax.spines.values():
        spine.set_visible(False)

    # Ticks — clean, readable
    ax.tick_params(axis="x", colors=TEXT_DIM, labelsize=8.5, length=0, pad=10)
    ax.tick_params(axis="y", colors=TEXT_DIM, labelsize=9, length=0, pad=6)

    # Horizontal grid — barely visible
    ax.grid(axis="y", linestyle="-", alpha=0.15, color="#ffffff", linewidth=0.5, zorder=0)
    ax.set_axisbelow(True)

    # Y-axis formatter
    def _y_fmt(x, _):
        v = abs(x)
        if v == 0:
            return "0"
        if v < 0.01:
            return f"{x:.4f}"
        if v < 1:
            return f"{x:.2f}"
        if v < 1000:
            return f"{x:.1f}"
        if v < 1_000_000:
            return f"{x:,.0f}"
        return f"{x / 1_000_000:.1f}M"
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(_y_fmt))

    # ── Data extraction helpers ───────────────────────────────────────────
    def _extract_xy(data_list):
        xs = [str(row.get(x_col, "")) for row in data_list]
        ys_raw = [row.get(y_col, 0) for row in data_list]
        try:
            ys = [float(v) for v in ys_raw]
        except (TypeError, ValueError):
            ys = list(range(len(xs)))
        return xs, ys

    # ── Plot types ────────────────────────────────────────────────────────
    if group_col and group_col in data[0]:
        from collections import defaultdict
        groups: dict = defaultdict(dict)
        all_xs: list = []
        for row in data:
            x = str(row.get(x_col, ""))
            g = str(row.get(group_col, "unknown"))
            y_raw = row.get(y_col, 0)
            try:
                y = float(y_raw)
            except (TypeError, ValueError):
                y = 0
            groups[g][x] = y
            if x not in all_xs:
                all_xs.append(x)
        all_xs.sort()
        x_indices = list(range(len(all_xs)))
        for i, (group_name, xy) in enumerate(sorted(groups.items())):
            ys = [xy.get(x, 0) for x in all_xs]
            color = ACCENT_COLORS[i % len(ACCENT_COLORS)]
            if chart_type == "line":
                ax.plot(x_indices, ys, linewidth=2, zorder=3, label=_pretty_label(group_name), color=color)
            else:
                ax.bar(x_indices, ys, label=_pretty_label(group_name), alpha=0.85, color=color, linewidth=0, zorder=3)
        ax.legend(framealpha=0.9, fontsize=9, facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT, loc="upper right")
        _thin_ticks(ax, all_xs)
        xs, ys = all_xs, None
    else:
        xs, ys = _extract_xy(data)
        x_indices = list(range(len(xs)))

        if chart_type == "bar":
            colors = [RED if x in highlighted else ACCENT for x in xs]
            bars = ax.bar(x_indices, ys, color=colors, alpha=0.85, linewidth=0, zorder=3, width=0.6)
            max_idx = ys.index(max(ys)) if ys else 0
            for i, (bar, y_val) in enumerate(zip(bars, ys)):
                if i == max_idx:
                    ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(),
                            f"{y_val:,.0f}" if y_val >= 1 else f"{y_val:.3f}",
                            ha="center", va="bottom", fontsize=8, color=TEXT, fontweight="bold")
            if highlighted:
                from matplotlib.patches import Patch
                ax.legend(handles=[Patch(facecolor=ACCENT, label="Normal"), Patch(facecolor=RED, label="Anomalous")],
                          fontsize=9, facecolor=SURFACE, edgecolor=BORDER, labelcolor=TEXT)
            _thin_ticks(ax, xs)

        elif chart_type == "line":
            # Main line — smooth, refined width
            ax.plot(x_indices, ys, color=ACCENT, linewidth=2.2, zorder=4, solid_capstyle="round", solid_joinstyle="round")
            # Single soft area fill below the line
            ax.fill_between(x_indices, ys, alpha=0.12, color=ACCENT_GLOW, zorder=2, linewidth=0)
            _thin_ticks(ax, xs)

        elif chart_type == "pie":
            ax.pie(ys, labels=[_pretty_label(x) for x in xs], autopct="%1.1f%%",
                   textprops={"color": TEXT}, colors=ACCENT_COLORS[:len(xs)])
        else:
            ax.bar(x_indices, ys, color=ACCENT, alpha=0.85)
            _thin_ticks(ax, xs)

    # ── Helper: format value for labels ─────────────────────────────────
    def _fmt_val(v):
        if v == int(v):
            return f"{int(v):,}"  # 18, 3, 1 — no decimals for whole numbers
        if abs(v) >= 10:
            return f"{v:,.1f}"
        if abs(v) >= 0.01:
            return f"{v:.2f}"
        return f"{v:.4f}"

    def _fmt_pair(a, b):
        """Format two values consistently — both use the same decimal style."""
        sa, sb = _fmt_val(a), _fmt_val(b)
        return f"{sa} → {sb}"

    # ── Collect all annotation points to avoid overlap ────────────────────
    _annotations = []  # list of (idx, line_y, label_y, color, label_text)
    # line_y = position on Steep line (dot sits here)
    # label_y = BQ ground-truth value (shown in label/pill/% calc)
    _anomaly_y = None   # WoW/DoD current value (BQ)
    _pace_y = None      # Pace current value (BQ)

    # anomaly_date = WoW/DoD current (yesterday) - RED
    if anomaly_date and chart_type != "pie":
        search_xs = all_xs if (group_col and group_col in data[0]) else xs
        anomaly_idx = None
        for idx, label in enumerate(search_xs):
            if label.startswith(anomaly_date):
                anomaly_idx = idx
                break
        if anomaly_idx is not None and chart_type == "line" and ys is not None:
            _anomaly_line_y = ys[anomaly_idx]
            _anomaly_y = anomaly_value if anomaly_value is not None else _anomaly_line_y
            _annotations.append((anomaly_idx, _anomaly_line_y, _anomaly_y, RED, f"Anomaly  ·  {_fmt_val(_anomaly_y)}"))

    # pace_date = Pace current (today) - ORANGE
    ORANGE = "#fb923c"  # orange-400
    _pace_idx = None
    if pace_date and chart_type == "line" and ys is not None:
        search_xs = all_xs if (group_col and group_col in data[0]) else xs
        for idx, lbl in enumerate(search_xs):
            if lbl.startswith(pace_date):
                _pace_idx = idx
                break
        if _pace_idx is not None:
            _pace_line_y = ys[_pace_idx]
            _pace_y = pace_value if pace_value is not None else _pace_line_y
            _annotations.append((_pace_idx, _pace_line_y, _pace_y, ORANGE, f"Pace (today)  ·  {_fmt_val(_pace_y)}"))

    def _find_baseline_idx(baseline_dt):
        search_xs_b = all_xs if (group_col and group_col in data[0]) else xs
        b_idx = None
        for idx, lbl in enumerate(search_xs_b):
            if lbl.startswith(baseline_dt):
                b_idx = idx
                break
        if b_idx is None and search_xs_b:
            import datetime as _dt
            try:
                target = _dt.date.fromisoformat(baseline_dt)
                best_idx, best_delta = None, None
                for idx, lbl in enumerate(search_xs_b):
                    try:
                        d = _dt.date.fromisoformat(lbl[:10])
                        delta = abs((d - target).days)
                        if best_delta is None or delta < best_delta:
                            best_idx, best_delta = idx, delta
                    except ValueError:
                        continue
                if best_delta is not None and best_delta <= 2:
                    b_idx = best_idx
            except ValueError:
                pass
        return b_idx

    def _pct_change_label(prefix, current_val, baseline_val):
        """Build label like 'WoW ▼ 26.5%  (18 → 1)' showing change."""
        if current_val is None or baseline_val is None or baseline_val == 0:
            return f"{prefix}  ·  {_fmt_val(baseline_val or 0)}"
        pct = ((current_val - baseline_val) / abs(baseline_val)) * 100
        arrow = "▲" if pct > 0 else "▼"
        return f"{prefix}  {arrow} {abs(pct):.1f}%  ({_fmt_pair(baseline_val, current_val)})"

    # baseline_date = WoW/Pace baseline (same day last week) - YELLOW
    _wow_baseline_idx = None
    _wow_baseline_y = None
    if baseline_date and chart_type == "line" and ys is not None:
        b_idx = _find_baseline_idx(baseline_date)
        if b_idx is not None:
            _wow_baseline_idx = b_idx
            _wow_baseline_line_y = ys[b_idx]
            _wow_baseline_y = baseline_value if baseline_value is not None else _wow_baseline_line_y
            # Label based on context:
            # - WoW (or combined WoW+Pace): red anomaly dot exists → use "WoW" with yesterday's value
            # - Pace-only: no red anomaly dot → use "Pace baseline" with today's pace value
            if _anomaly_y is not None:
                _baseline_pill_label = _pct_change_label("WoW", _anomaly_y, _wow_baseline_y)
            elif _pace_y is not None:
                _baseline_pill_label = _pct_change_label("Pace baseline", _pace_y, _wow_baseline_y)
            else:
                _baseline_pill_label = f"Last week  ·  {_fmt_val(_wow_baseline_y)}"
            _annotations.append((b_idx, _wow_baseline_line_y, _wow_baseline_y, YELLOW, _baseline_pill_label))

    # baseline_date_2 = DoD baseline (day before yesterday) - GREEN
    _dod_baseline_idx = None
    _dod_baseline_y = None
    if baseline_date_2 and chart_type == "line" and ys is not None:
        b_idx = _find_baseline_idx(baseline_date_2)
        if b_idx is not None:
            _dod_baseline_idx = b_idx
            _dod_baseline_line_y = ys[b_idx]
            _dod_baseline_y = baseline_value_2 if baseline_value_2 is not None else _dod_baseline_line_y
            _annotations.append((b_idx, _dod_baseline_line_y, _dod_baseline_y, GREEN, _pct_change_label("DoD", _anomaly_y, _dod_baseline_y)))

    # ── Build comparison pairs for connecting lines ───────────────────────
    # Uses line_y (Steep position) for visual arrows so they connect to the dots
    _comparison_pairs = []
    if chart_type == "line" and ys is not None:
        _anomaly_idx_for_pairs = None
        _anomaly_line_y_for_pairs = None
        if anomaly_date:
            search_xs = all_xs if (group_col and group_col in data[0]) else xs
            for idx, lbl in enumerate(search_xs):
                if lbl.startswith(anomaly_date):
                    _anomaly_idx_for_pairs = idx
                    _anomaly_line_y_for_pairs = ys[idx]
                    break
        if _wow_baseline_idx is not None and _anomaly_idx_for_pairs is not None:
            # Use BQ ground-truth values for pct calculation — Steep line may be 0 for that date
            _comparison_pairs.append((_wow_baseline_idx, _wow_baseline_y or ys[_wow_baseline_idx], _anomaly_idx_for_pairs, _anomaly_y or _anomaly_line_y_for_pairs, YELLOW, "WoW"))
        if _dod_baseline_idx is not None and _anomaly_idx_for_pairs is not None:
            _comparison_pairs.append((_dod_baseline_idx, _dod_baseline_y or ys[_dod_baseline_idx], _anomaly_idx_for_pairs, _anomaly_y or _anomaly_line_y_for_pairs, GREEN, "DoD"))
        if _wow_baseline_idx is not None and _pace_idx is not None:
            _comparison_pairs.append((_wow_baseline_idx, _wow_baseline_y or ys[_wow_baseline_idx], _pace_idx, _pace_y or ys[_pace_idx], ORANGE, "Pace"))

    # ── Draw annotations ───────────────────────────────────────────────
    if _annotations and ys:
        n_pts = len(xs) if xs else 1

        # Dot Y-position uses line value; label uses BQ ground-truth.
        # If line is 0/missing on that date, fall back to ground-truth value.
        def _resolve_dot_y(line_y, label_y):
            if line_y is None or line_y == 0:
                return label_y if label_y is not None else 0
            return line_y

        # Compute Y range based on what's actually shown (dots + line)
        dot_ys_used = [_resolve_dot_y(ly, gy) for _, ly, gy, _, _ in _annotations]
        all_y_candidates = list(ys) + dot_ys_used + [v for v in [_wow_baseline_y, _dod_baseline_y] if v is not None]
        _y_max_data = max(all_y_candidates) if all_y_candidates else 1
        y_min_data = min(all_y_candidates) if all_y_candidates else 0

        # Clip y-axis when an old historical spike dwarfs the comparison region.
        # Threshold 2.5x = leave moderate spikes visible, clip extreme outliers only.
        ann_focus_max = max([v for v in dot_ys_used + [_wow_baseline_y, _dod_baseline_y] if v is not None] or [_y_max_data])
        _spike_clipped = False
        if ann_focus_max > 0 and _y_max_data > ann_focus_max * 2.5:
            y_max = ann_focus_max * 1.7
            _spike_clipped = True
        else:
            y_max = _y_max_data

        y_min = min(0, y_min_data) if y_min_data >= 0 else y_min_data
        y_range = max(y_max - y_min, abs(y_max) * 0.01, 1e-9)
        # Headroom for top badges + dot value labels
        ax.set_ylim(bottom=y_min - y_range * 0.04, top=y_max + y_range * 0.28)

        # ── Horizontal reference lines at baseline values ─────────────────
        # Detect collision: if WoW and DoD baselines are within 8% of y-range,
        # offset DoD label to mid-chart to avoid overlap.
        _baselines_close = (
            _wow_baseline_y is not None and _dod_baseline_y is not None
            and abs(_wow_baseline_y - _dod_baseline_y) < y_range * 0.08
        )

        if _wow_baseline_y is not None:
            ax.axhline(y=_wow_baseline_y, color=YELLOW, lw=1.0, ls=(0, (4, 3)),
                       alpha=0.55, zorder=2.5)
            ax.text(0.005, _wow_baseline_y, f" Last week  {_fmt_val(_wow_baseline_y)} ",
                    transform=ax.get_yaxis_transform(),
                    color=YELLOW, fontsize=8.5, va="center", ha="left",
                    fontweight="600", zorder=3,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=BG,
                              edgecolor=YELLOW, alpha=0.95, linewidth=0.8))

        if _dod_baseline_y is not None and _dod_baseline_y != _wow_baseline_y:
            ax.axhline(y=_dod_baseline_y, color=GREEN, lw=1.0, ls=(0, (4, 3)),
                       alpha=0.55, zorder=2.5)
            # If close to WoW label, shift to mid-chart in axes coords
            dod_x = 0.45 if _baselines_close else 0.005
            dod_ha = "center" if _baselines_close else "left"
            ax.text(dod_x, _dod_baseline_y, f" Day before  {_fmt_val(_dod_baseline_y)} ",
                    transform=ax.get_yaxis_transform(),
                    color=GREEN, fontsize=8.5, va="center", ha=dod_ha,
                    fontweight="600", zorder=3,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=BG,
                              edgecolor=GREEN, alpha=0.95, linewidth=0.8))

        # ── Spike indicator if Y was clipped ─────────────────────────────
        if _spike_clipped:
            ax.text(0.01, 0.04, f"↑ peak  {_fmt_val(_y_max_data)}",
                    transform=ax.transAxes, color=TEXT_DIM,
                    fontsize=8, va="bottom", ha="left", zorder=10,
                    bbox=dict(boxstyle="round,pad=0.3", facecolor=SURFACE,
                              edgecolor=BORDER, alpha=0.85, linewidth=0.5))

        # ── Dots + value labels ──────────────────────────────────────────
        for a_idx, a_line_y, a_label_y, a_color, _text in _annotations:
            dot_y = _resolve_dot_y(a_line_y, a_label_y)

            # Outer halo (background colour) makes dot pop on coloured line
            ax.plot(a_idx, dot_y, "o", color=BG, markersize=15, zorder=6)
            # Coloured fill with white edge
            ax.plot(a_idx, dot_y, "o", color=a_color, markersize=9, zorder=7,
                    markeredgecolor="#ffffff", markeredgewidth=1.4)

            # Value label above dot — coloured text, no box (cleaner)
            rel = a_idx / max(n_pts - 1, 1)
            ha_dot = "right" if rel > 0.92 else ("left" if rel < 0.08 else "center")
            x_off = -10 if rel > 0.92 else (10 if rel < 0.08 else 0)
            ax.annotate(
                _fmt_val(a_label_y),
                xy=(a_idx, dot_y),
                xytext=(x_off, 12), textcoords="offset points",
                fontsize=10.5, color=a_color, ha=ha_dot, va="bottom",
                fontweight="700", zorder=8,
                path_effects=[pe.withStroke(linewidth=3, foreground=BG)],
            )

        # ── Delta badges (top-right of chart) ────────────────────────────
        badges = []  # (label, pct, color)
        if _wow_baseline_y is not None and _anomaly_y is not None and _wow_baseline_y != 0:
            pct = ((_anomaly_y - _wow_baseline_y) / abs(_wow_baseline_y)) * 100
            badges.append(("WoW", pct, YELLOW))
        if _dod_baseline_y is not None and _anomaly_y is not None and _dod_baseline_y != 0:
            pct = ((_anomaly_y - _dod_baseline_y) / abs(_dod_baseline_y)) * 100
            badges.append(("DoD", pct, GREEN))
        if _wow_baseline_y is not None and _pace_y is not None and _wow_baseline_y != 0:
            pct = ((_pace_y - _wow_baseline_y) / abs(_wow_baseline_y)) * 100
            badges.append(("Pace", pct, ORANGE))

        for i, (label, pct, b_color) in enumerate(badges):
            arrow = "▼" if pct < 0 else "▲"
            text = f"{label}  {arrow} {abs(pct):.1f}%"
            x_pos = 0.99
            y_pos = 0.97 - i * 0.085
            ax.text(
                x_pos, y_pos, text,
                transform=ax.transAxes,
                ha="right", va="top",
                fontsize=10, color=b_color, fontweight="700",
                bbox=dict(boxstyle="round,pad=0.55", facecolor=SURFACE,
                          edgecolor=b_color, alpha=0.97, linewidth=1.2),
                zorder=9,
            )

    # ── Title (left-aligned, clean) ───────────────────────────────────────
    ax.set_title(title, fontsize=14, fontweight="700", color=TEXT, loc="left", pad=16)
    ax.set_xlabel("")

    fig.tight_layout(pad=2.0)

    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False, prefix="mimir_chart_")
    fig.savefig(tmp.name, dpi=120, facecolor=fig.get_facecolor(), bbox_inches="tight")
    plt.close(fig)
    logger.info("Chart saved to %s", tmp.name)
    return tmp.name


# ── Agent loop ────────────────────────────────────────────────────────────────

class AgentResponse:
    """What the agent returns after processing a question."""
    def __init__(self, text: str, chart_path: str | None = None):
        self.text = text
        self.chart_path = chart_path  # path to PNG if a chart was generated


class Agent:
    def __init__(self, config: Any, bq_client: Any, steep_client: Any, percent_metric_ids: set[str] | None = None):
        self.config = config
        self.bq = bq_client
        self.steep = steep_client
        self.client = genai.Client(
            vertexai=True,
            project=config.GCP_PROJECT_ID,
            location=config.GCP_VERTEXAI_REGION,
        )
        self.model = config.GEMINI_MODEL
        self._percent_metric_ids: set[str] = percent_metric_ids or set()

    def ask(self, question: str, system_prompt: str | None = None, tools_enabled: bool = True) -> AgentResponse:
        """Send a question through the agentic loop and return the final answer."""
        active_system_prompt = system_prompt or SYSTEM_PROMPT
        contents: list[types.Content] = [
            types.Content(role="user", parts=[types.Part(text=question)])
        ]

        chart_path: str | None = None

        # No-tools fast path: single Gemini call, no agentic loop
        if not tools_enabled:
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=active_system_prompt,
                    temperature=0,
                    max_output_tokens=2048,
                ),
            )
            candidate = response.candidates[0]
            parts = candidate.content.parts if candidate.content and candidate.content.parts else []
            text_parts = [p.text for p in parts if p.text]
            return AgentResponse(text="\n".join(text_parts).strip() or "Here is the analysis:")

        max_iterations = 10

        for iteration in range(max_iterations):
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=active_system_prompt,
                    tools=[_get_tools()],
                    temperature=0,
                    max_output_tokens=2048,
                ),
            )

            candidate = response.candidates[0]
            parts = candidate.content.parts if candidate.content and candidate.content.parts else []
            if not parts:
                finish_reason = candidate.finish_reason
                logger.warning("Gemini returned empty parts. finish_reason=%s", finish_reason)
                if str(finish_reason) == "FinishReason.MALFORMED_FUNCTION_CALL":
                    # Retry once without tools – ask Gemini to answer in plain text
                    logger.info("Retrying without function calling due to MALFORMED_FUNCTION_CALL")
                    retry_response = self.client.models.generate_content(
                        model=self.model,
                        contents=contents,
                        config=types.GenerateContentConfig(
                            system_instruction=active_system_prompt,
                            temperature=0,
                        ),
                    )
                    retry_candidate = retry_response.candidates[0]
                    retry_parts = retry_candidate.content.parts if retry_candidate.content and retry_candidate.content.parts else []
                    text_parts = [p.text for p in retry_parts if p.text]
                    return AgentResponse(text="\n".join(text_parts).strip() or "Could not generate a response.", chart_path=chart_path)
                return AgentResponse(text=f"Gemini returned an empty response (finish_reason={finish_reason}).")
            contents.append(types.Content(role="model", parts=parts))

            # Collect any function calls in this response
            function_calls = [p for p in parts if p.function_call]

            if not function_calls:
                # No more tool calls – extract the final text answer
                text_parts = [p.text for p in parts if p.text]
                final_text = "\n".join(text_parts).strip()
                # Guard: if Gemini output code instead of using tools, log and return error
                if "default_api." in final_text or ("print(" in final_text and "import" in final_text):
                    logger.warning("Gemini output code instead of using tools. Returning error.")
                    return AgentResponse(text="⚠️ Analysis failed: Gemini generated code instead of using tools. Please try again.")
                # Strip file paths and any HTML img tags from the text — Discord users see the chart as an attachment
                import re
                if chart_path:
                    final_text = re.sub(r'[A-Za-z]:\\[^\s]+\.png', '', final_text)
                    final_text = re.sub(r'/tmp/[^\s]+\.png', '', final_text)
                # Always strip HTML img tags regardless of whether a chart was generated
                final_text = re.sub(r'<img[^>]*>', '', final_text)
                final_text = final_text.strip()
                return AgentResponse(text=final_text or "Here are the results:", chart_path=chart_path)

            # Execute each tool call and feed results back
            tool_results = []
            for part in function_calls:
                fc = part.function_call
                name = fc.name
                args = dict(fc.args)
                logger.info("Tool call: %s(%s)", name, list(args.keys()))

                if name == "query_steep_metric":
                    result = _query_steep_metric(
                        self.steep,
                        args["metric_id"],
                        days=int(args.get("days", 14)),
                        time_grain=args.get("time_grain", "daily"),
                    )
                    # Convert decimal to percentage for percent-format metrics
                    if args["metric_id"] in self._percent_metric_ids:
                        result = [{**p, "value": round(p["value"] * 100, 4), "unit": "%"} if "value" in p else p for p in result]
                    tool_results.append(
                        types.Part(
                            function_response=types.FunctionResponse(
                                name=name,
                                response={"result": result},
                            )
                        )
                    )

                elif name == "get_snapshot_history":
                    result = _get_snapshot_history(
                        self.bq,
                        args["metric_id"],
                        days=int(args.get("days", 7)),
                    )
                    tool_results.append(
                        types.Part(
                            function_response=types.FunctionResponse(
                                name=name,
                                response={"result": result},
                            )
                        )
                    )

                elif name == "get_jira_releases":
                    from config import Config
                    try:
                        lookup_date = date.fromisoformat(args["date"])
                        releases = jira_client.get_releases_near_date(lookup_date, Config.JIRA_PROJECT_KEY)
                        context = jira_client.format_release_context(releases, lookup_date)
                        result = context if context else "No Jira releases found near this date."
                    except Exception as e:
                        logger.error("Jira lookup failed: %s", e)
                        result = f"Jira lookup failed: {e}"
                    tool_results.append(
                        types.Part(
                            function_response=types.FunctionResponse(
                                name=name,
                                response={"result": result},
                            )
                        )
                    )

                elif name == "plot_results":
                    path = _plot_results(
                        data_json=args["data"],
                        chart_type=args.get("chart_type", "bar"),
                        x_col=args["x_col"],
                        y_col=args["y_col"],
                        title=args.get("title", ""),
                        group_col=args.get("group_col", ""),
                        highlight_values=args.get("highlight_values", ""),
                        anomaly_date=args.get("anomaly_date", ""),
                        anomaly_change_pct=args.get("anomaly_change_pct", ""),
                        baseline_date=args.get("baseline_date", ""),
                        baseline_date_2=args.get("baseline_date_2", ""),
                        pace_date=args.get("pace_date", ""),
                        anomaly_value=float(args["anomaly_value"]) if args.get("anomaly_value") is not None else None,
                        baseline_value=float(args["baseline_value"]) if args.get("baseline_value") is not None else None,
                        baseline_value_2=float(args["baseline_value_2"]) if args.get("baseline_value_2") is not None else None,
                        pace_value=float(args["pace_value"]) if args.get("pace_value") is not None else None,
                    )
                    if not path.startswith("error"):
                        chart_path = path
                    tool_results.append(
                        types.Part(
                            function_response=types.FunctionResponse(
                                name=name,
                                response={"file_path": path},
                            )
                        )
                    )

            contents.append(types.Content(role="user", parts=tool_results))

        return AgentResponse(
            text="Reached maximum iterations without a final answer.",
            chart_path=chart_path,
        )

    def ask_stream(self, question: str, system_prompt: str | None = None):
        """Stream a no-tools Gemini response, yielding text chunks as they arrive."""
        active_system_prompt = system_prompt or SYSTEM_PROMPT
        contents: list[types.Content] = [
            types.Content(role="user", parts=[types.Part(text=question)])
        ]
        for chunk in self.client.models.generate_content_stream(
            model=self.model,
            contents=contents,
            config=types.GenerateContentConfig(
                system_instruction=active_system_prompt,
                temperature=0,
                max_output_tokens=2048,
            ),
        ):
            if chunk.text:
                yield chunk.text
