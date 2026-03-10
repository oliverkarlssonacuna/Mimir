"""
Agent – simple agentic loop with Gemini function calling.

Tools exposed to the LLM:
  run_query(sql)                             – execute SQL, return rows
  plot_results(data, chart_type, x_col, y_col, title)  – draw a chart, return file path
"""

import json
import logging
import os
import tempfile
from datetime import date
from typing import Any

from google import genai
from google.genai import types

import jira_client

logger = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a data analyst assistant with access to three tools: `run_query`, `plot_results`, and `get_jira_releases`. You help users query and understand anomaly detection results stored in BigQuery.

CRITICAL INSTRUCTIONS:
- You MUST call tools using function calls, never output Python code or any code.
- Do NOT write `print(...)`, `import`, `pd.DataFrame`, `plt.plot` or any code.
- Do NOT use `default_api.` syntax. Use function calls directly.
- When plotting, ALWAYS aggregate data first (e.g. GROUP BY DATE) so data has one row per date, not one row per event.

## Anomaly tracking table
Full table name (always use this exactly):
  `lia-project-sandbox-deletable.anomaly_checks_demo.daily_anomaly_check_results`

## Schema (anomaly table)
| Column       | Type      | Description                                              |
|--------------|-----------|----------------------------------------------------------|
| table_name   | STRING    | Name of the source table that was checked                |
| is_valid     | BOOL      | True = no anomalies found, False = anomaly detected      |
| reason       | STRING    | Human-readable description of the problem (if is_valid=False, else null/empty) |
| checked_at   | TIMESTAMP | When the check was run                                   |

## Source tables
The actual source data lives in the dataset configured via BQ_SOURCE_DATASET (e.g. `project.dataset.<table_name>`).
When asked to analyse an anomaly for a specific table:
- **MANDATORY FIRST STEP:** Before writing ANY query against a source table, you MUST run `SELECT column_name, data_type FROM \`project.dataset.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = 'table'` to discover the exact column names. NEVER guess column names — always use the names returned by INFORMATION_SCHEMA.
- Then read the anomaly reason carefully and decide what SQL and chart type best reveals the problem:
  - **Enum/value anomaly** (reason mentions unexpected value): Query `SELECT column_value, COUNT(*) as count FROM table WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY) GROUP BY column_value ORDER BY count DESC`. Use a bar chart with `x_col=column_value`, `y_col=count`, and pass the anomalous values (extracted from reason) as `highlight_values` (comma-separated) so they appear red.
  - **Count/volume anomaly** (reason mentions too few/many rows): Query daily counts with `GROUP BY DATE(timestamp)`. Use a line chart.
  - Use your judgment for other cases based on the schema and reason.
- IMPORTANT: Always query data relative to TODAY (use `CURRENT_DATE()` or `CURRENT_TIMESTAMP()`), NOT relative to the anomaly date. The chart must show the full picture up to today so the user can see the current state.
- Always aggregate data by day (GROUP BY DATE(...)) before plotting — never fetch raw rows for charts.
- Use INFORMATION_SCHEMA if you need to discover the schema of a source table before querying it.

## Jira release context
You have access to `get_jira_releases(date)` to look up Jira releases near a specific date.
- Call this tool when the user asks about releases, versions, deployments, or when you need to correlate an anomaly with a release.
- Do NOT call it for unrelated questions (e.g. "show me row counts").
- If the anomaly timing coincides with a release (±7 days), mention it explicitly. The anomaly may have been caused by the release — e.g. a schema change, renamed enum value, or new pipeline version.
- If the anomaly is likely explained by a release, say so clearly: "This anomaly likely coincides with release X on <date>, which may have introduced this change."
- If no nearby release exists, do not speculate about releases.

## Rules
- ALWAYS use fully qualified table names in SQL.
- Use standard GoogleSQL (BigQuery dialect).
- ALWAYS call `run_query` and `plot_results` as function calls — NEVER output code.
- SQL MUST aggregate: for value anomalies use `GROUP BY column_value`; for volume anomalies use `GROUP BY DATE(...)`. Never fetch raw rows for charts.
- For enum/value anomalies: use `chart_type='bar'` and pass `highlight_values` with the anomalous values from the reason.
- When analysing an anomaly, ALWAYS pass `anomaly_date` (YYYY-MM-DD) to `plot_results` so a vertical marker line is drawn at the anomaly date on the chart.
- Pass the aggregated result directly to `plot_results`.
- Keep answers concise. If there are anomalies, highlight them clearly.
- If is_valid is False, always include the reason in your answer.
- Respond in the same language as the user's message. If the user writes in Swedish, respond in Swedish. Default to English.
"""

THREAD_SYSTEM_PROMPT = """You are a data analyst assistant with access to three tools: `run_query`, `plot_results`, and `get_jira_releases`. You help users explore a specific BigQuery source table via follow-up questions in a thread.

CRITICAL INSTRUCTIONS:
- You MUST call tools using function calls, never output Python code or any code.
- Do NOT write `print(...)`, `import`, `pd.DataFrame`, `plt.plot` or any code.
- Do NOT use `default_api.` syntax. Use function calls directly.
- When plotting, ALWAYS aggregate data first (e.g. GROUP BY DATE) so data has one row per date, not one row per event.
- NEVER query the anomaly check table. The user's questions are ALWAYS about the source table specified in the prompt.
- You ONLY answer questions related to the source table data, the anomaly, and Jira releases. If the user asks anything unrelated (math, trivia, general knowledge, etc.), politely decline and say you can only help with questions about the table and its data.

## Rules
- ALWAYS use fully qualified table names in SQL.
- Use standard GoogleSQL (BigQuery dialect).
- ALWAYS call `run_query` and `plot_results` as function calls — NEVER output code.
- **MANDATORY FIRST STEP:** Before writing ANY query against a source table, you MUST run `SELECT column_name, data_type FROM \`project.dataset.INFORMATION_SCHEMA.COLUMNS\` WHERE table_name = 'table'` to discover the exact column names. NEVER guess column names — always use the names returned by INFORMATION_SCHEMA.
- For line plots over time, always aggregate: `GROUP BY DATE(...)` and `ORDER BY date`.
- If the user says 'all', 'alla', or 'all occurrences', do NOT add a time filter — query all available data.
- Pass the aggregated result directly to `plot_results`.
- Keep answers concise. If there are anomalies, highlight them clearly.
- When you create a chart, ALWAYS include a text summary describing what the chart shows: the key values, trends, date range, and any notable patterns. The user cannot refer back to chart images in follow-up questions, so your text description is the only record of what was plotted.
- Respond in the same language as the user's message. If the user writes in Swedish, respond in Swedish. Default to English.
"""

# ── Tool definitions ──────────────────────────────────────────────────────────

def _get_tools() -> types.Tool:
    return types.Tool(function_declarations=[
        types.FunctionDeclaration(
            name="run_query",
            description="Execute a GoogleSQL query against BigQuery and return the results as a list of row objects.",
            parameters=types.Schema(
                type=types.Type.OBJECT,
                properties={
                    "sql": types.Schema(
                        type=types.Type.STRING,
                        description="Valid GoogleSQL query. Always use fully qualified table names.",
                    ),
                },
                required=["sql"],
            ),
        ),
        types.FunctionDeclaration(
            name="plot_results",
            description=(
                "Draw a chart from data and save it as a PNG file. "
                "Use this when the user asks for a graph, chart, or visualisation. "
                "Supports grouped multi-line charts via group_col. "
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
                        description="Column name to use for the X-axis (or pie labels).",
                    ),
                    "y_col": types.Schema(
                        type=types.Type.STRING,
                        description="Column name to use for the Y-axis (or pie values).",
                    ),
                    "title": types.Schema(
                        type=types.Type.STRING,
                        description="Chart title.",
                    ),
                    "group_col": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Optional. Column name to group by, producing one line/bar series per unique value. "
                            "Use this when comparing multiple categories over time, e.g. one line per game_format."
                        ),
                    ),
                    "highlight_values": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Optional. Comma-separated list of X-axis values to highlight in red. "
                            "Use this to visually mark anomalous values in a bar chart, e.g. the unexpected enum values mentioned in the anomaly reason."
                        ),
                    ),
                    "anomaly_date": types.Schema(
                        type=types.Type.STRING,
                        description=(
                            "Optional. The date (YYYY-MM-DD) when the anomaly was detected. "
                            "A vertical dashed line with a label will be drawn at this date on the chart. "
                            "Always pass this when analysing an anomaly so the user can see exactly when it was flagged."
                        ),
                    ),
                },
                required=["data", "chart_type", "x_col", "y_col", "title"],
            ),
        ),
        types.FunctionDeclaration(
            name="get_jira_releases",
            description=(
                "Look up Jira releases near a specific date (±7 days). "
                "Use this when the user asks about releases, versions, or deployments, "
                "or when you want to check if a release may explain an anomaly."
            ),
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

def _run_query(bq_client: Any, sql: str) -> list[dict]:
    # Block destructive SQL statements
    first_keyword = sql.strip().split()[0].upper() if sql.strip() else ""
    blocked = {"DROP", "DELETE", "TRUNCATE", "ALTER", "CREATE", "INSERT", "UPDATE", "MERGE", "GRANT", "REVOKE"}
    if first_keyword in blocked:
        logger.warning("Blocked dangerous SQL: %s", sql[:200])
        return [{"error": f"SQL statement '{first_keyword}' is not allowed. Only SELECT queries are permitted."}]
    try:
        return bq_client.run_query(sql)
    except Exception as e:
        logger.error("Query failed: %s", e)
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


def _plot_results(data_json: str, chart_type: str, x_col: str, y_col: str, title: str, group_col: str = "", highlight_values: str = "", anomaly_date: str = "") -> str:
    """Render a chart and save to a temp PNG. Returns the file path."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import matplotlib.ticker as mticker

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

    # Ensure data is a list of dicts — Gemini sometimes sends nested structures
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

    # Sort data by x_col if values look like dates (YYYY-MM-DD)
    import re as _re
    _date_pattern = _re.compile(r"^\d{4}-\d{2}-\d{2}")
    sample_x = str(data[0].get(x_col, ""))
    if _date_pattern.match(sample_x):
        data.sort(key=lambda r: str(r.get(x_col, "")))

    highlighted = {v.strip() for v in highlight_values.split(",") if v.strip()} if highlight_values else set()

    # Dark theme for Discord
    BG_COLOR = "#2b2d31"
    SURFACE_COLOR = "#1e1f22"
    TEXT_COLOR = "#e0e0e0"
    GRID_COLOR = "#3a3c41"
    NORMAL_COLOR = "#5865F2"       # Discord blurple
    HIGHLIGHT_COLOR = "#ed4245"    # Discord red
    ACCENT_COLORS = ["#5865F2", "#ed4245", "#57F287", "#FEE75C", "#EB459E", "#00BCD4"]

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.set_facecolor(SURFACE_COLOR)
    fig.patch.set_facecolor(BG_COLOR)
    ax.tick_params(colors=TEXT_COLOR, labelsize=9)
    ax.xaxis.label.set_color(TEXT_COLOR)
    ax.yaxis.label.set_color(TEXT_COLOR)
    ax.title.set_color(TEXT_COLOR)
    for spine in ax.spines.values():
        spine.set_color(GRID_COLOR)

    # Format large Y-axis numbers with thousands separator
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: f"{x:,.0f}"))

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
                ax.plot(x_indices, ys, marker="o", label=_pretty_label(group_name), color=color, linewidth=2, markersize=4)
            else:
                ax.bar(x_indices, ys, label=_pretty_label(group_name), alpha=0.85, color=color)
        ax.legend(framealpha=0.9, fontsize=10, facecolor=SURFACE_COLOR, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
        ax.set_xlabel(_pretty_label(x_col), fontsize=11)
        ax.set_ylabel(_pretty_label(y_col), fontsize=11)
        ax.grid(axis="y", linestyle="--", alpha=0.3, color=GRID_COLOR)
        _thin_ticks(ax, all_xs)
    else:
        xs = [str(row.get(x_col, "")) for row in data]
        ys_raw = [row.get(y_col, 0) for row in data]
        try:
            ys = [float(v) for v in ys_raw]
        except (TypeError, ValueError):
            ys = list(range(len(xs)))

        x_indices = list(range(len(xs)))

        if chart_type == "bar":
            colors = [HIGHLIGHT_COLOR if x in highlighted else NORMAL_COLOR for x in xs]
            ax.bar(x_indices, ys, color=colors, alpha=0.85, edgecolor=SURFACE_COLOR, linewidth=0.5)
            if highlighted:
                from matplotlib.patches import Patch
                legend_elements = [
                    Patch(facecolor=NORMAL_COLOR, label="Expected"),
                    Patch(facecolor=HIGHLIGHT_COLOR, label="Anomalous"),
                ]
                ax.legend(handles=legend_elements, fontsize=10, facecolor=SURFACE_COLOR, edgecolor=GRID_COLOR, labelcolor=TEXT_COLOR)
            ax.set_xlabel(_pretty_label(x_col), fontsize=11)
            ax.set_ylabel(_pretty_label(y_col), fontsize=11)
            ax.grid(axis="y", linestyle="--", alpha=0.3, color=GRID_COLOR)
            _thin_ticks(ax, xs)
        elif chart_type == "line":
            ax.plot(x_indices, ys, marker="o", color=NORMAL_COLOR, linewidth=2, markersize=4)
            ax.fill_between(x_indices, ys, alpha=0.1, color=NORMAL_COLOR)
            ax.set_xlabel(_pretty_label(x_col), fontsize=11)
            ax.set_ylabel(_pretty_label(y_col), fontsize=11)
            ax.grid(axis="y", linestyle="--", alpha=0.3, color=GRID_COLOR)
            ax.grid(axis="x", linestyle=":", alpha=0.15, color=GRID_COLOR)
            _thin_ticks(ax, xs)
        elif chart_type == "pie":
            ax.pie(ys, labels=[_pretty_label(x) for x in xs], autopct="%1.1f%%",
                   textprops={"color": TEXT_COLOR}, colors=ACCENT_COLORS[:len(xs)])
        else:
            ax.bar(x_indices, ys, color=NORMAL_COLOR, alpha=0.85)
            _thin_ticks(ax, xs)

    # Draw vertical marker line at anomaly date if provided
    if anomaly_date and chart_type != "pie":
        # Find the x-axis position matching the anomaly_date
        if group_col and group_col in data[0]:
            search_xs = all_xs
        else:
            search_xs = xs
        anomaly_idx = None
        for idx, label in enumerate(search_xs):
            if label.startswith(anomaly_date):
                anomaly_idx = idx
                break
        if anomaly_idx is not None:
            ax.axvline(x=anomaly_idx, color="#ed4245", linestyle="--", linewidth=2, alpha=0.8)
            ax.annotate(
                f"Anomaly\n{anomaly_date}",
                xy=(anomaly_idx, ax.get_ylim()[1] * 0.92),
                fontsize=9, color="#ed4245", fontweight="bold",
                ha="center", va="top",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#1e1f22", edgecolor="#ed4245", alpha=0.9),
            )

    ax.set_title(_pretty_label(title), fontsize=14, fontweight="bold", pad=14, color=TEXT_COLOR)
    fig.tight_layout()

    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False, prefix="bqbot_chart_")
    fig.savefig(tmp.name, dpi=150, facecolor=fig.get_facecolor())
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
    def __init__(self, config: Any, bq_client: Any):
        self.config = config
        self.bq = bq_client
        self.client = genai.Client(
            vertexai=True,
            project=config.GCP_PROJECT_ID,
            location=config.GCP_VERTEXAI_REGION,
        )
        self.model = config.GEMINI_MODEL

    def ask(self, question: str, system_prompt: str | None = None) -> AgentResponse:
        """Send a question through the agentic loop and return the final answer."""
        active_system_prompt = system_prompt or SYSTEM_PROMPT
        contents: list[types.Content] = [
            types.Content(role="user", parts=[types.Part(text=question)])
        ]

        chart_path: str | None = None
        max_iterations = 10

        for iteration in range(max_iterations):
            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=types.GenerateContentConfig(
                    system_instruction=active_system_prompt,
                    tools=[_get_tools()],
                    temperature=0,
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
                # Strip file paths from the text — Discord users see the chart as an attachment
                if chart_path:
                    import re
                    final_text = re.sub(r'[A-Za-z]:\\[^\s]+\.png', '', final_text)
                    final_text = re.sub(r'/tmp/[^\s]+\.png', '', final_text)
                    final_text = final_text.strip()
                return AgentResponse(text=final_text or "Here are the results:", chart_path=chart_path)

            # Execute each tool call and feed results back
            tool_results = []
            for part in function_calls:
                fc = part.function_call
                name = fc.name
                args = dict(fc.args)
                logger.info("Tool call: %s(%s)", name, list(args.keys()))

                if name == "run_query":
                    result = _run_query(self.bq, args["sql"])
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
