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
from typing import Any

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

# ── System prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """You are a data analyst assistant with access to two tools: `run_query` and `plot_results`. You help users query and understand anomaly detection results stored in BigQuery.

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
- First fetch the table schema using INFORMATION_SCHEMA.
- Then read the anomaly reason carefully and decide what SQL and chart type best reveals the problem:
  - **Enum/value anomaly** (reason mentions unexpected value): Query `SELECT column_value, COUNT(*) as count FROM table WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 60 DAY) GROUP BY column_value ORDER BY count DESC`. Use a bar chart with `x_col=column_value`, `y_col=count`, and pass the anomalous values (extracted from reason) as `highlight_values` (comma-separated) so they appear red.
  - **Count/volume anomaly** (reason mentions too few/many rows): Query daily counts with `GROUP BY DATE(timestamp)`. Use a line chart.
  - Use your judgment for other cases based on the schema and reason.
- Always aggregate data by day (GROUP BY DATE(...)) before plotting — never fetch raw rows for charts.
- Use INFORMATION_SCHEMA if you need to discover the schema of a source table before querying it.

## Jira release context
If the prompt includes Jira releases near the anomaly date, use them to enrich the analysis:
- If the anomaly timing coincides with a release (±3 days), mention it explicitly. The anomaly may have been caused by the release — e.g. a schema change, renamed enum value, or new pipeline version.
- If the anomaly is likely explained by a release, say so clearly: "This anomaly likely coincides with release X on <date>, which may have introduced this change."
- If no nearby release exists, do not speculate about releases.

## Rules
- ALWAYS use fully qualified table names in SQL.
- Use standard GoogleSQL (BigQuery dialect).
- ALWAYS call `run_query` and `plot_results` as function calls — NEVER output code.
- SQL MUST aggregate: for value anomalies use `GROUP BY column_value`; for volume anomalies use `GROUP BY DATE(...)`. Never fetch raw rows for charts.
- For enum/value anomalies: use `chart_type='bar'` and pass `highlight_values` with the anomalous values from the reason.
- Pass the aggregated result directly to `plot_results`.
- Keep answers concise. If there are anomalies, highlight them clearly.
- If is_valid is False, always include the reason in your answer.
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
                },
                required=["data", "chart_type", "x_col", "y_col", "title"],
            ),
        ),
    ])


# ── Tool implementations ──────────────────────────────────────────────────────

def _run_query(bq_client: Any, sql: str) -> list[dict]:
    try:
        return bq_client.run_query(sql)
    except Exception as e:
        logger.error("Query failed: %s", e)
        return [{"error": str(e)}]


def _plot_results(data_json: str, chart_type: str, x_col: str, y_col: str, title: str, group_col: str = "", highlight_values: str = "") -> str:
    """Render a chart and save to a temp PNG. Returns the file path."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    try:
        if isinstance(data_json, (list, dict)):
            data = data_json
        else:
            data = json.loads(data_json)
    except json.JSONDecodeError as e:
        return f"error: could not parse data JSON – {e}"

    if not data:
        return "error: no data to plot"

    highlighted = {v.strip() for v in highlight_values.split(",") if v.strip()} if highlight_values else set()

    fig, ax = plt.subplots(figsize=(14, 6))
    ax.set_facecolor("#f8f9fa")
    fig.patch.set_facecolor("#ffffff")

    NORMAL_COLOR = "#2196F3"
    HIGHLIGHT_COLOR = "#e53935"

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
        COLORS = ["#2196F3", "#FF5722", "#4CAF50", "#9C27B0", "#FF9800", "#00BCD4"]
        for i, (group_name, xy) in enumerate(sorted(groups.items())):
            ys = [xy.get(x, 0) for x in all_xs]
            color = COLORS[i % len(COLORS)]
            if chart_type == "line":
                ax.plot(all_xs, ys, marker="o", label=group_name, color=color, linewidth=2, markersize=5)
            else:
                ax.bar(all_xs, ys, label=group_name, alpha=0.8, color=color)
        ax.legend(framealpha=0.9, fontsize=10)
        ax.set_xlabel(x_col, fontsize=11)
        ax.set_ylabel(y_col, fontsize=11)
        ax.grid(axis="y", linestyle="--", alpha=0.5)
        plt.xticks(rotation=45, ha="right", fontsize=9)
    else:
        xs = [str(row.get(x_col, "")) for row in data]
        ys_raw = [row.get(y_col, 0) for row in data]
        try:
            ys = [float(v) for v in ys_raw]
        except (TypeError, ValueError):
            ys = list(range(len(xs)))

        if chart_type == "bar":
            colors = [HIGHLIGHT_COLOR if x in highlighted else NORMAL_COLOR for x in xs]
            bars = ax.bar(xs, ys, color=colors, alpha=0.85, edgecolor="white", linewidth=0.5)
            # Add legend if there are highlights
            if highlighted:
                from matplotlib.patches import Patch
                legend_elements = [
                    Patch(facecolor=NORMAL_COLOR, label="Expected"),
                    Patch(facecolor=HIGHLIGHT_COLOR, label="Anomalous"),
                ]
                ax.legend(handles=legend_elements, fontsize=10)
            ax.set_xlabel(x_col, fontsize=11)
            ax.set_ylabel(y_col, fontsize=11)
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            plt.xticks(rotation=45, ha="right", fontsize=9)
        elif chart_type == "line":
            ax.plot(xs, ys, marker="o", color=NORMAL_COLOR, linewidth=2, markersize=5)
            ax.set_xlabel(x_col, fontsize=11)
            ax.set_ylabel(y_col, fontsize=11)
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.grid(axis="x", linestyle=":", alpha=0.3)
            plt.xticks(rotation=45, ha="right", fontsize=9)
        elif chart_type == "pie":
            ax.pie(ys, labels=xs, autopct="%1.1f%%")
        else:
            ax.bar(xs, ys, color=NORMAL_COLOR, alpha=0.85)
            plt.xticks(rotation=45, ha="right", fontsize=9)

    ax.set_title(title, fontsize=13, fontweight="bold", pad=12)
    fig.tight_layout()

    tmp = tempfile.NamedTemporaryFile(suffix=".png", delete=False, prefix="bqbot_chart_")
    fig.savefig(tmp.name, dpi=150)
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

    def ask(self, question: str) -> AgentResponse:
        """Send a question through the agentic loop and return the final answer."""
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
                    system_instruction=SYSTEM_PROMPT,
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
                            system_instruction=SYSTEM_PROMPT,
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
                return AgentResponse(text=final_text, chart_path=chart_path)

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

                elif name == "plot_results":
                    path = _plot_results(
                        data_json=args["data"],
                        chart_type=args.get("chart_type", "bar"),
                        x_col=args["x_col"],
                        y_col=args["y_col"],
                        title=args.get("title", ""),
                        group_col=args.get("group_col", ""),
                        highlight_values=args.get("highlight_values", ""),
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
