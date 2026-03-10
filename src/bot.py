"""
Discord bot – slash commands + anomaly alert buttons.

Commands:
  /status   – run a manual BQ check and show results

After an analysis, a thread is created where users can ask
follow-up questions about the same table (with conversation history).

Run: python3 bot.py
"""

import asyncio
import logging
import os
import sys

import discord
from discord import app_commands
from discord.ext import tasks

from datetime import datetime

from config import Config
from bq_client import BQClient
from agent import Agent
import jira_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def split_message(text: str, limit: int = 1990) -> list[str]:
    """Split a long string into chunks that fit within Discord's character limit."""
    if len(text) <= limit:
        return [text]
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text)
            break
        split_at = text.rfind("\n", 0, limit)
        if split_at == -1:
            split_at = limit
        chunks.append(text[:split_at])
        text = text[split_at:].lstrip("\n")
    return chunks


def _clean_reason(reason: str) -> str:
    """Deduplicate repeated lines in a reason string and truncate to 300 chars."""
    if not reason:
        return "No reason provided"
    seen: list[str] = []
    for line in reason.splitlines():
        line = line.strip()
        if line and line not in seen:
            seen.append(line)
    text = " | ".join(seen) if seen else "No reason provided"
    return (text[:297] + "…") if len(text) > 300 else text


def _dedup_rows(rows: list[dict]) -> list[dict]:
    """Remove truly identical rows (same table_name AND checked_at). Different days are kept."""
    seen: set[tuple[str, str]] = set()
    result: list[dict] = []
    for r in rows:
        key = (r.get("table_name", ""), str(r.get("checked_at", "")))
        if key not in seen:
            seen.add(key)
            result.append(r)
    return result


def _group_anomalies(rows: list[dict]) -> list[dict]:
    """Group anomaly rows by table_name.

    Returns a list of dicts with keys:
      table_name, reason_lines (list of "reason — date"), dates (list[str]), latest_checked_at (str)
    """
    from collections import OrderedDict
    groups: OrderedDict[str, dict] = OrderedDict()

    for r in rows:
        table = r.get("table_name", "unknown")
        reason = _clean_reason(r.get("reason") or "")
        checked_at = str(r.get("checked_at", "unknown"))
        if table not in groups:
            groups[table] = {"reason_lines": [], "dates": []}
        groups[table]["reason_lines"].append((reason, checked_at[:10]))
        groups[table]["dates"].append(checked_at)

    result = []
    for table, info in groups.items():
        sorted_dates = sorted(info["dates"], reverse=True)
        # Deduplicate reason lines (same reason+date)
        seen = set()
        unique_lines = []
        for reason, date_short in info["reason_lines"]:
            key = (reason, date_short)
            if key not in seen:
                seen.add(key)
                unique_lines.append(f"{reason} — {date_short}")
        result.append({
            "table_name": table,
            "reason_lines": unique_lines,
            "dates": sorted_dates,
            "latest_checked_at": sorted_dates[0],
        })
    return result


# Keys (table_name, str(checked_at)) already alerted this session – avoids re-alerting on every poll
_alerted_keys: set[tuple[str, str]] = set()

# (table_name, checked_at) → (channel_id, message_id) so solve can update the original alert
_alert_messages: dict[tuple[str, str], tuple[int, int]] = {}

# Thread ID → source table name (fully qualified). Used for follow-up questions in threads.
_thread_tables: dict[int, str] = {}

# Thread ID → anomaly reason string. Gives context for what "the anomaly" means.
_thread_reasons: dict[int, str] = {}

# Thread ID → asyncio.Task for auto-close timer
_thread_timers: dict[int, asyncio.Task] = {}

THREAD_IDLE_TIMEOUT = 15 * 60  # 15 minutes in seconds


async def _auto_close_thread(thread_id: int):
    """Wait for THREAD_IDLE_TIMEOUT seconds, then close the thread."""
    try:
        await asyncio.sleep(THREAD_IDLE_TIMEOUT)
        # Still tracked? Close it.
        if thread_id in _thread_tables:
            channel = bot.get_channel(thread_id)
            if channel and isinstance(channel, discord.Thread):
                await channel.send("🔒 This thread has been closed due to 15 minutes of inactivity. Start a new analysis to continue.")
                await channel.edit(archived=True, locked=True)
            _thread_tables.pop(thread_id, None)
            _thread_reasons.pop(thread_id, None)
            _thread_timers.pop(thread_id, None)
    except asyncio.CancelledError:
        pass
    except Exception as e:
        logger.warning("Auto-close thread %d failed: %s", thread_id, e)


def _reset_thread_timer(thread_id: int):
    """Reset (or start) the auto-close timer for a thread."""
    old_task = _thread_timers.get(thread_id)
    if old_task:
        old_task.cancel()
    _thread_timers[thread_id] = asyncio.create_task(_auto_close_thread(thread_id))

# ── BQ + Agent setup ──────────────────────────────────────────────────────────

bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
agent = Agent(config=Config, bq_client=bq)

ALERT_QUERY = """
SELECT table_name, is_valid, reason, checked_at
FROM `{table}`
WHERE is_valid = FALSE
ORDER BY checked_at DESC
LIMIT 50
""".format(table=Config.BQ_TABLE)

# ── Button view for anomaly follow-up ─────────────────────────────────────────

# Tables that live outside BQ_SOURCE_DATASET – map table_name → fully qualified name
_TABLE_OVERRIDES: dict[str, str] = {
    "crash_rate_stats": "lia-project-sandbox-deletable.anomaly_checks_demo.crash_rate_stats",
}


def _resolve_source_table(table_name: str) -> str:
    """Return the fully qualified source table name."""
    if table_name in _TABLE_OVERRIDES:
        return _TABLE_OVERRIDES[table_name]
    return f"{Config.BQ_SOURCE_DATASET}.{table_name}"


class AnalyseView(discord.ui.View):
    """Buttons posted with each anomaly alert – lets users request a deeper analysis.
    Uses persistent custom_ids so buttons survive bot restarts.
    All button logic is handled by on_interaction()."""

    def __init__(self, table: str, reason: str, checked_at: str):
        super().__init__(timeout=None)  # persistent – no timeout
        self.table = table
        self.reason = reason
        self.checked_at = checked_at

        # Build persistent custom_ids encoding the metadata
        self.add_item(discord.ui.Button(
            label="Yes, show analysis", style=discord.ButtonStyle.primary, emoji="📊",
            custom_id=f"analyse:{table}:{checked_at}",
        ))
        self.add_item(discord.ui.Button(
            label="No thanks", style=discord.ButtonStyle.secondary, emoji="❌",
            custom_id=f"dismiss:{table}:{checked_at}",
        ))
        self.add_item(discord.ui.Button(
            label="Mark as solved", style=discord.ButtonStyle.success, emoji="✅",
            custom_id=f"solve:{table}:{checked_at}",
        ))



# (table, case_index_str) → { "reason": str, "checked_ats": list[str] }
# Stores grouped case info for per-case buttons in threads
_case_info: dict[tuple[str, str], dict] = {}


async def _handle_analyse(interaction: discord.Interaction, table: str, reason: str, checked_at: str):
    """Create a thread listing all anomaly cases for this table, grouped by unique reason."""
    source_table = _resolve_source_table(table)

    # Query BQ for all current anomalies on this table
    case_query = (
        f"SELECT table_name, reason, checked_at "
        f"FROM `{Config.BQ_TABLE}` "
        f"WHERE table_name = @table_name AND is_valid = FALSE "
        f"ORDER BY checked_at DESC"
    )
    from google.cloud import bigquery as _bq
    case_params = [_bq.ScalarQueryParameter("table_name", "STRING", table)]
    try:
        case_rows = bq.run_query(case_query, params=case_params)
    except Exception as e:
        logger.error("Failed to query cases: %s", e)
        case_rows = []

    if not case_rows:
        case_rows = [{"table_name": table, "reason": reason, "checked_at": checked_at}]

    # Deduplicate identical rows
    case_rows = _dedup_rows(case_rows)

    # Group rows by cleaned reason text → one case per unique problem
    from collections import OrderedDict
    grouped: OrderedDict[str, list[str]] = OrderedDict()
    for row in case_rows:
        r = _clean_reason(row.get("reason") or "")
        ca = str(row.get("checked_at", ""))
        grouped.setdefault(r, []).append(ca)

    cases = list(grouped.items())  # [(reason, [checked_at, ...]), ...]

    # Build the case overview message
    n = len(cases)
    lines = [f"🔍 **{n} unique anomaly problem{'s' if n > 1 else ''} for `{source_table}`:**\n"]
    for i, (r, dates) in enumerate(cases, 1):
        dates_sorted = sorted(dates)
        date_range = f"{dates_sorted[0][:10]}" if len(dates_sorted) == 1 else f"{dates_sorted[0][:10]} → {dates_sorted[-1][:10]}"
        lines.append(f"**{i}.** {r}\n   📅 {len(dates)} occurrence{'s' if len(dates) > 1 else ''}: {date_range}")
        # Store case info for button lookups — keyed by (table, "1"), (table, "2"), etc.
        _case_info[(table, str(i))] = {"reason": r, "checked_ats": dates}

    lines.append("\nClick a button below to analyse or solve a specific problem.")
    overview_text = "\n".join(lines)

    # Create thread
    try:
        msg = await interaction.followup.send(content=f"Opening analysis thread for `{table}`…", wait=True)
        channel = interaction.channel
        real_msg = await channel.fetch_message(msg.id)
        thread = await real_msg.create_thread(name=f"Analysis: {table}", auto_archive_duration=60)
        _thread_tables[thread.id] = source_table
        _thread_reasons[thread.id] = reason
        _reset_thread_timer(thread.id)

        # Build per-case buttons (max 5 per row in Discord)
        view = discord.ui.View(timeout=None)
        for i in range(1, min(len(cases) + 1, 11)):  # cap at 10
            view.add_item(discord.ui.Button(
                label=f"📊 Problem {i}",
                style=discord.ButtonStyle.primary,
                custom_id=f"case_analyse:{table}:{i}",
                row=0 if i <= 5 else 1,
            ))

        solve_view = discord.ui.View(timeout=None)
        for i in range(1, min(len(cases) + 1, 11)):
            solve_view.add_item(discord.ui.Button(
                label=f"✅ Solve {i}",
                style=discord.ButtonStyle.success,
                custom_id=f"case_solve:{table}:{i}",
                row=0 if i <= 5 else 1,
            ))

        await thread.send(content=overview_text, view=view)
        await thread.send(content="Mark individual problems as solved:", view=solve_view)
        await thread.send(
            f"💬 You can also ask me any question about `{source_table}` here!"
        )
    except Exception as e:
        logger.warning("Could not create analysis thread: %s", e)
        await interaction.followup.send(f"Failed to create thread: {e}")


# ── Bot setup ─────────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


# ── Slash commands ────────────────────────────────────────────────────────────

@tree.command(name="status", description="Run a manual BQ check and show the latest results")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    try:
        rows = bq.run_query(ALERT_QUERY)  # Only anomalies, LIMIT 50 – avoids missing cases mixed with valid rows
    except Exception as e:
        await interaction.followup.send(f"BQ error: {e}")
        return

    if not rows:
        embed = discord.Embed(
            title="✅ All clear",
            description="The anomaly table is empty – no rows found.",
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed)
        return

    anomalies = _dedup_rows([r for r in rows if not r.get("is_valid")])

    if anomalies:
        groups = _group_anomalies(anomalies)
        total = sum(len(g["dates"]) for g in groups)
        await interaction.followup.send(f"⚠️ **{total} anomal{'y' if total == 1 else 'ies'} detected** ({len(groups)} unique) – sending details below…")
        for g in groups:
            reasons_text = "\n".join(f"• {line}" for line in g["reason_lines"])
            n = len(g["dates"])
            title = f"⚠️ Anomaly Detected ({n} occurrence{'s' if n > 1 else ''})"
            embed = discord.Embed(title=title, color=discord.Color.red())
            embed.add_field(name="Table", value=f"`{g['table_name']}`", inline=False)
            label = "Reasons" if len(g["reason_lines"]) > 1 else "Reason"
            embed.add_field(name=label, value=reasons_text, inline=False)
            view = AnalyseView(
                table=g['table_name'],
                reason=reasons_text,
                checked_at=g["latest_checked_at"],
            )
            alert_msg = await interaction.followup.send(embed=embed, view=view, wait=True)
            for d in g["dates"]:
                _alert_messages[(g['table_name'], d)] = (interaction.channel_id, alert_msg.id)
    else:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies found in the latest check.",
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed)


# ── Alert function (called from monitor) ─────────────────────────────────────

async def send_anomaly_alert(channel_id: int, group: dict):
    """Send a grouped anomaly alert embed with analysis buttons."""
    channel = bot.get_channel(channel_id)
    if not channel:
        logger.error("Channel %d not found", channel_id)
        return

    table = group["table_name"]
    dates = group["dates"]
    latest = group["latest_checked_at"]

    reasons_text = "\n".join(f"• {line}" for line in group["reason_lines"])
    n = len(dates)
    title = f"⚠️ Anomaly Detected ({n} occurrence{'s' if n > 1 else ''})"
    embed = discord.Embed(title=title, color=discord.Color.red())
    embed.add_field(name="Table", value=f"`{table}`", inline=False)
    label = "Reasons" if len(group["reason_lines"]) > 1 else "Reason"
    embed.add_field(name=label, value=reasons_text, inline=False)
    view = AnalyseView(table=table, reason=reasons_text, checked_at=latest)
    msg = await channel.send(embed=embed, view=view)
    # Track all dates for this group so solve can find the original message
    for d in dates:
        _alert_messages[(table, d)] = (channel_id, msg.id)


# ── Main ──────────────────────────────────────────────────────────────────────

@tasks.loop(seconds=Config.MONITOR_INTERVAL_SECONDS)
async def monitor_loop():
    """Background task – polls BQ and sends anomaly alerts with buttons."""
    alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID
    if not alert_channel_id:
        logger.warning("DISCORD_ALERT_CHANNEL_ID not set – skipping monitor.")
        return

    try:
        rows = bq.run_query(ALERT_QUERY)
    except Exception as e:
        logger.error("Monitor BQ query failed: %s", e)
        return

    channel = bot.get_channel(int(alert_channel_id))
    if not channel:
        logger.error("Alert channel %s not found", alert_channel_id)
        return

    if rows:
        unique_rows = _dedup_rows(rows)
        new_alerts = []
        for row in unique_rows:
            table = row.get("table_name", "unknown")
            checked_at = str(row.get("checked_at", "unknown"))
            key = (table, checked_at)
            if key not in _alerted_keys:
                _alerted_keys.add(key)
                new_alerts.append(row)

        if new_alerts:
            groups = _group_anomalies(new_alerts)
            total = sum(len(g["dates"]) for g in groups)
            if total > 1:
                await channel.send(f"⚠️ **{total} anomalies detected.**")
            for g in groups:
                await send_anomaly_alert(int(alert_channel_id), g)
        else:
            logger.info("Monitor: no new anomalies (all already alerted).")
    else:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies detected in the latest check.",
            color=discord.Color.green(),
        )
        await channel.send(embed=embed)


MAX_HISTORY_MESSAGES = 10  # How many previous messages to include as context


async def _get_thread_history(thread: discord.Thread, skip_message_id: int = 0) -> str:
    """Fetch recent conversation history from a thread, formatted for the prompt.
    Skips the message with skip_message_id (the latest user message) to avoid duplication."""
    messages = []
    async for msg in thread.history(limit=MAX_HISTORY_MESSAGES + 5):
        if msg.id == skip_message_id:
            continue
        # Skip the initial bot greeting
        if msg.author.bot and "This thread is linked to" in (msg.content or ""):
            continue
        role = "Bot" if msg.author.bot else "User"
        text = msg.content.strip() if msg.content else ""
        has_image = any(a.content_type and a.content_type.startswith("image/") for a in msg.attachments)
        if has_image:
            text = (text + " [attached chart image]") if text else "[attached chart image]"
        if text:
            messages.append(f"{role}: {text}")
        if len(messages) >= MAX_HISTORY_MESSAGES:
            break
    messages.reverse()  # oldest first
    return "\n".join(messages)


@bot.event
async def on_message(message: discord.Message):
    """Handle follow-up questions in anomaly analysis threads."""
    if message.author.bot:
        return
    if not isinstance(message.channel, discord.Thread):
        return
    source_table = _thread_tables.get(message.channel.id)
    if not source_table:
        return

    question = message.content.strip()
    if not question:
        return

    _reset_thread_timer(message.channel.id)

    async with message.channel.typing():
        today = datetime.now().strftime("%Y-%m-%d")
        history = await _get_thread_history(message.channel, skip_message_id=message.id)
        anomaly_reason = _thread_reasons.get(message.channel.id, "")

        prompt = (
            f"Today's date is {today}.\n"
            f"The user is asking about the SOURCE TABLE `{source_table}`.\n"
        )
        if anomaly_reason:
            prompt += (
                f"\nThis thread was created because of an anomaly. "
                f"The anomaly reason was: {anomaly_reason}\n"
                f"When the user says 'the anomaly', 'this anomaly', or 'its occurrence', they mean "
                f"the specific issue described above — query the SOURCE TABLE `{source_table}`, "
                f"NOT the anomaly check table.\n"
            )
        prompt += "\n"
        if history:
            prompt += (
                "Previous conversation (background context only):\n"
                f"{history}\n\n"
            )
        prompt += (
            f">>> ANSWER THIS QUESTION: {question}\n\n"
            f"IMPORTANT RULES:\n"
            f"- ALWAYS query the source table `{source_table}`, never the anomaly check table.\n"
            f"- When the user says 'today' they mean {today}. Use CURRENT_DATE() or DATE('{today}') in SQL.\n"
            f"- If the user says 'all occurrences' or 'alla', do NOT add a time filter — query all data.\n"
            f"- Use the schema of `{source_table}` (via INFORMATION_SCHEMA if needed) to answer.\n"
            f"- Use run_query and plot_results as needed.\n"
            f"- The previous conversation is only for context if the user refers to something said earlier.\n"
            f"- Focus your answer entirely on the latest question above."
        )
        try:
            from agent import THREAD_SYSTEM_PROMPT
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, lambda: agent.ask(prompt, system_prompt=THREAD_SYSTEM_PROMPT))

            if response.chart_path:
                text = response.text or "Here are the results:"
                chunks = split_message(text)
                await message.channel.send(content=chunks[0], file=discord.File(response.chart_path, filename="chart.png"))
                for chunk in chunks[1:]:
                    await message.channel.send(content=chunk)
                try:
                    os.remove(response.chart_path)
                except OSError:
                    pass
            else:
                for chunk in split_message(response.text):
                    await message.channel.send(content=chunk)
        except Exception as e:
            logger.error("Thread follow-up failed: %s", e)
            await message.channel.send(f"Something went wrong: {e}")


@bot.event
async def on_ready():
    await tree.sync()
    logger.info("Bot is ready as %s", bot.user)
    if not monitor_loop.is_running():
        monitor_loop.start()


@bot.event
async def on_interaction(interaction: discord.Interaction):
    """Handle persistent button clicks (slash commands are dispatched internally before this event)."""
    if interaction.type != discord.InteractionType.component:
        return

    custom_id = interaction.data.get("custom_id", "")
    if custom_id:
        await _handle_button(interaction, custom_id)


async def _handle_button(interaction: discord.Interaction, custom_id: str):
    parts = custom_id.split(":", 2)
    if len(parts) < 3:
        return
    action, table, checked_at = parts[0], parts[1], parts[2]

    if action == "analyse":
        reason = ""
        if interaction.message and interaction.message.embeds:
            for field in interaction.message.embeds[0].fields:
                if field.name == "Reason":
                    reason = field.value
                    break
        await interaction.response.defer(thinking=True)
        await _handle_analyse(interaction, table, reason, checked_at)

    elif action == "dismiss":
        await interaction.response.send_message("OK, skipping analysis.", ephemeral=True)

    elif action == "solve":
        reason = ""
        if interaction.message and interaction.message.embeds:
            for field in interaction.message.embeds[0].fields:
                if field.name == "Reason":
                    reason = field.value
                    break
        from google.cloud import bigquery as _bq
        resolved_by = interaction.user.display_name
        resolved_date = datetime.now().strftime("%Y-%m-%d")
        resolve_sql = (
            f"UPDATE `{Config.BQ_TABLE}` "
            "SET is_valid = TRUE, "
            "reason = CONCAT(@resolve_prefix, reason) "
            "WHERE table_name = @table_name "
            "AND is_valid = FALSE"
        )
        resolve_prefix = f"✅ Resolved by {resolved_by} on {resolved_date} | "
        params = [
            _bq.ScalarQueryParameter("resolve_prefix", "STRING", resolve_prefix),
            _bq.ScalarQueryParameter("table_name", "STRING", table),
        ]
        await interaction.response.defer(thinking=True)
        try:
            logger.info("Resolve SQL: %s | params: %s", resolve_sql, params)
            affected = bq.run_update(resolve_sql, params=params)
            logger.info("Resolved anomaly group: %s (%d rows)", table, affected)

            resolved_embed = discord.Embed(title="✅ Anomaly Resolved", color=discord.Color.green())
            resolved_embed.add_field(name="Table", value=f"`{table}`", inline=False)
            resolved_embed.add_field(name="Reason", value=reason, inline=False)
            resolved_embed.add_field(name="Rows resolved", value=str(affected), inline=False)
            resolved_embed.add_field(name="Resolved by", value=interaction.user.display_name, inline=False)

            # Update the message where the button was clicked
            if interaction.message:
                try:
                    disabled_view = discord.ui.View(timeout=None)
                    for comp in (interaction.message.components or []):
                        for child in comp.children:
                            btn = discord.ui.Button(
                                label=child.label, style=child.style, emoji=child.emoji,
                                custom_id=child.custom_id, disabled=True,
                            )
                            disabled_view.add_item(btn)
                    await interaction.message.edit(embed=resolved_embed, view=disabled_view)
                except Exception as e:
                    logger.warning("Could not update clicked message: %s", e)

            # Also update the original alert in the channel (if solved from thread)
            alert_key = (table, checked_at)
            alert_info = _alert_messages.get(alert_key)
            if alert_info:
                try:
                    ch = bot.get_channel(alert_info[0])
                    if ch:
                        orig_msg = await ch.fetch_message(alert_info[1])
                        orig_disabled = discord.ui.View(timeout=None)
                        for comp in (orig_msg.components or []):
                            for child in comp.children:
                                btn = discord.ui.Button(
                                    label=child.label, style=child.style, emoji=child.emoji,
                                    custom_id=child.custom_id, disabled=True,
                                )
                                orig_disabled.add_item(btn)
                        await orig_msg.edit(embed=resolved_embed, view=orig_disabled)
                except Exception as e:
                    logger.warning("Could not update original alert message: %s", e)

            await interaction.followup.send("✅ Anomaly marked as solved.", ephemeral=True)

            # Close any linked analysis thread
            for thread_id, tbl in list(_thread_tables.items()):
                if tbl == _resolve_source_table(table):
                    thread_ch = bot.get_channel(thread_id)
                    if thread_ch and isinstance(thread_ch, discord.Thread):
                        await thread_ch.send("🔒 Anomaly marked as solved — closing thread.")
                        await thread_ch.edit(archived=True, locked=True)
                    _thread_tables.pop(thread_id, None)
                    _thread_reasons.pop(thread_id, None)
                    old_timer = _thread_timers.pop(thread_id, None)
                    if old_timer:
                        old_timer.cancel()
        except Exception as e:
            logger.error("Failed to resolve anomaly: %s", e)
            await interaction.followup.send(f"Failed to resolve: {e}", ephemeral=True)

    elif action == "case_analyse":
        # Per-case analysis: run Gemini for one grouped problem
        # checked_at here is actually the case index ("1", "2", etc.)
        case_idx = checked_at
        info = _case_info.get((table, case_idx))
        if not info:
            await interaction.response.send_message("Case not found — try running a new analysis.", ephemeral=True)
            return
        reason = info["reason"]
        dates = info["checked_ats"]
        latest_date = sorted(dates)[-1]
        source_table = _resolve_source_table(table)
        await interaction.response.defer(thinking=True)

        # Fetch Jira release context near the latest anomaly date
        jira_context = ""
        if Config.JIRA_PROJECT_KEY:
            try:
                anomaly_date = datetime.fromisoformat(latest_date).date()
                releases = jira_client.get_releases_near_date(anomaly_date, Config.JIRA_PROJECT_KEY)
                jira_context = jira_client.format_release_context(releases, anomaly_date)
            except Exception as e:
                logger.warning("Jira context lookup failed: %s", e)

        dates_str = ", ".join(d[:10] for d in sorted(dates))
        today = datetime.now().strftime("%Y-%m-%d")
        prompt = (
            f"Today's date is {today}.\n"
            f"An anomaly was detected in table `{source_table}`.\n"
            f"Reason: {reason}\n"
            f"This problem has occurred on these dates: {dates_str}\n"
            + (f"\n{jira_context}\n" if jira_context else "")
            + "\nFollow the analysis steps in your instructions. "
            f"Use the schema of `{source_table}` and the reason above to decide the best queries and charts. "
            "IMPORTANT: Always query data from TODAY backwards (e.g. WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)), not from the anomaly date. "
            "The chart should show the full picture up to today so we can see the current state. "
            f"When calling plot_results, ALWAYS pass anomaly_date='{sorted(dates)[0][:10]}' so the first occurrence is marked on the chart. "
            "If there are Jira releases listed above, consider whether they may explain the anomaly and mention it in your analysis. "
            "Always end with a structured written analysis including what was found, when it started, any patterns, and a concrete recommendation."
        )

        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, agent.ask, prompt)

            if response.chart_path:
                text = response.text or "Here is the trend chart:"
                chunks = split_message(text)
                await interaction.followup.send(
                    content=chunks[0],
                    file=discord.File(response.chart_path, filename="chart.png"),
                )
                for chunk in chunks[1:]:
                    await interaction.followup.send(content=chunk)
                try:
                    os.remove(response.chart_path)
                except OSError:
                    pass
            else:
                chunks = split_message(response.text)
                await interaction.followup.send(content=chunks[0])
                for chunk in chunks[1:]:
                    await interaction.followup.send(content=chunk)
        except Exception as e:
            logger.error("Case analysis failed: %s", e)
            await interaction.followup.send(f"Analysis failed: {e}")

    elif action == "case_solve":
        # Per-case solve: resolve all rows matching this grouped problem
        case_idx = checked_at
        info = _case_info.get((table, case_idx))
        if not info:
            await interaction.response.send_message("Case not found — try running a new analysis.", ephemeral=True)
            return
        dates = info["checked_ats"]
        from google.cloud import bigquery as _bq
        resolved_by = interaction.user.display_name
        resolved_date = datetime.now().strftime("%Y-%m-%d")
        resolve_prefix = f"✅ Resolved by {resolved_by} on {resolved_date} | "

        # Resolve all rows for this table with matching checked_at dates
        # Build IN clause with parameters
        param_names = [f"ca_{j}" for j in range(len(dates))]
        in_clause = ", ".join(f"@{p}" for p in param_names)
        resolve_sql = (
            f"UPDATE `{Config.BQ_TABLE}` "
            "SET is_valid = TRUE, "
            "reason = CONCAT(@resolve_prefix, reason) "
            "WHERE table_name = @table_name "
            f"AND CAST(checked_at AS STRING) IN ({in_clause}) "
            "AND is_valid = FALSE"
        )
        params = [
            _bq.ScalarQueryParameter("resolve_prefix", "STRING", resolve_prefix),
            _bq.ScalarQueryParameter("table_name", "STRING", table),
        ] + [
            _bq.ScalarQueryParameter(pn, "STRING", d) for pn, d in zip(param_names, dates)
        ]

        await interaction.response.defer(thinking=True)
        try:
            affected = bq.run_update(resolve_sql, params=params)
            logger.info("Resolved grouped case %s #%s (%d rows)", table, case_idx, affected)

            # Disable just this solve button
            if interaction.message:
                try:
                    new_view = discord.ui.View(timeout=None)
                    for comp in (interaction.message.components or []):
                        for child in comp.children:
                            disabled = child.custom_id == custom_id
                            btn = discord.ui.Button(
                                label=child.label if not disabled else f"✅ Solved",
                                style=discord.ButtonStyle.secondary if disabled else child.style,
                                emoji=child.emoji if not disabled else None,
                                custom_id=child.custom_id,
                                disabled=disabled,
                            )
                            new_view.add_item(btn)
                    await interaction.message.edit(view=new_view)
                except Exception as e:
                    logger.warning("Could not update solve buttons: %s", e)

            await interaction.followup.send(
                f"✅ Problem solved ({affected} row{'s' if affected != 1 else ''} updated).",
                ephemeral=True,
            )
        except Exception as e:
            logger.error("Failed to resolve case: %s", e)
            await interaction.followup.send(f"Failed to resolve: {e}", ephemeral=True)


def run():
    token = Config.DISCORD_BOT_TOKEN
    if not token:
        logger.error("DISCORD_BOT_TOKEN is not set in .env")
        sys.exit(1)
    bot.run(token)


if __name__ == "__main__":
    run()
