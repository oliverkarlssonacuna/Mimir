"""Discord bot – Steep metric anomaly monitoring.

Monitor loop runs every hour using BQ snapshots (no Steep API calls).
Steep API is used only for /status manual checks and deep analysis threads.

Commands:
  /status   – run a manual check and show all metrics

Run: python bot.py
"""

import asyncio
import logging
import os
import sys

import discord
from discord import app_commands
from discord.ext import tasks
from aiohttp import web as aiohttp_web

from datetime import datetime, timedelta

from config import Config, THRESHOLDS
from bq_client import BQClient
from steep_client import SteepClient
from detector import Detector, Anomaly, FieldAlert
from agent import Agent, THREAD_SYSTEM_PROMPT
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


# (metric_id, comparison, date_str) already alerted this session
_alerted_keys: set[tuple[str, str, str]] = set()

# (monitor_id, value, date_str) already alerted this session
_alerted_field_keys: set[tuple[str, str, str]] = set()

# Guard against on_ready firing multiple times on reconnect
_bot_initialized: bool = False

# Thread ID → metric info for follow-up questions
_thread_metrics: dict[int, dict] = {}

# metric_id → list[Anomaly], for use in deep analysis button
_pending_anomalies: dict[str, list["Anomaly"]] = {}

# Thread ID → asyncio.Task for auto-close timer
_thread_timers: dict[int, asyncio.Task] = {}

THREAD_IDLE_TIMEOUT = 15 * 60  # 15 minutes


async def _auto_close_thread(thread_id: int):
    """Wait then close an idle thread."""
    try:
        await asyncio.sleep(THREAD_IDLE_TIMEOUT)
        if thread_id in _thread_metrics:
            channel = bot.get_channel(thread_id)
            if channel and isinstance(channel, discord.Thread):
                await channel.send("🔒 Thread closed after 15 minutes of inactivity.")
                await channel.edit(archived=True, locked=True)
            _thread_metrics.pop(thread_id, None)
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


def _get_error_channel() -> discord.TextChannel | None:
    """Return the configured debug/error channel, or None if not set."""
    channel_id = Config.DISCORD_ERROR_CHANNEL_ID
    if channel_id:
        return bot.get_channel(int(channel_id))
    return None


def _build_field_alert_embed(fa: FieldAlert) -> discord.Embed:
    """Build a Discord embed for a field value monitor alert."""
    from datetime import datetime as _dt, timedelta as _td
    today = _dt.utcnow().date()
    window_start = (today - _td(days=7)).strftime("%b %-d")
    window_end = (today - _td(days=1)).strftime("%b %-d")
    n = len(fa.new_values)
    plural = "value" if n == 1 else "values"
    table_parts = fa.bq_table.split(".")
    table_short = ".".join(table_parts[-2:]) if len(table_parts) >= 2 else fa.bq_table
    type_tag = f" · {fa.field_type}" if fa.field_type else ""
    desc = (
        f"**Table:** `{table_short}`\n"
        f"**Field:** `{fa.field_name}`\n"
        f"**Window:** {window_start} – {window_end}\n\n"
        f"**{n} new {plural}**\n"
        + "\n".join(f"• `{v}`{type_tag}" for v in fa.new_values[:25])
    )
    if len(fa.new_values) > 25:
        desc += f"\n_…and {len(fa.new_values) - 25} more_"
    embed = discord.Embed(
        title=f"🔔 {fa.label}",
        description=desc,
        color=discord.Color.orange(),
        timestamp=_dt.utcnow(),
    )
    embed.set_footer(text="Mimir — Field Value Monitor")
    return embed


# ── Setup ─────────────────────────────────────────────────────────────────────

bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
steep = SteepClient(api_key=Config.STEEP_API_TOKEN)
detector = Detector(steep=steep, bq=bq)
percent_metric_ids = {m["metric_id"] for m in detector._metric_configs if m.get("display_format") == "percent"}
agent = Agent(config=Config, bq_client=bq, steep_client=steep, percent_metric_ids=percent_metric_ids)


# ── Runtime settings (editable from admin UI, override Config defaults) ───────

_runtime_settings: dict[str, str] = {}

def _load_runtime_settings() -> None:
    """Load settings from BQ and apply to Config where relevant."""
    global _runtime_settings
    _runtime_settings = bq.get_settings()
    baseline = _runtime_settings.get("baseline_start_date", "")
    if baseline:
        Config.BASELINE_START_DATE = baseline
    logger.info("Runtime settings loaded: %s", list(_runtime_settings.keys()))

def _get_setting(key: str, default: str = "") -> str:
    return _runtime_settings.get(key, default)


# ── Alert color/emoji ────────────────────────────────────────────────────────

def _alert_color(anomaly: Anomaly) -> discord.Color:
    return discord.Color.red()


def _alert_emoji(anomaly: Anomaly) -> str:
    return "🚨"


def _comparison_label(comp: str) -> str:
    return {"pace": "Pace (intraday)", "dod": "Day-over-day", "wow": "Week-over-week"}.get(comp, comp)


# ── Button view for anomaly alerts ────────────────────────────────────────────

class GroupedAnomalyView(discord.ui.View):
    """Buttons for a grouped (per-metric) anomaly alert."""

    def __init__(self, metric_id: str, reference_date: str):
        super().__init__(timeout=None)
        self.add_item(discord.ui.Button(
            label="Deep analysis", style=discord.ButtonStyle.primary, emoji="📊",
            custom_id=f"analyse:{metric_id}:{reference_date}",
        ))
        self.add_item(discord.ui.Button(
            label="Handled", style=discord.ButtonStyle.success, emoji="✅",
            custom_id=f"handled:{metric_id}:{reference_date}",
        ))


# ── Slash commands ────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


@tree.command(name="status", description="Run manual check — show all metrics right now")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    total_metrics = len(detector._metric_configs)
    progress_msg = await interaction.followup.send(
        f"⏳ Checking metrics... `0/{total_metrics}` {'░' * 20}",
        wait=True,
    )

    loop = asyncio.get_running_loop()

    def make_progress_bar(current: int, total: int) -> str:
        filled = int(20 * current / total) if total else 0
        bar = '█' * filled + '░' * (20 - filled)
        pct = int(100 * current / total) if total else 0
        return f"⏳ Checking metrics... `{current}/{total}` `{bar}` {pct}%"

    last_update = [0]

    def on_progress(current: int, total: int, label: str):
        # Update every 5 metrics to avoid Discord rate limits on edits
        if current - last_update[0] >= 5 or current == total:
            last_update[0] = current
            text = make_progress_bar(current, total)
            asyncio.run_coroutine_threadsafe(
                progress_msg.edit(content=text), loop
            )

    try:
        anomalies, failed_labels = await loop.run_in_executor(
            None, lambda: detector.collect_and_check(progress_callback=on_progress)
        )
    except Exception as e:
        await progress_msg.edit(content=f"❌ Error during check: {e}")
        return

    failed_count = len(failed_labels)
    checked_count = total_metrics - failed_count
    fail_note = ""
    if failed_labels:
        failed_list = ", ".join(f"`{lbl}` ({err})" for lbl, err in failed_labels)
        fail_note = f" ⚠️ {failed_count} could not be fetched: {failed_list}"
    await progress_msg.edit(content=f"✅ Done — checked `{checked_count}/{total_metrics}` metrics.{fail_note}")

    if not anomalies:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies detected among the monitored metrics.",
            color=discord.Color.green(),
        )
        # Add current values summary
        def _get_current_values():
            result = []
            for m in detector._metric_configs:
                try:
                    val, _ = detector._fetch_today_value(m["metric_id"])
                    if val is not None:
                        result.append(f"**{m['metric_label']}**: {val:,.1f}")
                except Exception:
                    pass
            return result

        loop = asyncio.get_running_loop()
        lines = await loop.run_in_executor(None, _get_current_values)
        if lines:
            embed.add_field(name="Current values (today)", value="\n".join(lines), inline=False)
        await interaction.followup.send(embed=embed)
        return

    grouped = _group_anomalies(anomalies)
    await interaction.followup.send(
        f"⚠️ **{len(grouped)} metric{'s' if len(grouped) > 1 else ''} with anomalies ({len(anomalies)} checks triggered):**"
    )
    for metric_anomalies in grouped.values():
        embed = _build_grouped_embed(metric_anomalies)
        view = GroupedAnomalyView(metric_anomalies[0].metric_id, metric_anomalies[0].reference_date)
        await interaction.followup.send(embed=embed, view=view)


@tree.command(name="admin", description="Open the Mimir admin panel")
async def admin_link(interaction: discord.Interaction):
    view = discord.ui.View()
    view.add_item(discord.ui.Button(
        label="Open Admin Panel",
        url=Config.ADMIN_URL,
        style=discord.ButtonStyle.link,
        emoji="⚙️",
    ))
    await interaction.response.send_message("🔧 Mimir admin panel:", view=view, ephemeral=True)


# ── /notes command group ──────────────────────────────────────────────────────
notes_group = app_commands.Group(name="notes", description="Manage context notes for deep analysis (release dates, events, etc.)")

@notes_group.command(name="add", description="Add a context note — e.g. 'Apr 7: Closed Android beta'")
@app_commands.describe(text="The note to add")
async def notes_add(interaction: discord.Interaction, text: str):
    await interaction.response.defer(ephemeral=True, thinking=True)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: bq.add_note(text, interaction.user.display_name))
    await interaction.followup.send(f"✅ Note added: *{text}*", ephemeral=True)

@notes_group.command(name="view", description="View all current context notes")
async def notes_view(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    loop = asyncio.get_running_loop()
    rows = await loop.run_in_executor(None, bq.get_notes)
    if not rows:
        await interaction.followup.send("📋 No context notes yet. Use `/notes add` to add one.", ephemeral=True)
        return
    lines = [f"• `{r['created_at'][:10]}` **{r['added_by'] or 'unknown'}**: {r['note']}" for r in rows]
    await interaction.followup.send("📋 **Context notes:**\n" + "\n".join(lines), ephemeral=True)

@notes_group.command(name="clear", description="Clear all context notes")
async def notes_clear(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True, thinking=True)
    loop = asyncio.get_running_loop()
    deleted = await loop.run_in_executor(None, bq.clear_notes)
    await interaction.followup.send(f"🗑️ Cleared {deleted} note(s).", ephemeral=True)

tree.add_command(notes_group)


def _group_anomalies(anomalies: list[Anomaly]) -> dict[str, list[Anomaly]]:
    """Group a list of anomalies by metric_id, preserving order."""
    grouped: dict[str, list[Anomaly]] = {}
    for a in anomalies:
        grouped.setdefault(a.metric_id, []).append(a)
    return grouped


def _fmt_date_short(date_str: str) -> str:
    """Format YYYY-MM-DD as 'Mar 15'."""
    try:
        from datetime import datetime as _dt
        return _dt.strptime(date_str, "%Y-%m-%d").strftime("%b %-d")
    except Exception:
        return date_str


def _build_grouped_embed(anomalies: list[Anomaly]) -> discord.Embed:
    """Build a single Discord embed combining all anomalies for one metric."""
    from datetime import datetime as _dt
    label = anomalies[0].metric_label
    is_drop = anomalies[0].change_pct < 0
    dir_emoji = "📉" if is_drop else "📈"

    embed = discord.Embed(
        title=f"🚨 {label}",
        color=discord.Color.red(),
        timestamp=_dt.utcnow(),
    )

    for a in anomalies:
        trend = "📉" if a.change_pct < 0 else "📈"
        base_short = _fmt_date_short(a.baseline_date) if a.baseline_date else ""

        # Always show relative % change (same as the threshold)
        change_str = f"{a.change_pct:+.1%}"

        # Format raw values — percent-format metrics shown as XX.XX%
        if a.display_format == "percent":
            cur_str = f"{a.current_value * 100:.2f}%"
            base_str = f"{a.baseline_value * 100:.2f}%"
        else:
            cur_str = f"{a.current_value:,.1f}"
            base_str = f"{a.baseline_value:,.1f}"

        # Human-readable labels depending on comparison type
        if a.comparison == "pace":
            if a.reference_hour >= 0:
                from zoneinfo import ZoneInfo
                from datetime import timezone as _tz
                _se_zone = ZoneInfo("Europe/Stockholm")
                try:
                    _ref = datetime.fromisoformat(a.reference_date)
                except Exception:
                    _ref = datetime(2000, 6, 1)
                _utc_dt = _ref.replace(hour=a.reference_hour, minute=0, second=0, tzinfo=_tz.utc)
                _se_hour = _utc_dt.astimezone(_se_zone).hour
                hour_str = f" kl {_se_hour:02d}:00"
            else:
                hour_str = ""
            cur_label = f"Today so far{hour_str}"
            base_label = f"Same day last week ({base_short}{hour_str})" if base_short else f"Same day last week{hour_str}"
        elif a.comparison == "dod":
            cur_label = f"Yesterday ({_fmt_date_short(a.reference_date)})"
            base_label = f"Day before ({base_short})" if base_short else "Day before"
        else:  # wow
            cur_label = f"Yesterday ({_fmt_date_short(a.reference_date)})"
            base_label = f"Same day last week ({base_short})" if base_short else "Same day last week"

        field_value = (
            f"{trend} **{change_str}**\n"
            f"{cur_label}: **{cur_str}**\n"
            f"{base_label}: {base_str}"
        )
        embed.add_field(
            name=f"{_comparison_label(a.comparison)}",
            value=field_value,
            inline=True,
        )

    if anomalies[0].steep_url:
        embed.add_field(name="\u200b", value=f"[🔗 View in Steep]({anomalies[0].steep_url})", inline=True)

    embed.set_footer(text="Mimir — Anomaly Monitor")
    return embed


# ── Alert sending (called from monitor) ──────────────────────────────────────

async def send_grouped_anomaly_alert(channel: discord.TextChannel, anomalies: list[Anomaly]):
    """Send a single grouped alert covering all triggered checks for one metric."""
    embed = _build_grouped_embed(anomalies)

    # Build combined analysis summary
    try:
        parts = []
        for a in anomalies:
            comp_label = _comparison_label(a.comparison)
            if a.display_format == "percent":
                cur_fmt = f"{a.current_value * 100:.2f}%"
                base_fmt = f"{a.baseline_value * 100:.2f}%"
            else:
                cur_fmt = f"{a.current_value:,.1f}"
                base_fmt = f"{a.baseline_value:,.1f}"
            parts.append(f"{comp_label}: {a.change_pct:+.1%} ({base_fmt} → {cur_fmt})")

        change_summary = " | ".join(parts)

        loop = asyncio.get_running_loop()
        ref_date = datetime.strptime(anomalies[0].reference_date, "%Y-%m-%d").date()
        releases = await loop.run_in_executor(
            None, lambda: jira_client.get_releases_near_date(ref_date, Config.JIRA_PROJECT_KEY)
        )
        release_note = ""
        if releases:
            release_names = ", ".join(r.get("name", "") for r in releases[:2])
            release_note = f" This coincides with release(s): {release_names}."

        analysis = f"{anomalies[0].metric_label}: {change_summary}.{release_note}"
        embed.add_field(name="Analysis", value=analysis, inline=False)
    except Exception as e:
        logger.warning("Analysis summary failed: %s", e)

    _pending_anomalies[anomalies[0].metric_id] = anomalies
    view = GroupedAnomalyView(anomalies[0].metric_id, anomalies[0].reference_date)
    try:
        await channel.send(embed=embed, view=view)
    except Exception as e:
        logger.error("Failed to send anomaly alert for %s: %s", anomalies[0].metric_label, e)
        err_ch = _get_error_channel()
        if err_ch and err_ch != channel:
            await err_ch.send(f"❌ **Failed to send anomaly alert** for `{anomalies[0].metric_label}`: {e}")


# ── Monitor loop ──────────────────────────────────────────────────────────────

@tasks.loop(seconds=Config.MONITOR_INTERVAL_SECONDS)
async def monitor_loop():
    """Background task – checks BQ snapshots for anomalies every hour."""
    alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID
    if not alert_channel_id:
        logger.warning("DISCORD_ALERT_CHANNEL_ID not set – skipping monitor.")
        return

    channel = bot.get_channel(int(alert_channel_id))
    if not channel:
        logger.error("Alert channel %s not found", alert_channel_id)
        return

    error_channel_id = Config.DISCORD_ERROR_CHANNEL_ID
    error_channel = bot.get_channel(int(error_channel_id)) if error_channel_id else channel
    if not error_channel:
        error_channel = channel

    total_metrics = len(detector._metric_configs)
    loop = asyncio.get_running_loop()

    try:
        anomalies, failed_labels = await loop.run_in_executor(
            None, lambda: detector.check_only()
        )
    except Exception as e:
        logger.error("Monitor check failed: %s", e, exc_info=True)
        await error_channel.send(f"❌ Monitor check failed: {e}")
        return

    unique_failed = len({lbl for lbl, _ in failed_labels})
    checked_count = total_metrics - unique_failed

    if failed_labels:
        now_cest = datetime.utcnow() + timedelta(hours=2)
        # Deduplicate by metric label — keep only the first (root cause) error per metric
        seen: dict[str, str] = {}
        for lbl, err in failed_labels:
            if lbl not in seen:
                seen[lbl] = err
        unique_count = len(seen)
        embed = discord.Embed(
            title="⚠️ Mimir – fetch errors",
            color=discord.Color.orange(),
            timestamp=datetime.utcnow(),
        )
        embed.add_field(name="Time (CEST)", value=now_cest.strftime("%Y-%m-%d %H:%M"), inline=True)
        embed.add_field(name="Coverage", value=f"`{checked_count}/{total_metrics}` metrics checked", inline=True)
        lines = [f"• `{lbl}` — {err}" for lbl, err in seen.items()]
        embed.add_field(name=f"Failed metrics ({unique_count})", value="\n".join(lines), inline=False)
        embed.set_footer(text="Mimir — Error Monitor")
        await error_channel.send(embed=embed)

    # ── Field value monitor checks (once per day at configured hour) ──────
    field_check_hour = int(_get_setting("field_monitor_check_hour", "8"))
    now_utc = datetime.utcnow()
    if now_utc.hour != field_check_hour:
        logger.info("Skipping field monitor check (hour %d UTC, configured for %d UTC).", now_utc.hour, field_check_hour)
        field_alerts = []
    else:
        try:
            field_alerts = await loop.run_in_executor(None, detector.check_field_monitors)
        except Exception as e:
            logger.error("Field monitor check failed: %s", e, exc_info=True)
            await error_channel.send(f"\u274c Field monitor check failed: {e}")
            field_alerts = []

    today_str_fa = datetime.now().strftime("%Y-%m-%d")
    for fa in field_alerts:
        new_unseen = [v for v in fa.new_values if (fa.monitor_id, v, today_str_fa) not in _alerted_field_keys]
        if not new_unseen:
            continue
        for v in new_unseen:
            _alerted_field_keys.add((fa.monitor_id, v, today_str_fa))
            bq.log_alert_key("field", fa.monitor_id, v, today_str_fa)
        fa_with_unseen = FieldAlert(
            monitor_id=fa.monitor_id,
            label=fa.label,
            bq_table=fa.bq_table,
            field_name=fa.field_name,
            new_values=new_unseen,
            today_date=fa.today_date,
            known_value_count=fa.known_value_count,
            field_type=fa.field_type,
        )
        try:
            await channel.send(embed=_build_field_alert_embed(fa_with_unseen))
        except Exception as e:
            logger.error("Failed to send field alert for %s: %s", fa.label, e)
            await error_channel.send(f"❌ **Failed to send field alert** for `{fa.label}`: {e}")

    if not anomalies:
        logger.info("Monitor: no anomalies detected.")
        status_ch = bot.get_channel(int(Config.DISCORD_STATUS_CHANNEL_ID)) if Config.DISCORD_STATUS_CHANNEL_ID else None
        if status_ch:
            await status_ch.send(f"✅ All clear — checked `{checked_count}/{total_metrics}` metrics, no anomalies detected.")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    new_anomalies = []
    for a in anomalies:
        key = (a.metric_id, a.comparison, today_str)
        if key not in _alerted_keys:
            _alerted_keys.add(key)
            bq.log_alert_key("metric", a.metric_id, a.comparison, today_str)
            new_anomalies.append(a)

    if not new_anomalies:
        logger.info("Monitor: anomalies exist but already alerted today.")
        status_ch = bot.get_channel(int(Config.DISCORD_STATUS_CHANNEL_ID)) if Config.DISCORD_STATUS_CHANNEL_ID else None
        if status_ch:
            await status_ch.send(f"ℹ️ No new anomalies — checked `{checked_count}/{total_metrics}` metrics, already alerted on all active issues today.")
        return

    grouped = _group_anomalies(new_anomalies)
    for metric_anomalies in grouped.values():
        try:
            await send_grouped_anomaly_alert(channel, metric_anomalies)
        except discord.DiscordServerError as e:
            logger.warning("Discord server error sending alert for %s: %s", metric_anomalies[0].metric_label, e)
            await error_channel.send(f"⚠️ **Discord server error** sending alert for `{metric_anomalies[0].metric_label}` — will retry next cycle: {e}")
        except Exception as e:
            logger.error("Failed to send alert for %s: %s", metric_anomalies[0].metric_label, e)
            await error_channel.send(f"❌ **Failed to send alert** for `{metric_anomalies[0].metric_label}`: {e}")


MAX_HISTORY_MESSAGES = 10


async def _get_thread_history(thread: discord.Thread, skip_message_id: int = 0) -> str:
    """Fetch recent conversation history from a thread."""
    messages = []
    async for msg in thread.history(limit=MAX_HISTORY_MESSAGES + 5):
        if msg.id == skip_message_id:
            continue
        role = "Bot" if msg.author.bot else "User"
        text = msg.content.strip() if msg.content else ""
        if text:
            messages.append(f"{role}: {text}")
        if len(messages) >= MAX_HISTORY_MESSAGES:
            break
    messages.reverse()
    return "\n".join(messages)


@bot.event
async def on_message(message: discord.Message):
    """Handle follow-up questions in analysis threads."""
    if message.author.bot:
        return
    if not isinstance(message.channel, discord.Thread):
        return
    metric_info = _thread_metrics.get(message.channel.id)
    if not metric_info:
        return

    question = message.content.strip()
    if not question:
        return

    _reset_thread_timer(message.channel.id)

    async with message.channel.typing():
        today = datetime.now().strftime("%Y-%m-%d")
        history = await _get_thread_history(message.channel, skip_message_id=message.id)

        prompt = (
            f"Today's date: {today}\n"
            f"Metric: {metric_info['metric_label']} (id: {metric_info['metric_id']})\n"
            f"Direction: {metric_info['direction']}\n"
        )
        if metric_info.get("anomaly_desc"):
            prompt += f"Anomaly context: {metric_info['anomaly_desc']}\n"
        if history:
            prompt += f"\nPrevious conversation:\n{history}\n\n"
        prompt += f">>> ANSWER THIS QUESTION: {question}"

        try:
            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(
                None, lambda: agent.ask(prompt, system_prompt=THREAD_SYSTEM_PROMPT)
            )

            logger.info(
                "Thread follow-up response: chart_path=%s text_len=%d",
                response.chart_path, len(response.text or ""),
            )

            if response.chart_path:
                if not os.path.exists(response.chart_path):
                    logger.error("Chart file missing before send: %s", response.chart_path)
                    response = type(response)(text=response.text, chart_path=None)

            if response.chart_path:
                text = response.text or "Here is the result:"
                chunks = split_message(text)
                logger.info("Sending chart to thread %d", message.channel.id)
                await message.channel.send(
                    content=chunks[0],
                    file=discord.File(response.chart_path, filename="chart.png"),
                )
                for chunk in chunks[1:]:
                    await message.channel.send(content=chunk)
                try:
                    os.remove(response.chart_path)
                except OSError:
                    pass
            else:
                for chunk in split_message(response.text or "No response generated."):
                    await message.channel.send(content=chunk)
        except Exception as e:
            logger.error("Thread follow-up failed: %s", e, exc_info=True)
            debug_ch = _get_error_channel()
            if debug_ch:
                await debug_ch.send(f"❌ Thread follow-up failed in `{message.channel.name}`: {e}")
            await message.channel.send(f"Something went wrong: {e}")


async def _start_internal_server():
    """Small HTTP server so the admin web UI can trigger a config reload."""
    async def handle_reload(request: aiohttp_web.Request) -> aiohttp_web.Response:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, detector.reload_configs)
        await loop.run_in_executor(None, _load_runtime_settings)
        count = len(detector._metric_configs)
        logger.info("Internal reload triggered via HTTP: %d metrics loaded.", count)
        return aiohttp_web.Response(
            text=f'{{"ok": true, "count": {count}}}',
            content_type="application/json",
        )

    async def handle_reset(request: aiohttp_web.Request) -> aiohttp_web.Response:
        global _alerted_keys
        _alerted_keys.clear()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, detector.reload_configs)
        count = len(detector._metric_configs)
        monitor_loop.restart()
        logger.info("Internal reset triggered: %d metrics, _alerted_keys cleared, monitor restarted.", count)
        return aiohttp_web.Response(
            text=f'{{"ok": true, "count": {count}}}',
            content_type="application/json",
        )

    async def handle_status(request: aiohttp_web.Request) -> aiohttp_web.Response:
        import datetime, json as _json
        interval = Config.MONITOR_INTERVAL_SECONDS
        next_iter = monitor_loop.next_iteration  # datetime | None
        if next_iter is not None:
            now = datetime.datetime.now(datetime.timezone.utc)
            secs = max(0, int((next_iter - now).total_seconds()))
        else:
            secs = None
        return aiohttp_web.Response(
            text=_json.dumps({
                "running": monitor_loop.is_running(),
                "interval_seconds": interval,
                "seconds_until_next_run": secs,
            }),
            content_type="application/json",
        )

    internal_app = aiohttp_web.Application()
    internal_app.router.add_post("/internal/reload", handle_reload)
    internal_app.router.add_post("/internal/reset", handle_reset)
    internal_app.router.add_get("/internal/status", handle_status)

    async def handle_run_field_monitors(request: aiohttp_web.Request) -> aiohttp_web.Response:
        """Trigger an immediate field monitor check, bypassing the time gate."""
        alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID
        channel = bot.get_channel(int(alert_channel_id)) if alert_channel_id else None
        loop = asyncio.get_running_loop()
        try:
            field_alerts = await loop.run_in_executor(None, detector.check_field_monitors)
        except Exception as e:
            logger.error("Manual field monitor check failed: %s", e)
            err_ch = _get_error_channel()
            if err_ch:
                asyncio.create_task(err_ch.send(f"❌ **Manual field monitor check failed**: `{type(e).__name__}: {e}`"))
            return aiohttp_web.Response(
                text=f'{{"ok": false, "error": "{e}"}}',
                content_type="application/json",
            )
        sent = 0
        for fa in field_alerts:
            if not channel:
                continue
            # Debug trigger: ignore dedup, always send all detected values
            asyncio.create_task(channel.send(embed=_build_field_alert_embed(fa)))
            sent += 1
        logger.info("Manual field monitor check: %d alerts sent.", sent)
        return aiohttp_web.Response(
            text=f'{{"ok": true, "alerts_sent": {sent}}}',
            content_type="application/json",
        )

    internal_app.router.add_post("/internal/run-field-monitors", handle_run_field_monitors)

    async def handle_run_monitor(request: aiohttp_web.Request) -> aiohttp_web.Response:
        """Trigger a full metrics anomaly check, bypassing dedup — for debug/testing."""
        alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID
        channel = bot.get_channel(int(alert_channel_id)) if alert_channel_id else None
        loop = asyncio.get_running_loop()
        try:
            anomalies, failed_labels = await loop.run_in_executor(
                None, lambda: detector.collect_and_check(force_pace=True)
            )
        except Exception as e:
            logger.error("Manual monitor check failed: %s", e)
            err_ch = _get_error_channel()
            if err_ch:
                asyncio.create_task(err_ch.send(f"❌ **Manual monitor check failed**: `{type(e).__name__}: {e}`\nCheck Cloud Run logs for full traceback."))
            return aiohttp_web.Response(
                text=f'{{"ok": false, "error": "{e}"}}',
                content_type="application/json",
            )
        if failed_labels:
            err_ch = _get_error_channel()
            if err_ch:
                unique_failed = {lbl: err for lbl, err in failed_labels}
                lines = "\n".join(f"• `{lbl}` — {err}" for lbl, err in unique_failed.items())
                asyncio.create_task(err_ch.send(f"⚠️ **Manual monitor — {len(unique_failed)} metric(s) failed to fetch:**\n{lines}"))
        if not anomalies or not channel:
            return aiohttp_web.Response(
                text=f'{{"ok": true, "alerts_sent": 0, "anomalies": 0}}',
                content_type="application/json",
            )
        grouped = _group_anomalies(anomalies)
        sent = 0
        for metric_anomalies in grouped.values():
            try:
                asyncio.create_task(send_grouped_anomaly_alert(channel, metric_anomalies))
                sent += 1
            except Exception as e:
                logger.error("Manual monitor: failed to send for %s: %s", metric_anomalies[0].metric_label, e)
                err_ch = _get_error_channel()
                if err_ch:
                    asyncio.create_task(err_ch.send(f"❌ **Manual monitor — failed to send alert** for `{metric_anomalies[0].metric_label}`: {e}"))
        logger.info("Manual monitor check: %d anomaly groups sent.", sent)
        return aiohttp_web.Response(
            text=f'{{"ok": true, "alerts_sent": {sent}, "anomalies": {len(anomalies)}}}',
            content_type="application/json",
        )

    internal_app.router.add_post("/internal/run-monitor", handle_run_monitor)
    runner = aiohttp_web.AppRunner(internal_app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", Config.BOT_INTERNAL_PORT)
    await site.start()
    logger.info("Internal HTTP server listening on port %d", Config.BOT_INTERNAL_PORT)


@bot.event
async def on_ready():
    global _bot_initialized
    if _bot_initialized:
        logger.info("on_ready called again (reconnect) — skipping re-init.")
        return
    _bot_initialized = True
    synced = await tree.sync()
    logger.info("Synced %d commands: %s", len(synced), [c.name for c in synced])
    logger.info("Bot is ready as %s", bot.user)
    loop = asyncio.get_event_loop()
    _init_errors: list[str] = []
    for _fn, _label in [
        (bq.ensure_notes_table, "notes table"),
        (bq.ensure_field_monitors_table, "field monitors table"),
        (bq.ensure_settings_table, "settings table"),
        (bq.ensure_alert_log_table, "alert log table"),
        (_load_runtime_settings, "runtime settings"),
    ]:
        try:
            await loop.run_in_executor(None, _fn)
        except Exception as _e:
            logger.error("on_ready: failed to initialise %s: %s", _label, _e)
            _init_errors.append(f"• {_label}: `{_e}`")
    if _init_errors:
        err_ch = _get_error_channel()
        if err_ch:
            await err_ch.send(f"⚠️ **Bot startup — {len(_init_errors)} BQ init step(s) failed:**\n" + "\n".join(_init_errors))
    # Restore today's alerted keys from BQ so restarts don't resend alerts
    today_str = datetime.now().strftime("%Y-%m-%d")
    try:
        rows = await loop.run_in_executor(None, lambda: bq.load_today_alert_keys(today_str))
    except Exception as _e:
        logger.error("on_ready: failed to restore alert keys: %s", _e)
        rows = []
    for alert_type, k1, k2, k3 in rows:
        if alert_type == "metric":
            _alerted_keys.add((k1, k2, k3))
        elif alert_type == "field":
            _alerted_field_keys.add((k1, k2, k3))
    logger.info("Restored %d metric + %d field alert keys from BQ.", len(_alerted_keys), len(_alerted_field_keys))
    await _start_internal_server()
    if not monitor_loop.is_running():
        monitor_loop.start()


@bot.event
async def on_interaction(interaction: discord.Interaction):
    """Handle persistent button clicks."""
    if interaction.type != discord.InteractionType.component:
        return
    custom_id = interaction.data.get("custom_id", "")
    if custom_id:
        await _handle_button(interaction, custom_id)


async def _handle_button(interaction: discord.Interaction, custom_id: str):
    # analyse custom_id format: "analyse:metric_id:reference_date"
    # handled custom_id format:  "handled:metric_id:reference_date"
    parts = custom_id.split(":", 2)
    if len(parts) < 2:
        return
    action = parts[0]
    metric_id = parts[1] if len(parts) > 1 else ""
    reference_date = parts[2] if len(parts) > 2 else datetime.now().strftime("%Y-%m-%d")

    # Find metric info
    metric_info = next((m for m in detector._metric_configs if m["metric_id"] == metric_id), None)

    if action == "analyse" and metric_info:
        # Guard against double-acknowledge (e.g. user double-clicked)
        if interaction.response.is_done():
            return
        await interaction.response.defer(thinking=True)

        # Disable buttons and mark as "analysing" on the original message
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
                embed = interaction.message.embeds[0] if interaction.message.embeds else None
                if embed:
                    embed.set_footer(text=f"\ud83d\udd0d Analysed by {interaction.user.display_name}")
                    embed.timestamp = datetime.utcnow()
                    await interaction.message.edit(embed=embed, view=disabled_view)
                else:
                    await interaction.message.edit(view=disabled_view)
            except Exception as e:
                logger.warning("Could not disable buttons: %s", e)

        # Create analysis thread
        try:
            steep_url = metric_info.get("steep_url")
            msg = await interaction.followup.send(content=f"📊 Analysing **{metric_info['metric_label']}**…", wait=True)
            channel = interaction.channel
            real_msg = await channel.fetch_message(msg.id)
            thread = await real_msg.create_thread(
                name=f"Analysis: {metric_info['metric_label']}", auto_archive_duration=60
            )

            # Build combined context from all pending anomalies for this metric
            saved_anomalies = _pending_anomalies.get(metric_id, [])
            anomaly_parts = []
            ref_dt = datetime.strptime(reference_date, "%Y-%m-%d").date()
            baseline_date = ""    # WoW baseline — only set if WoW actually triggered
            baseline_date_2 = ""  # DoD baseline — only set if DoD actually triggered
            for sa in saved_anomalies:
                comp_label = _comparison_label(sa.comparison)
                if sa.display_format == "percent":
                    cur_fmt = f"{sa.current_value * 100:.2f}%"
                    base_fmt = f"{sa.baseline_value * 100:.2f}%"
                else:
                    cur_fmt = f"{sa.current_value:,.1f}"
                    base_fmt = f"{sa.baseline_value:,.1f}"
                anomaly_parts.append(f"{comp_label}: {sa.change_pct:+.1%} (baseline {base_fmt} → current {cur_fmt})")
                if sa.baseline_date:
                    if sa.comparison in ("wow", "pace"):
                        baseline_date = sa.baseline_date
                    elif sa.comparison == "dod":
                        baseline_date_2 = sa.baseline_date
                    else:
                        baseline_date = sa.baseline_date

            anomaly_detail = " | ".join(anomaly_parts) if anomaly_parts else "anomaly detected"
            triggered_comparisons = ", ".join(_comparison_label(sa.comparison) for sa in saved_anomalies) if saved_anomalies else "unknown"

            _thread_metrics[thread.id] = {
                "metric_id": metric_id,
                "metric_label": metric_info["metric_label"],
                "direction": metric_info["direction"],
                "anomaly_desc": anomaly_detail,
            }
            _reset_thread_timer(thread.id)

            # Pre-fetch data in parallel to reduce Gemini round-trips
            today = datetime.now().strftime("%Y-%m-%d")
            baseline = Config.BASELINE_START_DATE
            today_date = datetime.strptime(today, "%Y-%m-%d").date()
            baseline_date_obj = datetime.strptime(baseline, "%Y-%m-%d").date()

            # Determine correct anomaly date for each triggered comparison type
            from datetime import timedelta as _td
            triggered_comps = {sa.comparison for sa in saved_anomalies}
            yesterday_str = (today_date - _td(days=1)).isoformat()
            # WoW/DoD reference = yesterday (completed day); Pace reference = today
            # For pace-only alerts, don't set anomaly_date — the orange pace dot is sufficient
            chart_anomaly_date = yesterday_str if ("wow" in triggered_comps or "dod" in triggered_comps) else ""
            chart_pace_date = today if "pace" in triggered_comps else ""

            # Only include today's partial data if Pace is triggered (today is the current point)
            # For WoW/DoD-only, stop at yesterday so the anomaly dot is the last visible point
            chart_end_date = today_date if "pace" in triggered_comps else (today_date - _td(days=1))

            # Chart window sized to the comparison type — enough context to see the pattern,
            # but not so much that a historical spike (e.g. closed beta in March) crushes the Y-axis.
            # Rule: 3 full weeks for WoW/Pace (weekly rhythm visible), 2 weeks for DoD-only.
            if "wow" in triggered_comps or "pace" in triggered_comps:
                chart_days = 21  # 3 weeks: see 3x the weekly cycle
            else:
                chart_days = 14  # DoD-only: 2 weeks of daily trend is plenty

            chart_start_date = chart_end_date - _td(days=chart_days - 1)
            # Never go before the baseline start date (data before this is unreliable)
            chart_start_date = max(chart_start_date, baseline_date_obj)
            days_since_baseline = (chart_end_date - chart_start_date).days + 1

            from concurrent.futures import ThreadPoolExecutor as _TPE
            def _fetch_steep():
                """Fetch chart data from BQ daily values table (fast ~2s).
                Falls back to Steep API only if BQ has no data for this metric."""
                try:
                    rows = bq.fetch_daily_values(
                        metric_id=metric_id,
                        from_date=chart_start_date.isoformat(),
                        to_date=chart_end_date.isoformat(),
                    )
                    if rows:
                        data = [{"date": r["date"], "value": r["value"]} for r in rows]
                        if metric_id in percent_metric_ids:
                            data = [{**p, "value": round(p["value"] * 100, 4), "unit": "%"} for p in data]
                        logger.info("Chart data for %s: fetched %d rows from BQ daily values.", metric_id, len(data))
                        return data
                except Exception as e:
                    logger.warning("BQ daily values fetch failed for %s, falling back to Steep: %s", metric_id, e)

                # Fallback: Steep API (slow but always available)
                import time as _time
                from agent import _query_steep_metric
                last_exc = None
                for attempt in range(2):  # max 2 attempts — we already tried BQ
                    try:
                        data = _query_steep_metric(steep, metric_id, days=days_since_baseline)
                        if metric_id in percent_metric_ids:
                            data = [{**p, "value": round(p["value"] * 100, 4), "unit": "%"} if "value" in p else p for p in data]
                        logger.info("Chart data for %s: fetched from Steep API (BQ fallback).", metric_id)
                        return data
                    except Exception as e:
                        last_exc = e
                        logger.warning("Steep fetch attempt %d failed: %s", attempt + 1, e)
                        if attempt < 1:
                            _time.sleep(2)
                logger.error("Chart data fetch failed for %s after BQ + Steep attempts: %s", metric_id, last_exc)
                return []

            def _fetch_jira():
                try:
                    _ref_dt = datetime.strptime(reference_date, "%Y-%m-%d").date()
                    releases = jira_client.get_releases_near_date(_ref_dt, Config.JIRA_PROJECT_KEY)
                    return jira_client.format_release_context(releases, _ref_dt) or "No Jira releases found near this date."
                except Exception as e:
                    logger.error("Jira lookup failed: %s", e)
                    return f"Jira lookup failed: {e}"

            def _fetch_notes():
                try:
                    rows = bq.get_notes()
                    return "\n".join(f"- [{r['created_at'][:10]}] {r['note']}" for r in rows) if rows else "None"
                except Exception as e:
                    logger.warning("Could not fetch context notes: %s", e)
                    return "None"

            def _fetch_correlations():
                try:
                    corr_baseline = baseline_date or (today_date - _td(days=7)).isoformat()
                    corr_anomaly  = chart_anomaly_date
                    # Determine direction from the first saved anomaly's change_pct
                    direction = 1 if (saved_anomalies and saved_anomalies[0].change_pct >= 0) else -1
                    # Proportional threshold: correlated metric must move ≥50% of main metric's move,
                    # with a floor of 30% to avoid noise on modest anomalies
                    main_pct = abs(saved_anomalies[0].change_pct * 100) if saved_anomalies else 50.0
                    min_pct = max(30.0, main_pct * 0.5)
                    rows = bq.get_correlated_metrics(
                        exclude_metric_id=metric_id,
                        baseline_date=corr_baseline,
                        anomaly_date=corr_anomaly,
                        anomaly_direction=direction,
                        min_pct=min_pct,
                        top_n=3,
                    )
                    if not rows:
                        return "None"
                    lines = []
                    for r in rows:
                        pct = r["pct_change"]
                        arrow = "▲" if pct > 0 else "▼"
                        lines.append(
                            f"- {r['metric_label']}: {arrow} {abs(pct):.1f}% "
                            f"({r['baseline_val']:,.1f} → {r['anomaly_val']:,.1f})"
                        )
                    return "\n".join(lines)
                except Exception as e:
                    logger.warning("Correlation fetch failed: %s", e)
                    return "None"

            with _TPE(max_workers=4) as pre_exec:
                steep_future = pre_exec.submit(_fetch_steep)
                jira_future = pre_exec.submit(_fetch_jira)
                notes_future = pre_exec.submit(_fetch_notes)
                corr_future  = pre_exec.submit(_fetch_correlations)
                steep_data = steep_future.result()
                jira_context = jira_future.result()
                context_notes = notes_future.result()
                correlated_metrics = corr_future.result()

            import json as _json
            steep_json = _json.dumps(steep_data)

            # Extract BQ ground-truth dot values from saved_anomalies
            _chart_anomaly_val = None
            _chart_baseline_val = None
            _chart_baseline_val_2 = None
            _chart_pace_val = None
            for sa in saved_anomalies:
                if sa.comparison in ("wow", "dod") and _chart_anomaly_val is None:
                    _chart_anomaly_val = sa.current_value
                if sa.comparison in ("wow", "pace") and _chart_baseline_val is None:
                    _chart_baseline_val = sa.baseline_value
                if sa.comparison == "dod" and _chart_baseline_val_2 is None:
                    _chart_baseline_val_2 = sa.baseline_value
                if sa.comparison == "pace":
                    _chart_pace_val = sa.current_value

            # Pre-render chart in thread executor — savefig blocks event loop if run inline
            from agent import _plot_results
            import asyncio as _asyncio, functools as _functools
            chart_path = await _asyncio.get_event_loop().run_in_executor(
                None,
                _functools.partial(
                    _plot_results,
                    data_json=steep_json,
                    chart_type="line",
                    x_col="date",
                    y_col="value",
                    title=metric_info["metric_label"],
                    anomaly_date=chart_anomaly_date,
                    baseline_date=baseline_date or "",
                    baseline_date_2=baseline_date_2 or "",
                    pace_date=chart_pace_date,
                    anomaly_value=_chart_anomaly_val,
                    baseline_value=_chart_baseline_val,
                    baseline_value_2=_chart_baseline_val_2,
                    pace_value=_chart_pace_val,
                )
            )
            pre_chart = chart_path if not chart_path.startswith("error") else None

            # Single Gemini call — only analysis text, no tool calls
            # Build ground-truth trigger values from saved_anomalies (these are the
            # EXACT values that triggered the alert — use these, not Steep API values)
            trigger_lines = []
            for sa in saved_anomalies:
                comp_label = _comparison_label(sa.comparison)
                if sa.display_format == "percent":
                    cur_fmt  = f"{sa.current_value * 100:.2f}%"
                    base_fmt = f"{sa.baseline_value * 100:.2f}%"
                else:
                    cur_fmt  = f"{sa.current_value:,.1f}"
                    base_fmt = f"{sa.baseline_value:,.1f}"
                trigger_lines.append(
                    f"- {comp_label}: {base_fmt} → {cur_fmt} ({sa.change_pct:+.1%})"
                )
            trigger_values_block = "\n".join(trigger_lines)

            prompt = (
                f"Today's date: {today}\n\n"
                f"## Metric: {metric_info['metric_label']} (id: {metric_id})\n"
                f"Direction: {metric_info['direction']}\n\n"
                f"## TRIGGER VALUES (ground truth — use THESE exact numbers in your analysis, not values from the daily data):\n"
                f"{trigger_values_block}\n\n"
                f"## Daily data ({baseline} to {reference_date}) — use for trend/context only:\n"
                f"{steep_json}\n\n"
                f"## Game milestones:\n{_get_setting('game_milestones', Config.GAME_MILESTONES)}\n\n"
                f"## Jira releases near {reference_date}:\n{jira_context}\n\n"
                f"## Team context notes:\n{context_notes}\n\n"
                f"## Correlated metrics (same direction, same day):\n{correlated_metrics}\n\n"
                "## Output format\n"
                "You are analysing a mobile game analytics metric for a Discord channel. "
                "A chart is already attached — do NOT generate a chart or call any functions.\n\n"
                "Use this EXACT structure with Discord markdown. Each section is a bold label followed by 1-2 sentences max:\n\n"
                "**📉 What happened**\n"
                "<One sentence: metric name, date, exact numbers, % change vs which comparison (WoW/DoD/Pace).>\n\n"
                "**🔍 Likely cause**\n"
                "<One sentence: name the most probable cause explicitly — beta end, release, known event, or data pattern. "
                "Cross-reference milestones, Jira releases, and team notes. No hedging like 'could be many things'.>\n\n"
                "**🔗 Supporting signals**\n"
                "<If correlated metrics exist: list them as bullets (• Metric: ▲/▼ X%) and state whether this confirms a systemic or isolated issue. "
                "If none: write '• No other metrics moved significantly — likely isolated to this metric.'>\n\n"
                "**📈 Trend**\n"
                "<One sentence: overall data shape since baseline (e.g. 'Rising through beta, sharp drop after Mar 30 close, now stabilising near zero').>\n\n"
                "Rules:\n"
                "- Use real numbers everywhere. No vague statements.\n"
                "- Reference specific dates (e.g. 'Mar 30 beta close') not just 'recently'.\n"
                "- If speculating, use 'likely' or 'possibly' — once, not repeatedly.\n"
                "- NEVER say 'unknown issue' or 'unclear cause' — always commit to the single most plausible explanation based on milestones, correlated metrics, and data shape.\n"
                "- Do NOT add a summary or closing sentence. Stop after **📈 Trend**.\n"
                "- Do NOT say 'investigate further'.\n"
            )

            loop = asyncio.get_running_loop()
            from concurrent.futures import ThreadPoolExecutor as _TPE2
            with _TPE2(max_workers=1) as gemini_exec:
                response = await loop.run_in_executor(gemini_exec, lambda: agent.ask(prompt, tools_enabled=False))

            text = response.text or "Here is the analysis:"
            # Use pre-rendered chart, fall back to agent chart if pre-render failed
            final_chart = pre_chart or response.chart_path
            if final_chart:
                chunks = split_message(text)
                await thread.send(
                    content=chunks[0],
                    file=discord.File(final_chart, filename="chart.png"),
                )
                for chunk in chunks[1:]:
                    await thread.send(content=chunk)
                for p in (pre_chart, response.chart_path):
                    if p:
                        try:
                            os.remove(p)
                        except OSError:
                            pass
            else:
                for chunk in split_message(response.text):
                    await thread.send(content=chunk)

            if steep_url:
                await thread.send(f"🔗 [View metric in Steep]({steep_url})")

        except Exception as e:
            logger.error("Analysis thread failed: %s", e)
            debug_ch = _get_error_channel()
            if debug_ch:
                await debug_ch.send(f"❌ Analysis thread failed for `{metric_info.get('metric_label', metric_id)}`: {e}")
            await interaction.followup.send("Could not create analysis thread — see debug channel for details.", ephemeral=True)

    elif action == "handled":
        handled_by = interaction.user.display_name
        await interaction.response.defer()

        # Disable buttons on the message
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

                # Update embed to show resolved
                embed = interaction.message.embeds[0] if interaction.message.embeds else None
                if embed:
                    embed.color = discord.Color.green()
                    embed.title = f"\u2705 Handled: {embed.title.split(': ', 1)[-1] if ': ' in embed.title else embed.title}"
                    embed.add_field(name="Handled by", value=handled_by, inline=True)
                    await interaction.message.edit(embed=embed, view=disabled_view)
                else:
                    await interaction.message.edit(view=disabled_view)
            except Exception as e:
                logger.warning("Could not update message: %s", e)

        await interaction.followup.send(
            f"\u2705 Marked as handled by {handled_by}.", ephemeral=True
        )


def run():
    token = Config.DISCORD_BOT_TOKEN
    if not token:
        logger.error("DISCORD_BOT_TOKEN is not set in .env")
        sys.exit(1)
    bot.run(token)


if __name__ == "__main__":
    run()
