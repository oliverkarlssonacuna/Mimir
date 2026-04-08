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
from detector import Detector, Anomaly
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


# ── Setup ─────────────────────────────────────────────────────────────────────

bq = BQClient(project_id=Config.GCP_PROJECT_ID, max_rows=Config.MAX_QUERY_ROWS)
steep = SteepClient(api_key=Config.STEEP_API_TOKEN)
detector = Detector(steep=steep, bq=bq)
percent_metric_ids = {m["metric_id"] for m in detector._metric_configs if m.get("display_format") == "percent"}
agent = Agent(config=Config, bq_client=bq, steep_client=steep, percent_metric_ids=percent_metric_ids)


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
        anomalies = await loop.run_in_executor(
            None, lambda: detector.collect_and_check(progress_callback=on_progress)
        )
    except Exception as e:
        await progress_msg.edit(content=f"❌ Error during check: {e}")
        return

    await progress_msg.edit(content=f"✅ Done — checked `{total_metrics}` metrics.")

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
            cur_label = f"Today so far"
            base_label = f"Same day last week ({base_short})" if base_short else "Same day last week"
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

        analysis = f"{anomalies[0].metric_label}: {change_summary}.{release_note} Investigate further to confirm the cause."
        embed.add_field(name="Analysis", value=analysis, inline=False)
    except Exception as e:
        logger.warning("Analysis summary failed: %s", e)

    _pending_anomalies[anomalies[0].metric_id] = anomalies
    view = GroupedAnomalyView(anomalies[0].metric_id, anomalies[0].reference_date)
    await channel.send(embed=embed, view=view)


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

    total_metrics = len(detector._metric_configs)
    progress_msg = await channel.send(f"⏳ Checking metrics... `0/{total_metrics}` {'░' * 20}")

    loop = asyncio.get_running_loop()
    last_update = [0]

    def make_bar(current: int, total: int) -> str:
        filled = int(20 * current / total) if total else 0
        bar = '█' * filled + '░' * (20 - filled)
        pct = int(100 * current / total) if total else 0
        return f"⏳ Checking metrics... `{current}/{total}` `{bar}` {pct}%"

    def on_progress(current: int, total: int, label: str):
        if current - last_update[0] >= 5 or current == total:
            last_update[0] = current
            asyncio.run_coroutine_threadsafe(
                progress_msg.edit(content=make_bar(current, total)), loop
            )

    try:
        anomalies = await loop.run_in_executor(
            None, lambda: detector.check_only(progress_callback=on_progress)
        )
    except Exception as e:
        logger.error("Monitor check failed: %s", e)
        await progress_msg.edit(content=f"❌ Check failed: {e}")
        return

    if not anomalies:
        logger.info("Monitor: no anomalies detected.")
        await progress_msg.edit(content=f"✅ All clear — checked `{total_metrics}` metrics, no anomalies detected.")
        return

    today_str = datetime.now().strftime("%Y-%m-%d")
    new_anomalies = []
    for a in anomalies:
        key = (a.metric_id, a.comparison, today_str)
        if key not in _alerted_keys:
            _alerted_keys.add(key)
            new_anomalies.append(a)

    if not new_anomalies:
        logger.info("Monitor: anomalies exist but already alerted today.")
        return

    grouped = _group_anomalies(new_anomalies)
    for metric_anomalies in grouped.values():
        try:
            await send_grouped_anomaly_alert(channel, metric_anomalies)
        except discord.DiscordServerError as e:
            logger.warning("Discord server error sending alert for %s: %s", metric_anomalies[0].metric_label, e)
        except Exception as e:
            logger.error("Failed to send alert for %s: %s", metric_anomalies[0].metric_label, e)


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
            await message.channel.send(f"Something went wrong: {e}")


async def _start_internal_server():
    """Small HTTP server so the admin web UI can trigger a config reload."""
    async def handle_reload(request: aiohttp_web.Request) -> aiohttp_web.Response:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, detector.reload_configs)
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
    runner = aiohttp_web.AppRunner(internal_app)
    await runner.setup()
    site = aiohttp_web.TCPSite(runner, "0.0.0.0", Config.BOT_INTERNAL_PORT)
    await site.start()
    logger.info("Internal HTTP server listening on port %d", Config.BOT_INTERNAL_PORT)


@bot.event
async def on_ready():
    synced = await tree.sync()
    logger.info("Synced %d commands: %s", len(synced), [c.name for c in synced])
    logger.info("Bot is ready as %s", bot.user)
    # Ensure context notes table exists in BQ
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, bq.ensure_notes_table)
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
            chart_anomaly_date = yesterday_str if ("wow" in triggered_comps or "dod" in triggered_comps) else today
            chart_pace_date = today if "pace" in triggered_comps else ""

            # Only include today's partial data if Pace is triggered (today is the current point)
            # For WoW/DoD-only, stop at yesterday so the anomaly dot is the last visible point
            chart_end_date = today_date if "pace" in triggered_comps else (today_date - _td(days=1))
            days_since_baseline = (chart_end_date - baseline_date_obj).days + 2

            from concurrent.futures import ThreadPoolExecutor as _TPE
            def _fetch_steep():
                from agent import _query_steep_metric
                data = _query_steep_metric(steep, metric_id, days=days_since_baseline)
                # Convert percent metrics
                if metric_id in percent_metric_ids:
                    data = [{**p, "value": round(p["value"] * 100, 4), "unit": "%"} if "value" in p else p for p in data]
                return data

            def _fetch_jira():
                try:
                    releases = jira_client.get_releases_near_date(ref_date_obj, Config.JIRA_PROJECT_KEY)
                    return jira_client.format_release_context(releases, ref_date_obj) or "No Jira releases found near this date."
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
                )
            )
            pre_chart = chart_path if not chart_path.startswith("error") else None

            # Single Gemini call — only analysis text, no tool calls
            prompt = (
                f"Today's date: {today}\n\n"
                f"## Metric: {metric_info['metric_label']} (id: {metric_id})\n"
                f"Direction: {metric_info['direction']}\n"
                f"Anomaly detected on {reference_date}: {triggered_comparisons}\n"
                f"{anomaly_detail}\n\n"
                f"## Daily data ({baseline} to {reference_date}):\n"
                f"{steep_json}\n\n"
                f"## Game milestones (use these to explain peaks, dips, and trends):\n{Config.GAME_MILESTONES}\n\n"
                f"## Jira releases near {reference_date}:\n{jira_context}\n\n"
                f"## Team context notes:\n{context_notes}\n\n"
                f"## Other metrics that moved ≥20% on the same day (same-direction = systemic, opposite = isolated):\n{correlated_metrics}\n\n"
                "## Instructions\n"
                "You are analysing a mobile game analytics metric. A chart is already attached — do NOT generate a chart or call any functions.\n\n"
                "Write a focused analysis of 4-6 sentences. Structure it as follows:\n"
                "1. **What happened** — describe the anomaly quantitatively (e.g. 'DAU dropped 38% vs last week, from 1 420 to 880').\n"
                "2. **Why it happened** — cross-reference the data pattern against the game milestones, Jira releases, and team notes. "
                "Look for peaks after releases, dips after beta close, spikes around launch events. "
                "If a milestone or release closely precedes or coincides with the anomaly, name it explicitly as the likely cause. "
                "If a milestone is far from the anomaly date but the trend since that date is relevant, mention the trend.\n"
                "3. **Correlated metrics** — if other metrics moved similarly, mention 1-2 by name and use that to confirm or rule out a systemic cause. "
                "If no other metrics moved, note that the drop appears isolated to this metric.\n"
                "4. **Broader trend** — briefly describe the overall shape of the data over the shown period "
                "(e.g. growing since beta launch, declining since beta closed, flat since v0.64, volatile).\n"
                "5. **Confidence** — if you're speculating, say so in one short clause (e.g. 'likely related to…', 'possibly caused by…'). "
                "Do NOT hedge with 'it could be many things' or 'investigate further'.\n\n"
                "Be direct and specific. Use numbers. Reference dates explicitly when relevant.\n"
            )

            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, lambda: agent.ask(prompt, tools_enabled=False))

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
            await interaction.followup.send(f"Could not create analysis thread: {e}")

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
