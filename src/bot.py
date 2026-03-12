"""
Discord bot – Steep metric anomaly monitoring.

Polls Steep every 4 hours, saves snapshots to BQ,
detects anomalies, and sends alerts to Discord.

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

from datetime import datetime

from config import Config, MONITORED_METRICS
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
agent = Agent(config=Config, bq_client=bq, steep_client=steep)


# ── Severity helpers ──────────────────────────────────────────────────────────

def _severity_color(anomaly: Anomaly) -> discord.Color:
    if anomaly.severity == "critical":
        return discord.Color.red()
    return discord.Color.orange()


def _severity_emoji(anomaly: Anomaly) -> str:
    return "🚨" if anomaly.severity == "critical" else "⚠️"


def _comparison_label(comp: str) -> str:
    return {"pace": "Pace (intraday)", "dod": "Day-over-day", "wow": "Week-over-week"}.get(comp, comp)


# ── Button view for anomaly alerts ────────────────────────────────────────────

class AnomalyView(discord.ui.View):
    """Buttons posted with each anomaly alert."""

    def __init__(self, anomaly: Anomaly):
        super().__init__(timeout=None)
        key = f"{anomaly.metric_id}:{anomaly.comparison}"
        self.add_item(discord.ui.Button(
            label="Deep analysis", style=discord.ButtonStyle.primary, emoji="📊",
            custom_id=f"analyse:{key}",
        ))
        self.add_item(discord.ui.Button(
            label="Handled", style=discord.ButtonStyle.success, emoji="✅",
            custom_id=f"handled:{key}",
        ))


# ── Slash commands ────────────────────────────────────────────────────────────

intents = discord.Intents.default()
intents.message_content = True

bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)


@tree.command(name="status", description="Run manual check — show all metrics right now")
async def status(interaction: discord.Interaction):
    await interaction.response.defer(thinking=True)

    try:
        anomalies = await asyncio.get_running_loop().run_in_executor(
            None, detector.collect_and_check
        )
    except Exception as e:
        await interaction.followup.send(f"Error during check: {e}")
        return

    if not anomalies:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies detected among the monitored metrics.",
            color=discord.Color.green(),
        )
        # Add current values summary
        lines = []
        for m in MONITORED_METRICS:
            try:
                val, _ = detector._fetch_today_value(m["id"])
                if val is not None:
                    lines.append(f"**{m['label']}**: {val:,.1f}")
            except Exception:
                pass
        if lines:
            embed.add_field(name="Current values (today)", value="\n".join(lines), inline=False)
        await interaction.followup.send(embed=embed)
        return

    await interaction.followup.send(
        f"⚠️ **{len(anomalies)} anomal{'ies' if len(anomalies) > 1 else 'y'} detected:**"
    )
    for anomaly in anomalies:
        embed = _build_anomaly_embed(anomaly)
        view = AnomalyView(anomaly)
        await interaction.followup.send(embed=embed, view=view)


def _build_anomaly_embed(anomaly: Anomaly) -> discord.Embed:
    """Build a Discord embed for an anomaly."""
    emoji = _severity_emoji(anomaly)
    title = f"{emoji} {anomaly.severity.upper()}: {anomaly.metric_label}"

    embed = discord.Embed(title=title, color=_severity_color(anomaly))
    embed.add_field(
        name="Change",
        value=f"`{anomaly.change_pct:+.1%}`",
        inline=True,
    )
    embed.add_field(
        name="Comparison",
        value=_comparison_label(anomaly.comparison),
        inline=True,
    )
    embed.add_field(
        name="Current → Baseline",
        value=f"{anomaly.current_value:,.1f} → {anomaly.baseline_value:,.1f}",
        inline=False,
    )
    return embed


# ── Alert sending (called from monitor) ──────────────────────────────────────

async def send_anomaly_alert(channel: discord.TextChannel, anomaly: Anomaly):
    """Send an anomaly alert with Gemini summary."""
    embed = _build_anomaly_embed(anomaly)

    # Quick Gemini summary
    try:
        today = datetime.now().strftime("%Y-%m-%d")
        prompt = (
            f"Today's date: {today}\n"
            f"An anomaly has been detected:\n"
            f"Metric: {anomaly.metric_label} (id: {anomaly.metric_id})\n"
            f"Change: {anomaly.change_pct:+.1%} ({_comparison_label(anomaly.comparison)})\n"
            f"Current: {anomaly.current_value:,.1f}, Baseline: {anomaly.baseline_value:,.1f}\n"
            f"Direction: {anomaly.direction}\n\n"
            "Give a brief summary (max 3 sentences) of what might be causing this and a recommendation. "
            "Fetch the last 7 days of data with query_steep_metric for context."
        )
        loop = asyncio.get_running_loop()
        response = await loop.run_in_executor(None, agent.ask, prompt)
        if response.text:
            embed.add_field(name="Analysis", value=response.text[:1024], inline=False)
    except Exception as e:
        logger.warning("Gemini summary failed: %s", e)

    view = AnomalyView(anomaly)
    await channel.send(embed=embed, view=view)


# ── Monitor loop ──────────────────────────────────────────────────────────────

@tasks.loop(seconds=Config.MONITOR_INTERVAL_SECONDS)
async def monitor_loop():
    """Background task – polls Steep, saves snapshots, checks for anomalies."""
    alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID
    if not alert_channel_id:
        logger.warning("DISCORD_ALERT_CHANNEL_ID not set – skipping monitor.")
        return

    channel = bot.get_channel(int(alert_channel_id))
    if not channel:
        logger.error("Alert channel %s not found", alert_channel_id)
        return

    try:
        loop = asyncio.get_running_loop()
        anomalies = await loop.run_in_executor(None, detector.collect_and_check)
    except Exception as e:
        logger.error("Monitor check failed: %s", e)
        return

    if not anomalies:
        logger.info("Monitor: no anomalies detected.")
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

    for anomaly in new_anomalies:
        try:
            await send_anomaly_alert(channel, anomaly)
        except discord.DiscordServerError as e:
            logger.warning("Discord server error sending alert for %s: %s", anomaly.metric_label, e)
        except Exception as e:
            logger.error("Failed to send alert for %s: %s", anomaly.metric_label, e)


MAX_HISTORY_MESSAGES = 10


async def _get_thread_history(thread: discord.Thread, skip_message_id: int = 0) -> str:
    """Fetch recent conversation history from a thread."""
    messages = []
    async for msg in thread.history(limit=MAX_HISTORY_MESSAGES + 5):
        if msg.id == skip_message_id:
            continue
        role = "Bot" if msg.author.bot else "User"
        text = msg.content.strip() if msg.content else ""
        has_image = any(a.content_type and a.content_type.startswith("image/") for a in msg.attachments)
        if has_image:
            text = (text + " [bifogad graf]") if text else "[bifogad graf]"
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
            f"Metric: {metric_info['label']} (id: {metric_info['metric_id']})\n"
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

            if response.chart_path:
                text = response.text or "Here is the result:"
                chunks = split_message(text)
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
    """Handle persistent button clicks."""
    if interaction.type != discord.InteractionType.component:
        return
    custom_id = interaction.data.get("custom_id", "")
    if custom_id:
        await _handle_button(interaction, custom_id)


async def _handle_button(interaction: discord.Interaction, custom_id: str):
    parts = custom_id.split(":", 2)
    if len(parts) < 2:
        return
    action = parts[0]
    key = parts[1] if len(parts) > 1 else ""

    # key format: "metric_id:comparison"
    key_parts = key.split(":", 1)
    metric_id = key_parts[0] if key_parts else ""
    comparison = key_parts[1] if len(key_parts) > 1 else ""

    # Find metric info
    metric_info = next((m for m in MONITORED_METRICS if m["id"] == metric_id), None)

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
            msg = await interaction.followup.send(
                content=f"📊 Opening analysis thread for **{metric_info['label']}**…", wait=True
            )
            channel = interaction.channel
            real_msg = await channel.fetch_message(msg.id)
            thread = await real_msg.create_thread(
                name=f"Analysis: {metric_info['label']}", auto_archive_duration=60
            )
            _thread_metrics[thread.id] = {
                "metric_id": metric_id,
                "label": metric_info["label"],
                "direction": metric_info["direction"],
                "anomaly_desc": f"{comparison} anomaly detected",
            }
            _reset_thread_timer(thread.id)

            # Run deep analysis
            today = datetime.now().strftime("%Y-%m-%d")
            baseline = Config.BASELINE_START_DATE
            prompt = (
                f"Today's date: {today}\n"
                f"Do a detailed analysis of the metric {metric_info['label']} (id: {metric_id}).\n"
                f"Direction: {metric_info['direction']}\n"
                f"Anomaly detected via: {_comparison_label(comparison)}\n\n"
                "Steps:\n"
                f"1. Fetch daily data FROM {baseline} (beta launch) to today with query_steep_metric. "
                f"Do NOT use data before {baseline} – it is unreliable.\n"
                f"2. Draw a line chart with plot_results. Use anomaly_date=\"{today}\" to mark today's anomaly on the chart.\n"
                "3. Check for relevant Jira releases with get_jira_releases\n"
                "4. Provide a detailed analysis with possible causes and recommendation\n"
            )

            loop = asyncio.get_running_loop()
            response = await loop.run_in_executor(None, agent.ask, prompt)

            if response.chart_path:
                text = response.text or "Here is the analysis:"
                chunks = split_message(text)
                await thread.send(
                    content=chunks[0],
                    file=discord.File(response.chart_path, filename="chart.png"),
                )
                for chunk in chunks[1:]:
                    await thread.send(content=chunk)
                try:
                    os.remove(response.chart_path)
                except OSError:
                    pass
            else:
                for chunk in split_message(response.text):
                    await thread.send(content=chunk)

            await thread.send("💬 Feel free to ask more questions about this metric here!")

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
