"""
Discord bot – slash commands + anomaly alert buttons.

Commands:
  /status   – run a manual BQ check and show results
  /query    – ask a free question via the Gemini agent

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


# Keys (table_name, str(checked_at)) already alerted this session – avoids re-alerting on every poll
_alerted_keys: set[tuple[str, str]] = set()

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

STATUS_QUERY = """
SELECT table_name, is_valid, reason, checked_at
FROM `{table}`
ORDER BY checked_at DESC
LIMIT 10
""".format(table=Config.BQ_TABLE)

# ── Button view for anomaly follow-up ─────────────────────────────────────────

class AnalyseView(discord.ui.View):
    """Buttons posted with each anomaly alert – lets users request a deeper analysis."""

    def __init__(self, table: str, reason: str, checked_at: str):
        super().__init__(timeout=300)  # buttons expire after 5 min
        self.table = table
        self.reason = reason
        self.checked_at = checked_at

    @discord.ui.button(label="Yes, show analysis", style=discord.ButtonStyle.primary, emoji="📊")
    async def analyse(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(thinking=True)
        button.disabled = True
        self.stop()

        source_table = f"{Config.BQ_SOURCE_DATASET}.{self.table}"

        # Fetch Jira release context near the anomaly date
        jira_context = ""
        if Config.JIRA_PROJECT_KEY:
            try:
                anomaly_date = datetime.fromisoformat(str(self.checked_at)).date()
                releases = jira_client.get_releases_near_date(anomaly_date, Config.JIRA_PROJECT_KEY)
                jira_context = jira_client.format_release_context(releases, anomaly_date)
            except Exception as e:
                logger.warning("Jira context lookup failed: %s", e)

        prompt = (
            f"An anomaly was detected in table `{source_table}` at {self.checked_at}.\n"
            f"Reason: {self.reason}\n"
            + (f"\n{jira_context}\n" if jira_context else "")
            + "\nFollow the analysis steps in your instructions. "
            f"Use the schema of `{source_table}` and the reason above to decide the best queries and charts. "
            "If there are Jira releases listed above, consider whether they may explain the anomaly and mention it in your analysis. "
            "Always end with a structured written analysis including what was found, when it started, any patterns, and a concrete recommendation."
        )

        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(None, agent.ask, prompt)

            if response.chart_path:
                text = response.text or "Here is the trend chart:"
                chunks = split_message(text)
                await interaction.followup.send(content=chunks[0], file=discord.File(response.chart_path, filename="chart.png"))
                for chunk in chunks[1:]:
                    await interaction.followup.send(content=chunk)
                try:
                    os.remove(response.chart_path)
                except OSError:
                    pass
            else:
                for chunk in split_message(response.text):
                    await interaction.followup.send(content=chunk)

        except Exception as e:
            logger.error("Agent analysis failed: %s", e)
            await interaction.followup.send(content=f"Analysis failed: {e}")

    @discord.ui.button(label="No thanks", style=discord.ButtonStyle.secondary, emoji="❌")
    async def dismiss(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("OK, skipping analysis.", ephemeral=True)
        self.stop()


# ── Bot setup ─────────────────────────────────────────────────────────────────

intents = discord.Intents.default()

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
        await interaction.followup.send(f"⚠️ **{len(anomalies)} anomal{'y' if len(anomalies) == 1 else 'ies'} detected** – sending details below…")
        for r in anomalies:
            clean = _clean_reason(r.get("reason") or "")
            embed = discord.Embed(title="⚠️ Anomaly Detected", color=discord.Color.red())
            embed.add_field(name="Table", value=f"`{r['table_name']}`", inline=False)
            embed.add_field(name="Reason", value=clean, inline=False)
            embed.add_field(name="Detected at", value=str(r['checked_at']), inline=False)
            view = AnalyseView(
                table=r['table_name'],
                reason=clean,
                checked_at=str(r['checked_at']),
            )
            await interaction.followup.send(embed=embed, view=view)
    else:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies found in the latest check.",
            color=discord.Color.green(),
        )
        await interaction.followup.send(embed=embed)


# ── Alert function (called from monitor) ─────────────────────────────────────

async def send_anomaly_alert(channel_id: int, table: str, reason: str, checked_at: str):
    """Send an anomaly alert embed with analysis buttons to a Discord channel."""
    channel = bot.get_channel(channel_id)
    if not channel:
        logger.error("Channel %d not found", channel_id)
        return

    clean = _clean_reason(reason)
    embed = discord.Embed(title="⚠️ Anomaly Detected", color=discord.Color.red())
    embed.add_field(name="Table", value=f"`{table}`", inline=False)
    embed.add_field(name="Reason", value=clean, inline=False)
    embed.add_field(name="Detected at", value=checked_at, inline=False)
    view = AnalyseView(table=table, reason=clean, checked_at=checked_at)
    await channel.send(embed=embed, view=view)


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
        for row in new_alerts:
            table = row.get("table_name", "unknown")
            reason = row.get("reason") or "No reason provided"
            checked_at = str(row.get("checked_at", "unknown"))
            await send_anomaly_alert(int(alert_channel_id), table, reason, checked_at)
        if not new_alerts:
            logger.info("Monitor: no new anomalies (all already alerted).")
    else:
        embed = discord.Embed(
            title="✅ All clear",
            description="No anomalies detected in the latest check.",
            color=discord.Color.green(),
        )
        await channel.send(embed=embed)


@bot.event
async def on_ready():
    await tree.sync()
    logger.info("Bot is ready as %s", bot.user)
    if not monitor_loop.is_running():
        monitor_loop.start()


def run():
    token = Config.DISCORD_BOT_TOKEN
    if not token:
        logger.error("DISCORD_BOT_TOKEN is not set in .env")
        sys.exit(1)
    bot.run(token)


if __name__ == "__main__":
    run()
