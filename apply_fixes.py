#!/usr/bin/env python3
"""Apply all pending fixes to bot.py and detector.py."""
import re

# ── detector.py fixes ─────────────────────────────────────────────────────────

with open("/home/oliver/Mimir/src/detector.py", "r") as f:
    det = f.read()

# 1. collect_and_check: track failed metrics in _process_one, return tuple
det = det.replace(
    "    def collect_and_check(self, progress_callback=None) -> list[Anomaly]:\n"
    '        """Collect snapshots from Steep, save to BQ, run comparisons.\n'
    "\n"
    "        Returns a list of Anomaly objects (empty if all is well).\n"
    "        progress_callback: optional callable(current, total, label) called after each metric.\n"
    '        """',
    "    def collect_and_check(self, progress_callback=None) -> tuple[list[Anomaly], list[tuple[str, str]]]:\n"
    '        """Collect snapshots from Steep, save to BQ, run comparisons.\n'
    "\n"
    "        Returns (anomalies, failed_labels) where failed_labels is a list of (label, error_str).\n"
    "        progress_callback: optional callable(current, total, label) called after each metric.\n"
    '        """'
)

det = det.replace(
    "        anomalies: list[Anomaly] = []\n"
    "        total = len(self._metric_configs)\n"
    "        _lock = threading.Lock()\n"
    "        _counter = [0]\n"
    "\n"
    "        def _process_one(metric: dict) -> list[Anomaly]:",
    "        anomalies: list[Anomaly] = []\n"
    "        failed_labels: list[tuple[str, str]] = []\n"
    "        total = len(self._metric_configs)\n"
    "        _lock = threading.Lock()\n"
    "        _counter = [0]\n"
    "\n"
    "        def _process_one(metric: dict) -> tuple[list[Anomaly], list[tuple[str, str]]]:"
)

det = det.replace(
    "            try:\n"
    "                value, refreshed_at, historical = self._fetch_values(metric_id)\n"
    "            except Exception as e:\n"
    "                logger.error(\"Failed to fetch %s from Steep: %s\", label, e)\n"
    "                with _lock:\n"
    "                    _counter[0] += 1\n"
    "                    if progress_callback:\n"
    "                        progress_callback(_counter[0], total, label)\n"
    "                return result\n"
    "\n"
    "            if value is None:\n"
    "                logger.warning(\"No data for %s today, skipping.\", label)\n"
    "                with _lock:\n"
    "                    _counter[0] += 1\n"
    "                    if progress_callback:\n"
    "                        progress_callback(_counter[0], total, label)\n"
    "                return result",
    "            failed: list[tuple[str, str]] = []\n"
    "            try:\n"
    "                value, refreshed_at, historical = self._fetch_values(metric_id)\n"
    "            except Exception as e:\n"
    "                logger.error(\"Failed to fetch %s from Steep: %s\", label, e)\n"
    "                failed.append((label, type(e).__name__))\n"
    "                with _lock:\n"
    "                    _counter[0] += 1\n"
    "                    if progress_callback:\n"
    "                        progress_callback(_counter[0], total, label)\n"
    "                return result, failed\n"
    "\n"
    "            if value is None:\n"
    "                logger.warning(\"No data for %s today, skipping.\", label)\n"
    "                failed.append((label, \"No data\"))\n"
    "                with _lock:\n"
    "                    _counter[0] += 1\n"
    "                    if progress_callback:\n"
    "                        progress_callback(_counter[0], total, label)\n"
    "                return result, failed"
)

det = det.replace(
    "            with _lock:\n"
    "                _counter[0] += 1\n"
    "                if progress_callback:\n"
    "                    progress_callback(_counter[0], total, label)\n"
    "\n"
    "            return result\n"
    "\n"
    "        with ThreadPoolExecutor(max_workers=8) as executor:\n"
    "            steep_futures = [executor.submit(_process_one, m) for m in self._metric_configs]\n"
    "            bq_future = executor.submit(self.check_bq_metrics)\n"
    "\n"
    "            for future in as_completed(steep_futures):\n"
    "                try:\n"
    "                    anomalies.extend(future.result())\n"
    "                except Exception as e:\n"
    "                    logger.error(\"Unhandled error in metric worker: %s\", e)\n"
    "\n"
    "            try:\n"
    "                anomalies.extend(bq_future.result())\n"
    "            except Exception as e:\n"
    "                logger.error(\"BQ metric check failed: %s\", e)\n"
    "\n"
    "        return anomalies",
    "            with _lock:\n"
    "                _counter[0] += 1\n"
    "                if progress_callback:\n"
    "                    progress_callback(_counter[0], total, label)\n"
    "\n"
    "            return result, failed\n"
    "\n"
    "        with ThreadPoolExecutor(max_workers=8) as executor:\n"
    "            steep_futures = [executor.submit(_process_one, m) for m in self._metric_configs]\n"
    "            bq_future = executor.submit(self.check_bq_metrics)\n"
    "\n"
    "            for future in as_completed(steep_futures):\n"
    "                try:\n"
    "                    res, fl = future.result()\n"
    "                    anomalies.extend(res)\n"
    "                    failed_labels.extend(fl)\n"
    "                except Exception as e:\n"
    "                    logger.error(\"Unhandled error in metric worker: %s\", e)\n"
    "\n"
    "            try:\n"
    "                anomalies.extend(bq_future.result())\n"
    "            except Exception as e:\n"
    "                logger.error(\"BQ metric check failed: %s\", e)\n"
    "\n"
    "        return anomalies, failed_labels"
)

# 2. check_only: track skipped/failed, return tuple
det = det.replace(
    "    def check_only(self, progress_callback=None) -> list[Anomaly]:\n"
    '        """Run anomaly checks using only BQ snapshot data — no Steep API calls.\n'
    "\n"
    "        Fetches all snapshot data in 2 batch queries, then compares in memory.\n"
    "        Used by the monitor loop; snapshot collection is handled by the snapshot job.\n"
    '        """',
    "    def check_only(self, progress_callback=None) -> tuple[list[Anomaly], list[tuple[str, str]]]:\n"
    '        """Run anomaly checks using only BQ snapshot data — no Steep API calls.\n'
    "\n"
    "        Fetches all snapshot data in 2 batch queries, then compares in memory.\n"
    "        Used by the monitor loop; snapshot collection is handled by the snapshot job.\n"
    "        Returns (anomalies, failed_labels).\n"
    '        """'
)

det = det.replace(
    "        try:\n"
    "            today_rows = self.bq.run_query(sql_today)\n"
    "            history_rows = self.bq.run_query(sql_history)\n"
    "            pace_rows = self.bq.run_query(sql_pace)\n"
    "        except Exception as e:\n"
    "            logger.error(\"check_only batch query failed: %s\", e)\n"
    "            return []",
    "        failed_labels: list[tuple[str, str]] = []\n"
    "        try:\n"
    "            today_rows = self.bq.run_query(sql_today)\n"
    "            history_rows = self.bq.run_query(sql_history)\n"
    "            pace_rows = self.bq.run_query(sql_pace)\n"
    "        except Exception as e:\n"
    "            logger.error(\"check_only batch query failed: %s\", e)\n"
    "            return [], []"
)

det = det.replace(
    "                    except Exception as e:\n"
    "                        logger.warning(\"%s: Steep fallback failed: %s\", label, e)\n"
    "                else:",
    "                    except Exception as e:\n"
    "                        logger.warning(\"%s: Steep fallback failed: %s\", label, e)\n"
    "                        failed_labels.append((label, f\"Steep {type(e).__name__}\"))\n"
    "                else:"
)

det = det.replace(
    "            current_value = today_values.get(metric_id)\n"
    "            if current_value is None:\n"
    "                logger.info(\"%s: no BQ snapshot for today, skipping.\", label)\n"
    "                if progress_callback:\n"
    "                    progress_callback(i + 1, total, label)\n"
    "                continue",
    "            current_value = today_values.get(metric_id)\n"
    "            if current_value is None:\n"
    "                logger.info(\"%s: no BQ snapshot for today, skipping.\", label)\n"
    "                failed_labels.append((label, \"No snapshot\"))\n"
    "                if progress_callback:\n"
    "                    progress_callback(i + 1, total, label)\n"
    "                continue"
)

det = det.replace(
    "        # BQ metrics (SQL-based) still run as before\n"
    "        try:\n"
    "            anomalies.extend(self.check_bq_metrics())\n"
    "        except Exception as e:\n"
    "            logger.error(\"BQ metric check failed: %s\", e)\n"
    "\n"
    "        return anomalies\n"
    "\n"
    "    def _check_metric_from_cache(",
    "        # BQ metrics (SQL-based) still run as before\n"
    "        try:\n"
    "            anomalies.extend(self.check_bq_metrics())\n"
    "        except Exception as e:\n"
    "            logger.error(\"BQ metric check failed: %s\", e)\n"
    "\n"
    "        return anomalies, failed_labels\n"
    "\n"
    "    def _check_metric_from_cache("
)

with open("/home/oliver/Mimir/src/detector.py", "w") as f:
    f.write(det)

print("detector.py done")

# ── bot.py fixes ───────────────────────────────────────────────────────────────

with open("/home/oliver/Mimir/src/bot.py", "r") as f:
    bot = f.read()

# 3. status command: unpack tuple, show failures
bot = bot.replace(
    "    try:\n"
    "        anomalies = await loop.run_in_executor(\n"
    "            None, lambda: detector.collect_and_check(progress_callback=on_progress)\n"
    "        )\n"
    "    except Exception as e:\n"
    "        await progress_msg.edit(content=f\"❌ Error during check: {e}\")\n"
    "        return\n"
    "\n"
    "    await progress_msg.edit(content=f\"✅ Done — checked `{total_metrics}` metrics.\")",
    "    try:\n"
    "        anomalies, failed_labels = await loop.run_in_executor(\n"
    "            None, lambda: detector.collect_and_check(progress_callback=on_progress)\n"
    "        )\n"
    "    except Exception as e:\n"
    "        await progress_msg.edit(content=f\"❌ Error during check: {e}\")\n"
    "        return\n"
    "\n"
    "    failed_count = len(failed_labels)\n"
    "    checked_count = total_metrics - failed_count\n"
    "    fail_note = \"\"\n"
    "    if failed_labels:\n"
    "        failed_list = \", \".join(f\"`{lbl}` ({err})\" for lbl, err in failed_labels)\n"
    "        fail_note = f\" ⚠️ {failed_count} could not be fetched: {failed_list}\"\n"
    "    await progress_msg.edit(content=f\"✅ Done — checked `{checked_count}/{total_metrics}` metrics.{fail_note}\")"
)

# 4. monitor_loop: complete rewrite
OLD_MONITOR = (
    "@tasks.loop(seconds=Config.MONITOR_INTERVAL_SECONDS)\n"
    "async def monitor_loop():\n"
    '    """Background task – checks BQ snapshots for anomalies every hour."""\n'
    "    alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID\n"
    "    if not alert_channel_id:\n"
    "        logger.warning(\"DISCORD_ALERT_CHANNEL_ID not set – skipping monitor.\")\n"
    "        return\n"
    "\n"
    "    channel = bot.get_channel(int(alert_channel_id))\n"
    "    if not channel:\n"
    "        logger.error(\"Alert channel %s not found\", alert_channel_id)\n"
    "        return\n"
    "\n"
    "    total_metrics = len(detector._metric_configs)\n"
    "    progress_msg = await channel.send(f\"⏳ Checking metrics... `0/{total_metrics}` {'░' * 20}\")\n"
    "\n"
    "    loop = asyncio.get_running_loop()\n"
    "    last_update = [0]\n"
    "\n"
    "    def make_bar(current: int, total: int) -> str:\n"
    "        filled = int(20 * current / total) if total else 0\n"
    "        bar = '█' * filled + '░' * (20 - filled)\n"
    "        pct = int(100 * current / total) if total else 0\n"
    "        return f\"⏳ Checking metrics... `{current}/{total}` `{bar}` {pct}%\"\n"
    "\n"
    "    def on_progress(current: int, total: int, label: str):\n"
    "        if current - last_update[0] >= 5 or current == total:\n"
    "            last_update[0] = current\n"
    "            asyncio.run_coroutine_threadsafe(\n"
    "                progress_msg.edit(content=make_bar(current, total)), loop\n"
    "            )\n"
    "\n"
    "    try:\n"
    "        anomalies = await loop.run_in_executor(\n"
    "            None, lambda: detector.check_only(progress_callback=on_progress)\n"
    "        )\n"
    "    except Exception as e:\n"
    "        logger.error(\"Monitor check failed: %s\", e)\n"
    "        await progress_msg.edit(content=f\"❌ Check failed: {e}\")\n"
    "        return\n"
    "\n"
    "    if not anomalies:\n"
    "        logger.info(\"Monitor: no anomalies detected.\")\n"
    "        await progress_msg.edit(content=f\"✅ All clear — checked `{total_metrics}` metrics, no anomalies detected.\")\n"
    "        return\n"
    "\n"
    "    today_str = datetime.now().strftime(\"%Y-%m-%d\")\n"
    "    new_anomalies = []\n"
    "    for a in anomalies:\n"
    "        key = (a.metric_id, a.comparison, today_str)\n"
    "        if key not in _alerted_keys:\n"
    "            _alerted_keys.add(key)\n"
    "            new_anomalies.append(a)\n"
    "\n"
    "    if not new_anomalies:\n"
    "        logger.info(\"Monitor: anomalies exist but already alerted today.\")\n"
    "        return\n"
    "\n"
    "    grouped = _group_anomalies(new_anomalies)\n"
    "    for metric_anomalies in grouped.values():\n"
    "        try:\n"
    "            await send_grouped_anomaly_alert(channel, metric_anomalies)\n"
    "        except discord.DiscordServerError as e:\n"
    "            logger.warning(\"Discord server error sending alert for %s: %s\", metric_anomalies[0].metric_label, e)\n"
    "        except Exception as e:\n"
    "            logger.error(\"Failed to send alert for %s: %s\", metric_anomalies[0].metric_label, e)"
)

NEW_MONITOR = (
    "@tasks.loop(seconds=Config.MONITOR_INTERVAL_SECONDS)\n"
    "async def monitor_loop():\n"
    '    """Background task – checks BQ snapshots for anomalies every hour."""\n'
    "    alert_channel_id = Config.DISCORD_ALERT_CHANNEL_ID\n"
    "    if not alert_channel_id:\n"
    "        logger.warning(\"DISCORD_ALERT_CHANNEL_ID not set – skipping monitor.\")\n"
    "        return\n"
    "\n"
    "    channel = bot.get_channel(int(alert_channel_id))\n"
    "    if not channel:\n"
    "        logger.error(\"Alert channel %s not found\", alert_channel_id)\n"
    "        return\n"
    "\n"
    "    error_channel_id = Config.DISCORD_ERROR_CHANNEL_ID\n"
    "    error_channel = bot.get_channel(int(error_channel_id)) if error_channel_id else channel\n"
    "    if not error_channel:\n"
    "        error_channel = channel\n"
    "\n"
    "    total_metrics = len(detector._metric_configs)\n"
    "    loop = asyncio.get_running_loop()\n"
    "\n"
    "    try:\n"
    "        anomalies, failed_labels = await loop.run_in_executor(\n"
    "            None, lambda: detector.check_only()\n"
    "        )\n"
    "    except Exception as e:\n"
    "        logger.error(\"Monitor check failed: %s\", e, exc_info=True)\n"
    "        await error_channel.send(f\"❌ Monitor check failed: {e}\")\n"
    "        return\n"
    "\n"
    "    failed_count = len(failed_labels)\n"
    "    checked_count = total_metrics - failed_count\n"
    "\n"
    "    if failed_labels:\n"
    "        now_cest = datetime.utcnow() + timedelta(hours=2)\n"
    "        embed = discord.Embed(\n"
    "            title=\"⚠️ Mimir – fetch errors\",\n"
    "            color=discord.Color.orange(),\n"
    "            timestamp=datetime.utcnow(),\n"
    "        )\n"
    "        embed.add_field(name=\"Time (CEST)\", value=now_cest.strftime(\"%Y-%m-%d %H:%M\"), inline=True)\n"
    "        embed.add_field(name=\"Coverage\", value=f\"`{checked_count}/{total_metrics}` metrics checked\", inline=True)\n"
    "        by_error: dict[str, list[str]] = {}\n"
    "        for lbl, err in failed_labels:\n"
    "            by_error.setdefault(err, []).append(lbl)\n"
    "        lines = [f\"**{et}**: {', '.join(f'`{l}`' for l in ls)}\" for et, ls in by_error.items()]\n"
    "        embed.add_field(name=f\"Failed metrics ({failed_count})\", value=\"\\n\".join(lines), inline=False)\n"
    "        embed.set_footer(text=\"Mimir — Error Monitor\")\n"
    "        await error_channel.send(embed=embed)\n"
    "\n"
    "    if not anomalies:\n"
    "        logger.info(\"Monitor: no anomalies detected.\")\n"
    "        await channel.send(f\"✅ All clear — checked `{checked_count}/{total_metrics}` metrics, no anomalies detected.\")\n"
    "        return\n"
    "\n"
    "    today_str = datetime.now().strftime(\"%Y-%m-%d\")\n"
    "    new_anomalies = []\n"
    "    for a in anomalies:\n"
    "        key = (a.metric_id, a.comparison, today_str)\n"
    "        if key not in _alerted_keys:\n"
    "            _alerted_keys.add(key)\n"
    "            new_anomalies.append(a)\n"
    "\n"
    "    if not new_anomalies:\n"
    "        logger.info(\"Monitor: anomalies exist but already alerted today.\")\n"
    "        await channel.send(f\"✅ No new anomalies — checked `{checked_count}/{total_metrics}` metrics, already alerted on all active issues today.\")\n"
    "        return\n"
    "\n"
    "    grouped = _group_anomalies(new_anomalies)\n"
    "    for metric_anomalies in grouped.values():\n"
    "        try:\n"
    "            await send_grouped_anomaly_alert(channel, metric_anomalies)\n"
    "        except discord.DiscordServerError as e:\n"
    "            logger.warning(\"Discord server error sending alert for %s: %s\", metric_anomalies[0].metric_label, e)\n"
    "        except Exception as e:\n"
    "            logger.error(\"Failed to send alert for %s: %s\", metric_anomalies[0].metric_label, e)"
)

if OLD_MONITOR in bot:
    bot = bot.replace(OLD_MONITOR, NEW_MONITOR)
    print("monitor_loop replaced OK")
else:
    print("ERROR: monitor_loop old string not found!")

with open("/home/oliver/Mimir/src/bot.py", "w") as f:
    f.write(bot)

print("bot.py done")

# Verify
import subprocess
r = subprocess.run(["python3", "-m", "py_compile", "/home/oliver/Mimir/src/detector.py"], capture_output=True, text=True)
print("detector.py syntax:", "OK" if r.returncode == 0 else r.stderr)
r = subprocess.run(["python3", "-m", "py_compile", "/home/oliver/Mimir/src/bot.py"], capture_output=True, text=True)
print("bot.py syntax:", "OK" if r.returncode == 0 else r.stderr)
