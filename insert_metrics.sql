-- Insert new Steep metrics into metric_configs.
-- Safe to run multiple times — MERGE skips existing metric_ids.
-- ~74 metrics based on boss's requirements:
--   D1-D30, Minutes/User Game, Sessions/User Game, Coverage (%), FTUE-allt,
--   MM-metrics, dbt Cost, allt med "/" i namnet, ratio/procent-metrics.

MERGE `lia-project-sandbox-deletable.anomaly_checks_demo.metric_configs` AS T
USING (
  -- Challenges (only ratio + /-metrics)
  SELECT '3cvKKOHwahY9' AS metric_id, 'Challenges Claim Ratio' AS metric_label, 'alert_on_drop' AS direction, 'https://web.steep.app/FPxmrNPhps6x/metrics/3cvKKOHwahY9' AS steep_url, 0.25 AS pace_threshold, 0.20 AS dod_threshold, 0.15 AS wow_threshold, TRUE AS enabled, 'percent' AS display_format
  UNION ALL SELECT 's9qBgBjZlZC3', 'Challenges Claimed / User', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/s9qBgBjZlZC3', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Costs
  UNION ALL SELECT 'mfUFMGbWKVcs', 'dbt Cost', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/mfUFMGbWKVcs', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Coverage (procent)
  UNION ALL SELECT 'VauKXJVDPRPT', 'Coverage 1v1', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/VauKXJVDPRPT', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT '1GcifDM_9YYn', 'Coverage 5v5', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/1GcifDM_9YYn', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'wXBVuoQeQ-Jg', 'Coverage Arena', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/wXBVuoQeQ-Jg', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT '86FkOV4dl7ik', 'Coverage Bots', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/86FkOV4dl7ik', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'MrsjCmuqAbyr', 'Coverage Solo', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/MrsjCmuqAbyr', 0.25, 0.20, 0.15, TRUE, 'percent'
  -- Currency (only /-metric)
  UNION ALL SELECT 'a5aUBjlNsOte', 'Currency Flow / Active User', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/a5aUBjlNsOte', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Retention (Dx)
  UNION ALL SELECT 'lt0vobt4hCc1', 'D1 App', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/lt0vobt4hCc1', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'S603A2sZ4Bah', 'D1 Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/S603A2sZ4Bah', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'GoPdDg8Kj1Wy', 'D3 Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/GoPdDg8Kj1Wy', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'axPfI-Yzc1fv', 'D7 Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/axPfI-Yzc1fv', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT '1x0g6yH9SSx9', 'D30 Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/1x0g6yH9SSx9', 0.25, 0.20, 0.15, TRUE, 'percent'
  -- FTUE (allt)
  UNION ALL SELECT 'S3l8w3QNkyhx', 'FTUE Active Users Arena', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/S3l8w3QNkyhx', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '3vINFATWc_Ag', 'FTUE Active Users Bots', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/3vINFATWc_Ag', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'Mc6uSdhfEMhR', 'FTUE Active Users PvP', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Mc6uSdhfEMhR', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'tsYpHdl3284i', 'FTUE Active Users Solo', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/tsYpHdl3284i', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'BnMCHKjm8pP0', 'FTUE Challenges Claimed', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/BnMCHKjm8pP0', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'W-LsOxMNS3vb', 'FTUE Challenges Claimed / Completed Ratio', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/W-LsOxMNS3vb', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'Mlk14ER1j96F', 'FTUE Challenges Claimed / User', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Mlk14ER1j96F', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'bThxe6wrVTI9', 'FTUE Challenges Claimed Coverage', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/bThxe6wrVTI9', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'Y3wGzZ8LCkFe', 'FTUE Challenges Claimed Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Y3wGzZ8LCkFe', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '4nIchRI1Ggiv', 'FTUE Challenges Completed', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/4nIchRI1Ggiv', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'nHdIbJH4A16Q', 'FTUE Coverage Arena', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/nHdIbJH4A16Q', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'NHkmaZNTST-g', 'FTUE Coverage Bots', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/NHkmaZNTST-g', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'uk0hlA0LggID', 'FTUE Coverage PvP', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/uk0hlA0LggID', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'vG2JoidfRpGc', 'FTUE Coverage Solo', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/vG2JoidfRpGc', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'wo4K3fy9rPk0', 'FTUE First Day Playtime', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/wo4K3fy9rPk0', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'Nq5ceso0K746', 'FTUE First Match', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Nq5ceso0K746', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'OJZRF6WP2fAa', 'FTUE First Match Win Ratio', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/OJZRF6WP2fAa', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'QT6huTC8wVbQ', 'FTUE First Match Won', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/QT6huTC8wVbQ', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'yOf-ecYo7pNX', 'FTUE First Session Playtime', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/yOf-ecYo7pNX', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'hZYyijS0d27y', 'FTUE MM Coverage', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/hZYyijS0d27y', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT '9i-gLaSLu5zw', 'FTUE MM Match Confirmed Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/9i-gLaSLu5zw', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '_x1h8jZEzFGG', 'FTUE MM Success Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/_x1h8jZEzFGG', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'TLL89ys_axoa', 'FTUE MM Ticket Created Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/TLL89ys_axoa', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'U6ubXugdn995', 'FTUE Playtime Day Total Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/U6ubXugdn995', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'znQJ0_gomV6k', 'FTUE Playtime Session Total Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/znQJ0_gomV6k', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'Ajy4h9vDSrBq', 'FTUE Practice Scenario Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Ajy4h9vDSrBq', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'Go82qedTwVX3', 'FTUE Practice Scenario / User', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Go82qedTwVX3', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '_bLsrij4-BzD', 'FTUE Practice Scenarios Coverage', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/_bLsrij4-BzD', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'pnZcCY-ebEO5', 'FTUE Practice Scenario Claimed', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/pnZcCY-ebEO5', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '5g0vwVloMH1r', 'FTUE Sessions', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/5g0vwVloMH1r', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'i4cfSWJ7JTg4', 'FTUE Sessions / User', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/i4cfSWJ7JTg4', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'CkJ5GAOly0vq', 'FTUE User Skill', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/CkJ5GAOly0vq', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Matches (only /-metrics)
  UNION ALL SELECT 'GAMDvc4V0rI8', 'Matches 1v1 Ranked / User 1v1 Ranked', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/GAMDvc4V0rI8', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'XtIRkq6t3bYH', 'Matches 1v1 / User 1v1', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/XtIRkq6t3bYH', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'mRW1zMHvDtvA', 'Matches 1v1 Private / User 1v1 Private', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/mRW1zMHvDtvA', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'kuHBo0negIwv', 'Matches 1v1 Quickplay / User 1v1 Quickplay', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/kuHBo0negIwv', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'cE3ieJWt8QeP', 'Matches 5v5 / User 5v5', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/cE3ieJWt8QeP', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '32JWbU1GMKlu', 'Matches Solo / User Solo', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/32JWbU1GMKlu', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Minutes (1v1 + all /-metrics)
  UNION ALL SELECT 'maOAAXx2sco0', 'Minutes 1v1', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/maOAAXx2sco0', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'HDzURPhIsFo4', 'Minutes 1v1 / Active User 1v1', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/HDzURPhIsFo4', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'QjAPNE1t3VaH', 'Minutes 5v5 / User 5v5', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/QjAPNE1t3VaH', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'WBAlCtxQfsSU', 'Minutes Arena / Active User Arena', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/WBAlCtxQfsSU', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'IhkQvFVzqBeb', 'Minutes Bots / Active User Bots', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/IhkQvFVzqBeb', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'tWRGNmgASlka', 'Minutes / User Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/tWRGNmgASlka', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'QCnlhhJdhCwN', 'Minutes / Session Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/QCnlhhJdhCwN', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'Ka7u3mlIqZCq', 'Minutes / User App', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Ka7u3mlIqZCq', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'xm9qy3a8aJxU', 'Minutes Solo / Active User Solo', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/xm9qy3a8aJxU', 0.25, 0.20, 0.15, TRUE, 'number'
  -- MM (Matchmaking)
  UNION ALL SELECT '2NTDsW-KkWpS', 'MM Coverage', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/2NTDsW-KkWpS', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'WjMaini0oP4h', 'MM Cancel Ratio', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/WjMaini0oP4h', 0.25, 0.20, 0.15, TRUE, 'percent'
  UNION ALL SELECT 'Z3Ase97snd7k', 'MM Match Confirmed', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Z3Ase97snd7k', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'vdAU23l-4BUr', 'MM Success', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/vdAU23l-4BUr', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'LlFt6XrhGcyr', 'MM Tickets Created', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/LlFt6XrhGcyr', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'GZto5VRMuoWi', 'MM Tickets Created Users', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/GZto5VRMuoWi', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'OZmqt7loldpx', 'MM Tickets Cancelled', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/OZmqt7loldpx', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'u7Rg7zrZAtpa', 'MM Waiting Time Before Cancel', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/u7Rg7zrZAtpa', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'mffjXF_0Hcfk', 'MM Waiting Time Before Cancel Total', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/mffjXF_0Hcfk', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT 'GZYNGemy8tTu', 'MM Waiting Time For Match Total', 'alert_on_rise', 'https://web.steep.app/FPxmrNPhps6x/metrics/GZYNGemy8tTu', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Packs (/-metric)
  UNION ALL SELECT 'EbmBINor8wtA', 'Packs / Active User Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/EbmBINor8wtA', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Sessions (only /-metrics)
  UNION ALL SELECT 'lhl8vTFvXU5l', 'Sessions / User Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/lhl8vTFvXU5l', 0.25, 0.20, 0.15, TRUE, 'number'
  UNION ALL SELECT '_5dqd0D14l5e', 'Sessions / User App', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/_5dqd0D14l5e', 0.25, 0.20, 0.15, TRUE, 'number'
  -- Swaps (/-metric)
  UNION ALL SELECT 'Z7Rc6GP9OcQf', 'Swaps Submitted / Active User Game', 'alert_on_drop', 'https://web.steep.app/FPxmrNPhps6x/metrics/Z7Rc6GP9OcQf', 0.25, 0.20, 0.15, TRUE, 'number'
) AS S
ON T.metric_id = S.metric_id
WHEN NOT MATCHED THEN
  INSERT (metric_id, metric_label, direction, steep_url, pace_threshold, dod_threshold, wow_threshold, enabled, updated_at, display_format)
  VALUES (S.metric_id, S.metric_label, S.direction, S.steep_url, S.pace_threshold, S.dod_threshold, S.wow_threshold, S.enabled, CURRENT_TIMESTAMP(), S.display_format);
