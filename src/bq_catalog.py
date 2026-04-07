"""
BQ Metric Catalog – predefined queries for known Goals Analytics tables.

Each entry defines a ready-to-use metric. The user picks from this list
in the admin UI and can optionally edit the SQL before saving.

All queries MUST return exactly two columns:
  date  DATE    – one row per day
  value NUMERIC – the measured value for that day
"""

_EC = "goals-analytics.prod_event_classes"
_INT = "goals-analytics.prod_intermediate"

CATALOG: list[dict] = [

    # ── Character events ──────────────────────────────────────────────────────

    {
        "id": "ec_character_generated_count",
        "label": "Characters Generated",
        "category": "Character Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_character_CharacterGenerated`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    # ── Game client events ────────────────────────────────────────────────────

    {
        "id": "ec_gameclient_started_count",
        "label": "Game Client Started",
        "category": "Game Client Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_gameclient_GameClientStarted`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameclient_crash_ratio",
        "label": "App Crash Ratio (Crashes / App Opens)",
        "category": "Game Client Events",
        "direction": "alert_on_rise",
        "display_format": "number",
        "sql_query": f"""
SELECT
    ao.partition_date AS date,
    SAFE_DIVIDE(COALESCE(ac.crash_count, 0), COUNT(*)) AS value
FROM `{_EC}.ec_launcher_AppOpen` ao
LEFT JOIN (
    SELECT
        DATE(timestamp) AS crash_date,
        COUNT(*) AS crash_count
    FROM `{_EC}.ec_gameclient_AppCrash`
    WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY 1
) ac ON ao.partition_date = ac.crash_date
WHERE ao.partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY ao.partition_date, ac.crash_count
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameclient_match_started_count",
        "label": "Matches Started",
        "category": "Game Client Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_gameclient_MatchStarted`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameclient_match_ended_count",
        "label": "Matches Ended",
        "category": "Game Client Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_gameclient_MatchEnded`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameclient_screen_viewed_count",
        "label": "Screen Views",
        "category": "Game Client Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_gameclient_ScreenViewed`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameclient_screen_clicked_count",
        "label": "Screen Element Clicks",
        "category": "Game Client Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_gameclient_ScreenElementClicked`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    # ── Game server events (per match) ────────────────────────────────────────

    {
        "id": "ec_gameserver_ball_blocked_per_match",
        "label": "Ball Blocked (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallBlocked`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_hit_goal_frame_per_match",
        "label": "Ball Hit Goal Frame (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallHitGoalFrame`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_intercepted_per_match",
        "label": "Ball Intercepted (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallIntercepted`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_outside_field_per_match",
        "label": "Ball Outside Field (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallOutsideField`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_position_per_match",
        "label": "Ball Position Events (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallPosition`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_possession_changed_per_match",
        "label": "Ball Possession Changed (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallPossessionChanged`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_ball_trapped_per_match",
        "label": "Ball Trapped (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_BallTrapped`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_character_fouled_per_match",
        "label": "Character Fouled (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_CharacterFouled`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_character_got_card_per_match",
        "label": "Character Got Card (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_CharacterGotCard`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_character_position_per_match",
        "label": "Character Position Events (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_CharacterPosition`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_character_substituted_per_match",
        "label": "Character Substituted (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_CharacterSubstituted`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_clearance_per_match",
        "label": "Clearance (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Clearance`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_collision_per_match",
        "label": "Collision (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Collision`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_corner_kick_per_match",
        "label": "Corner Kick (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_CornerKick`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_dribble_touch_per_match",
        "label": "Dribble Touch (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_DribbleTouch`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_duel_per_match",
        "label": "Duel (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Duel`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_free_kick_per_match",
        "label": "Free Kick (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_FreeKick`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_goal_scored_per_match",
        "label": "Goals Scored (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_GoalScored`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_goalie_save_per_match",
        "label": "Goalie Save (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_GoalieSave`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_goalie_save_attempt_per_match",
        "label": "Goalie Save Attempt (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_GoalieSaveAttempt`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_kick_off_per_match",
        "label": "Kick Off (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_KickOff`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_offside_per_match",
        "label": "Offside (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Offside`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_pass_per_match",
        "label": "Pass (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Pass`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_penalty_kick_per_match",
        "label": "Penalty Kick (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_PenaltyKick`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_scoring_attempt_per_match",
        "label": "Scoring Attempt (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_ScoringAttempt`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_skill_move_per_match",
        "label": "Skill Move (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_SkillMove`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_tackle_per_match",
        "label": "Tackle (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_Tackle`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_gameserver_throw_in_per_match",
        "label": "Throw In (per match)",
        "category": "Game Server Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT match.match_id)) AS value
FROM `{_EC}.ec_gameserver_ThrowIn`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    # ── Inventory events ──────────────────────────────────────────────────────

    {
        "id": "ec_inventory_transaction_count",
        "label": "Inventory Transactions",
        "category": "Inventory Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_inventory_InventoryTransaction`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    # ── Launcher events ───────────────────────────────────────────────────────

    {
        "id": "ec_launcher_app_open_count",
        "label": "App Opens",
        "category": "Launcher Events",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    COUNT(*) AS value
FROM `{_EC}.ec_launcher_AppOpen`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "ec_launcher_download_time_per_mb",
        "label": "Game Client Download Time (seconds per MB)",
        "category": "Launcher Events",
        "direction": "alert_on_rise",
        "display_format": "number",
        "sql_query": f"""
SELECT
    partition_date AS date,
    AVG(SAFE_DIVIDE(elapsed, NULLIF(size, 0))) AS value
FROM `{_EC}.ec_launcher_GameClientDownload`
WHERE partition_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    AND size > 0
GROUP BY date
ORDER BY date
""".strip(),
    },

    # ── Intermediate models ───────────────────────────────────────────────────

    {
        "id": "int_screen_id_distinct_count",
        "label": "Unique Screen IDs (screenviews)",
        "category": "Intermediate Models",
        "direction": "alert_on_rise",
        "display_format": "number",
        "sql_query": f"""
SELECT
    DATE(timestamp) AS date,
    COUNT(DISTINCT screen_id) AS value
FROM `{_INT}.user_screenviews_game`
WHERE DATE(timestamp) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "int_avg_fps",
        "label": "Average FPS",
        "category": "Intermediate Models",
        "direction": "alert_on_drop",
        "display_format": "number",
        "sql_query": f"""
SELECT
    date_day AS date,
    AVG(fps) AS value
FROM `{_INT}.user_daily_ping_fps`
WHERE date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

    {
        "id": "int_avg_ping_ms",
        "label": "Average Ping (ms)",
        "category": "Intermediate Models",
        "direction": "alert_on_rise",
        "display_format": "number",
        "sql_query": f"""
SELECT
    date_day AS date,
    AVG(ping_ms) AS value
FROM `{_INT}.user_daily_ping_fps`
WHERE date_day >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date
""".strip(),
    },

]


def get_catalog() -> list[dict]:
    """Return the full catalog."""
    return CATALOG


def get_catalog_excluding(existing_ids: set[str]) -> list[dict]:
    """Return catalog entries whose id is not already in existing_ids."""
    return [e for e in CATALOG if e["id"] not in existing_ids]
