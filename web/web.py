"""FastAPI admin web interface for configuring bot alert thresholds."""

import logging

import os

import pathlib

import sys

from fastapi import FastAPI, HTTPException, Request

from fastapi.responses import HTMLResponse, RedirectResponse

from fastapi.staticfiles import StaticFiles

from fastapi.templating import Jinja2Templates

from starlette.middleware.sessions import SessionMiddleware

from authlib.integrations.starlette_client import OAuth

from bq_client import BQClient

from config import Config

from steep_client import SteepClient

logger = logging.getLogger(__name__)

# -> App setup ->

app = FastAPI(title="Mimir Admin", docs_url=None, redoc_url=None)

_session_secret = os.environ.get("SESSION_SECRET")

if not _session_secret:

    print("FATAL: SESSION_SECRET env var is not set.")

    sys.exit(1)

# Cloud Run always sets K_SERVICE; use it to detect production.

_is_cloud = bool(os.environ.get("K_SERVICE"))

app.add_middleware(

    SessionMiddleware,

    secret_key=_session_secret,

    max_age=28800,          # 8 hours

    https_only=_is_cloud,   # Secure cookie flag on Cloud Run; off for local dev

)

# -> Google OAuth ->

oauth = OAuth()

oauth.register(

    name="google",

    server_metadata_url="https://accounts.google.com/.well-known/openid-configuration",

    client_id=os.environ.get("GOOGLE_CLIENT_ID", ""),

    client_secret=os.environ.get("GOOGLE_CLIENT_SECRET", ""),

    client_kwargs={"scope": "openid email profile https://www.googleapis.com/auth/bigquery"},

)

# Only accounts from this domain are allowed. Leave empty to allow any Google account.

ALLOWED_DOMAIN = os.environ.get("ALLOWED_DOMAIN", "")

# -> Templates & BQ ->

_BASE = pathlib.Path(__file__).parent.parent  # repo root

templates = Jinja2Templates(directory=str(_BASE / "templates"))

app.mount("/static", StaticFiles(directory=str(_BASE / "static")), name="static")

bq = BQClient(Config.GCP_PROJECT_ID)

# URL of the bot's internal HTTP server -> used to trigger config reload

_BOT_INTERNAL_URL = os.environ.get("BOT_INTERNAL_URL", "http://localhost:8081")

async def _signal_bot_reload() -> None:

    """Tell the bot to reload its metric config cache. Fire-and-forget."""

    try:

        import httpx

        async with httpx.AsyncClient(timeout=3.0) as client:

            await client.post(f"{_BOT_INTERNAL_URL}/internal/reload")

        logger.info("Bot config reload signalled.")

    except Exception as exc:

        logger.warning("Could not signal bot reload: %s", exc)

@app.post("/admin/reset", include_in_schema=False)

async def reset_bot(request: Request):

    """Clear alert history, reload configs and trigger an immediate monitor run."""

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated.")

    try:

        import httpx

        async with httpx.AsyncClient(timeout=15.0) as client:

            await client.post(f"{_BOT_INTERNAL_URL}/internal/reset")

        logger.info("Bot reset signalled.")

    except Exception as exc:

        logger.warning("Could not signal bot reset (bot may be offline): %s", exc)

    return {"ok": True}

@app.get("/admin/bot-status", include_in_schema=False)

async def bot_status(request: Request):

    """Proxy the bot's internal status (next run time, interval)."""

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated.")

    try:

        import httpx

        async with httpx.AsyncClient(timeout=3.0) as client:

            r = await client.get(f"{_BOT_INTERNAL_URL}/internal/status")

            return r.json()

    except Exception:

        return {"running": False, "interval_seconds": Config.MONITOR_INTERVAL_SECONDS, "seconds_until_next_run": None}

def _user(request: Request) -> dict | None:

    u = request.session.get("user")

    if u:

        return u

    # Local dev: auto-login when not on Cloud Run

    if not _is_cloud:

        dev_user = {"email": "dev@localhost", "name": "Dev"}

        request.session["user"] = dev_user

        return dev_user

    return None

# -> Auth routes ->

@app.get("/", include_in_schema=False)

async def root():

    return RedirectResponse("/admin")

@app.get("/auth/login", include_in_schema=False)

async def login(request: Request):

    redirect_uri = str(request.url_for("auth_callback")).replace("http://", "https://", 1)

    return await oauth.google.authorize_redirect(
        request,
        redirect_uri,
        prompt="consent",
        access_type="offline",
        scope="openid email profile https://www.googleapis.com/auth/bigquery",
    )

@app.get("/auth/callback", name="auth_callback", include_in_schema=False)

async def auth_callback(request: Request):

    try:

        token = await oauth.google.authorize_access_token(request)

    except Exception:

        logger.exception("OAuth callback failed")

        raise HTTPException(status_code=400, detail="Authentication failed.")

    userinfo = token.get("userinfo") or {}

    email = userinfo.get("email", "")

    if ALLOWED_DOMAIN and not email.lower().endswith(f"@{ALLOWED_DOMAIN.lower()}"):

        raise HTTPException(status_code=403, detail="Access denied: account not allowed.")

    request.session["user"] = {

        "email": email,

        "name": userinfo.get("name", email),

    }

    request.session["access_token"] = token.get("access_token", "")

    return RedirectResponse("/admin")

@app.get("/auth/logout", include_in_schema=False)

async def logout(request: Request):

    request.session.clear()

    return RedirectResponse("/auth/login")

# -> Admin page ->

@app.get("/admin", response_class=HTMLResponse, include_in_schema=False)

async def admin(request: Request):

    if not _user(request):

        return RedirectResponse("/auth/login")

    sql = f"""

        SELECT

            metric_id,

            metric_label,

            direction,

            display_format,

            pace_threshold,

            dod_threshold,

            wow_threshold,

            enabled,

            steep_url,

            updated_at

        FROM `{Config.BQ_METRIC_CONFIGS_TABLE}`

        ORDER BY metric_label

    """

    metrics = bq.run_query(sql)

    active_count = sum(1 for m in metrics if m.get("enabled"))

    # Load BQ monitors

    try:

        bq_monitors_sql = f"SELECT * FROM `{Config.BQ_METRICS_CONFIGS_TABLE}` ORDER BY metric_label"

        bq_monitors = bq.run_query(bq_monitors_sql)

    except Exception:

        bq_monitors = []

    bq_active_count = sum(1 for m in bq_monitors if m.get("enabled"))

    return templates.TemplateResponse(

        request=request,

        name="admin.html",

        context={

            "user": _user(request),

            "metrics": metrics,

            "active_count": active_count,

            "total_count": len(metrics),

            "bq_monitors": bq_monitors,

            "bq_active_count": bq_active_count,

            "bq_total_count": len(bq_monitors),

        },

    )

# -> API routes ->

@app.get("/api/steep/available-metrics", include_in_schema=False)

async def available_steep_metrics(request: Request):

    """List all Steep metrics not yet in the config table."""

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated")

    # Get existing metric IDs from BQ

    sql = f"SELECT metric_id FROM `{Config.BQ_METRIC_CONFIGS_TABLE}`"

    existing = {row["metric_id"] for row in bq.run_query(sql)}

    # Fetch all from Steep

    try:

        steep = SteepClient(api_key=Config.STEEP_API_TOKEN)

        all_metrics = steep.list_metrics(expand=True)

    except Exception as e:

        logger.error("Failed to fetch Steep metrics: %s", e)

        raise HTTPException(status_code=502, detail="Could not fetch metrics from Steep")

    available = []

    for m in sorted(all_metrics, key=lambda x: (x.get("name") or x.get("label") or "").lower()):

        mid = m.get("id", "")

        name = m.get("name") or m.get("label") or m.get("title") or mid

        if mid and mid not in existing:

            available.append({"id": mid, "name": name})

    return available

@app.post("/api/metrics/{metric_id}/threshold")

async def update_threshold(metric_id: str, request: Request):

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    comparison = body.get("comparison", "")

    raw = body.get("value")

    if comparison not in ("pace", "dod", "wow"):

        raise HTTPException(status_code=400, detail="comparison must be pace, dod, or wow")

    try:

        pct = float(raw)

    except (TypeError, ValueError):

        raise HTTPException(status_code=400, detail="value must be a number")

    if not (0 < pct <= 100):

        raise HTTPException(status_code=400, detail="value must be between 0 and 100 (%)")

    rows = bq.update_threshold(

        Config.BQ_METRIC_CONFIGS_TABLE,

        metric_id,

        comparison,

        pct / 100,  # store as fraction (e.g. 15 -> 0.15)

    )

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info(

        "[admin] %s updated %s %s_threshold -> %.1f%%",

        _user(request)["email"], metric_id, comparison, pct,

    )

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/metrics/{metric_id}/toggle")

async def toggle_metric(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    enabled = body.get("enabled")

    if not isinstance(enabled, bool):

        raise HTTPException(status_code=400, detail="enabled must be a boolean")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRIC_CONFIGS_TABLE}` "

        "SET enabled = @enabled, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("enabled", "BOOL", enabled),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s toggled %s -> enabled=%s", user["email"], metric_id, enabled)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/metrics/{metric_id}/toggle-collect")

async def toggle_metric_collect(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    collect_data = body.get("collect_data")

    if not isinstance(collect_data, bool):

        raise HTTPException(status_code=400, detail="collect_data must be a boolean")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRIC_CONFIGS_TABLE}` "

        "SET collect_data = @collect_data, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("collect_data", "BOOL", collect_data),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s toggled collect_data %s -> %s", user["email"], metric_id, collect_data)

    return {"ok": True}

async def bulk_update_threshold(request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    metric_ids = body.get("metric_ids", [])

    values = body.get("values", {})  # e.g. {"pace": 8.0, "wow": 10.0}  -> only non-null keys

    if not metric_ids or not isinstance(metric_ids, list):

        raise HTTPException(status_code=400, detail="metric_ids must be a non-empty list")

    if not values:

        raise HTTPException(status_code=400, detail="values must specify at least one comparison")

    valid_comparisons = {"pace", "dod", "wow"}

    from google.cloud import bigquery as _bq

    updated = 0

    for comp, raw in values.items():

        if comp not in valid_comparisons:

            continue

        try:

            pct = float(raw)

        except (TypeError, ValueError):

            raise HTTPException(status_code=400, detail=f"value for {comp} must be a number")

        if not (0 < pct <= 100):

            raise HTTPException(status_code=400, detail=f"value for {comp} must be between 0 and 100")

        col = f"{comp}_threshold"

        placeholders = ", ".join(f"@id_{i}" for i in range(len(metric_ids)))

        sql = (

            f"UPDATE `{Config.BQ_METRIC_CONFIGS_TABLE}` "

            f"SET {col} = @value, updated_at = CURRENT_TIMESTAMP() "

            f"WHERE metric_id IN ({placeholders})"

        )

        params = [_bq.ScalarQueryParameter("value", "FLOAT64", pct / 100)]

        params += [

            _bq.ScalarQueryParameter(f"id_{i}", "STRING", mid)

            for i, mid in enumerate(metric_ids)

        ]

        updated += bq.run_update(sql, params)

    logger.info("[admin] %s bulk-updated %d metrics: %s", user["email"], len(metric_ids), values)

    await _signal_bot_reload()

    return {"ok": True, "updated": updated}

@app.post("/api/metrics/bulk-toggle")

async def bulk_toggle_metrics(request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    metric_ids = body.get("metric_ids", [])

    enabled = body.get("enabled")

    if not metric_ids or not isinstance(metric_ids, list):

        raise HTTPException(status_code=400, detail="metric_ids must be a non-empty list")

    if enabled is None or not isinstance(enabled, bool):

        raise HTTPException(status_code=400, detail="enabled must be a boolean")

    from google.cloud import bigquery as _bq

    placeholders = ", ".join(f"@id_{i}" for i in range(len(metric_ids)))

    sql = (

        f"UPDATE `{Config.BQ_METRIC_CONFIGS_TABLE}` "

        f"SET enabled = @enabled, updated_at = CURRENT_TIMESTAMP() "

        f"WHERE metric_id IN ({placeholders})"

    )

    params = [_bq.ScalarQueryParameter("enabled", "BOOL", enabled)]

    params += [

        _bq.ScalarQueryParameter(f"id_{i}", "STRING", mid)

        for i, mid in enumerate(metric_ids)

    ]

    bq.run_update(sql, params)

    logger.info("[admin] %s bulk-toggled %d metrics -> enabled=%s", user["email"], len(metric_ids), enabled)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/bq-monitors/bulk-toggle")

async def bulk_toggle_bq_monitors(request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    metric_ids = body.get("metric_ids", [])

    enabled = body.get("enabled")

    if not metric_ids or not isinstance(metric_ids, list):

        raise HTTPException(status_code=400, detail="metric_ids must be a non-empty list")

    if enabled is None or not isinstance(enabled, bool):

        raise HTTPException(status_code=400, detail="enabled must be a boolean")

    from google.cloud import bigquery as _bq

    placeholders = ", ".join(f"@id_{i}" for i in range(len(metric_ids)))

    sql = (

        f"UPDATE `{Config.BQ_MONITOR_TABLE}` "

        f"SET enabled = @enabled, updated_at = CURRENT_TIMESTAMP() "

        f"WHERE metric_id IN ({placeholders})"

    )

    params = [_bq.ScalarQueryParameter("enabled", "BOOL", enabled)]

    params += [

        _bq.ScalarQueryParameter(f"id_{i}", "STRING", mid)

        for i, mid in enumerate(metric_ids)

    ]

    bq.run_update(sql, params)

    logger.info("[admin] %s bulk-toggled %d BQ monitors -> enabled=%s", user["email"], len(metric_ids), enabled)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/metrics/add")

async def add_steep_metric(request: Request):

    """Add a new Steep metric to the config table."""

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    metric_id = (body.get("metric_id") or "").strip()

    label = (body.get("metric_label") or "").strip()

    direction = body.get("direction", "alert_on_drop")

    display_format = body.get("display_format", "number")

    steep_url = (body.get("steep_url") or "").strip()

    if not metric_id or not label:

        raise HTTPException(status_code=400, detail="metric_id and metric_label are required")

    if direction not in ("alert_on_rise", "alert_on_drop"):

        raise HTTPException(status_code=400, detail="direction must be alert_on_rise or alert_on_drop")

    if display_format not in ("number", "percent"):

        raise HTTPException(status_code=400, detail="display_format must be number or percent")

    from google.cloud import bigquery as _bq

    # Check if metric already exists

    check_sql = f"SELECT 1 FROM `{Config.BQ_METRIC_CONFIGS_TABLE}` WHERE metric_id = @metric_id LIMIT 1"

    check_params = [_bq.ScalarQueryParameter("metric_id", "STRING", metric_id)]

    if bq.run_query(check_sql, params=check_params):

        raise HTTPException(status_code=409, detail="Metric with this ID already exists")

    sql = (

        f"INSERT INTO `{Config.BQ_METRIC_CONFIGS_TABLE}` "

        "(metric_id, metric_label, direction, display_format, steep_url, "

        "pace_threshold, dod_threshold, wow_threshold, enabled, updated_at) "

        "VALUES (@metric_id, @label, @direction, @display_format, @steep_url, "

        "0.25, 0.20, 0.15, TRUE, CURRENT_TIMESTAMP())"

    )

    params = [

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

        _bq.ScalarQueryParameter("label", "STRING", label),

        _bq.ScalarQueryParameter("direction", "STRING", direction),

        _bq.ScalarQueryParameter("display_format", "STRING", display_format),

        _bq.ScalarQueryParameter("steep_url", "STRING", steep_url),

    ]

    bq.run_update(sql, params)

    logger.info("[admin] %s added Steep metric %s (%s)", user["email"], metric_id, label)

    await _signal_bot_reload()

    return {"ok": True}

# -> BQ Monitor CRUD ->

def _ensure_bq_metrics_table():

    """Create the bq_metrics_configs table if it doesn't exist."""

    sql = f"""

    CREATE TABLE IF NOT EXISTS `{Config.BQ_METRICS_CONFIGS_TABLE}` (

        metric_id STRING NOT NULL,

        metric_label STRING NOT NULL,

        direction STRING DEFAULT 'alert_on_drop',

        steep_url STRING,

        pace_threshold FLOAT64 DEFAULT 0.25,

        dod_threshold FLOAT64 DEFAULT 0.20,

        wow_threshold FLOAT64 DEFAULT 0.15,

        enabled BOOL DEFAULT TRUE,

        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),

        display_format STRING DEFAULT 'number'

    )

    """

    try:

        bq.run_update(sql)

    except Exception as e:

        logger.debug("bq_metrics_configs table check: %s", e)

_ensure_bq_metrics_table()

@app.get("/api/bq-monitors/catalog", include_in_schema=False)
async def bq_monitor_catalog(request: Request):
    """Return catalog entries not yet added as BQ monitors."""
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    from bq_catalog import get_catalog_excluding
    sql = f"SELECT catalog_id FROM `{Config.BQ_METRICS_CONFIGS_TABLE}` WHERE catalog_id IS NOT NULL"
    existing = {row["catalog_id"] for row in bq.run_query(sql)}
    return get_catalog_excluding(existing)


@app.get("/api/bq-monitors", include_in_schema=False)

async def list_bq_monitors(request: Request):

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated")

    sql = f"SELECT * FROM `{Config.BQ_METRICS_CONFIGS_TABLE}` ORDER BY metric_label"

    monitors = bq.run_query(sql)

    return monitors

@app.post("/api/bq-monitors")

async def add_bq_monitor(request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    label = (body.get("metric_label") or "").strip()

    direction = body.get("direction", "alert_on_drop")

    display_format = body.get("display_format", "number")

    if not label:

        raise HTTPException(status_code=400, detail="metric_label is required")

    if direction not in ("alert_on_rise", "alert_on_drop"):

        raise HTTPException(status_code=400, detail="direction must be alert_on_rise or alert_on_drop")

    sql_query = (body.get("sql_query") or "").strip()

    catalog_id = (body.get("catalog_id") or "").strip() or None

    import uuid

    metric_id = str(uuid.uuid4())[:8]

    from google.cloud import bigquery as _bq

    sql = (

        f"INSERT INTO `{Config.BQ_METRICS_CONFIGS_TABLE}` "

        "(metric_id, metric_label, direction, display_format, "

        "pace_threshold, dod_threshold, wow_threshold, enabled, updated_at, sql_query, catalog_id) "

        "VALUES (@metric_id, @label, @direction, @display_format, "

        "0.25, 0.20, 0.15, TRUE, CURRENT_TIMESTAMP(), @sql_query, @catalog_id)"

    )

    params = [

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

        _bq.ScalarQueryParameter("label", "STRING", label),

        _bq.ScalarQueryParameter("direction", "STRING", direction),

        _bq.ScalarQueryParameter("display_format", "STRING", display_format),

        _bq.ScalarQueryParameter("sql_query", "STRING", sql_query or None),

        _bq.ScalarQueryParameter("catalog_id", "STRING", catalog_id),

    ]

    bq.run_update(sql, params)

    logger.info("[admin] %s added BQ metric %s (%s)", user["email"], metric_id, label)

    await _signal_bot_reload()

    return {"ok": True, "metric_id": metric_id}

@app.post("/api/bq-monitors/{metric_id}/threshold")

async def update_bq_monitor_threshold(metric_id: str, request: Request):

    if not _user(request):

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    comparison = body.get("comparison", "")

    raw = body.get("value")

    if comparison not in ("pace", "dod", "wow"):

        raise HTTPException(status_code=400, detail="comparison must be pace, dod, or wow")

    try:

        pct = float(raw)

    except (TypeError, ValueError):

        raise HTTPException(status_code=400, detail="value must be a number")

    if not (0 < pct <= 100):

        raise HTTPException(status_code=400, detail="value must be between 0 and 100")

    from google.cloud import bigquery as _bq

    col = f"{comparison}_threshold"

    sql = (

        f"UPDATE `{Config.BQ_METRICS_CONFIGS_TABLE}` "

        f"SET {col} = @value, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("value", "FLOAT64", pct / 100),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s updated BQ metric %s %s -> %.1f%%", _user(request)["email"], metric_id, comparison, pct)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/bq-monitors/{metric_id}/toggle")

async def toggle_bq_monitor(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    enabled = body.get("enabled")

    if not isinstance(enabled, bool):

        raise HTTPException(status_code=400, detail="enabled must be a boolean")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRICS_CONFIGS_TABLE}` "

        "SET enabled = @enabled, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("enabled", "BOOL", enabled),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s toggled BQ metric %s -> enabled=%s", user["email"], metric_id, enabled)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/bq-monitors/{metric_id}/toggle-collect")

async def toggle_bq_monitor_collect(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    collect_data = body.get("collect_data")

    if not isinstance(collect_data, bool):

        raise HTTPException(status_code=400, detail="collect_data must be a boolean")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRICS_CONFIGS_TABLE}` "

        "SET collect_data = @collect_data, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("collect_data", "BOOL", collect_data),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s toggled BQ collect_data %s -> %s", user["email"], metric_id, collect_data)

    return {"ok": True}

@app.post("/api/bq-monitors/{metric_id}/direction")

async def update_bq_monitor_direction(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    direction = body.get("direction")

    if direction not in ("alert_on_rise", "alert_on_drop"):

        raise HTTPException(status_code=400, detail="direction must be alert_on_rise or alert_on_drop")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRICS_CONFIGS_TABLE}` "

        "SET direction = @direction, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("direction", "STRING", direction),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s updated BQ metric %s direction -> %s", user["email"], metric_id, direction)

    await _signal_bot_reload()

    return {"ok": True}

@app.delete("/api/bq-monitors/{metric_id}")

async def delete_bq_monitor(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    from google.cloud import bigquery as _bq

    sql = f"DELETE FROM `{Config.BQ_METRICS_CONFIGS_TABLE}` WHERE metric_id = @metric_id"

    params = [_bq.ScalarQueryParameter("metric_id", "STRING", metric_id)]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s deleted BQ metric %s", user["email"], metric_id)

    await _signal_bot_reload()

    return {"ok": True}

@app.post("/api/metrics/{metric_id}/direction")

async def update_direction(metric_id: str, request: Request):

    user = _user(request)

    if not user:

        raise HTTPException(status_code=401, detail="Not authenticated")

    body = await request.json()

    direction = body.get("direction")

    if direction not in ("alert_on_rise", "alert_on_drop"):

        raise HTTPException(status_code=400, detail="direction must be alert_on_rise or alert_on_drop")

    from google.cloud import bigquery as _bq

    sql = (

        f"UPDATE `{Config.BQ_METRIC_CONFIGS_TABLE}` "

        "SET direction = @direction, updated_at = CURRENT_TIMESTAMP() "

        "WHERE metric_id = @metric_id"

    )

    params = [

        _bq.ScalarQueryParameter("direction", "STRING", direction),

        _bq.ScalarQueryParameter("metric_id", "STRING", metric_id),

    ]

    rows = bq.run_update(sql, params)

    if rows == 0:

        raise HTTPException(status_code=404, detail="Metric not found")

    logger.info("[admin] %s updated %s direction -> %s", user["email"], metric_id, direction)

    await _signal_bot_reload()

    return {"ok": True}

# -> Field Monitor CRUD ->

@app.get("/api/field-monitors/catalog", include_in_schema=False)
async def field_monitor_catalog(request: Request):
    """Return catalog entries with their main BQ table, for use in Field Monitor picker."""
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    import re
    from bq_catalog import get_catalog
    result = []
    for entry in get_catalog():
        match = re.search(r"FROM\s+`([^`]+)`", entry.get("sql_query", ""), re.IGNORECASE)
        bq_table = match.group(1) if match else ""
        result.append({
            "id":       entry["id"],
            "label":    entry["label"],
            "category": entry.get("category", ""),
            "bq_table": bq_table,
        })
    return result


@app.get("/api/bq-table-columns", include_in_schema=False)
async def bq_table_columns(request: Request, table: str):
    """Return flat schema for a BQ table (including nested STRUCT fields) using get_table()."""
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not table or "`" in table or ";" in table or len(table) > 300:
        raise HTTPException(status_code=400, detail="Invalid table name")
    parts = table.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail="Expected project.dataset.table")
    project, dataset, tbl_name = parts
    from google.cloud import bigquery as _bq

    def _get_client():
        access_token = request.session.get("access_token", "")
        if access_token and _is_cloud:
            from google.oauth2.credentials import Credentials
            return _bq.Client(project=project, credentials=Credentials(token=access_token))
        return bq.client

    def _flatten(fields, prefix=""):
        out = []
        for f in fields:
            path = f"{prefix}.{f.name}" if prefix else f.name
            if f.field_type in ("RECORD", "STRUCT"):
                out.extend(_flatten(f.fields, path))
            else:
                out.append({
                    "name": path,
                    "type": f.field_type,
                    "description": f.description or "",
                    "mode": f.mode,
                })
        return out

    try:
        tbl = _get_client().get_table(f"{project}.{dataset}.{tbl_name}")
        return _flatten(tbl.schema)
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Could not fetch schema: {e}")

@app.post("/api/field-monitors/discover", include_in_schema=False)
async def discover_field_monitors(request: Request):
    """
    Analyse selected catalog tables: schema descriptions + APPROX_COUNT_DISTINCT over 60 days.
    Returns suggestions: {label, bq_table, field_name, date_field, distinct, description, recommended}
    """
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()
    tables = body.get("tables", [])  # list of {label, bq_table, date_field}
    if not tables:
        raise HTTPException(status_code=400, detail="No tables provided")

    from google.cloud import bigquery as _bq

    access_token = request.session.get("access_token", "")

    def _get_bq_client(project: str):
        if access_token and _is_cloud:
            from google.oauth2.credentials import Credentials
            return _bq.Client(project=project, credentials=Credentials(token=access_token))
        return bq.client

    def _exec(client, sql, params=None):
        cfg = _bq.QueryJobConfig(query_parameters=params or [])
        return [dict(r) for r in client.query(sql, job_config=cfg).result()]

    DESC_BAD = ['identifier', 'uuid', 'unique key', 'foreign key', 'primary key',
                'session id', 'url', 'image', 'path', 'link', 'first name',
                'last name', 'display name', 'full name']
    ID_HINTS = ['_id', 'uuid', 'guid', '_key', '_hash', '_token', 'timestamp',
                '_at', '_ts', '_url', '_image', '_link', '_path']

    def _is_id_name(name: str) -> bool:
        leaf = name.split('.')[-1].lower()
        return leaf == 'id' or leaf.endswith('_id') or any(h in leaf for h in ID_HINTS)

    def _schema_rec(description: str, name: str) -> bool | None:
        """True=recommended, False=not, None=unknown (use cardinality)"""
        if description:
            desc = description.lower()
            if any(w in desc for w in DESC_BAD):
                return False
            return True  # has description and not bad → trust it
        return None  # no description → decide by cardinality

    def _flatten_strings(fields, prefix=""):
        out = []
        for f in fields:
            path = f"{prefix}.{f.name}" if prefix else f.name
            if f.field_type in ("RECORD", "STRUCT"):
                out.extend(_flatten_strings(f.fields, path))
            elif f.field_type == "STRING":
                out.append({"name": path, "description": f.description or ""})
        return out

    suggestions = []
    errors = []

    for entry in tables:
        tbl_full = entry.get("bq_table", "")
        tbl_label = entry.get("label", tbl_full)
        date_field = entry.get("date_field", "partition_date")
        parts = tbl_full.split(".")
        if len(parts) != 3:
            errors.append(f"Skipped {tbl_full}: invalid format")
            continue
        project, dataset, tbl_name = parts

        try:
            client = _get_bq_client(project)
            tbl_param = [_bq.ScalarQueryParameter("tbl", "STRING", tbl_name)]

            # 1. Schema via get_table() — free API call
            tbl_obj = client.get_table(tbl_full)
            string_fields = _flatten_strings(tbl_obj.schema)[:60]
            if not string_fields:
                continue

            # 2. Latest partition with data (free metadata)
            latest_date = None
            try:
                part_rows = _exec(client,
                    f"SELECT partition_id FROM `{project}.{dataset}.INFORMATION_SCHEMA.PARTITIONS` "
                    f"WHERE table_name = @tbl AND partition_id NOT IN ('__NULL__','__UNPARTITIONED__') "
                    f"AND total_rows > 0 ORDER BY partition_id DESC LIMIT 1",
                    tbl_param)
                if part_rows:
                    raw = part_rows[0]["partition_id"]
                    latest_date = f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
            except Exception:
                pass

            if latest_date:
                where = (f"WHERE `{date_field}` BETWEEN "
                         f"DATE_SUB('{latest_date}', INTERVAL 60 DAY) AND '{latest_date}'")
            else:
                where = f"WHERE `{date_field}` >= DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)"

            # 3. APPROX_COUNT_DISTINCT for all string fields in one query
            aliases = [f"_c{i}_" for i in range(len(string_fields))]
            approx_parts = ", ".join(
                f"APPROX_COUNT_DISTINCT({'`' + f['name'] + '`' if '.' not in f['name'] else f['name']}) AS {a}"
                for f, a in zip(string_fields, aliases)
            )
            rows = _exec(client,
                f"SELECT COUNT(*) AS _total, {approx_parts} FROM `{tbl_full}` {where}")

            if not rows or int(rows[0].get("_total") or 0) == 0:
                continue

            row = rows[0]
            total = int(row["_total"] or 1)

            for field, alias in zip(string_fields, aliases):
                distinct = int(row.get(alias) or 0)
                ratio = round(distinct / total, 3)
                desc = field["description"]
                name = field["name"]

                schema_rec = _schema_rec(desc, name)
                if schema_rec is False:
                    continue  # explicit bad signal from description → skip entirely
                if schema_rec is True:
                    rec = distinct > 0  # has description → recommend unless all NULL
                else:
                    # No description: use cardinality
                    if distinct == 0:
                        continue  # all NULL
                    rec = (distinct <= 50 or ratio < 0.1) and not _is_id_name(name)

                if not rec:
                    continue  # don't include non-recommended in suggestions

                suggestions.append({
                    "label": f"{tbl_label} · {name.split('.')[-1]}",
                    "bq_table": tbl_full,
                    "field_name": name,
                    "date_field": date_field,
                    "distinct": distinct,
                    "total": total,
                    "description": desc,
                })

        except Exception as e:
            errors.append(f"{tbl_full}: {e}")
            logger.warning("Discover failed for %s: %s", tbl_full, e)

    return {"suggestions": suggestions, "errors": errors}


@app.get("/api/field-monitors", include_in_schema=False)
async def list_field_monitors(request: Request):
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    monitors = bq.load_field_monitor_configs()
    return monitors

@app.post("/api/field-monitors")
async def add_field_monitor(request: Request):
    user = _user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()
    label       = (body.get("label") or "").strip()
    bq_table    = (body.get("bq_table") or "").strip()
    field_name  = (body.get("field_name") or "").strip()
    date_field  = (body.get("date_field") or "partition_date").strip()
    filter_sql  = (body.get("filter_sql") or "").strip()
    if not label or not bq_table or not field_name:
        raise HTTPException(status_code=400, detail="label, bq_table and field_name are required")
    import uuid
    from google.cloud import bigquery as _bq
    monitor_id = str(uuid.uuid4())[:8]
    sql = (
        f"INSERT INTO `{Config.BQ_FIELD_MONITORS_TABLE}` "
        "(monitor_id, label, bq_table, field_name, date_field, filter_sql, enabled, created_at) "
        "VALUES (@monitor_id, @label, @bq_table, @field_name, @date_field, @filter_sql, TRUE, CURRENT_TIMESTAMP())"
    )
    params = [
        _bq.ScalarQueryParameter("monitor_id",  "STRING", monitor_id),
        _bq.ScalarQueryParameter("label",       "STRING", label),
        _bq.ScalarQueryParameter("bq_table",    "STRING", bq_table),
        _bq.ScalarQueryParameter("field_name",  "STRING", field_name),
        _bq.ScalarQueryParameter("date_field",  "STRING", date_field),
        _bq.ScalarQueryParameter("filter_sql",  "STRING", filter_sql or None),
    ]
    bq.run_update(sql, params)
    logger.info("[admin] %s added field monitor %s (%s)", user["email"], monitor_id, label)
    await _signal_bot_reload()
    return {"ok": True, "monitor_id": monitor_id}

@app.post("/api/field-monitors/{monitor_id}/toggle")
async def toggle_field_monitor(monitor_id: str, request: Request):
    user = _user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    body = await request.json()
    enabled = body.get("enabled")
    if not isinstance(enabled, bool):
        raise HTTPException(status_code=400, detail="enabled must be a boolean")
    from google.cloud import bigquery as _bq
    sql = (
        f"UPDATE `{Config.BQ_FIELD_MONITORS_TABLE}` "
        "SET enabled = @enabled WHERE monitor_id = @monitor_id"
    )
    params = [
        _bq.ScalarQueryParameter("enabled",    "BOOL",   enabled),
        _bq.ScalarQueryParameter("monitor_id", "STRING", monitor_id),
    ]
    rows = bq.run_update(sql, params)
    if rows == 0:
        raise HTTPException(status_code=404, detail="Monitor not found")
    logger.info("[admin] %s toggled field monitor %s -> %s", user["email"], monitor_id, enabled)
    await _signal_bot_reload()
    return {"ok": True}

@app.delete("/api/field-monitors/{monitor_id}")
async def delete_field_monitor(monitor_id: str, request: Request):
    user = _user(request)
    if not user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    from google.cloud import bigquery as _bq
    sql = f"DELETE FROM `{Config.BQ_FIELD_MONITORS_TABLE}` WHERE monitor_id = @monitor_id"
    params = [_bq.ScalarQueryParameter("monitor_id", "STRING", monitor_id)]
    rows = bq.run_update(sql, params)
    if rows == 0:
        raise HTTPException(status_code=404, detail="Monitor not found")
    logger.info("[admin] %s deleted field monitor %s", user["email"], monitor_id)
    await _signal_bot_reload()
    return {"ok": True}

# -> Local dev entrypoint ->

if __name__ == "__main__":

    import uvicorn

    logging.basicConfig(level=logging.INFO)

    uvicorn.run("web:app", host="0.0.0.0", port=8080, reload=True)

