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
    """Return column names for a given BQ table via INFORMATION_SCHEMA, using user's OAuth token."""
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not table or "`" in table or ";" in table or len(table) > 300:
        raise HTTPException(status_code=400, detail="Invalid table name")
    parts = table.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail="Expected project.dataset.table")
    project, dataset, tbl_name = parts
    from google.cloud import bigquery as _bq
    from google.oauth2.credentials import Credentials
    params = [_bq.ScalarQueryParameter("tbl", "STRING", tbl_name)]
    try:
        access_token = request.session.get("access_token", "")

        def _run_sql(sql):
            if access_token and _is_cloud:
                from google.oauth2.credentials import Credentials as _Creds
                creds = _Creds(token=access_token)
                client = _bq.Client(project=project, credentials=creds)
                job_config = _bq.QueryJobConfig(query_parameters=params)
                return [dict(r) for r in client.query(sql, job_config=job_config).result()]
            return bq.run_query(sql, params=params, max_rows=500)

        # Top-level columns (try with description, fall back without)
        try:
            sql = (
                f"SELECT column_name, data_type, description "
                f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = @tbl ORDER BY ordinal_position"
            )
            top_rows = _run_sql(sql)
            top_cols = [{"name": r["column_name"], "type": r["data_type"], "description": r.get("description") or ""} for r in top_rows]
        except Exception:
            sql = (
                f"SELECT column_name, data_type "
                f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = @tbl ORDER BY ordinal_position"
            )
            top_rows = _run_sql(sql)
            top_cols = [{"name": r["column_name"], "type": r["data_type"], "description": ""} for r in top_rows]

        # Nested STRUCT sub-fields (STRING only) via COLUMN_FIELD_PATHS
        try:
            sql = (
                f"SELECT field_path "
                f"FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
                f"WHERE table_name = @tbl AND data_type = 'STRING' AND field_path != column_name "
                f"ORDER BY field_path"
            )
            nested_rows = _run_sql(sql)
            nested_cols = [{"name": r["field_path"], "type": "STRING", "description": ""} for r in nested_rows]
        except Exception:
            nested_cols = []

        return top_cols + nested_cols
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Could not fetch schema: {e}")

@app.get("/api/bq-table-columns/analyze", include_in_schema=False)
async def analyze_bq_table_columns(request: Request, table: str, date_field: str = "partition_date"):
    """Run APPROX_COUNT_DISTINCT on STRING columns to estimate uniqueness ratio."""
    if not _user(request):
        raise HTTPException(status_code=401, detail="Not authenticated")
    if not table or "`" in table or ";" in table or len(table) > 300:
        raise HTTPException(status_code=400, detail="Invalid table name")
    parts = table.split(".")
    if len(parts) != 3:
        raise HTTPException(status_code=400, detail="Expected project.dataset.table")
    project, dataset, tbl_name = parts
    from google.cloud import bigquery as _bq
    from google.oauth2.credentials import Credentials

    access_token = request.session.get("access_token", "")
    use_user_creds = bool(access_token and _is_cloud)

    def _exec(sql, params=None):
        if use_user_creds:
            creds = Credentials(token=access_token)
            client = _bq.Client(project=project, credentials=creds)
            cfg = _bq.QueryJobConfig(query_parameters=params or [])
            return [dict(r) for r in client.query(sql, job_config=cfg).result()]
        return bq.run_query(sql, params=params or [], max_rows=500)

    try:
        tbl_param = [_bq.ScalarQueryParameter("tbl", "STRING", tbl_name)]
        # Top-level STRING columns
        string_cols = [
            r["column_name"]
            for r in _exec(
                f"SELECT column_name FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS` "
                f"WHERE table_name = @tbl AND data_type = 'STRING' ORDER BY ordinal_position",
                tbl_param,
            )
        ]
        # Nested STRUCT sub-fields that are STRING
        try:
            nested_cols = [
                r["field_path"]
                for r in _exec(
                    f"SELECT field_path FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS` "
                    f"WHERE table_name = @tbl AND data_type = 'STRING' AND field_path != column_name "
                    f"ORDER BY field_path",
                    tbl_param,
                )
            ]
        except Exception:
            nested_cols = []
        string_cols = string_cols + nested_cols
        if not string_cols:
            return {}

        # Use safe positional aliases to avoid reserved-word issues
        aliases = [f"_c{i}_" for i in range(len(string_cols))]
        approx_parts = ", ".join(
            # Nested paths (e.g. metadata.event_type) must NOT use backticks
            f"APPROX_COUNT_DISTINCT({'`' + c + '`' if '.' not in c else c}) AS {a}"
            for c, a in zip(string_cols, aliases)
        )

        rows = None
        for with_filter in (True, False):
            try:
                where = (
                    f"WHERE `{date_field}` = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)"
                    if with_filter else ""
                )
                rows = _exec(f"SELECT COUNT(*) AS _total, {approx_parts} FROM `{table}` {where}")
                break
            except Exception:
                if not with_filter:
                    raise

        if not rows:
            return {}

        row = rows[0]
        total = int(row.get("_total") or 1)
        return {
            c: {
                "distinct": int(row.get(a) or 0),
                "total": total,
                "ratio": round(int(row.get(a) or 0) / total, 3),
            }
            for c, a in zip(string_cols, aliases)
        }
    except Exception as e:
        logger.warning("Column analysis failed for %s: %s", table, e)
        raise HTTPException(status_code=500, detail=str(e))

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

