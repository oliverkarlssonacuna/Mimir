"""
Jira client – fetches release versions to provide context for anomaly analysis.
"""

import base64
import logging
from datetime import date, datetime, timedelta

import requests

from config import Config

logger = logging.getLogger(__name__)

_WINDOW_DAYS = 7  # Look for releases within ±7 days of the anomaly date


def _auth_header() -> dict[str, str]:
    credentials = f"{Config.JIRA_EMAIL}:{Config.JIRA_API_TOKEN}"
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}", "Accept": "application/json"}


def get_releases_near_date(anomaly_date: date, project_key: str) -> list[dict]:
    """Return Jira releases within ±WINDOW_DAYS of anomaly_date for the given project.

    Each result dict has: name, releaseDate, released, description.
    Returns an empty list if Jira is not configured or the request fails.
    """
    if not Config.JIRA_BASE_URL or not Config.JIRA_EMAIL or not Config.JIRA_API_TOKEN:
        return []

    url = f"{Config.JIRA_BASE_URL.rstrip('/')}/rest/api/3/project/{project_key}/versions"
    try:
        response = requests.get(url, headers=_auth_header(), timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Jira versions request failed: %s", e)
        return []

    versions = response.json()
    window_start = anomaly_date - timedelta(days=_WINDOW_DAYS)
    window_end = anomaly_date + timedelta(days=_WINDOW_DAYS)

    nearby = []
    for v in versions:
        release_date_str = v.get("releaseDate")
        if not release_date_str:
            continue
        try:
            release_date = date.fromisoformat(release_date_str)
        except ValueError:
            continue
        if window_start <= release_date <= window_end:
            nearby.append({
                "name": v.get("name", ""),
                "releaseDate": release_date_str,
                "released": v.get("released", False),
                "description": v.get("description", ""),
            })

    return sorted(nearby, key=lambda v: v["releaseDate"])


def format_release_context(releases: list[dict], anomaly_date: date) -> str:
    """Format releases into a human-readable string for inclusion in the LLM prompt."""
    if not releases:
        return ""

    lines = [f"Jira releases near the anomaly date ({anomaly_date}):"]
    for r in releases:
        status = "released" if r["released"] else "planned"
        desc = f" – {r['description']}" if r["description"] else ""
        lines.append(f"  - {r['name']} ({status}, {r['releaseDate']}){desc}")

    return "\n".join(lines)
