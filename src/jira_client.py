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


# ── Keyword mapping: metric label → JIRA search terms ─────────────────────────

_METRIC_KEYWORDS: dict[str, list[str]] = {
    "MM": ["matchmaking", "MM"],
    "Matches": ["match", "matchmaking"],
    "Crash": ["crash", "ANR", "stability"],
    "FTUE": ["FTUE", "onboarding", "tutorial", "first time"],
    "Active Users": ["DAU", "active users", "login"],
    "Minutes": ["playtime", "session", "minutes"],
    "Sessions": ["session", "login"],
    "Coverage": ["coverage", "game mode"],
    "Retention": ["retention", "D1", "D7", "D30"],
    "Currency": ["currency", "economy", "coins"],
    "Challenges": ["challenge", "quest", "mission"],
    "Packs": ["pack", "store", "purchase"],
    "dbt": ["dbt", "pipeline", "data"],
    "First Opens": ["install", "first open", "acquisition"],
}


def _keywords_for_metric(metric_label: str) -> list[str]:
    """Extract JIRA search keywords from a metric label."""
    keywords = []
    for prefix, terms in _METRIC_KEYWORDS.items():
        if prefix.lower() in metric_label.lower():
            keywords.extend(terms)
    # Fallback: use the first 2 words of the label
    if not keywords:
        words = metric_label.split()[:2]
        keywords = [" ".join(words)]
    return keywords


def search_related_tickets(metric_label: str, anomaly_date: date, max_results: int = 5) -> list[dict]:
    """Search JIRA for recent bugs/issues related to a metric.

    Searches for tickets created or updated within ±3 days of the anomaly date
    with keywords matching the metric name.

    Returns list of dicts with: key, summary, status, created, issue_type, priority.
    """
    if not Config.JIRA_BASE_URL or not Config.JIRA_EMAIL or not Config.JIRA_API_TOKEN:
        return []

    keywords = _keywords_for_metric(metric_label)
    text_query = " OR ".join(f'text ~ "{kw}"' for kw in keywords)
    window_start = (anomaly_date - timedelta(days=3)).isoformat()
    window_end = (anomaly_date + timedelta(days=3)).isoformat()

    jql = (
        f"({text_query}) "
        f"AND (created >= '{window_start}' OR updated >= '{window_start}') "
        f"AND (created <= '{window_end}' OR updated <= '{window_end}') "
        f"ORDER BY created DESC"
    )

    url = f"{Config.JIRA_BASE_URL.rstrip('/')}/rest/api/3/search/jql"
    params = {
        "jql": jql,
        "maxResults": max_results,
        "fields": "summary,status,created,issuetype,priority",
    }

    try:
        response = requests.get(url, headers=_auth_header(), params=params, timeout=10)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.warning("Jira ticket search failed: %s", e)
        return []

    issues = response.json().get("issues", [])
    results = []
    for issue in issues:
        fields = issue.get("fields", {})
        results.append({
            "key": issue.get("key", ""),
            "summary": fields.get("summary", ""),
            "status": (fields.get("status") or {}).get("name", ""),
            "created": (fields.get("created") or "")[:10],
            "issue_type": (fields.get("issuetype") or {}).get("name", ""),
            "priority": (fields.get("priority") or {}).get("name", ""),
        })
    return results


def format_ticket_context(tickets: list[dict]) -> str:
    """Format tickets into a string for inclusion in the LLM prompt."""
    if not tickets:
        return ""
    lines = []
    for t in tickets:
        priority_tag = f" [{t['priority']}]" if t["priority"] else ""
        lines.append(f"  - {t['key']}: {t['summary']} ({t['issue_type']}, {t['status']}, created {t['created']}){priority_tag}")
    return "\n".join(lines)
