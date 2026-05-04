"""GitHub client – fetch merged PRs near an anomaly date for context."""

import logging
from datetime import date, timedelta

import requests

from config import Config

logger = logging.getLogger(__name__)


def get_merged_prs_on_date(anomaly_date: date) -> list[dict]:
    """Return PRs merged in goalsgame org on anomaly_date (±1 day window).

    Each result dict has: repo, title, author, merged_at, url.
    Returns an empty list if GitHub is not configured or the request fails.
    """
    if not Config.GITHUB_PAT or not Config.GITHUB_ORG:
        return []

    # Search window: day before through day after to catch deploys near midnight
    date_from = (anomaly_date - timedelta(days=1)).isoformat()
    date_to = (anomaly_date + timedelta(days=1)).isoformat()

    query = f"is:pr is:merged merged:{date_from}..{date_to} org:{Config.GITHUB_ORG}"
    url = "https://api.github.com/search/issues"
    headers = {
        "Authorization": f"Bearer {Config.GITHUB_PAT}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    params = {"q": query, "per_page": 20, "sort": "updated"}

    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        resp.raise_for_status()
    except requests.RequestException as e:
        logger.warning("GitHub PR search failed: %s", e)
        return []

    items = resp.json().get("items", [])
    results = []
    for item in items:
        repo_url = item.get("repository_url", "")
        repo_name = repo_url.split("/repos/", 1)[-1] if "/repos/" in repo_url else repo_url
        # Strip org prefix for display: "goalsgame/backend" → "backend"
        org_prefix = Config.GITHUB_ORG + "/"
        if repo_name.startswith(org_prefix):
            repo_name = repo_name[len(org_prefix):]
        results.append({
            "repo": repo_name,
            "title": item.get("title", ""),
            "author": item.get("user", {}).get("login", ""),
            "merged_at": (item.get("pull_request") or {}).get("merged_at", ""),
            "url": item.get("html_url", ""),
        })
    return results
