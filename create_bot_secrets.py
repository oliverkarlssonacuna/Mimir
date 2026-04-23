#!/usr/bin/env python3
"""Creates GCP Secret Manager secrets for the Discord bot."""
import pathlib
import subprocess
import sys

PROJECT = "lia-project-sandbox-deletable"

_ENV_FILE = pathlib.Path(__file__).parent / ".env"

# Read .env
env = {}
with open(_ENV_FILE) as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v

token = env.get("DISCORD_BOT_TOKEN", "")
channel = env.get("DISCORD_ALERT_CHANNEL_ID", "")
error_channel = env.get("DISCORD_ERROR_CHANNEL_ID", "")

print(f"Token length: {len(token)}, Channel ID: {channel}, Error Channel ID: {error_channel}")

def create_or_update_secret(name, value):
    # Try create first
    result = subprocess.run(
        ["gcloud", "secrets", "create", name, "--project", PROJECT, "--data-file=-"],
        input=value.encode(),
        capture_output=True,
    )
    if result.returncode == 0:
        print(f"  Created secret: {name}")
    else:
        # Already exists - add new version
        result = subprocess.run(
            ["gcloud", "secrets", "versions", "add", name, "--project", PROJECT, "--data-file=-"],
            input=value.encode(),
            capture_output=True,
        )
        if result.returncode == 0:
            print(f"  Updated secret: {name}")
        else:
            print(f"  ERROR for {name}: {result.stderr.decode()}")
            sys.exit(1)

create_or_update_secret("mimir-discord-bot-token", token)
create_or_update_secret("mimir-discord-alert-channel-id", channel)
create_or_update_secret("mimir-discord-error-channel-id", error_channel)
print("Done!")
