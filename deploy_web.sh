#!/bin/bash
# Manuellt deployskript (används lokalt / som fallback).
# I produktion körs deployment automatiskt via GitHub Actions.
set -e
cd "$(dirname "$0")"

get_env() { grep -E "^$1=" .env | head -1 | tr -d '\r' | cut -d= -f2-; }

ALLOWED_DOMAIN=$(get_env ALLOWED_DOMAIN)

# Hemligheter hämtas från GCP Secret Manager (--set-secrets).
# Lägg ALDRIG lösenord eller tokens direkt i detta skript.

gcloud run deploy mimir-web \
  --project=lia-project-sandbox-deletable \
  --region=europe-west4 \
  --dockerfile=Dockerfile.web \
  --source=. \
  --allow-unauthenticated \
  --port=8080 \
  --min-instances=0 \
  --max-instances=5 \
  --set-secrets="SESSION_SECRET=mimir-session-secret:latest,GOOGLE_CLIENT_ID=mimir-google-client-id:latest,GOOGLE_CLIENT_SECRET=mimir-google-client-secret:latest,STEEP_API_TOKEN=mimir-steep-api-token:latest" \
  --set-env-vars="GCP_PROJECT_ID=lia-project-sandbox-deletable,ALLOWED_DOMAIN=${ALLOWED_DOMAIN},BOT_INTERNAL_URL=http://localhost:8081"
