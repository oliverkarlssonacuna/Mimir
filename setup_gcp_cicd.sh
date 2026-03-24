#!/bin/bash
# =============================================================================
# setup_gcp_cicd.sh
# Kör detta EN GÅNG för att konfigurera GCP-infrastruktur för GitHub Actions.
#
# Usage: ./setup_gcp_cicd.sh <github-owner/repo-name>
# Exempel: ./setup_gcp_cicd.sh oliverbolano/mimir
# =============================================================================
set -euo pipefail

GITHUB_REPO="${1:?Användning: $0 <github-owner/repo-name>  (t.ex. oliverbolano/mimir)}"

# ── Konfiguration ─────────────────────────────────────────────────────────────
PROJECT_ID="lia-project-sandbox-deletable"
REGION="europe-west4"
AR_REPO="mimir"                       # Artifact Registry repository-namn
SA_NAME="github-actions-deploy"       # Service Account-namn
POOL_NAME="github-pool"               # Workload Identity Pool
PROVIDER_NAME="github-provider"       # Workload Identity Provider
# ──────────────────────────────────────────────────────────────────────────────

SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"

echo "================================================================"
echo " Konfigurerar GCP CI/CD"
echo "  Projekt:     $PROJECT_ID"
echo "  GitHub-repo: $GITHUB_REPO"
echo "================================================================"
echo ""

# 1. Aktivera nödvändiga API:er
echo ">>> [1/7] Aktiverar GCP API:er..."
gcloud services enable \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  iam.googleapis.com \
  iamcredentials.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  --project="$PROJECT_ID"

# 2. Skapa Artifact Registry Docker-repo
echo ""
echo ">>> [2/7] Skapar Artifact Registry-repository '$AR_REPO'..."
gcloud artifacts repositories create "$AR_REPO" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT_ID" \
  --description="Mimir Docker images" 2>/dev/null \
  || echo "    (finns redan, hoppar över)"

# 3. Skapa Service Account för GitHub Actions
echo ""
echo ">>> [3/7] Skapar Service Account '$SA_NAME'..."
gcloud iam service-accounts create "$SA_NAME" \
  --project="$PROJECT_ID" \
  --display-name="GitHub Actions Deploy" 2>/dev/null \
  || echo "    (finns redan, hoppar över)"

# 4. Tilldela roller
echo ""
echo ">>> [4/7] Tilldelar IAM-roller till Service Account..."
for ROLE in \
  roles/run.admin \
  roles/iam.serviceAccountUser \
  roles/artifactregistry.writer \
  roles/secretmanager.secretAccessor; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="$ROLE" \
    --quiet
  echo "    ✓ $ROLE"
done

# 5. Sätt upp Workload Identity Federation
echo ""
echo ">>> [5/7] Skapar Workload Identity Pool & Provider..."

gcloud iam workload-identity-pools create "$POOL_NAME" \
  --project="$PROJECT_ID" \
  --location=global \
  --display-name="GitHub Actions Pool" 2>/dev/null \
  || echo "    Pool finns redan"

POOL_RESOURCE=$(gcloud iam workload-identity-pools describe "$POOL_NAME" \
  --project="$PROJECT_ID" \
  --location=global \
  --format="value(name)")

gcloud iam workload-identity-pools providers create-oidc "$PROVIDER_NAME" \
  --project="$PROJECT_ID" \
  --location=global \
  --workload-identity-pool="$POOL_NAME" \
  --display-name="GitHub OIDC Provider" \
  --attribute-mapping="google.subject=assertion.sub,attribute.repository=assertion.repository,attribute.actor=assertion.actor,attribute.ref=assertion.ref" \
  --attribute-condition="assertion.repository=='${GITHUB_REPO}'" \
  --issuer-uri="https://token.actions.githubusercontent.com" 2>/dev/null \
  || echo "    Provider finns redan"

PROVIDER_RESOURCE=$(gcloud iam workload-identity-pools providers describe "$PROVIDER_NAME" \
  --project="$PROJECT_ID" \
  --location=global \
  --workload-identity-pool="$POOL_NAME" \
  --format="value(name)")

# 6. Koppla Service Account till WIF (bara för detta repo)
echo ""
echo ">>> [6/7] Kopplar Service Account till WIF för repo '$GITHUB_REPO'..."
gcloud iam service-accounts add-iam-policy-binding "$SA_EMAIL" \
  --project="$PROJECT_ID" \
  --role="roles/iam.workloadIdentityUser" \
  --member="principalSet://iam.googleapis.com/${POOL_RESOURCE}/attribute.repository/${GITHUB_REPO}"

# 7. Skapa Secret Manager-hemligheter (tomma skal – fyll i värden efteråt)
echo ""
echo ">>> [7/7] Skapar Secret Manager-hemligheter..."

create_secret_if_missing() {
  local SECRET_NAME="$1"
  gcloud secrets describe "$SECRET_NAME" --project="$PROJECT_ID" &>/dev/null \
    && echo "    (${SECRET_NAME} finns redan)" \
    || gcloud secrets create "$SECRET_NAME" \
         --project="$PROJECT_ID" \
         --replication-policy=automatic \
    && echo "    ✓ Skapade $SECRET_NAME"
}

create_secret_if_missing "mimir-session-secret"
create_secret_if_missing "mimir-google-client-id"
create_secret_if_missing "mimir-google-client-secret"
create_secret_if_missing "mimir-steep-api-token"

# ── Resultat ──────────────────────────────────────────────────────────────────
echo ""
echo "================================================================"
echo " KLART! Lägg till dessa i GitHub Secrets:"
echo " (Settings → Secrets and variables → Actions → New secret)"
echo "================================================================"
echo ""
echo "  Namn:                WIF_PROVIDER"
echo "  Värde:               ${PROVIDER_RESOURCE}"
echo ""
echo "  Namn:                WIF_SERVICE_ACCOUNT"
echo "  Värde:               ${SA_EMAIL}"
echo ""
echo "================================================================"
echo " NÄSTA STEG: Lägg värden i Secret Manager"
echo "================================================================"
echo ""
echo " Kör dessa kommandon (byt ut platshhållarna):"
echo ""
echo "  echo -n 'DITT_SESSION_SECRET' | \\"
echo "    gcloud secrets versions add mimir-session-secret --data-file=- --project=$PROJECT_ID"
echo ""
echo "  echo -n 'DIN_GOOGLE_CLIENT_ID' | \\"
echo "    gcloud secrets versions add mimir-google-client-id --data-file=- --project=$PROJECT_ID"
echo ""
echo "  echo -n 'DIN_GOOGLE_CLIENT_SECRET' | \\"
echo "    gcloud secrets versions add mimir-google-client-secret --data-file=- --project=$PROJECT_ID"
echo ""
echo "  echo -n 'DIN_STEEP_API_TOKEN' | \\"
echo "    gcloud secrets versions add mimir-steep-api-token --data-file=- --project=$PROJECT_ID"
echo ""
echo "================================================================"
echo " GitHub Variables (Settings → Variables → Actions):"
echo "================================================================"
echo ""
echo "  ALLOWED_DOMAIN   = din Google Workspace-domän (t.ex. dittforetag.com)"
echo "  BOT_INTERNAL_URL = lämna tom tills vidare (sätts när bot deployats)"
echo ""
