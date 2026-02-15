#!/bin/bash
set -euo pipefail

# Auto re-registration helper for ephemeral GitHub Actions runner.
# - If a stale local runner config is found (container reboot), remove it
#   and re-run `config.sh` so the runner can accept jobs again.
# - Best-effort `./config.sh remove` is attempted before local cleanup.

cleanup_local_config() {
  echo "[runner] cleaning local runner files"
  # stop any service (best-effort)
  if [ -x ./svc.sh ]; then
    ./svc.sh stop || true
  fi
  rm -f .runner .credentials .service .credentials_rsaparams || true
  rm -rf _work _diag || true
}

# If a previous configuration exists, try to remove it and wipe local state.
if [ -f .runner ] || [ -f .credentials ] || [ -d _work ]; then
  echo "[runner] detected existing configuration — attempting cleanup"
  # Try to unregister from GitHub (best-effort). If this fails we still remove
  # local files to allow fresh registration.
  if ./config.sh remove --unattended 2>/dev/null; then
    echo "[runner] config.sh remove succeeded"
  else
    echo "[runner] config.sh remove failed or not applicable; proceeding to wipe local state"
  fi
  cleanup_local_config
fi

# Obtain a fresh registration token from GitHub
REG_TOKEN=$(curl -s -X POST \
  -H "Authorization: token $GITHUB_PAT" \
  -H "Accept: application/vnd.github+json" \
  https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token \
  | jq -r .token)

if [ -z "$REG_TOKEN" ] || [ "$REG_TOKEN" = "null" ]; then
  echo "[runner] Failed to fetch runner token"
  exit 1
fi

# Configure the runner (ephemeral) — --replace is safe because we removed local files above
./config.sh \
  --url "https://github.com/$GITHUB_OWNER/$GITHUB_REPO" \
  --token "$REG_TOKEN" \
  --unattended \
  --ephemeral \
  --replace

# Start the runner (this blocks)
./run.sh
