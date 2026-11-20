#!/bin/bash
set -euo pipefail

# Register a fresh ephemeral runner after a completed job or a host reboot.
: "${GITHUB_PAT:?Set GITHUB_PAT to a token with repository runner administration access}"
: "${GITHUB_OWNER:?Set GITHUB_OWNER}"
: "${GITHUB_REPO:?Set GITHUB_REPO}"

if ! REG_TOKEN=$(curl --fail --silent --show-error --retry 5 --max-time 30 -X POST \
  -H "Authorization: Bearer $GITHUB_PAT" \
  -H "Accept: application/vnd.github+json" \
  "https://api.github.com/repos/$GITHUB_OWNER/$GITHUB_REPO/actions/runners/registration-token" \
  | jq -er '.token | strings | select(length > 0)'); then
  echo "[runner] Failed to fetch a GitHub registration token" >&2
  exit 1
fi
unset GITHUB_PAT

# The workspace is a Docker tmpfs. Replace the previous registration for this name.
rm -f .runner .credentials .credentials_rsaparams
./config.sh \
  --url "https://github.com/$GITHUB_OWNER/$GITHUB_REPO" \
  --token "$REG_TOKEN" \
  --unattended \
  --ephemeral \
  --replace

exec ./run.sh
