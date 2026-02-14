#!/usr/bin/env bash
set -euo pipefail

# Start multiple self-hosted runner containers (default: 4)
# Usage: ./startupContainer.sh [--force|-f] [N]
#    --force, -f   : forcibly remove and recreate any existing same-named containers
#    N             : number of containers to start (default: 4)
# Example: ./startupContainer.sh --force 4

# Defaults
IMAGE=${IMAGE:-gh-runner}
OWNER=${GITHUB_OWNER:-brkronheim}
REPO=${GITHUB_REPO:-RDFAnalyzerCore}
FORCE=0
NUM_ARG=""

# Parse args (simple): accept --force/-f and an optional numeric N
while [[ $# -gt 0 ]]; do
  case "$1" in
    -f|--force)
      FORCE=1
      shift
      ;;
    -h|--help)
      echo "Usage: $0 [--force|-f] [N]" && exit 0
      ;;
    *)
      # first non-option argument is treated as NUM
      if [[ -z "$NUM_ARG" ]]; then
        NUM_ARG="$1"
        shift
      else
        echo "Unknown argument: $1" >&2
        exit 1
      fi
      ;;
  esac
done

NUM=${NUM_ARG:-4}

if [ -z "${GITHUB_PAT:-}" ] || [ "${GITHUB_PAT}" = "GITHUB_PAT" ]; then
  echo "WARNING: GITHUB_PAT is not set or uses the placeholder value. Set GITHUB_PAT in the environment before running."
fi

if [ "$FORCE" -eq 1 ]; then
  echo "[startupContainer] FORCE mode enabled — existing containers will be removed and recreated"
fi

for i in $(seq 1 "$NUM"); do
  NAME="gh-runner-${i}"

  if docker ps -a --format '{{.Names}}' | grep -qx "$NAME"; then
    if [ "$FORCE" -eq 1 ]; then
      echo "Container $NAME exists — removing (force)"
      docker rm -f "$NAME" >/dev/null 2>&1 || true
    else
      echo "Container $NAME already exists — skipping (remove with: docker rm -f $NAME)"
      continue
    fi
  fi

  echo "Starting container: $NAME"
  docker run -d \
    --name "$NAME" \
    -e GITHUB_PAT="${GITHUB_PAT:-}" \
    -e GITHUB_OWNER="$OWNER" \
    -e GITHUB_REPO="$REPO" \
    --restart unless-stopped \
    --tmpfs /tmp \
    --tmpfs /home/runner/_work:rw,uid=1000,gid=1000,exec \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    "$IMAGE"

  sleep 1
done


