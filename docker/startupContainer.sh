#!/usr/bin/env bash
set -euo pipefail

# Start multiple self-hosted runner containers (default: 4)
# Usage: bash startupContainer.sh [--force|-f] [N]
#    --force, -f   : forcibly remove and recreate any existing same-named containers
#    N             : number of containers to start (default: 4)
# Example: bash startupContainer.sh --force 4

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
      echo "Usage: bash $0 [--force|-f] [N]" && exit 0
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

if [[ ! "$NUM" =~ ^[1-9][0-9]*$ ]]; then
  echo "Runner count must be a positive integer" >&2
  exit 1
fi

existing_names=$(docker ps -a --format '{{.Names}}')

for i in $(seq 1 "$NUM"); do
  NAME="gh-runner-${i}"

  if grep -Fxq "$NAME" <<< "$existing_names" && [ "$FORCE" -eq 0 ]; then
    docker update --restart always "$NAME" >/dev/null
    docker start "$NAME"
    continue
  fi

  if [[ -z "${GITHUB_PAT:-}" || "$GITHUB_PAT" == "GITHUB_PAT" ]]; then
    echo "Set GITHUB_PAT in the environment to create runner containers" >&2
    exit 1
  fi
  if grep -Fxq "$NAME" <<< "$existing_names"; then
    docker image inspect "$IMAGE" >/dev/null
    docker rm -f "$NAME" >/dev/null
  fi

  echo "Starting container: $NAME"
  docker run -d \
    --name "$NAME" \
    -e GITHUB_PAT \
    -e GITHUB_OWNER="$OWNER" \
    -e GITHUB_REPO="$REPO" \
    --restart always \
    --tmpfs /tmp \
    --tmpfs /home/runner/_work:rw,uid=1000,gid=1000,exec \
    --cap-drop ALL \
    --security-opt no-new-privileges \
    "$IMAGE"

  sleep 1
done


