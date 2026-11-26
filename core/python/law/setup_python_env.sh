#!/usr/bin/env bash
# Compatibility entrypoint for archives that also ship the LAW worker packages.
set -euo pipefail
script_dir="$(CDPATH= cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
extras="law luigi"
args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --extras)
            extras+=" $2"
            shift 2
            ;;
        *)
            args+=("$1")
            shift
            ;;
    esac
done
exec bash "$script_dir/../setup_python_env.sh" --extras "$extras" "${args[@]}"
