#!/usr/bin/env bash
# Install optional inference dependencies without modifying the analysis venv.
set -euo pipefail
repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
rabbit_mode="${1:-gpu}"
case "$rabbit_mode" in cpu|gpu) ;; *) echo 'Usage: scripts/setup_rabbit.sh [gpu|cpu]' >&2; exit 2 ;; esac
rabbit_env="${RDF_RABBIT_VENV:-$repo_dir/.venv-rabbit}"
rabbit_base_python="${RDF_RABBIT_BASE_PYTHON:-$repo_dir/.venv/bin/python}"
if [ ! -x "$rabbit_base_python" ]; then rabbit_base_python=python3; fi
"$rabbit_base_python" -m venv "$rabbit_env"
"$rabbit_env/bin/python" -m pip install -r "$repo_dir/requirements/rabbit-$rabbit_mode.txt"
echo "Rabbit interpreter: $rabbit_env/bin/python"
echo 'Set RDF_RABBIT_PYTHON if using a non-default environment.'
