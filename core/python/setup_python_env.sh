#!/usr/bin/env bash
# core/python/setup_python_env.sh
#
# Creates a portable Python environment tarball for use in remote condor jobs.
#
# The tarball collects Python dependencies in a single directory
# (using pip --target) that can be unpacked on any worker node.  The worker setup
# adds the archive to the Python, executable, native
# library, and ROOT include search paths. Workers provide compatible Python,
# ROOT, and system or site libraries.
#
# USAGE
# -----
#   # 1. Enter the container you will be using for remote jobs, e.g.:
#   cmssw-el9 --command-to-run "bash core/python/setup_python_env.sh [OPTIONS]"
#
#   # 2. Or run directly if you are already inside the container:
#   bash core/python/setup_python_env.sh [OPTIONS]
#
# OPTIONS
#   -o, --output FILE    Output tarball path (default: python_env.tar.gz)
#   --extras "PKG ..."   Additional pip packages to install (space-separated)
#   RDFANALYZER_PYTHON_MODULE_DIR overrides the default build/python search directory.
#   --no-rdfanalyzer     Skip copying the rdfanalyzer Python module
#   --python PYTHON      Python executable to use (default: python3)
#   -h, --help           Show this help message
#
# EXAMPLES
#   # Basic: package only the rdfanalyzer bindings + runtime deps
#   bash core/python/setup_python_env.sh
#
#   # With extra packages (e.g. numpy, uproot)
#   bash core/python/setup_python_env.sh --extras "numpy uproot"
#
#   # Specify a custom output path
#   bash core/python/setup_python_env.sh --output /eos/user/u/username/python_env.tar.gz
#
#   # Then set options.python_env in the production spec.
#
# HOW IT WORKS
# ------------
# 1. A staging directory (python_packages/) is created in a temporary location.
# 2. Required packages are installed there with `pip install --target`.
# 3. The matching RDFAnalyzer bindings, Python API, and build-local native
#    libraries are copied into the directory.
# 4. The directory is archived into a .tar.gz tarball.
# 5. On each remote worker node the runscript:
#      tar -xzf python_env.tar.gz -C _python_env
#      export PYTHONPATH="$PWD/_python_env:..."
#      export LD_LIBRARY_PATH="$PWD/_python_env:..."

set -euo pipefail

OUTPUT="python_env.tar.gz"
EXTRA_PACKAGES=""
INCLUDE_RDFANALYZER=true
PYTHON="${PYTHON:-python3}"

# ---------- parse arguments -------------------------------------------------
while [[ $# -gt 0 ]]; do
    case $1 in
        -o|--output)
            OUTPUT="$2"; shift 2 ;;
        --extras)
            EXTRA_PACKAGES="$2"; shift 2 ;;
        --no-rdfanalyzer)
            INCLUDE_RDFANALYZER=false; shift ;;
        --python)
            PYTHON="$2"; shift 2 ;;
        -h|--help)
            sed -n '3,50p' "$0" | grep '^#' | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *)
            echo "Unknown argument: $1" >&2; exit 1 ;;
    esac
done

# ---------- locate repository root ------------------------------------------
SCRIPT_DIR="$(CDPATH= cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/controller/CMakeLists.txt" ]; then
    REPO_ROOT="$(CDPATH= cd "$SCRIPT_DIR/../.." && pwd)"
    DEFAULT_MODULE_DIR="$REPO_ROOT/build/python"
else
    REPO_ROOT="$(CDPATH= cd "$SCRIPT_DIR/.." && pwd)"
    DEFAULT_MODULE_DIR="$SCRIPT_DIR"
fi

# ---------- create a temporary staging directory ----------------------------
STAGING="$(mktemp -d)"
trap 'rm -rf -- "$STAGING"' EXIT
PACKAGES_DIR="$STAGING/python_packages"
mkdir -p "$PACKAGES_DIR"

echo "==> Staging directory: $STAGING"
echo "==> Python:            $("$PYTHON" --version)"
echo ""

# ---------- install worker packages ----------------------------------------
packages=(requests PyYAML)
if [ -n "$EXTRA_PACKAGES" ]; then
    read -r -a extra_specs <<< "$EXTRA_PACKAGES"
    packages+=("${extra_specs[@]}")
fi
echo "==> Installing worker packages: ${packages[*]}"
"$PYTHON" -m pip install --quiet --target="$PACKAGES_DIR" "${packages[@]}"

# ---------- copy rdfanalyzer Python bindings --------------------------------
if [ "$INCLUDE_RDFANALYZER" = true ]; then
    MODULE_DIR="${RDFANALYZER_PYTHON_MODULE_DIR:-$DEFAULT_MODULE_DIR}"
    PYTHONPATH="$SCRIPT_DIR:$PACKAGES_DIR${PYTHONPATH:+:$PYTHONPATH}" \
        "$PYTHON" - "$MODULE_DIR" "$PACKAGES_DIR" "$REPO_ROOT" <<'PYTHON'
import sys

from rdfproduction.python_runtime import stage_python_bindings

module = stage_python_bindings(*sys.argv[1:])
if module:
    print(f"==> Staged RDFAnalyzer Python API and local native runtime: {module}")
else:
    print(f"==> WARNING: No matching rdfanalyzer module in {sys.argv[1]}")
    print("            Build the project first with:  bash build.sh")
    print("            Then re-run this script.")
PYTHON
fi

# ---------- create the tarball ----------------------------------------------
OUTPUT_ABS="$(realpath -m "$OUTPUT")"
echo ""
echo "==> Creating tarball: $OUTPUT_ABS"
# tar the contents of the packages directory directly (no subdirectory wrapper)
# so that after  tar -xzf <tarball> -C _python_env  the packages are at
# _python_env/ directly (PYTHONPATH=$PWD/_python_env).
tar -czf "$OUTPUT_ABS" -C "$PACKAGES_DIR" .

echo ""
echo "Done!  Python environment tarball: $OUTPUT_ABS"
echo ""
echo "Set options.python_env to $OUTPUT_ABS in the production spec."
