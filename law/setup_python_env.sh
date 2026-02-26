#!/usr/bin/env bash
# law/setup_python_env.sh
#
# Creates a portable Python environment tarball for use in remote condor jobs.
#
# The tarball packages Python dependencies into a self-contained directory
# (using pip --target) that can be unpacked on any worker node.  The law
# tasks automatically set PYTHONPATH / PATH / LD_LIBRARY_PATH after
# unpacking, so the packaged packages (including rdfanalyzer Python bindings)
# are available without any additional setup.
#
# USAGE
# -----
#   # 1. Enter the container you will be using for remote jobs, e.g.:
#   cmssw-el9 --command-to-run "bash law/setup_python_env.sh [OPTIONS]"
#
#   # 2. Or run directly if you are already inside the container:
#   bash law/setup_python_env.sh [OPTIONS]
#
# OPTIONS
#   -o, --output FILE    Output tarball path (default: python_env.tar.gz)
#   --extras "PKG ..."   Additional pip packages to install (space-separated)
#   --no-rdfanalyzer     Skip copying the rdfanalyzer Python module
#   --python PYTHON      Python executable to use (default: python3)
#   -h, --help           Show this help message
#
# EXAMPLES
#   # Basic: package only the rdfanalyzer bindings + runtime deps
#   bash law/setup_python_env.sh
#
#   # With extra packages (e.g. numpy, uproot)
#   bash law/setup_python_env.sh --extras "numpy uproot"
#
#   # Specify a custom output path
#   bash law/setup_python_env.sh --output /eos/user/u/username/python_env.tar.gz
#
#   # Then pass to a law task:
#   law run BuildNANOSubmission ... --python-env python_env.tar.gz
#
# HOW IT WORKS
# ------------
# 1. A staging directory (python_packages/) is created in a temporary location.
# 2. Required packages are installed there with `pip install --target`.
# 3. The rdfanalyzer.so shared module (if built) is copied into the directory.
# 4. The directory is archived into a .tar.gz tarball.
# 5. On each remote worker node the law runscript:
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
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ---------- create a temporary staging directory ----------------------------
STAGING="$(mktemp -d)"
PACKAGES_DIR="$STAGING/python_packages"
mkdir -p "$PACKAGES_DIR"

echo "==> Staging directory: $STAGING"
echo "==> Python:            $($PYTHON --version)"
echo ""

# ---------- install base runtime packages -----------------------------------
echo "==> Installing base packages (law, luigi, requests) ..."
"$PYTHON" -m pip install --quiet --target="$PACKAGES_DIR" --no-deps \
    law luigi requests

# ---------- install extra packages ------------------------------------------
if [ -n "$EXTRA_PACKAGES" ]; then
    echo "==> Installing extra packages: $EXTRA_PACKAGES ..."
    # shellcheck disable=SC2086
    "$PYTHON" -m pip install --quiet --target="$PACKAGES_DIR" $EXTRA_PACKAGES
fi

# ---------- copy rdfanalyzer Python bindings --------------------------------
if [ "$INCLUDE_RDFANALYZER" = true ]; then
    # Search build/ for the compiled Python extension
    SO_FILE="$(find "$REPO_ROOT/build" -name "rdfanalyzer*.so" 2>/dev/null | head -1 || true)"
    if [ -n "$SO_FILE" ]; then
        echo "==> Copying rdfanalyzer Python module: $SO_FILE"
        cp "$SO_FILE" "$PACKAGES_DIR/"
    else
        echo "==> WARNING: rdfanalyzer.so not found under $REPO_ROOT/build/"
        echo "            Build the project first with:  bash build.sh"
        echo "            Then re-run this script."
    fi
fi

# ---------- create the tarball ----------------------------------------------
OUTPUT_ABS="$(realpath -m "$OUTPUT")"
echo ""
echo "==> Creating tarball: $OUTPUT_ABS"
# tar the contents of the packages directory directly (no subdirectory wrapper)
# so that after  tar -xzf <tarball> -C _python_env  the packages are at
# _python_env/ directly (PYTHONPATH=$PWD/_python_env).
tar -czf "$OUTPUT_ABS" -C "$PACKAGES_DIR" .

# ---------- clean up --------------------------------------------------------
rm -rf "$STAGING"

echo ""
echo "Done!  Python environment tarball: $OUTPUT_ABS"
echo ""
echo "To use it with law tasks, add:"
echo "  --python-env $OUTPUT_ABS"
echo ""
echo "Example:"
echo "  law run BuildNANOSubmission \\"
echo "    --submit-config analyses/myAnalysis/cfg/submit_config.txt \\"
echo "    --name myRun --x509 x509 --exe build/analyses/myAnalysis/myanalysis \\"
echo "    --python-env $OUTPUT_ABS"
