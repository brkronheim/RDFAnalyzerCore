#!/usr/bin/env bash
# ===========================================================================
# htcondor_bootstrap.sh
#
# Bootstrap script executed on every HTCondor worker node before LAW runs the
# branch task.  Customize this file for your site's software environment.
#
# LAW automatically transfers this file together with the job payload when
# RunNANOJobs or RunOpenDataJobs is submitted with --workflow htcondor.
#
# This template handles the most common CMS/ATLAS use cases:
#   1. Unpacking a Python environment tarball (law/setup_python_env.sh output)
#   2. Sourcing CVMFS-based ROOT/CMSSW environment (optional)
#   3. Setting up xrootd / EOS for input/output staging
#
# The script receives the following environment variables from LAW:
#   LAW_JOB_INIT_DIR   – directory where the job was initialised
#   LAW_JOB_HOME       – working directory of the job
#
# Site-specific configuration is supplied through law.cfg ([htcondor_workflow]
# section) or via command-line flags.  No hard-coded site paths belong here.
# ===========================================================================

set -euo pipefail

echo "=== htcondor_bootstrap.sh start ==="
echo "Hostname : $(hostname -f 2>/dev/null || hostname)"
echo "Working dir: $PWD"

# ---------------------------------------------------------------------------
# 1.  Python environment tarball
#     If a tarball named *.tar.gz is present in the job directory, unpack it
#     and prepend its site-packages to PYTHONPATH so that law, luigi, and any
#     other Python dependencies are available without a CMSSW environment.
# ---------------------------------------------------------------------------
_py_tarball=""
# Use nullglob so the loop does not iterate over literal glob strings when
# no matching files exist.
shopt -s nullglob
for _t in *.tar.gz python_env*.tgz shared_inputs/*.tar.gz shared_inputs/python_env*.tgz; do
    _py_tarball="$_t"
    break
done
shopt -u nullglob

if [ -n "$_py_tarball" ]; then
    echo "Unpacking Python environment tarball: $_py_tarball"
    mkdir -p _python_env
    tar xzf "$_py_tarball" -C _python_env
    # Discover the site-packages directory inside the tarball
    _sp=$(find _python_env -type d -name "site-packages" | head -1)
    if [ -n "$_sp" ]; then
        export PYTHONPATH="$(realpath "$_sp"):${PYTHONPATH:-}"
        echo "PYTHONPATH prepended with: $(realpath "$_sp")"
    fi
    _bin=$(find _python_env -type d -name "bin" | head -1)
    if [ -n "$_bin" ]; then
        export PATH="$(realpath "$_bin"):${PATH}"
    fi
fi

# ---------------------------------------------------------------------------
# 2.  Verify law is available
# ---------------------------------------------------------------------------
if ! python3 -c "import law" 2>/dev/null; then
    echo "WARNING: law is not importable after bootstrap."
    echo "  If you rely on a CVMFS-based Python, uncomment the CVMFS section"
    echo "  below and point it at your experiment's software setup."
fi

# ---------------------------------------------------------------------------
# 3.  Optional: CVMFS-based experiment software setup
#     Uncomment and adapt for CMS (CMSSW) or ATLAS (LCG) environments.
# ---------------------------------------------------------------------------
# --- CMS / CMSSW example ---------------------------------------------------
# if [ -d "/cvmfs/cms.cern.ch" ]; then
#     source /cvmfs/cms.cern.ch/cmsset_default.sh
#     export SCRAM_ARCH=el9_amd64_gcc12
# fi

# --- ATLAS / LCG example ---------------------------------------------------
# if [ -d "/cvmfs/sft.cern.ch" ]; then
#     source /cvmfs/sft.cern.ch/lcg/views/LCG_105/x86_64-el9-gcc13-opt/setup.sh
# fi

# ---------------------------------------------------------------------------
# 4.  Optional: x509 proxy for XRootD / EOS access
#     LAW transfers the proxy file automatically when --x509 is given.
#     We just need to point X509_USER_PROXY at it.
# ---------------------------------------------------------------------------
if [ -f "x509" ]; then
    export X509_USER_PROXY="$PWD/x509"
    echo "Using x509 proxy: $X509_USER_PROXY"
elif [ -f "shared_inputs/x509" ]; then
    export X509_USER_PROXY="$PWD/shared_inputs/x509"
    echo "Using x509 proxy: $X509_USER_PROXY"
fi

echo "=== htcondor_bootstrap.sh done ==="
