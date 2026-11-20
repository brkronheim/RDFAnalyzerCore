#!/bin/bash
# Environment setup for RDFAnalyzerCore inside the cmssw-el9 Singularity container.
#
# Uses lcg/app/releases ROOT (standalone build) rather than lcg/releases (LCG
# pipeline build) because the lcg/releases builds embed hardcoded build-server
# absolute paths inside their .pcm module files (e.g. MathCore.pcm imports
# Vc.pcm at /build/jenkins/...). This causes cling to fail to load modules at
# runtime with "module file not found" errors even though the .pcm files exist.
#
# The lcg/app/releases ROOT build:
#   - Has no hardcoded build-server paths in its PCM files.
#   - Was compiled with the system GCC 11.5 present in the cmssw-el9 container.
#   - Sets ROOTSYS = its own prefix, so the CMakeLists.txt ROOTSYS check passes.
#
# All supporting packages use x86_64-el9-gcc11-opt to match the ROOT ABI.
# Platform: x86_64-almalinux9 / cmssw-el9 container, system GCC 11.5

# Resolve paths relative to this file so the environment works when sourced
# from outside the repository root as well.
export REPO_DIR="$(CDPATH= cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)"

ulimit -n 10000

# Compiler: use the system GCC 11.5.0 that ships in the cmssw-el9 container.
# This matches the compiler used to build the ROOT below.
export CC=/usr/bin/gcc
export CXX=/usr/bin/g++
export FC=/usr/bin/gfortran

# ---------------------------------------------------------------------------
# 1. Standalone ROOT. Set RDF_CVMFS_ROOT_RELEASE=6.40.04 before sourcing to
#    select the previous stable release.
# ---------------------------------------------------------------------------
rdf_root_prefix="/cvmfs/sft.cern.ch/lcg/app/releases/ROOT/${RDF_CVMFS_ROOT_RELEASE:-6.42.00-rc1}/x86_64-almalinux9.8-gcc115-opt"
if [ ! -x "${rdf_root_prefix}/bin/root-config" ]; then
    printf 'ROOT installation missing at %s\n' "${rdf_root_prefix}" >&2
    return 1
fi

# The release candidate's thisroot.sh misidentifies Bash when its command line
# contains newlines, as in a batch script passed to bash -c. Set the Linux
# paths from the known prefix, replacing entries from a previous ROOT setup.
rdf_prepend_root_path() {
    local variable=$1 suffix=$2 entry previous
    local updated="${rdf_root_prefix}${suffix}"
    local -a entries
    previous=${!variable-}
    IFS=: read -r -a entries <<< "$previous"
    for entry in "${entries[@]}"; do
        if [[ "$entry" == "${rdf_root_prefix}${suffix}" || ( -n "${ROOTSYS:-}" && "$entry" == "${ROOTSYS}${suffix}" ) ]]; then
            continue
        fi
        updated+=:"$entry"
    done
    if [[ "$variable" == MANPATH && ( -z "$previous" || "$previous" == *: ) ]]; then
        updated+=:
    fi
    export "$variable=$updated"
}
rdf_prepend_root_path PATH /bin
rdf_prepend_root_path LD_LIBRARY_PATH /lib
rdf_prepend_root_path PYTHONPATH /lib
rdf_prepend_root_path CMAKE_PREFIX_PATH ''
rdf_prepend_root_path JUPYTER_PATH /etc/notebook
rdf_prepend_root_path JUPYTER_CONFIG_PATH /etc/notebook
rdf_prepend_root_path MANPATH /man
export ROOTSYS="$rdf_root_prefix"
unset rdf_root_prefix
unset -f rdf_prepend_root_path

# ---------------------------------------------------------------------------
# 2. vdt 0.4.4 for consumers that link it directly, including Combine.
#    ROOT 6.40.04 also exports its bundled Vdt through ROOTConfig.cmake.
# ---------------------------------------------------------------------------
LCG_VDT_ROOT=/cvmfs/sft.cern.ch/lcg/releases/vdt/0.4.4-f1a43/x86_64-el9-gcc11-opt
export CMAKE_PREFIX_PATH=${LCG_VDT_ROOT}:${CMAKE_PREFIX_PATH}
export LD_LIBRARY_PATH=${LCG_VDT_ROOT}/lib:${LD_LIBRARY_PATH}

# ---------------------------------------------------------------------------
# 3. xxHash 0.8.2  (libxxhash.so.0 — needed at runtime by libCore.so,
#                   libImt.so, libROOTDataFrame.so, libTMVA.so; NOT bundled
#                   in the ROOT lib dir and NOT a system package in el9)
# NOTE: this ROOT build's libMathMore.so needs libgsl.so.25 and its TMVA
#       components need libgslcblas.so.0. Use the matching GSL SONAME below.
# ---------------------------------------------------------------------------
LCG_XXHASH_ROOT=/cvmfs/sft.cern.ch/lcg/releases/xxHash/0.8.2-cf6e8/x86_64-el9-gcc11-opt
export CMAKE_PREFIX_PATH=${LCG_XXHASH_ROOT}:${CMAKE_PREFIX_PATH}
export LD_LIBRARY_PATH=${LCG_XXHASH_ROOT}/lib:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${LCG_XXHASH_ROOT}/lib:${LIBRARY_PATH}

# ---------------------------------------------------------------------------
# 4. tbb + GSL (for linker resolution of ROOT transitive deps)
#    Some cmssw-el9 images/remote nodes do not provide libtbb.so.2 and
#    the GSL libraries in default linker search paths. ROOT's
#    libMathMore.so was built against libgsl.so.25, so select a package that
#    provides that SONAME instead of simply taking the newest GSL release.
# ---------------------------------------------------------------------------
LCG_TBB_ROOT=$(for d in /cvmfs/sft.cern.ch/lcg/releases/tbb/*/x86_64-el9-gcc11-opt; do
	if [ -f "${d}/lib/libtbb.so.2" ]; then
		echo "${d}"
	fi
done | tail -n 1)
if [ -n "${LCG_TBB_ROOT}" ] && [ -f "${LCG_TBB_ROOT}/lib/libtbb.so.2" ]; then
	export CMAKE_PREFIX_PATH=${LCG_TBB_ROOT}:${CMAKE_PREFIX_PATH}
	export LD_LIBRARY_PATH=${LCG_TBB_ROOT}/lib:${LD_LIBRARY_PATH}
	export LIBRARY_PATH=${LCG_TBB_ROOT}/lib:${LIBRARY_PATH}
fi

LCG_GSL_ROOT=$(for d in /cvmfs/sft.cern.ch/lcg/releases/GSL/*/x86_64-el9-gcc11-opt; do
	if [ -f "${d}/lib/libgsl.so.25" ] && [ -f "${d}/lib/libgslcblas.so.0" ]; then
		echo "${d}"
	fi
done | tail -n 1)
if [ -n "${LCG_GSL_ROOT}" ] && [ -f "${LCG_GSL_ROOT}/lib/libgsl.so.25" ] && [ -f "${LCG_GSL_ROOT}/lib/libgslcblas.so.0" ]; then
	export CMAKE_PREFIX_PATH=${LCG_GSL_ROOT}:${CMAKE_PREFIX_PATH}
	export LD_LIBRARY_PATH=${LCG_GSL_ROOT}/lib:${LD_LIBRARY_PATH}
	export LIBRARY_PATH=${LCG_GSL_ROOT}/lib:${LIBRARY_PATH}
fi

# RooFitCore loads FFTW on the standalone ROOT build. Some EL9 hosts have no
# system libfftw3.so.3, including for PyROOT's lazy library loading.
LCG_FFTW_ROOT=/cvmfs/sft.cern.ch/lcg/releases/fftw3/3.3.10-33229/x86_64-el9-gcc11-opt
if [ -f "${LCG_FFTW_ROOT}/lib/libfftw3.so.3" ]; then
    export CMAKE_PREFIX_PATH=${LCG_FFTW_ROOT}:${CMAKE_PREFIX_PATH}
    export LD_LIBRARY_PATH=${LCG_FFTW_ROOT}/lib:${LD_LIBRARY_PATH}
    export LIBRARY_PATH=${LCG_FFTW_ROOT}/lib:${LIBRARY_PATH}
fi

# Source the environment hooks declared by installed extensions. Each extension
# owns its enable option and presets; the framework only discovers manifests.
# RDF_EXTENSION_ENV_BEGIN
rdf_extension_env_paths=$(python3 "${REPO_DIR}/scripts/extension_env.py") || return 1
while IFS= read -r rdf_extension_environment; do
    if [ -n "${rdf_extension_environment}" ]; then
        source "${rdf_extension_environment}" || return 1
    fi
done <<< "${rdf_extension_env_paths}"
# RDF_EXTENSION_ENV_END

# Honor an explicitly selected XRootD package with or without an experiment
# checkout. Optional presets may supply a default prefix.
LCG_XROOTD_ROOT=${XROOTD_ROOT:-${XRD_ROOT:-}}
if [ -n "${LCG_XROOTD_ROOT}" ]; then
    LCG_XROOTD_LIBDIR=${LCG_XROOTD_ROOT}/lib64
    if [ ! -f "${LCG_XROOTD_LIBDIR}/libXrdCl.so" ]; then
        LCG_XROOTD_LIBDIR=${LCG_XROOTD_ROOT}/lib
    fi
    if [ -d "${LCG_XROOTD_ROOT}/include/xrootd/XrdCl" ] && [ -f "${LCG_XROOTD_LIBDIR}/libXrdCl.so" ]; then
        export PATH=${LCG_XROOTD_ROOT}/bin:${PATH}
        export CMAKE_PREFIX_PATH=${LCG_XROOTD_ROOT}:${CMAKE_PREFIX_PATH}
        export LD_LIBRARY_PATH=${LCG_XROOTD_LIBDIR}:${LD_LIBRARY_PATH}
        export LIBRARY_PATH=${LCG_XROOTD_LIBDIR}:${LIBRARY_PATH}
    fi
fi

# ---------------------------------------------------------------------------
# 5. Boost 1.88.0  (binary + CMake config — satisfies find_package(Boost CONFIG))
# ---------------------------------------------------------------------------
LCG_BOOST_ROOT=/cvmfs/sft.cern.ch/lcg/releases/Boost/1.88.0-7f2c3/x86_64-el9-gcc11-opt
export CMAKE_PREFIX_PATH=${LCG_BOOST_ROOT}:${CMAKE_PREFIX_PATH}
export LD_LIBRARY_PATH=${LCG_BOOST_ROOT}/lib:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${LCG_BOOST_ROOT}/lib:${LIBRARY_PATH}

# ---------------------------------------------------------------------------
# 6. Python 3.12.11  (interpreter + headers for pybind11 bindings)
# ---------------------------------------------------------------------------
LCG_PYTHON_ROOT=/cvmfs/sft.cern.ch/lcg/releases/Python/3.12.11-531c6/x86_64-el9-gcc11-opt
# Keep an already activated virtual environment first on PATH. Its Python
# executable and installed packages must stay paired when running analysis tools.
if [ -z "${VIRTUAL_ENV:-}" ]; then
	export PATH=${LCG_PYTHON_ROOT}/bin:${PATH}
fi
export LD_LIBRARY_PATH=${LCG_PYTHON_ROOT}/lib:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${LCG_PYTHON_ROOT}/lib:${LIBRARY_PATH}
export CMAKE_PREFIX_PATH=${LCG_PYTHON_ROOT}:${CMAKE_PREFIX_PATH}

# ---------------------------------------------------------------------------
# Remaining dependencies (ZLIB, pthreads) come from the el9 system libraries.
# pybind11, yaml-cpp, gtest, correctionlib, fastforest are vendored in core/extern/.
# ONNX Runtime is auto-downloaded by cmake/SetupOnnxRuntime.cmake at configure time.
# ---------------------------------------------------------------------------
