#!/bin/bash
# Environment setup for RDFAnalyzerCore inside the cmssw-el9 Singularity container.
#
# Uses lcg/app/releases ROOT (standalone build) rather than lcg/releases (LCG
# pipeline build) because the lcg/releases builds embed hardcoded build-server
# absolute paths inside their .pcm module files (e.g. MathCore.pcm imports
# Vc.pcm at /build/jenkins/...). This causes cling to fail to load modules at
# runtime with "module file not found" errors even though the .pcm files exist.
#
# The lcg/app/releases ROOT 6.36.08 build:
#   - Has no hardcoded build-server paths in its PCM files.
#   - Was compiled with the system GCC 11.5 present in the cmssw-el9 container.
#   - Sets ROOTSYS = its own prefix, so the CMakeLists.txt ROOTSYS check passes.
#
# All supporting packages use x86_64-el9-gcc11-opt to match the ROOT ABI.
# Platform: x86_64-almalinux9 / cmssw-el9 container, system GCC 11.5

ulimit -n 10000

# Compiler: use the system GCC 11.5.0 that ships in the cmssw-el9 container.
# This matches the compiler used to build the ROOT below.
export CC=/usr/bin/gcc
export CXX=/usr/bin/g++
export FC=/usr/bin/gfortran

# ---------------------------------------------------------------------------
# 1. ROOT 6.36.08  (lcg/app/releases standalone build — no build-server paths
#                   in PCM files; ROOTSYS set to cvmfs prefix so CMake check passes)
# ---------------------------------------------------------------------------
source /cvmfs/sft.cern.ch/lcg/app/releases/ROOT/6.36.08/x86_64-almalinux9.7-gcc115-opt/bin/thisroot.sh

# ---------------------------------------------------------------------------
# 2. vdt 0.4.4  (ROOT's cmake FindVdt.cmake calls find_dependency(Vdt); the
#                app/releases ROOT bundles libvdt.so in its own lib/ dir and
#                thisroot.sh prepends ROOTSYS to CMAKE_PREFIX_PATH so FindVdt
#                finds it there first — this entry is a safe fallback only)
# ---------------------------------------------------------------------------
LCG_VDT_ROOT=/cvmfs/sft.cern.ch/lcg/releases/vdt/0.4.4-f1a43/x86_64-el9-gcc11-opt
export CMAKE_PREFIX_PATH=${LCG_VDT_ROOT}:${CMAKE_PREFIX_PATH}
export LD_LIBRARY_PATH=${LCG_VDT_ROOT}/lib:${LD_LIBRARY_PATH}

# ---------------------------------------------------------------------------
# 3. xxHash 0.8.2  (libxxhash.so.0 — needed at runtime by libCore.so,
#                   libImt.so, libROOTDataFrame.so, libTMVA.so; NOT bundled
#                   in the ROOT lib dir and NOT a system package in el9)
# NOTE: libtbb.so.2 (ROOT::Imt) and libgslcblas.so.0 (ROOT::TMVA) are
#       provided by system /lib64/ for this ROOT build — no LCG TBB/GSL needed.
# ---------------------------------------------------------------------------
LCG_XXHASH_ROOT=/cvmfs/sft.cern.ch/lcg/releases/xxHash/0.8.2-cf6e8/x86_64-el9-gcc11-opt
export CMAKE_PREFIX_PATH=${LCG_XXHASH_ROOT}:${CMAKE_PREFIX_PATH}
export LD_LIBRARY_PATH=${LCG_XXHASH_ROOT}/lib:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${LCG_XXHASH_ROOT}/lib:${LIBRARY_PATH}

# ---------------------------------------------------------------------------
# 4. tbb + GSL CBLAS fallback (for linker resolution of ROOT transitive deps)
#    Some cmssw-el9 images/remote nodes do not provide libtbb.so.2 and
#    libgslcblas.so.0 in default linker search paths.
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
	if [ -f "${d}/lib/libgslcblas.so.0" ]; then
		echo "${d}"
	fi
done | tail -n 1)
if [ -n "${LCG_GSL_ROOT}" ] && [ -f "${LCG_GSL_ROOT}/lib/libgslcblas.so.0" ]; then
	export CMAKE_PREFIX_PATH=${LCG_GSL_ROOT}:${CMAKE_PREFIX_PATH}
	export LD_LIBRARY_PATH=${LCG_GSL_ROOT}/lib:${LD_LIBRARY_PATH}
	export LIBRARY_PATH=${LCG_GSL_ROOT}/lib:${LIBRARY_PATH}
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
export PATH=${LCG_PYTHON_ROOT}/bin:${PATH}
export LD_LIBRARY_PATH=${LCG_PYTHON_ROOT}/lib:${LD_LIBRARY_PATH}
export LIBRARY_PATH=${LCG_PYTHON_ROOT}/lib:${LIBRARY_PATH}
export CMAKE_PREFIX_PATH=${LCG_PYTHON_ROOT}:${CMAKE_PREFIX_PATH}

# ---------------------------------------------------------------------------
# Remaining dependencies (ZLIB, pthreads) come from the el9 system libraries.
# pybind11, yaml-cpp, gtest, correctionlib, fastforest are vendored in core/extern/.
# ONNX Runtime is auto-downloaded by cmake/SetupOnnxRuntime.cmake at configure time.
# ---------------------------------------------------------------------------
