include(CMakePrintHelpers)

cmake_minimum_required(VERSION 3.19.0)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_CXX_STANDARD 17)
# Ensure static libraries are built position-independent so they can be linked
# into shared objects (Python extension modules) without linker errors.
# This prevents "recompile with -fPIC" errors in CI where CMake may not set PIC.
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake" ${CMAKE_MODULE_PATH})

project(RDFAnalyzer)

# Build options
option(BUILD_TESTS "Build analysis tests" ON)
option(BUILD_COMBINE "Build CMS Combine package for statistical analysis" OFF)
option(BUILD_COMBINE_HARVESTER "Build CombineHarvester tools (requires BUILD_COMBINE)" OFF)
option(USE_CUDA "Enable CUDA GPU support for kinematic fits" OFF)

if(USE_CUDA)
    enable_language(CUDA)
    find_package(CUDAToolkit REQUIRED)
endif()

enable_testing()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)
find_package(ZLIB)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)

find_package(ROOT REQUIRED)
find_package(Boost REQUIRED CONFIG)

# Setup yaml-cpp
# include(SetupYamlCpp)

# Setup ONNX Runtime
include(SetupOnnxRuntime)

# Setup CMS Combine (optional)
if(BUILD_COMBINE OR BUILD_COMBINE_HARVESTER)
	include(SetupCombine)
endif()

set(PYBIND11_FINDPYTHON ON)

# Extra transitive dependencies that some ROOT builds require at link/runtime
# (e.g. libImt -> libtbb, libTMVA -> libgslcblas/openblas).
set(RDF_ROOT_TRANSITIVE_LIBS "")
set(RDF_ROOT_TRANSITIVE_RPATH_DIRS "")

# Ensure runtime uses the same ROOT installation found at configure time.
get_target_property(ROOT_CORE_LOCATION ROOT::Core IMPORTED_LOCATION)
if(NOT ROOT_CORE_LOCATION)
	get_target_property(ROOT_CORE_LOCATION ROOT::Core IMPORTED_LOCATION_RELEASE)
endif()
if(NOT ROOT_CORE_LOCATION)
	get_target_property(ROOT_CORE_LOCATION ROOT::Core IMPORTED_LOCATION_RELWITHDEBINFO)
endif()
if(NOT ROOT_CORE_LOCATION)
	get_target_property(ROOT_CORE_LOCATION ROOT::Core IMPORTED_LOCATION_MINSIZEREL)
endif()
if(NOT ROOT_CORE_LOCATION)
	get_target_property(ROOT_CORE_LOCATION ROOT::Core IMPORTED_LOCATION_DEBUG)
endif()

if(ROOT_CORE_LOCATION)
	get_filename_component(ROOT_LIB_DIR "${ROOT_CORE_LOCATION}" DIRECTORY)
	set(CMAKE_BUILD_RPATH "${ROOT_LIB_DIR}")
	set(CMAKE_INSTALL_RPATH "${ROOT_LIB_DIR}")
	set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

	# xxhash is a transitive dep of ROOT that is not bundled in ROOT's lib dir
	# and not a system package in el9. Find it and bake its path into RPATH
	# so binaries work on batch nodes without needing LD_LIBRARY_PATH to be set.
	find_library(XXHASH_LIB NAMES xxhash)
	if(XXHASH_LIB)
		get_filename_component(XXHASH_LIB_DIR "${XXHASH_LIB}" DIRECTORY)
		list(APPEND CMAKE_BUILD_RPATH "${XXHASH_LIB_DIR}")
		list(APPEND CMAKE_INSTALL_RPATH "${XXHASH_LIB_DIR}")
		message(STATUS "Found xxhash: ${XXHASH_LIB}")
	else()
		message(WARNING "xxhash library not found — runtime loading of ROOT may fail. Add libxxhash.so.0 to LD_LIBRARY_PATH or CMAKE_PREFIX_PATH.")
	endif()

	# Some ROOT binaries depend on libtbb/libgslcblas that may not be in system paths
	# on cmssw-el9 worker nodes. Resolve them at configure time and propagate both link
	# libraries and RPATH entries.
	find_library(RDF_TBB_LIB NAMES tbb)
	if(RDF_TBB_LIB)
		get_filename_component(RDF_TBB_LIB_DIR "${RDF_TBB_LIB}" DIRECTORY)
		list(APPEND RDF_ROOT_TRANSITIVE_LIBS "${RDF_TBB_LIB}")
		list(APPEND RDF_ROOT_TRANSITIVE_RPATH_DIRS "${RDF_TBB_LIB_DIR}")
		message(STATUS "Found ROOT transitive dep tbb: ${RDF_TBB_LIB}")
	else()
		message(WARNING "Could not find libtbb for ROOT::Imt. Link failures may occur if libImt.so is used.")
	endif()

	find_library(RDF_CBLAS_LIB NAMES gslcblas openblas cblas blas)
	if(RDF_CBLAS_LIB)
		get_filename_component(RDF_CBLAS_LIB_DIR "${RDF_CBLAS_LIB}" DIRECTORY)
		list(APPEND RDF_ROOT_TRANSITIVE_LIBS "${RDF_CBLAS_LIB}")
		list(APPEND RDF_ROOT_TRANSITIVE_RPATH_DIRS "${RDF_CBLAS_LIB_DIR}")
		message(STATUS "Found ROOT transitive dep CBLAS: ${RDF_CBLAS_LIB}")
	else()
		message(WARNING "Could not find libgslcblas/openblas for ROOT::TMVA. Link failures may occur if libTMVA.so is used.")
	endif()

	if(RDF_ROOT_TRANSITIVE_RPATH_DIRS)
		list(APPEND CMAKE_BUILD_RPATH ${RDF_ROOT_TRANSITIVE_RPATH_DIRS})
		list(APPEND CMAKE_INSTALL_RPATH ${RDF_ROOT_TRANSITIVE_RPATH_DIRS})
	endif()

	if(CMAKE_BUILD_RPATH)
		list(REMOVE_DUPLICATES CMAKE_BUILD_RPATH)
	endif()
	if(CMAKE_INSTALL_RPATH)
		list(REMOVE_DUPLICATES CMAKE_INSTALL_RPATH)
	endif()

	if(DEFINED ENV{ROOTSYS})
		file(TO_CMAKE_PATH "$ENV{ROOTSYS}" ROOTSYS_PATH)
		string(FIND "${ROOT_LIB_DIR}" "${ROOTSYS_PATH}" ROOTSYS_POS)
		if(NOT ROOTSYS_POS EQUAL 0)
			message(FATAL_ERROR "ROOTSYS='${ROOTSYS_PATH}' does not match ROOT libraries at '${ROOT_LIB_DIR}'. Source env.sh and reconfigure from a clean build directory.")
		endif()
	endif()
endif()

# Optional custom ROOT dictionary support
set(RDF_CUSTOM_DICT_HEADERS "" CACHE STRING "Semicolon-separated list of headers for custom ROOT dictionary generation")
set(RDF_CUSTOM_DICT_LINKDEF "" CACHE FILEPATH "Path to LinkDef.h for custom ROOT dictionary generation")
set(RDF_CUSTOM_DICT_INCLUDE_DIRS "" CACHE STRING "Semicolon-separated list of include dirs for custom ROOT dictionary")
set(RDF_CUSTOM_DICT_SOURCES "" CACHE STRING "Semicolon-separated list of sources for custom ROOT dictionary library")
set(RDF_CUSTOM_DICT_TARGET "RDFCustomDict" CACHE STRING "Target name for the custom ROOT dictionary library")

add_subdirectory(core)
add_subdirectory(analyses)
