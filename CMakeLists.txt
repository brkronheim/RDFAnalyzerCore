include(CMakePrintHelpers)

cmake_minimum_required(VERSION 3.19.0)
set_property(GLOBAL PROPERTY USE_FOLDERS ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set(CMAKE_MODULE_PATH "cmake/" ${CMAKE_MODULE_PATH})

project(RDFAnalyzer)

enable_testing()

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads)
find_package(ZLIB)



set_property(GLOBAL PROPERTY USE_FOLDERS ON)

find_package(ROOT REQUIRED)
find_package(Boost REQUIRED CONFIG)

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
