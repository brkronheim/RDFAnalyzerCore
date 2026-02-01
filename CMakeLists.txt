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

# Optional custom ROOT dictionary support
set(RDF_CUSTOM_DICT_HEADERS "" CACHE STRING "Semicolon-separated list of headers for custom ROOT dictionary generation")
set(RDF_CUSTOM_DICT_LINKDEF "" CACHE FILEPATH "Path to LinkDef.h for custom ROOT dictionary generation")
set(RDF_CUSTOM_DICT_INCLUDE_DIRS "" CACHE STRING "Semicolon-separated list of include dirs for custom ROOT dictionary")
set(RDF_CUSTOM_DICT_SOURCES "" CACHE STRING "Semicolon-separated list of sources for custom ROOT dictionary library")
set(RDF_CUSTOM_DICT_TARGET "RDFCustomDict" CACHE STRING "Target name for the custom ROOT dictionary library")

add_subdirectory(core)
add_subdirectory(analyses)
