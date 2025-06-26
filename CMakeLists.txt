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

add_subdirectory(core)
add_subdirectory(analyses)
add_subdirectory(runners)
