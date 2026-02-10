# core/bindings/CMakeLists.txt
# Python bindings for RDFAnalyzerCore using pybind11

# Find Python 3.8 or later
find_package(Python3 3.8 COMPONENTS Interpreter Development)
if(NOT Python3_FOUND)
    message(STATUS "Python 3.8+ not found, skipping Python bindings")
    return()
endif()

# Try to find pybind11
execute_process(
    COMMAND ${Python3_EXECUTABLE} -c "import pybind11; print(pybind11.get_cmake_dir())"
    OUTPUT_VARIABLE PYBIND11_CMAKE_DIR
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_QUIET
)

if(PYBIND11_CMAKE_DIR)
    list(APPEND CMAKE_PREFIX_PATH ${PYBIND11_CMAKE_DIR})
    find_package(pybind11 CONFIG)
endif()

if(NOT pybind11_FOUND)
    message(STATUS "pybind11 not found, skipping Python bindings")
    message(STATUS "Install with: pip install pybind11")
    return()
endif()

message(STATUS "Building Python bindings with pybind11 ${pybind11_VERSION}")

# Create the Python module
pybind11_add_module(rdfanalyzer python_bindings.cpp)

# Link against the core library
target_link_libraries(rdfanalyzer PRIVATE
    core
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
)

# Include directories
target_include_directories(rdfanalyzer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface/api
)

# Set output directory for the Python module
set_target_properties(rdfanalyzer PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python"
)

# Installation rules
install(TARGETS rdfanalyzer
    LIBRARY DESTINATION python
)
