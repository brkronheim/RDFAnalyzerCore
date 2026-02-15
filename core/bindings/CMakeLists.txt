# core/bindings/CMakeLists.txt
# Python bindings for RDFAnalyzerCore using pybind11

# Find Python 3.8 or later
# find_package(Python3 3.8 COMPONENTS Interpreter Development)
#if(NOT Python3_FOUND)
#    message(STATUS "Python 3.8+ not found, skipping Python bindings")
#    return()
#endif()

# Try to find pybind11 first via CMake or the Python-provided cmake dir
#find_package(pybind11 CONFIG QUIET)

#if(NOT pybind11_FOUND)
#    # Fall back to fetching pybind11 sources and building it as part of the project
#    message(STATUS "pybind11 not found via find_package; fetching pybind11 with CMake FetchContent")
#    include(FetchContent)
#    FetchContent_Declare(
#        pybind11
#        GIT_REPOSITORY https://github.com/pybind/pybind11.git
#        # Use a recent stable tag; change if you need a different version
#        GIT_TAG v3.0.1
#    )
#    FetchContent_MakeAvailable(pybind11)
#endif()

#if(NOT pybind11_FOUND)
#    message(STATUS "pybind11 still not available, skipping Python bindings")
#    message(STATUS "To enable: either install pybind11 (pip install pybind11) or ensure network access for CMake FetchContent")
#    return()
#endif()

#message(STATUS "Building Python bindings with pybind11 ${pybind11_VERSION}")



# Create the Python module
pybind11_add_module(rdfanalyzer python_bindings.cpp)

# Link against the core library
target_link_libraries(rdfanalyzer PRIVATE
    core
    corePlugins
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
)

# Include directories
target_include_directories(rdfanalyzer PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface/api
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/OnnxManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/BDTManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/CorrectionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/SofieManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/NDHistogramManager
    ${CMAKE_CURRENT_SOURCE_DIR}/../plugins/NamedObjectManager
)

# Set output directory for the Python module
set_target_properties(rdfanalyzer PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/python"
)

# Installation rules
install(TARGETS rdfanalyzer
    LIBRARY DESTINATION python
)

# Python binding runtime tests are registered in core/test/CMakeLists.txt
# so they run as part of the standard test suite.
