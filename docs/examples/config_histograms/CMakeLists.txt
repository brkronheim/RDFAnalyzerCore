cmake_minimum_required(VERSION 3.16)

# Example analysis with config-driven histograms
project(ExampleAnalysisConfigHistograms CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Find ROOT
find_package(ROOT REQUIRED COMPONENTS ROOTDataFrame)

# Include directories
include_directories(${CMAKE_SOURCE_DIR}/core/interface)
include_directories(${CMAKE_SOURCE_DIR}/core/interface/api)
include_directories(${CMAKE_SOURCE_DIR}/core/plugins/NDHistogramManager)
include_directories(${ROOT_INCLUDE_DIRS})

# Build the executable
add_executable(ExampleAnalysisConfigHistograms analysis.cc)

# Link against core libraries
target_link_libraries(ExampleAnalysisConfigHistograms
    PRIVATE
    RDFAnalyzerCore
    ${ROOT_LIBRARIES}
)
