# Avoid warning about DOWNLOAD_EXTRACT_TIMESTAMP in CMake 3.24:
if(CMAKE_VERSION VERSION_GREATER_EQUAL "3.24.0")
    cmake_policy(SET CMP0135 NEW)
endif()

option(EXPERIMENTAL_TMVA_SUPPORT "Build the experimental TMVA support" OFF)

include_directories(XGBoost-FastForest/include)
include(GNUInstallDirs)

# Gather FastForest source files
if(EXPERIMENTAL_TMVA_SUPPORT)
    file(GLOB_RECURSE FASTFOREST_SRC "XGBoost-FastForest/*.cpp")
else()
    file(GLOB_RECURSE FASTFOREST_SRC
        "XGBoost-FastForest/src/common_details.cpp"
        "XGBoost-FastForest/src/fastforest_functions.cpp"
        "XGBoost-FastForest/src/fastforest.cpp"
    )
endif()

add_library(fastforest OBJECT ${FASTFOREST_SRC})
target_compile_features(fastforest PUBLIC cxx_std_17)
target_link_libraries(fastforest PRIVATE)

set(CORRECTIONLIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/correctionlib")

configure_file(
    ${CORRECTIONLIB_DIR}/include/version.h.in
    ${CORRECTIONLIB_DIR}/include/correctionlib_version.h
)

# Gather CorrectionLib source files (explicitly list only the required C++ files to avoid pybind/Python dependencies)
set(CORRECTIONLIB_SRC
    ${CORRECTIONLIB_DIR}/src/correction.cc
    ${CORRECTIONLIB_DIR}/src/formula_ast.cc
)

add_library(correctionlib OBJECT ${CORRECTIONLIB_SRC})
set_target_properties(correctionlib PROPERTIES PUBLIC_HEADER
    ${CORRECTIONLIB_DIR}/include/correction.h
    WINDOWS_EXPORT_ALL_SYMBOLS OFF
)
target_include_directories(correctionlib PUBLIC
    $<BUILD_INTERFACE:${CORRECTIONLIB_DIR}/include>
    PRIVATE $<BUILD_INTERFACE:${CORRECTIONLIB_DIR}/rapidjson/include>
    $<BUILD_INTERFACE:${CORRECTIONLIB_DIR}/cpp-peglib>
    $<BUILD_INTERFACE:${CORRECTIONLIB_DIR}/xxhash>
    $<BUILD_INTERFACE:${CORRECTIONLIB_DIR}/pcg-cpp/include>
)
target_link_libraries(correctionlib PRIVATE ZLIB::ZLIB Threads::Threads)


add_subdirectory(gtest)