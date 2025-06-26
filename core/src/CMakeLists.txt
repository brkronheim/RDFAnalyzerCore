# core/src/CMakeLists.txt

# Gather all source files in this directory
file(GLOB CORE_SRC "${CMAKE_CURRENT_SOURCE_DIR}/*.cc")

add_library(core STATIC ${CORE_SRC})

target_compile_features(core PUBLIC cxx_std_17)

target_include_directories(core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/XGBoost-FastForest/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/correctionlib/include
)

target_link_libraries(core PUBLIC
    fastforest
    correctionlib
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    ROOT::Hist
    ROOT::MathCore
)
