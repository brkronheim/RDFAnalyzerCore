add_library(GoldenJsonManager OBJECT GoldenJsonManager.cc)
target_include_directories(GoldenJsonManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(GoldenJsonManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
)
