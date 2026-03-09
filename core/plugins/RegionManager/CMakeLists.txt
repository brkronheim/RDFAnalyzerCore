add_library(RegionManager OBJECT RegionManager.cc)
target_include_directories(RegionManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(RegionManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
)
