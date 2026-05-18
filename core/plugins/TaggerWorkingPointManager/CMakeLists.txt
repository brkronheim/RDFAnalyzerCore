add_library(TaggerWorkingPointManager OBJECT TaggerWorkingPointManager.cc)
target_include_directories(TaggerWorkingPointManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(TaggerWorkingPointManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
