add_library(JetTaggingWorkingPointManager OBJECT JetTaggingWorkingPointManager.cc)
target_include_directories(JetTaggingWorkingPointManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(JetTaggingWorkingPointManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
