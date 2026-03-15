add_library(MuonRochesterManager OBJECT MuonRochesterManager.cc)
target_include_directories(MuonRochesterManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(MuonRochesterManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
