add_library(JetEnergyScaleManager OBJECT JetEnergyScaleManager.cc)
target_include_directories(JetEnergyScaleManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(JetEnergyScaleManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
