add_library(TauEnergyScaleManager OBJECT TauEnergyScaleManager.cc)
target_include_directories(TauEnergyScaleManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(TauEnergyScaleManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
