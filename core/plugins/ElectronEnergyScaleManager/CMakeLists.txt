add_library(ElectronEnergyScaleManager OBJECT ElectronEnergyScaleManager.cc)
target_include_directories(ElectronEnergyScaleManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(ElectronEnergyScaleManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
