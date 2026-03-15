add_library(PhotonEnergyScaleManager OBJECT PhotonEnergyScaleManager.cc)
target_include_directories(PhotonEnergyScaleManager PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(PhotonEnergyScaleManager PUBLIC
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    ROOT::Core
    correctionlib
)
