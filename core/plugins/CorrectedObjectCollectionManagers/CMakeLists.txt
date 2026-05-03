add_library(CorrectedObjectCollectionManagers OBJECT CorrectedObjectCollectionManagers.cc)
target_include_directories(CorrectedObjectCollectionManagers PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
)
target_link_libraries(CorrectedObjectCollectionManagers PUBLIC
    ElectronEnergyScaleManager
    JetEnergyScaleManager
    MuonRochesterManager
    PhotonEnergyScaleManager
    TauEnergyScaleManager
    ROOT::Core
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
)