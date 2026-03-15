# CMakeLists.txt for core/plugins

set(PLUGIN_SOURCE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/BDTManager
    ${CMAKE_CURRENT_SOURCE_DIR}/CorrectionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/CutflowManager
    ${CMAKE_CURRENT_SOURCE_DIR}/ElectronEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/JetEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/KinematicFitManager
    ${CMAKE_CURRENT_SOURCE_DIR}/GoldenJsonManager
    ${CMAKE_CURRENT_SOURCE_DIR}/MuonRochesterManager
    ${CMAKE_CURRENT_SOURCE_DIR}/NDHistogramManager
    ${CMAKE_CURRENT_SOURCE_DIR}/PhotonEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/RegionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/TauEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/NamedObjectManager
    ${CMAKE_CURRENT_SOURCE_DIR}/OnnxManager
    ${CMAKE_CURRENT_SOURCE_DIR}/SofieManager
    ${CMAKE_CURRENT_SOURCE_DIR}/WeightManager
)

# Create static library
add_library(corePlugins STATIC)

# Add subdirectories
add_subdirectory(BDTManager)
add_subdirectory(CorrectionManager)
add_subdirectory(CutflowManager)
add_subdirectory(ElectronEnergyScaleManager)
add_subdirectory(JetEnergyScaleManager)
add_subdirectory(KinematicFitManager)
add_subdirectory(GoldenJsonManager)
add_subdirectory(MuonRochesterManager)
add_subdirectory(NDHistogramManager)
add_subdirectory(PhotonEnergyScaleManager)
add_subdirectory(RegionManager)
add_subdirectory(TauEnergyScaleManager)
add_subdirectory(TriggerManager)
add_subdirectory(OnnxManager)
add_subdirectory(SofieManager)
add_subdirectory(WeightManager)

# Link all plugin libraries to corePlugins
target_link_libraries(corePlugins
    BDTManager
    CorrectionManager
    CutflowManager
    ElectronEnergyScaleManager
    JetEnergyScaleManager
    KinematicFitManager
    GoldenJsonManager
    MuonRochesterManager
    NDHistogramManager
    PhotonEnergyScaleManager
    RegionManager
    TauEnergyScaleManager
    TriggerManager
    OnnxManager
    SofieManager
    WeightManager
    core
    fastforest
    correctionlib
    OnnxRuntime::OnnxRuntime
)