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
    ${CMAKE_CURRENT_SOURCE_DIR}/ObjectEnergyManagerBase
    ${CMAKE_CURRENT_SOURCE_DIR}/PhotonEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/RegionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/TauEnergyScaleManager
    ${CMAKE_CURRENT_SOURCE_DIR}/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/NamedObjectManager
    ${CMAKE_CURRENT_SOURCE_DIR}/OnnxManager
    ${CMAKE_CURRENT_SOURCE_DIR}/SofieManager
    ${CMAKE_CURRENT_SOURCE_DIR}/WeightManager
    # Extern headers needed so plugin .cc files can #include <analyzer.h>
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/correctionlib/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../extern/XGBoost-FastForest/include
)

# Create static library
add_library(corePlugins STATIC)

# Add subdirectories
add_subdirectory(BDTManager)
add_subdirectory(CorrectionManager)
add_subdirectory(CutflowManager)
add_subdirectory(ObjectEnergyManagerBase)
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

# Explicitly embed all OBJECT library object files into the static archive.
# This ensures libcorePlugins.a contains every plugin's symbols, so executables
# that link against corePlugins can resolve them even if they don't directly
# reference a particular plugin (e.g. tests that use Analyzer but not NDHistogramManager).
target_sources(corePlugins PRIVATE
    $<TARGET_OBJECTS:BDTManager>
    $<TARGET_OBJECTS:CorrectionManager>
    $<TARGET_OBJECTS:CutflowManager>
    $<TARGET_OBJECTS:ObjectEnergyManagerBase>
    $<TARGET_OBJECTS:ElectronEnergyScaleManager>
    $<TARGET_OBJECTS:JetEnergyScaleManager>
    $<TARGET_OBJECTS:KinematicFitManager>
    $<TARGET_OBJECTS:GoldenJsonManager>
    $<TARGET_OBJECTS:MuonRochesterManager>
    $<TARGET_OBJECTS:NDHistogramManager>
    $<TARGET_OBJECTS:PhotonEnergyScaleManager>
    $<TARGET_OBJECTS:RegionManager>
    $<TARGET_OBJECTS:TauEnergyScaleManager>
    $<TARGET_OBJECTS:TriggerManager>
    $<TARGET_OBJECTS:OnnxManager>
    $<TARGET_OBJECTS:SofieManager>
    $<TARGET_OBJECTS:WeightManager>
)

# Link all plugin libraries to corePlugins
target_link_libraries(corePlugins
    BDTManager
    CorrectionManager
    CutflowManager
    ObjectEnergyManagerBase
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