# CMakeLists.txt for core/plugins

set(PLUGIN_SOURCE_DIRECTORIES
    ${CMAKE_CURRENT_SOURCE_DIR}/BDTManager
    ${CMAKE_CURRENT_SOURCE_DIR}/CorrectionManager
    ${CMAKE_CURRENT_SOURCE_DIR}/KinematicFitManager
    ${CMAKE_CURRENT_SOURCE_DIR}/GoldenJsonManager
    ${CMAKE_CURRENT_SOURCE_DIR}/NDHistogramManager
    ${CMAKE_CURRENT_SOURCE_DIR}/TriggerManager
    ${CMAKE_CURRENT_SOURCE_DIR}/NamedObjectManager
    ${CMAKE_CURRENT_SOURCE_DIR}/OnnxManager
    ${CMAKE_CURRENT_SOURCE_DIR}/SofieManager
)

# Create static library
add_library(corePlugins STATIC)

# Add subdirectories
add_subdirectory(BDTManager)
add_subdirectory(CorrectionManager)
add_subdirectory(KinematicFitManager)
add_subdirectory(GoldenJsonManager)
add_subdirectory(NDHistogramManager)
add_subdirectory(TriggerManager)
add_subdirectory(OnnxManager)
add_subdirectory(SofieManager)

# Link all plugin libraries to corePlugins
target_link_libraries(corePlugins
    BDTManager
    CorrectionManager
    KinematicFitManager
    GoldenJsonManager
    NDHistogramManager
    TriggerManager
    OnnxManager
    SofieManager
    core
    fastforest
    correctionlib
    OnnxRuntime::OnnxRuntime
)