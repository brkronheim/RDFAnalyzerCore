# core/plugins/cms/CMakeLists.txt
#
# Generic CMS analysis infrastructure plugin.
# Provides year-registry, nanoAOD compatibility, correction workflow,
# weight pattern utilities, and golden-JSON filtering for CMS analyses
# using RDFAnalyzerCore.

# ── CMS infrastructure object library ───────────────────────────────────────

add_library(CmsInfrastructure OBJECT
    CmsYearConfig.cc
    CmsNanoAodCompat.cc
    CmsCorrections.cc
    CmsWeights.cc
    GoldenJsonManager/GoldenJsonManager.cc
)

target_include_directories(CmsInfrastructure PUBLIC
    ${PLUGIN_SOURCE_DIRECTORIES}
    ${CMAKE_CURRENT_SOURCE_DIR}/../../interface
    ${CMAKE_CURRENT_SOURCE_DIR}/../..
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_SOURCE_DIR}/GoldenJsonManager
)

target_link_libraries(CmsInfrastructure PUBLIC
    ROOT::Core
    ROOT::Hist
    ROOT::ROOTDataFrame
    ROOT::ROOTVecOps
    yaml-cpp
)

# Ensure CmsInfrastructure is propagated to the parent scope
set(CMS_INFRASTRUCTURE_LIB CmsInfrastructure PARENT_SCOPE)
