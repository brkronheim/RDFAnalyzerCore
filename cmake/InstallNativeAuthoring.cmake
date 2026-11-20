include_guard(GLOBAL)

# Optional stdlib-only helpers, independent of ROOT and the Analyzer library.
function(rdf_install_native_authoring)
    cmake_parse_arguments(PARSE_ARGV 0 _native "" "COMPONENT" "")
    if(NOT _native_COMPONENT OR _native_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "rdf_install_native_authoring requires COMPONENT <name>")
    endif()
    get_filename_component(_native_source "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../core/interface" ABSOLUTE)
    add_library(rdf_native_authoring INTERFACE)
    add_library(RDFAnalyzer::NativeAuthoring ALIAS rdf_native_authoring)
    set_target_properties(rdf_native_authoring PROPERTIES EXPORT_NAME NativeAuthoring)
    target_compile_features(rdf_native_authoring INTERFACE cxx_std_17)
    target_include_directories(rdf_native_authoring INTERFACE
        "$<BUILD_INTERFACE:${_native_source}>" "$<INSTALL_INTERFACE:include/rdfanalyzer>")
    install(TARGETS rdf_native_authoring EXPORT RDFNativeAuthoringTargets COMPONENT "${_native_COMPONENT}")
    install(EXPORT RDFNativeAuthoringTargets FILE RDFNativeAuthoringTargets.cmake
        NAMESPACE RDFAnalyzer:: DESTINATION lib/cmake/RDFNativeAuthoring COMPONENT "${_native_COMPONENT}")
    install(FILES "${_native_source}/CommonSettings.h" "${_native_source}/CommonSettingsSchema.h"
        "${_native_source}/ValidationReport.h" "${_native_source}/NativePreflight.h" "${_native_source}/RunResult.h"
        "${_native_source}/TagAndProbe.h"
        DESTINATION include/rdfanalyzer COMPONENT "${_native_COMPONENT}")
    install(FILES "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/RDFNativeAuthoringConfig.cmake"
        DESTINATION lib/cmake/RDFNativeAuthoring COMPONENT "${_native_COMPONENT}")
endfunction()
