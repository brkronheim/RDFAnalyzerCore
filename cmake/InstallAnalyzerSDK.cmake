include_guard(GLOBAL)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include("${CMAKE_CURRENT_LIST_DIR}/InstallOnnxRuntime.cmake")

# The first validated SDK is intentionally explicit about its feature closure.
# Ordinary builds and independent Python/NativeAuthoring installs are unchanged.
function(rdf_install_analyzer_sdk)
    get_property(_sdk_extensions GLOBAL PROPERTY RDF_EXTENSION_TARGETS)
    if(_sdk_extensions OR RDF_ENABLE_XRD_ADAPTOR OR USE_CUDA OR ONNXRUNTIME_USE_CUDA
            OR RDF_CUSTOM_DICT_HEADERS OR RDF_CUSTOM_DICT_SOURCES)
        message(FATAL_ERROR "AnalyzerSDK currently requires a core-only CPU build: RDF_BUILD_EXTENSIONS=OFF, RDF_ENABLE_XRD_ADAPTOR=OFF, USE_CUDA=OFF, ONNXRUNTIME_USE_CUDA=OFF, and no custom dictionary. Other configurations retain the existing build/install paths.")
    endif()
    if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux" OR NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "AnalyzerSDK static rescan packaging currently supports Linux GNU/Clang builds")
    endif()
    foreach(_sdk_dir IN ITEMS CMAKE_INSTALL_LIBDIR CMAKE_INSTALL_INCLUDEDIR CMAKE_INSTALL_DATADIR)
        if(IS_ABSOLUTE "${${_sdk_dir}}")
            message(FATAL_ERROR "AnalyzerSDK requires relative ${_sdk_dir} for relocatable installation")
        endif()
    endforeach()
    set(_sdk_source "${PROJECT_SOURCE_DIR}/core")
    set(_sdk_headers "${CMAKE_INSTALL_INCLUDEDIR}/rdfanalyzer")
    set(_sdk_libraries "${CMAKE_INSTALL_LIBDIR}/rdfanalyzer")
    # Keep native overrides in executable consumers only, just as in the build tree.
    set(SDK_NATIVE_OBJECTS "")
    set(SDK_NATIVE_EXPORTS "")
    foreach(_target IN ITEMS RDFMimalloc RDFRootRegistryCompat)
        if(TARGET ${_target})
            install(FILES $<TARGET_OBJECTS:${_target}> DESTINATION "${_sdk_libraries}/native" COMPONENT AnalyzerSDK)
            get_target_property(_sources ${_target} SOURCES)
            foreach(_source IN LISTS _sources)
                get_filename_component(_filename "${_source}" NAME)
                list(APPEND SDK_NATIVE_OBJECTS "${_filename}.o")
            endforeach()
        endif()
    endforeach()
    if(TARGET RDFRootRegistryCompat)
        get_target_property(SDK_NATIVE_EXPORTS RDFRootRegistryCompat RDF_EXPORTED_SYMBOLS)
    endif()
    if(TARGET RDFMimalloc)
        install(FILES "${_sdk_source}/extern/mimalloc/LICENSE"
            DESTINATION "${CMAKE_INSTALL_DATADIR}/rdfanalyzer/licenses/mimalloc" COMPONENT AnalyzerSDK)
    endif()
    install(TARGETS core corePlugins yaml-cpp ARCHIVE DESTINATION "${_sdk_libraries}" COMPONENT AnalyzerSDK)
    install(DIRECTORY "${_sdk_source}/interface/" DESTINATION "${_sdk_headers}"
        COMPONENT AnalyzerSDK FILES_MATCHING PATTERN "*.h" PATTERN "*.hxx")
    install(FILES "${CMAKE_BINARY_DIR}/include/GitVersion.h" DESTINATION "${_sdk_headers}" COMPONENT AnalyzerSDK)
    install(FILES "${CMAKE_BINARY_DIR}/include/correctionlib_version.h"
        DESTINATION "${_sdk_headers}/dependencies" COMPONENT AnalyzerSDK)
    file(GLOB _sdk_plugins LIST_DIRECTORIES true "${_sdk_source}/plugins/*")
    set(SDK_PLUGIN_NAMES "")
    foreach(_sdk_plugin IN LISTS _sdk_plugins)
        if(IS_DIRECTORY "${_sdk_plugin}")
            file(GLOB _sdk_plugin_headers "${_sdk_plugin}/*.h" "${_sdk_plugin}/*.hxx")
            if(_sdk_plugin_headers)
                get_filename_component(_sdk_name "${_sdk_plugin}" NAME)
                list(APPEND SDK_PLUGIN_NAMES "${_sdk_name}")
                install(FILES ${_sdk_plugin_headers} DESTINATION "${_sdk_headers}/plugins/${_sdk_name}" COMPONENT AnalyzerSDK)
            endif()
        endif()
    endforeach()
    install(DIRECTORY "${_sdk_source}/extern/XGBoost-FastForest/include/"
        "${_sdk_source}/extern/correctionlib/include/" "${_sdk_source}/extern/yaml-cpp/include/"
        DESTINATION "${_sdk_headers}/dependencies" COMPONENT AnalyzerSDK FILES_MATCHING PATTERN "*.h" PATTERN "*.hxx" PATTERN "*.hpp"
        PATTERN "correctionlib_version.h" EXCLUDE)
    install(DIRECTORY "${ONNXRUNTIME_INCLUDE_DIRS}/" DESTINATION "${_sdk_headers}/dependencies/onnxruntime" COMPONENT AnalyzerSDK)
    rdf_install_onnx_runtime(COMPONENT AnalyzerSDK REQUIRED_NOTICES)
    foreach(_sdk_dependency IN ITEMS XGBoost-FastForest correctionlib yaml-cpp)
        install(FILES "${_sdk_source}/extern/${_sdk_dependency}/LICENSE"
            DESTINATION "${CMAKE_INSTALL_DATADIR}/rdfanalyzer/licenses/${_sdk_dependency}" COMPONENT AnalyzerSDK)
    endforeach()
    # correctionlib embeds these implementation dependencies into the archives.
    foreach(_sdk_notice IN ITEMS rapidjson/license.txt cpp-peglib/LICENSE xxhash/LICENSE pcg-cpp/LICENSE-MIT.txt pcg-cpp/LICENSE-APACHE.txt)
        get_filename_component(_sdk_notice_dir "${_sdk_notice}" DIRECTORY)
        install(FILES "${_sdk_source}/extern/correctionlib/${_sdk_notice}"
            DESTINATION "${CMAKE_INSTALL_DATADIR}/rdfanalyzer/licenses/correctionlib/${_sdk_notice_dir}" COMPONENT AnalyzerSDK)
    endforeach()
    install(FILES
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/CMakeLists.txt"
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/analysis.cc"
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/python_analysis.py"
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/physics.py"
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/steer.py"
        "${PROJECT_SOURCE_DIR}/examples/analyzer-sdk/README.md"
        DESTINATION "${CMAKE_INSTALL_DATADIR}/rdfanalyzer/examples/analyzer-sdk" COMPONENT AnalyzerSDK)
    set(SDK_CORE_FILE "${CMAKE_STATIC_LIBRARY_PREFIX}core${CMAKE_STATIC_LIBRARY_SUFFIX}")
    set(SDK_PLUGINS_FILE "${CMAKE_STATIC_LIBRARY_PREFIX}corePlugins${CMAKE_STATIC_LIBRARY_SUFFIX}")
    get_target_property(_sdk_yaml_name yaml-cpp OUTPUT_NAME)
    if(NOT _sdk_yaml_name)
        set(_sdk_yaml_name yaml-cpp)
    endif()
    set(SDK_YAML_FILE "${CMAKE_STATIC_LIBRARY_PREFIX}${_sdk_yaml_name}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    get_filename_component(SDK_ONNX_FILE "${ONNXRUNTIME_LIBRARY}" NAME)
    get_directory_property(SDK_FEATURE_DEFINITIONS DIRECTORY "${PROJECT_SOURCE_DIR}/core" COMPILE_DEFINITIONS)
    configure_package_config_file("${CMAKE_CURRENT_FUNCTION_LIST_DIR}/RDFAnalyzerCoreConfig.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/RDFAnalyzerCoreConfig.cmake"
        INSTALL_DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/RDFAnalyzerCore")
    install(FILES "${CMAKE_CURRENT_BINARY_DIR}/RDFAnalyzerCoreConfig.cmake"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/SetupSofieBLAS.cmake"
        DESTINATION "${CMAKE_INSTALL_LIBDIR}/cmake/RDFAnalyzerCore" COMPONENT AnalyzerSDK)
endfunction()
