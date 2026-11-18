include_guard(GLOBAL)

# Shared payload for independent SDK and Python installation components.
# Both install the same selected runtime bytes and use one prefix-relative lib.
function(rdf_install_onnx_runtime)
    cmake_parse_arguments(PARSE_ARGV 0 _runtime "REQUIRED_NOTICES" "COMPONENT" "")
    if(NOT _runtime_COMPONENT OR _runtime_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "rdf_install_onnx_runtime requires COMPONENT <name>")
    endif()
    if(NOT ONNXRUNTIME_LIB_DIR)
        message(FATAL_ERROR "Selected ONNX runtime library directory is required")
    endif()
    install(DIRECTORY "${ONNXRUNTIME_LIB_DIR}/" DESTINATION lib COMPONENT "${_runtime_COMPONENT}"
        FILES_MATCHING PATTERN "*.so*" PATTERN "cmake" EXCLUDE PATTERN "pkgconfig" EXCLUDE)
    if(_runtime_REQUIRED_NOTICES)
        install(FILES "${ONNXRUNTIME_ROOT_DIR}/LICENSE" "${ONNXRUNTIME_ROOT_DIR}/ThirdPartyNotices.txt"
            DESTINATION share/rdfanalyzer/onnxruntime COMPONENT "${_runtime_COMPONENT}")
    else()
        install(FILES "${ONNXRUNTIME_ROOT_DIR}/LICENSE" "${ONNXRUNTIME_ROOT_DIR}/ThirdPartyNotices.txt"
            DESTINATION share/rdfanalyzer/onnxruntime COMPONENT "${_runtime_COMPONENT}" OPTIONAL)
    endif()
endfunction()
