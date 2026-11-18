include_guard(GLOBAL)
include("${CMAKE_CURRENT_LIST_DIR}/InstallPythonController.cmake")

function(rdf_install_python_authoring)
    cmake_parse_arguments(PARSE_ARGV 0 _authoring "" "COMPONENT" "")
    if(NOT _authoring_COMPONENT OR _authoring_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "rdf_install_python_authoring requires COMPONENT <name>")
    endif()
    get_filename_component(_authoring_source "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../core/python" ABSOLUTE)
    set(_authoring_files _rdftext.py rdfanalysis.py rdfanalysis.pyi rdfsettings.py rdfsettings.pyi
        rdfcolumns.py rdfcolumns.pyi rdfdeclarations.py rdfdeclarations.pyi
        rdfmodules.py rdfmodules.pyi rdfvalidation.py rdfvalidation.pyi optional_extensions.py rdftnp.py rdftnp.pyi)
    foreach(_authoring_file IN LISTS _authoring_files)
        configure_file("${_authoring_source}/${_authoring_file}"
            "${CMAKE_BINARY_DIR}/python/${_authoring_file}" COPYONLY)
    endforeach()
    list(TRANSFORM _authoring_files PREPEND "${_authoring_source}/")
    install(FILES ${_authoring_files} DESTINATION python COMPONENT "${_authoring_COMPONENT}")
    # rdftnp JITs this stdlib-only header; the Python component must be relocatable
    # without requiring a separate NativeAuthoring installation.
    install(FILES "${_authoring_source}/../interface/TagAndProbe.h"
        DESTINATION include/rdfanalyzer COMPONENT "${_authoring_COMPONENT}")
    rdf_install_python_controller(COMPONENT "${_authoring_COMPONENT}")
    install(FILES
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/python-authoring/analysis.py"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/python-authoring/README.md"
        DESTINATION share/rdfanalyzer/examples/python-authoring COMPONENT "${_authoring_COMPONENT}")
    install(FILES
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/systematics-inference/analysis.py"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/systematics-inference/toy_bdt.txt"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/systematics-inference/README.md"
        DESTINATION share/rdfanalyzer/examples/systematics-inference COMPONENT "${_authoring_COMPONENT}")
endfunction()
