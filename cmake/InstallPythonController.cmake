# Shared by the native build and the standalone, language-free controller build.
include_guard(GLOBAL)

function(rdf_install_python_controller)
    cmake_parse_arguments(PARSE_ARGV 0 _controller "" "COMPONENT" "")
    if(NOT _controller_COMPONENT OR _controller_UNPARSED_ARGUMENTS)
        message(FATAL_ERROR "rdf_install_python_controller requires COMPONENT <name>")
    endif()
    get_filename_component(_controller_source
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../core/python" ABSOLUTE)
    set(_controller_files _rdfjson.py rdfworkflow.py rdfworkflow.pyi rdfresults.py rdfresults.pyi
        rdfschema.py rdfschema.pyi
        run_bundle.py run_collection.py)
    foreach(_controller_file IN LISTS _controller_files)
        configure_file("${_controller_source}/${_controller_file}"
            "${CMAKE_BINARY_DIR}/python/${_controller_file}" COPYONLY)
    endforeach()
    list(TRANSFORM _controller_files PREPEND "${_controller_source}/")
    install(FILES ${_controller_files} DESTINATION python
        COMPONENT "${_controller_COMPONENT}")
    install(FILES
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/compiled-controller/steer.py"
        "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/../examples/compiled-controller/README.md"
        DESTINATION share/rdfanalyzer/examples/compiled-controller
        COMPONENT "${_controller_COMPONENT}")
endfunction()
