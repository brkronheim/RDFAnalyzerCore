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
        native_config_io.py sample_config.py dataset_manifest.py run_bundle.py run_collection.py)
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

# The workflow controller has optional runtime dependencies (Snakemake, ROOT,
# fitting tools). Keep the language-free PythonController component lightweight.
function(rdf_install_snakemake_controller)
    set(_root "${CMAKE_CURRENT_FUNCTION_LIST_DIR}/..")
    set(_src "${_root}/core/python")
    set(_modules _rdfjson analytic_datacards condor_submission cpp_wrapper create_datacards sample_config dataset_manifest
        derivation_correctionlib extract_nd_histograms histogram_io io_policy job_sizing job_sizing_worker
        metadata_io native_config_io refresh_xrootd_runtime nuisance_groups opendata_discovery optional_extensions
        output_schema partial_merge partition_utils plot_config production_artifacts
        production_build production_campaign production_discovery production_downstream
        production_executors production_jobs production_joint_fit production_python
        production_stitching production_stitching_work rdf_production rdfresults rdfschema rdfworkflow
        run_bundle run_collection stitching_plotting submission_backend verified_outputs
        xrd_site_rankings xrootd_site_selector)
    foreach(_module IN LISTS _modules)
        install(FILES "${_src}/${_module}.py" DESTINATION python COMPONENT SnakemakeController)
    endforeach()
    install(FILES "${_src}/rdfworkflow.pyi" "${_src}/rdfresults.pyi" "${_src}/rdfschema.pyi"
        DESTINATION python COMPONENT SnakemakeController)
    install(PROGRAMS "${_src}/setup_python_env.sh" DESTINATION python COMPONENT SnakemakeController)
    install(DIRECTORY "${_src}/rdfproduction" "${_src}/statistical_inference"
        DESTINATION python COMPONENT SnakemakeController FILES_MATCHING PATTERN "*.py"
        PATTERN "__pycache__" EXCLUDE)
    install(DIRECTORY "${_root}/examples/snakemake/" DESTINATION share/rdfanalyzer/snakemake
        COMPONENT SnakemakeController PATTERN ".snakemake" EXCLUDE)
    install(FILES "${_root}/requirements-snakemake.txt" "${_root}/requirements-workflow.txt"
        DESTINATION share/rdfanalyzer COMPONENT SnakemakeController)
endfunction()
