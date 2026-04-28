# Test configuration for C++ and Python framework tests
# C++ tests are in core/tests/cpp; Python tests for law and core/python are here

add_subdirectory(cpp)

set(_PYTHON_EXECUTABLE "")

if(DEFINED ENV{VIRTUAL_ENV})
    set(_PYTHON_EXECUTABLE "$ENV{VIRTUAL_ENV}/bin/python")
    if(NOT EXISTS "${_PYTHON_EXECUTABLE}")
        unset(_PYTHON_EXECUTABLE)
    else()
        message(STATUS "Using Python3 interpreter from virtual environment: ${_PYTHON_EXECUTABLE}")
    endif()
endif()

if(NOT DEFINED _PYTHON_EXECUTABLE AND DEFINED Python3_EXECUTABLE)
    set(_PYTHON_EXECUTABLE "${Python3_EXECUTABLE}")
endif()

if(NOT DEFINED _PYTHON_EXECUTABLE)
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_FOUND)
        set(_PYTHON_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()
endif()

if(NOT DEFINED _PYTHON_EXECUTABLE OR _PYTHON_EXECUTABLE STREQUAL "")
    message(STATUS "Python3 not found; skipping Python test registration.")
    return()
endif()

set(_LAW_DIR  "${CMAKE_SOURCE_DIR}/core/python/law")
set(_CORE_PY  "${CMAKE_SOURCE_DIR}/core/python")
set(_TEST_DIR "${CMAKE_SOURCE_DIR}/core/tests")

# Add the repository source root so imports like `core.python.submission_backend`
# can resolve when CTest runs from the build directory.
set(_PYTHONPATH "${CMAKE_SOURCE_DIR}:${_LAW_DIR}:${_CORE_PY}:$ENV{PYTHONPATH}")

# Several tests invoke _run_analysis_job() which creates shell scripts in a
# temporary directory and then exec()s them.  If the system /tmp is mounted
# noexec (common in hardened CI environments), those scripts fail with exit 126.
# Point TMPDIR at a directory inside the build tree which is guaranteed to be
# on an exec-capable filesystem.
file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/tmp")
set(_EXEC_TMPDIR "TMPDIR=${CMAKE_BINARY_DIR}/tmp")

# Tests that execute shell scripts from tmpdir need TMPDIR overridden.
set(_EXEC_TESTS
    LawWorkflowExecutorsTest
    LawExecutorParityTest
    LawPerformanceRecorderTest
    LawAnalysisTasksTest
    LawCombineTasksTest
    LawMergeTasksTest
)

macro(add_law_unittest _test_name _file_stem)
    add_test(
        NAME    ${_test_name}
        COMMAND ${_PYTHON_EXECUTABLE} ${_TEST_DIR}/law/${_file_stem}.py
    )
    # Check if this test needs the TMPDIR override
    list(FIND _EXEC_TESTS "${_test_name}" _idx)
    if(_idx GREATER_EQUAL 0)
        set_tests_properties(${_test_name} PROPERTIES
            ENVIRONMENT "PYTHONPATH=${_PYTHONPATH};${_EXEC_TMPDIR}"
        )
    else()
        set_tests_properties(${_test_name} PROPERTIES
            ENVIRONMENT "PYTHONPATH=${_PYTHONPATH}"
        )
    endif()
endmacro()

# Law/workflow tests
add_law_unittest(LawAnalysisTasksTest       test_analysis_tasks)
add_law_unittest(LawBranchMapPolicyTest     test_branch_map_policy)
add_law_unittest(LawCombineTasksTest        test_combine_tasks)
add_law_unittest(LawDagTasksTest            test_dag_tasks)
add_law_unittest(LawExecutorParityTest      test_executor_parity)
add_law_unittest(LawFailureHandlerTest      test_failure_handler)
add_law_unittest(LawMergeTasksTest          test_merge_tasks)
add_law_unittest(LawManifestPlotTasksTest   test_manifest_plot_tasks)
add_law_unittest(LawRucioTasksTest          test_rucio_tasks)
add_law_unittest(LawOpenDataTasksTest       test_opendata_tasks)
add_law_unittest(LawPlotTasksTest             test_plot_tasks)
add_law_unittest(LawPerformanceRecorderTest test_performance_recorder)
add_law_unittest(LawPartitionUtilsTest      test_partition_utils)
add_law_unittest(LawXRootDTasksTest         test_xrdfs_tasks)
add_law_unittest(LawWorkflowExecutorsTest   test_workflow_executors)

# Python framework tests (from core/python, now in core/tests/python)
# These are registered as generic Python tests without special TMPDIR handling
macro(add_python_unittest _test_name _file_stem)
    add_test(
        NAME    ${_test_name}
        COMMAND ${_PYTHON_EXECUTABLE} ${_TEST_DIR}/python/${_file_stem}.py
    )
    set_tests_properties(${_test_name} PROPERTIES
        ENVIRONMENT "PYTHONPATH=${_PYTHONPATH}"
    )
endmacro()

add_python_unittest(PythonAnalysisConfigValidationTest test_analysis_config_validation)
add_python_unittest(PythonCppWrapperTest               test_cpp_wrapper)
add_python_unittest(PythonDatacardGeneratorTest        test_datacard_generator)
add_python_unittest(PythonDatasetManifestTest          test_dataset_manifest)
add_python_unittest(PythonNuisanceGroupsTest           test_nuisance_groups)
add_python_unittest(PythonOutputSchemaTest             test_output_schema)
add_python_unittest(PythonProductionManagerTest        test_production_manager)
add_python_unittest(PythonRegionSchemaTest             test_region_schema)
add_python_unittest(PythonReproducibilityReportTest    test_reproducibility_report)
add_python_unittest(PythonSubmissionBackendTest        test_submission_backend)
add_python_unittest(PythonUprootDatacardTest           test_uproot_datacard)
add_python_unittest(PythonValidateConfigTest           test_validate_config)
add_python_unittest(PythonValidationReportTest         test_validation_report)
add_python_unittest(PythonVariationOrchestratorTest    test_variation_orchestrator)
add_python_unittest(PythonVersionInfoTest             test_version_info)
add_python_unittest(PythonProductionMonitorTest       test_production_monitor)
add_python_unittest(PythonRucioDiscoveryTest           test_rucio_discovery)
add_python_unittest(PythonOpenDataDiscoveryTest        test_opendata_discovery)
add_python_unittest(PythonConvertConfigTest            test_convert_config)
add_python_unittest(PythonResubmitJobsTest             test_resubmit_jobs)
add_python_unittest(PythonValidateDatacardGeneratorTest test_validate_datacard_generator)
add_python_unittest(PythonYamlConfigTest               test_yaml_config)
