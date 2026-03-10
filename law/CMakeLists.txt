find_package(Python3 COMPONENTS Interpreter QUIET)
if(NOT Python3_FOUND)
    message(STATUS "Python3 not found; skipping law Python test registration.")
    return()
endif()

set(_LAW_DIR  "${CMAKE_SOURCE_DIR}/law")
set(_CORE_PY  "${CMAKE_SOURCE_DIR}/core/python")
# law/ itself on PYTHONPATH makes combine_tasks, merge_tasks, etc. importable
# by their bare module names.  The real 'law' PyPI package is found downstream
# in site-packages (namespace packages lose to regular packages).
set(_PYTHONPATH "${_LAW_DIR}:${_CORE_PY}:$ENV{PYTHONPATH}")

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
        COMMAND ${Python3_EXECUTABLE} ${_LAW_DIR}/${_file_stem}.py
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

add_law_unittest(LawAnalysisTasksTest       test_analysis_tasks)
add_law_unittest(LawBranchMapPolicyTest     test_branch_map_policy)
add_law_unittest(LawCombineTasksTest        test_combine_tasks)
add_law_unittest(LawDagTasksTest            test_dag_tasks)
add_law_unittest(LawExecutorParityTest      test_executor_parity)
add_law_unittest(LawFailureHandlerTest      test_failure_handler)
add_law_unittest(LawMergeTasksTest          test_merge_tasks)
add_law_unittest(LawNanoTasksTest           test_nano_tasks)
add_law_unittest(LawOpenDataTasksTest       test_opendata_tasks)
add_law_unittest(LawPerformanceRecorderTest test_performance_recorder)
add_law_unittest(LawWorkflowExecutorsTest   test_workflow_executors)
