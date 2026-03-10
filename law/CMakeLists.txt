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

macro(add_law_unittest _test_name _file_stem)
    add_test(
        NAME    ${_test_name}
        COMMAND ${Python3_EXECUTABLE} ${_LAW_DIR}/${_file_stem}.py
    )
    set_tests_properties(${_test_name} PROPERTIES
        ENVIRONMENT "PYTHONPATH=${_PYTHONPATH}"
    )
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
