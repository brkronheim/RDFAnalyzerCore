cmake_minimum_required(VERSION 3.16)
project(RDFNativeAuthoringChecks LANGUAGES CXX)
enable_testing()
include("${CMAKE_CURRENT_LIST_DIR}/Targets.cmake")
find_package(Python3 3.10 COMPONENTS Interpreter QUIET)
if(Python3_FOUND)
  add_test(NAME NativeValidationReportInteropTest
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_LIST_DIR}/test_interop.py" -v)
  set_tests_properties(NativeValidationReportInteropTest PROPERTIES
    ENVIRONMENT "RDF_COMMON_SETTINGS_FIXTURE=$<TARGET_FILE:testCommonSettings>;RDF_NATIVE_PREFLIGHT_FIXTURE=$<TARGET_FILE:testNativePreflight>")
  add_test(NAME NativeRunResultInteropTest
    COMMAND "${Python3_EXECUTABLE}" "${CMAKE_CURRENT_LIST_DIR}/test_run_result_interop.py" -v)
  set_tests_properties(NativeRunResultInteropTest PROPERTIES
    ENVIRONMENT "RDF_RUN_RESULT_FIXTURE=$<TARGET_FILE:testRunResult>")
endif()
