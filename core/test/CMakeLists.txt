include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../extern/gtest/googletest/include ${CMAKE_CURRENT_SOURCE_DIR})

# Define the test source directory for all test executables
add_compile_definitions(TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")

# Basic functionality tests

add_executable(testConfigurationManager testConfigurationManager.cc)
target_link_libraries(testConfigurationManager core gtest gtest_main)
add_test(NAME ConfigurationManagerTest COMMAND testConfigurationManager)

add_executable(testConfigurationManager_Basic testConfigurationManager_Basic.cc)
target_link_libraries(testConfigurationManager_Basic core gtest gtest_main)
add_test(NAME ConfigurationManagerBasicTest COMMAND testConfigurationManager_Basic)

add_executable(testConfigurationManager_StringParsing testConfigurationManager_StringParsing.cc)
target_link_libraries(testConfigurationManager_StringParsing core gtest gtest_main)
add_test(NAME ConfigurationManagerStringParsingTest COMMAND testConfigurationManager_StringParsing)

add_executable(testConfigurationManager_ConfigParsing testConfigurationManager_ConfigParsing.cc)
target_link_libraries(testConfigurationManager_ConfigParsing core gtest gtest_main)
add_test(NAME ConfigurationManagerConfigParsingTest COMMAND testConfigurationManager_ConfigParsing)

add_executable(testConfigurationManager_ErrorHandling testConfigurationManager_ErrorHandling.cc)
target_link_libraries(testConfigurationManager_ErrorHandling core gtest gtest_main)
add_test(NAME ConfigurationManagerErrorHandlingTest COMMAND testConfigurationManager_ErrorHandling)

add_executable(testConfigurationManager_Immutability testConfigurationManager_Immutability.cc)
target_link_libraries(testConfigurationManager_Immutability core gtest gtest_main)
add_test(NAME ConfigurationManagerImmutabilityTest COMMAND testConfigurationManager_Immutability)

add_executable(testConfigurationManager_MemoryAndSemantics testConfigurationManager_MemoryAndSemantics.cc)
target_link_libraries(testConfigurationManager_MemoryAndSemantics core gtest gtest_main)
add_test(NAME ConfigurationManagerMemoryAndSemanticsTest COMMAND testConfigurationManager_MemoryAndSemantics)

add_executable(testSystematicManager testSystematicManager.cc)
target_link_libraries(testSystematicManager core gtest gtest_main)
add_test(NAME SystematicManagerTest COMMAND testSystematicManager)

add_executable(testCounterService testCounterService.cc)
target_link_libraries(testCounterService core gtest gtest_main)
add_test(NAME CounterServiceTest COMMAND testCounterService)

#add_executable(testDataManager testDataManager.cc)
#target_link_libraries(testDataManager core corePlugins gtest gtest_main)
#add_test(NAME DataManagerTest COMMAND testDataManager)





# Plugin tests

add_executable(testNamedObjectManager testNamedObjectManager.cc)
target_link_libraries(testNamedObjectManager core gtest gtest_main)
add_test(NAME NamedObjectManagerTest COMMAND testNamedObjectManager)

add_executable(testCorrectionManager testCorrectionManager.cc)
target_link_libraries(testCorrectionManager core corePlugins gtest gtest_main)
add_test(NAME CorrectionManagerTest COMMAND testCorrectionManager)

add_executable(testBDTManager testBDTManager.cc)
target_link_libraries(testBDTManager core corePlugins gtest gtest_main)
add_test(NAME BDTManagerTest COMMAND testBDTManager)

add_executable(testOnnxManager testOnnxManager.cc)
target_link_libraries(testOnnxManager core corePlugins gtest gtest_main)
add_test(NAME OnnxManagerTest COMMAND testOnnxManager)

add_executable(testSofieManager testSofieManager.cc)
target_link_libraries(testSofieManager core corePlugins gtest gtest_main)
add_test(NAME SofieManagerTest COMMAND testSofieManager)

add_executable(testNDHistogramManager testNDHistogramManager.cc)
target_link_libraries(testNDHistogramManager core corePlugins gtest gtest_main)
add_test(NAME NDHistogramManagerTest COMMAND testNDHistogramManager)

add_executable(testTriggerManager testTriggerManager.cc)
target_link_libraries(testTriggerManager core corePlugins gtest gtest_main)
add_test(NAME TriggerManagerTest COMMAND testTriggerManager)


add_executable(testAnalyzer_TriggerLogic testAnalyzer_TriggerLogic.cc)
target_link_libraries(testAnalyzer_TriggerLogic core corePlugins gtest gtest_main)
add_test(NAME AnalyzerTriggerLogicTest COMMAND testAnalyzer_TriggerLogic)

add_executable(testAnalyzer_PluginWiring testAnalyzer_PluginWiring.cc)
target_link_libraries(testAnalyzer_PluginWiring core corePlugins gtest gtest_main)
add_test(NAME AnalyzerPluginWiringTest COMMAND testAnalyzer_PluginWiring)

add_executable(testPluginLifecycle testPluginLifecycle.cc)
target_link_libraries(testPluginLifecycle core corePlugins gtest gtest_main)
add_test(NAME PluginLifecycleTest COMMAND testPluginLifecycle)

add_executable(testRootEnvironmentGuard testRootEnvironmentGuard.cc)
target_link_libraries(testRootEnvironmentGuard core gtest gtest_main)
add_test(NAME RootEnvironmentGuardTest COMMAND testRootEnvironmentGuard)

add_executable(testPlottingUtility testPlottingUtility.cc)
target_link_libraries(testPlottingUtility core gtest gtest_main)
add_test(NAME PlottingUtilityTest COMMAND testPlottingUtility)
if(TARGET rdfanalyzer AND Python_EXECUTABLE)
add_test(NAME PythonBindingsNumbaIntegration
         COMMAND ${Python_EXECUTABLE} ${CMAKE_CURRENT_SOURCE_DIR}/test_python_bindings.py)
# Prepend our build/python to PYTHONPATH but preserve the user's PYTHONPATH
# so imports provided by the user's environment (e.g. ROOT from an LCG view)
# remain available when running under CTest.
set_tests_properties(PythonBindingsNumbaIntegration PROPERTIES
                    ENVIRONMENT "PYTHONPATH=${CMAKE_BINARY_DIR}/python:$ENV{PYTHONPATH}"
                    SKIP_RETURN_CODE 77)
endif()


# -----------------------------------------------------------------------
# Pure-Python tests (pytest-based, no C++ build artefacts needed)
# -----------------------------------------------------------------------
# Use Python_EXECUTABLE set by pybind11/FindPython above; fall back to an
# explicit find_package so the tests are registered even when the Python
# bindings are disabled or pybind11 was not found.
if(NOT Python_EXECUTABLE)
    find_package(Python3 COMPONENTS Interpreter QUIET)
    if(Python3_FOUND)
        set(Python_EXECUTABLE "${Python3_EXECUTABLE}")
    endif()
endif()

if(Python_EXECUTABLE)
    set(CORE_PYTHON_DIR "${CMAKE_SOURCE_DIR}/core/python")

    # Thin wrapper copied to the build directory: runs pytest and exits with
    # code 77 (CTest SKIP) when pytest itself is not installed on the runner.
    configure_file(
        ${CMAKE_CURRENT_SOURCE_DIR}/run_pytest.py.in
        ${CMAKE_CURRENT_BINARY_DIR}/run_pytest.py
        COPYONLY
    )

    # Helper macro to register a single pytest test with a consistent set of
    # properties.  Each test prepends CORE_PYTHON_DIR to PYTHONPATH so that
    # all modules in core/python/ are importable without installation.
    macro(add_pytest_test TEST_NAME TEST_FILE)
        add_test(
            NAME ${TEST_NAME}
            COMMAND ${Python_EXECUTABLE} ${CMAKE_CURRENT_BINARY_DIR}/run_pytest.py
                    ${TEST_FILE} -v --tb=short -p no:cacheprovider
        )
        set_tests_properties(${TEST_NAME} PROPERTIES
            ENVIRONMENT "PYTHONPATH=${CORE_PYTHON_DIR}:$ENV{PYTHONPATH}"
            SKIP_RETURN_CODE 77
        )
    endmacro()

    # Output-schema tests (core/python/test_output_schema.py)
    add_pytest_test(PythonOutputSchemaTest
                    ${CORE_PYTHON_DIR}/test_output_schema.py)

    # YAML/text config-parsing tests (core/test/test_yaml_config.py)
    add_pytest_test(PythonYamlConfigTest
                    ${CMAKE_CURRENT_SOURCE_DIR}/test_yaml_config.py)
endif()


# Integration tests

#add_executable(testSystematicPropagation testSystematicPropagation.cc)
#target_link_libraries(testSystematicPropagation core corePlugins gtest gtest_main)
#add_test(NAME SystematicPropagationTest COMMAND testSystematicPropagation)


