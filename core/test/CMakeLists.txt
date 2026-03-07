include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../extern/gtest/googletest/include ${CMAKE_CURRENT_SOURCE_DIR})

# Define the test source directory for all test executables
add_compile_definitions(TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
add_compile_definitions(TEST_BINARY_DIR="${CMAKE_BINARY_DIR}")

add_executable(testRootOutputSink testRootOutputSink.cc)
target_link_libraries(testRootOutputSink core gtest gtest_main)
add_test(NAME RootOutputSinkTest COMMAND testRootOutputSink)

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

#add_executable(testCounterService_Dedup testCounterService_Dedup.cc)
#target_link_libraries(testCounterService_Dedup core corePlugins gtest gtest_main)
#add_test(NAME CounterServiceDedupTest COMMAND testCounterService_Dedup)

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

add_executable(testNDHistogramManagerConfig testNDHistogramManagerConfig.cc)
target_link_libraries(testNDHistogramManagerConfig core corePlugins gtest gtest_main)
add_test(NAME NDHistogramManagerConfigTest COMMAND testNDHistogramManagerConfig)

add_executable(testTriggerManager testTriggerManager.cc)
target_link_libraries(testTriggerManager core corePlugins gtest gtest_main)
add_test(NAME TriggerManagerTest COMMAND testTriggerManager)

add_executable(testKinematicFitManager testKinematicFitManager.cc)
target_link_libraries(testKinematicFitManager core corePlugins gtest gtest_main)
add_test(NAME KinematicFitManagerTest COMMAND testKinematicFitManager)

if(USE_CUDA)
add_executable(testKinematicFitManagerGpu testKinematicFitManagerGpu.cc)
target_link_libraries(testKinematicFitManagerGpu core corePlugins gtest gtest_main)
add_test(NAME KinematicFitManagerGpuIntegrationTest COMMAND testKinematicFitManagerGpu)
endif()

add_executable(testGoldenJsonManager testGoldenJsonManager.cc)
target_link_libraries(testGoldenJsonManager core corePlugins gtest gtest_main)
add_test(NAME GoldenJsonManagerTest COMMAND testGoldenJsonManager)


add_executable(testAnalyzer_TriggerLogic testAnalyzer_TriggerLogic.cc)
target_link_libraries(testAnalyzer_TriggerLogic core corePlugins gtest gtest_main)
add_test(NAME AnalyzerTriggerLogicTest COMMAND testAnalyzer_TriggerLogic)

add_executable(testAnalyzer_PluginWiring testAnalyzer_PluginWiring.cc)
target_link_libraries(testAnalyzer_PluginWiring core corePlugins gtest gtest_main)
add_test(NAME AnalyzerPluginWiringTest COMMAND testAnalyzer_PluginWiring)

add_executable(testAnalyzer_ManagerContextPersistence testAnalyzer_ManagerContextPersistence.cc)
target_link_libraries(testAnalyzer_ManagerContextPersistence core corePlugins gtest gtest_main)
add_test(NAME AnalyzerManagerContextPersistenceTest COMMAND testAnalyzer_ManagerContextPersistence)

add_executable(testRootEnvironmentGuard testRootEnvironmentGuard.cc)
target_link_libraries(testRootEnvironmentGuard core gtest gtest_main)
add_test(NAME RootEnvironmentGuardTest COMMAND testRootEnvironmentGuard)

add_executable(testPlottingUtility testPlottingUtility.cc)
target_link_libraries(testPlottingUtility core gtest gtest_main)
add_test(NAME PlottingUtilityTest COMMAND testPlottingUtility)

if(ONNXRUNTIME_USE_CUDA)
add_executable(testOnnxManagerGpu testOnnxManagerGpu.cc)
target_link_libraries(testOnnxManagerGpu core corePlugins gtest gtest_main)
add_test(NAME OnnxManagerGpuIntegrationTest COMMAND testOnnxManagerGpu)
endif()

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




# Integration tests

add_executable(testSystematicPropagation testSystematicPropagation.cc)
target_link_libraries(testSystematicPropagation core corePlugins gtest gtest_main)
add_test(NAME SystematicPropagationTest COMMAND testSystematicPropagation)

if(BUILD_COMBINE)
add_test(
    NAME CombineConfigurationTest
    COMMAND ${CMAKE_COMMAND}
        -DCOMBINE_PREFIX=${COMBINE_PREFIX}
        -DCOMBINE_EXECUTABLE=${COMBINE_EXECUTABLE}
        -DCOMBINE_BIN_DIR=${COMBINE_BIN_DIR}
        -DCOMBINE_LIBRARY_DIR=${COMBINE_LIBRARY_DIR}
        -DCOMBINE_PYTHON_DIR=${COMBINE_PYTHON_DIR}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/VerifyCombineConfig.cmake)
endif()

if(BUILD_COMBINE_HARVESTER)
add_test(
    NAME CombineHarvesterConfigurationTest
    COMMAND ${CMAKE_COMMAND}
        -DCOMBINEHARVESTER_ROOT_DIR=${COMBINEHARVESTER_ROOT_DIR}
        -DCOMBINEHARVESTER_TOOLS_DIR=${COMBINEHARVESTER_TOOLS_DIR}
        -DCOMBINEHARVESTER_PYTHON_DIR=${COMBINEHARVESTER_PYTHON_DIR}
        -DCOMBINEHARVESTER_PDFS_DIR=${COMBINEHARVESTER_PDFS_DIR}
        -DCOMBINEHARVESTER_LIBRARY_DIR=${COMBINEHARVESTER_LIBRARY_DIR}
        -P ${CMAKE_CURRENT_SOURCE_DIR}/VerifyCombineHarvesterConfig.cmake)
endif()


