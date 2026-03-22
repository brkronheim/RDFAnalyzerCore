include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../extern/gtest/googletest/include ${CMAKE_CURRENT_SOURCE_DIR})

# Define the test source directory for all test executables
add_compile_definitions(TEST_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}")
add_compile_definitions(TEST_BINARY_DIR="${CMAKE_BINARY_DIR}")

add_executable(testRootOutputSink testRootOutputSink.cc)
target_link_libraries(testRootOutputSink core gtest gtest_main)
add_test(NAME RootOutputSinkTest COMMAND testRootOutputSink)

add_executable(testProvenanceService testProvenanceService.cc)
target_link_libraries(testProvenanceService core gtest gtest_main)
add_test(NAME ProvenanceServiceTest COMMAND testProvenanceService)

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

add_executable(testConfigurationManager_YamlParsing testConfigurationManager_YamlParsing.cc)
target_link_libraries(testConfigurationManager_YamlParsing core gtest gtest_main)
add_test(NAME ConfigurationManagerYamlParsingTest COMMAND testConfigurationManager_YamlParsing)

add_executable(testManagerRegistry testManagerRegistry.cc)
target_link_libraries(testManagerRegistry coreAll)
add_test(NAME ManagerRegistryTest COMMAND testManagerRegistry)

add_executable(testSystematicManager testSystematicManager.cc)
target_link_libraries(testSystematicManager core gtest gtest_main)
add_test(NAME SystematicManagerTest COMMAND testSystematicManager)

add_executable(testCounterService testCounterService.cc)
target_link_libraries(testCounterService core gtest gtest_main)
add_test(NAME CounterServiceTest COMMAND testCounterService)

add_executable(testCounterService_Dedup testCounterService_Dedup.cc)
target_link_libraries(testCounterService_Dedup coreAll gtest gtest_main)
add_test(NAME CounterServiceDedupTest COMMAND testCounterService_Dedup)

add_executable(testDataManager testDataManager.cc)
target_link_libraries(testDataManager coreAll gtest gtest_main)
add_test(NAME DataManagerTest COMMAND testDataManager)





# Plugin tests

add_executable(testPluginLifecycle testPluginLifecycle.cc)
target_link_libraries(testPluginLifecycle coreAll gtest gtest_main)
add_test(NAME PluginLifecycleTest COMMAND testPluginLifecycle)

add_executable(testNamedObjectManager testNamedObjectManager.cc)
target_link_libraries(testNamedObjectManager core gtest gtest_main)
add_test(NAME NamedObjectManagerTest COMMAND testNamedObjectManager)

add_executable(testPhysicsObjectCollection testPhysicsObjectCollection.cc)
target_link_libraries(testPhysicsObjectCollection core gtest gtest_main)
add_test(NAME PhysicsObjectCollectionTest COMMAND testPhysicsObjectCollection)

add_executable(testCorrectionManager testCorrectionManager.cc)
target_link_libraries(testCorrectionManager coreAll gtest gtest_main)
add_test(NAME CorrectionManagerTest COMMAND testCorrectionManager)

add_executable(testBDTManager testBDTManager.cc)
target_link_libraries(testBDTManager coreAll gtest gtest_main)
add_test(NAME BDTManagerTest COMMAND testBDTManager)

add_executable(testOnnxManager testOnnxManager.cc)
target_link_libraries(testOnnxManager coreAll gtest gtest_main)
add_test(NAME OnnxManagerTest COMMAND testOnnxManager)

add_executable(testSofieManager testSofieManager.cc)
target_link_libraries(testSofieManager coreAll gtest gtest_main)
add_test(NAME SofieManagerTest COMMAND testSofieManager)

add_executable(testNDHistogramManager testNDHistogramManager.cc)
target_link_libraries(testNDHistogramManager coreAll gtest gtest_main)
add_test(NAME NDHistogramManagerTest COMMAND testNDHistogramManager)

add_executable(testNDHistogramManagerConfig testNDHistogramManagerConfig.cc)
target_link_libraries(testNDHistogramManagerConfig coreAll gtest gtest_main)
add_test(NAME NDHistogramManagerConfigTest COMMAND testNDHistogramManagerConfig)

add_executable(testTriggerManager testTriggerManager.cc)
target_link_libraries(testTriggerManager coreAll gtest gtest_main)
add_test(NAME TriggerManagerTest COMMAND testTriggerManager)

add_executable(testKinematicFitManager testKinematicFitManager.cc)
target_link_libraries(testKinematicFitManager coreAll gtest gtest_main)
add_test(NAME KinematicFitManagerTest COMMAND testKinematicFitManager)

if(USE_CUDA)
add_executable(testKinematicFitManagerGpu testKinematicFitManagerGpu.cc)
target_link_libraries(testKinematicFitManagerGpu coreAll gtest gtest_main)
add_test(NAME KinematicFitManagerGpuIntegrationTest COMMAND testKinematicFitManagerGpu)
endif()

add_executable(testCutflowManager testCutflowManager.cc)
target_link_libraries(testCutflowManager coreAll gtest gtest_main)
add_test(NAME CutflowManagerTest COMMAND testCutflowManager)

add_executable(testJetEnergyScaleManager testJetEnergyScaleManager.cc)
target_link_libraries(testJetEnergyScaleManager coreAll gtest gtest_main)
add_test(NAME JetEnergyScaleManagerTest COMMAND testJetEnergyScaleManager)

add_executable(testLeptonPhotonEnergyManagers testLeptonPhotonEnergyManagers.cc)
target_link_libraries(testLeptonPhotonEnergyManagers coreAll gtest gtest_main)
add_test(NAME LeptonPhotonEnergyManagersTest COMMAND testLeptonPhotonEnergyManagers)

add_executable(testRegionManager testRegionManager.cc)
target_link_libraries(testRegionManager coreAll gtest gtest_main)
add_test(NAME RegionManagerTest COMMAND testRegionManager)

add_executable(testWeightManager testWeightManager.cc)
target_link_libraries(testWeightManager coreAll gtest gtest_main)
add_test(NAME WeightManagerTest COMMAND testWeightManager)

add_executable(testGoldenJsonManager testGoldenJsonManager.cc)
target_link_libraries(testGoldenJsonManager coreAll gtest gtest_main)
add_test(NAME GoldenJsonManagerTest COMMAND testGoldenJsonManager)


add_executable(testAnalyzer_TriggerLogic testAnalyzer_TriggerLogic.cc)
target_link_libraries(testAnalyzer_TriggerLogic coreAll gtest gtest_main)
add_test(NAME AnalyzerTriggerLogicTest COMMAND testAnalyzer_TriggerLogic)

add_executable(testAnalyzer_PluginWiring testAnalyzer_PluginWiring.cc)
target_link_libraries(testAnalyzer_PluginWiring coreAll gtest gtest_main)
add_test(NAME AnalyzerPluginWiringTest COMMAND testAnalyzer_PluginWiring)

add_executable(testAnalyzer_ManagerContextPersistence testAnalyzer_ManagerContextPersistence.cc)
target_link_libraries(testAnalyzer_ManagerContextPersistence coreAll gtest gtest_main)
add_test(NAME AnalyzerManagerContextPersistenceTest COMMAND testAnalyzer_ManagerContextPersistence)

add_executable(testRootEnvironmentGuard testRootEnvironmentGuard.cc)
target_link_libraries(testRootEnvironmentGuard core gtest gtest_main)
add_test(NAME RootEnvironmentGuardTest COMMAND testRootEnvironmentGuard)

add_executable(testPlottingUtility testPlottingUtility.cc)
target_link_libraries(testPlottingUtility core gtest gtest_main)
add_test(NAME PlottingUtilityTest COMMAND testPlottingUtility)

if(ONNXRUNTIME_USE_CUDA)
add_executable(testOnnxManagerGpu testOnnxManagerGpu.cc)
target_link_libraries(testOnnxManagerGpu coreAll gtest gtest_main)
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
target_link_libraries(testSystematicPropagation coreAll gtest gtest_main)
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



# ---------------------------------------------------------------------------
# core/python Python tests
# ---------------------------------------------------------------------------
find_package(Python3 COMPONENTS Interpreter QUIET)
if(Python3_FOUND)
    set(_CORE_PY "${CMAKE_SOURCE_DIR}/core/python")
    # Configure the pytest runner shim that exits non-zero when pytest is absent
    # so the test is visible as FAILED rather than silently skipped.
    configure_file(run_pytest.py.in "${CMAKE_BINARY_DIR}/run_pytest.py" @ONLY)

    # PYTHONPATH for most tests (core/python only)
    set(_PYPATH "${_CORE_PY}:$ENV{PYTHONPATH}")
    # test_submission_backend does `from core.python.xxx import`, so it also needs
    # the repo root on PYTHONPATH.
    set(_PYPATH_WITH_ROOT_REPO "${CMAKE_SOURCE_DIR}:${_CORE_PY}:$ENV{PYTHONPATH}")
    # ROOT Python bindings path (for tests that import ROOT directly).
    # ROOT_LIB_DIR is set as a CACHE INTERNAL variable in the top-level CMakeLists.
    set(_PYPATH_ROOT "${ROOT_LIB_DIR}:${_CORE_PY}:$ENV{PYTHONPATH}")

    # Ensure a local tmp directory exists in the build tree so that tests which
    # execute shell scripts are not affected by noexec mounts on the system /tmp.
    file(MAKE_DIRECTORY "${CMAKE_BINARY_DIR}/tmp")
    set(_EXEC_TMPDIR "TMPDIR=${CMAKE_BINARY_DIR}/tmp")

    # pytest-based tests
    macro(add_core_pytest _name _stem _pypath)
        add_test(
            NAME    ${_name}
            COMMAND ${Python3_EXECUTABLE} ${CMAKE_BINARY_DIR}/run_pytest.py
                    ${_CORE_PY}/${_stem}.py -v
        )
        set_tests_properties(${_name} PROPERTIES
            ENVIRONMENT "PYTHONPATH=${_pypath}"
        )
    endmacro()

    add_core_pytest(CorePyAnalysisConfigValidationTest  test_analysis_config_validation  "${_PYPATH}")
    add_core_pytest(CorePyDatasetManifestTest            test_dataset_manifest            "${_PYPATH}")
    add_core_pytest(CorePyNuisanceGroupsTest             test_nuisance_groups             "${_PYPATH}")
    add_core_pytest(CorePyOutputSchemaTest               test_output_schema               "${_PYPATH}")
    add_core_pytest(CorePyRegionSchemaTest               test_region_schema               "${_PYPATH}")
    add_core_pytest(CorePyReproducibilityReportTest      test_reproducibility_report      "${_PYPATH}")
    add_core_pytest(CorePyValidateConfigTest             test_validate_config             "${_PYPATH}")
    add_core_pytest(CorePyValidationReportTest           test_validation_report           "${_PYPATH}")
    add_core_pytest(CorePyVariationOrchestratorTest      test_variation_orchestrator      "${_PYPATH}")
    # test_submission_backend uses absolute `core.python.*` imports so it needs
    # both core/python and the repo root on PYTHONPATH.
    add_core_pytest(CorePySubmissionBackendTest          test_submission_backend          "${_PYPATH_WITH_ROOT_REPO}")

    # unittest-based tests (have __main__ guard)
    macro(add_core_unittest _name _stem _pypath)
        add_test(
            NAME    ${_name}
            COMMAND ${Python3_EXECUTABLE} ${_CORE_PY}/${_stem}.py
        )
        set_tests_properties(${_name} PROPERTIES
            ENVIRONMENT "PYTHONPATH=${_pypath}"
        )
    endmacro()

    add_core_unittest(CorePyProductionManagerTest  test_production_manager  "${_PYPATH}")
    # test_uproot_datacard has a pre-existing uproot TAxis API mismatch; registered
    # so the failure is visible in CI rather than silently absent.
    add_core_unittest(CorePyUprootDatacardTest     test_uproot_datacard     "${_PYPATH}")

    # test_datacard_generator used to require ROOT Python bindings but has
    # been rewritten to use uproot; no special environment variable is needed.
    add_test(
        NAME    CorePyDatacardGeneratorTest
        COMMAND ${Python3_EXECUTABLE} ${_CORE_PY}/test_datacard_generator.py
    )
    set_tests_properties(CorePyDatacardGeneratorTest PROPERTIES
        ENVIRONMENT "PYTHONPATH=${_PYPATH}"
    )
endif()
