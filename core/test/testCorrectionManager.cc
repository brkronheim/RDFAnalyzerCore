/**
 * @file testCorrectionManager.cc
 * @brief Unit tests for the CorrectionManager class
 * @date 2025
 *
 * This file contains comprehensive unit tests for the CorrectionManager class.
 * The tests cover basic functionality, correction application, error handling,
 * edge cases, and integration with ROOT RDataFrame to ensure the manager
 * works correctly in all scenarios.
 */

#include <ConfigurationManager.h>
#include <test_util.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <atomic>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>
#include <vector>
#include <ManagerFactory.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <api/ManagerContext.h>
#include <ROOT/TThreadExecutor.hxx>
#include <ROOT/RDF/RCutFlowReport.hxx>
#include <ROOT/RDF/RInterface.hxx>
#include <ROOT/RDFHelpers.hxx>
#include <SystematicManager.h>

/**
 * @brief Test fixture for CorrectionManager tests
 *
 * This class provides a common setup and teardown for all CorrectionManager
 * tests. It creates a CorrectionManager instance with a predefined
 * configuration file and ensures the correction library configuration is
 * available in the working directory.
 */
class CorrectionManagerTest : public ::testing::Test {
protected:
  /**
   * @brief Set up the test fixture
   *
   * Copies the correction configuration file to the working directory and
   * creates ConfigurationManager and CorrectionManager instances for testing.
   */
  void SetUp() override {
    ChangeToTestSourceDir();
    // Ensure correctionlibConfig (multi-key format) is available in the working directory
    std::string src = "cfg/correction.txt";
    std::string dst = "correctionlibConfig";
    std::ifstream srcFile(src, std::ios::binary);
    std::ofstream dstFile(dst, std::ios::binary);
    dstFile << srcFile.rdbuf();
    srcFile.close();
    dstFile.close();
    std::string configFile = "cfg/test_data_config_minimal.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    systematicManager = std::make_unique<SystematicManager>();
    dataManager = std::make_unique<DataManager>(2);
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();

    // Create CorrectionManager and set up its dependencies
    correctionManager = std::make_unique<CorrectionManager>(*configManager);

    // Set up the CorrectionManager with its dependencies
    ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    correctionManager->setContext(ctx);
  }

  /**
   * @brief Tear down the test fixture
   *
   * Cleans up the manager instances to prevent memory leaks.
   */
  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }

  void setContextFor(DataManager& manager) {
    ManagerContext ctx{*configManager, manager, *systematicManager, *logger, *skimSink, *metaSink};
    correctionManager->setContext(ctx);
  }

  std::unique_ptr<IConfigurationProvider> configManager; ///< The configuration manager instance for testing
  std::unique_ptr<CorrectionManager> correctionManager; ///< The correction manager instance for testing
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DataManager> dataManager; ///< The data manager instance for testing
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// ============================================================================
// Basic Functionality Tests
// ============================================================================

// Note: Basic constructor and getter functionality is tested in
// testNamedObjectManager.cc since CorrectionManager inherits from
// NamedObjectManager<correction::Correction::Ref>

// ============================================================================
// Error Handling Tests
// ============================================================================

// Note: Basic error handling for missing objects/features is tested in
// testNamedObjectManager.cc The following test focuses specifically on
// CorrectionManager's applyCorrection method

/**
 * @brief Test error handling for applyCorrection with missing correction
 *
 * Verifies that the applyCorrection method throws an appropriate exception
 * when attempting to apply a non-existent correction to a dataframe.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionThrowsForMissing) {
  std::vector<std::string> stringArguments = {"test"};
  EXPECT_THROW({
    correctionManager->applyCorrection("nonexistent_correction", stringArguments);
  }, std::runtime_error);
}

// ============================================================================
// Correction Application Tests
// ============================================================================

/**
 * @brief Test successful application of corrections
 *
 * Verifies that corrections can be successfully applied to a dataframe and
 * that the resulting values match the expected correction factors based on
 * the input parameters.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionPositive) {
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 2);
  EXPECT_NEAR(result->at(0), 0.1, 1e-6);
  EXPECT_NEAR(result->at(1), 0.4, 1e-6);
}

/**
 * @brief Test correction application with different string arguments
 *
 * Verifies that corrections work correctly with different string argument
 * values, ensuring that the categorical branching in the correction works as
 * expected.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionWithDifferentStringArguments) {
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"B"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_B");
  ASSERT_EQ(result->size(), 2);
  EXPECT_NEAR(result->at(0), 0.5, 1e-6);
  EXPECT_NEAR(result->at(1), 0.8, 1e-6);
}

// ============================================================================
// Edge Cases and Robustness Tests
// ============================================================================

/**
 * @brief Test correction application with boundary values
 *
 * Verifies that corrections handle boundary values correctly, including
 * values at the edges of bins and extreme values.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionWithBoundaryValues) {
  // Create a new DataManager for this test with 4 entries
  auto testDataManager = std::make_unique<DataManager>(4);
  setContextFor(*testDataManager);
  
  testDataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0 ? 0.0 : (i == 1 ? 1.0 : (i == 2 ? 2.0 : 3.0));
                },
                {"rdfentry_"}, *systematicManager);
  testDataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 4);
  EXPECT_NEAR(result->at(0), 0.1, 1e-6); // 0.0 falls in first bin [0.0, 1.0)
  EXPECT_NEAR(result->at(1), 0.2, 1e-6); // 1.0 falls in second bin [1.0, 2.0)
  EXPECT_NEAR(result->at(2), 0.0, 1e-6); // 2.0 is overflow (>=2.0)
  EXPECT_NEAR(result->at(3), 0.0, 1e-6); // 3.0 is overflow (>=2.0)
  
  // Restore the original data manager
  setContextFor(*dataManager);
}

/**
 * @brief Test correction application with empty dataframe
 *
 * Verifies that the correction application works correctly with an empty
 * dataframe, ensuring no crashes or unexpected behavior.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionWithEmptyDataframe) {
  // Create a new DataManager for this test with 0 entries
  auto testDataManager = std::make_unique<DataManager>(0);
  setContextFor(*testDataManager);
  
  // Define required columns even for empty dataframe
  testDataManager->Define("float_arg", []() -> double { return 0.5; }, {}, *systematicManager);
  testDataManager->Define("int_arg", []() -> double { return 1; }, {}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 0);
}

// ============================================================================
// Integration Tests
// ============================================================================

// Note: Basic getter functionality is tested in testNamedObjectManager.cc
// The following tests focus on CorrectionManager-specific integration

/**
 * @brief Test configuration file integration
 *
 * Verifies that the CorrectionManager correctly integrates with the
 * ConfigurationManager and can read correction configurations from files.
 */
TEST_F(CorrectionManagerTest, ConfigurationFileIntegration) {
  // Verify that the configuration was loaded correctly
  EXPECT_NO_THROW(
      { auto corr = correctionManager->getCorrection("test_correction"); });

  // Verify that the correction has the expected structure
  const auto &features =
      correctionManager->getCorrectionFeatures("test_correction");
  EXPECT_EQ(features.size(), 2);
  EXPECT_EQ(features[0], "float_arg");
  EXPECT_EQ(features[1], "int_arg");
}

// ============================================================================
// Data Type Edge Case Tests
// ============================================================================

/**
 * @brief Test handling of extreme numeric values
 *
 * Verifies that the CorrectionManager handles very large and very small
 * numeric values correctly without overflow or precision issues.
 */
TEST_F(CorrectionManagerTest, ExtremeNumericValues) {
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(4);
  setContextFor(*testDataManager);
  
  testDataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0   ? std::numeric_limits<double>::max()
                         : i == 1 ? std::numeric_limits<double>::min()
                         : i == 2 ? -std::numeric_limits<double>::max()
                                  : std::numeric_limits<double>::lowest();
                },
                {"rdfentry_"}, *systematicManager);
  testDataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection("test_correction", stringArguments);
  });
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 4);
  for (size_t i = 0; i < result->size(); ++i) {
    EXPECT_TRUE(std::abs(result->at(i)) < 1e-6 ||
                std::abs(result->at(i) - 0.1) < 1e-6 ||
                std::abs(result->at(i) - 0.2) < 1e-6);
  }
  
  // Restore the original data manager
  setContextFor(*dataManager);
}

/**
 * @brief Test handling of NaN and infinity values
 *
 * Verifies that the CorrectionManager handles NaN and infinity values
 * gracefully without crashing or producing undefined behavior.
 */
TEST_F(CorrectionManagerTest, NaNAndInfinityHandling) {
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(3);
  setContextFor(*testDataManager);
  
  testDataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0   ? std::numeric_limits<double>::quiet_NaN()
                         : i == 1 ? std::numeric_limits<double>::infinity()
                                  : -std::numeric_limits<double>::infinity();
                },
                {"rdfentry_"}, *systematicManager);
  testDataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection("test_correction", stringArguments);
  });
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 3);
  for (size_t i = 0; i < result->size(); ++i) {
    EXPECT_TRUE(std::isnan(result->at(i)) || std::abs(result->at(i)) < 1e-6);
  }
  
  // Restore the original data manager
  setContextFor(*dataManager);
}

/**
 * @brief Test handling of zero and negative values
 *
 * Verifies that the CorrectionManager handles zero and negative values
 * correctly, especially in edge cases.
 */
TEST_F(CorrectionManagerTest, ZeroAndNegativeValues) {
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(4);
  setContextFor(*testDataManager);
  
  testDataManager->Define("float_arg",
                [](ULong64_t i) -> double { return i == 0   ? 0.0 : (i == 1 ? -1.0 : (i == 2 ? -2.0 : 1.0)); },
                {"rdfentry_"}, *systematicManager);
  testDataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 4);
  EXPECT_NEAR(result->at(0), 0.1, 1e-6); // 0.0 in first bin
  EXPECT_NEAR(result->at(1), 0.0, 1e-6); // -1.0 is flow
  EXPECT_NEAR(result->at(2), 0.0, 1e-6); // -2.0 is flow
  EXPECT_NEAR(result->at(3), 0.2, 1e-6); // 1.0 in second bin
  
  // Restore the original data manager
  setContextFor(*dataManager);
}

// ============================================================================
// Input Validation Tests
// ============================================================================

/**
 * @brief Test handling of mismatched input feature names
 *
 * Verifies that the CorrectionManager properly handles cases where the
 * provided input features don't match the correction's expected features.
 * Note: The current implementation may not validate this strictly.
 */
TEST_F(CorrectionManagerTest, MismatchedInputFeatureNames) {
  auto dataManager = std::make_unique<DataManager>(2);
  dataManager->Define("wrong_feature_name", [](ULong64_t i) -> double { return 0.5; },
                {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"wrong_feature_name", "int_arg"};
  EXPECT_THROW({
    correctionManager->applyCorrection("test_correction", stringArguments);
  }, std::runtime_error);
}

/**
 * @brief Test handling of missing dataframe columns
 *
 * Verifies that the CorrectionManager properly handles cases where the
 * specified input features don't exist in the dataframe.
 * Note: The current implementation may not validate this strictly.
 * 
 */
TEST_F(CorrectionManagerTest, MissingDataframeColumns) {
  auto dataManager = std::make_unique<DataManager>(2);
  
  // Only define int_arg, not float_arg
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"}, *systematicManager);
  // Note: float_arg is not defined
  
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};

  // The current implementation should throw a runtime_error when columns are missing
  EXPECT_THROW({
    correctionManager->applyCorrection("test_correction", stringArguments);
  }, std::runtime_error);
}

// Test multiple corrections
TEST_F(CorrectionManagerTest, MultipleCorrections) {
  // Check both corrections are present
  EXPECT_NO_THROW({
    auto corr1 = correctionManager->getCorrection("test_correction");
    auto corr2 = correctionManager->getCorrection("test_correction2");
    const auto &features1 = correctionManager->getCorrectionFeatures("test_correction");
    const auto &features2 = correctionManager->getCorrectionFeatures("test_correction2");
    EXPECT_EQ(features1.size(), 2);
    EXPECT_EQ(features2.size(), 2);
  });
  // Apply both corrections to a DataManager
  auto dataManager = std::make_unique<DataManager>(2);
  setContextFor(*dataManager);
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("float_arg2", [](ULong64_t i) -> double { return i == 0 ? 1.0 : 3.0; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg2", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  correctionManager->applyCorrection("test_correction2", {});
  auto df = dataManager->getDataFrame();
  auto result1 = df.Take<float>("test_correction_A");
  auto result2 = df.Take<float>("test_correction2");
  ASSERT_EQ(result1->size(), 2);
  ASSERT_EQ(result2->size(), 2);
}

// Thread safety test using ROOT's implicit multithreading
TEST_F(CorrectionManagerTest, ThreadSafetyWithROOTImplicitMT) {
  ROOT::EnableImplicitMT();
  ASSERT_TRUE(ROOT::IsImplicitMTEnabled());
  int nThreads = ROOT::GetThreadPoolSize();
  EXPECT_GT(nThreads, 1) << "ROOT ImplicitMT is enabled but only one thread is available.";
  auto dataManager = std::make_unique<DataManager>(100);
  setContextFor(*dataManager);
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i % 2 == 0 ? 0.5 : 1.5; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i % 2 == 0 ? 1 : 2; }, {"rdfentry_"}, *systematicManager);
  std::vector<std::string> stringArguments = {"A"};
  correctionManager->applyCorrection("test_correction", stringArguments);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_A");
  ASSERT_EQ(result->size(), 100);
  for (const auto &val : *result) {
    EXPECT_GE(val, 0.0f);
    EXPECT_LE(val, 1.0f);
  }
}

// ============================================================================
// Vector Correction Tests
// ============================================================================

/**
 * @brief Test basic vector correction (applyCorrectionVec)
 *
 * Verifies that applyCorrectionVec correctly evaluates a correction for each
 * element of per-event RVec input columns and stores the results as an
 * RVec<Float_t> output column.
 *
 * Per-event inputs (2 objects):
 *   float_arg = {0.5, 1.5}, int_arg = {1, 2}, str_arg = "A"
 * Expected outputs from correction.json:
 *   object 0: float=0.5 → bin [0,1), int=1, str=A → 0.1
 *   object 1: float=1.5 → bin [1,2), int=2, str=A → 0.4
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrectionBasic) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      []() -> ROOT::VecOps::RVec<double> { return {0.5, 1.5}; }, {},
      *systematicManager);
  testDataManager->Define(
      "int_arg",
      []() -> ROOT::VecOps::RVec<double> { return {1.0, 2.0}; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction", {"A"});

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("test_correction_A");
  ASSERT_EQ(result->size(), 1u);          // one event
  ASSERT_EQ((*result)[0].size(), 2u);     // two objects per event
  EXPECT_NEAR((*result)[0][0], 0.1f, 1e-6f);
  EXPECT_NEAR((*result)[0][1], 0.4f, 1e-6f);

  setContextFor(*dataManager);
}

/**
 * @brief Test vector correction with a different string argument
 *
 * Same as ApplyVectorCorrectionBasic but with str_arg = "B".
 * Expected outputs:
 *   object 0: float=0.5, int=1, str=B → 0.5
 *   object 1: float=1.5, int=2, str=B → 0.8
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrectionStringArgB) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      []() -> ROOT::VecOps::RVec<double> { return {0.5, 1.5}; }, {},
      *systematicManager);
  testDataManager->Define(
      "int_arg",
      []() -> ROOT::VecOps::RVec<double> { return {1.0, 2.0}; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction", {"B"});

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("test_correction_B");
  ASSERT_EQ(result->size(), 1u);
  ASSERT_EQ((*result)[0].size(), 2u);
  EXPECT_NEAR((*result)[0][0], 0.5f, 1e-6f);
  EXPECT_NEAR((*result)[0][1], 0.8f, 1e-6f);

  setContextFor(*dataManager);
}

/**
 * @brief Test vector correction with an empty object collection
 *
 * Verifies that applyCorrectionVec handles a per-event RVec with zero elements
 * without crashing and returns an empty RVec<Float_t>.
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrectionEmptyVector) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      []() -> ROOT::VecOps::RVec<double> { return {}; }, {},
      *systematicManager);
  testDataManager->Define(
      "int_arg",
      []() -> ROOT::VecOps::RVec<double> { return {}; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction", {"A"});

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("test_correction_A");
  ASSERT_EQ(result->size(), 1u);
  EXPECT_EQ((*result)[0].size(), 0u);

  setContextFor(*dataManager);
}

/**
 * @brief Test that applyCorrectionVec throws for a non-existent correction
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrectionThrowsForMissing) {
  EXPECT_THROW(
      { correctionManager->applyCorrectionVec("nonexistent_correction", {}); },
      std::runtime_error);
}

/**
 * @brief Test that applyCorrectionVec throws when input columns are absent
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrectionThrowsForMissingColumns) {
  // Do not define float_arg or int_arg: the call should throw.
  EXPECT_THROW(
      { correctionManager->applyCorrectionVec("test_correction", {"A"}); },
      std::runtime_error);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// ---------------------------------------------------------------------------
// Lifecycle hook tests
// ---------------------------------------------------------------------------

TEST_F(CorrectionManagerTest, InitializeIsCallable) {
  EXPECT_NO_THROW(correctionManager->initialize());
}

TEST_F(CorrectionManagerTest, ReportMetadataIsCallable) {
  EXPECT_NO_THROW(correctionManager->reportMetadata());
}

TEST_F(CorrectionManagerTest, ExecuteAndFinalizeAreNoOps) {
  EXPECT_NO_THROW(correctionManager->execute());
  EXPECT_NO_THROW(correctionManager->finalize());
}

TEST_F(CorrectionManagerTest, GetDependenciesReturnsEmpty) {
  EXPECT_TRUE(correctionManager->getDependencies().empty());
}

// ============================================================================
// C++ API Tests: string argument branch naming, explicit input/output branches,
// and programmatic registration
// ============================================================================

/**
 * @brief Test that applying the same correction with different string arguments
 *        creates separate output columns.
 *
 * The main motivation from the issue: corrections like muon SFs can be applied
 * multiple times (nominal, syst_up, syst_down) and each variation lives in its
 * own dataframe column.
 */
TEST_F(CorrectionManagerTest, SameCorrection_DifferentStringArgs_CreatesDistinctColumns) {
  dataManager->Define("float_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 1 : 2; },
                      {"rdfentry_"}, *systematicManager);

  correctionManager->applyCorrection("test_correction", {"A"});
  correctionManager->applyCorrection("test_correction", {"B"});

  auto df = dataManager->getDataFrame();

  auto resultA = df.Take<float>("test_correction_A");
  auto resultB = df.Take<float>("test_correction_B");

  ASSERT_EQ(resultA->size(), 2u);
  ASSERT_EQ(resultB->size(), 2u);

  // "A" path: float=0.5→bin[0,1), int=1 → 0.1; float=1.5→bin[1,2), int=2 → 0.4
  EXPECT_NEAR(resultA->at(0), 0.1f, 1e-6f);
  EXPECT_NEAR(resultA->at(1), 0.4f, 1e-6f);

  // "B" path: same inputs → 0.5 and 0.8
  EXPECT_NEAR(resultB->at(0), 0.5f, 1e-6f);
  EXPECT_NEAR(resultB->at(1), 0.8f, 1e-6f);
}

/**
 * @brief Test explicit output branch name override in applyCorrection.
 *
 * When @p outputBranch is provided the output column gets that exact name
 * instead of the auto-derived "correctionName_stringArg" name.
 */
TEST_F(CorrectionManagerTest, ApplyCorrection_ExplicitOutputBranch) {
  dataManager->Define("float_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 1 : 2; },
                      {"rdfentry_"}, *systematicManager);

  correctionManager->applyCorrection("test_correction", {"A"}, {}, "my_custom_sf");

  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("my_custom_sf");
  ASSERT_EQ(result->size(), 2u);
  EXPECT_NEAR(result->at(0), 0.1f, 1e-6f);
  EXPECT_NEAR(result->at(1), 0.4f, 1e-6f);
}

/**
 * @brief Test explicit input column name override in applyCorrection.
 *
 * When @p inputColumns is provided, those columns are used instead of the
 * ones registered in the configuration.  This lets you reuse the same
 * correction with different branch names without modifying the config.
 */
TEST_F(CorrectionManagerTest, ApplyCorrection_ExplicitInputColumns) {
  // Use deliberately different column names from those in the config.
  dataManager->Define("my_pt",
                      [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("my_bin",
                      [](ULong64_t i) -> double { return i == 0 ? 1 : 2; },
                      {"rdfentry_"}, *systematicManager);

  correctionManager->applyCorrection("test_correction", {"A"},
                                     {"my_pt", "my_bin"}, "sf_from_custom_cols");

  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("sf_from_custom_cols");
  ASSERT_EQ(result->size(), 2u);
  // Same numeric inputs, same expected values as the standard test.
  EXPECT_NEAR(result->at(0), 0.1f, 1e-6f);
  EXPECT_NEAR(result->at(1), 0.4f, 1e-6f);
}

/**
 * @brief Test programmatic registration of a correction (registerCorrection).
 *
 * Registers a correction directly from C++ without a config file entry, then
 * applies it and verifies the output values.
 */
TEST_F(CorrectionManagerTest, RegisterCorrection_ProgrammaticCppRegistration) {
  // Register the same JSON correction under a new name via the C++ API.
  correctionManager->registerCorrection(
      "cpp_registered_sf",
      "aux/correction.json",
      "test_correction",
      {"float_arg", "int_arg"});

  dataManager->Define("float_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg",
                      [](ULong64_t i) -> double { return i == 0 ? 1 : 2; },
                      {"rdfentry_"}, *systematicManager);

  correctionManager->applyCorrection("cpp_registered_sf", {"A"});

  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("cpp_registered_sf_A");
  ASSERT_EQ(result->size(), 2u);
  EXPECT_NEAR(result->at(0), 0.1f, 1e-6f);
  EXPECT_NEAR(result->at(1), 0.4f, 1e-6f);
}

/**
 * @brief Test programmatic registration combined with explicit input/output branches.
 *
 * Demonstrates the full C++-only workflow: register + apply with custom branches.
 */
TEST_F(CorrectionManagerTest, RegisterCorrection_WithExplicitBranches) {
  correctionManager->registerCorrection(
      "my_sf",
      "aux/correction.json",
      "test_correction",
      {"float_arg", "int_arg"});

  dataManager->Define("jet_pt",
                      [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("jet_bin",
                      [](ULong64_t i) -> double { return i == 0 ? 1 : 2; },
                      {"rdfentry_"}, *systematicManager);

  // Apply with explicit input columns and explicit output branch name.
  correctionManager->applyCorrection("my_sf", {"B"},
                                     {"jet_pt", "jet_bin"}, "jet_sf_down");

  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("jet_sf_down");
  ASSERT_EQ(result->size(), 2u);
  EXPECT_NEAR(result->at(0), 0.5f, 1e-6f);
  EXPECT_NEAR(result->at(1), 0.8f, 1e-6f);
}

/**
 * @brief Test explicit input column override for applyCorrectionVec.
 */
TEST_F(CorrectionManagerTest, ApplyVectorCorrection_ExplicitInputColumns) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  // Use non-standard column names.
  testDataManager->Define(
      "jet_pt_vec",
      []() -> ROOT::VecOps::RVec<double> { return {0.5, 1.5}; }, {},
      *systematicManager);
  testDataManager->Define(
      "jet_bin_vec",
      []() -> ROOT::VecOps::RVec<double> { return {1.0, 2.0}; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction", {"A"},
                                        {"jet_pt_vec", "jet_bin_vec"},
                                        "jet_sf_vec_nominal");

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("jet_sf_vec_nominal");
  ASSERT_EQ(result->size(), 1u);
  ASSERT_EQ((*result)[0].size(), 2u);
  EXPECT_NEAR((*result)[0][0], 0.1f, 1e-6f);
  EXPECT_NEAR((*result)[0][1], 0.4f, 1e-6f);

  setContextFor(*dataManager);
}
/**
 * @brief Test that string arguments containing invalid characters are rejected.
 *
 * Branch names containing spaces, slashes, or other special characters would
 * create invalid ROOT column names, so makeBranchName should throw.
 */
TEST_F(CorrectionManagerTest, ApplyCorrection_InvalidStringArgRejected) {
  dataManager->Define("float_arg",
                      [](ULong64_t) -> double { return 0.5; },
                      {"rdfentry_"}, *systematicManager);
  dataManager->Define("int_arg",
                      [](ULong64_t) -> double { return 1; },
                      {"rdfentry_"}, *systematicManager);

  // Space is not allowed in a branch name component.
  EXPECT_THROW(
      correctionManager->applyCorrection("test_correction", {"syst up"}),
      std::invalid_argument);

  // Slash is not allowed.
  EXPECT_THROW(
      correctionManager->applyCorrection("test_correction", {"pt/eta"}),
      std::invalid_argument);

  // Empty string is not allowed.
  EXPECT_THROW(
      correctionManager->applyCorrection("test_correction", {""}),
      std::invalid_argument);
}

/**
 * @brief Test that registering the same correction name twice throws.
 */
TEST_F(CorrectionManagerTest, RegisterCorrection_DuplicateNameThrows) {
  correctionManager->registerCorrection(
      "once_only_sf", "aux/correction.json", "test_correction",
      {"float_arg", "int_arg"});
  EXPECT_THROW(
      correctionManager->registerCorrection(
          "once_only_sf", "aux/correction.json", "test_correction",
          {"float_arg", "int_arg"}),
      std::runtime_error);
}

/**
 * @brief Test that registerCorrection throws for a missing file.
 */
TEST_F(CorrectionManagerTest, RegisterCorrection_MissingFileThrows) {
  EXPECT_THROW(
      correctionManager->registerCorrection(
          "bad_sf", "nonexistent_file.json", "test_correction",
          {"float_arg", "int_arg"}),
      std::runtime_error);
}

/**
 * @brief Test that registerCorrection throws for an unknown correction name inside a valid file.
 */
TEST_F(CorrectionManagerTest, RegisterCorrection_UnknownCorrectionNameThrows) {
  EXPECT_THROW(
      correctionManager->registerCorrection(
          "bad_sf", "aux/correction.json", "no_such_correction",
          {"float_arg", "int_arg"}),
      std::runtime_error);
}
