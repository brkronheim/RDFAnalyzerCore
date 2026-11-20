#include <limits>
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
#include <RDFColumnPacker.h>
#include <DataManager.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <fstream>
#include <gtest/gtest.h>
#include <stdexcept>
#include <string>
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

TEST_F(CorrectionManagerTest, ScalarDoublePrecisionIsOptInAndPreservesVariations) {
  ROOT::DisableImplicitMT();
  dataManager->DefineNominal("float_arg", [] { return .5; });
  dataManager->DefineNominal("shifted", [] { return 1.5; });
  dataManager->DefineNominal("int_arg", [] { return 1.; });
  dataManager->DefineNominal("active", [](ULong64_t i) { return i == 0; }, {"rdfentry_"});
  systematicManager->registerVariationColumn("float_arg", "shape", "shifted");
  correctionManager->applyCorrection("test_correction", {"A"}, {}, "default_precision");
  configManager->set("correctionScalarOutputType", "double");
  correctionManager->applyCorrection("test_correction", {"A"}, {}, "double_precision");
  correctionManager->applyConditionalCorrections("test_correction", {{"A"}}, {},
      "active", {"conditional_double"});
  auto df = dataManager->getDataFrame();
  EXPECT_EQ(df.GetColumnType("default_precision"), "float");
  EXPECT_EQ(df.GetColumnType("double_precision"), "double");
  auto result = df.Take<double>("double_precision");
  auto shifted = df.Take<double>("double_precision_shape");
  auto gated = df.Take<double>("conditional_double");
  EXPECT_DOUBLE_EQ((*result)[0], .1);
  EXPECT_NE((*result)[0], double(float(.1)));
  EXPECT_DOUBLE_EQ((*shifted)[0], .2);
  EXPECT_DOUBLE_EQ((*gated)[0], .1);
  EXPECT_DOUBLE_EQ((*gated)[1], 1.);
}

TEST_F(CorrectionManagerTest, ScalarOutputPrecisionRejectsInvalidConfiguration) {
  configManager->set("correctionScalarOutputType", "invalid");
  EXPECT_THROW(correctionManager->applyCorrection("test_correction", {"A"}, {}, "bad_precision"), std::invalid_argument);
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

TEST_F(CorrectionManagerTest, ApplyCorrectionRangeErrorsUseNeutralFallback) {
  auto testDataManager = std::make_unique<DataManager>(3);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      [](ULong64_t i) -> double {
        return i == 0 ? -0.5 : (i == 1 ? 0.5 : 2.0);
      },
      {"rdfentry_"}, *systematicManager);
  testDataManager->Define("int_arg", [](ULong64_t) -> double { return 1.0; },
                          {"rdfentry_"}, *systematicManager);

  correctionManager->applyCorrection("test_correction_range_error", {"A"});
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_correction_range_error_A");
  testing::internal::CaptureStderr();
  const auto values = result.GetValue();
  const auto warnings = testing::internal::GetCapturedStderr();
  EXPECT_NE(warnings.find("Index below bounds"), std::string::npos);
  EXPECT_NE(warnings.find("Index above bounds"), std::string::npos);
  EXPECT_NE(warnings.find("using fallback 1"), std::string::npos);
  ASSERT_EQ(values.size(), 3u);
  EXPECT_FLOAT_EQ(result->at(0), 1.0f);
  EXPECT_NEAR(result->at(1), 0.1f, 1e-6f);
  EXPECT_FLOAT_EQ(result->at(2), 1.0f);

  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, MissingCategoryWarnsForEveryScalarFallback) {
  dataManager->Define("float_arg", []() { return 0.5; }, {}, *systematicManager);
  dataManager->Define("int_arg", []() { return 1; }, {}, *systematicManager);
  correctionManager->applyCorrection("test_correction", {"missing_year"});
  auto result = dataManager->getDataFrame().Take<float>(
      "test_correction_missing_year");

  testing::internal::CaptureStderr();
  const auto values = result.GetValue();
  const std::string warnings = testing::internal::GetCapturedStderr();

  ASSERT_EQ(values.size(), 2u);
  EXPECT_FLOAT_EQ(values[0], 1.0f);
  EXPECT_FLOAT_EQ(values[1], 1.0f);
  EXPECT_NE(warnings.find("correction 'test_correction'"), std::string::npos);
  EXPECT_NE(warnings.find("Index not available in Category"), std::string::npos);
  EXPECT_NE(warnings.find("inputs [0.5, 1, missing_year]"), std::string::npos);
  const auto first = warnings.find("std::out_of_range");
  ASSERT_NE(first, std::string::npos);
  EXPECT_NE(warnings.find("std::out_of_range", first + 1), std::string::npos);
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
TEST_F(CorrectionManagerTest, NaNAndInfinityFailWithDiagnostic) {
  for (const auto bad : {std::numeric_limits<double>::quiet_NaN(),
                         std::numeric_limits<double>::infinity(),
                         -std::numeric_limits<double>::infinity()}) {
    DataManager dm(1);
    setContextFor(dm);
    dm.DefineNominal("float_arg", [bad] { return bad; });
    dm.DefineNominal("int_arg", [] { return 1.; });
    correctionManager->applyCorrection("test_correction", {"A"});
    try {
      dm.getDataFrame().Take<float>("test_correction_A").GetValue();
      FAIL() << "non-finite input accepted";
    } catch (const std::runtime_error &error) {
      EXPECT_NE(std::string(error.what()).find("non-finite numeric correction input"), std::string::npos);
    }
  }
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
  struct RestoreImplicitMT {
    bool wasEnabled = ROOT::IsImplicitMTEnabled();
    ~RestoreImplicitMT() { if (!wasEnabled) ROOT::DisableImplicitMT(); }
  } restoreImplicitMT;
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

TEST_F(CorrectionManagerTest, ApplyVectorCorrectionRangeErrorsUseNeutralFallback) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      []() -> ROOT::VecOps::RVec<double> { return {-0.5, 0.5, 2.0}; }, {},
      *systematicManager);
  testDataManager->Define(
      "int_arg",
      []() -> ROOT::VecOps::RVec<double> { return {1.0, 1.0, 1.0}; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction_range_error", {"A"});
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>(
      "test_correction_range_error_A");
  testing::internal::CaptureStderr();
  const auto values = result.GetValue();
  const auto warnings = testing::internal::GetCapturedStderr();
  EXPECT_NE(warnings.find("Index below bounds"), std::string::npos);
  EXPECT_NE(warnings.find("Index above bounds"), std::string::npos);
  EXPECT_NE(warnings.find("inputs [-0.5, 1, A]"), std::string::npos);
  EXPECT_NE(warnings.find("inputs [2, 1, A]"), std::string::npos);
  ASSERT_EQ(values.size(), 1u);
  ASSERT_EQ(result->at(0).size(), 3u);
  EXPECT_FLOAT_EQ(result->at(0)[0], 1.0f);
  EXPECT_NEAR(result->at(0)[1], 0.1f, 1e-6f);
  EXPECT_FLOAT_EQ(result->at(0)[2], 1.0f);

  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, MissingCategoryWarnsForEveryVectorFallback) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);
  testDataManager->Define(
      "float_arg", []() -> ROOT::VecOps::RVec<double> { return {0.5, 1.5}; },
      {}, *systematicManager);
  testDataManager->Define(
      "int_arg", []() -> ROOT::VecOps::RVec<int> { return {1, 1}; },
      {}, *systematicManager);
  correctionManager->applyCorrectionVec("test_correction", {"missing_year"});
  auto result = testDataManager->getDataFrame().Take<ROOT::VecOps::RVec<float>>(
      "test_correction_missing_year");

  testing::internal::CaptureStderr();
  const auto values = result.GetValue();
  const std::string warnings = testing::internal::GetCapturedStderr();

  ASSERT_EQ(values.size(), 1u);
  ASSERT_EQ(values[0].size(), 2u);
  EXPECT_FLOAT_EQ(values[0][0], 1.0f);
  EXPECT_FLOAT_EQ(values[0][1], 1.0f);
  EXPECT_NE(warnings.find("using fallback 1"), std::string::npos);
  EXPECT_NE(warnings.find("inputs [0.5, 1, missing_year]"), std::string::npos);
  EXPECT_NE(warnings.find("inputs [1.5, 1, missing_year]"), std::string::npos);
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

TEST_F(CorrectionManagerTest, ApplyVectorCorrectionSupportsMixedScalarAndRVecInputs) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  testDataManager->Define(
      "float_arg",
      []() -> ROOT::VecOps::RVec<Float_t> { return {0.5f, 1.5f}; }, {},
      *systematicManager);
  testDataManager->Define(
      "int_arg",
      []() -> double { return 1.0; }, {},
      *systematicManager);

  correctionManager->applyCorrectionVec("test_correction", {"A"}, {},
                                        "test_correction_mixed");

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("test_correction_mixed");
  ASSERT_EQ(result->size(), 1u);
  ASSERT_EQ((*result)[0].size(), 2u);
  EXPECT_NEAR((*result)[0][0], 0.1f, 1e-6f);
  EXPECT_NEAR((*result)[0][1], 0.2f, 1e-6f);

  setContextFor(*dataManager);
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

TEST_F(CorrectionManagerTest, RegisterCompoundCorrectionAndApplyScalar) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  const std::string file = "aux/mock_electron_ss.json";

  testDataManager->Define("run", []() -> double { return 355862.0; }, {}, *systematicManager);
  testDataManager->Define("Electron_scEta", []() -> double { return 0.15; }, {}, *systematicManager);
  testDataManager->Define("Electron_r9", []() -> double { return 0.95; }, {}, *systematicManager);
  testDataManager->Define("Electron_pt", []() -> double { return 45.0; }, {}, *systematicManager);
  testDataManager->Define("Electron_seedGain", []() -> double { return 12.0; }, {}, *systematicManager);

  correctionManager->registerCorrection(
    "electron_scale_compound",
    file,
    "Scale",
    {"run", "Electron_scEta", "Electron_r9", "Electron_pt", "Electron_seedGain"});
  correctionManager->applyCorrection(
    "electron_scale_compound", {"scale"}, {}, "electron_scale_sf");

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("electron_scale_sf");
  ASSERT_EQ(result->size(), 1u);

  auto cset = correction::CorrectionSet::from_file(file);
  const auto expected = static_cast<float>(
    cset->compound().at("Scale")->evaluate(
      {std::string("scale"), 355862.0, 0.15, 0.95, 45.0, 12.0}));
  EXPECT_NEAR(result->at(0), expected, 1e-6f);

  EXPECT_NO_THROW(correctionManager->getCompoundCorrection("electron_scale_compound"));

  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, ApplyCompoundCorrectionVecSupportsMixedScalarAndRVecInputs) {
  auto testDataManager = std::make_unique<DataManager>(1);
  setContextFor(*testDataManager);

  const std::string file = "aux/mock_jet_jerc.json";
  const std::string compoundName =
    "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi";

  testDataManager->Define(
    "Jet_area",
    []() -> ROOT::VecOps::RVec<Float_t> { return {0.52f, 0.61f}; }, {}, *systematicManager);
  testDataManager->Define(
    "Jet_eta",
    []() -> ROOT::VecOps::RVec<Float_t> { return {0.3f, -1.1f}; }, {}, *systematicManager);
  testDataManager->Define(
    "Jet_pt_raw",
    []() -> ROOT::VecOps::RVec<Float_t> { return {55.0f, 110.0f}; }, {}, *systematicManager);
  testDataManager->Define(
    "Rho_fixedGridRhoFastjetAll",
    []() -> double { return 21.0; }, {}, *systematicManager);

  correctionManager->registerCorrection(
    "jme_compound",
    file,
    compoundName,
    {"Jet_area", "Jet_eta", "Jet_pt_raw", "Rho_fixedGridRhoFastjetAll"});
  correctionManager->applyCorrectionVec(
    "jme_compound", {}, {}, "jme_sf");

  auto df = testDataManager->getDataFrame();
  auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("jme_sf");
  ASSERT_EQ(result->size(), 1u);
  ASSERT_EQ((*result)[0].size(), 2u);

  auto cset = correction::CorrectionSet::from_file(file);
  const auto compound = cset->compound().at(compoundName);
  const auto expected0 = static_cast<float>(compound->evaluate(
    {0.52, 0.3, 55.0, 21.0}));
  const auto expected1 = static_cast<float>(compound->evaluate(
    {0.61, -1.1, 110.0, 21.0}));
  EXPECT_NEAR((*result)[0][0], expected0, 1e-6f);
  EXPECT_NEAR((*result)[0][1], expected1, 1e-6f);

  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, RejectsWrongInputCountsBeforeDefiningGraph) {
  // No columns are needed: schema errors must be reported before packing or
  // evaluating inputs, rather than dereferencing past a numeric/string vector.
  for (const auto &columns : std::vector<std::vector<std::string>>{
           {"float_arg"}, {"float_arg", "int_arg", "extra"}}) {
    EXPECT_THROW(correctionManager->applyCorrection(
                     "test_correction", {"A"}, columns), std::runtime_error);
    EXPECT_THROW(correctionManager->applyCorrectionVec(
                     "test_correction", {"A"}, columns), std::runtime_error);
  }
  for (const auto &strings : std::vector<std::vector<std::string>>{
           {}, {"A", "B"}}) {
    EXPECT_THROW(correctionManager->applyCorrection(
                     "test_correction", strings), std::runtime_error);
    EXPECT_THROW(correctionManager->applyCorrectionVec(
                     "test_correction", strings), std::runtime_error);
  }
  EXPECT_FALSE(dataManager->getDataFrame().HasColumn("input_test_correction_A"));
}

TEST_F(CorrectionManagerTest, RejectsIncompleteCompoundInputs) {
  correctionManager->registerCorrection(
      "incomplete_jec", "aux/mock_jet_jerc.json",
      "Summer22_22Sep2023_V3_MC_L1L2L3Res_AK4PFPuppi",
      {"Jet_area", "Jet_eta", "Jet_pt_raw"});
  EXPECT_THROW(correctionManager->applyCorrection("incomplete_jec", {}),
               std::runtime_error);
  EXPECT_THROW(correctionManager->applyCorrectionVec("incomplete_jec", {}),
               std::runtime_error);
}

TEST_F(CorrectionManagerTest, CompoundPreservesIntegerInputTypes) {
  dataManager->Define("run_scalar", []() { return 1; }, {}, *systematicManager);
  dataManager->Define("run_vector", []() {
    return ROOT::VecOps::RVec<int>{1, 2};
  }, {}, *systematicManager);
  correctionManager->registerCorrection(
      "integer_compound", "aux/mock_compound_integer.json", "integer_compound",
      {"run_scalar"});
  correctionManager->applyCorrection("integer_compound", {}, {}, "scalar_sf");
  correctionManager->applyCorrectionVec(
      "integer_compound", {}, {"run_vector"}, "vector_sf");
  auto df = dataManager->getDataFrame();
  const auto scalars = df.Take<float>("scalar_sf").GetValue();
  const auto vectors = df.Take<ROOT::VecOps::RVec<float>>("vector_sf").GetValue();
  ASSERT_EQ(scalars.size(), 2u);
  ASSERT_EQ(vectors.size(), 2u);
  for (size_t i = 0; i < 2; ++i) {
    EXPECT_EQ(scalars[i], 1.5f);
    ASSERT_EQ(vectors[i].size(), 2u);
    EXPECT_EQ(vectors[i][0], 1.5f);
    EXPECT_EQ(vectors[i][1], 2.5f);
  }
}

TEST_F(CorrectionManagerTest, BatchSharesInputsAndPreservesMappedShapeVariations) {
  dataManager->DefineNominal("float_arg", []() { return .5; });
  dataManager->DefineNominal("float_high", []() { return 1.5; });
  dataManager->DefineNominal("float_low", []() { return .5; });
  dataManager->DefineNominal("int_arg", []() { return 1.; });
  systematicManager->registerVariationColumns("float_arg", "shape", "float_high", "float_low");
  correctionManager->applyCorrections("test_correction", {{"A"}, {"B"}}, {}, {"batch_a", "batch_b"});
  auto df = dataManager->getDataFrame();
  EXPECT_FALSE(df.HasColumn("input_batch_b"));
  EXPECT_NEAR(df.Take<float>("batch_a")->at(0), .1f, 1.e-6);
  EXPECT_NEAR(df.Take<float>("batch_b")->at(0), .5f, 1.e-6);
  EXPECT_NEAR(df.Take<float>("batch_a_shapeUp")->at(0), .2f, 1.e-6);
  EXPECT_NEAR(df.Take<float>("batch_b_shapeUp")->at(0), .6f, 1.e-6);
  // A later batch uses new readers after Redefine, rather than a global input cache.
  dataManager->Redefine("float_arg", []() { return 1.5; });
  correctionManager->applyCorrections("test_correction", {{"A"}}, {}, {"batch_new"});
  EXPECT_NEAR(dataManager->getDataFrame().Take<float>("batch_new")->at(0), .2f, 1.e-6);
  EXPECT_THROW(correctionManager->applyCorrections("test_correction", {{"A"}}, {}, {"x", "y"}), std::invalid_argument);
}

TEST_F(CorrectionManagerTest, BatchVectorSharesMixedInputs) {
  dataManager->DefineNominal("float_arg", []() { return ROOT::VecOps::RVec<float>{.5f, 1.5f}; });
  dataManager->DefineNominal("int_arg", []() { return 1.; });
  correctionManager->applyCorrectionsVec("test_correction", {{"A"}, {"B"}}, {}, {"batch_a", "batch_b"});
  auto df = dataManager->getDataFrame();
  EXPECT_FALSE(df.HasColumn("input_vec_batch_b"));
  const auto a = df.Take<ROOT::VecOps::RVec<float>>("batch_a")->at(0);
  const auto b = df.Take<ROOT::VecOps::RVec<float>>("batch_b")->at(0);
  ASSERT_EQ(a.size(), 2u);
  ASSERT_EQ(b.size(), 2u);
  EXPECT_NEAR(a[1], .2f, 1.e-6);
  EXPECT_NEAR(b[1], .6f, 1.e-6);
}

TEST(ColumnPackerTest, CorrectionPackingRejectsUnequalArraysIncludingEmptyAndSingleton) {
  for (const auto size : {0u, 1u, 2u, 4u}) {
    ROOT::RDataFrame input(1);
    auto df = ROOT::RDF::RNode(input)
        .Define("a", [] { return ROOT::VecOps::RVec<float>{1, 2, 3}; })
        .Define("b", [size] { return ROOT::VecOps::RVec<int>(size, 4); });
    df = rdfcolumnpacker::defineFlattenedNumericInputs(df, "packed", {"a", "b"}, "test");
    try {
      df.Take<ROOT::VecOps::RVec<double>>("packed").GetValue();
      FAIL() << "misaligned input accepted";
    } catch (const std::runtime_error &error) {
      EXPECT_NE(std::string(error.what()).find("packed"), std::string::npos);
      EXPECT_NE(std::string(error.what()).find("array 'b'"), std::string::npos);
    }
  }
}

TEST(ColumnPackerTest, CorrectionPackingBroadcastsScalarsAndPreservesEmptyArrays) {
  ROOT::RDataFrame input(2);
  auto df = ROOT::RDF::RNode(input)
      .Define("a", [](ULong64_t i) { return ROOT::VecOps::RVec<float>(i * 3, 2); }, {"rdfentry_"})
      .Define("b", [](ULong64_t i) { return ROOT::VecOps::RVec<int>(i * 3, 4); }, {"rdfentry_"})
      .Define("c", [] { return 7.; });
  df = rdfcolumnpacker::defineFlattenedNumericInputs(df, "packed", {"a", "b", "c", "a"}, "test");
  const auto packed = df.Take<ROOT::VecOps::RVec<double>>("packed").GetValue();
  EXPECT_TRUE(packed[0].empty());
  const std::vector<double> expected{2,4,7,2,2,4,7,2,2,4,7,2};
  EXPECT_EQ(std::vector<double>(packed[1].begin(), packed[1].end()), expected);
}

TEST(ColumnPackerTest, SizedPackingPreservesRowsAndConcatenationAcrossStorageSizes) {
  ROOT::RDataFrame input(4);
  auto df = ROOT::RDF::RNode(input)
      .Define("a", [](ULong64_t i) {
        ROOT::VecOps::RVec<float> values(i == 0 ? 0 : i == 1 ? 1 : i == 2 ? 16 : 65);
        for (std::size_t j = 0; j < values.size(); ++j) values[j] = j + .5f;
        return values;
      }, {"rdfentry_"})
      .Define("b", []() { return ROOT::VecOps::RVec<int>{-2, 3}; })
      .Define("scalar", []() { return 7.; });
  df = rdfcolumnpacker::definePackedVector(df, "rows", {"a", "scalar", "b", "a"},
      "double", rdfcolumnpacker::PackingLayout::Rows);
  df = rdfcolumnpacker::definePackedVector(df, "concat", {"a", "b", "a"},
      "float", rdfcolumnpacker::PackingLayout::Concatenate);
  auto rows = df.Take<ROOT::VecOps::RVec<double>>("rows");
  auto concat = df.Take<ROOT::VecOps::RVec<float>>("concat");
  const auto &rowValues = rows.GetValue();
  const auto &concatValues = concat.GetValue();
  for (std::size_t entry = 0; entry < 4; ++entry) {
    const std::size_t n = entry == 0 ? 0 : entry == 1 ? 1 : entry == 2 ? 16 : 65;
    ASSERT_EQ(rowValues[entry].size(), n == 0 ? 0 : 4 * std::max(n, std::size_t(2)));
    for (std::size_t row = 0; row < rowValues[entry].size() / 4; ++row) {
      EXPECT_EQ(rowValues[entry][row * 4], std::min(row, n - 1) + .5);
      EXPECT_EQ(rowValues[entry][row * 4 + 1], 7.);
      EXPECT_EQ(rowValues[entry][row * 4 + 2], row == 0 ? -2 : 3);
      EXPECT_EQ(rowValues[entry][row * 4 + 3], std::min(row, n - 1) + .5);
    }
    ASSERT_EQ(concatValues[entry].size(), n * 2 + 2);
    EXPECT_EQ(concatValues[entry][n], -2.f);
    EXPECT_EQ(concatValues[entry][n + 1], 3.f);
    for (std::size_t j = 0; j < n; ++j) {
      EXPECT_EQ(concatValues[entry][j], j + .5f);
      EXPECT_EQ(concatValues[entry][n + 2 + j], j + .5f);
    }
  }
}

TEST_F(CorrectionManagerTest, BatchCanExpandOnlyNominalOutputWithLaterPropagationRequest) {
  dataManager->DefineNominal("float_arg", []() { return .5; });
  dataManager->DefineNominal("float_high", []() { return 1.5; });
  dataManager->DefineNominal("float_low", []() { return .5; });
  dataManager->DefineNominal("int_arg", []() { return 1.; });
  systematicManager->registerVariationColumns("float_arg", "shape", "float_high", "float_low");
  correctionManager->applyCorrections("test_correction", {{"B"}, {"A"}}, {},
                                    {"terminal_b", "nominal_a"}, {false, true});
  auto df = dataManager->getDataFrame();
  EXPECT_FALSE(df.HasColumn("terminal_b_shapeUp"));
  EXPECT_TRUE(df.HasColumn("nominal_a_shapeUp"));
  EXPECT_NEAR(df.Take<float>("terminal_b")->at(0), .5f, 1.e-6);
  EXPECT_NEAR(df.Take<float>("nominal_a_shapeUp")->at(0), .2f, 1.e-6);
}

TEST(ColumnPackerTest, ScalarConjunctionHandlesMixedTypesRepeatedInputsAndEmptyChain) {
  ROOT::RDataFrame input(2);
  auto df = ROOT::RDF::RNode(input)
      .Define("a", []() { return -1; })
      .Define("b", [](ULong64_t i) { return i == 0 ? 2. : 0.; }, {"rdfentry_"});
  df = rdfcolumnpacker::defineScalarAll(df, "pass", {"a", "b", "a"});
  df = rdfcolumnpacker::defineScalarAll(df, "empty", {});
  const auto pass = df.Take<bool>("pass").GetValue();
  EXPECT_TRUE(pass[0]);
  EXPECT_FALSE(pass[1]);
  EXPECT_TRUE(df.Take<bool>("empty")->at(1));
}

TEST_F(CorrectionManagerTest, InvalidNumericInputsFailBeforePayloadEvaluation) {
  for (const auto bad : {std::numeric_limits<double>::quiet_NaN(),
                         std::numeric_limits<double>::infinity(), 1.e20, 1.5}) {
    DataManager dm(1);
    setContextFor(dm);
    dm.DefineNominal("float_arg", [] { return 0.5; });
    dm.DefineNominal("int_arg", [bad] { return bad; });
    correctionManager->applyCorrection("test_correction", {"A"});
    EXPECT_THROW(dm.getDataFrame().Take<float>("test_correction_A").GetValue(), std::runtime_error);
  }
  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, SelectedCorrectionsEvaluateOnlyTheChosenCaseAndPropagateSelectors) {
  dataManager->DefineNominal("float_arg", [] { return ROOT::VecOps::RVec<double>{.5, 1.5}; });
  dataManager->DefineNominal("floatUp", [] { return ROOT::VecOps::RVec<double>{1.5, .5}; });
  dataManager->DefineNominal("floatDown", [] { return ROOT::VecOps::RVec<double>{.5, .5}; });
  systematicManager->registerVariationColumns("float_arg", "shape", "floatUp", "floatDown");
  dataManager->DefineNominal("int_arg", [] { return 1.; });
  dataManager->DefineNominal("case", [] { return ROOT::VecOps::RVec<int>{0, 1}; });
  dataManager->DefineNominal("caseUp", [] { return ROOT::VecOps::RVec<int>{1, 0}; });
  dataManager->DefineNominal("caseDown", [] { return ROOT::VecOps::RVec<int>{0, 0}; });
  systematicManager->registerVariationColumns("case", "shape", "caseUp", "caseDown");
  correctionManager->applySelectedCorrectionsVec(
      {{"test_correction", {"A"}}, {"test_correction", {"B"}},
       {"test_correction", {"missing_category_must_not_be_evaluated"}}},
      "case", {"float_arg", "int_arg"}, "selected_sf");
  auto df = dataManager->getDataFrame();
  testing::internal::CaptureStderr();
  const auto nominal = df.Take<ROOT::VecOps::RVec<float>>("selected_sf").GetValue();
  const auto up = df.Take<ROOT::VecOps::RVec<float>>("selected_sf_shapeUp").GetValue();
  const auto warnings = testing::internal::GetCapturedStderr();
  EXPECT_TRUE(warnings.empty()) << warnings;
  EXPECT_FLOAT_EQ(nominal[0][0], .1f);
  EXPECT_FLOAT_EQ(nominal[0][1], .6f);
  EXPECT_FLOAT_EQ(up[0][0], .6f);
  EXPECT_FLOAT_EQ(up[0][1], .1f);
}

TEST_F(CorrectionManagerTest, SelectedCorrectionRejectsInvalidOrMisalignedSelectors) {
  for (const auto selectors : {ROOT::VecOps::RVec<int>{-1, 0}, {0, 1}, {0}}) {
    DataManager dm(1);
    setContextFor(dm);
    dm.DefineNominal("float_arg", [] { return ROOT::VecOps::RVec<double>{.5, 1.5}; });
    dm.DefineNominal("int_arg", [] { return 1.; });
    dm.DefineNominal("case", [selectors] { return selectors; });
    correctionManager->applySelectedCorrectionsVec({{"test_correction", {"A"}}},
        "case", {"float_arg", "int_arg"}, "selected_sf");
    EXPECT_THROW(dm.getDataFrame().Take<ROOT::VecOps::RVec<float>>("selected_sf").GetValue(),
                 std::runtime_error);
  }
  setContextFor(*dataManager);
}

TEST_F(CorrectionManagerTest, SelectedCompoundCorrectionBroadcastsScalarsAndHandlesEmptySelections) {
  dataManager->DefineNominal("run", [] { return 1; });
  dataManager->DefineNominal("case", [](ULong64_t i) {
    return i == 0 ? ROOT::VecOps::RVec<int>{0, 0} : ROOT::VecOps::RVec<int>{};
  }, {"rdfentry_"});
  correctionManager->registerCorrection("selected_compound", "aux/mock_compound_integer.json",
                                        "integer_compound", {"run"});
  correctionManager->applySelectedCorrectionsVec({{"selected_compound", {}}}, "case", {"run"}, "sf");
  const auto result = dataManager->getDataFrame().Take<ROOT::VecOps::RVec<float>>("sf").GetValue();
  ASSERT_EQ(result.size(), 2u);
  ASSERT_EQ(result[0].size(), 2u);
  EXPECT_FLOAT_EQ(result[0][0], 1.5f);
  EXPECT_FLOAT_EQ(result[0][1], 1.5f);
  EXPECT_TRUE(result[1].empty());
}

TEST_F(CorrectionManagerTest, ConditionalCorrectionSkipsInactiveInvalidInputsAndKeepsWeightVariationsNominal) {
  dataManager->DefineNominal("float_arg", [] { return .5; });
  dataManager->DefineNominal("int_arg", [] { return 1; });
  dataManager->DefineNominal("active", [] { return false; });
  dataManager->DefineNominal("activeUp", [] { return true; });
  dataManager->DefineNominal("activeDown", [] { return false; });
  systematicManager->registerVariationColumns("active", "shape", "activeUp", "activeDown");
  correctionManager->applyConditionalCorrection("test_correction", {"A"}, {}, "active", "conditional");
  correctionManager->applyConditionalCorrection("test_correction", {"missing_category"}, {},
                                                 "active", "weight_only", false);
  auto df = dataManager->getDataFrame();
  EXPECT_FALSE(df.HasColumn("weight_only_shapeUp"));
  testing::internal::CaptureStderr();
  const auto nominal = df.Take<float>("conditional").GetValue();
  const auto up = df.Take<float>("conditional_shapeUp").GetValue();
  const auto weightOnly = df.Take<float>("weight_only").GetValue();
  const auto warnings = testing::internal::GetCapturedStderr();
  EXPECT_TRUE(warnings.empty()) << warnings;
  EXPECT_FLOAT_EQ(nominal[0], 1.f);
  EXPECT_FLOAT_EQ(up[0], .1f);
  EXPECT_FLOAT_EQ(weightOnly[0], 1.f);
}

TEST_F(CorrectionManagerTest, SelectedBatchSharesPacksAndRefreshesReadersAfterRedefine) {
  using Vec = ROOT::VecOps::RVec<float>;
  dataManager->DefineNominal("float_arg", [] { return Vec{.5f, 1.5f}; });
  dataManager->DefineNominal("numericUp", [] { return Vec{1.5f, .5f}; });
  dataManager->DefineNominal("numericDown", [] { return Vec{.5f, .5f}; });
  dataManager->DefineNominal("int_arg", [] { return 1; });
  dataManager->DefineNominal("case", [] { return ROOT::VecOps::RVec<int>{0, 1}; });
  systematicManager->registerVariationColumns("float_arg", "shape", "numericUp", "numericDown");
  const std::vector<CorrectionManager::VectorCorrectionCase> first{
      {"test_correction", {"A"}}, {"test_correction", {"B"}}};
  const std::vector<CorrectionManager::VectorCorrectionCase> second{
      {"test_correction", {"B"}}, {"test_correction", {"A"}}};
  correctionManager->applySelectedCorrectionsVecBatch({first, second}, "case",
      {"float_arg", "int_arg"}, {"one", "two"}, {false, true});
  auto df = dataManager->getDataFrame();
  const auto columns = df.GetColumnNames();
  EXPECT_EQ(std::count_if(columns.begin(), columns.end(), [](const auto &name) {
    return name.find("__rdf_selected_correction_input_") == 0;
  }), 3); // one nominal pack plus the two shape packs, shared by both outputs
  EXPECT_FALSE(df.HasColumn("one_shapeUp"));
  EXPECT_FLOAT_EQ(df.Take<Vec>("one").GetValue()[0][0], .1f);
  EXPECT_FLOAT_EQ(df.Take<Vec>("two").GetValue()[0][0], .5f);
  EXPECT_FLOAT_EQ(df.Take<Vec>("two_shapeUp").GetValue()[0][0], .6f);
  dataManager->Redefine("float_arg", [] { return Vec{1.5f, 1.5f}; });
  correctionManager->applySelectedCorrectionsVecBatch({first, first}, "case",
      {"float_arg", "int_arg"}, {"one", "fresh"});
  EXPECT_FLOAT_EQ(dataManager->getDataFrame().Take<Vec>("fresh").GetValue()[0][0], .2f);
  EXPECT_THROW(correctionManager->applySelectedCorrectionsVecBatch({first}, "case",
      {"float_arg", "int_arg"}, {"a", "b"}), std::invalid_argument);
}

TEST_F(CorrectionManagerTest, ConditionalBatchSharesPacksWithoutBoxingScalarOutputs) {
  dataManager->DefineNominal("float_arg", [] { return .5; });
  dataManager->DefineNominal("numericUp", [] { return 1.5; });
  dataManager->DefineNominal("numericDown", [] { return .5; });
  dataManager->DefineNominal("int_arg", [] { return 1; });
  dataManager->DefineNominal("active", [](ULong64_t i) { return i == 0; }, {"rdfentry_"});
  systematicManager->registerVariationColumns("float_arg", "shape", "numericUp", "numericDown");
  correctionManager->applyConditionalCorrections("test_correction", {{"A"}, {"B"}}, {},
      "active", {"one", "two"}, {false, true});
  auto df = dataManager->getDataFrame();
  const auto columns = df.GetColumnNames();
  EXPECT_EQ(std::count_if(columns.begin(), columns.end(), [](const auto &name) {
    return name.find("__rdf_conditional_correction_input_") == 0;
  }), 3);
  EXPECT_FALSE(df.HasColumn("one__active_case"));
  EXPECT_FALSE(df.HasColumn("one_shapeUp"));
  const auto one = df.Take<float>("one").GetValue();
  EXPECT_FLOAT_EQ(one[0], .1f);
  EXPECT_FLOAT_EQ(one[1], 1.f);
  EXPECT_FLOAT_EQ(df.Take<float>("two_shapeUp").GetValue()[0], .6f);
  dataManager->Redefine("float_arg", [] { return 1.5; });
  correctionManager->applyConditionalCorrections("test_correction", {{"A"}}, {},
      "active", {"fresh"});
  EXPECT_FLOAT_EQ(dataManager->getDataFrame().Take<float>("fresh").GetValue()[0], .2f);
  EXPECT_THROW(correctionManager->applyConditionalCorrections("test_correction", {{"A"}}, {},
      "active", {"a", "b"}), std::invalid_argument);
}

TEST_F(CorrectionManagerTest, ConditionalPreparationIsGatedAndDoesNotMutateSharedInputs) {
  dataManager->DefineNominal("float_arg", [] { return -1.; });
  dataManager->DefineNominal("numericUp", [] { return 1.5; });
  dataManager->DefineNominal("numericDown", [] { return -.5; });
  dataManager->DefineNominal("int_arg", [] { return 1; });
  dataManager->DefineNominal("active", [](ULong64_t i) { return i == 0; }, {"rdfentry_"});
  dataManager->DefineNominal("inactive", [] { return false; });
  systematicManager->registerVariationColumns("float_arg", "shape", "numericUp", "numericDown");
  correctionManager->applyConditionalCorrections("test_correction", {{"A"}, {"B"}}, {},
      "active", {"prepared_one", "prepared_two"}, {true, false},
      [](ROOT::VecOps::RVec<double> &values) {
        if (values[0] < 0.) values[0] = .5;
      });
  correctionManager->applyConditionalCorrections("test_correction", {{"missing_category"}}, {},
      "inactive", {"prepared_inactive"}, {false},
      [](ROOT::VecOps::RVec<double> &) { throw std::runtime_error("must be gated"); });
  auto df = dataManager->getDataFrame();
  auto one = df.Take<float>("prepared_one");
  auto two = df.Take<float>("prepared_two");
  auto up = df.Take<float>("prepared_one_shapeUp");
  auto down = df.Take<float>("prepared_one_shapeDown");
  auto inactive = df.Take<float>("prepared_inactive");
  auto original = df.Take<double>("float_arg");
  EXPECT_FLOAT_EQ((*one)[0], .1f);
  EXPECT_FLOAT_EQ((*two)[0], .5f);
  EXPECT_FLOAT_EQ((*up)[0], .2f);
  EXPECT_FLOAT_EQ((*down)[0], .1f);
  EXPECT_FLOAT_EQ((*one)[1], 1.f);
  EXPECT_FLOAT_EQ((*inactive)[0], 1.f);
  EXPECT_DOUBLE_EQ((*original)[0], -1.);
  EXPECT_FALSE(df.HasColumn("prepared_two_shapeUp"));
}

TEST_F(CorrectionManagerTest, SelectedVectorExplicitLabelsPropagateNumericInputsAndSelectorsWithoutCartesianExpansion) {
  using Vec = ROOT::VecOps::RVec<float>;
  using Cases = ROOT::VecOps::RVec<int>;
  dataManager->DefineNominal("float_arg", [] { return Vec{.5f, 1.5f}; });
  dataManager->DefineNominal("numeric_shift", [] { return Vec{1.5f, .5f}; });
  dataManager->DefineNominal("int_arg", [] { return 1; });
  dataManager->DefineNominal("case", [] { return Cases{0, 1}; });
  dataManager->DefineNominal("selector_shift", [] { return Cases{1, 0}; });
  dataManager->DefineNominal("unrelated", [] { return false; });
  dataManager->DefineNominal("unrelated_shift", [] { return true; });
  systematicManager->registerVariationColumn("float_arg", "numeric", "numeric_shift");
  systematicManager->registerVariationColumn("case", "selector", "selector_shift");
  systematicManager->registerVariationColumn("float_arg", "coherent", "numeric_shift");
  systematicManager->registerVariationColumn("case", "coherent", "selector_shift");
  systematicManager->registerVariationColumn("unrelated", "unused", "unrelated_shift");
  systematicManager->registerVariationColumn("selected_sf", "coherent", "custom_coherent_sf");
  const std::vector<CorrectionManager::VectorCorrectionCase> cases{
      {"test_correction", {"A"}}, {"test_correction", {"B"}}};
  correctionManager->applySelectedCorrectionsVecBatch({cases, cases}, "case",
      {"float_arg", "int_arg"}, {"selected_sf", "terminal_sf"}, {true, false});
  auto df = dataManager->getDataFrame();
  EXPECT_FALSE(df.HasColumn("selected_sf_coherent"));
  EXPECT_FALSE(df.HasColumn("selected_sf_unused"));
  EXPECT_FALSE(df.HasColumn("selected_sf_numeric_selector"));
  EXPECT_FALSE(df.HasColumn("terminal_sf_numeric"));
  EXPECT_FALSE(df.HasColumn("terminal_sf_selector"));
  EXPECT_FALSE(df.HasColumn("terminal_sf_coherent"));
  const auto labels = systematicManager->getVariationLabels();
  EXPECT_EQ(labels.size(), 5u);
  for (const auto &label : {"Nominal", "numeric", "selector", "coherent", "unused"})
    EXPECT_NE(std::find(labels.begin(), labels.end(), label), labels.end());
  // Book every result before the first GetValue so all assertions use one loop.
  auto nominal = df.Take<Vec>("selected_sf");
  auto numeric = df.Take<Vec>("selected_sf_numeric");
  auto selector = df.Take<Vec>("selected_sf_selector");
  auto coherent = df.Take<Vec>("custom_coherent_sf");
  auto terminal = df.Take<Vec>("terminal_sf");
  ASSERT_EQ(nominal.GetValue().size(), 2u);
  for (std::size_t event = 0; event < 2; ++event) {
    ASSERT_EQ(nominal.GetValue()[event].size(), 2u);
    ASSERT_EQ(numeric.GetValue()[event].size(), 2u);
    ASSERT_EQ(selector.GetValue()[event].size(), 2u);
    ASSERT_EQ(coherent.GetValue()[event].size(), 2u);
    ASSERT_EQ(terminal.GetValue()[event].size(), 2u);
    // Independent payload oracle: A=[.1,.2], B=[.5,.6] in the two pt bins.
    EXPECT_FLOAT_EQ(nominal.GetValue()[event][0], .1f);
    EXPECT_FLOAT_EQ(nominal.GetValue()[event][1], .6f);
    EXPECT_FLOAT_EQ(numeric.GetValue()[event][0], .2f);
    EXPECT_FLOAT_EQ(numeric.GetValue()[event][1], .5f);
    EXPECT_FLOAT_EQ(selector.GetValue()[event][0], .5f);
    EXPECT_FLOAT_EQ(selector.GetValue()[event][1], .2f);
    EXPECT_FLOAT_EQ(coherent.GetValue()[event][0], .6f);
    EXPECT_FLOAT_EQ(coherent.GetValue()[event][1], .1f);
    EXPECT_FLOAT_EQ(terminal.GetValue()[event][0], .1f);
    EXPECT_FLOAT_EQ(terminal.GetValue()[event][1], .6f);
  }
  EXPECT_EQ(df.GetNRuns(), 1u);
}

TEST_F(CorrectionManagerTest, GroupedTablesPreserveScalarEdgesFlowAndOrdering) {
  using Vec = ROOT::VecOps::RVec<float>;
  const std::vector<std::string> inputs{"eta", "pt"};
  dataManager->DefineNominal("eta", [] { return Vec{-2, 0, 1, 2, -3}; });
  dataManager->DefineNominal("pt", [] { return Vec{-1, 10, 21, 0, 0}; });
  for (const auto& name : {"a", "b", "scalar"})
    correctionManager->registerCorrection(name, "aux/mock_binned_group.json", "table", inputs);
  EXPECT_FALSE(correctionManager->tryApplyBinnedGroup({"a"}, inputs));
  EXPECT_FALSE(correctionManager->tryApplyBinnedGroup({"a", "test_correction"}, inputs));
  EXPECT_FALSE(dataManager->getDataFrame().HasColumn("input_vec_a"));
  ASSERT_TRUE(correctionManager->tryApplyBinnedGroup({"a", "b"}, inputs));
  correctionManager->applyCorrectionVec("scalar", {}, inputs, "reference");
  auto df = dataManager->getDataFrame();
  auto a = df.Take<Vec>("a");
  auto b = df.Take<Vec>("b");
  auto ref = df.Take<Vec>("reference");
  const std::vector<float> expected{1, 23, 43, -999, -999};
  ASSERT_EQ(a->at(0).size(), expected.size());
  for (size_t k = 0; k < expected.size(); ++k) {
    EXPECT_EQ(a->at(0)[k], expected[k]);
    EXPECT_EQ(b->at(0)[k], ref->at(0)[k]);
  }
  EXPECT_FALSE(correctionManager->tryApplyBinnedGroup({"a", "b"}, inputs));
}

TEST_F(CorrectionManagerTest, GroupedTablesRetainAlignedAndFiniteInputChecks) {
  using Vec = ROOT::VecOps::RVec<float>;
  const std::vector<std::string> inputs{"eta", "pt"};
  for (const auto& name : {"a", "b"})
    correctionManager->registerCorrection(name, "aux/mock_binned_group.json", "table", inputs);
  for (const auto& pts : {Vec{}, Vec{std::numeric_limits<float>::quiet_NaN()}}) {
    DataManager dm(1);
    setContextFor(dm);
    dm.DefineNominal("eta", [] { return Vec{1}; });
    dm.DefineNominal("pt", [pts] { return pts; });
    ASSERT_TRUE(correctionManager->tryApplyBinnedGroup({"a", "b"}, inputs));
    EXPECT_THROW(dm.getDataFrame().Take<Vec>("a").GetValue(), std::runtime_error);
  }
}

TEST_F(CorrectionManagerTest, GroupPreparationChecksBothAxesFlowAndFormula) {
  std::ifstream input("aux/mock_binned_group.json");
  ASSERT_TRUE(input.good());
  const std::string json{std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
  const auto reference = correction::CorrectionSet::from_string(json.c_str())->at("table");
  const auto changed = [&](const std::string& before, const std::string& after) {
    std::string copy = json;
    copy.replace(copy.find(before), before.size(), after);
    return correction::CorrectionSet::from_string(copy.c_str())->at("table");
  };
  using Group = correction::BinnedFormulaGroup;
  EXPECT_FALSE(Group::prepare({reference, changed("[-2.0,2.0]", "[-2.0,3.0]")}));
  EXPECT_FALSE(Group::prepare({reference, changed("[0.0,10.0,20.0]", "[0.0,9.0,20.0]")}));
  EXPECT_FALSE(Group::prepare({reference, changed("\"clamp\"", "\"error\"")}));
  EXPECT_FALSE(Group::prepare({reference, changed("x,[3]", "x,[2]")}));
  const auto second = changed("[2,3,10,20]", "[2,-3,10,20]");
  const auto group = Group::prepare({second, reference});
  ASSERT_TRUE(group);
  double values[2];
  EXPECT_THROW(group->evaluate(0, 10, values, 1), std::invalid_argument);
  group->evaluate(0, 10, values, 2);
  EXPECT_EQ(values[0], 17.);
  EXPECT_EQ(values[1], 23.);
}
