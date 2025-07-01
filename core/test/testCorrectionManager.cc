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

#include "ConfigurationManager.h"
#include "test_util.h"
#include "CorrectionManager.h"
#include "DataManager.h"
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
#include "ManagerFactory.h"
#include <api/ICorrectionManager.h>
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
    correctionManager = ManagerFactory::createCorrectionManager(*configManager);
  }

  /**
   * @brief Tear down the test fixture
   *
   * Cleans up the manager instances to prevent memory leaks.
   */
  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }

  std::unique_ptr<IConfigurationProvider> configManager; ///< The configuration manager instance for testing
  std::unique_ptr<ICorrectionManager> correctionManager; ///< The correction manager instance for testing
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
  auto dataManager = std::make_unique<DataManager>(2);
  std::vector<std::string> stringArguments = {"test"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  EXPECT_THROW({
    correctionManager->applyCorrection(*dataManager, "nonexistent_correction", stringArguments, inputFeatures);
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
  auto dataManager = std::make_unique<DataManager>(2);
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; }, {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
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
  auto dataManager = std::make_unique<DataManager>(2);
  dataManager->Define("float_arg", [](ULong64_t i) -> double { return i == 0 ? 0.5 : 1.5; }, {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return i == 0 ? 1 : 2; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"B"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
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
  auto dataManager = std::make_unique<DataManager>(4);
  dataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0 ? 0.0 : (i == 1 ? 1.0 : (i == 2 ? 2.0 : 3.0));
                },
                {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
  ASSERT_EQ(result->size(), 4);
  EXPECT_NEAR(result->at(0), 0.1, 1e-6); // 0.0 falls in first bin [0.0, 1.0)
  EXPECT_NEAR(result->at(1), 0.2, 1e-6); // 1.0 falls in second bin [1.0, 2.0)
  EXPECT_NEAR(result->at(2), 0.0, 1e-6); // 2.0 is overflow (>=2.0)
  EXPECT_NEAR(result->at(3), 0.0, 1e-6); // 3.0 is overflow (>=2.0)
}

/**
 * @brief Test correction application with empty dataframe
 *
 * Verifies that the correction application works correctly with an empty
 * dataframe, ensuring no crashes or unexpected behavior.
 */
TEST_F(CorrectionManagerTest, ApplyCorrectionWithEmptyDataframe) {
  auto dataManager = std::make_unique<DataManager>(0);
  // Define required columns even for empty dataframe
  dataManager->Define("float_arg", []() -> double { return 0.5; });
  dataManager->Define("int_arg", []() -> double { return 1; });
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  });
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
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
  EXPECT_EQ(features.size(), 3);
  EXPECT_EQ(features[0], "float_arg");
  EXPECT_EQ(features[1], "int_arg");
  EXPECT_EQ(features[2], "str_arg");
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
  auto dataManager = std::make_unique<DataManager>(4);
  dataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0   ? std::numeric_limits<double>::max()
                         : i == 1 ? std::numeric_limits<double>::min()
                         : i == 2 ? -std::numeric_limits<double>::max()
                                  : std::numeric_limits<double>::lowest();
                },
                {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  });
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
  ASSERT_EQ(result->size(), 4);
  for (size_t i = 0; i < result->size(); ++i) {
    EXPECT_TRUE(std::abs(result->at(i)) < 1e-6 ||
                std::abs(result->at(i) - 0.1) < 1e-6 ||
                std::abs(result->at(i) - 0.2) < 1e-6);
  }
}

/**
 * @brief Test handling of NaN and infinity values
 *
 * Verifies that the CorrectionManager handles NaN and infinity values
 * gracefully without crashing or producing undefined behavior.
 */
TEST_F(CorrectionManagerTest, NaNAndInfinityHandling) {
  auto dataManager = std::make_unique<DataManager>(3);
  dataManager->Define("float_arg",
                [](ULong64_t i) -> double {
                  return i == 0   ? std::numeric_limits<double>::quiet_NaN()
                         : i == 1 ? std::numeric_limits<double>::infinity()
                                  : -std::numeric_limits<double>::infinity();
                },
                {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  });
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
  ASSERT_EQ(result->size(), 3);
  for (size_t i = 0; i < result->size(); ++i) {
    EXPECT_TRUE(std::isnan(result->at(i)) || std::abs(result->at(i)) < 1e-6);
  }
}

/**
 * @brief Test handling of zero and negative values
 *
 * Verifies that the CorrectionManager handles zero and negative values
 * correctly, especially in edge cases.
 */
TEST_F(CorrectionManagerTest, ZeroAndNegativeValues) {
  auto dataManager = std::make_unique<DataManager>(4);
  dataManager->Define("float_arg",
                [](ULong64_t i) -> double { return i == 0   ? 0.0 : (i == 1 ? -1.0 : (i == 2 ? -2.0 : 1.0)); },
                {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};
  correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
  ASSERT_EQ(result->size(), 4);
  EXPECT_NEAR(result->at(0), 0.1, 1e-6); // 0.0 in first bin
  EXPECT_NEAR(result->at(1), 0.0, 1e-6); // -1.0 is flow
  EXPECT_NEAR(result->at(2), 0.0, 1e-6); // -2.0 is flow
  EXPECT_NEAR(result->at(3), 0.2, 1e-6); // 1.0 in second bin
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
                {"rdfentry_"});
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"wrong_feature_name", "int_arg"};
  EXPECT_NO_THROW({
    correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  });
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_correction");
  ASSERT_EQ(result->size(), 2);
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
  for (const auto &column : dataManager->getDataFrame().GetColumnNames()) {
    std::cout << "Starting Column: " << column << std::endl;
  }
  
  // Only define int_arg, not float_arg
  dataManager->Define("int_arg", [](ULong64_t i) -> double { return 1; }, {"rdfentry_"});
  // Note: float_arg is not defined
  
  std::vector<std::string> stringArguments = {"A"};
  std::vector<std::string> inputFeatures = {"float_arg", "int_arg"};

  // The current implementation should throw a runtime_error when columns are missing
  EXPECT_THROW({
    correctionManager->applyCorrection(*dataManager, "test_correction", stringArguments, inputFeatures);
  }, std::runtime_error);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}