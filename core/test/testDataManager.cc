/**
 * @file testDataManager.cc
 * @brief Unit tests for the DataManager class
 * @date 2025
 *
 * This file contains unit tests for the DataManager class, focusing on
 * construction, DataFrame manipulation, error handling, and integration
 * with ConfigurationManager and SystematicManager. Redundant and trivial
 * tests have been removed for clarity and maintainability.
 */

#include <ConfigurationManager.h>
#include "test_util.h"
#include <DataManager.h>
#include <ManagerFactory.h>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>
#include <filesystem>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>
#include <SystematicManager.h>

/**
 * @brief Test fixture for DataManager tests
 *
 * Provides common setup and teardown for all DataManager tests, using
 * ManagerFactory and smart pointers for consistency with other manager tests.
 */
class DataManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config_minimal.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    dataManager = ManagerFactory::createDataManager(*configManager);
  }
  void TearDown() override {}
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<IDataFrameProvider> dataManager;
};

/**
 * @brief Test construction of DataManager
 */
TEST_F(DataManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto manager = ManagerFactory::createDataManager(*config);
  });
}

/**
 * @brief Test DataFrame creation and retrieval
 */
TEST_F(DataManagerTest, DataFrameDefineAndRetrieve) {
  // Use a unique column name to avoid conflicts
  EXPECT_NO_THROW({
    dataManager->Define("unique_test_col", []() { return 42; });
    auto df = dataManager->getDataFrame();
    auto result = df.Take<int>("unique_test_col");
    ASSERT_EQ(result->size(), df.Count().GetValue());
    if (!result->empty()) {
      EXPECT_EQ(result->at(0), 42);
    }
  });
}

/**
 * @brief Test DataFrame filtering
 */
TEST_F(DataManagerTest, DataFrameFilter) {
  dataManager->Define("unique_test_col2", []() { return 1; });
  EXPECT_NO_THROW({
    dataManager->Filter([](int x) { return x > 0; }, {"unique_test_col2"});
    auto df = dataManager->getDataFrame();
    auto result = df.Take<int>("unique_test_col2");
    for (auto v : *result) {
      EXPECT_GT(v, 0);
    }
  });
}

/**
 * @brief Test error handling for invalid column names
 */
TEST_F(DataManagerTest, ErrorHandlingInvalidColumn) {
  EXPECT_THROW({
    dataManager->Define("test_error", [](int x) { return x; }, {"nonexistent_column"});
  }, std::exception);
}

/**
 * @brief Test integration with ConfigurationManager
 */
TEST_F(DataManagerTest, ConfigurationManagerIntegration) {
  EXPECT_EQ(configManager->get("saveFile"), "test_output_minimal.root");
  EXPECT_EQ(configManager->get("saveTree"), "Events");
  EXPECT_EQ(configManager->get("threads"), "1");
}

/**
 * @brief Test memory management for DataManager
 */
TEST_F(DataManagerTest, MemoryManagement) {
  auto localConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto localManager = ManagerFactory::createDataManager(*localConfig);
  EXPECT_TRUE(localManager != nullptr);
}

/**
 * @brief Test template method variations for Define
 */
TEST_F(DataManagerTest, TemplateMethodVariations) {
  EXPECT_NO_THROW({
    dataManager->Define("int_var", []() { return 42; });
    dataManager->Define("double_var", []() { return 3.14; });
    dataManager->Define("string_var", []() { return std::string("test"); });
    dataManager->Define("bool_var", []() { return true; });
    dataManager->Define("func_int", [](int x) { return x * 2; }, {"int_var"});
    dataManager->Define("func_double", [](double x) { return x * 2.0; }, {"double_var"});
  });
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}