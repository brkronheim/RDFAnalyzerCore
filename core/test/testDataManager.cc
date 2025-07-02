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
    systematicManager = std::make_unique<SystematicManager>();
  }
  void TearDown() override {}
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<IDataFrameProvider> dataManager;
  std::unique_ptr<SystematicManager> systematicManager;
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
    dataManager->Define("unique_test_col", []() { return 42; }, {}, *systematicManager);
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
  dataManager->Define("unique_test_col2", []() { return 1; }, {}, *systematicManager);
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
    dataManager->Define("test_error", [](int x) { return x; }, {"nonexistent_column"}, *systematicManager);
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
    dataManager->Define("int_var", []() { return 42; }, {}, *systematicManager);
    dataManager->Define("double_var", []() { return 3.14; }, {}, *systematicManager);
    dataManager->Define("string_var", []() { return std::string("test"); }, {}, *systematicManager);
    dataManager->Define("bool_var", []() { return true; }, {}, *systematicManager);
    dataManager->Define("func_int", [](int x) { return x * 2; }, {"int_var"}, *systematicManager);
    dataManager->Define("func_double", [](double x) { return x * 2.0; }, {"double_var"}, *systematicManager);
  });
}

/**
 * @brief Test setting a custom RNode using setDataFrame
 *
 * Verifies that setDataFrame correctly updates the internal dataframe node and that subsequent operations use the new node.
 */
TEST_F(DataManagerTest, SetDataFrameUpdatesNode) {
  // Create a new DataManager with a different config
  auto altConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto altManager = ManagerFactory::createDataManager(*altConfig);
  // Define a column in the alternate manager
  altManager->Define("alt_col", []() { return 99; }, {}, *systematicManager);
  // Set the data frame of the main manager to the alternate's
  dataManager->setDataFrame(altManager->getDataFrame());
  // Check that the new column is available
  auto df = dataManager->getDataFrame();
  auto result = df.Take<int>("alt_col");
  ASSERT_EQ(result->size(), df.Count().GetValue());
  if (!result->empty()) {
    EXPECT_EQ(result->at(0), 99);
  }
}

/**
 * @brief Test getChain returns a valid TChain pointer
 *
 * Verifies that getChain returns a non-null pointer when constructed with a config file.
 */
TEST_F(DataManagerTest, GetChainReturnsValidPointer) {
  auto chain = dynamic_cast<DataManager*>(dataManager.get())->getChain();
  EXPECT_NE(chain, nullptr);
  // Optionally check chain properties if needed
}

/**
 * @brief Test DefineVector creates a vector column
 *
 * Verifies that DefineVector correctly creates a vector column from existing scalar columns.
 */
TEST_F(DataManagerTest, DefineVectorCreatesVectorColumn) {
  dataManager->Define("col1", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("col2", []() { return 2.0f; }, {}, *systematicManager);
  EXPECT_NO_THROW({
    dataManager->DefineVector("vec_col", {"col1", "col2"}, "float", *systematicManager);
    auto df = dataManager->getDataFrame();
    auto result = df.Take<ROOT::VecOps::RVec<float>>("vec_col");
    ASSERT_EQ(result->size(), df.Count().GetValue());
    if (!result->empty()) {
      EXPECT_EQ((*result)[0][0], 1.0f);
      EXPECT_EQ((*result)[0][1], 2.0f);
    }
  });
}

/**
 * @brief Test Filter_m applies a filter to the dataframe
 *
 * Verifies that Filter_m correctly filters rows based on a predicate.
 */
TEST_F(DataManagerTest, Filter_mAppliesFilter) {
  dataManager->Define("filt_col", []() { return 5; }, {}, *systematicManager);
  dynamic_cast<DataManager*>(dataManager.get())->Filter_m([](int x) { return x == 5; }, {"filt_col"});
  auto df = dataManager->getDataFrame();
  auto result = df.Take<int>("filt_col");
  for (auto v : *result) {
    EXPECT_EQ(v, 5);
  }
}

/**
 * @brief Test DefinePerSample_m defines a per-sample variable
 *
 * Verifies that DefinePerSample_m creates a column with the expected value for all samples.
 */
TEST_F(DataManagerTest, DefinePerSample_mCreatesColumn) {
  dynamic_cast<DataManager*>(dataManager.get())->DefinePerSample_m("sample_col", [](unsigned int, const ROOT::RDF::RSampleInfo&) { return 7; });
  auto df = dataManager->getDataFrame();
  auto result = df.Take<int>("sample_col");
  for (auto v : *result) {
    EXPECT_EQ(v, 7);
  }
}

/**
 * @brief Test defineConstant creates a constant column
 *
 * Verifies that defineConstant creates a column with the same value for all entries.
 */
TEST_F(DataManagerTest, DefineConstantCreatesConstantColumn) {
  dynamic_cast<DataManager*>(dataManager.get())->defineConstant("const_col", 123);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<int>("const_col");
  for (auto v : *result) {
    EXPECT_EQ(v, 123);
  }
}

/**
 * @brief Test Redefine updates an existing column
 *
 * Verifies that Redefine changes the values of an existing column.
 */
TEST_F(DataManagerTest, RedefineUpdatesColumn) {
  dataManager->Define("to_redefine", []() { return 1; }, {}, *systematicManager);
  dynamic_cast<DataManager*>(dataManager.get())->Redefine("to_redefine", [](int) { return 42; }, {"to_redefine"});
  auto df = dataManager->getDataFrame();
  auto result = df.Take<int>("to_redefine");
  for (auto v : *result) {
    EXPECT_EQ(v, 42);
  }
}

/**
 * @brief Test makeSystList creates systematic variation columns
 *
 * Verifies that makeSystList creates columns for each systematic variation and the nominal branch.
 */
TEST_F(DataManagerTest, MakeSystListCreatesSystematicColumns) {
  // Register a systematic
  systematicManager->registerSystematic("testSyst", {"branch"});
  auto systList = dynamic_cast<DataManager*>(dataManager.get())->makeSystList("syst_branch", *systematicManager);
  // Check that the returned list includes Nominal, testSystUp, testSystDown
  EXPECT_NE(std::find(systList.begin(), systList.end(), "Nominal"), systList.end());
  EXPECT_NE(std::find(systList.begin(), systList.end(), "testSystUp"), systList.end());
  EXPECT_NE(std::find(systList.begin(), systList.end(), "testSystDown"), systList.end());
  // Check that the columns exist in the dataframe
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "syst_branch"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "syst_branch_testSystUp"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "syst_branch_testSystDown"), colNames.end());
}

/**
 * @brief Test registerConstants registers constants from config
 *
 * Verifies that registerConstants creates columns for each constant defined in the config.
 */
TEST_F(DataManagerTest, RegisterConstantsCreatesColumns) {
  auto dm = dynamic_cast<DataManager*>(dataManager.get());
  configManager->set("floatConfigDM", "cfg/floats.txt");
  configManager->set("intConfigDM", "cfg/ints.txt");
  dm->registerConstants(*configManager, "floatConfigDM", "intConfigDM");
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "float1"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "int1"), colNames.end());
}

/**
 * @brief Test registerAliases registers aliases from config
 *
 * Verifies that registerAliases creates alias columns as specified in the config.
 */
TEST_F(DataManagerTest, RegisterAliasesCreatesAliasColumns) {
  std::cout << "Getting data manager" << std::endl;
  auto dm = dynamic_cast<DataManager*>(dataManager.get());
  std::cout << "Setting alias config" << std::endl;
  configManager->set("aliasConfigDM", "cfg/alias.txt");
  std::cout << "Defining existing column" << std::endl;
  dataManager->Define("existing_col", []() { return 11; }, {}, *systematicManager);
  std::cout << "Registering aliases" << std::endl;
  dm->registerAliases(*configManager, "aliasConfigDM");
  std::cout << "Getting dataframe" << std::endl;
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "new_col"), colNames.end());
}

/**
 * @brief Test registerOptionalBranches registers optional branches from config
 *
 * Verifies that registerOptionalBranches creates columns for optional branches as specified in the config.
 */
TEST_F(DataManagerTest, RegisterOptionalBranchesCreatesOptionalColumns) {
  auto dm = dynamic_cast<DataManager*>(dataManager.get());
  configManager->set("optionalBranchesConfigDM", "cfg/optionalBranches.txt");
  dm->registerOptionalBranches(*configManager, "optionalBranchesConfigDM");
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "optional_col"), colNames.end());
}

/**
 * @brief Test finalizeSetup calls all registration methods
 *
 * Verifies that finalizeSetup results in all expected columns being created from config.
 */
TEST_F(DataManagerTest, FinalizeSetupRegistersAll) {
  auto dm = dynamic_cast<DataManager*>(dataManager.get());
  dataManager->Define("existing_col", []() { return 1.0; }, {}, *systematicManager);
  configManager->set("floatConfigDM", "cfg/floats.txt");
  configManager->set("intConfigDM", "cfg/ints.txt");
  configManager->set("aliasConfigDM", "cfg/alias.txt");
  configManager->set("optionalBranchesConfigDM", "cfg/optionalBranches.txt");
  dm->finalizeSetup(*configManager, "floatConfigDM", "intConfigDM", "aliasConfigDM", "optionalBranchesConfigDM");
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "float1"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "int1"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "new_col"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "optional_col"), colNames.end());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}