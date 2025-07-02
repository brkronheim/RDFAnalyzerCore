#include <BDTManager.h>
#include "test_util.h"
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <cstdlib>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>
#include <ManagerFactory.h>
#include <api/IBDTManager.h>
#include <SystematicManager.h>
#include <ROOT/TThreadExecutor.hxx>

class BDTManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    bdtManager = ManagerFactory::createBDTManager(*configManager);
    systematicManager = std::make_unique<SystematicManager>();
  }
  void TearDown() override {
    // Using smart pointers, so nothing to delete
    
  }
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<IBDTManager> bdtManager;
  std::unique_ptr<SystematicManager> systematicManager;
};

// Construction
TEST_F(BDTManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto manager = ManagerFactory::createBDTManager(*config);
  });
}

// BDT retrieval
TEST_F(BDTManagerTest, GetBDT_Valid) {
  EXPECT_NO_THROW({
    auto bdt = bdtManager->getBDT("test_bdt");
    EXPECT_TRUE(bdt != nullptr);
  });
}
TEST_F(BDTManagerTest, GetBDT_Invalid) {
  EXPECT_THROW(bdtManager->getBDT("nonexistent_bdt"), std::runtime_error);
}

// Feature retrieval
TEST_F(BDTManagerTest, GetBDTFeatures_Valid) {
  EXPECT_NO_THROW({
    const auto &features = bdtManager->getBDTFeatures("test_bdt");
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(features[0], "feature1");
  });
}
TEST_F(BDTManagerTest, GetBDTFeatures_Invalid) {
  EXPECT_THROW(bdtManager->getBDTFeatures("nonexistent_bdt"),
               std::runtime_error);
}

// RunVar retrieval
TEST_F(BDTManagerTest, GetRunVar_Valid) {
  EXPECT_NO_THROW({
    const auto &runVar = bdtManager->getRunVar("test_bdt");
    EXPECT_EQ(runVar, "run_number");
  });
}
TEST_F(BDTManagerTest, GetRunVar_Invalid) {
  EXPECT_THROW(bdtManager->getRunVar("nonexistent_bdt"), std::runtime_error);
}

// All BDT names
TEST_F(BDTManagerTest, GetAllBDTNames) {
  const auto &names = bdtManager->getAllBDTNames();
  EXPECT_EQ(names.size(), 2);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt2") != names.end());
}

// Base class interface
TEST_F(BDTManagerTest, BaseClassInterface_Valid) {
  // Downcast to concrete type for base class interface tests
  auto* concrete = dynamic_cast<BDTManager*>(bdtManager.get());
  ASSERT_TRUE(concrete != nullptr);
  EXPECT_NO_THROW({
    const auto &obj = concrete->getObject("test_bdt");
    const auto &features = concrete->getFeatures("test_bdt");
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(features.size(), 3);
  });
}
TEST_F(BDTManagerTest, BaseClassInterface_Invalid) {
  auto* concrete = dynamic_cast<BDTManager*>(bdtManager.get());
  ASSERT_TRUE(concrete != nullptr);
  EXPECT_THROW(concrete->getObject("nonexistent_bdt"), std::runtime_error);
  EXPECT_THROW(concrete->getFeatures("nonexistent_bdt"), std::runtime_error);
}

// Apply BDT - Invalid case only (valid case requires complex ROOT DataFrame
// setup)
TEST_F(BDTManagerTest, ApplyBDT_Invalid) {
  auto dataManager = ManagerFactory::createDataManager(*configManager);
  std::vector<std::string> inputFeatures = {"feature1", "feature2", "feature3"};
  std::string runVar = "run_number";
  EXPECT_ANY_THROW(bdtManager->applyBDT(*dataManager, "nonexistent_bdt", *systematicManager));
}

/**
 * @brief Test successful application of BDT
 *
 * Verifies that the BDT can be successfully applied to a dataframe and
 * that the resulting values match the expected BDT output based on the
 * input features and the test BDT tree.
 */
TEST_F(BDTManagerTest, ApplyBDT_Valid) {
  auto dataManager = std::make_unique<DataManager>(2);
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT(*dataManager, "test_bdt", *systematicManager);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_bdt");
  ASSERT_EQ(result->size(), 2);
  auto sigmoid = [](float x) { return 1.0f / (1.0f + std::exp(-x)); };
  EXPECT_NEAR(result->at(0), sigmoid(0.1f), 1e-6);
  EXPECT_NEAR(result->at(1), sigmoid(0.9f), 1e-6);
}

// Const correctness
TEST_F(BDTManagerTest, ConstCorrectness) {
  const IBDTManager* constManager = bdtManager.get();
  EXPECT_NO_THROW({
    auto bdt = constManager->getBDT("test_bdt");
    const auto &features = constManager->getBDTFeatures("test_bdt");
    const auto &runVar = constManager->getRunVar("test_bdt");
    const auto &names = constManager->getAllBDTNames();
    auto* concrete = dynamic_cast<const BDTManager*>(constManager);
    ASSERT_TRUE(concrete != nullptr);
    const auto &obj = concrete->getObject("test_bdt");
    const auto &objFeatures = concrete->getFeatures("test_bdt");
    EXPECT_TRUE(bdt != nullptr);
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(runVar, "run_number");
    EXPECT_EQ(names.size(), 2);
    EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt2") != names.end());
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(objFeatures.size(), 3);
  });
}

// Test multiple BDTs
TEST_F(BDTManagerTest, MultipleBDTs) {
  // Check both BDTs are present
  const auto &names = bdtManager->getAllBDTNames();
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_bdt2") != names.end());

  // Retrieve and check features for both
  EXPECT_NO_THROW({
    const auto &features1 = bdtManager->getBDTFeatures("test_bdt");
    EXPECT_EQ(features1.size(), 3);
    const auto &features2 = bdtManager->getBDTFeatures("test_bdt2");
    EXPECT_EQ(features2.size(), 3);
  });

  // Apply both BDTs to a DataManager
  auto dataManager = std::make_unique<DataManager>(2);
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature4", [](ULong64_t i) -> float { return i == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature5", [](ULong64_t i) -> float { return 5.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature6", [](ULong64_t i) -> float { return 6.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number2", [](ULong64_t i) -> bool { return false; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT(*dataManager, "test_bdt", *systematicManager);
  bdtManager->applyBDT(*dataManager, "test_bdt2", *systematicManager);
  auto df = dataManager->getDataFrame();
  auto result1 = df.Take<float>("test_bdt");
  auto result2 = df.Take<float>("test_bdt2");
  ASSERT_EQ(result1->size(), 2);
  ASSERT_EQ(result2->size(), 2);
}

// Thread safety test using ROOT's implicit multithreading
TEST_F(BDTManagerTest, ThreadSafetyWithROOTImplicitMT) {
  ROOT::EnableImplicitMT();
  ASSERT_TRUE(ROOT::IsImplicitMTEnabled());
  int nThreads = ROOT::GetThreadPoolSize();
  EXPECT_GT(nThreads, 1) << "ROOT ImplicitMT is enabled but only one thread is available.";
  auto dataManager = std::make_unique<DataManager>(100);
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i % 2 == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT(*dataManager, "test_bdt", *systematicManager);
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_bdt");
  ASSERT_EQ(result->size(), 100);
  // Check that all results are within expected sigmoid range
  for (const auto &val : *result) {
    EXPECT_GE(val, 0.0f);
    EXPECT_LE(val, 1.0f);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}