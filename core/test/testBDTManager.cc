#include <BDTManager.h>
#include <test_util.h>
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
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <api/ManagerContext.h>
#include <SystematicManager.h>
#include <ROOT/TThreadExecutor.hxx>

class BDTManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    systematicManager = std::make_unique<SystematicManager>();
    dataManager = std::make_unique<DataManager>(2);
    
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();

    // Create BDTManager and set up its dependencies
    bdtManager = std::make_unique<BDTManager>(*configManager);

    // Set up the BDTManager with its dependencies
    ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    bdtManager->setContext(ctx);
  }
  void TearDown() override {
    // Using smart pointers, so nothing to delete
    
  }

  void setContextFor(DataManager& manager) {
    ManagerContext ctx{*configManager, manager, *systematicManager, *logger, *skimSink, *metaSink};
    bdtManager->setContext(ctx);
  }
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<BDTManager> bdtManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DataManager> dataManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// Construction
TEST_F(BDTManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto manager = std::make_unique<BDTManager>(*config);
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
  EXPECT_NO_THROW({
    const auto &obj = bdtManager->getObject("test_bdt");
    const auto &features = bdtManager->getFeatures("test_bdt");
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(features.size(), 3);
  });
}
TEST_F(BDTManagerTest, BaseClassInterface_Invalid) {
  EXPECT_THROW(bdtManager->getObject("nonexistent_bdt"), std::runtime_error);
  EXPECT_THROW(bdtManager->getFeatures("nonexistent_bdt"), std::runtime_error);
}

// Apply BDT - Invalid case only (valid case requires complex ROOT DataFrame
// setup)
TEST_F(BDTManagerTest, ApplyBDT_Invalid) {
  EXPECT_ANY_THROW(bdtManager->applyBDT("nonexistent_bdt"));
}

/**
 * @brief Test successful application of BDT
 *
 * Verifies that the BDT can be successfully applied to a dataframe and
 * that the resulting values match the expected BDT output based on the
 * input features and the test BDT tree.
 */
TEST_F(BDTManagerTest, ApplyBDT_Valid) {
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT("test_bdt");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_bdt");
  ASSERT_EQ(result->size(), 2);
  auto sigmoid = [](float x) { return 1.0f / (1.0f + std::exp(-x)); };
    // The FastForest implementation adds a base_score from the model file
  // (see aux/test_bdt.txt). The raw scores are therefore leaf_value + base_score.
  EXPECT_NEAR(result->at(0), sigmoid(0.1f + 0.5f), 1e-6);
  EXPECT_NEAR(result->at(1), sigmoid(0.9f + 0.5f), 1e-6);}

// Const correctness
TEST_F(BDTManagerTest, ConstCorrectness) {
  const BDTManager* constManager = bdtManager.get();
  EXPECT_NO_THROW({
    auto bdt = constManager->getBDT("test_bdt");
    const auto &features = constManager->getBDTFeatures("test_bdt");
    const auto &runVar = constManager->getRunVar("test_bdt");
    const auto &names = constManager->getAllBDTNames();
    const auto &obj = constManager->getObject("test_bdt");
    const auto &objFeatures = constManager->getFeatures("test_bdt");
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
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature4", [](ULong64_t i) -> float { return i == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature5", [](ULong64_t i) -> float { return 5.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature6", [](ULong64_t i) -> float { return 6.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number2", [](ULong64_t i) -> bool { return false; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT("test_bdt");
  bdtManager->applyBDT("test_bdt2");
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
  
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(100);
  setContextFor(*testDataManager);
  
  testDataManager->Define("feature1", [](ULong64_t i) -> float { return i % 2 == 0 ? 0.0f : 1.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  bdtManager->applyBDT("test_bdt");
  auto df = testDataManager->getDataFrame();
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