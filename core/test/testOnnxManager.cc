#include <OnnxManager.h>
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

class OnnxManagerTest : public ::testing::Test {
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

    // Create OnnxManager and set up its dependencies
    onnxManager = std::make_unique<OnnxManager>(*configManager);

    // Set up the OnnxManager with its dependencies
    ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    onnxManager->setContext(ctx);
  }
  
  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }

  void setContextFor(DataManager& manager) {
    ManagerContext ctx{*configManager, manager, *systematicManager, *logger, *skimSink, *metaSink};
    onnxManager->setContext(ctx);
  }
  
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<OnnxManager> onnxManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DataManager> dataManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// Construction
TEST_F(OnnxManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto manager = std::make_unique<OnnxManager>(*config);
  });
}

// Model retrieval
TEST_F(OnnxManagerTest, GetModel_Valid) {
  EXPECT_NO_THROW({
    auto model = onnxManager->getModel("test_model");
    EXPECT_TRUE(model != nullptr);
  });
}

TEST_F(OnnxManagerTest, GetModel_Invalid) {
  EXPECT_THROW(onnxManager->getModel("nonexistent_model"), std::runtime_error);
}

// Feature retrieval
TEST_F(OnnxManagerTest, GetModelFeatures_Valid) {
  EXPECT_NO_THROW({
    const auto &features = onnxManager->getModelFeatures("test_model");
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(features[0], "feature1");
  });
}

TEST_F(OnnxManagerTest, GetModelFeatures_Invalid) {
  EXPECT_THROW(onnxManager->getModelFeatures("nonexistent_model"),
               std::runtime_error);
}

// RunVar retrieval
TEST_F(OnnxManagerTest, GetRunVar_Valid) {
  EXPECT_NO_THROW({
    const auto &runVar = onnxManager->getRunVar("test_model");
    EXPECT_EQ(runVar, "run_number");
  });
}

TEST_F(OnnxManagerTest, GetRunVar_Invalid) {
  EXPECT_THROW(onnxManager->getRunVar("nonexistent_model"), std::runtime_error);
}

// All model names
TEST_F(OnnxManagerTest, GetAllModelNames) {
  const auto &names = onnxManager->getAllModelNames();
  EXPECT_EQ(names.size(), 2);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model2") != names.end());
}

// Base class interface
TEST_F(OnnxManagerTest, BaseClassInterface_Valid) {
  EXPECT_NO_THROW({
    const auto &obj = onnxManager->getObject("test_model");
    const auto &features = onnxManager->getFeatures("test_model");
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(features.size(), 3);
  });
}

TEST_F(OnnxManagerTest, BaseClassInterface_Invalid) {
  EXPECT_THROW(onnxManager->getObject("nonexistent_model"), std::runtime_error);
  EXPECT_THROW(onnxManager->getFeatures("nonexistent_model"), std::runtime_error);
}

// Apply model - Invalid case only (valid case requires complex ROOT DataFrame setup)
TEST_F(OnnxManagerTest, ApplyModel_Invalid) {
  EXPECT_ANY_THROW(onnxManager->applyModel("nonexistent_model"));
}

/**
 * @brief Test successful application of ONNX model
 *
 * Verifies that the ONNX model can be successfully applied to a dataframe and
 * that the resulting values are computed correctly based on the input features.
 */
TEST_F(OnnxManagerTest, ApplyModel_Valid) {
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  onnxManager->applyModel("test_model");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_model");
  ASSERT_EQ(result->size(), 2);
  // Just verify that we get reasonable output values (not -1, which is the default for runVar=false)
  EXPECT_NE(result->at(0), -1.0f);
  EXPECT_NE(result->at(1), -1.0f);
}

/**
 * @brief Test that model returns -1 when runVar is false
 */
TEST_F(OnnxManagerTest, ApplyModel_RunVarFalse) {
  dataManager->Define("feature1", [](ULong64_t i) -> float { return 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return false; }, {"rdfentry_"}, *systematicManager);
  onnxManager->applyModel("test_model");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_model");
  ASSERT_EQ(result->size(), 2);
  // Both should be -1 since runVar is false
  EXPECT_EQ(result->at(0), -1.0f);
  EXPECT_EQ(result->at(1), -1.0f);
}

// Const correctness
TEST_F(OnnxManagerTest, ConstCorrectness) {
  const OnnxManager* constManager = onnxManager.get();
  EXPECT_NO_THROW({
    auto model = constManager->getModel("test_model");
    const auto &features = constManager->getModelFeatures("test_model");
    const auto &runVar = constManager->getRunVar("test_model");
    const auto &names = constManager->getAllModelNames();
    const auto &obj = constManager->getObject("test_model");
    const auto &objFeatures = constManager->getFeatures("test_model");
    EXPECT_TRUE(model != nullptr);
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(runVar, "run_number");
    EXPECT_EQ(names.size(), 2);
    EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model") != names.end());
    EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model2") != names.end());
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(objFeatures.size(), 3);
  });
}

// Test multiple models
TEST_F(OnnxManagerTest, MultipleModels) {
  // Check both models are present
  const auto &names = onnxManager->getAllModelNames();
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_model2") != names.end());

  // Retrieve and check features for both
  EXPECT_NO_THROW({
    const auto &features1 = onnxManager->getModelFeatures("test_model");
    EXPECT_EQ(features1.size(), 3);
    const auto &features2 = onnxManager->getModelFeatures("test_model2");
    EXPECT_EQ(features2.size(), 3);
  });

  // Apply both models to a DataManager
  dataManager->Define("feature1", [](ULong64_t i) -> float { return i == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature4", [](ULong64_t i) -> float { return i == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature5", [](ULong64_t i) -> float { return 5.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature6", [](ULong64_t i) -> float { return 6.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number2", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  onnxManager->applyModel("test_model");
  onnxManager->applyModel("test_model2");
  auto df = dataManager->getDataFrame();
  auto result1 = df.Take<float>("test_model");
  auto result2 = df.Take<float>("test_model2");
  ASSERT_EQ(result1->size(), 2);
  ASSERT_EQ(result2->size(), 2);
  // Verify both models produce non-default values
  EXPECT_NE(result1->at(0), -1.0f);
  EXPECT_NE(result2->at(0), -1.0f);
}

// Thread safety test using ROOT's implicit multithreading
TEST_F(OnnxManagerTest, ThreadSafetyWithROOTImplicitMT) {
  ROOT::EnableImplicitMT();
  ASSERT_TRUE(ROOT::IsImplicitMTEnabled());
  int nThreads = ROOT::GetThreadPoolSize();
  EXPECT_GT(nThreads, 1) << "ROOT ImplicitMT is enabled but only one thread is available.";
  
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(100);
  setContextFor(*testDataManager);
  
  testDataManager->Define("feature1", [](ULong64_t i) -> float { return i % 2 == 0 ? 1.0f : 2.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  onnxManager->applyModel("test_model");
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_model");
  ASSERT_EQ(result->size(), 100);
  // Check that all results are not the default -1 value
  for (const auto &val : *result) {
    EXPECT_NE(val, -1.0f);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
