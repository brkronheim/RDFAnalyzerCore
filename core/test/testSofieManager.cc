#include <SofieManager.h>
#include <test_util.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <ManagerFactory.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <api/ManagerContext.h>
#include <SystematicManager.h>
#include <ROOT/TThreadExecutor.hxx>

// Mock SOFIE inference function for testing
std::vector<float> mockSofieInference(const std::vector<float>& input) {
  // Simple mock: return sum of inputs
  float sum = 0.0f;
  for (float val : input) {
    sum += val;
  }
  return {sum};
}

class SofieManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    
    // Create a simple configuration without sofieConfig
    configManager = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
    systematicManager = std::make_unique<SystematicManager>();
    dataManager = std::make_unique<DataManager>(2);
    
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();

    // Create SofieManager - won't load models from config since sofieConfig is not set
    sofieManager = std::make_unique<SofieManager>(*configManager);

    // Set up the SofieManager with its dependencies
    ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    sofieManager->setContext(ctx);
    
    // Manually register a test SOFIE model
    auto inferenceFunc = std::make_shared<SofieInferenceFunction>(mockSofieInference);
    std::vector<std::string> features = {"feature1", "feature2", "feature3"};
    sofieManager->registerModel("test_sofie", inferenceFunc, features, "run_number");
  }
  
  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }

  void setContextFor(DataManager& manager) {
    ManagerContext ctx{*configManager, manager, *systematicManager, *logger, *skimSink, *metaSink};
    sofieManager->setContext(ctx);
  }
  
  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<SofieManager> sofieManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DataManager> dataManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// Construction
TEST_F(SofieManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
    auto manager = std::make_unique<SofieManager>(*config);
  });
}

// Model retrieval
TEST_F(SofieManagerTest, GetModel_Valid) {
  EXPECT_NO_THROW({
    auto model = sofieManager->getModel("test_sofie");
    EXPECT_TRUE(model != nullptr);
  });
}

TEST_F(SofieManagerTest, GetModel_Invalid) {
  EXPECT_THROW(sofieManager->getModel("nonexistent_model"), std::runtime_error);
}

// Feature retrieval
TEST_F(SofieManagerTest, GetModelFeatures_Valid) {
  EXPECT_NO_THROW({
    const auto &features = sofieManager->getModelFeatures("test_sofie");
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(features[0], "feature1");
    EXPECT_EQ(features[1], "feature2");
    EXPECT_EQ(features[2], "feature3");
  });
}

TEST_F(SofieManagerTest, GetModelFeatures_Invalid) {
  EXPECT_THROW(sofieManager->getModelFeatures("nonexistent_model"),
               std::runtime_error);
}

// RunVar retrieval
TEST_F(SofieManagerTest, GetRunVar_Valid) {
  EXPECT_NO_THROW({
    const auto &runVar = sofieManager->getRunVar("test_sofie");
    EXPECT_EQ(runVar, "run_number");
  });
}

TEST_F(SofieManagerTest, GetRunVar_Invalid) {
  EXPECT_THROW(sofieManager->getRunVar("nonexistent_model"), std::runtime_error);
}

// All model names
TEST_F(SofieManagerTest, GetAllModelNames) {
  const auto &names = sofieManager->getAllModelNames();
  EXPECT_EQ(names.size(), 1);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_sofie") != names.end());
}

// Base class interface
TEST_F(SofieManagerTest, BaseClassInterface_Valid) {
  EXPECT_NO_THROW({
    const auto &obj = sofieManager->getObject("test_sofie");
    const auto &features = sofieManager->getFeatures("test_sofie");
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(features.size(), 3);
  });
}

TEST_F(SofieManagerTest, BaseClassInterface_Invalid) {
  EXPECT_THROW(sofieManager->getObject("nonexistent_model"), std::runtime_error);
  EXPECT_THROW(sofieManager->getFeatures("nonexistent_model"), std::runtime_error);
}

// Apply model - Invalid case only
TEST_F(SofieManagerTest, ApplyModel_Invalid) {
  EXPECT_ANY_THROW(sofieManager->applyModel("nonexistent_model"));
}

/**
 * @brief Test successful application of SOFIE model
 *
 * Verifies that the SOFIE model can be successfully applied to a dataframe and
 * that the resulting values are computed correctly based on the input features.
 */
TEST_F(SofieManagerTest, ApplyModel_Valid) {
  dataManager->Define("feature1", [](ULong64_t i) -> float { return 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  sofieManager->applyModel("test_sofie");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_sofie");
  ASSERT_EQ(result->size(), 2);
  // Mock function returns sum of inputs: 1 + 2 + 3 = 6
  EXPECT_FLOAT_EQ(result->at(0), 6.0f);
  EXPECT_FLOAT_EQ(result->at(1), 6.0f);
}

/**
 * @brief Test that model returns -1 when runVar is false
 */
TEST_F(SofieManagerTest, ApplyModel_RunVarFalse) {
  dataManager->Define("feature1", [](ULong64_t i) -> float { return 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return false; }, {"rdfentry_"}, *systematicManager);
  sofieManager->applyModel("test_sofie");
  auto df = dataManager->getDataFrame();
  auto result = df.Take<float>("test_sofie");
  ASSERT_EQ(result->size(), 2);
  // Both should be -1 since runVar is false
  EXPECT_EQ(result->at(0), -1.0f);
  EXPECT_EQ(result->at(1), -1.0f);
}

// Const correctness
TEST_F(SofieManagerTest, ConstCorrectness) {
  const SofieManager* constManager = sofieManager.get();
  EXPECT_NO_THROW({
    auto model = constManager->getModel("test_sofie");
    const auto &features = constManager->getModelFeatures("test_sofie");
    const auto &runVar = constManager->getRunVar("test_sofie");
    const auto &names = constManager->getAllModelNames();
    const auto &obj = constManager->getObject("test_sofie");
    const auto &objFeatures = constManager->getFeatures("test_sofie");
    EXPECT_TRUE(model != nullptr);
    EXPECT_EQ(features.size(), 3);
    EXPECT_EQ(runVar, "run_number");
    EXPECT_EQ(names.size(), 1);
    EXPECT_TRUE(std::find(names.begin(), names.end(), "test_sofie") != names.end());
    EXPECT_TRUE(obj != nullptr);
    EXPECT_EQ(objFeatures.size(), 3);
  });
}

// Test multiple models
TEST_F(SofieManagerTest, MultipleModels) {
  // Register a second model
  auto inferenceFunc2 = std::make_shared<SofieInferenceFunction>(
    [](const std::vector<float>& input) -> std::vector<float> {
      // Different mock: return product
      float product = 1.0f;
      for (float val : input) {
        product *= val;
      }
      return {product};
    }
  );
  std::vector<std::string> features2 = {"feature4", "feature5", "feature6"};
  sofieManager->registerModel("test_sofie2", inferenceFunc2, features2, "run_number2");
  
  // Check both models are present
  const auto &names = sofieManager->getAllModelNames();
  EXPECT_EQ(names.size(), 2);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_sofie") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "test_sofie2") != names.end());

  // Retrieve and check features for both
  EXPECT_NO_THROW({
    const auto &features1 = sofieManager->getModelFeatures("test_sofie");
    EXPECT_EQ(features1.size(), 3);
    const auto &features2 = sofieManager->getModelFeatures("test_sofie2");
    EXPECT_EQ(features2.size(), 3);
  });

  // Apply both models to a DataManager
  dataManager->Define("feature1", [](ULong64_t i) -> float { return 1.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature4", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature5", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("feature6", [](ULong64_t i) -> float { return 4.0f; }, {"rdfentry_"}, *systematicManager);
  dataManager->Define("run_number2", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  sofieManager->applyModel("test_sofie");
  sofieManager->applyModel("test_sofie2");
  auto df = dataManager->getDataFrame();
  auto result1 = df.Take<float>("test_sofie");
  auto result2 = df.Take<float>("test_sofie2");
  ASSERT_EQ(result1->size(), 2);
  ASSERT_EQ(result2->size(), 2);
  // First model: sum = 1 + 2 + 3 = 6
  EXPECT_FLOAT_EQ(result1->at(0), 6.0f);
  // Second model: product = 2 * 3 * 4 = 24
  EXPECT_FLOAT_EQ(result2->at(0), 24.0f);
}

// Thread safety test using ROOT's implicit multithreading
TEST_F(SofieManagerTest, ThreadSafetyWithROOTImplicitMT) {
  ROOT::EnableImplicitMT();
  ASSERT_TRUE(ROOT::IsImplicitMTEnabled());
  int nThreads = ROOT::GetThreadPoolSize();
  EXPECT_GT(nThreads, 1) << "ROOT ImplicitMT is enabled but only one thread is available.";
  
  // Create a new DataManager for this test
  auto testDataManager = std::make_unique<DataManager>(100);
  setContextFor(*testDataManager);
  
  testDataManager->Define("feature1", [](ULong64_t i) -> float { return 1.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature2", [](ULong64_t i) -> float { return 2.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("feature3", [](ULong64_t i) -> float { return 3.0f; }, {"rdfentry_"}, *systematicManager);
  testDataManager->Define("run_number", [](ULong64_t i) -> bool { return true; }, {"rdfentry_"}, *systematicManager);
  sofieManager->applyModel("test_sofie");
  auto df = testDataManager->getDataFrame();
  auto result = df.Take<float>("test_sofie");
  ASSERT_EQ(result->size(), 100);
  // Check that all results are the expected value (sum = 6.0)
  for (const auto &val : *result) {
    EXPECT_FLOAT_EQ(val, 6.0f);
  }
}

// Test manual registration
TEST_F(SofieManagerTest, ManualRegistration) {
  // Create a fresh manager
  auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  auto manager = std::make_unique<SofieManager>(*config);
  
  // Manually register a model
  auto inferenceFunc = std::make_shared<SofieInferenceFunction>(
    [](const std::vector<float>& input) -> std::vector<float> {
      return {input[0] * 2.0f};
    }
  );
  std::vector<std::string> features = {"x"};
  manager->registerModel("double_x", inferenceFunc, features, "always_true");
  
  // Verify registration
  EXPECT_NO_THROW({
    auto model = manager->getModel("double_x");
    EXPECT_TRUE(model != nullptr);
  });
  
  const auto &retrievedFeatures = manager->getModelFeatures("double_x");
  EXPECT_EQ(retrievedFeatures.size(), 1);
  EXPECT_EQ(retrievedFeatures[0], "x");
  
  const auto &runVar = manager->getRunVar("double_x");
  EXPECT_EQ(runVar, "always_true");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
