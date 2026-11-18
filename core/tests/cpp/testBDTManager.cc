#include <BDTManager.h>
#include <atomic>
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
#include <filesystem>
#include <fstream>
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

TEST_F(BDTManagerTest, LifecycleReusesLoadedModelAfterSourceIsRemoved) {
  char pattern[] = "/tmp/rdf_bdt_loaded_XXXXXX";
  const auto created = mkdtemp(pattern);
  ASSERT_NE(created, nullptr);
  struct Directory {
    std::filesystem::path path;
    ~Directory() { std::filesystem::remove_all(path); }
  } directory{created};
  const auto model = directory.path / "model.txt";
  const auto models = directory.path / "models.txt";
  const auto settings = directory.path / "config.txt";
  std::filesystem::copy_file("aux/test_bdt.txt", model);
  { std::ofstream file(models);
    file << "file=" << model.string()
         << " name=cached inputVariables=feature1,feature2,feature3 runVar=run_number\n"; }
  { std::ofstream file(settings); file << "bdtConfig=" << models.string() << '\n'; }
  ConfigurationManager config(settings.string());
  BDTManager manager(config);
  const auto loaded = manager.getBDT("cached");
  std::filesystem::remove(model);
  ManagerContext context{config, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  manager.setContext(context);
  EXPECT_NO_THROW(manager.setupFromConfigFile());
  EXPECT_EQ(manager.getBDT("cached"), loaded);
  const float features[] = {0, 2, 3};
  EXPECT_TRUE(std::isfinite((*manager.getBDT("cached"))(features)));
}

TEST_F(BDTManagerTest, ApplyAllPreservesFeatureOrderGatingAndExplicitVariations) {
  char pattern[] = "/tmp/rdf_bdt_shared_XXXXXX";
  const auto created = mkdtemp(pattern);
  ASSERT_NE(created, nullptr);
  struct Directory { std::filesystem::path path; ~Directory() { std::filesystem::remove_all(path); } } directory{created};
  const auto models = directory.path / "models.txt", settings = directory.path / "config.txt";
  { std::ofstream file(models);
    for (const auto &name : {"a", "b", "c"})
      file << "file=" << std::filesystem::absolute("aux/test_bdt.txt").string() << " name=" << name
           << " inputVariables=" << (std::string(name) == "c" ? "feature3,feature2,feature1" : "feature1,feature2,feature3")
           << " runVar=" << (std::string(name) == "b" ? "gate_b" : "gate_a") << '\n'; }
  for (const bool combined : {false, true}) for (const bool variedGate : {false, true}) {
    DataManager data(4);
    SystematicManager systematics;
    { std::ofstream file(settings); file << "bdtConfig=" << models.string() << '\n'
        << "bdtCombineModels=" << (combined ? "true" : "false") << '\n'; }
    ConfigurationManager config(settings.string());
    BDTManager manager(config);
    auto node = data.getDataFrame()
        .Define("feature1", [](ULong64_t i) { return float(i); }, {"rdfentry_"})
        .Define("feature2", [] { return 2.f; }).Define("feature3", [] { return 3.f; })
        .Define("feature1_JESUp", [] { return 1.f; }).Define("feature1_JESDown", [] { return 0.f; })
        .Define("feature1_custom", [] { return 1.f; })
        .Define("gate_a", [](ULong64_t i) { return i == 0 || i == 2; }, {"rdfentry_"})
        .Define("gate_b", [](ULong64_t i) { return i == 1 || i == 2; }, {"rdfentry_"});
    if (variedGate) node = node.Define("gate_a_shifted", [] { return false; });
    data.setDataFrame(node);
    systematics.registerSystematic("JES", {"feature1"});
    systematics.registerVariationColumn("feature1", "custom", "feature1_custom");
    if (variedGate) systematics.registerVariationColumn("gate_a", "gateVariation", "gate_a_shifted");
    ManagerContext context{config, data, systematics, *logger, *skimSink, *metaSink};
    manager.setContext(context);
    manager.applyAllBDTs();
    node = data.getDataFrame();
    int packedDefines = 0;
    for (const auto &name : node.GetDefinedColumnNames())
      if (name.rfind("input_", 0) == 0) ++packedDefines;
    EXPECT_EQ(packedDefines, 12); // three models, each nominal + three variations
    auto a = node.Take<float>("a"), b = node.Take<float>("b"), c = node.Take<float>("c");
    auto up = node.Take<float>(systematics.getVariationColumnName("a", "JESUp"));
    auto down = node.Take<float>(systematics.getVariationColumnName("a", "JESDown"));
    auto custom = node.Take<float>(systematics.getVariationColumnName("a", "custom"));
    const auto sigmoid = [](float x) { return 1.f/(1.f+std::exp(-x)); };
    EXPECT_NEAR(a->at(0), sigmoid(.6f), 1e-6);
    EXPECT_EQ(a->at(1), -1.f);
    EXPECT_EQ(b->at(0), -1.f);
    EXPECT_NEAR(a->at(2), sigmoid(1.4f), 1e-6);
    EXPECT_NEAR(b->at(2), sigmoid(1.4f), 1e-6);
    EXPECT_EQ(a->at(3), -1.f);
    EXPECT_EQ(b->at(3), -1.f);
    EXPECT_NEAR(b->at(1), sigmoid(1.4f), 1e-6);
    // The text model maps named features to the configured order. Its prediction
    // remains the same, while the exposed packed column must retain that order.
    EXPECT_NEAR(c->at(0), sigmoid(.6f), 1e-6);
    auto ordered = node.Take<ROOT::VecOps::RVec<float>>("input_c");
    ASSERT_EQ(ordered->at(0).size(), 3);
    EXPECT_FLOAT_EQ(ordered->at(0)[0], 3.f);
    EXPECT_FLOAT_EQ(ordered->at(0)[1], 2.f);
    EXPECT_FLOAT_EQ(ordered->at(0)[2], 0.f);
    EXPECT_NEAR(up->at(0), sigmoid(1.4f), 1e-6);
    EXPECT_NEAR(down->at(0), sigmoid(.6f), 1e-6);
    EXPECT_EQ(custom->at(0), up->at(0));
    if (variedGate) {
      EXPECT_EQ(systematics.getVariationColumnName("b", "gateVariation"), "b");
      auto shifted = node.Take<float>(systematics.getVariationColumnName("a", "gateVariation"));
      for (const auto score : *shifted) EXPECT_EQ(score, -1.f);
    }
  }
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
  EXPECT_NEAR(result->at(1), sigmoid(0.9f + 0.5f), 1e-6);
}

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

// ---------------------------------------------------------------------------
// Lifecycle hook tests
// ---------------------------------------------------------------------------

TEST_F(BDTManagerTest, InitializeIsCallable) {
  EXPECT_NO_THROW(bdtManager->initialize());
}

TEST_F(BDTManagerTest, ReportMetadataIsCallable) {
  EXPECT_NO_THROW(bdtManager->reportMetadata());
}

TEST_F(BDTManagerTest, ExecuteAndFinalizeAreNoOps) {
  EXPECT_NO_THROW(bdtManager->execute());
  EXPECT_NO_THROW(bdtManager->finalize());
}

TEST_F(BDTManagerTest, GetDependenciesReturnsEmpty) {
  EXPECT_TRUE(bdtManager->getDependencies().empty());
}

TEST_F(BDTManagerTest, LifecycleOrderViaSetupThenInitialize) {
  // Simulate the Analyzer lifecycle: setupFromConfigFile → initialize
  // The BDT count should be consistent after both calls.
  bdtManager->setupFromConfigFile();
  EXPECT_NO_THROW(bdtManager->initialize());
  EXPECT_FALSE(bdtManager->getAllBDTNames().empty());
}

TEST_F(BDTManagerTest, FlatRowsCollectionInferencePreservesOrderGatesAndVariations) {
  // Earlier tests can leave IMT enabled; Take ordering is unspecified under IMT.
  ROOT::DisableImplicitMT();
  char pattern[] = "/tmp/rdf_bdt_rows_XXXXXX";
  const auto created = mkdtemp(pattern);
  ASSERT_NE(created, nullptr);
  const std::filesystem::path directory(created);
  const auto models = directory/"models.txt", configPath = directory/"config.txt";
  { std::ofstream out(models); out << "file=" << std::filesystem::absolute("aux/test_bdt.txt").string()
      << " name=rows inputVariables=feature1,feature2,feature3 runVar=gate\n"; }
  { std::ofstream out(configPath); out << "bdtConfig=" << models.string()
      << "\nbdtInputLayout=flatRows\nbdtCombineModels=true\n"; }
  ConfigurationManager config(configPath.string());
  BDTManager manager(config);
  DataManager data(4);
  SystematicManager systematics;
  auto node = data.getDataFrame()
      .Define("input_rows", [](ULong64_t i) {
        return i == 2 ? ROOT::VecOps::RVec<float>{} : ROOT::VecOps::RVec<float>{0,2,3,1,2,3};
      }, {"rdfentry_"})
      .Define("shift_rows", [](const ROOT::VecOps::RVec<float>& values) {
        auto result = values; if (!result.empty()) result[0] = 1; return result;
      }, {"input_rows"})
      .Define("gate", [](ULong64_t i) { return i != 1; }, {"rdfentry_"})
      .Define("shift_gate", [] { return true; });
  data.setDataFrame(node);
  systematics.registerVariationColumn("input_rows", "objectShift", "shift_rows");
  systematics.registerVariationColumn("gate", "gateShift", "shift_gate");
  ManagerContext context{config, data, systematics, *logger, *skimSink, *metaSink};
  manager.setContext(context);
  manager.applyAllBDTs();
  node = data.getDataFrame();
  auto nominal = node.Take<ROOT::VecOps::RVec<float>>("rows");
  auto shifted = node.Take<ROOT::VecOps::RVec<float>>(systematics.getVariationColumnName("rows", "objectShift"));
  auto gated = node.Take<ROOT::VecOps::RVec<float>>(systematics.getVariationColumnName("rows", "gateShift"));
  const auto model = manager.getBDT("rows");
  const float first[]{0,2,3}, second[]{1,2,3};
  const auto sigmoid = [&](const float* values) {return 1./(1.+std::exp(-(*model)(values)));};
  ASSERT_EQ(nominal->at(0).size(), 2);
  EXPECT_NEAR(nominal->at(0)[0], sigmoid(first), 1e-7);
  EXPECT_NEAR(nominal->at(0)[1], sigmoid(second), 1e-7);
  EXPECT_EQ(nominal->at(1)[0], -1.f);
  EXPECT_EQ(nominal->at(1)[1], -1.f);
  EXPECT_TRUE(nominal->at(2).empty());
  EXPECT_NEAR(shifted->at(0)[0], sigmoid(second), 1e-7);
  EXPECT_NEAR(gated->at(1)[0], sigmoid(first), 1e-7);
  std::filesystem::remove_all(directory);
}

TEST_F(BDTManagerTest, FlatRowsRejectsMalformedDimensionsAndMissingPack) {
  char pattern[] = "/tmp/rdf_bdt_badrows_XXXXXX";
  const auto created = mkdtemp(pattern);
  ASSERT_NE(created, nullptr);
  const std::filesystem::path directory(created);
  const auto models = directory/"models.txt", configPath = directory/"config.txt";
  { std::ofstream out(models); out << "file=" << std::filesystem::absolute("aux/test_bdt.txt").string()
      << " name=rows inputVariables=feature1,feature2,feature3 runVar=gate\n"; }
  { std::ofstream out(configPath); out << "bdtConfig=" << models.string() << "\nbdtInputLayout=flatRows\n"; }
  ConfigurationManager config(configPath.string());
  BDTManager manager(config);
  DataManager data(1);
  SystematicManager systematics;
  auto node = data.getDataFrame().Define("gate", [] {return false;});
  data.setDataFrame(node);
  ManagerContext context{config, data, systematics, *logger, *skimSink, *metaSink};
  manager.setContext(context);
  EXPECT_THROW(manager.applyAllBDTs(), std::runtime_error);
  node = node.Define("input_rows", [] {return ROOT::VecOps::RVec<float>{1,2};});
  data.setDataFrame(node);
  manager.applyAllBDTs();
  auto values = data.getDataFrame().Take<ROOT::VecOps::RVec<float>>("rows");
  EXPECT_THROW(values.GetValue(), std::runtime_error);
  std::filesystem::remove_all(directory);
}

TEST_F(BDTManagerTest, SharedFlatRowsUseOneInputAndPropagateFeaturesWithIndependentGates) {
  ROOT::DisableImplicitMT();
  char pattern[]="/tmp/rdf_bdt_shared_XXXXXX";
  const auto created=mkdtemp(pattern);ASSERT_NE(created,nullptr);
  const std::filesystem::path directory(created);
  const auto models=directory/"models.txt",configuration=directory/"config.txt";
  {std::ofstream out(models);for(const auto name:{"a","b"})out<<"file="<<std::filesystem::absolute("aux/test_bdt.txt").string()
    <<" name="<<name<<" inputVariables=feature1,feature2,feature3 runVar=gate_"<<name<<"\n";}
  {std::ofstream out(configuration);out<<"bdtConfig="<<models.string()<<"\nbdtInputLayout=flatRows\nbdtFlatRowsInputColumn=shared_rows\n";}
  ConfigurationManager config(configuration.string());BDTManager manager(config);
  DataManager data(4);SystematicManager systematics;
  auto calls=std::make_shared<std::atomic<unsigned>>(0);
  auto node=data.getDataFrame()
    .Define("shared_rows",[calls]{calls->fetch_add(1);return ROOT::VecOps::RVec<float>{0,2,3,1,2,3};})
    .Define("shifted_rows",[]{return ROOT::VecOps::RVec<float>{1,2,3,0,2,3};})
    .Define("gate_a",[](ULong64_t i){return i%2==0;},{"rdfentry_"})
    .Define("gate_b",[](ULong64_t i){return i%2!=0;},{"rdfentry_"});
  data.setDataFrame(node);systematics.registerVariationColumn("shared_rows","shape","shifted_rows");
  ManagerContext context{config,data,systematics,*logger,*skimSink,*metaSink};manager.setContext(context);
  manager.applyAllBDTs();node=data.getDataFrame();
  EXPECT_FALSE(node.HasColumn("input_a"));EXPECT_FALSE(node.HasColumn("input_b"));
  auto a=node.Take<ROOT::VecOps::RVec<float>>("a");auto b=node.Take<ROOT::VecOps::RVec<float>>("b");
  auto up=node.Take<ROOT::VecOps::RVec<float>>(systematics.getVariationColumnName("a","shape"));
  EXPECT_EQ(a->at(1)[0],-1.f);EXPECT_EQ(b->at(0)[0],-1.f);
  EXPECT_FLOAT_EQ(a->at(0)[0],b->at(1)[0]);
  EXPECT_FLOAT_EQ(up->at(0)[0],a->at(0)[1]);
  EXPECT_FLOAT_EQ(up->at(0)[1],a->at(0)[0]);
  EXPECT_EQ(calls->load(),4u);
  std::filesystem::remove_all(directory);
}

TEST_F(BDTManagerTest, SharedFlatRowsRejectDifferentFeatureOrdering) {
  char pattern[]="/tmp/rdf_bdt_shared_order_XXXXXX";
  const auto created=mkdtemp(pattern);ASSERT_NE(created,nullptr);
  const std::filesystem::path directory(created);
  const auto models=directory/"models.txt",configuration=directory/"config.txt";
  {std::ofstream out(models);const auto file=std::filesystem::absolute("aux/test_bdt.txt").string();
    out<<"file="<<file<<" name=a inputVariables=feature1,feature2,feature3 runVar=gate\n";
    out<<"file="<<file<<" name=b inputVariables=feature2,feature1,feature3 runVar=gate\n";}
  {std::ofstream out(configuration);out<<"bdtConfig="<<models.string()<<"\nbdtInputLayout=flatRows\nbdtFlatRowsInputColumn=shared\n";}
  ConfigurationManager config(configuration.string());BDTManager manager(config);
  DataManager data(1);SystematicManager systematics;
  ManagerContext context{config,data,systematics,*logger,*skimSink,*metaSink};manager.setContext(context);
  EXPECT_THROW(manager.applyAllBDTs(),std::invalid_argument);
  std::filesystem::remove_all(directory);
}
