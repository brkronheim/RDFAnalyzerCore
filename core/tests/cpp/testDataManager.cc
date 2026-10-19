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
#include <test_util.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <ROOT/RDataFrame.hxx>
#include <TChain.h>
#include <TEnv.h>
#include <TTreeCache.h>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>
#include <SystematicManager.h>
#include <util.h>

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

TEST_F(DataManagerTest, ConfiguredInputWithoutEntriesFailsClearly) {
  const auto configPath = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-no-input-" + std::to_string(getpid()) + ".txt");
  const auto missingDirectory = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-missing-input-" + std::to_string(getpid()));
  {
    std::ofstream configFile(configPath);
    ASSERT_TRUE(configFile.good());
    configFile << "directory=" << missingDirectory.string() << "\n"
               << "threads=1\n"
               << "treeList=Events\n";
  }

  ConfigurationManager config(configPath.string());
  std::string errorMessage;
  try {
    DataManager manager(config);
  } catch (const std::runtime_error &error) {
    errorMessage = error.what();
  }
  std::filesystem::remove(configPath);

  EXPECT_NE(errorMessage.find("no input entries found"), std::string::npos);
  EXPECT_NE(errorMessage.find(missingDirectory.string()), std::string::npos);
  EXPECT_NE(errorMessage.find("Events"), std::string::npos);
}

TEST(DataManagerInputTest, ExplicitInMemoryConstructorCreatesRequestedEntries) {
  DataManager dataManager(3);
  EXPECT_EQ(dataManager.getDataFrame().Count().GetValue(), 3u);
}

TEST(DataManagerInputTest, InvalidThreadSettingFailsWithValueContext) {
  const auto configPath = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-invalid-threads-" + std::to_string(getpid()) + ".txt");
  const auto missingDirectory = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-missing-input-" + std::to_string(getpid()));

  for (const std::string &invalidValue : {"4oops", "nope"}) {
    {
      std::ofstream configFile(configPath);
      ASSERT_TRUE(configFile.good());
      configFile << "threads=" << invalidValue << "\n"
                 << "directory=" << missingDirectory.string() << "\n"
                 << "treeList=Events\n";
    }

    ConfigurationManager config(configPath.string());
    std::string errorMessage;
    try {
      DataManager manager(config);
    } catch (const std::runtime_error &error) {
      errorMessage = error.what();
    }
    std::filesystem::remove(configPath);

    EXPECT_NE(errorMessage.find("Invalid 'threads' value '" + invalidValue + "'"),
              std::string::npos)
        << "Malformed thread setting should fail before input discovery";
  }
}

TEST(DataManagerInputTest, RootInputCacheSettingsApplyBeforeOpeningFiles) {
  const auto configPath = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-cache-" + std::to_string(getpid()) + ".txt");
  const char *previousFactor = std::getenv("ROOT_TTREECACHE_SIZE");
  const char *previousPrefill = std::getenv("ROOT_TTREECACHE_PREFILL");
  const std::string oldFactor = previousFactor ? previousFactor : "";
  const std::string oldPrefill = previousPrefill ? previousPrefill : "";
  const int oldLearnEntries = TTreeCache::GetLearnEntries();
  const int oldAsync = gEnv->GetValue("TFile.AsyncReading", 0);

  auto writeConfig = [&](const std::string &settings) {
    std::ofstream file(configPath);
    file << "threads=1\n"
         << "treeList=Events\n"
         << "directory=" << configPath.parent_path().string()
         << "/rdfanalyzer-nonexistent-input\n" << settings;
  };

  writeConfig("treeCacheSizeMB=64\n"
              "treeCacheLearnEntries=7\n"
              "treeCachePrefill=0\n"
              "treeCacheAsyncPrefetch=1\n");
  ConfigurationManager fixedConfig(configPath.string());
  auto chains = makeTChain(fixedConfig);
  ASSERT_EQ(chains.size(), 1u);
  EXPECT_EQ(chains[0]->GetCacheSize(), 64LL * 1024 * 1024);
  EXPECT_EQ(TTreeCache::GetLearnEntries(), 7);
  EXPECT_STREQ(std::getenv("ROOT_TTREECACHE_PREFILL"), "0");
  EXPECT_EQ(gEnv->GetValue("TFile.AsyncReading", 0), 1);

  writeConfig("ROOT_TTREECACHE_SIZE=2.5\n");
  ConfigurationManager factorConfig(configPath.string());
  EXPECT_NO_THROW(makeTChain(factorConfig));
  EXPECT_STREQ(std::getenv("ROOT_TTREECACHE_SIZE"), "2.5");

  writeConfig("rootTTreeCacheSize=67108864\n");
  ConfigurationManager invalidConfig(configPath.string());
  EXPECT_THROW(makeTChain(invalidConfig), std::runtime_error);

  if (previousFactor) setenv("ROOT_TTREECACHE_SIZE", oldFactor.c_str(), 1);
  else unsetenv("ROOT_TTREECACHE_SIZE");
  if (previousPrefill) setenv("ROOT_TTREECACHE_PREFILL", oldPrefill.c_str(), 1);
  else unsetenv("ROOT_TTREECACHE_PREFILL");
  TTreeCache::SetLearnEntries(oldLearnEntries);
  gEnv->SetValue("TFile.AsyncReading", oldAsync);
  std::filesystem::remove(configPath);
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
  auto& chain = dynamic_cast<DataManager*>(dataManager.get())->getChain();
  EXPECT_TRUE(chain.GetEntries() >= 0);
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

TEST_F(DataManagerTest, DefineVectorRepeatedScalarsPreserveOrderAndEventValues) {
  DataManager local(3);
  local.Define("x", [](ULong64_t entry) { return double(entry) + 0.5; },
               {"rdfentry_"}, *systematicManager);
  local.Define("y", [](ULong64_t entry) { return int(entry) + 10; },
               {"rdfentry_"}, *systematicManager);
  local.DefineVectorNominal("repeated", {"x", "y", "x", "x", "y"},
                            "Float_t", *systematicManager);
  const auto values = local.getDataFrame()
                          .Take<ROOT::VecOps::RVec<Float_t>>("repeated").GetValue();
  ASSERT_EQ(values.size(), 3u);
  for (std::size_t i = 0; i < values.size(); ++i) {
    const ROOT::VecOps::RVec<Float_t> expected{
        float(i) + 0.5f, float(i) + 10.f, float(i) + 0.5f,
        float(i) + 0.5f, float(i) + 10.f};
    EXPECT_EQ(std::vector<Float_t>(values[i].begin(), values[i].end()),
              std::vector<Float_t>(expected.begin(), expected.end()));
  }
}

TEST_F(DataManagerTest, WidePackingCreatesOneColumnAndPreservesInputOrder) {
  DataManager local(3);
  std::vector<std::string> inputs;
  for (int i = 0; i < 96; ++i) {
    const auto name = "input_" + std::to_string(i);
    local.Define(name, [i](ULong64_t entry) { return i + .25f * entry; },
                 {"rdfentry_"}, *systematicManager);
    inputs.push_back(name);
  }
  // Exercise repeated dependencies and nontrivial requested ordering too.
  inputs.push_back(inputs[3]);
  std::reverse(inputs.begin(), inputs.end());
  const auto before = local.getDataFrame().GetColumnNames().size();
  local.DefineVectorNominal("wide", inputs, "float", *systematicManager);
  EXPECT_EQ(local.getDataFrame().GetColumnNames().size(), before + 1);
  const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<float>>("wide").GetValue();
  ASSERT_EQ(values.size(), 3u);
  for (std::size_t entry = 0; entry < values.size(); ++entry) {
    ASSERT_EQ(values[entry].size(), inputs.size());
    for (std::size_t i = 0; i < inputs.size(); ++i)
      EXPECT_FLOAT_EQ(values[entry][i], std::stoi(inputs[i].substr(6)) + .25f * entry);
  }
}

TEST_F(DataManagerTest, CachedPackingBindsNewColumnNamesAndPreservesNumericTypes) {
  for (const std::string prefix : {"first_", "second_"}) {
    DataManager local(2);
    local.Define(prefix + "x", [](ULong64_t entry) { return double(entry) + .125; },
                 {"rdfentry_"}, *systematicManager);
    local.Define(prefix + "i", []() { return 7; }, {}, *systematicManager);
    local.Define(prefix + "b", []() { return true; }, {}, *systematicManager);
    local.Define(prefix + "wide", []() { return ULong64_t{9007199254740993ULL}; }, {}, *systematicManager);
    local.DefineVectorNominal(prefix + "packed", {prefix + "x", prefix + "i", prefix + "b", prefix + "x"},
                              "double", *systematicManager);
    local.DefineVectorNominal(prefix + "integers", {prefix + "wide", prefix + "i"},
                              "ULong64_t", *systematicManager);
    const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<double>>(prefix + "packed").GetValue();
    ASSERT_EQ(values.size(), 2u);
    for (std::size_t entry = 0; entry < values.size(); ++entry) {
      const ROOT::VecOps::RVec<double> expected{entry + .125, 7, 1, entry + .125};
      EXPECT_EQ(std::vector<double>(values[entry].begin(), values[entry].end()),
                std::vector<double>(expected.begin(), expected.end()));
    }
    const auto integers = local.getDataFrame().Take<ROOT::VecOps::RVec<ULong64_t>>(prefix + "integers").GetValue();
    EXPECT_EQ(integers.front()[0], 9007199254740993ULL);
    EXPECT_EQ(integers.front()[1], 7u);
  }
}

TEST_F(DataManagerTest, VectorPackingConcatenatesRepeatedAndEmptyVectorsPerEvent) {
  DataManager local(3);
  local.Define("a", [](ULong64_t entry) { return ROOT::VecOps::RVec<int>(entry, int(entry)); },
               {"rdfentry_"}, *systematicManager);
  local.Define("b", []() { return ROOT::VecOps::RVec<double>{.5}; }, {}, *systematicManager);
  local.Define("empty", []() { return ROOT::VecOps::RVec<bool>{}; }, {}, *systematicManager);
  const auto before = local.getDataFrame().GetColumnNames().size();
  local.DefineVectorNominal("packed", {"a", "empty", "b", "a"}, "double", *systematicManager);
  EXPECT_EQ(local.getDataFrame().GetColumnNames().size(), before + 1);
  const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<double>>("packed").GetValue();
  for (std::size_t entry = 0; entry < values.size(); ++entry) {
    std::vector<double> expected(entry, double(entry));
    expected.push_back(.5);
    expected.insert(expected.end(), entry, double(entry));
    EXPECT_EQ(std::vector<double>(values[entry].begin(), values[entry].end()), expected);
  }
}

TEST_F(DataManagerTest, PackedBooleansAndRuntimeNumericTypesUseDirectCasts) {
  DataManager local(1);
  local.Define("extended", []() { return long{123456789}; }, {}, *systematicManager);
  local.Define("zero", []() { return 0; }, {}, *systematicManager);
  local.Define("negative", []() { return -2; }, {}, *systematicManager);
  local.DefineVectorNominal("numeric", {"extended", "negative"}, "double", *systematicManager);
  local.DefineVectorNominal("bits", {"zero", "negative", "zero"}, "Bool_t", *systematicManager);
  const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<double>>("numeric").GetValue();
  EXPECT_DOUBLE_EQ(values.front()[0], 123456789);
  EXPECT_DOUBLE_EQ(values.front()[1], -2);
  const auto bits = local.getDataFrame().Take<ROOT::VecOps::RVec<bool>>("bits").GetValue();
  EXPECT_FALSE(bits.front()[0]);
  EXPECT_TRUE(bits.front()[1]);
  EXPECT_FALSE(bits.front()[2]);
}

TEST_F(DataManagerTest, PackedVectorsEvaluateIndependentlyAcrossRdfSlots) {
  struct DisablePoolOnExit {
    ~DisablePoolOnExit() { ROOT::DisableImplicitMT(); }
  } resetPool;
  ROOT::EnableImplicitMT(2);
  {
    DataManager local(1000);
    local.Define("entry", [](ULong64_t entry) { return double(entry); },
                 {"rdfentry_"}, *systematicManager);
    local.Define("twice", [](double entry) { return entry * 2; },
                 {"entry"}, *systematicManager);
    local.DefineVectorNominal("packed", {"entry", "twice", "entry"}, "double", *systematicManager);
    const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<double>>("packed").GetValue();
    ASSERT_EQ(values.size(), 1000u);
    for (const auto &value : values) {
      ASSERT_EQ(value.size(), 3u);
      EXPECT_DOUBLE_EQ(value[1], 2 * value[0]);
      EXPECT_DOUBLE_EQ(value[2], value[0]);
    }
  }
}

TEST_F(DataManagerTest, UnknownInterpreterInputFailsWithoutBreakingLaterPacking) {
  DataManager local(1);
  struct UnregisteredNumber {
    operator double() const { return 3.125; }
  };
  local.Define("unknown", []() { return UnregisteredNumber{}; }, {}, *systematicManager);
  local.Define("known", []() { return 2; }, {}, *systematicManager);
  // This local C++ type has no interpreter declaration or ROOT dictionary.
  ASSERT_NE(local.getDataFrame().GetColumnType("unknown").find("CLING_UNKNOWN_TYPE_"), std::string::npos);
  EXPECT_THROW(local.DefineVectorNominal("bad", {"unknown", "known"}, "double", *systematicManager),
               std::runtime_error);
  local.DefineVectorNominal("good", {"known", "known"}, "int", *systematicManager);
  const auto values = local.getDataFrame().Take<ROOT::VecOps::RVec<int>>("good").GetValue();
  EXPECT_EQ(values.front().size(), 2u);
  EXPECT_EQ(values.front()[0], 2);
  EXPECT_EQ(values.front()[1], 2);
}

TEST_F(DataManagerTest, DefineVectorPropagatesPairedSystematicVariations) {
  DataManager localDataManager(5);
  localDataManager.Define("pt", []() { return 100.0f; }, {}, *systematicManager);
  localDataManager.Define("eta", []() { return 1.0f; }, {}, *systematicManager);
  localDataManager.Define("pt_shift_up", []() { return 120.0f; }, {},
                          *systematicManager);
  localDataManager.Define("pt_shift_down", []() { return 80.0f; }, {},
                          *systematicManager);
  localDataManager.Define("eta_scale_up", []() { return 2.0f; }, {},
                          *systematicManager);
  localDataManager.Define("eta_scale_down", []() { return 0.5f; }, {},
                          *systematicManager);

  systematicManager->registerVariationColumns("pt", "jes", "pt_shift_up",
                                               "pt_shift_down");
  systematicManager->registerVariationColumns(
      "leptons", "jes", "leptons_jes_custom_up",
      "leptons_jes_custom_down");
  systematicManager->registerVariationColumns("eta", "etaScale", "eta_scale_up",
                                               "eta_scale_down");

  const auto beforePacking = localDataManager.getDataFrame().GetColumnNames().size();
  localDataManager.DefineVector("leptons", {"pt", "eta"}, "Float_t",
                                *systematicManager);
  // Nominal plus the two paired sources, each as one direct Define.
  EXPECT_EQ(localDataManager.getDataFrame().GetColumnNames().size(), beforePacking + 5);
  localDataManager.Define(
      "lepton_pt_sum",
      [](const ROOT::VecOps::RVec<float> &values) { return values[0] + values[1]; },
      {"leptons"}, *systematicManager);

  auto df = localDataManager.getDataFrame();
  auto nominal = df.Take<ROOT::VecOps::RVec<float>>("leptons");
  auto jesUp = df.Take<ROOT::VecOps::RVec<float>>("leptons_jes_custom_up");
  auto jesDown = df.Take<ROOT::VecOps::RVec<float>>("leptons_jes_custom_down");
  auto etaUp = df.Take<ROOT::VecOps::RVec<float>>("leptons_etaScaleUp");
  auto etaDown = df.Take<ROOT::VecOps::RVec<float>>("leptons_etaScaleDown");

  ASSERT_EQ(nominal->size(), 5u);
  ASSERT_EQ(jesUp->size(), 5u);
  ASSERT_EQ(jesDown->size(), 5u);
  ASSERT_EQ(etaUp->size(), 5u);
  ASSERT_EQ(etaDown->size(), 5u);
  ASSERT_EQ(nominal->front().size(), 2u);
  EXPECT_FLOAT_EQ(nominal->front()[0], 100.0f);
  EXPECT_FLOAT_EQ(nominal->front()[1], 1.0f);
  EXPECT_FLOAT_EQ(jesUp->front()[0], 120.0f);
  EXPECT_FLOAT_EQ(jesUp->front()[1], 1.0f);
  EXPECT_FLOAT_EQ(jesDown->front()[0], 80.0f);
  EXPECT_FLOAT_EQ(jesDown->front()[1], 1.0f);
  EXPECT_FLOAT_EQ(etaUp->front()[0], 100.0f);
  EXPECT_FLOAT_EQ(etaUp->front()[1], 2.0f);
  EXPECT_FLOAT_EQ(etaDown->front()[0], 100.0f);
  EXPECT_FLOAT_EQ(etaDown->front()[1], 0.5f);
  EXPECT_TRUE(systematicManager->isVariableAffectedBySystematic("leptons", "jes"));
  EXPECT_TRUE(systematicManager->isVariableAffectedBySystematic("leptons", "etaScale"));
  EXPECT_EQ(systematicManager->getVariationColumnName("leptons", "jesUp"),
            "leptons_jes_custom_up");
  auto jesUpSum =
      localDataManager.getDataFrame().Take<float>("lepton_pt_sum_jesUp");
  ASSERT_EQ(jesUpSum->size(), 5u);
  EXPECT_FLOAT_EQ(jesUpSum->front(), 121.0f);
}

TEST_F(DataManagerTest, DefineVectorConcatenatesRVecColumnsAndCasts) {
  dataManager->Define(
      "vec1",
      []() -> ROOT::VecOps::RVec<int> { return {1, 2}; },
      {}, *systematicManager);
  dataManager->Define(
      "vec2",
      []() -> ROOT::VecOps::RVec<double> { return {3.5, 4.5}; },
      {}, *systematicManager);

  EXPECT_NO_THROW({
    dataManager->DefineVector("vec_concat", {"vec1", "vec2"}, "Float_t",
                              *systematicManager);
    auto df = dataManager->getDataFrame();
    auto result = df.Take<ROOT::VecOps::RVec<Float_t>>("vec_concat");
    ASSERT_EQ(result->size(), df.Count().GetValue());
    ASSERT_FALSE(result->empty());
    ASSERT_EQ((*result)[0].size(), 4u);
    EXPECT_FLOAT_EQ((*result)[0][0], 1.0f);
    EXPECT_FLOAT_EQ((*result)[0][1], 2.0f);
    EXPECT_FLOAT_EQ((*result)[0][2], 3.5f);
    EXPECT_FLOAT_EQ((*result)[0][3], 4.5f);
  });
}

TEST_F(DataManagerTest, DefineVectorRejectsMixedScalarAndRVecInputs) {
  dataManager->Define("scalar_col", []() { return 1.0f; }, {},
                      *systematicManager);
  dataManager->Define(
      "vector_col",
      []() -> ROOT::VecOps::RVec<float> { return {2.0f, 3.0f}; },
      {}, *systematicManager);

  EXPECT_THROW(
      { dataManager->DefineVector("mixed_vec", {"scalar_col", "vector_col"},
                                  "Float_t", *systematicManager); },
      std::runtime_error);
}

TEST_F(DataManagerTest, DefineVectorWithNoInputsCreatesEmptyVectorColumn) {
  EXPECT_NO_THROW({
    dataManager->DefineVector("empty_vec", {}, "float", *systematicManager);
    auto df = dataManager->getDataFrame();
    auto result = df.Take<ROOT::VecOps::RVec<float>>("empty_vec");
    ASSERT_EQ(result->size(), df.Count().GetValue());
    ASSERT_FALSE(result->empty());
    EXPECT_TRUE((*result)[0].empty());
  });
}

/**
 * @brief Test Filter_m applies a filter to the dataframe
 *
 * Verifies that Filter_m correctly filters rows based on a predicate.
 */
TEST_F(DataManagerTest, Filter_mAppliesFilter) {
  dataManager->Define("filt_col", []() { return 5; }, {}, *systematicManager);
  dynamic_cast<DataManager*>(dataManager.get())->Filter([](int x) { return x == 5; }, {"filt_col"});
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
  dynamic_cast<DataManager*>(dataManager.get())->DefinePerSample("sample_col", [](unsigned int, const ROOT::RDF::RSampleInfo&) { return 7; });
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
  auto systList = systematicManager->makeSystList("syst_branch", *dataManager);
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
 * @brief Test makeSystList is idempotent when called multiple times with the same branchName
 *
 * Verifies that multiple calls with the same branchName return the same list and
 * do not cause errors (columns are defined only once).
 */
TEST_F(DataManagerTest, MakeSystListIdempotentSameBranchName) {
  systematicManager->registerSystematic("jes", {"pt"});
  auto list1 = systematicManager->makeSystList(ISystematicManager::CANONICAL_SYST_BRANCH_NAME, *dataManager);
  // Second call with same branchName should be a no-op and return same list
  auto list2 = systematicManager->makeSystList(ISystematicManager::CANONICAL_SYST_BRANCH_NAME, *dataManager);
  EXPECT_EQ(list1, list2);
  EXPECT_TRUE(systematicManager->isBranchNameMaterialized(ISystematicManager::CANONICAL_SYST_BRANCH_NAME));
}

/**
 * @brief Test makeSystList supports multiple branch namespaces safely
 *
 * Verifies that two callers using different branchNames each get their own
 * counter columns without collisions or missing columns.
 */
TEST_F(DataManagerTest, MakeSystListMultipleBranchNamesSafe) {
  systematicManager->registerSystematic("jes", {"pt"});
  auto list1 = systematicManager->makeSystList("BranchA", *dataManager);
  auto list2 = systematicManager->makeSystList("BranchB", *dataManager);
  // Both namespaces return the same ordered variation list
  EXPECT_EQ(list1, list2);
  // Both namespaces should be materialized
  EXPECT_TRUE(systematicManager->isBranchNameMaterialized("BranchA"));
  EXPECT_TRUE(systematicManager->isBranchNameMaterialized("BranchB"));
  // An unrelated name should not be materialized
  EXPECT_FALSE(systematicManager->isBranchNameMaterialized("BranchC"));
  // All columns for both namespaces should exist in the dataframe
  auto df = dataManager->getDataFrame();
  auto colNames = df.GetColumnNames();
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchA"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchA_jesUp"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchA_jesDown"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchB"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchB_jesUp"), colNames.end());
  EXPECT_NE(std::find(colNames.begin(), colNames.end(), "BranchB_jesDown"), colNames.end());
}

/**
 * @brief Test that the canonical branch name constant is "SystematicCounter"
 *
 * Verifies that ISystematicManager::CANONICAL_SYST_BRANCH_NAME matches the
 * value used by NDHistogramManager and other callers.
 */
TEST(SystematicManagerCanonicalNameTest, CanonicalBranchNameIsSystematicCounter) {
  EXPECT_STREQ(ISystematicManager::CANONICAL_SYST_BRANCH_NAME, "SystematicCounter");
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

// ---------------------------------------------------------------------------
// Friend tree tests
// ---------------------------------------------------------------------------

/**
 * @brief Test parseFriendTreeConfig parses a valid YAML friend config
 */
TEST(FriendTreeConfigTest, ParseValidConfig) {
  ChangeToTestSourceDir();
  EXPECT_NO_THROW({
    auto specs = parseFriendTreeConfig("cfg/test_friend_config.yaml");
    ASSERT_EQ(specs.size(), 2u);

    // First entry: calib with explicit file list and index branches
    EXPECT_EQ(specs[0].alias, "calib");
    EXPECT_EQ(specs[0].treeName, "Events");
    ASSERT_EQ(specs[0].files.size(), 2u);
    EXPECT_EQ(specs[0].files[0], "/nonexistent/calib1.root");
    EXPECT_EQ(specs[0].files[1], "/nonexistent/calib2.root");
    ASSERT_EQ(specs[0].indexBranches.size(), 2u);
    EXPECT_EQ(specs[0].indexBranches[0], "run");
    EXPECT_EQ(specs[0].indexBranches[1], "luminosityBlock");
    EXPECT_TRUE(specs[0].directory.empty());

    // Second entry: taggers with directory scan settings
    EXPECT_EQ(specs[1].alias, "taggers");
    EXPECT_EQ(specs[1].treeName, "BTagging");
    EXPECT_TRUE(specs[1].files.empty());
    EXPECT_EQ(specs[1].directory, "/nonexistent/tagger_dir");
    ASSERT_EQ(specs[1].globs.size(), 1u);
    EXPECT_EQ(specs[1].globs[0], ".root");
    ASSERT_EQ(specs[1].antiglobs.size(), 1u);
    EXPECT_EQ(specs[1].antiglobs[0], "output.root");
    EXPECT_TRUE(specs[1].indexBranches.empty());
  });
}

/**
 * @brief Test parseFriendTreeConfig returns empty list for missing file
 */
TEST(FriendTreeConfigTest, MissingFileThrows) {
  EXPECT_THROW({
    parseFriendTreeConfig("/nonexistent/friend_config.yaml");
  }, std::runtime_error);
}

/**
 * @brief Test registerFriendTrees is a no-op when friendConfig key is absent
 */
TEST_F(DataManagerTest, RegisterFriendTreesNoOpWhenKeyAbsent) {
  EXPECT_NO_THROW({
    auto dm = dynamic_cast<DataManager*>(dataManager.get());
    dm->registerFriendTrees(*configManager, "nonExistentFriendConfig");
  });
}

/**
 * @brief Test registerFriendTrees is a no-op when the config file does not exist
 */
TEST_F(DataManagerTest, RegisterFriendTreesNoOpWhenFileMissing) {
  configManager->set("friendConfig", "/nonexistent/friends.yaml");
  EXPECT_NO_THROW({
    auto dm = dynamic_cast<DataManager*>(dataManager.get());
    dm->registerFriendTrees(*configManager);
  });
}

/**
 * @brief Test registerFriendTrees parses a real config without crashing
 *
 * The referenced friend files do not exist, so no entries are read, but the
 * parsing and attachment logic should complete without throwing.
 */
TEST_F(DataManagerTest, RegisterFriendTreesParsesConfigGracefully) {
  ChangeToTestSourceDir();
  configManager->set("friendConfig", "cfg/test_friend_config.yaml");
  EXPECT_NO_THROW({
    auto dm = dynamic_cast<DataManager*>(dataManager.get());
    dm->registerFriendTrees(*configManager);
  });
}

/**
 * @brief Test attachFriendTree with missing files logs a warning but does not throw
 */
TEST_F(DataManagerTest, AttachFriendTreeMissingFilesDoesNotThrow) {
  FriendTreeSpec spec;
  spec.alias = "test_friend";
  spec.treeName = "Events";
  spec.files = {"/nonexistent/file.root"};
  EXPECT_NO_THROW({
    auto dm = dynamic_cast<DataManager*>(dataManager.get());
    dm->attachFriendTree(spec);
  });
}

/**
 * @brief Test attachFriendTree with no files and no directory skips gracefully
 */
TEST_F(DataManagerTest, AttachFriendTreeNoSourceSkipsGracefully) {
  FriendTreeSpec spec;
  spec.alias = "empty_friend";
  spec.treeName = "Events";
  // No files, no directory
  EXPECT_NO_THROW({
    auto dm = dynamic_cast<DataManager*>(dataManager.get());
    dm->attachFriendTree(spec);
  });
}

TEST(DataManagerConfigTest, RelativeAuxiliaryPathsResolveFromMainConfigDirectory) {
  const auto tempRoot = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-relative-config-" + std::to_string(getpid()));
  const auto configDir = tempRoot / "cfg";
  const auto workingDir = tempRoot / "work";
  std::filesystem::create_directories(configDir);
  std::filesystem::create_directories(workingDir);

  {
    std::ofstream(configDir / "floats.txt") << "mass=125.0\n";
    std::ofstream(configDir / "ints.txt") << "count=7\n";
    std::ofstream(configDir / "aliases.txt")
        << "existingName=source newName=source_alias\n";
    std::ofstream(configDir / "optional.txt")
        << "name=optional_branch type=1 default=42\n";
    std::ofstream(tempRoot / "submit.txt")
        << "floatConfig=cfg/floats.txt\n"
        << "intConfig=cfg/ints.txt\n"
        << "aliasConfig=cfg/aliases.txt\n"
        << "optionalBranchesConfig=cfg/optional.txt\n";
  }

  const auto oldWorkingDir = std::filesystem::current_path();
  std::filesystem::current_path(workingDir);
  {
    ConfigurationManager config((tempRoot / "submit.txt").string());
    DataManager data(2);
    SystematicManager systematics;
    data.Define("source", []() { return 3.0; }, {}, systematics);
    data.finalizeSetup(config);

    const auto names = data.getDataFrame().GetColumnNames();
    EXPECT_NE(std::find(names.begin(), names.end(), "mass"), names.end());
    EXPECT_NE(std::find(names.begin(), names.end(), "count"), names.end());
    EXPECT_NE(std::find(names.begin(), names.end(), "source_alias"), names.end());
    EXPECT_NE(std::find(names.begin(), names.end(), "optional_branch"), names.end());
  }
  std::filesystem::current_path(oldWorkingDir);
  std::filesystem::remove_all(tempRoot);
}

TEST(DataManagerConfigTest, ConstantValuesRequireFullTokensAndCppRanges) {
  const auto tempRoot = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-invalid-constant-" + std::to_string(getpid()));
  const auto valueDirName = "invalid-values-" + std::to_string(getpid());
  std::filesystem::create_directories(tempRoot / valueDirName);

  const auto expectInvalid = [&](const std::string& key,
                                 const std::string& value,
                                 const std::string& expectedMessage) {
    const bool isFloat = key == "floatConfig";
    const auto valuePath = tempRoot / valueDirName /
        (isFloat ? "floats.txt" : "ints.txt");
    {
      std::ofstream values(valuePath);
      values << "bad=" << value << "\n";
    }
    const auto configPath = tempRoot / ("main-" + key + ".txt");
    {
      std::ofstream configFile(configPath);
      configFile << key << "=" << valueDirName << "/"
                 << valuePath.filename().string() << "\n";
    }
    ConfigurationManager config(configPath.string());
    DataManager data(1);
    std::string observed;
    try {
      data.registerConstants(config);
    } catch (const std::runtime_error& error) {
      observed = error.what();
    }
    EXPECT_NE(observed.find(expectedMessage), std::string::npos)
        << "input value was " << value << "; observed " << observed;
  };

  for (const auto& value : {"12junk", "3.5", "2147483648"}) {
    expectInvalid("intConfig", value, "invalid integer constant 'bad=");
  }
  for (const auto& value : {"12junk", "1e39", "nan", "inf"}) {
    expectInvalid("floatConfig", value, "invalid float constant 'bad=");
  }

  std::filesystem::remove_all(tempRoot);
}

TEST(DataManagerConfigTest, ThreadCountOneDisablesGlobalImplicitMTAfterPriorEnable) {
  const auto tempRoot = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-thread-config-" + std::to_string(getpid()));
  std::filesystem::create_directories(tempRoot);

  const auto constructWithThreads = [&](const std::string& value) {
    const auto configPath = tempRoot / ("threads-" + value + ".txt");
    {
      std::ofstream configFile(configPath);
      configFile << "threads=" << value << "\n"
                 << "directory=" << (tempRoot / "missing").string() << "\n";
    }
    ConfigurationManager config(configPath.string());
    EXPECT_THROW(DataManager data(config), std::runtime_error);
  };

  ROOT::DisableImplicitMT();
  constructWithThreads("2");
  EXPECT_TRUE(ROOT::IsImplicitMTEnabled());
  constructWithThreads("1");
  EXPECT_FALSE(ROOT::IsImplicitMTEnabled());
  std::filesystem::remove_all(tempRoot);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
