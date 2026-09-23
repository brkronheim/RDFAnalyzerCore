#include <analyzer.h>
#include <DataManager.h>
#include <SystematicManager.h>
#include <test_util.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <ROOT/RDataFrame.hxx>
#include <algorithm>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <NDHistogramManager.h>
#include <TriggerManager.h>
#include <BDTManager.h>
#include <unordered_map>
#include <api/IPluggableManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <THnSparse.h>
#include <map>
/**
 * @file testSystematicPropagation.cc
 * @brief Full stack integration tests for systematic propagation using Analyzer, DataManager, and SystematicManager.
 *
 * This test verifies that systematics registered in SystematicManager are correctly propagated through DataManager and Analyzer,
 * and that defining and filtering variables with systematics results in the expected systematic variation columns in the output.
 */

class SystematicPropagationTest : public ::testing::Test {
protected:
  void SetUp() override {
    std::cout << "Setting up SystematicPropagationTest" << std::endl;
    ChangeToTestSourceDir();
    // Set up SystematicManager and register a systematic for "energy"
    auto sysMgr = std::make_unique<SystematicManager>();
    sysMgr->registerSystematic("testSyst", {"energy"});

    // Set up DataManager with 5 in-memory entries
    auto dataMgr = std::make_unique<DataManager>(5);

    // Set up dummy ConfigurationManager
    auto configMgr = std::make_unique<ConfigurationManager>("cfg/test_data_config_minimal.txt");

    // Set up dummy CorrectionManager
    auto corrMgr = std::make_shared<CorrectionManager>(*configMgr);

    // Set up dummy NDHistogramManager
    //auto ndHistMgr = std::make_shared<NDHistogramManager>(*configMgr);
    
    // Set up dummy TriggerManager
    auto trigMgr = std::make_shared<TriggerManager>(*configMgr);
    
    // Set up dummy BDTManager
    auto bdtMgr = std::make_shared<BDTManager>(*configMgr);
    
    std::unordered_map<std::string, std::shared_ptr<IPluggableManager>> plugins;
    plugins["bdt"] = bdtMgr;
    plugins["corr"] = corrMgr;
    plugins["trig"] = trigMgr;
    //plugins["ndhist"] = ndHistMgr;

    // Transfer ownership to Analyzer
    auto logger = std::make_unique<DefaultLogger>();
    auto skimSink = std::make_unique<NullOutputSink>();
    auto metaSink = std::make_unique<NullOutputSink>();

    analyzer = std::make_unique<Analyzer>(
      std::move(configMgr),
      std::move(dataMgr),
      std::move(plugins),
      std::move(sysMgr),
      std::move(logger),
      std::move(skimSink),
      std::move(metaSink)
    );

    std::cout << "Analyzer set up" << std::endl;
    // After this, do not use dataMgr or sysMgr
  }

  void TearDown() override {
    // Unique pointers clean up
  }

  std::unique_ptr<Analyzer> analyzer;
};

/**
 * @brief Test that defining a variable with systematics results in systematic variation columns.
 */
TEST_F(SystematicPropagationTest, DefineWithSystematicsPropagates) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  // Use makeSystList to define systematic index variables for 'energy'
  sysManager->makeSystList("energy", *dataManager);

  auto df = dataManager->getDataFrame();
  auto columns = df.GetColumnNames();
  // These are index variables for systematic bins, not physics variables
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystUp"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystDown"), columns.end());
}

/**
 * @brief Test that filtering on a variable with systematics does not remove systematic columns.
 */
TEST_F(SystematicPropagationTest, FilterWithSystematicsKeepsSystematicColumns) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  sysManager->makeSystList("energy", *dataManager);
  // Apply a filter on the systematic index variable (e.g., energy > -1)
  analyzer->Filter("energy_valid", [](float x) { return x > -1.0f; }, {"energy"});
  auto df = dataManager->getDataFrame();
  auto columns = df.GetColumnNames();
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystUp"), columns.end());
  EXPECT_NE(std::find(columns.begin(), columns.end(), "energy_testSystDown"), columns.end());
}

TEST_F(SystematicPropagationTest, FilterAutoRegistersConventionalInputVariations) {
  analyzer->Define("auto_selection_input", []() { return false; }, {});
  analyzer->Define("auto_selection_input_migrationUp", []() { return true; }, {});
  analyzer->Define("auto_selection_input_migrationDown", []() { return false; }, {});

  analyzer->Filter("auto_selection", [](bool pass) { return pass; },
                   {"auto_selection_input"});

  const auto &systematics = analyzer->getSystematicManager();
  EXPECT_NE(systematics.getSystematics().find("migration"),
            systematics.getSystematics().end());
  auto df = analyzer->getDataFrameProvider().getDataFrame();
  auto retainedEntries = df.Take<ULong64_t>("rdfentry_");
  auto upMask = df.Take<bool>(
      systematics.getSelectionMaskColumn("migrationUp"));

  analyzer->run();

  EXPECT_EQ(retainedEntries.GetValue(),
            (std::vector<ULong64_t>{0, 1, 2, 3, 4}));
  ASSERT_EQ(upMask->size(), 5u);
  EXPECT_TRUE(std::all_of(upMask->begin(), upMask->end(),
                          [](bool passed) { return passed; }));
}

TEST_F(SystematicPropagationTest, FilterDefinesShiftedPredicateColumns) {
  auto &systematics = analyzer->getSystematicManager();
  analyzer->Define("selection_input", []() { return 15.0f; }, {});
  analyzer->Define("selection_input_up", []() { return 20.0f; }, {});
  analyzer->Define("selection_input_down", []() { return 5.0f; }, {});
  systematics.registerVariationColumns("selection_input", "testSyst",
                                       "selection_input_up",
                                       "selection_input_down");

  analyzer->Filter("selection", [](float value) { return value > 10.0f; },
                   {"selection_input"});

  auto df = analyzer->getDataFrameProvider().getDataFrame();
  auto nominal = df.Take<bool>("pass_selection");
  auto up = df.Take<bool>("pass_selection_testSystUp");
  auto down = df.Take<bool>("pass_selection_testSystDown");
  ASSERT_EQ(nominal->size(), 5u);
  ASSERT_EQ(up->size(), 5u);
  ASSERT_EQ(down->size(), 5u);
  EXPECT_TRUE(nominal->front());
  EXPECT_TRUE(up->front());
  EXPECT_FALSE(down->front());
}

TEST_F(SystematicPropagationTest,
       SequentialFiltersRetainUnionAndMaskHistogramPerVariation) {
  auto &systematics = analyzer->getSystematicManager();

  // The per-filter pass decisions deliberately migrate between nominal, Up,
  // and Down.  The second filter also has events that pass a variation which
  // failed the first filter, so a filter-by-filter OR would incorrectly count
  // those events in that variation's histogram.
  analyzer->Define(
      "first_input",
      [](ULong64_t entry) { return entry == 1 || entry == 3; }, {"rdfentry_"});
  analyzer->Define(
      "first_input_up",
      [](ULong64_t entry) { return entry == 0 || entry == 2; }, {"rdfentry_"});
  analyzer->Define("first_input_down",
                   [](ULong64_t entry) { return entry == 1; }, {"rdfentry_"});
  systematics.registerVariationColumns("first_input", "testSyst",
                                       "first_input_up", "first_input_down");
  analyzer->Filter("first", [](bool pass) { return pass; }, {"first_input"});

  analyzer->Define("second_input",
                   [](ULong64_t entry) { return entry == 0 || entry == 3; },
                   {"rdfentry_"});
  analyzer->Define("second_input_up",
                   [](ULong64_t entry) { return entry == 0 || entry == 1 || entry == 3; },
                   {"rdfentry_"});
  analyzer->Define("second_input_down",
                   [](ULong64_t entry) { return entry == 1; }, {"rdfentry_"});
  systematics.registerVariationColumns("second_input", "testSyst",
                                       "second_input_up", "second_input_down");
  analyzer->Filter("second", [](bool pass) { return pass; }, {"second_input"});

  // Expected cumulative pass masks after both filters:
  // entry 0: nominal=0, Up=1, Down=0
  // entry 1: nominal=0, Up=0, Down=1
  // entry 2: nominal=0, Up=0, Down=0 (must be removed)
  // entry 3: nominal=1, Up=0, Down=0
  // entry 4: all variations fail the first filter and must be removed.
  analyzer->Define("event_value",
                   [](ULong64_t entry) {
                     return static_cast<Float_t>(entry) + 0.5f;
                   },
                   {"rdfentry_"});
  analyzer->Define("event_weight", []() { return 1.0f; }, {});

  auto histogramManager = NDHistogramManager::create(*analyzer);
  histInfo info("filter_migration", "event_value", "event value",
                "event_weight", 5, 0.0f, 5.0f);
  histogramManager->BookSingleHistogram(info);

  const auto systematicLabels =
      systematics.makeSystList("SystematicCounter", analyzer->getDataFrameProvider());
  ASSERT_EQ(systematicLabels,
            (std::vector<std::string>{"Nominal", "testSystUp", "testSystDown"}));

  // Book the row-count action on the histogram's final dataframe node so both
  // assertions are materialized by the same lazy event loop.
  auto retainedEntries = analyzer->getDataFrameProvider()
                             .getDataFrame()
                             .Take<ULong64_t>("rdfentry_");
  analyzer->run();

  const auto entries = retainedEntries.GetValue();
  EXPECT_EQ(entries, (std::vector<ULong64_t>{0, 1, 3}));

  ASSERT_EQ(histogramManager->GetHistos().size(), 1u);
  auto *histogram = histogramManager->GetHistos().front().GetPtr();
  ASSERT_NE(histogram, nullptr);

  // Sparse bin indices on axis 3 identify the systematic label; axis 4 is the
  // event_value bin.  These expected cells prove both the accumulated masks
  // and the per-variation weight masking seen by the histogram filler.
  std::map<std::pair<std::string, Int_t>, Float_t> contents;
  std::vector<Int_t> indices(histogram->GetNdimensions());
  for (Long64_t bin = 0; bin < histogram->GetNbins(); ++bin) {
    const Float_t content = histogram->GetBinContent(bin, indices.data());
    if (content == 0.0f) {
      continue;
    }
    const Int_t systematicIndex = indices[3] - 1;
    ASSERT_GE(systematicIndex, 0);
    ASSERT_LT(systematicIndex, static_cast<Int_t>(systematicLabels.size()));
    contents[{systematicLabels[systematicIndex], indices[4]}] += content;
  }

  EXPECT_FLOAT_EQ(contents[std::make_pair(std::string("Nominal"), 4)],
                  1.0f); // entry 3
  EXPECT_FLOAT_EQ(contents[std::make_pair(std::string("testSystUp"), 1)],
                  1.0f); // entry 0
  EXPECT_FLOAT_EQ(contents[std::make_pair(std::string("testSystDown"), 2)],
                  1.0f); // entry 1
  EXPECT_EQ(contents.size(), 3u);
}

TEST_F(SystematicPropagationTest,
       FilterPropagatesExplicitVariationLabels) {
  auto &systematics = analyzer->getSystematicManager();
  analyzer->Define("replica_input", [](ULong64_t) { return false; },
                   {"rdfentry_"});
  analyzer->Define("replica_input_replica_1",
                   [](ULong64_t entry) { return entry == 1; }, {"rdfentry_"});
  systematics.registerVariationColumn("replica_input", "replica_1",
                                      "replica_input_replica_1");

  analyzer->Filter("replica", [](bool pass) { return pass; },
                   {"replica_input"});

  auto df = analyzer->getDataFrameProvider().getDataFrame();
  auto retainedEntries = df.Take<ULong64_t>("rdfentry_");
  const auto labels = systematics.getVariationLabels();
  ASSERT_EQ(labels, (std::vector<std::string>{
                        "Nominal", "testSystUp", "testSystDown", "replica_1"}));
  auto nominalMask = df.Take<bool>(
      systematics.getSelectionMaskColumn("Nominal"));
  auto pairedMask = df.Take<bool>(
      systematics.getSelectionMaskColumn("testSystUp"));
  auto replicaMask = df.Take<bool>(
      systematics.getSelectionMaskColumn("replica_1"));

  analyzer->run();

  EXPECT_EQ(retainedEntries.GetValue(), (std::vector<ULong64_t>{1}));
  ASSERT_EQ(nominalMask->size(), 1u);
  ASSERT_EQ(pairedMask->size(), 1u);
  ASSERT_EQ(replicaMask->size(), 1u);
  EXPECT_FALSE(nominalMask->front());
  EXPECT_FALSE(pairedMask->front());
  EXPECT_TRUE(replicaMask->front());
}

TEST_F(SystematicPropagationTest,
       FilterMaskSkipsFailedVariationInBoostHistogramBackend) {
  analyzer->getConfigurationProvider().set("histogramBackend", "boost");
  auto &systematics = analyzer->getSystematicManager();
  analyzer->Define("boost_selection_input", []() { return false; }, {});
  analyzer->Define("boost_selection_input_up", []() { return true; }, {});
  analyzer->Define("boost_selection_input_down", []() { return false; }, {});
  systematics.registerVariationColumns("boost_selection_input", "testSyst",
                                       "boost_selection_input_up",
                                       "boost_selection_input_down");
  analyzer->Filter("boost_selection", [](bool pass) { return pass; },
                   {"boost_selection_input"});

  analyzer->Define("boost_hist_value", []() { return 5.5f; }, {});
  analyzer->Define("boost_hist_weight", []() { return 1.0f; }, {});
  auto histogramManager = NDHistogramManager::create(*analyzer);
  histInfo info("boost_filter_mask", "boost_hist_value", "value",
                "boost_hist_weight", 10, 0.0f, 10.0f);
  histogramManager->BookSingleHistogram(info);
  const auto labels =
      systematics.makeSystList("SystematicCounter", analyzer->getDataFrameProvider());

  analyzer->run();

  ASSERT_EQ(histogramManager->GetHistos().size(), 1u);
  auto *histogram = histogramManager->GetHistos().front().GetPtr();
  ASSERT_NE(histogram, nullptr);
  std::vector<Float_t> contents(labels.size(), 0.0f);
  std::vector<Int_t> indices(histogram->GetNdimensions());
  for (Long64_t bin = 0; bin < histogram->GetNbins(); ++bin) {
    const Float_t content = histogram->GetBinContent(bin, indices.data());
    if (content == 0.0f) {
      continue;
    }
    const Int_t systematicIndex = indices[3] - 1;
    ASSERT_GE(systematicIndex, 0);
    ASSERT_LT(systematicIndex, static_cast<Int_t>(labels.size()));
    contents[systematicIndex] += content;
  }

  ASSERT_EQ(labels,
            (std::vector<std::string>{"Nominal", "testSystUp", "testSystDown"}));
  EXPECT_FLOAT_EQ(contents[0], 0.0f);
  EXPECT_FLOAT_EQ(contents[1], 5.0f);
  EXPECT_FLOAT_EQ(contents[2], 0.0f);
}

/**
 * @brief Test that the values in the systematic columns are as expected.
 */
TEST_F(SystematicPropagationTest, SystematicColumnValuesAreCorrect) {
  IDataFrameProvider* idata = &analyzer->getDataFrameProvider();
  auto* dataManager = dynamic_cast<DataManager*>(idata);
  ASSERT_NE(dataManager, nullptr);
  ISystematicManager* sysManager = &analyzer->getSystematicManager();
  ASSERT_NE(sysManager, nullptr);
  sysManager->makeSystList("Systematic", *dataManager);
  auto df = dataManager->getDataFrame();
  auto up = df.Take<float>("Systematic_testSystUp");
  auto down = df.Take<float>("Systematic_testSystDown");
  ASSERT_EQ(up->size(), 5);
  ASSERT_EQ(down->size(), 5);
  for (size_t i = 0; i < 5; ++i) {
    EXPECT_EQ((*up)[i], 1);
    EXPECT_EQ((*down)[i], 2);
  }
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
