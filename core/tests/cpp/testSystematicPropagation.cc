#include <analyzer.h>
#include <CheckedDefine.h>
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
    auto configMgr = std::make_unique<ConfigurationManager>("cfg/test_data_config_minimal.yaml");

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

TEST_F(SystematicPropagationTest, DerivedChainPropagatesExplicitReplicas) {
  auto &systematics = analyzer->getSystematicManager();
  analyzer->DefineNominal("replica_pt", [](ULong64_t e) { return float(e + 24); },
                           {"rdfentry_"});
  analyzer->DefineNominal("replica_shift", [](ULong64_t e) { return float(e + 27); },
                           {"rdfentry_"});
  systematics.registerVariationColumn("replica_pt", "pdf_0", "replica_shift");
  analyzer->Define("scaled_pt", [](float pt) { return pt * 2; }, {"replica_pt"});
  analyzer->Define("observable", [](float pt) { return pt + 1; }, {"scaled_pt"});
  analyzer->Filter("kin", [](float value) { return value > 50; }, {"observable"});
  auto df = analyzer->getDataFrameProvider().getDataFrame();
  auto nominal = df.Take<float>("observable");
  auto varied = df.Take<float>("observable_pdf_0");
  auto mask = df.Take<bool>(systematics.getSelectionMaskColumn("Nominal"));
  analyzer->run();
  EXPECT_EQ(nominal.GetValue(), (std::vector<float>{49, 51, 53, 55, 57}));
  EXPECT_EQ(varied.GetValue(), (std::vector<float>{55, 57, 59, 61, 63}));
  EXPECT_EQ(mask.GetValue(), (std::vector<bool>{false, true, true, true, true}));
}

TEST(SelectionMaskUnionTest, CompiledAndFallbackUnionsPreserveAcceptance) {
  ROOT::RDataFrame source(5);
  ROOT::RDF::RNode df = source;
  std::vector<std::string> masks;
  for (unsigned i = 0; i < 5; ++i) {
    const auto name = "mask_" + std::to_string(i);
    df = df.Define(name, [i](ULong64_t entry) { return entry == i; }, {"rdfentry_"});
    masks.push_back(name);
  }
  for (unsigned count = 1; count <= 5; ++count) {
    const std::vector<std::string> inputs(masks.begin(), masks.begin() + count);
    auto selected = rdf::detail::filterAnyMask(df, inputs, "selection");
    EXPECT_EQ(*selected.Count(), count);
  }
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

TEST_F(SystematicPropagationTest, TerminalVariationDoesNotExpandIndependentShapes) {
  analyzer->DefineNominal("shape_input", []() { return 2.0f; });
  analyzer->DefineNominal("shape_input_shiftUp", []() { return 3.0f; });
  analyzer->DefineNominal("shape_input_shiftDown", []() { return 1.0f; });
  auto &sys = analyzer->getSystematicManager();
  sys.registerVariationColumns("shape_input", "shift", "shape_input_shiftUp", "shape_input_shiftDown");
  analyzer->Define("nominal_weight", [](float x) { return x * 10; }, {"shape_input"});
  analyzer->DefineNominal("weight_pileup_up", [](float x) { return x * 11; }, {"shape_input"});
  sys.registerVariationColumn("nominal_weight", "pileupUp", "weight_pileup_up");
  auto df = analyzer->getDataFrameProvider().getDataFrame();
  EXPECT_FLOAT_EQ(*df.Take<float>("nominal_weight_shiftUp")->begin(), 30.f);
  EXPECT_FLOAT_EQ(*df.Take<float>(sys.getVariationColumnName("nominal_weight", "pileupUp"))->begin(), 22.f);
  EXPECT_FALSE(df.HasColumn("weight_pileup_up_shiftUp"));
  EXPECT_FALSE(sys.isVariableAffectedBySystematic("weight_pileup_up", "shiftUp"));
}

TEST(VariationDefinitionPreflight, RejectsConflictingRequestsBeforeLowering) {
  for (bool pairedCollision : {false, true}) {
    SCOPED_TRACE(pairedCollision);
    ROOT::RDataFrame frame(1);
    ROOT::RDF::RNode df = frame.Define("input", [] { return 1.f; })
        .Define("up", [] { return 2.f; })
        .Define("down", [] { return 0.f; })
        .Define("replica", [] { return 3.f; });
    SystematicManager sm;
    sm.registerVariationColumn("input", "pdfA", "up");
    sm.registerVariationColumn("input", "pdfB", "replica");
    const std::string target = pairedCollision ? "result_shiftUp" : "shared_result";
    if (pairedCollision) {
      sm.registerVariationColumns("input", "shift", "up", "down");
      sm.registerVariationColumn("result", "pdfB", target);
    } else {
      sm.registerVariationColumn("result", "pdfA", target);
      sm.registerVariationColumn("result", "pdfB", target);
    }
    const auto beforeLabels = sm.getVariationLabels();
    const auto beforeResultFamilies = sm.getSystematicsForVariable("result");
    unsigned definitions = 0;
    try {
      rdf::detail::defineWithVariations(df, "result", {"input"}, sm,
          [&](ROOT::RDF::RNode node, const std::string &name,
              const std::vector<std::string> &inputs) {
            ++definitions;
            return node.Define(name, [](float value) { return 2 * value; }, inputs);
          });
      FAIL() << "Conflicting producers must not silently share a column";
    } catch (const std::invalid_argument &error) {
      const std::string message = error.what();
      EXPECT_NE(message.find(target), std::string::npos);
      EXPECT_NE(message.find("pdfB"), std::string::npos);
      EXPECT_NE(message.find("replica"), std::string::npos);
      EXPECT_NE(message.find(pairedCollision ? "shiftUp" : "pdfA"), std::string::npos);
      EXPECT_NE(message.find("up"), std::string::npos);
    }
    EXPECT_EQ(definitions, 0u);
    EXPECT_FALSE(df.HasColumn("result"));
    EXPECT_EQ(sm.getVariationLabels(), beforeLabels);
    EXPECT_EQ(sm.getSystematicsForVariable("result"), beforeResultFamilies);
  }
}

TEST(VariationDefinitionPreflight, SharedIdenticalRequestsUseOneComputation) {
  ROOT::RDataFrame frame(4);
  ROOT::RDF::RNode df = frame.Define("input", [] { return 1.f; })
      .Define("shifted", [] { return 2.f; });
  SystematicManager sm;
  sm.registerVariationColumn("input", "pdfA", "shifted");
  sm.registerVariationColumn("input", "pdfB", "shifted");
  sm.registerVariationColumn("result", "pdfA", "shared_result");
  sm.registerVariationColumn("result", "pdfB", "shared_result");
  unsigned definitions = 0, calls = 0;
  df = rdf::detail::defineWithVariations(df, "result", {"input"}, sm,
      [&](ROOT::RDF::RNode node, const std::string &name,
          const std::vector<std::string> &inputs) {
        ++definitions;
        return node.Define(name, [&calls](float value) { ++calls; return 2 * value; }, inputs);
      });
  auto a = df.Take<float>(sm.getVariationColumnName("result", "pdfA"));
  auto b = df.Take<float>(sm.getVariationColumnName("result", "pdfB"));
  EXPECT_EQ(a.GetValue(), (std::vector<float>{4, 4, 4, 4}));
  EXPECT_EQ(b.GetValue(), a.GetValue());
  EXPECT_EQ(definitions, 2u); // Shared varied producer plus nominal.
  EXPECT_EQ(calls, 4u); // Only the demanded shared producer, once per entry.
}

TEST(VariationDefinitionPreflight, DistinguishesBareExplicitLabelFromPairedDirections) {
  ROOT::RDataFrame source(1);
  ROOT::RDF::RNode df = source.Define("jet", [] { return 10; })
      .Define("jet_scaleUp", [] { return 11; })
      .Define("jet_scaleDown", [] { return 9; })
      .Define("weight", [] { return 2; })
      .Define("weight_alternate", [] { return 3; });
  SystematicManager sm;
  sm.registerSystematic("scale", {"jet"});
  sm.registerVariationColumn("weight", "scale", "weight_alternate");
  EXPECT_EQ(sm.getVariationColumnName("jet", "scale"), "jet");
  df = rdf::detail::defineWithVariations(df, "weighted", {"jet", "weight"}, sm,
      [](ROOT::RDF::RNode node, const std::string &name, const auto &inputs) {
        return node.Define(name, [](int jet, int weight) { return jet * weight; }, inputs);
      });
  auto nominal = df.Take<int>("weighted");
  auto up = df.Take<int>("weighted_scaleUp");
  auto down = df.Take<int>("weighted_scaleDown");
  auto replica = df.Take<int>("weighted_scale");
  EXPECT_EQ(nominal->front(), 20);
  EXPECT_EQ(up->front(), 22);
  EXPECT_EQ(down->front(), 18);
  EXPECT_EQ(replica->front(), 30);
}

TEST(VariationDefinitionPreflight, DistinctOutputsKeepStatefulCallableInvocations) {
  DataManager data(4);
  SystematicManager sm;
  data.DefineNominal("x", [] { return 1; });
  data.DefineNominal("shared_input", [] { return 2; });
  sm.registerVariationColumn("x", "replicaA", "shared_input");
  sm.registerVariationColumn("x", "replicaB", "shared_input");
  unsigned calls = 0;
  data.Define("sample", [&calls](int value) { ++calls; return value; }, {"x"}, sm);
  auto df = data.getDataFrame();
  auto a = df.Sum<int>("sample_replicaA");
  auto b = df.Sum<int>("sample_replicaB");
  EXPECT_EQ(*a, 8);
  EXPECT_EQ(*b, 8);
  EXPECT_EQ(calls, 8u);
}

TEST(VariationDefinitionPreflight, PreexistingExplicitProducerKeepsLegacyBehavior) {
  ROOT::RDataFrame frame(1);
  ROOT::RDF::RNode df = frame.Define("input", [] { return 1.f; })
      .Define("shifted", [] { return 2.f; })
      .Define("calibrated", [] { return 42.f; });
  SystematicManager sm;
  sm.registerVariationColumn("input", "pdfA", "shifted");
  sm.registerVariationColumn("result", "pdfA", "calibrated");
  df = rdf::detail::defineWithVariations(df, "result", {"input"}, sm,
      [](ROOT::RDF::RNode node, const std::string &name,
          const std::vector<std::string> &inputs) {
        return node.Define(name, [](float value) { return 2 * value; }, inputs);
      });
  EXPECT_EQ(sm.getVariationColumnName("result", "pdfA"), "calibrated");
  EXPECT_FLOAT_EQ(df.Take<float>("calibrated").GetValue()[0], 42.f);
  EXPECT_FLOAT_EQ(df.Take<float>("result").GetValue()[0], 2.f);
}

TEST_F(SystematicPropagationTest, CheckedDefinitionRejectsNominalAndBooksFreshOutputs) {
  auto twice = [](int n) { return 2 * n; };
  rdf::DefineChecked(*analyzer, "checked_input", [] { return 4; });
  analyzer->DefineNominal("checked_shift", [] { return 7; });
  auto &sm = analyzer->getSystematicManager();
  sm.registerVariationColumn("checked_input", "replica", "checked_shift");
  ASSERT_EQ(rdf::DefineChecked(*analyzer, "checked_weight", twice, {"checked_input"}),
            analyzer.get());
  try {
    rdf::DefineChecked(*analyzer, "checked_weight", twice, {"checked_shift"}, "selection");
    FAIL() << "expected nominal collision";
  } catch (const std::invalid_argument &e) {
    const std::string message = e.what();
    for (auto word : {"nominal", "checked_weight", "checked_shift", "selection"})
      EXPECT_NE(message.find(word), std::string::npos) << message;
  }
  auto df = analyzer->getDataFrameProvider().getDataFrame();
  auto nominal = df.Take<int>("checked_weight");
  auto varied = df.Take<int>("checked_weight_replica");
  EXPECT_EQ(nominal->front(), 8);
  EXPECT_EQ(varied->front(), 14);
}

TEST_F(SystematicPropagationTest, CheckedDefinitionRejectsExternalMappedOutputBeforeBooking) {
  auto twice = [](int n) { return 2 * n; };
  for (auto name : {"input_x", "input_x_shapeUp", "input_x_shapeDown", "external_shift"})
    analyzer->DefineNominal(name, [] { return 5; });
  auto &sm = analyzer->getSystematicManager();
  sm.registerSystematic("shape", {"input_x"});
  sm.registerVariationColumn("input_x", "replica", "input_x_shapeUp");
  sm.registerVariationColumn("value_x", "replica", "external_shift");
  try {
    rdf::DefineChecked(*analyzer, "value_x", twice, {"input_x"}, "jet calibration");
    FAIL() << "expected physical collision";
  } catch (const std::invalid_argument &e) {
    const std::string message = e.what();
    for (auto word : {"external_shift", "replica", "input_x_shapeUp", "jet calibration"})
      EXPECT_NE(message.find(word), std::string::npos) << message;
  }
  auto df = analyzer->getDataFrameProvider().getDataFrame();
  EXPECT_FALSE(df.HasColumn("value_x"));
  EXPECT_FALSE(df.HasColumn("value_x_shapeUp"));
  EXPECT_FALSE(df.HasColumn("value_x_shapeDown"));
  EXPECT_FALSE(sm.isVariableAffectedBySystematic("value_x", "shapeUp"));
  EXPECT_EQ(sm.getVariationColumnName("value_x", "replica"), "external_shift");
  // An analysis explicitly opting into external ownership can still use Define.
  analyzer->Define("value_x", twice, {"input_x"});
  EXPECT_EQ(analyzer->getDataFrameProvider().getDataFrame().Take<int>("external_shift")->front(), 5);
}

TEST_F(SystematicPropagationTest, CheckedDefinitionRejectsSharedRequestedOutput) {
  analyzer->DefineNominal("checked_base", [] { return 1; });
  analyzer->DefineNominal("checked_alt", [] { return 2; });
  auto &sm = analyzer->getSystematicManager();
  for (auto label : {"replicaA", "replicaB"}) {
    sm.registerVariationColumn("checked_base", label, "checked_alt");
    sm.registerVariationColumn("checked_score", label, "shared_physical_score");
  }
  EXPECT_THROW(rdf::DefineChecked(*analyzer, "checked_score", [](int n) { return n; },
                                  {"checked_base"}), std::invalid_argument);
  EXPECT_FALSE(analyzer->getDataFrameProvider().getDataFrame().HasColumn("checked_score"));
  EXPECT_FALSE(analyzer->getDataFrameProvider().getDataFrame().HasColumn("shared_physical_score"));
}
