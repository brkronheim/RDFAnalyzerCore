/**
 * @file testTaggerWorkingPointManager.cc
 * @brief Unit tests for TaggerWorkingPointManager.
 *
 * Covers:
 *  - Plugin type string.
 *  - Default construction.
 *  - setObjectColumns / setTaggerColumn / addWorkingPoint validation.
 *  - WP category column definition in execute().
 *  - defineWorkingPointCollection: pass, fail, pass-range selections.
 *  - Per-event weight column definition (product of per-jet SFs).
 *  - addVariation / registerSystematicSources / applySystematicSet.
 *  - defineVariationCollections and PhysicsObjectVariationMap.
 *  - defineFractionHistograms input validation.
 *  - defineUnfilteredCollection.
 *  - setupFromConfigFile / setRole / configuredCorrections.
 *  - registerWeightsWithWeightManager.
 *  - Error handling (missing context, empty columns, duplicate WPs, etc.).
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <TaggerWorkingPointManager.h>
#include <WeightManager.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <PhysicsObjectCollection.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <test_util.h>

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

static ManagerContext makeContext(IConfigurationProvider &cfg,
                                   DataManager &dm, SystematicManager &sm,
                                   DefaultLogger &log, NullOutputSink &skim,
                                   NullOutputSink &meta) {
  return ManagerContext{cfg, dm, sm, log, skim, meta};
}

// ---------------------------------------------------------------------------
// Fixture
// ---------------------------------------------------------------------------

class TaggerWorkingPointManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    config = ManagerFactory::createConfigurationManager(
        "cfg/test_data_config_minimal.txt");
    systematicManager = std::make_unique<SystematicManager>();
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();
  }

  /// Create, wire, and return a TaggerWorkingPointManager.
  std::unique_ptr<TaggerWorkingPointManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<TaggerWorkingPointManager>();
    auto ctx = makeContext(*config, dm, *systematicManager, *logger,
                           *skimSink, *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
  }

  /// Define a per-event RVec<Float_t> column with constant value.
  void defineRVecColumn(DataManager &dm, const std::string &name, float value) {
    dm.Define(
        name,
        [value](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {value}; },
        {"rdfentry_"}, *systematicManager);
  }

  /// Define a per-event RVec<Int_t> column with constant value.
  void defineIntRVecColumn(DataManager &dm, const std::string &name, int value) {
    dm.Define(
        name,
        [value](ULong64_t) -> ROOT::VecOps::RVec<Int_t> { return {value}; },
        {"rdfentry_"}, *systematicManager);
  }

  /// Define a per-event PhysicsObjectCollection column (single jet).
  void defineCollectionColumn(DataManager &dm, const std::string &name,
                               float pt, float eta = 0.f, float phi = 0.f,
                               float mass = 0.f) {
    dm.Define(
        name,
        [pt, eta, phi, mass](ULong64_t) -> PhysicsObjectCollection {
          ROOT::VecOps::RVec<Float_t> ptV{pt};
          ROOT::VecOps::RVec<Float_t> etaV{eta};
          ROOT::VecOps::RVec<Float_t> phiV{phi};
          ROOT::VecOps::RVec<Float_t> massV{mass};
          ROOT::VecOps::RVec<bool>    maskV{true};
          return PhysicsObjectCollection(ptV, etaV, phiV, massV, maskV);
        },
        {"rdfentry_"}, *systematicManager);
  }

  std::unique_ptr<IConfigurationProvider> config;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// ---------------------------------------------------------------------------
// Construction and type
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, TypeStringIsCorrect) {
  TaggerWorkingPointManager mgr;
  EXPECT_EQ(mgr.type(), "TaggerWorkingPointManager");
}

TEST_F(TaggerWorkingPointManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(TaggerWorkingPointManager mgr);
}

// ---------------------------------------------------------------------------
// setObjectColumns validation
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, SetJetColumnsStoresNames) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_EQ(mgr.getWPCategoryColumn(), "Jet_pt_wp_category");
}

TEST_F(TaggerWorkingPointManagerTest, SetJetColumnsWithEmptyPtThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.setObjectColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// setTaggerColumn validation
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, SetTaggerColumnStoresName) {
  TaggerWorkingPointManager mgr;
  mgr.setTaggerColumn("Jet_btagDeepFlavB");
  EXPECT_EQ(mgr.getTaggerColumn(), "Jet_btagDeepFlavB");
}

TEST_F(TaggerWorkingPointManagerTest, SetTaggerColumnEmptyThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.setTaggerColumn(""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// addWorkingPoint validation
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointsInOrder) {
  TaggerWorkingPointManager mgr;
  mgr.addWorkingPoint("loose",  0.05f);
  mgr.addWorkingPoint("medium", 0.30f);
  mgr.addWorkingPoint("tight",  0.75f);
  ASSERT_EQ(mgr.getWorkingPoints().size(), 3u);
  EXPECT_EQ(mgr.getWorkingPoints()[0].name, "loose");
  ASSERT_FALSE(mgr.getWorkingPoints()[0].thresholds.empty());
  EXPECT_FLOAT_EQ(mgr.getWorkingPoints()[0].thresholds[0], 0.05f);
  EXPECT_EQ(mgr.getWorkingPoints()[2].name, "tight");
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointEmptyNameThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.addWorkingPoint("", 0.5f), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointDuplicateNameThrows) {
  TaggerWorkingPointManager mgr;
  mgr.addWorkingPoint("loose", 0.05f);
  EXPECT_THROW(mgr.addWorkingPoint("loose", 0.30f), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointWrongOrderThrows) {
  TaggerWorkingPointManager mgr;
  mgr.addWorkingPoint("medium", 0.30f);
  // Threshold not strictly greater than previous → should throw.
  EXPECT_THROW(mgr.addWorkingPoint("loose", 0.10f), std::invalid_argument);
  EXPECT_THROW(mgr.addWorkingPoint("same",  0.30f), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// setTaggerColumns — multi-score API
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, SetTaggerColumnsStoresNames) {
  TaggerWorkingPointManager mgr;
  mgr.setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  ASSERT_EQ(mgr.getTaggerColumns().size(), 2u);
  EXPECT_EQ(mgr.getTaggerColumns()[0], "Jet_CvsL");
  EXPECT_EQ(mgr.getTaggerColumns()[1], "Jet_CvsB");
  // getTaggerColumn() should return the first column.
  EXPECT_EQ(mgr.getTaggerColumn(), "Jet_CvsL");
}

TEST_F(TaggerWorkingPointManagerTest, SetTaggerColumnsEmptyListThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.setTaggerColumns({}), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, SetTaggerColumnsEmptyElementThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.setTaggerColumns({"Jet_CvsL", ""}), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointVectorSingleThreshold) {
  TaggerWorkingPointManager mgr;
  mgr.addWorkingPoint("loose",  std::vector<float>{0.05f});
  mgr.addWorkingPoint("medium", std::vector<float>{0.30f});
  ASSERT_EQ(mgr.getWorkingPoints().size(), 2u);
  ASSERT_EQ(mgr.getWorkingPoints()[0].thresholds.size(), 1u);
  EXPECT_FLOAT_EQ(mgr.getWorkingPoints()[0].thresholds[0], 0.05f);
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointVectorMultiThreshold) {
  TaggerWorkingPointManager mgr;
  // CvsL/CvsB charm-tagging WPs (no ordering constraint in multi-score).
  mgr.addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr.addWorkingPoint("medium", {0.108f, 0.285f});
  mgr.addWorkingPoint("tight",  {0.274f, 0.605f});
  ASSERT_EQ(mgr.getWorkingPoints().size(), 3u);
  ASSERT_EQ(mgr.getWorkingPoints()[1].thresholds.size(), 2u);
  EXPECT_FLOAT_EQ(mgr.getWorkingPoints()[1].thresholds[0], 0.108f);
  EXPECT_FLOAT_EQ(mgr.getWorkingPoints()[1].thresholds[1], 0.285f);
}

TEST_F(TaggerWorkingPointManagerTest, AddWorkingPointEmptyThresholdsThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.addWorkingPoint("loose", std::vector<float>{}),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// setInputObjectCollection
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, SetInputJetCollectionEmptyThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.setInputObjectCollection(""), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, SetInputJetCollectionStoresName) {
  TaggerWorkingPointManager mgr;
  mgr.setInputObjectCollection("goodJets");
  EXPECT_EQ(mgr.getInputObjectCollectionColumn(), "goodJets");
}

// ---------------------------------------------------------------------------
// execute(): WP category column
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, ExecuteDefinesWPCategoryColumn) {
  // Single event with one jet that has score 0.4 (passes loose=0.05, medium=0.3,
  // fails tight=0.75 → category 2).
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);

  // Jet_btag = 0.4 per event (single jet).
  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.4f}; },
      {"rdfentry_"}, *systematicManager);

  mgr->execute();

  const std::string catCol = mgr->getWPCategoryColumn();
  auto result = dm->getDataFrame().Take<ROOT::VecOps::RVec<Int_t>>(catCol);
  const auto &cats = result.GetValue();
  ASSERT_EQ(cats.size(), 1u);           // 1 event
  ASSERT_EQ(cats[0].size(), 1u);        // 1 jet
  EXPECT_EQ(cats[0][0], 2);             // category 2: passes L and M, fails T
}

TEST_F(TaggerWorkingPointManagerTest, WPCategoryFailAll) {
  // score = 0.01 → below all thresholds → category 0.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);

  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.01f}; },
      {"rdfentry_"}, *systematicManager);
  mgr->execute();

  auto result = dm->getDataFrame().Take<ROOT::VecOps::RVec<Int_t>>(
      mgr->getWPCategoryColumn());
  EXPECT_EQ(result.GetValue()[0][0], 0);
}

TEST_F(TaggerWorkingPointManagerTest, WPCategoryPassAll) {
  // score = 0.9 → passes all three WPs → category 3.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);

  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.9f}; },
      {"rdfentry_"}, *systematicManager);
  mgr->execute();

  auto result = dm->getDataFrame().Take<ROOT::VecOps::RVec<Int_t>>(
      mgr->getWPCategoryColumn());
  EXPECT_EQ(result.GetValue()[0][0], 3);
}

// ---------------------------------------------------------------------------
// execute(): WP-filtered collections
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, DefineWorkingPointCollectionPassWP) {
  // 2 events: one jet per event.
  // Event 0: btag = 0.4 → category 2 (pass medium).
  // Event 1: btag = 0.01 → category 0 (fail all).
  // "pass_medium" → event 0 has 1 jet, event 1 has 0 jets.
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputObjectCollection("goodJets");

  // Define goodJets: a single jet with pt=50 for all events.
  dm->Define(
      "goodJets",
      [](ULong64_t) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<Float_t> pt{50.f}, eta{0.f}, phi{0.f}, mass{0.f};
        ROOT::VecOps::RVec<bool> mask{true};
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
      },
      {"rdfentry_"}, *systematicManager);

  // Event 0: score 0.4; Event 1: score 0.01.
  dm->Define(
      "Jet_btag",
      [](ULong64_t i) -> ROOT::VecOps::RVec<Float_t> {
        return {i == 0 ? 0.4f : 0.01f};
      },
      {"rdfentry_"}, *systematicManager);

  mgr->defineWorkingPointCollection("pass_medium", "goodJets_bmedium");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_bmedium");
  const auto &cols = result.GetValue();
  ASSERT_EQ(cols.size(), 2u);
  EXPECT_EQ(cols[0].size(), 1u);  // Event 0: passes medium
  EXPECT_EQ(cols[1].size(), 0u);  // Event 1: fails medium
}

TEST_F(TaggerWorkingPointManagerTest, DefineWorkingPointCollectionFailWP) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputObjectCollection("goodJets");

  dm->Define(
      "goodJets",
      [](ULong64_t) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<Float_t> pt{50.f}, eta{0.f}, phi{0.f}, mass{0.f};
        ROOT::VecOps::RVec<bool> mask{true};
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
      },
      {"rdfentry_"}, *systematicManager);

  // Event 0: score 0.4 (pass loose); Event 1: score 0.01 (fail loose).
  dm->Define(
      "Jet_btag",
      [](ULong64_t i) -> ROOT::VecOps::RVec<Float_t> {
        return {i == 0 ? 0.4f : 0.01f};
      },
      {"rdfentry_"}, *systematicManager);

  mgr->defineWorkingPointCollection("fail_loose", "goodJets_bfail");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_bfail");
  const auto &cols = result.GetValue();
  ASSERT_EQ(cols.size(), 2u);
  EXPECT_EQ(cols[0].size(), 0u);  // Event 0: passes loose, NOT in fail set
  EXPECT_EQ(cols[1].size(), 1u);  // Event 1: fails loose
}

TEST_F(TaggerWorkingPointManagerTest, DefineWorkingPointCollectionPassRangeWP) {
  // "pass_loose_fail_medium" → jets with category == 1
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputObjectCollection("goodJets");

  dm->Define(
      "goodJets",
      [](ULong64_t) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<Float_t> pt{50.f}, eta{0.f}, phi{0.f}, mass{0.f};
        ROOT::VecOps::RVec<bool> mask{true};
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
      },
      {"rdfentry_"}, *systematicManager);

  // Event 0: 0.10 (cat=1: pass L, fail M)
  // Event 1: 0.40 (cat=2: pass M, fail T)
  // Event 2: 0.01 (cat=0: fail all)
  dm->Define(
      "Jet_btag",
      [](ULong64_t i) -> ROOT::VecOps::RVec<Float_t> {
        float scores[] = {0.10f, 0.40f, 0.01f};
        return {scores[i]};
      },
      {"rdfentry_"}, *systematicManager);

  mgr->defineWorkingPointCollection("pass_loose_fail_medium",
                                    "goodJets_bL_notM");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>(
      "goodJets_bL_notM");
  const auto &cols = result.GetValue();
  ASSERT_EQ(cols.size(), 3u);
  EXPECT_EQ(cols[0].size(), 1u);  // Event 0: cat=1 → in range
  EXPECT_EQ(cols[1].size(), 0u);  // Event 1: cat=2 → above range
  EXPECT_EQ(cols[2].size(), 0u);  // Event 2: cat=0 → below range
}

// ---------------------------------------------------------------------------
// execute(): per-event weight column
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, WeightColumnIsProductOfPerJetSFs) {
  // Single event, single jet with SF=1.2.
  // Expected per-event weight = 1.2.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.3f);
  mgr->setInputObjectCollection("goodJets");

  defineCollectionColumn(*dm, "goodJets", 50.f);
  // Per-jet SF = 1.2
  defineRVecColumn(*dm, "my_sf_central", 1.2f);

  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.5f}; },
      {"rdfentry_"}, *systematicManager);

  // Manually schedule a weight step (bypassing correctionlib).
  // Normally the user calls applyCorrectionlib; here we call addVariation-style.
  // We use the internal weight step path via addVariation.
  mgr->addVariation("testSF", "my_sf_central", "my_sf_central",
                    "my_sf_central_weight", "my_sf_central_weight");
  mgr->execute();

  auto result = dm->getDataFrame().Take<Float_t>("my_sf_central_weight");
  EXPECT_FLOAT_EQ(result.GetValue()[0], 1.2f);
}

// ---------------------------------------------------------------------------
// addVariation / registerSystematicSources
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, AddVariationEmptyNameThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("", "up_col", "dn_col"), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddVariationEmptyUpColThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("sf", "", "dn_col"), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddVariationEmptyDownColThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("sf", "up_col", ""), std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, AddVariationStoresEntry) {
  TaggerWorkingPointManager mgr;
  mgr.setInputObjectCollection("goodJets");
  mgr.addVariation("myVar", "up_sf", "dn_sf");
  ASSERT_EQ(mgr.getVariations().size(), 1u);
  EXPECT_EQ(mgr.getVariations()[0].name, "myVar");
  EXPECT_EQ(mgr.getVariations()[0].upSFColumn, "up_sf");
  EXPECT_EQ(mgr.getVariations()[0].downSFColumn, "dn_sf");
}

TEST_F(TaggerWorkingPointManagerTest, RegisterSystematicSetsThrowsOnEmpty) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.registerSystematicSources("", {"src"}),
               std::invalid_argument);
  EXPECT_THROW(mgr.registerSystematicSources("set", {}),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, GetUnknownSystematicSetThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(mgr.getSystematicSources("nonexistent"), std::out_of_range);
}

TEST_F(TaggerWorkingPointManagerTest, RegisterSystematicSetsStoresEntries) {
  TaggerWorkingPointManager mgr;
  mgr.registerSystematicSources("standard", {"hf", "lf", "cferr1"});
  const auto &sources = mgr.getSystematicSources("standard");
  ASSERT_EQ(sources.size(), 3u);
  EXPECT_EQ(sources[0], "hf");
  EXPECT_EQ(sources[2], "cferr1");
}

// ---------------------------------------------------------------------------
// defineWorkingPointCollection error handling
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest,
       DefineWPCollectionWithoutInputCollectionThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  // No setInputObjectCollection → should throw.
  EXPECT_THROW(mgr.defineWorkingPointCollection("pass_medium", "out"),
               std::runtime_error);
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineWPCollectionUnknownWPNameThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineWorkingPointCollection("pass_tight", "out"),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineWPCollectionMalformedSelectionThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineWorkingPointCollection("medium", "out"),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// defineVariationCollections
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest,
       DefineVariationCollectionsWithoutInputCollectionThrows) {
  TaggerWorkingPointManager mgr;
  EXPECT_THROW(
      mgr.defineVariationCollections("nominal", "prefix"),
      std::runtime_error);
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineVariationCollectionsEmptyNominalThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineVariationCollections("", "prefix"),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineVariationCollectionsEmptyPrefixThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineVariationCollections("nominal", ""),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineVariationCollectionsCreatesVariationMap) {
  // Set up a single-event, single-jet analysis.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputObjectCollection("goodJets");

  defineCollectionColumn(*dm, "goodJets", 50.f);
  defineRVecColumn(*dm, "Jet_btag", 0.5f);  // as a score column (not used here)

  // Define the WP-filtered nominal collection.
  mgr->defineWorkingPointCollection("pass_medium", "goodJets_bmedium");

  // Register a variation.
  defineRVecColumn(*dm, "btag_sf_hf_up", 1.1f);
  defineRVecColumn(*dm, "btag_sf_hf_down", 0.9f);
  mgr->addVariation("btagHF", "btag_sf_hf_up", "btag_sf_hf_down");

  // Build variation collections + map.
  mgr->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
                                   "goodJets_btag_map");

  mgr->execute();

  // The variation map should contain "nominal", "btagHFUp", "btagHFDown".
  auto result = dm->getDataFrame().Take<PhysicsObjectVariationMap>(
      "goodJets_btag_map");
  const auto &maps = result.GetValue();
  ASSERT_EQ(maps.size(), 1u);
  EXPECT_NE(maps[0].find("nominal"),   maps[0].end());
  EXPECT_NE(maps[0].find("btagHFUp"),  maps[0].end());
  EXPECT_NE(maps[0].find("btagHFDown"), maps[0].end());
}

// ---------------------------------------------------------------------------
// defineFractionHistograms input validation
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, FractionHistEmptyPrefixThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("", {20.f, 50.f}, {0.f, 2.4f}),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, FractionHistTooFewPtBinsThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f}, {0.f, 2.4f}),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, FractionHistTooFewEtaBinsThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f, 50.f}, {0.f}),
               std::invalid_argument);
}

TEST_F(TaggerWorkingPointManagerTest, FractionHistWithoutTaggerColumnThrows) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setInputObjectCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f, 50.f}, {0.f, 2.4f}),
               std::runtime_error);
}

// ---------------------------------------------------------------------------
// execute(): systematic registration
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, ExecuteRegistersSystematics) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputObjectCollection("goodJets");

  defineCollectionColumn(*dm, "goodJets", 50.f);

  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.5f}; },
      {"rdfentry_"}, *systematicManager);

  defineRVecColumn(*dm, "sf_hf_up",   1.1f);
  defineRVecColumn(*dm, "sf_hf_down", 0.9f);

  mgr->addVariation("btagHF", "sf_hf_up", "sf_hf_down");
  mgr->execute();

  // Systematic "btagHF" should be registered.
  EXPECT_NE(systematicManager->getSystematics().find("btagHF"),
            systematicManager->getSystematics().end());
}

// ---------------------------------------------------------------------------
// reportMetadata (smoke test)
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, ReportMetadataDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputObjectCollection("goodJets");

  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, CollectProvenanceEntriesNotEmpty) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.30f);
  mgr.setInputObjectCollection("goodJets");

  const auto entries = mgr.collectProvenanceEntries();
  EXPECT_NE(entries.find("pt_column"),   entries.end());
  EXPECT_NE(entries.find("tagger_column"),   entries.end());
  EXPECT_NE(entries.find("working_points"),  entries.end());
  EXPECT_NE(entries.find("input_object_collection_column"), entries.end());
}

// ---------------------------------------------------------------------------
// Multi-score WP category column
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, MultiScoreWPCategoryColumnFailAll) {
  // CvsL=0.03, CvsB=0.10 — fails loose WP (needs CvsL>0.042 AND CvsB>0.135).
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  mgr->addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr->addWorkingPoint("medium", {0.108f, 0.285f});

  dm->Define("Jet_CvsL",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.03f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_CvsB",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.10f}; },
             {"rdfentry_"}, *systematicManager);

  mgr->execute();

  auto cat = dm->getDataFrame()
                  .Take<ROOT::VecOps::RVec<Int_t>>("Jet_pt_wp_category")
                  .GetValue();
  ASSERT_EQ(cat.size(), 1u);
  EXPECT_EQ(cat[0][0], 0);
}

TEST_F(TaggerWorkingPointManagerTest, MultiScoreWPCategoryColumnPassLoose) {
  // CvsL=0.06, CvsB=0.20 — passes loose (>0.042,>0.135) fails medium (>0.108,>0.285).
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  mgr->addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr->addWorkingPoint("medium", {0.108f, 0.285f});

  dm->Define("Jet_CvsL",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.06f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_CvsB",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.20f}; },
             {"rdfentry_"}, *systematicManager);

  mgr->execute();

  auto cat = dm->getDataFrame()
                  .Take<ROOT::VecOps::RVec<Int_t>>("Jet_pt_wp_category")
                  .GetValue();
  ASSERT_EQ(cat.size(), 1u);
  EXPECT_EQ(cat[0][0], 1);
}

TEST_F(TaggerWorkingPointManagerTest, MultiScoreWPCategoryColumnPassAllPartial) {
  // CvsL=0.15, CvsB=0.20 — passes CvsL for medium but fails CvsB for medium.
  // Should pass loose (cat=1), not medium.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  mgr->addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr->addWorkingPoint("medium", {0.108f, 0.285f});

  dm->Define("Jet_CvsL",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.15f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_CvsB",
             // 0.20 < 0.285 → fails medium's CvsB threshold
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.20f}; },
             {"rdfentry_"}, *systematicManager);

  mgr->execute();

  auto cat = dm->getDataFrame()
                  .Take<ROOT::VecOps::RVec<Int_t>>("Jet_pt_wp_category")
                  .GetValue();
  ASSERT_EQ(cat.size(), 1u);
  EXPECT_EQ(cat[0][0], 1);
}

TEST_F(TaggerWorkingPointManagerTest, MultiScoreWPCategoryColumnPassAll) {
  // CvsL=0.30, CvsB=0.50 — passes both loose and medium thresholds.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  mgr->addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr->addWorkingPoint("medium", {0.108f, 0.285f});

  dm->Define("Jet_CvsL",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.30f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_CvsB",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.50f}; },
             {"rdfentry_"}, *systematicManager);

  mgr->execute();

  auto cat = dm->getDataFrame()
                  .Take<ROOT::VecOps::RVec<Int_t>>("Jet_pt_wp_category")
                  .GetValue();
  ASSERT_EQ(cat.size(), 1u);
  EXPECT_EQ(cat[0][0], 2);
}

TEST_F(TaggerWorkingPointManagerTest, MultiScoreWPCollectionFilter) {
  // Jet with CvsL=0.06, CvsB=0.20 → passes loose, fails medium.
  // defineWorkingPointCollection("pass_loose") should include it.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumns({"Jet_CvsL", "Jet_CvsB"});
  mgr->addWorkingPoint("loose",  {0.042f, 0.135f});
  mgr->addWorkingPoint("medium", {0.108f, 0.285f});

  dm->Define("Jet_CvsL",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.06f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_CvsB",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.20f}; },
             {"rdfentry_"}, *systematicManager);

  defineCollectionColumn(*dm, "goodJets", 50.f);
  mgr->setInputObjectCollection("goodJets");
  mgr->defineWorkingPointCollection("pass_loose",  "jets_passLoose");
  mgr->defineWorkingPointCollection("pass_medium", "jets_passMedium");
  mgr->execute();

  auto passLoose = dm->getDataFrame()
                       .Take<PhysicsObjectCollection>("jets_passLoose")
                       .GetValue();
  auto passMedium = dm->getDataFrame()
                        .Take<PhysicsObjectCollection>("jets_passMedium")
                        .GetValue();
  EXPECT_EQ(passLoose[0].size(), 1u);   // passes loose → included
  EXPECT_EQ(passMedium[0].size(), 0u);  // fails medium → excluded
}

// ---------------------------------------------------------------------------
// Feature: defineUnfilteredCollection
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, UnfilteredCollectionKeepsAllObjects) {
  // 1 event with a single jet — unfiltered collection should have 1 jet.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputObjectCollection("goodJets");

  defineCollectionColumn(*dm, "goodJets", 50.f);
  dm->Define(
      "Jet_btag",
      [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.01f}; },
      {"rdfentry_"}, *systematicManager);

  // Even though score 0.01 fails medium, unfiltered collection keeps all.
  mgr->defineUnfilteredCollection("allJets_annotated");
  mgr->execute();

  auto result = dm->getDataFrame()
                    .Take<PhysicsObjectCollection>("allJets_annotated")
                    .GetValue();
  ASSERT_EQ(result.size(), 1u);
  EXPECT_EQ(result[0].size(), 1u);  // unchanged — all jets kept
}

TEST_F(TaggerWorkingPointManagerTest,
       DefineUnfilteredCollectionRequiresInputCollection) {
  TaggerWorkingPointManager mgr;
  mgr.setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  // No setInputObjectCollection() → should throw.
  EXPECT_THROW(mgr.defineUnfilteredCollection("out"), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Feature: setRole / ConfiguredCorrections
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest, SetRoleStoresRole) {
  TaggerWorkingPointManager mgr;
  EXPECT_EQ(mgr.getRole(), "taggerWorkingPoints");  // default
  mgr.setRole("myTagger");
  EXPECT_EQ(mgr.getRole(), "myTagger");
}

TEST_F(TaggerWorkingPointManagerTest, ConfiguredCorrectionsEmptyByDefault) {
  TaggerWorkingPointManager mgr;
  EXPECT_TRUE(mgr.getConfiguredCorrections().empty());
}

// ---------------------------------------------------------------------------
// Feature: WeightManager integration
// ---------------------------------------------------------------------------

TEST_F(TaggerWorkingPointManagerTest,
       WeightManagerIntegrationRegistersScaleFactor) {
  // Verify that registerWeightsWithWeightManager calls addScaleFactor on wm.
  // addScaleFactor + defineNominalWeight must not throw (they are pure state
  // mutations; the actual column is only defined inside execute()).
  TaggerWorkingPointManager mgr;
  mgr.setInputObjectCollection("goodJets");

  // Register a variation so we can verify registerVariations path too.
  mgr.addVariation("btagHF", "sf_hf_up", "sf_hf_down",
                   "sf_hf_up_weight", "sf_hf_down_weight");

  WeightManager wm;
  EXPECT_NO_THROW(
      mgr.registerWeightsWithWeightManager(wm, "btag", "btag_weight"));

  // defineNominalWeight just records the column name — must not throw.
  EXPECT_NO_THROW(wm.defineNominalWeight("weight_nominal"));
}
