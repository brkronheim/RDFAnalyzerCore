/**
 * @file testJetTaggingWorkingPointManager.cc
 * @brief Unit tests for JetTaggingWorkingPointManager.
 *
 * Covers:
 *  - Plugin type string.
 *  - Default construction.
 *  - setJetColumns / setTaggerColumn / addWorkingPoint validation.
 *  - WP category column definition in execute().
 *  - defineWorkingPointCollection: pass, fail, pass-range selections.
 *  - Per-event weight column definition (product of per-jet SFs).
 *  - addVariation / registerSystematicSources / applySystematicSet.
 *  - defineVariationCollections and PhysicsObjectVariationMap.
 *  - defineFractionHistograms input validation.
 *  - Error handling (missing context, empty columns, duplicate WPs, etc.).
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <JetTaggingWorkingPointManager.h>
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

class JetTaggingWorkingPointManagerTest : public ::testing::Test {
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

  /// Create, wire, and return a JetTaggingWorkingPointManager.
  std::unique_ptr<JetTaggingWorkingPointManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<JetTaggingWorkingPointManager>();
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

TEST_F(JetTaggingWorkingPointManagerTest, TypeStringIsCorrect) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_EQ(mgr.type(), "JetTaggingWorkingPointManager");
}

TEST_F(JetTaggingWorkingPointManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(JetTaggingWorkingPointManager mgr);
}

// ---------------------------------------------------------------------------
// setJetColumns validation
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, SetJetColumnsStoresNames) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_EQ(mgr.getWPCategoryColumn(), "Jet_pt_wp_category");
}

TEST_F(JetTaggingWorkingPointManagerTest, SetJetColumnsWithEmptyPtThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.setJetColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// setTaggerColumn validation
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, SetTaggerColumnStoresName) {
  JetTaggingWorkingPointManager mgr;
  mgr.setTaggerColumn("Jet_btagDeepFlavB");
  EXPECT_EQ(mgr.getTaggerColumn(), "Jet_btagDeepFlavB");
}

TEST_F(JetTaggingWorkingPointManagerTest, SetTaggerColumnEmptyThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.setTaggerColumn(""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// addWorkingPoint validation
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, AddWorkingPointsInOrder) {
  JetTaggingWorkingPointManager mgr;
  mgr.addWorkingPoint("loose",  0.05f);
  mgr.addWorkingPoint("medium", 0.30f);
  mgr.addWorkingPoint("tight",  0.75f);
  ASSERT_EQ(mgr.getWorkingPoints().size(), 3u);
  EXPECT_EQ(mgr.getWorkingPoints()[0].name, "loose");
  EXPECT_FLOAT_EQ(mgr.getWorkingPoints()[0].threshold, 0.05f);
  EXPECT_EQ(mgr.getWorkingPoints()[2].name, "tight");
}

TEST_F(JetTaggingWorkingPointManagerTest, AddWorkingPointEmptyNameThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.addWorkingPoint("", 0.5f), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, AddWorkingPointDuplicateNameThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.addWorkingPoint("loose", 0.05f);
  EXPECT_THROW(mgr.addWorkingPoint("loose", 0.30f), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, AddWorkingPointWrongOrderThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.addWorkingPoint("medium", 0.30f);
  // Threshold not strictly greater than previous → should throw.
  EXPECT_THROW(mgr.addWorkingPoint("loose", 0.10f), std::invalid_argument);
  EXPECT_THROW(mgr.addWorkingPoint("same",  0.30f), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// setInputJetCollection
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, SetInputJetCollectionEmptyThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.setInputJetCollection(""), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, SetInputJetCollectionStoresName) {
  JetTaggingWorkingPointManager mgr;
  mgr.setInputJetCollection("goodJets");
  EXPECT_EQ(mgr.getInputJetCollectionColumn(), "goodJets");
}

// ---------------------------------------------------------------------------
// execute(): WP category column
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, ExecuteDefinesWPCategoryColumn) {
  // Single event with one jet that has score 0.4 (passes loose=0.05, medium=0.3,
  // fails tight=0.75 → category 2).
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
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

TEST_F(JetTaggingWorkingPointManagerTest, WPCategoryFailAll) {
  // score = 0.01 → below all thresholds → category 0.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
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

TEST_F(JetTaggingWorkingPointManagerTest, WPCategoryPassAll) {
  // score = 0.9 → passes all three WPs → category 3.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
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

TEST_F(JetTaggingWorkingPointManagerTest, DefineWorkingPointCollectionPassWP) {
  // 2 events: one jet per event.
  // Event 0: btag = 0.4 → category 2 (pass medium).
  // Event 1: btag = 0.01 → category 0 (fail all).
  // "pass_medium" → event 0 has 1 jet, event 1 has 0 jets.
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, DefineWorkingPointCollectionFailWP) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, DefineWorkingPointCollectionPassRangeWP) {
  // "pass_loose_fail_medium" → jets with category == 1
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->addWorkingPoint("tight",  0.75f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, WeightColumnIsProductOfPerJetSFs) {
  // Single event, single jet with SF=1.2.
  // Expected per-event weight = 1.2.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.3f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, AddVariationEmptyNameThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("", "up_col", "dn_col"), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, AddVariationEmptyUpColThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("sf", "", "dn_col"), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, AddVariationEmptyDownColThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.addVariation("sf", "up_col", ""), std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, AddVariationStoresEntry) {
  JetTaggingWorkingPointManager mgr;
  mgr.setInputJetCollection("goodJets");
  mgr.addVariation("myVar", "up_sf", "dn_sf");
  ASSERT_EQ(mgr.getVariations().size(), 1u);
  EXPECT_EQ(mgr.getVariations()[0].name, "myVar");
  EXPECT_EQ(mgr.getVariations()[0].upSFColumn, "up_sf");
  EXPECT_EQ(mgr.getVariations()[0].downSFColumn, "dn_sf");
}

TEST_F(JetTaggingWorkingPointManagerTest, RegisterSystematicSetsThrowsOnEmpty) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.registerSystematicSources("", {"src"}),
               std::invalid_argument);
  EXPECT_THROW(mgr.registerSystematicSources("set", {}),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, GetUnknownSystematicSetThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(mgr.getSystematicSources("nonexistent"), std::out_of_range);
}

TEST_F(JetTaggingWorkingPointManagerTest, RegisterSystematicSetsStoresEntries) {
  JetTaggingWorkingPointManager mgr;
  mgr.registerSystematicSources("standard", {"hf", "lf", "cferr1"});
  const auto &sources = mgr.getSystematicSources("standard");
  ASSERT_EQ(sources.size(), 3u);
  EXPECT_EQ(sources[0], "hf");
  EXPECT_EQ(sources[2], "cferr1");
}

// ---------------------------------------------------------------------------
// defineWorkingPointCollection error handling
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineWPCollectionWithoutInputCollectionThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  // No setInputJetCollection → should throw.
  EXPECT_THROW(mgr.defineWorkingPointCollection("pass_medium", "out"),
               std::runtime_error);
}

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineWPCollectionUnknownWPNameThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineWorkingPointCollection("pass_tight", "out"),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineWPCollectionMalformedSelectionThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.3f);
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineWorkingPointCollection("medium", "out"),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// defineVariationCollections
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineVariationCollectionsWithoutInputCollectionThrows) {
  JetTaggingWorkingPointManager mgr;
  EXPECT_THROW(
      mgr.defineVariationCollections("nominal", "prefix"),
      std::runtime_error);
}

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineVariationCollectionsEmptyNominalThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineVariationCollections("", "prefix"),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineVariationCollectionsEmptyPrefixThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineVariationCollections("nominal", ""),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest,
       DefineVariationCollectionsCreatesVariationMap) {
  // Set up a single-event, single-jet analysis.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, FractionHistEmptyPrefixThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("", {20.f, 50.f}, {0.f, 2.4f}),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, FractionHistTooFewPtBinsThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f}, {0.f, 2.4f}),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, FractionHistTooFewEtaBinsThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f, 50.f}, {0.f}),
               std::invalid_argument);
}

TEST_F(JetTaggingWorkingPointManagerTest, FractionHistWithoutTaggerColumnThrows) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setInputJetCollection("goodJets");
  EXPECT_THROW(mgr.defineFractionHistograms("prefix", {20.f, 50.f}, {0.f, 2.4f}),
               std::runtime_error);
}

// ---------------------------------------------------------------------------
// execute(): systematic registration
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, ExecuteRegistersSystematics) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputJetCollection("goodJets");

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

TEST_F(JetTaggingWorkingPointManagerTest, ReportMetadataDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setTaggerColumn("Jet_btag");
  mgr->addWorkingPoint("loose",  0.05f);
  mgr->addWorkingPoint("medium", 0.30f);
  mgr->setInputJetCollection("goodJets");

  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries
// ---------------------------------------------------------------------------

TEST_F(JetTaggingWorkingPointManagerTest, CollectProvenanceEntriesNotEmpty) {
  JetTaggingWorkingPointManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setTaggerColumn("Jet_btag");
  mgr.addWorkingPoint("medium", 0.30f);
  mgr.setInputJetCollection("goodJets");

  const auto entries = mgr.collectProvenanceEntries();
  EXPECT_NE(entries.find("jet_pt_column"),   entries.end());
  EXPECT_NE(entries.find("tagger_column"),   entries.end());
  EXPECT_NE(entries.find("working_points"),  entries.end());
  EXPECT_NE(entries.find("input_jet_collection_column"), entries.end());
}
