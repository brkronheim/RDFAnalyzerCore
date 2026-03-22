/**
 * @file testJetEnergyScaleManager.cc
 * @brief Unit tests for the JetEnergyScaleManager plugin.
 *
 * Tests cover: jet column registration, raw-pT computation, correction step
 * scheduling, CMS correctionlib convenience API, systematic variation
 * registration, lifecycle hooks, PhysicsObjectCollection integration, and
 * error handling.
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <JetEnergyScaleManager.h>
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
// Test fixture
// ---------------------------------------------------------------------------

class JetEnergyScaleManagerTest : public ::testing::Test {
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

  /// Create, wire, and return a JetEnergyScaleManager attached to the given
  /// DataManager.
  std::unique_ptr<JetEnergyScaleManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<JetEnergyScaleManager>();
    auto ctx = makeContext(*config, dm, *systematicManager, *logger,
                           *skimSink, *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
  }

  /// Define per-event RVec columns on the dataframe.
  /// Each event has a single-element RVec whose value equals the lambda result.
  void defineRVecColumn(DataManager &dm, const std::string &name,
                        std::function<Float_t(ULong64_t)> fn) {
    dm.Define(
        name,
        [fn](ULong64_t i) -> ROOT::VecOps::RVec<Float_t> { return {fn(i)}; },
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

TEST_F(JetEnergyScaleManagerTest, TypeStringIsCorrect) {
  JetEnergyScaleManager mgr;
  EXPECT_EQ(mgr.type(), "JetEnergyScaleManager");
}

TEST_F(JetEnergyScaleManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(JetEnergyScaleManager mgr);
}

// ---------------------------------------------------------------------------
// setJetColumns validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, SetJetColumnsStoresNames) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_EQ(mgr.getPtColumn(),   "Jet_pt");
  EXPECT_EQ(mgr.getMassColumn(), "Jet_mass");
}

TEST_F(JetEnergyScaleManagerTest, SetJetColumnsWithEmptyPtThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setJetColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, EmptyMassColumnDisablesMassCorrections) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "");
  EXPECT_TRUE(mgr.getMassColumn().empty());
}

// ---------------------------------------------------------------------------
// removeExistingCorrections / setRawPtColumn
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, RemoveExistingCorrectionsSetsRawPtColumn) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.removeExistingCorrections("Jet_rawFactor");
  EXPECT_EQ(mgr.getRawPtColumn(), "Jet_pt_raw");
}

TEST_F(JetEnergyScaleManagerTest, RemoveExistingCorrectionsWithEmptyFactorThrows) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_THROW(mgr.removeExistingCorrections(""), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, SetRawPtColumnStoresName) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setRawPtColumn("Jet_pt_rawCustom");
  EXPECT_EQ(mgr.getRawPtColumn(), "Jet_pt_rawCustom");
}

TEST_F(JetEnergyScaleManagerTest, SetRawPtColumnAfterRemoveExistingThrows) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.removeExistingCorrections("Jet_rawFactor");
  EXPECT_THROW(mgr.setRawPtColumn("Jet_pt_raw2"), std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, RemoveExistingAfterSetRawPtColumnThrows) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr.setRawPtColumn("Jet_pt_raw");
  EXPECT_THROW(mgr.removeExistingCorrections("Jet_rawFactor"),
               std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, SetRawPtColumnWithEmptyStringThrows) {
  JetEnergyScaleManager mgr;
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_THROW(mgr.setRawPtColumn(""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// applyCorrection validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ApplyCorrectionWithEmptyInputThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.applyCorrection("", "sf_col", "out_pt"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, ApplyCorrectionWithEmptySFThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.applyCorrection("Jet_pt_raw", "", "out_pt"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, ApplyCorrectionWithEmptyOutputThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.applyCorrection("Jet_pt_raw", "sf_col", ""),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// addVariation validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, AddVariationWithEmptyNameThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.addVariation("", "up_col", "dn_col"), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, AddVariationWithEmptyUpColThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.addVariation("jesTotal", "", "dn_col"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, AddVariationWithEmptyDownColThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.addVariation("jesTotal", "up_col", ""),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, AddVariationStoresEntry) {
  JetEnergyScaleManager mgr;
  mgr.addVariation("jesTotal", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  ASSERT_EQ(mgr.getVariations().size(), 1u);
  EXPECT_EQ(mgr.getVariations()[0].name,        "jesTotal");
  EXPECT_EQ(mgr.getVariations()[0].upPtColumn,  "Jet_pt_jes_up");
  EXPECT_EQ(mgr.getVariations()[0].downPtColumn,"Jet_pt_jes_dn");
}

TEST_F(JetEnergyScaleManagerTest, AddMultipleVariationsStoredInOrder) {
  JetEnergyScaleManager mgr;
  mgr.addVariation("jesTotal", "Jet_pt_jes_up",  "Jet_pt_jes_dn");
  mgr.addVariation("jer",      "Jet_pt_jer_up",  "Jet_pt_jer_dn");
  ASSERT_EQ(mgr.getVariations().size(), 2u);
  EXPECT_EQ(mgr.getVariations()[0].name, "jesTotal");
  EXPECT_EQ(mgr.getVariations()[1].name, "jer");
}

// ---------------------------------------------------------------------------
// Lifecycle hooks on empty / minimal manager
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ExecuteWithNoConfigDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->execute());
}

TEST_F(JetEnergyScaleManagerTest, FinalizeIsNoOp) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  EXPECT_NO_THROW(mgr->finalize());
}

TEST_F(JetEnergyScaleManagerTest, ReportMetadataOnEmptyManagerDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(JetEnergyScaleManagerTest, InitializeIsCallable) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->initialize());
}

TEST_F(JetEnergyScaleManagerTest, GetDependenciesReturnsEmpty) {
  JetEnergyScaleManager mgr;
  EXPECT_TRUE(mgr.getDependencies().empty());
}

// ---------------------------------------------------------------------------
// removeExistingCorrections – raw pT and mass columns are defined correctly
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, RawPtAndMassColumnsDefinedByRemoveExisting) {
  // 3 events; Jet_pt = [100, 200, 300], Jet_mass = [10, 20, 30],
  // Jet_rawFactor = [0.1, 0.2, 0.3].
  // Expected: Jet_pt_raw = [90, 160, 210], Jet_mass_raw = [9, 16, 21].
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 100); });
  defineRVecColumn(*dm, "Jet_mass",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 10); });
  defineRVecColumn(*dm, "Jet_rawFactor",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 0.1f); });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->removeExistingCorrections("Jet_rawFactor");
  mgr->execute();

  // Read back the raw pT column.
  auto ptRawResult =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_raw");
  auto massRawResult =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_mass_raw");

  const auto &ptRaw   = ptRawResult.GetValue();
  const auto &massRaw = massRawResult.GetValue();

  ASSERT_EQ(ptRaw.size(),   3u);
  ASSERT_EQ(massRaw.size(), 3u);

  // Event 0: pt=100, factor=0.1 → raw=90
  EXPECT_FLOAT_EQ(ptRaw[0][0],   90.0f);
  EXPECT_FLOAT_EQ(massRaw[0][0],  9.0f);
  // Event 1: pt=200, factor=0.2 → raw=160
  EXPECT_FLOAT_EQ(ptRaw[1][0],  160.0f);
  EXPECT_FLOAT_EQ(massRaw[1][0], 16.0f);
  // Event 2: pt=300, factor=0.3 → raw=210
  EXPECT_FLOAT_EQ(ptRaw[2][0],  210.0f);
  EXPECT_FLOAT_EQ(massRaw[2][0], 21.0f);
}

// ---------------------------------------------------------------------------
// applyCorrection – corrected pT and mass columns are defined correctly
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ApplyCorrectionDefinesCorrectedPtColumn) {
  // 2 events; Jet_pt_raw = [100, 200]; jec_sf = [1.1, 0.9].
  // Expected Jet_pt_jec = [110, 180].
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 100); });
  defineRVecColumn(*dm, "Jet_mass_raw",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 10); });
  defineRVecColumn(*dm, "jec_sf",
      [](ULong64_t i) { return i == 0 ? 1.1f : 0.9f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jec_sf", "Jet_pt_jec");
  mgr->execute();

  auto ptResult =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_jec");
  auto massResult =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_mass_jec");

  const auto &pt   = ptResult.GetValue();
  const auto &mass = massResult.GetValue();

  ASSERT_EQ(pt.size(),   2u);
  ASSERT_EQ(mass.size(), 2u);

  EXPECT_FLOAT_EQ(pt[0][0],   110.0f);
  EXPECT_FLOAT_EQ(pt[1][0],   180.0f);
  EXPECT_FLOAT_EQ(mass[0][0],  11.0f);
  EXPECT_FLOAT_EQ(mass[1][0],  18.0f);
}

TEST_F(JetEnergyScaleManagerTest, ApplyCorrectionWithMassDisabledNoMassColumn) {
  // applyToMass = false → no mass output column should be created.
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",
      [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "jec_sf",
      [](ULong64_t) { return 1.05f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jec_sf", "Jet_pt_jec",
                        /*applyToMass=*/false);
  mgr->execute();

  // Jet_pt_jec should exist.
  auto cols = dm->getDataFrame().GetColumnNames();
  EXPECT_NE(std::find(cols.begin(), cols.end(), "Jet_pt_jec"), cols.end());
  // Jet_mass_jec should NOT exist.
  EXPECT_EQ(std::find(cols.begin(), cols.end(), "Jet_mass_jec"), cols.end());
}

// ---------------------------------------------------------------------------
// Chained corrections – each step uses the output of the previous step
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ChainedCorrectionStepsApplySequentially) {
  // Jet_pt_raw = [100]; jec_sf = [2.0]; jes_up_sf = [1.1]
  // Step 1: Jet_pt_jec   = 100 * 2.0 = 200
  // Step 2: Jet_pt_jes_up = 200 * 1.1 = 220
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",  [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass_raw",[](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "jec_sf",      [](ULong64_t) { return   2.0f; });
  defineRVecColumn(*dm, "jes_up_sf",   [](ULong64_t) { return   1.1f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jec_sf",    "Jet_pt_jec");
  mgr->applyCorrection("Jet_pt_jec", "jes_up_sf", "Jet_pt_jes_up");
  mgr->execute();

  auto ptJec =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_jec");
  auto ptJesUp =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_jes_up");

  EXPECT_FLOAT_EQ(ptJec.GetValue()[0][0],   200.0f);
  EXPECT_FLOAT_EQ(ptJesUp.GetValue()[0][0], 220.0f);
}

// ---------------------------------------------------------------------------
// Systematic variation registration
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest,
       AddVariationRegistersUpAndDownWithSystematicManager) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass_raw",  [](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "jes_up_sf",     [](ULong64_t) { return   1.1f; });
  defineRVecColumn(*dm, "jes_dn_sf",     [](ULong64_t) { return   0.9f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jes_up_sf", "Jet_pt_jes_up");
  mgr->applyCorrection("Jet_pt_raw", "jes_dn_sf", "Jet_pt_jes_dn");
  mgr->addVariation("jesTotal", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->execute();

  const auto &systs = systematicManager->getSystematics();
  EXPECT_NE(systs.find("jesTotalUp"),   systs.end());
  EXPECT_NE(systs.find("jesTotalDown"), systs.end());
}

TEST_F(JetEnergyScaleManagerTest,
       VariationColumnsAreRegisteredAsAffectedVariables) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw", [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass_raw", [](ULong64_t) { return 10.0f; });
  defineRVecColumn(*dm, "jer_sf_up",  [](ULong64_t) { return 1.2f; });
  defineRVecColumn(*dm, "jer_sf_dn",  [](ULong64_t) { return 0.8f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jer_sf_up", "Jet_pt_jer_up");
  mgr->applyCorrection("Jet_pt_raw", "jer_sf_dn", "Jet_pt_jer_dn");
  mgr->addVariation("jer", "Jet_pt_jer_up", "Jet_pt_jer_dn");
  mgr->execute();

  // "jerUp" systematic should list Jet_pt_jer_up as an affected variable.
  const auto &affUp =
      systematicManager->getVariablesForSystematic("jerUp");
  EXPECT_NE(affUp.find("Jet_pt_jer_up"), affUp.end());

  const auto &affDn =
      systematicManager->getVariablesForSystematic("jerDown");
  EXPECT_NE(affDn.find("Jet_pt_jer_dn"), affDn.end());
}

// ---------------------------------------------------------------------------
// Systematic variation with mass columns
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest,
       VariationWithMassRegistersAllAffectedColumns) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",   [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass_raw", [](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "sf_up",        [](ULong64_t) { return   1.1f; });
  defineRVecColumn(*dm, "sf_dn",        [](ULong64_t) { return   0.9f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "sf_up", "Jet_pt_jes_up");
  mgr->applyCorrection("Jet_pt_raw", "sf_dn", "Jet_pt_jes_dn");
  mgr->addVariation("jesTotal",
                    "Jet_pt_jes_up",  "Jet_pt_jes_dn",
                    "Jet_mass_jes_up","Jet_mass_jes_dn");
  mgr->execute();

  const auto &affUp =
      systematicManager->getVariablesForSystematic("jesTotalUp");
  EXPECT_NE(affUp.find("Jet_pt_jes_up"),   affUp.end());
  EXPECT_NE(affUp.find("Jet_mass_jes_up"), affUp.end());

  const auto &affDn =
      systematicManager->getVariablesForSystematic("jesTotalDown");
  EXPECT_NE(affDn.find("Jet_pt_jes_dn"),   affDn.end());
  EXPECT_NE(affDn.find("Jet_mass_jes_dn"), affDn.end());
}

// ---------------------------------------------------------------------------
// Full workflow: raw pT → JEC → JES variation → systematic registration
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, FullWorkflowRawToJECToJESVariation) {
  // 2 events
  // Jet_pt = [100, 200], Jet_rawFactor = [0.1, 0.2]
  // → Jet_pt_raw = [90, 160]
  // jec_sf = [1.05, 1.05] → Jet_pt_jec = [94.5, 168]
  // jes_up_sf = [1.1, 1.1] → Jet_pt_jes_up = [103.95, 184.8]
  // jes_dn_sf = [0.9, 0.9] → Jet_pt_jes_dn = [85.05, 151.2]
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 100); });
  defineRVecColumn(*dm, "Jet_mass",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 10); });
  defineRVecColumn(*dm, "Jet_rawFactor",
      [](ULong64_t i) { return static_cast<Float_t>((i + 1) * 0.1f); });
  defineRVecColumn(*dm, "jec_sf",
      [](ULong64_t) { return 1.05f; });
  defineRVecColumn(*dm, "jes_up_sf",
      [](ULong64_t) { return 1.1f; });
  defineRVecColumn(*dm, "jes_dn_sf",
      [](ULong64_t) { return 0.9f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->removeExistingCorrections("Jet_rawFactor");
  mgr->applyCorrection("Jet_pt_raw", "jec_sf",    "Jet_pt_jec");
  mgr->applyCorrection("Jet_pt_jec", "jes_up_sf", "Jet_pt_jes_up");
  mgr->applyCorrection("Jet_pt_jec", "jes_dn_sf", "Jet_pt_jes_dn");
  mgr->addVariation("jesTotal", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->execute();

  // Verify raw pT
  auto ptRaw =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_raw");
  EXPECT_FLOAT_EQ(ptRaw.GetValue()[0][0],  90.0f);
  EXPECT_FLOAT_EQ(ptRaw.GetValue()[1][0], 160.0f);

  // Verify JEC pT
  auto ptJec =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_jec");
  EXPECT_FLOAT_EQ(ptJec.GetValue()[0][0],  94.5f);
  EXPECT_FLOAT_EQ(ptJec.GetValue()[1][0], 168.0f);

  // Verify JES up pT
  auto ptJesUp =
      dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("Jet_pt_jes_up");
  EXPECT_FLOAT_EQ(ptJesUp.GetValue()[0][0], 94.5f * 1.1f);

  // Verify systematic registered
  EXPECT_NE(systematicManager->getSystematics().find("jesTotalUp"),
            systematicManager->getSystematics().end());
  EXPECT_NE(systematicManager->getSystematics().find("jesTotalDown"),
            systematicManager->getSystematics().end());
}

// ---------------------------------------------------------------------------
// reportMetadata – does not throw after various configurations
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ReportMetadataAfterFullSetupDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt",         [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass",       [](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "Jet_rawFactor",  [](ULong64_t) { return   0.1f; });
  defineRVecColumn(*dm, "sf_nom",         [](ULong64_t) { return   1.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->removeExistingCorrections("Jet_rawFactor");
  mgr->applyCorrection("Jet_pt_raw", "sf_nom", "Jet_pt_jec");
  mgr->addVariation("testSyst", "Jet_pt_jec", "Jet_pt_jec");
  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceEmptyBeforeSetup) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_TRUE(mgr->collectProvenanceEntries().empty());
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsJetColumns) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("jet_pt_column"),   entries.end());
  ASSERT_NE(entries.find("jet_mass_column"), entries.end());
  EXPECT_EQ(entries.at("jet_pt_column"),   "Jet_pt");
  EXPECT_EQ(entries.at("jet_mass_column"), "Jet_mass");
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsRawFactor) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->removeExistingCorrections("Jet_rawFactor");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("raw_factor_column"), entries.end());
  EXPECT_EQ(entries.at("raw_factor_column"), "Jet_rawFactor");
  ASSERT_NE(entries.find("raw_pt_column"), entries.end());
  EXPECT_EQ(entries.at("raw_pt_column"), "Jet_pt_raw");
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsCorrectionSteps) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->applyCorrection("Jet_pt_raw", "jec_sf", "Jet_pt_jec",
                        /*applyToMass=*/false);

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("correction_steps"), entries.end());
  const std::string &steps = entries.at("correction_steps");
  EXPECT_NE(steps.find("Jet_pt_raw"),  std::string::npos);
  EXPECT_NE(steps.find("Jet_pt_jec"),  std::string::npos);
  EXPECT_NE(steps.find("jec_sf"),      std::string::npos);
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsVariations) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->addVariation("jesTotal", "Jet_pt_jes_up", "Jet_pt_jes_dn");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("variations"), entries.end());
  const std::string &vars = entries.at("variations");
  EXPECT_NE(vars.find("jesTotal"),       std::string::npos);
  EXPECT_NE(vars.find("Jet_pt_jes_up"), std::string::npos);
  EXPECT_NE(vars.find("Jet_pt_jes_dn"), std::string::npos);
}

// ---------------------------------------------------------------------------
// deriveMassColumnName – edge cases via applyCorrection
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest,
       ApplyCorrectionInfersMassColumnsWhenInputPtHasPtPrefix) {
  // When inputPtColumn starts with ptColumn_m, mass columns should be derived.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt_raw",   [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_mass_raw", [](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "jec_sf",       [](ULong64_t) { return   1.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  // "Jet_pt_raw" starts with "Jet_pt", so "Jet_mass_raw" should be inferred.
  mgr->applyCorrection("Jet_pt_raw", "jec_sf", "Jet_pt_jec");
  mgr->execute();

  auto cols = dm->getDataFrame().GetColumnNames();
  EXPECT_NE(std::find(cols.begin(), cols.end(), "Jet_mass_jec"), cols.end());
}

TEST_F(JetEnergyScaleManagerTest,
       ApplyCorrectionSkipsMassWhenPtColDoesNotMatchPrefix) {
  // When inputPtColumn does NOT start with ptColumn_m, mass derivation fails
  // and no mass column should be created.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "custom_pt",  [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "jec_sf",     [](ULong64_t) { return   1.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  // "custom_pt" does not start with "Jet_pt" so mass derivation yields "".
  mgr->applyCorrection("custom_pt", "jec_sf", "custom_pt_jec");
  mgr->execute();

  auto cols = dm->getDataFrame().GetColumnNames();
  EXPECT_NE(std::find(cols.begin(), cols.end(), "custom_pt_jec"), cols.end());
  // No mass column should be created with an inferred name.
  EXPECT_EQ(std::find(cols.begin(), cols.end(), "Jet_mass_jec"), cols.end());
}

// ---------------------------------------------------------------------------
// setMETColumns validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, SetMETColumnsStoresNames) {
  JetEnergyScaleManager mgr;
  mgr.setMETColumns("MET_pt", "MET_phi");
  EXPECT_EQ(mgr.getMETPtColumn(),  "MET_pt");
  EXPECT_EQ(mgr.getMETPhiColumn(), "MET_phi");
}

TEST_F(JetEnergyScaleManagerTest, SetMETColumnsWithEmptyPtThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setMETColumns("", "MET_phi"), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, SetMETColumnsWithEmptyPhiThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setMETColumns("MET_pt", ""), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, DefaultMETColumnsAreEmpty) {
  JetEnergyScaleManager mgr;
  EXPECT_TRUE(mgr.getMETPtColumn().empty());
  EXPECT_TRUE(mgr.getMETPhiColumn().empty());
}

// ---------------------------------------------------------------------------
// registerSystematicSources / getSystematicSources
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, RegisterAndRetrieveSystematicSources) {
  JetEnergyScaleManager mgr;
  mgr.registerSystematicSources("full", {"AbsoluteCal", "FlavorQCD", "PileUpDataMC"});
  const auto &sources = mgr.getSystematicSources("full");
  ASSERT_EQ(sources.size(), 3u);
  EXPECT_EQ(sources[0], "AbsoluteCal");
  EXPECT_EQ(sources[1], "FlavorQCD");
  EXPECT_EQ(sources[2], "PileUpDataMC");
}

TEST_F(JetEnergyScaleManagerTest, RegisterSystematicSourcesReplacesExistingSet) {
  JetEnergyScaleManager mgr;
  mgr.registerSystematicSources("reduced", {"Total"});
  mgr.registerSystematicSources("reduced", {"TotalUp", "TotalDown"});
  EXPECT_EQ(mgr.getSystematicSources("reduced").size(), 2u);
}

TEST_F(JetEnergyScaleManagerTest, RegisterSystematicSourcesMultipleSets) {
  JetEnergyScaleManager mgr;
  mgr.registerSystematicSources("full",    {"AbsoluteCal", "FlavorQCD"});
  mgr.registerSystematicSources("reduced", {"Total"});
  EXPECT_EQ(mgr.getSystematicSources("full").size(),    2u);
  EXPECT_EQ(mgr.getSystematicSources("reduced").size(), 1u);
}

TEST_F(JetEnergyScaleManagerTest, RegisterSystematicSourcesEmptyNameThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.registerSystematicSources("", {"src"}), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, RegisterSystematicSourcesEmptyListThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.registerSystematicSources("full", {}), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, RegisterSystematicSourcesEmptySourceNameThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.registerSystematicSources("full", {"ok", ""}),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, GetSystematicSourcesUnknownSetThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.getSystematicSources("nonexistent"), std::runtime_error);
}

// ---------------------------------------------------------------------------
// applySystematicSet validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ApplySystematicSetUnknownSetThrows) {
  auto dm = std::make_unique<DataManager>(1);
  // No CorrectionManager needed since we throw before using it.
  // Use a minimal CM with no corrections registered to avoid constructor errors.
  std::unique_ptr<IConfigurationProvider> cm_cfg =
      ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  auto cm = std::make_unique<CorrectionManager>(*cm_cfg);

  JetEnergyScaleManager mgr;
  auto ctx = makeContext(*config, *dm, *systematicManager, *logger,
                         *skimSink, *metaSink);
  mgr.setContext(ctx);
  mgr.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");

  EXPECT_THROW(
      mgr.applySystematicSet(*cm, "jes_unc", "not_registered",
                             "Jet_pt_jec", "Jet_pt_jes"),
      std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, ApplySystematicSetEmptyPrefixThrows) {
  JetEnergyScaleManager mgr;
  std::unique_ptr<IConfigurationProvider> cm_cfg =
      ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  auto cm = std::make_unique<CorrectionManager>(*cm_cfg);
  mgr.registerSystematicSources("reduced", {"Total"});

  EXPECT_THROW(
      mgr.applySystematicSet(*cm, "jes_unc", "reduced", "Jet_pt_jec", ""),
      std::invalid_argument);
}

// ---------------------------------------------------------------------------
// propagateMET validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, PropagateMETWithoutJetColumnsThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  // phiColumn_m is empty because setJetColumns was not called.
  EXPECT_THROW(
      mgr->propagateMET("MET_pt", "MET_phi",
                        "Jet_pt_raw", "Jet_pt_jec",
                        "MET_pt_jec", "MET_phi_jec"),
      std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETWithEmptyBasePtThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_THROW(
      mgr->propagateMET("", "MET_phi",
                        "Jet_pt_raw", "Jet_pt_jec",
                        "MET_pt_jec", "MET_phi_jec"),
      std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETWithEmptyOutputThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  EXPECT_THROW(
      mgr->propagateMET("MET_pt", "MET_phi",
                        "Jet_pt_raw", "Jet_pt_jec",
                        "", "MET_phi_jec"),
      std::invalid_argument);
}

// ---------------------------------------------------------------------------
// propagateMET – Type-1 MET propagation correctness
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, PropagateMETNominalCorrectionNoJetChange) {
  // When nominal == varied, MET should be unchanged.
  // 1 event, 1 jet: MET_pt = 50, MET_phi = 0, Jet_pt_nominal = 100,
  // Jet_pt_varied = 100 (no change), Jet_phi = 0.
  // dMET = 0 → MET_pt_out = 50, MET_phi_out = 0.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 50.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  defineRVecColumn(*dm, "Jet_phi",       [](ULong64_t) { return 0.0f; });
  defineRVecColumn(*dm, "Jet_pt_nom",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_varied", [](ULong64_t) { return 100.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_nom", "Jet_pt_varied",
                    "MET_pt_out", "MET_phi_out");
  mgr->execute();

  auto ptResult  = dm->getDataFrame().Take<Float_t>("MET_pt_out");
  auto phiResult = dm->getDataFrame().Take<Float_t>("MET_phi_out");
  EXPECT_FLOAT_EQ(ptResult.GetValue()[0],  50.0f);
  EXPECT_FLOAT_EQ(phiResult.GetValue()[0], 0.0f);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETJetPtIncreaseReducesMET) {
  // 1 event, 1 jet pointing in same direction as MET.
  // MET_pt = 100, MET_phi = 0.
  // Jet_phi = 0 (collinear with MET).
  // Jet_pt_nom = 50, Jet_pt_varied = 60 (increase by 10).
  // dMET_x = -(60-50)*cos(0) = -10
  // new_MET_x = 100*cos(0) + (-10) = 100 - 10 = 90
  // new_MET_y = 100*sin(0) = 0
  // new_MET_pt = 90, new_MET_phi = 0
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 100.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  defineRVecColumn(*dm, "Jet_phi",       [](ULong64_t) { return 0.0f; });
  defineRVecColumn(*dm, "Jet_pt_nom",    [](ULong64_t) { return 50.0f; });
  defineRVecColumn(*dm, "Jet_pt_varied", [](ULong64_t) { return 60.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_nom", "Jet_pt_varied",
                    "MET_pt_out", "MET_phi_out");
  mgr->execute();

  auto ptResult  = dm->getDataFrame().Take<Float_t>("MET_pt_out");
  auto phiResult = dm->getDataFrame().Take<Float_t>("MET_phi_out");
  EXPECT_FLOAT_EQ(ptResult.GetValue()[0],  90.0f);
  EXPECT_FLOAT_EQ(phiResult.GetValue()[0], 0.0f);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETJetPtDecreaseIncreasesMET) {
  // Jet pT decreases: MET should increase in jet direction.
  // MET_pt = 50, MET_phi = 0.
  // Jet_phi = 0, Jet_pt_nom = 80, Jet_pt_varied = 60.
  // dMET_x = -(60-80)*cos(0) = -(-20) = 20
  // new_MET_x = 50 + 20 = 70, new_MET_pt = 70, new_MET_phi = 0
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 50.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  defineRVecColumn(*dm, "Jet_phi",       [](ULong64_t) { return 0.0f; });
  defineRVecColumn(*dm, "Jet_pt_nom",    [](ULong64_t) { return 80.0f; });
  defineRVecColumn(*dm, "Jet_pt_varied", [](ULong64_t) { return 60.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_nom", "Jet_pt_varied",
                    "MET_pt_out", "MET_phi_out");
  mgr->execute();

  auto ptResult = dm->getDataFrame().Take<Float_t>("MET_pt_out");
  EXPECT_FLOAT_EQ(ptResult.GetValue()[0], 70.0f);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETPtThresholdExcludesLowPtJets) {
  // Jet below threshold should NOT contribute to MET propagation.
  // MET_pt = 100, MET_phi = 0.
  // 2 jets: phi=0, pt_nom=[10, 50], pt_varied=[20, 50].
  // With threshold=15: only jet[0] is above threshold in nominal? No:
  //   jet[0].nom = 10 < 15 → excluded; jet[1].nom = 50 ≥ 15 → delta = 0.
  // → MET unchanged = 100.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 100.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  // 2 jets per event
  dm->Define("Jet_phi",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {0.0f, 0.0f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_pt_nom",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {10.0f, 50.0f}; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("Jet_pt_varied",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {20.0f, 50.0f}; },
             {"rdfentry_"}, *systematicManager);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  // Default threshold=15: jet[0].nom=10 < 15, excluded
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_nom", "Jet_pt_varied",
                    "MET_pt_out", "MET_phi_out");
  mgr->execute();

  auto ptResult = dm->getDataFrame().Take<Float_t>("MET_pt_out");
  EXPECT_FLOAT_EQ(ptResult.GetValue()[0], 100.0f);
}

TEST_F(JetEnergyScaleManagerTest, PropagateMETPhiChangeWithPerpendicularJet) {
  // Jet perpendicular to MET direction.
  // MET_pt = 100, MET_phi = 0.
  // Jet_phi = pi/2 (pointing along +y), pt_nom = 40, pt_varied = 50.
  // dMET_x = -(50-40)*cos(pi/2) = 0
  // dMET_y = -(50-40)*sin(pi/2) = -10
  // new_MET_x = 100, new_MET_y = -10
  // new_MET_pt = sqrt(100^2 + 10^2) = sqrt(10100) ≈ 100.499
  // new_MET_phi = atan2(-10, 100) ≈ -0.0997
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  const float pi = static_cast<float>(M_PI);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 100.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  defineRVecColumn(*dm, "Jet_phi",
      [pi](ULong64_t) { return pi / 2.0f; });
  defineRVecColumn(*dm, "Jet_pt_nom",    [](ULong64_t) { return 40.0f; });
  defineRVecColumn(*dm, "Jet_pt_varied", [](ULong64_t) { return 50.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_nom", "Jet_pt_varied",
                    "MET_pt_out", "MET_phi_out");
  mgr->execute();

  auto ptResult  = dm->getDataFrame().Take<Float_t>("MET_pt_out");
  auto phiResult = dm->getDataFrame().Take<Float_t>("MET_phi_out");

  const float expectedPt  = std::sqrt(100.0f * 100.0f + 10.0f * 10.0f);
  const float expectedPhi = std::atan2(-10.0f, 100.0f);
  EXPECT_NEAR(ptResult.GetValue()[0],  expectedPt,  1e-3f);
  EXPECT_NEAR(phiResult.GetValue()[0], expectedPhi, 1e-4f);
}

// ---------------------------------------------------------------------------
// Multiple MET propagation steps (chained)
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ChainedMETPropagationSteps) {
  // Step 1: nominal JEC. Step 2: JES up variation.
  // MET_pt = 100, MET_phi = 0.
  // Step 1: jet[0] pt_nom=50 → pt_jec=60. dMET_x = -(60-50)*1 = -10.
  //         MET_jec_pt = 90, MET_jec_phi = 0.
  // Step 2: jet[0] pt_jec=60 → pt_jes_up=66. dMET_x = -(66-60)*1 = -6.
  //         MET_jes_up_pt = 90 - 6 = 84.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 100.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);
  defineRVecColumn(*dm, "Jet_phi",        [](ULong64_t) { return 0.0f; });
  defineRVecColumn(*dm, "Jet_pt_raw",     [](ULong64_t) { return 50.0f; });
  defineRVecColumn(*dm, "Jet_pt_jec",     [](ULong64_t) { return 60.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up",  [](ULong64_t) { return 66.0f; });

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  // Step 1: raw → jec
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_raw", "Jet_pt_jec",
                    "MET_pt_jec", "MET_phi_jec");
  // Step 2: jec → jes_up
  mgr->propagateMET("MET_pt_jec", "MET_phi_jec",
                    "Jet_pt_jec", "Jet_pt_jes_up",
                    "MET_pt_jes_up", "MET_phi_jes_up");
  mgr->execute();

  auto ptJec    = dm->getDataFrame().Take<Float_t>("MET_pt_jec");
  auto ptJesUp  = dm->getDataFrame().Take<Float_t>("MET_pt_jes_up");
  EXPECT_FLOAT_EQ(ptJec.GetValue()[0],   90.0f);
  EXPECT_FLOAT_EQ(ptJesUp.GetValue()[0], 84.0f);
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries – new fields
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsMETColumns) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setMETColumns("MET_pt", "MET_phi");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("met_pt_column"),  entries.end());
  ASSERT_NE(entries.find("met_phi_column"), entries.end());
  EXPECT_EQ(entries.at("met_pt_column"),  "MET_pt");
  EXPECT_EQ(entries.at("met_phi_column"), "MET_phi");
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsSystematicSets) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->registerSystematicSources("reduced", {"Total"});
  mgr->registerSystematicSources("full",    {"AbsoluteCal", "FlavorQCD"});

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("systematic_sets"), entries.end());
  const std::string &sets = entries.at("systematic_sets");
  EXPECT_NE(sets.find("reduced"),     std::string::npos);
  EXPECT_NE(sets.find("Total"),       std::string::npos);
  EXPECT_NE(sets.find("full"),        std::string::npos);
  EXPECT_NE(sets.find("AbsoluteCal"), std::string::npos);
  EXPECT_NE(sets.find("FlavorQCD"),   std::string::npos);
}

TEST_F(JetEnergyScaleManagerTest, CollectProvenanceContainsMETPropagationSteps) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_raw", "Jet_pt_jec",
                    "MET_pt_jec", "MET_phi_jec");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("met_propagation_steps"), entries.end());
  const std::string &steps = entries.at("met_propagation_steps");
  EXPECT_NE(steps.find("Jet_pt_raw"),  std::string::npos);
  EXPECT_NE(steps.find("Jet_pt_jec"),  std::string::npos);
  EXPECT_NE(steps.find("MET_pt_jec"),  std::string::npos);
}

// ---------------------------------------------------------------------------
// reportMetadata – new fields logged without throwing
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, ReportMetadataWithAllFeaturesDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Jet_pt",        [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_eta",       [](ULong64_t) { return   0.5f; });
  defineRVecColumn(*dm, "Jet_phi",       [](ULong64_t) { return   1.0f; });
  defineRVecColumn(*dm, "Jet_mass",      [](ULong64_t) { return  10.0f; });
  defineRVecColumn(*dm, "Jet_rawFactor", [](ULong64_t) { return   0.1f; });
  defineRVecColumn(*dm, "sf_nom",        [](ULong64_t) { return   1.0f; });
  dm->Define("MET_pt",  [](ULong64_t) -> Float_t { return 50.0f; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("MET_phi", [](ULong64_t) -> Float_t { return 0.0f; },
             {"rdfentry_"}, *systematicManager);

  mgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  mgr->setMETColumns("MET_pt", "MET_phi");
  mgr->registerSystematicSources("reduced", {"Total"});
  mgr->removeExistingCorrections("Jet_rawFactor");
  mgr->applyCorrection("Jet_pt_raw", "sf_nom", "Jet_pt_jec");
  mgr->addVariation("testSyst", "Jet_pt_jec", "Jet_pt_jec");
  mgr->propagateMET("MET_pt", "MET_phi",
                    "Jet_pt_raw", "Jet_pt_jec",
                    "MET_pt_jec", "MET_phi_jec");
  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ---------------------------------------------------------------------------
// setInputJetCollection validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, SetInputJetCollectionStoresName) {
  JetEnergyScaleManager mgr;
  mgr.setInputJetCollection("goodJets");
  EXPECT_EQ(mgr.getInputJetCollectionColumn(), "goodJets");
}

TEST_F(JetEnergyScaleManagerTest, SetInputJetCollectionEmptyThrows) {
  JetEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setInputJetCollection(""), std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, DefaultInputJetCollectionIsEmpty) {
  JetEnergyScaleManager mgr;
  EXPECT_TRUE(mgr.getInputJetCollectionColumn().empty());
}

// ---------------------------------------------------------------------------
// defineCollectionOutput validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputWithoutSetInputThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  // setInputJetCollection not called
  EXPECT_THROW(mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec"),
               std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputEmptyPtColumnThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  EXPECT_THROW(mgr->defineCollectionOutput("", "goodJets_jec"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputEmptyOutputColumnThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  EXPECT_THROW(mgr->defineCollectionOutput("Jet_pt_jec", ""),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// defineVariationCollections validation
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsWithoutSetInputThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(
      mgr->defineVariationCollections("goodJets_jec", "goodJets"),
      std::runtime_error);
}

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsEmptyNominalThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  EXPECT_THROW(mgr->defineVariationCollections("", "goodJets"),
               std::invalid_argument);
}

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsEmptyPrefixThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  EXPECT_THROW(mgr->defineVariationCollections("goodJets_jec", ""),
               std::invalid_argument);
}

// ---------------------------------------------------------------------------
// defineCollectionOutput – correctness in execute()
// ---------------------------------------------------------------------------

/// Helper: build a PhysicsObjectCollection in the dataframe.
/// Defines @p colName column: single jet with given pT, eta=1.0, phi=0.3, mass=10.
static void defineJetCollection(DataManager &dm,
                                 SystematicManager &sm,
                                 const std::string &colName,
                                 float jetPt) {
  dm.Define(
      colName,
      [jetPt](ULong64_t) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<Float_t> pt   = {jetPt};
        ROOT::VecOps::RVec<Float_t> eta  = {1.0f};
        ROOT::VecOps::RVec<Float_t> phi  = {0.3f};
        ROOT::VecOps::RVec<Float_t> mass = {10.0f};
        ROOT::VecOps::RVec<bool>    mask = {true};
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
      },
      {"rdfentry_"}, sm);
}

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputPtOnly) {
  // After applying a corrected-pT column via defineCollectionOutput,
  // the output PhysicsObjectCollection should have the updated pT.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  // Input jet collection: 1 jet with pT=100
  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  // Corrected pT column: pT=110 (scale factor 1.1)
  defineRVecColumn(*dm, "Jet_pt_jec", [](ULong64_t) { return 110.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_jec");
  const PhysicsObjectCollection &col = result.GetValue()[0];
  ASSERT_EQ(col.size(), 1u);
  EXPECT_NEAR(static_cast<float>(col.at(0).Pt()), 110.0f, 0.01f);
  // Eta and phi should be preserved
  EXPECT_NEAR(static_cast<float>(col.at(0).Eta()), 1.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(col.at(0).Phi()), 0.3f, 0.01f);
}

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputPtAndMass) {
  // With a corrected-mass column, both pT and mass should be updated.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",   [](ULong64_t) { return 110.0f; });
  defineRVecColumn(*dm, "Jet_mass_jec", [](ULong64_t) { return  11.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec", "Jet_mass_jec");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_jec");
  const PhysicsObjectCollection &col = result.GetValue()[0];
  ASSERT_EQ(col.size(), 1u);
  EXPECT_NEAR(static_cast<float>(col.at(0).Pt()),   110.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(col.at(0).M()),     11.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(col.at(0).Eta()),    1.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(col.at(0).Phi()),    0.3f, 0.01f);
}

TEST_F(JetEnergyScaleManagerTest, DefineCollectionOutputIndexPreserved) {
  // The collection should preserve the original index (0 for the only jet).
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec", [](ULong64_t) { return 110.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->execute();

  auto result = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_jec");
  const PhysicsObjectCollection &col = result.GetValue()[0];
  ASSERT_EQ(col.size(), 1u);
  EXPECT_EQ(col.index(0), 0);  // original index preserved
}

// ---------------------------------------------------------------------------
// defineVariationCollections – correctness in execute()
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsCreatesUpDownColumns) {
  // Registers a single variation "jesTest" and verifies that both
  // "goodJets_jesTestUp" and "goodJets_jesTestDown" columns are produced.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up", [](ULong64_t) { return 105.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_dn", [](ULong64_t) { return  95.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("jesTest", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets");
  mgr->execute();

  auto cols = dm->getDataFrame().GetColumnNames();
  EXPECT_NE(std::find(cols.begin(), cols.end(), "goodJets_jesTestUp"),   cols.end());
  EXPECT_NE(std::find(cols.begin(), cols.end(), "goodJets_jesTestDown"), cols.end());
}

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsUpPtCorrect) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up", [](ULong64_t) { return 108.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_dn", [](ULong64_t) { return  92.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("jesTest", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets");
  mgr->execute();

  auto upResult = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_jesTestUp");
  auto dnResult = dm->getDataFrame().Take<PhysicsObjectCollection>("goodJets_jesTestDown");

  EXPECT_NEAR(static_cast<float>(upResult.GetValue()[0].at(0).Pt()), 108.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(dnResult.GetValue()[0].at(0).Pt()),  92.0f, 0.01f);
}

// ---------------------------------------------------------------------------
// defineVariationCollections with variation map
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsMapContainsNominal) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up", [](ULong64_t) { return 105.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_dn", [](ULong64_t) { return  95.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("jesTest", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets", "goodJets_varMap");
  mgr->execute();

  auto mapResult = dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodJets_varMap");
  const PhysicsObjectVariationMap &vm = mapResult.GetValue()[0];
  EXPECT_NE(vm.find("nominal"),      vm.end());
  EXPECT_NE(vm.find("jesTestUp"),    vm.end());
  EXPECT_NE(vm.find("jesTestDown"),  vm.end());
}

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsMapNominalPt) {
  // The "nominal" entry in the map should have the nominal pT.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up", [](ULong64_t) { return 105.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_dn", [](ULong64_t) { return  95.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("jesTest", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets", "goodJets_varMap");
  mgr->execute();

  auto mapResult = dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodJets_varMap");
  const PhysicsObjectVariationMap &vm = mapResult.GetValue()[0];
  EXPECT_NEAR(static_cast<float>(vm.at("nominal").at(0).Pt()),     100.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(vm.at("jesTestUp").at(0).Pt()),   105.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(vm.at("jesTestDown").at(0).Pt()),  95.0f, 0.01f);
}

TEST_F(JetEnergyScaleManagerTest, DefineVariationCollectionsMapMultipleVariations) {
  // Multiple variations should all appear in the map.
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",      [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_var1_up",  [](ULong64_t) { return 103.0f; });
  defineRVecColumn(*dm, "Jet_pt_var1_dn",  [](ULong64_t) { return  97.0f; });
  defineRVecColumn(*dm, "Jet_pt_var2_up",  [](ULong64_t) { return 106.0f; });
  defineRVecColumn(*dm, "Jet_pt_var2_dn",  [](ULong64_t) { return  94.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("var1", "Jet_pt_var1_up", "Jet_pt_var1_dn");
  mgr->addVariation("var2", "Jet_pt_var2_up", "Jet_pt_var2_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets", "goodJets_varMap");
  mgr->execute();

  auto mapResult = dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodJets_varMap");
  const PhysicsObjectVariationMap &vm = mapResult.GetValue()[0];
  EXPECT_NE(vm.find("nominal"),   vm.end());
  EXPECT_NE(vm.find("var1Up"),    vm.end());
  EXPECT_NE(vm.find("var1Down"),  vm.end());
  EXPECT_NE(vm.find("var2Up"),    vm.end());
  EXPECT_NE(vm.find("var2Down"),  vm.end());
  EXPECT_EQ(vm.size(), 5u);
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries – PhysicsObjectCollection fields
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest,
       CollectProvenanceContainsInputJetCollectionColumn) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("input_jet_collection_column"), entries.end());
  EXPECT_EQ(entries.at("input_jet_collection_column"), "goodJets");
}

TEST_F(JetEnergyScaleManagerTest,
       CollectProvenanceContainsCollectionOutputSteps) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("collection_output_steps"), entries.end());
  const std::string &steps = entries.at("collection_output_steps");
  EXPECT_NE(steps.find("Jet_pt_jec"),  std::string::npos);
  EXPECT_NE(steps.find("goodJets_jec"), std::string::npos);
}

TEST_F(JetEnergyScaleManagerTest,
       CollectProvenanceContainsVariationCollectionSteps) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputJetCollection("goodJets");
  mgr->defineVariationCollections("goodJets_jec", "goodJets",
                                   "goodJets_varMap");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("variation_collection_steps"), entries.end());
  const std::string &steps = entries.at("variation_collection_steps");
  EXPECT_NE(steps.find("goodJets"),       std::string::npos);
  EXPECT_NE(steps.find("goodJets_jec"),   std::string::npos);
  EXPECT_NE(steps.find("goodJets_varMap"), std::string::npos);
}

// ---------------------------------------------------------------------------
// reportMetadata with collection fields
// ---------------------------------------------------------------------------

TEST_F(JetEnergyScaleManagerTest,
       ReportMetadataWithCollectionFeaturesDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineJetCollection(*dm, *systematicManager, "goodJets", 100.0f);
  defineRVecColumn(*dm, "Jet_pt_jec",    [](ULong64_t) { return 100.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_up", [](ULong64_t) { return 105.0f; });
  defineRVecColumn(*dm, "Jet_pt_jes_dn", [](ULong64_t) { return  95.0f; });

  mgr->setInputJetCollection("goodJets");
  mgr->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
  mgr->addVariation("jesTest", "Jet_pt_jes_up", "Jet_pt_jes_dn");
  mgr->defineVariationCollections("goodJets_jec", "goodJets", "goodJets_varMap");
  mgr->execute();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
