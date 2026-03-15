/**
 * @file testJetEnergyScaleManager.cc
 * @brief Unit tests for the JetEnergyScaleManager plugin.
 *
 * Tests cover: jet column registration, raw-pT computation, correction step
 * scheduling, CMS correctionlib convenience API, systematic variation
 * registration, lifecycle hooks, and error handling.
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <JetEnergyScaleManager.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
