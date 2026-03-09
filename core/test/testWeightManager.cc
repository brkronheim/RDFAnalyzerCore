/**
 * @file testWeightManager.cc
 * @brief Unit tests for the WeightManager plugin.
 *
 * Tests cover: scale factor registration, normalization factors, weight
 * variation registration, nominal/varied column definition, audit statistics,
 * lifecycle hooks, and error handling.
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <WeightManager.h>
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

class WeightManagerTest : public ::testing::Test {
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

  /// Create, wire, and return a WeightManager attached to the given DataManager.
  std::unique_ptr<WeightManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<WeightManager>();
    auto ctx = makeContext(*config, dm, *systematicManager, *logger,
                           *skimSink, *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
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

TEST_F(WeightManagerTest, TypeStringIsCorrect) {
  WeightManager mgr;
  EXPECT_EQ(mgr.type(), "WeightManager");
}

TEST_F(WeightManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(WeightManager mgr);
}

// ---------------------------------------------------------------------------
// Lifecycle hooks on empty manager
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, ExecuteWithNoComponentsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->execute());
}

TEST_F(WeightManagerTest, FinalizeWithNoComponentsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  EXPECT_NO_THROW(mgr->finalize());
}

TEST_F(WeightManagerTest, EmptyManagerAuditEntriesAreEmpty) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  mgr->finalize();
  EXPECT_TRUE(mgr->getAuditEntries().empty());
}

TEST_F(WeightManagerTest, ReportMetadataOnEmptyManagerDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(WeightManagerTest, InitializeIsCallable) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->initialize());
}

TEST_F(WeightManagerTest, GetDependenciesReturnsEmpty) {
  WeightManager mgr;
  EXPECT_TRUE(mgr.getDependencies().empty());
}

// ---------------------------------------------------------------------------
// Normalization factors
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, TotalNormalizationDefaultsToOne) {
  WeightManager mgr;
  EXPECT_DOUBLE_EQ(mgr.getTotalNormalization(), 1.0);
}

TEST_F(WeightManagerTest, SingleNormalizationFactor) {
  WeightManager mgr;
  mgr.addNormalization("lumi", 2.5);
  EXPECT_DOUBLE_EQ(mgr.getTotalNormalization(), 2.5);
}

TEST_F(WeightManagerTest, MultipleNormalizationFactorsAreMultiplied) {
  WeightManager mgr;
  mgr.addNormalization("lumi",   3.0);
  mgr.addNormalization("xsec",   0.5);
  mgr.addNormalization("filter", 0.8);
  EXPECT_DOUBLE_EQ(mgr.getTotalNormalization(), 3.0 * 0.5 * 0.8);
}

TEST_F(WeightManagerTest, AddNormalizationWithEmptyNameThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addNormalization("", 1.0), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Scale factor registration
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, AddScaleFactorWithEmptyNameThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addScaleFactor("", "col"), std::invalid_argument);
}

TEST_F(WeightManagerTest, AddScaleFactorWithEmptyColumnThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addScaleFactor("sf", ""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Weight variation registration
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, AddWeightVariationWithEmptyNameThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addWeightVariation("", "up_col", "dn_col"), std::invalid_argument);
}

TEST_F(WeightManagerTest, AddWeightVariationWithEmptyUpColThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addWeightVariation("pileup", "", "dn_col"), std::invalid_argument);
}

TEST_F(WeightManagerTest, AddWeightVariationWithEmptyDownColThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.addWeightVariation("pileup", "up_col", ""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// defineNominalWeight / defineVariedWeight validation
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, DefineNominalWeightWithEmptyColumnThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.defineNominalWeight(""), std::invalid_argument);
}

TEST_F(WeightManagerTest, DefineVariedWeightWithEmptyVariationThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.defineVariedWeight("", "up", "col"), std::invalid_argument);
}

TEST_F(WeightManagerTest, DefineVariedWeightWithBadDirectionThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.defineVariedWeight("pileup", "bad", "col"), std::invalid_argument);
}

TEST_F(WeightManagerTest, DefineVariedWeightWithEmptyOutputColThrows) {
  WeightManager mgr;
  EXPECT_THROW(mgr.defineVariedWeight("pileup", "up", ""), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// Nominal weight column – scalar normalization only (no per-event SF)
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, NominalWeightWithNormOnly) {
  // 4 events; no per-event SF; normalization = 2.0.
  // Expected nominal weight for every event = 2.0.
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  mgr->addNormalization("scale", 2.0);
  mgr->defineNominalWeight("weight_nominal");

  mgr->execute();
  mgr->finalize();

  ASSERT_EQ(mgr->getNominalWeightColumn(), "weight_nominal");
  ASSERT_EQ(mgr->getAuditEntries().size(), 1u);
  const auto &e = mgr->getAuditEntries()[0];
  EXPECT_DOUBLE_EQ(e.sumWeights, 4 * 2.0);
  EXPECT_DOUBLE_EQ(e.meanWeight, 2.0);
  EXPECT_DOUBLE_EQ(e.minWeight,  2.0);
  EXPECT_DOUBLE_EQ(e.maxWeight,  2.0);
  EXPECT_EQ(e.negativeCount, 0LL);
  EXPECT_EQ(e.zeroCount, 0LL);
}

// ---------------------------------------------------------------------------
// Nominal weight column – single per-event SF × normalization
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, NominalWeightWithSingleScaleFactor) {
  // 4 events (indices 0-3); SF = (index + 1) * 0.25 → 0.25, 0.50, 0.75, 1.0.
  // Normalization = 2.0.
  // Expected weights: 0.50, 1.00, 1.50, 2.00.
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("my_sf",
             [](ULong64_t i) { return (static_cast<double>(i) + 1.0) * 0.25; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("my_sf", "my_sf");
  mgr->addNormalization("norm", 2.0);
  mgr->defineNominalWeight("weight_nominal");

  mgr->execute();
  mgr->finalize();

  ASSERT_EQ(mgr->getAuditEntries().size(), 1u);
  const auto &e = mgr->getAuditEntries()[0];
  // sum = 2*(0.25+0.50+0.75+1.0) = 2*2.5 = 5.0
  EXPECT_DOUBLE_EQ(e.sumWeights, 5.0);
  EXPECT_DOUBLE_EQ(e.meanWeight, 5.0 / 4.0);
  EXPECT_DOUBLE_EQ(e.minWeight,  0.50);
  EXPECT_DOUBLE_EQ(e.maxWeight,  2.00);
  EXPECT_EQ(e.negativeCount, 0LL);
}

// ---------------------------------------------------------------------------
// Nominal weight – two scale factors × normalization
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, NominalWeightWithTwoScaleFactors) {
  // 2 events; sf1 = [1.0, 2.0], sf2 = [0.5, 0.5]; norm = 1.0.
  // Weights: [0.5, 1.0].
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("sf1", [](ULong64_t i) { return (i == 0) ? 1.0 : 2.0; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("sf2", [](ULong64_t) { return 0.5; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("sf1", "sf1");
  mgr->addScaleFactor("sf2", "sf2");
  mgr->defineNominalWeight("weight_nominal");

  mgr->execute();
  mgr->finalize();

  ASSERT_EQ(mgr->getAuditEntries().size(), 1u);
  const auto &e = mgr->getAuditEntries()[0];
  EXPECT_DOUBLE_EQ(e.sumWeights, 1.5);
  EXPECT_DOUBLE_EQ(e.minWeight, 0.5);
  EXPECT_DOUBLE_EQ(e.maxWeight, 1.0);
}

// ---------------------------------------------------------------------------
// Negative-weight counting
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, NegativeWeightCounting) {
  // 3 events: sf = [-1.0, 0.0, 1.0].
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  dm->Define("sf_neg",
             [](ULong64_t i) { return static_cast<double>(i) - 1.0; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("sf_neg", "sf_neg");
  mgr->defineNominalWeight("weight_nominal");

  mgr->execute();
  mgr->finalize();

  ASSERT_EQ(mgr->getAuditEntries().size(), 1u);
  const auto &e = mgr->getAuditEntries()[0];
  EXPECT_EQ(e.negativeCount, 1LL); // only i=0 gives -1.0
  EXPECT_EQ(e.zeroCount,     1LL); // i=1 gives 0.0
}

// ---------------------------------------------------------------------------
// Varied weight columns
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, VariedWeightColumnNominalNotDefined) {
  // If no nominal column was scheduled, getNominalWeightColumn() returns "".
  WeightManager mgr;
  EXPECT_EQ(mgr.getNominalWeightColumn(), "");
}

TEST_F(WeightManagerTest, GetWeightColumnUnknownVariationReturnsEmpty) {
  WeightManager mgr;
  EXPECT_EQ(mgr.getWeightColumn("unknown", "up"), "");
}

TEST_F(WeightManagerTest, VariedWeightColumnDefinedAndAudited) {
  // 3 events; nominal sf = [1.0, 1.0, 1.0]; pileup up = [1.1, 1.2, 1.3].
  // norm = 1.0.
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  dm->Define("sf_nominal", [](ULong64_t) { return 1.0; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pu_up",
             [](ULong64_t i) { return 1.1 + static_cast<double>(i) * 0.1; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pu_down",
             [](ULong64_t i) { return 0.9 - static_cast<double>(i) * 0.05; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("pileup", "sf_nominal");
  mgr->addWeightVariation("pileup", "pu_up", "pu_down");
  mgr->defineNominalWeight("weight_nominal");
  mgr->defineVariedWeight("pileup", "up",   "weight_pileup_up");
  mgr->defineVariedWeight("pileup", "down", "weight_pileup_down");

  mgr->execute();

  EXPECT_EQ(mgr->getWeightColumn("pileup", "up"),   "weight_pileup_up");
  EXPECT_EQ(mgr->getWeightColumn("pileup", "down"), "weight_pileup_down");

  mgr->finalize();

  // Should have 3 audit entries: nominal, up, down.
  EXPECT_EQ(mgr->getAuditEntries().size(), 3u);
}

TEST_F(WeightManagerTest, VariedWeightExecuteThrowsOnUnknownVariation) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  // Schedule a varied column without registering the variation.
  mgr->defineVariedWeight("ghost_variation", "up", "weight_ghost_up");

  EXPECT_THROW(mgr->execute(), std::runtime_error);
}

// ---------------------------------------------------------------------------
// getWeightColumn returns correct column names
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, GetWeightColumnAfterDefineVariedWeight) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("sf1", [](ULong64_t) { return 1.0; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("sf1_up",   [](ULong64_t) { return 1.05; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("sf1_down", [](ULong64_t) { return 0.95; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("sf1", "sf1");
  mgr->addWeightVariation("sf1", "sf1_up", "sf1_down");
  mgr->defineNominalWeight("w_nom");
  mgr->defineVariedWeight("sf1", "up",   "w_sf1_up");
  mgr->defineVariedWeight("sf1", "down", "w_sf1_down");

  mgr->execute();

  EXPECT_EQ(mgr->getNominalWeightColumn(), "w_nom");
  EXPECT_EQ(mgr->getWeightColumn("sf1", "up"),   "w_sf1_up");
  EXPECT_EQ(mgr->getWeightColumn("sf1", "down"), "w_sf1_down");
}

// ---------------------------------------------------------------------------
// reportMetadata does not throw after a full run
// ---------------------------------------------------------------------------

TEST_F(WeightManagerTest, ReportMetadataAfterRunDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  dm->Define("sf", [](ULong64_t) { return 0.9; },
             {"rdfentry_"}, *systematicManager);

  mgr->addScaleFactor("sf", "sf");
  mgr->addNormalization("norm", 1.5);
  mgr->defineNominalWeight("weight_nominal");

  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
