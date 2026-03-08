/**
 * @file testGoldenJsonManager.cc
 * @brief Unit tests for the GoldenJsonManager class
 * @date 2025
 *
 * Tests cover JSON loading, validity checks, dataframe filtering for data
 * samples, the no-op behaviour for MC samples, and multi-file merging.
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <GoldenJsonManager.h>
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
                                  DataManager &dm,
                                  SystematicManager &sm,
                                  DefaultLogger &log,
                                  NullOutputSink &skim,
                                  NullOutputSink &meta) {
  return ManagerContext{cfg, dm, sm, log, skim, meta};
}

// ---------------------------------------------------------------------------
// Fixture: loads golden JSON files and wires a DataManager
// ---------------------------------------------------------------------------

class GoldenJsonManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    configData = ManagerFactory::createConfigurationManager(
        "cfg/test_golden_json_config_data.txt");
    configMC = ManagerFactory::createConfigurationManager(
        "cfg/test_golden_json_config_mc.txt");
    systematicManager = std::make_unique<SystematicManager>();
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();
  }

  void TearDown() override {}

  // Wire a manager and call setupFromConfigFile().
  std::unique_ptr<GoldenJsonManager>
  makeManager(IConfigurationProvider &cfg, DataManager &dm) {
    auto mgr = std::make_unique<GoldenJsonManager>();
    auto ctx = makeContext(cfg, dm, *systematicManager, *logger, *skimSink,
                           *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
  }

  std::unique_ptr<IConfigurationProvider> configData;
  std::unique_ptr<IConfigurationProvider> configMC;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

// ---------------------------------------------------------------------------
// Construction and loading
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, ConstructorAndSetupDoNotThrow) {
  auto dm = std::make_unique<DataManager>(0);
  EXPECT_NO_THROW({ auto mgr = makeManager(*configData, *dm); });
}

TEST_F(GoldenJsonManagerTest, LoadsRunsFromMultipleFiles) {
  auto dm = std::make_unique<DataManager>(0);
  auto mgr = makeManager(*configData, *dm);
  // Runs from both golden_json_2022.json and golden_json_2023.json should be
  // loaded (355100, 355101, 362760 from 2022; 362760, 370000 from 2023).
  EXPECT_GE(mgr->numRuns(), std::size_t(4));
}

// ---------------------------------------------------------------------------
// isValid() correctness
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, IsValidForCertifiedPairs) {
  auto dm = std::make_unique<DataManager>(0);
  auto mgr = makeManager(*configData, *dm);

  // From golden_json_2022.json: run 355100, ranges [[1,100],[150,200]]
  EXPECT_TRUE(mgr->isValid(355100, 1));
  EXPECT_TRUE(mgr->isValid(355100, 50));
  EXPECT_TRUE(mgr->isValid(355100, 100));
  EXPECT_TRUE(mgr->isValid(355100, 150));
  EXPECT_TRUE(mgr->isValid(355100, 200));

  // From golden_json_2022.json: run 355101, range [[1,50]]
  EXPECT_TRUE(mgr->isValid(355101, 1));
  EXPECT_TRUE(mgr->isValid(355101, 50));

  // From golden_json_2023.json: run 370000, range [[1,200]]
  EXPECT_TRUE(mgr->isValid(370000, 1));
  EXPECT_TRUE(mgr->isValid(370000, 200));
}

TEST_F(GoldenJsonManagerTest, IsValidReturnsFalseForGaps) {
  auto dm = std::make_unique<DataManager>(0);
  auto mgr = makeManager(*configData, *dm);

  // Gap between lumi 100 and 150 for run 355100
  EXPECT_FALSE(mgr->isValid(355100, 101));
  EXPECT_FALSE(mgr->isValid(355100, 149));

  // Beyond end of range for run 355101
  EXPECT_FALSE(mgr->isValid(355101, 51));
  EXPECT_FALSE(mgr->isValid(355101, 200));
}

TEST_F(GoldenJsonManagerTest, IsValidReturnsFalseForUnknownRun) {
  auto dm = std::make_unique<DataManager>(0);
  auto mgr = makeManager(*configData, *dm);

  EXPECT_FALSE(mgr->isValid(999999, 1));
  EXPECT_FALSE(mgr->isValid(0, 0));
}

TEST_F(GoldenJsonManagerTest, RunPresentInBothFilesIsMerged) {
  // Run 362760 appears in both JSON files with non-overlapping ranges:
  //   golden_json_2022.json: [[1,500]]
  //   golden_json_2023.json: [[501,1000]]
  auto dm = std::make_unique<DataManager>(0);
  auto mgr = makeManager(*configData, *dm);

  EXPECT_TRUE(mgr->isValid(362760, 1));
  EXPECT_TRUE(mgr->isValid(362760, 500));
  EXPECT_TRUE(mgr->isValid(362760, 501));
  EXPECT_TRUE(mgr->isValid(362760, 1000));
  EXPECT_FALSE(mgr->isValid(362760, 1001));
}

// ---------------------------------------------------------------------------
// applyGoldenJson() – data sample (filter should be applied)
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, ApplyFiltersDataEvents) {
  // 6 events: 3 should pass, 3 should fail
  auto dm = std::make_unique<DataManager>(6);
  auto mgr = makeManager(*configData, *dm);

  // Define run and luminosityBlock columns.
  // Events 0-2: valid (run 355100, lumis 1, 50, 150)
  // Events 3-5: invalid (run 355100 lumi 101, run 999999 lumi 1, run 355101 lumi 60)
  dm->Define(
      "run",
      [](ULong64_t i) -> unsigned int {
        if (i == 0) return 355100;
        if (i == 1) return 355100;
        if (i == 2) return 355100;
        if (i == 3) return 355100;
        if (i == 4) return 999999;
        return 355101;
      },
      {"rdfentry_"}, *systematicManager);

  dm->Define(
      "luminosityBlock",
      [](ULong64_t i) -> unsigned int {
        if (i == 0) return 1;
        if (i == 1) return 50;
        if (i == 2) return 150;
        if (i == 3) return 101; // gap
        if (i == 4) return 1;   // unknown run
        return 60;              // beyond range of run 355101
      },
      {"rdfentry_"}, *systematicManager);

  mgr->applyGoldenJson();

  auto df = dm->getDataFrame();
  auto count = df.Count();
  EXPECT_EQ(count.GetValue(), 3ULL);
}

// ---------------------------------------------------------------------------
// applyGoldenJson() – MC sample (filter must be skipped)
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, ApplyIsNoOpForMCSample) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeManager(*configMC, *dm);

  dm->Define(
      "run",
      [](ULong64_t) -> unsigned int { return 999999; },
      {"rdfentry_"}, *systematicManager);
  dm->Define(
      "luminosityBlock",
      [](ULong64_t) -> unsigned int { return 9999; },
      {"rdfentry_"}, *systematicManager);

  // All events would fail the JSON filter, but type != "data" so no filter
  // should be applied.
  EXPECT_NO_THROW(mgr->applyGoldenJson());

  auto df = dm->getDataFrame();
  auto count = df.Count();
  EXPECT_EQ(count.GetValue(), 4ULL);
}

// ---------------------------------------------------------------------------
// Error handling
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, SetupThrowsWhenContextNotSet) {
  auto mgr = std::make_unique<GoldenJsonManager>();
  EXPECT_THROW(mgr->setupFromConfigFile(), std::runtime_error);
}

TEST_F(GoldenJsonManagerTest, ApplyThrowsWhenContextNotSet) {
  auto mgr = std::make_unique<GoldenJsonManager>();
  EXPECT_THROW(mgr->applyGoldenJson(), std::runtime_error);
}

TEST_F(GoldenJsonManagerTest, TypeStringReturnsExpectedValue) {
  GoldenJsonManager mgr;
  EXPECT_EQ(mgr.type(), "GoldenJsonManager");
}

// ---------------------------------------------------------------------------
// Lifecycle hook tests
// ---------------------------------------------------------------------------

TEST_F(GoldenJsonManagerTest, InitializeIsCallable) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeManager(*configData, *dm);
  EXPECT_NO_THROW(mgr->initialize());
}

TEST_F(GoldenJsonManagerTest, ReportMetadataIsCallable) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeManager(*configData, *dm);
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(GoldenJsonManagerTest, ExecuteAndFinalizeAreNoOps) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeManager(*configData, *dm);
  EXPECT_NO_THROW(mgr->execute());
  EXPECT_NO_THROW(mgr->finalize());
}

TEST_F(GoldenJsonManagerTest, GetDependenciesReturnsEmpty) {
  GoldenJsonManager mgr;
  EXPECT_TRUE(mgr.getDependencies().empty());
}

TEST_F(GoldenJsonManagerTest, InitializeReflectsLoadedRuns) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeManager(*configData, *dm);
  // After setup, some runs should be loaded and initialize should succeed
  EXPECT_GT(mgr->numRuns(), 0UL);
  EXPECT_NO_THROW(mgr->initialize());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
