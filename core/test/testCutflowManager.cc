/**
 * @file testCutflowManager.cc
 * @brief Unit tests for the CutflowManager plugin.
 *
 * Tests cover: addCut() mechanics, sequential cutflow counts,
 * N-1 counts, empty-cut behaviour, lifecycle hooks, and error handling.
 */

#include <ConfigurationManager.h>
#include <CutflowManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <RegionManager.h>
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

class CutflowManagerTest : public ::testing::Test {
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

  /// Create, wire, and return a CutflowManager.
  std::unique_ptr<CutflowManager>
  makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<CutflowManager>();
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

TEST_F(CutflowManagerTest, TypeStringIsCorrect) {
  CutflowManager mgr;
  EXPECT_EQ(mgr.type(), "CutflowManager");
}

TEST_F(CutflowManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(CutflowManager mgr);
}

// ---------------------------------------------------------------------------
// Empty cut list
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, ExecuteWithNoCutsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->execute());
}

TEST_F(CutflowManagerTest, FinalizeWithNoCutsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  EXPECT_NO_THROW(mgr->finalize());
}

TEST_F(CutflowManagerTest, EmptyCutsProduceEmptyResults) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  mgr->execute();
  mgr->finalize();
  EXPECT_TRUE(mgr->getCutflowCounts().empty());
  EXPECT_TRUE(mgr->getNMinusOneCounts().empty());
  EXPECT_EQ(mgr->getTotalCount(), 0ULL);
}

// ---------------------------------------------------------------------------
// addCut() before setContext() throws
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, AddCutWithoutContextThrows) {
  CutflowManager mgr;
  EXPECT_THROW(mgr.addCut("x", "pass_x"), std::runtime_error);
}

// ---------------------------------------------------------------------------
// Sequential cutflow: all events pass all cuts
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, CutflowAllEventsPassBothCuts) {
  // 5 events; both boolean columns are always true.
  auto dm = std::make_unique<DataManager>(5);
  auto mgr = makeMgr(*dm);

  // Define boolean columns (all-true).
  dm->Define("pass_cutA",
             [](ULong64_t) { return true; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutB",
             [](ULong64_t) { return true; },
             {"rdfentry_"}, *systematicManager);

  mgr->addCut("cutA", "pass_cutA");
  mgr->addCut("cutB", "pass_cutB");

  mgr->execute();
  mgr->finalize();

  EXPECT_EQ(mgr->getTotalCount(), 5ULL);
  ASSERT_EQ(mgr->getCutflowCounts().size(), 2u);
  EXPECT_EQ(mgr->getCutflowCounts()[0].second, 5ULL); // after cutA
  EXPECT_EQ(mgr->getCutflowCounts()[1].second, 5ULL); // after cutB
}

// ---------------------------------------------------------------------------
// Sequential cutflow: half the events pass each cut
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, CutflowHalfEventsPassFirstCutNonePassSecond) {
  // 4 events (indices 0-3).
  // cutA: indices 0,1 pass (even indices); cutB: always false.
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_cutA",
             [](ULong64_t i) { return (i % 2 == 0); },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutB",
             [](ULong64_t) { return false; },
             {"rdfentry_"}, *systematicManager);

  mgr->addCut("cutA", "pass_cutA");
  mgr->addCut("cutB", "pass_cutB");

  mgr->execute();
  mgr->finalize();

  EXPECT_EQ(mgr->getTotalCount(), 4ULL);
  ASSERT_EQ(mgr->getCutflowCounts().size(), 2u);
  EXPECT_EQ(mgr->getCutflowCounts()[0].second, 2ULL); // 2 pass cutA
  EXPECT_EQ(mgr->getCutflowCounts()[1].second, 0ULL); // 0 pass cutA+cutB
}

// ---------------------------------------------------------------------------
// N-1 table
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, NMinusOneCountsCorrect) {
  // 6 events (indices 0-5).
  // cutA: even indices pass  → 0,2,4 → 3 events
  // cutB: indices 0,1,2 pass → 0,1,2 → 3 events
  // cutA AND cutB: indices 0,2 → 2 events
  //
  // N-1 for cutA (remove cutA, keep cutB): 0,1,2 → 3 events
  // N-1 for cutB (remove cutB, keep cutA): 0,2,4 → 3 events
  auto dm = std::make_unique<DataManager>(6);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_cutA",
             [](ULong64_t i) { return (i % 2 == 0); },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutB",
             [](ULong64_t i) { return (i < 3); },
             {"rdfentry_"}, *systematicManager);

  mgr->addCut("cutA", "pass_cutA");
  mgr->addCut("cutB", "pass_cutB");

  mgr->execute();
  mgr->finalize();

  ASSERT_EQ(mgr->getNMinusOneCounts().size(), 2u);
  // N-1 for cutA: only cutB is active → events with i<3 → 3 events
  EXPECT_EQ(mgr->getNMinusOneCounts()[0].first, "cutA");
  EXPECT_EQ(mgr->getNMinusOneCounts()[0].second, 3ULL);
  // N-1 for cutB: only cutA is active → even i → 3 events
  EXPECT_EQ(mgr->getNMinusOneCounts()[1].first, "cutB");
  EXPECT_EQ(mgr->getNMinusOneCounts()[1].second, 3ULL);
}

// ---------------------------------------------------------------------------
// Label ordering is preserved
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, CutNamesPreservedInOrder) {
  auto dm = std::make_unique<DataManager>(3);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_first",  [](ULong64_t) { return true; }, {"rdfentry_"}, *systematicManager);
  dm->Define("pass_second", [](ULong64_t) { return true; }, {"rdfentry_"}, *systematicManager);
  dm->Define("pass_third",  [](ULong64_t) { return true; }, {"rdfentry_"}, *systematicManager);

  mgr->addCut("first",  "pass_first");
  mgr->addCut("second", "pass_second");
  mgr->addCut("third",  "pass_third");

  mgr->execute();
  mgr->finalize();

  const auto &cf = mgr->getCutflowCounts();
  ASSERT_EQ(cf.size(), 3u);
  EXPECT_EQ(cf[0].first, "first");
  EXPECT_EQ(cf[1].first, "second");
  EXPECT_EQ(cf[2].first, "third");

  const auto &nm1 = mgr->getNMinusOneCounts();
  ASSERT_EQ(nm1.size(), 3u);
  EXPECT_EQ(nm1[0].first, "first");
  EXPECT_EQ(nm1[1].first, "second");
  EXPECT_EQ(nm1[2].first, "third");
}

// ---------------------------------------------------------------------------
// Lifecycle hook no-ops
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, InitializeIsCallable) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->initialize());
}

TEST_F(CutflowManagerTest, ReportMetadataWithNoCutsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(CutflowManagerTest, ReportMetadataAfterRunDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_cut", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->addCut("cut", "pass_cut");

  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(CutflowManagerTest, GetDependenciesReturnsEmpty) {
  CutflowManager mgr;
  EXPECT_TRUE(mgr.getDependencies().empty());
}

// ---------------------------------------------------------------------------
// Single cut
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, SingleCutCutflowAndNMinus1) {
  // 4 events; 3 pass the single cut.
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_only", [](ULong64_t i) { return (i < 3); }, {"rdfentry_"},
             *systematicManager);

  mgr->addCut("only", "pass_only");

  mgr->execute();
  mgr->finalize();

  EXPECT_EQ(mgr->getTotalCount(), 4ULL);
  ASSERT_EQ(mgr->getCutflowCounts().size(), 1u);
  EXPECT_EQ(mgr->getCutflowCounts()[0].second, 3ULL);

  // N-1 for the single cut removes it → no cuts → all 4 events pass.
  ASSERT_EQ(mgr->getNMinusOneCounts().size(), 1u);
  EXPECT_EQ(mgr->getNMinusOneCounts()[0].second, 4ULL);
}

// ---------------------------------------------------------------------------
// Helpers shared by region binding tests
// ---------------------------------------------------------------------------

/// Wire and return a RegionManager backed by *dm*.
static std::unique_ptr<RegionManager>
makeRegionMgr(DataManager &dm, IConfigurationProvider &cfg,
              SystematicManager &sm, DefaultLogger &log, NullOutputSink &skim,
              NullOutputSink &meta) {
  auto rm = std::make_unique<RegionManager>();
  ManagerContext ctx{cfg, dm, sm, log, skim, meta};
  rm->setContext(ctx);
  rm->setupFromConfigFile();
  return rm;
}

// ---------------------------------------------------------------------------
// bindToRegionManager() before setContext() is safe (nullptr)
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, BindToNullptrRegionManagerIsNoOp) {
  CutflowManager mgr;
  EXPECT_NO_THROW(mgr.bindToRegionManager(nullptr));
}

// ---------------------------------------------------------------------------
// Region binding: single region, single cut
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, RegionCutflowSingleRegionSingleCut) {
  // 6 events (0-5).
  // region "signal": i >= 2 → 4 events (2-5).
  // cut "ptCut": i < 4 → within region: events 2,3 → 2 events.
  auto dm = std::make_unique<DataManager>(6);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_signal", [](ULong64_t i) { return i >= 2; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_pt",     [](ULong64_t i) { return i < 4; },
             {"rdfentry_"}, *systematicManager);

  rm->declareRegion("signal", "pass_signal");
  cfm->addCut("ptCut", "pass_pt");
  cfm->bindToRegionManager(rm.get());

  cfm->execute();
  cfm->finalize();

  // Global cutflow: 6 total, 4 pass ptCut.
  EXPECT_EQ(cfm->getTotalCount(), 6ULL);
  ASSERT_EQ(cfm->getCutflowCounts().size(), 1u);
  EXPECT_EQ(cfm->getCutflowCounts()[0].second, 4ULL);

  // Region "signal" has 4 events total; 2 pass ptCut inside the region.
  EXPECT_EQ(cfm->getRegionTotalCount("signal"), 4ULL);
  ASSERT_EQ(cfm->getRegionCutflowCounts("signal").size(), 1u);
  EXPECT_EQ(cfm->getRegionCutflowCounts("signal")[0].second, 2ULL);
}

// ---------------------------------------------------------------------------
// Region binding: multiple regions, multiple cuts
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, RegionCutflowMultipleRegionsTwoCuts) {
  // 10 events (0-9).
  // presel:  i >= 1  → 9 events (1-9)
  // signal:  presel AND i < 5  → events 1-4 → 4 events
  // cutA: i % 2 == 0 (even)
  // cutB: i < 7
  auto dm = std::make_unique<DataManager>(10);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_presel", [](ULong64_t i) { return i >= 1; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_signal", [](ULong64_t i) { return i < 5; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutA",   [](ULong64_t i) { return i % 2 == 0; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutB",   [](ULong64_t i) { return i < 7; },
             {"rdfentry_"}, *systematicManager);

  rm->declareRegion("presel", "pass_presel");
  rm->declareRegion("signal", "pass_signal", "presel");

  cfm->addCut("cutA", "pass_cutA");
  cfm->addCut("cutB", "pass_cutB");
  cfm->bindToRegionManager(rm.get());

  cfm->execute();
  cfm->finalize();

  // "presel" region: 9 events (1-9).
  //   After cutA (even): 2,4,6,8 → 4 events.
  //   After cutA+cutB (even AND <7): 2,4,6 → 3 events.
  EXPECT_EQ(cfm->getRegionTotalCount("presel"), 9ULL);
  ASSERT_EQ(cfm->getRegionCutflowCounts("presel").size(), 2u);
  EXPECT_EQ(cfm->getRegionCutflowCounts("presel")[0].second, 4ULL); // after cutA
  EXPECT_EQ(cfm->getRegionCutflowCounts("presel")[1].second, 3ULL); // after cutA+cutB

  // "signal" region: events 1-4 (presel AND i<5), so 1,2,3,4.
  //   After cutA (even): 2,4 → 2 events.
  //   After cutA+cutB (even AND <7): 2,4 → 2 events (both < 7 already).
  EXPECT_EQ(cfm->getRegionTotalCount("signal"), 4ULL);
  ASSERT_EQ(cfm->getRegionCutflowCounts("signal").size(), 2u);
  EXPECT_EQ(cfm->getRegionCutflowCounts("signal")[0].second, 2ULL);
  EXPECT_EQ(cfm->getRegionCutflowCounts("signal")[1].second, 2ULL);
}

// ---------------------------------------------------------------------------
// Region binding: N-1 counts per region
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, RegionNMinusOneCountsPerRegion) {
  // 8 events (0-7).
  // region "ctrl": i >= 4 → events 4-7 → 4 events.
  // cutA: i < 7 (passes 4,5,6 in region → 3 events)
  // cutB: i % 2 == 0 (passes 4,6 in region → 2 events)
  // N-1 for cutA (skip cutA, apply cutB): 4,6 → 2 events
  // N-1 for cutB (skip cutB, apply cutA): 4,5,6 → 3 events
  auto dm = std::make_unique<DataManager>(8);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_ctrl", [](ULong64_t i) { return i >= 4; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutA", [](ULong64_t i) { return i < 7; },
             {"rdfentry_"}, *systematicManager);
  dm->Define("pass_cutB", [](ULong64_t i) { return i % 2 == 0; },
             {"rdfentry_"}, *systematicManager);

  rm->declareRegion("ctrl", "pass_ctrl");

  cfm->addCut("cutA", "pass_cutA");
  cfm->addCut("cutB", "pass_cutB");
  cfm->bindToRegionManager(rm.get());

  cfm->execute();
  cfm->finalize();

  ASSERT_EQ(cfm->getRegionNMinusOneCounts("ctrl").size(), 2u);
  EXPECT_EQ(cfm->getRegionNMinusOneCounts("ctrl")[0].first,  "cutA");
  EXPECT_EQ(cfm->getRegionNMinusOneCounts("ctrl")[0].second, 2ULL); // only cutB
  EXPECT_EQ(cfm->getRegionNMinusOneCounts("ctrl")[1].first,  "cutB");
  EXPECT_EQ(cfm->getRegionNMinusOneCounts("ctrl")[1].second, 3ULL); // only cutA
}

// ---------------------------------------------------------------------------
// Accessors throw for unknown region names
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, RegionAccessorsThrowForUnknownRegion) {
  auto dm = std::make_unique<DataManager>(4);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_cut", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_r",   [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  rm->declareRegion("r", "pass_r");
  cfm->addCut("cut", "pass_cut");
  cfm->bindToRegionManager(rm.get());
  cfm->execute();
  cfm->finalize();

  EXPECT_THROW(cfm->getRegionCutflowCounts("no_such_region"),   std::runtime_error);
  EXPECT_THROW(cfm->getRegionNMinusOneCounts("no_such_region"), std::runtime_error);
  EXPECT_THROW(cfm->getRegionTotalCount("no_such_region"),      std::runtime_error);
}

// ---------------------------------------------------------------------------
// Region binding with no cuts: only total counts per region
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, RegionBindingNoCutsOnlyTotalCounts) {
  auto dm = std::make_unique<DataManager>(10);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_r", [](ULong64_t i) { return i < 6; }, {"rdfentry_"},
             *systematicManager);
  rm->declareRegion("r", "pass_r");
  cfm->bindToRegionManager(rm.get());

  // execute() with empty cuts_m returns early and does not book region actions.
  cfm->execute();
  cfm->finalize();

  // No per-region results because execute() returned early (no cuts).
  EXPECT_TRUE(cfm->getCutflowCounts().empty());
}

// ---------------------------------------------------------------------------
// initialize() with a bound RegionManager does not throw
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, InitializeWithBoundRegionManagerDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  rm->declareRegion("r", "pass_r");
  cfm->bindToRegionManager(rm.get());

  EXPECT_NO_THROW(cfm->initialize());
}

// ---------------------------------------------------------------------------
// reportMetadata() with region binding does not throw
// ---------------------------------------------------------------------------

TEST_F(CutflowManagerTest, ReportMetadataWithRegionBindingDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(4);
  auto cfm = makeMgr(*dm);
  auto rm  = makeRegionMgr(*dm, *config, *systematicManager, *logger,
                            *skimSink, *metaSink);

  dm->Define("pass_r",   [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_cut", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  rm->declareRegion("r", "pass_r");
  cfm->addCut("cut", "pass_cut");
  cfm->bindToRegionManager(rm.get());
  cfm->execute();
  cfm->finalize();
  EXPECT_NO_THROW(cfm->reportMetadata());
}
TEST_F(CutflowManagerTest, CollectProvenanceEntriesHasNumCuts) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  // Zero cuts registered
  const auto entries0 = mgr->collectProvenanceEntries();
  ASSERT_NE(entries0.find("num_cuts"), entries0.end());
  EXPECT_EQ(entries0.at("num_cuts"), "0");

  dm->Define("pass_pt",  [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_eta", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->addCut("ptCut",  "pass_pt");
  mgr->addCut("etaCut", "pass_eta");

  const auto entries2 = mgr->collectProvenanceEntries();
  ASSERT_NE(entries2.find("num_cuts"), entries2.end());
  EXPECT_EQ(entries2.at("num_cuts"), "2");
}

TEST_F(CutflowManagerTest, CollectProvenanceEntriesContainsCuts) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_pt", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->addCut("ptCut", "pass_pt");

  const auto entries = mgr->collectProvenanceEntries();
  ASSERT_NE(entries.find("cuts"), entries.end());
  const std::string& cuts = entries.at("cuts");
  EXPECT_NE(cuts.find("ptCut"),   std::string::npos);
  EXPECT_NE(cuts.find("pass_pt"), std::string::npos);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
