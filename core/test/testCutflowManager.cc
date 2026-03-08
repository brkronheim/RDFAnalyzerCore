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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
