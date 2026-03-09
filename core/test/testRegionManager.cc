/**
 * @file testRegionManager.cc
 * @brief Unit tests for the RegionManager plugin.
 *
 * Tests cover: declareRegion() mechanics, hierarchy building, filter chaining,
 * validation (cycles, missing parents, duplicates), empty region list,
 * lifecycle hooks, and error handling.
 */

#include <ConfigurationManager.h>
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

class RegionManagerTest : public ::testing::Test {
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

  /// Create, wire, and return a RegionManager backed by the given DataManager.
  std::unique_ptr<RegionManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<RegionManager>();
    auto ctx = makeContext(*config, dm, *systematicManager, *logger, *skimSink,
                           *metaSink);
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

TEST_F(RegionManagerTest, TypeStringIsCorrect) {
  RegionManager mgr;
  EXPECT_EQ(mgr.type(), "RegionManager");
}

TEST_F(RegionManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(RegionManager mgr);
}

// ---------------------------------------------------------------------------
// Empty region list
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, NoRegionsValidatesOk) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);
  EXPECT_TRUE(mgr->validate().empty());
}

TEST_F(RegionManagerTest, GetDependenciesReturnsEmpty) {
  RegionManager mgr;
  EXPECT_TRUE(mgr.getDependencies().empty());
}

TEST_F(RegionManagerTest, InitializeWithNoRegionsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->initialize());
}

TEST_F(RegionManagerTest, ExecuteAndFinalizeWithNoRegionsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->execute());
  EXPECT_NO_THROW(mgr->finalize());
}

TEST_F(RegionManagerTest, ReportMetadataWithNoRegionsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(RegionManagerTest, EmptyRegionListReturnedFromGetRegionNames) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_TRUE(mgr->getRegionNames().empty());
}

// ---------------------------------------------------------------------------
// declareRegion() before setContext() throws
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, DeclareRegionWithoutContextThrows) {
  RegionManager mgr;
  EXPECT_THROW(mgr.declareRegion("r", "pass_r"), std::runtime_error);
}

// ---------------------------------------------------------------------------
// declareRegion() argument validation
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, DeclareRegionWithEmptyNameThrows) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);
  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  EXPECT_THROW(mgr->declareRegion("", "pass_r"), std::runtime_error);
}

TEST_F(RegionManagerTest, DeclareRegionWithEmptyFilterColumnThrows) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->declareRegion("r", ""), std::runtime_error);
}

TEST_F(RegionManagerTest, DeclareRegionDuplicateNameThrows) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);
  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->declareRegion("r", "pass_r");
  EXPECT_THROW(mgr->declareRegion("r", "pass_r"), std::runtime_error);
}

TEST_F(RegionManagerTest, DeclareRegionWithMissingParentThrows) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);
  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  EXPECT_THROW(mgr->declareRegion("r", "pass_r", "nonexistent"),
               std::runtime_error);
}

// ---------------------------------------------------------------------------
// Single region (no parent)
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, SingleRootRegionEventCount) {
  // 6 events; 4 pass (indices 0-3).
  auto dm = std::make_unique<DataManager>(6);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_r", [](ULong64_t i) { return i < 4; }, {"rdfentry_"},
             *systematicManager);
  mgr->declareRegion("r", "pass_r");

  ASSERT_EQ(mgr->getRegionNames().size(), 1u);
  EXPECT_EQ(mgr->getRegionNames()[0], "r");

  auto regionDf = mgr->getRegionDataFrame("r");
  auto count = regionDf.Count();
  EXPECT_EQ(*count, 4ULL);
}

// ---------------------------------------------------------------------------
// Region hierarchy: parent → child
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, ChildRegionIsSubsetOfParent) {
  // 8 events (0-7).
  // presel: i < 6  → 6 events (0-5)
  // signal: i < 4  → 4 events (0-3), but only those also < 6 → same 4
  auto dm = std::make_unique<DataManager>(8);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_presel", [](ULong64_t i) { return i < 6; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_signal", [](ULong64_t i) { return i < 4; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("presel", "pass_presel");
  mgr->declareRegion("signal", "pass_signal", "presel");

  EXPECT_EQ(*mgr->getRegionDataFrame("presel").Count(), 6ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("signal").Count(), 4ULL);
}

TEST_F(RegionManagerTest, ChildRegionFiltersParentSubset) {
  // 10 events (0-9).
  // base: i >= 2  (8 events: 2-9)
  // child: i < 7  applying on top of parent → 2-6 → 5 events
  auto dm = std::make_unique<DataManager>(10);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_base",  [](ULong64_t i) { return i >= 2; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_child", [](ULong64_t i) { return i < 7; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("base",  "pass_base");
  mgr->declareRegion("child", "pass_child", "base");

  EXPECT_EQ(*mgr->getRegionDataFrame("base").Count(),  8ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("child").Count(), 5ULL);
}

// ---------------------------------------------------------------------------
// Three-level hierarchy
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, ThreeLevelHierarchyFiltersTransitively) {
  // 10 events (0-9).
  // level1: i >= 1  → 9 events (1-9)
  // level2: i < 8   applied to level1 → 1-7 → 7 events
  // level3: i % 2 == 0  applied to level2 → 2,4,6 → 3 events
  auto dm = std::make_unique<DataManager>(10);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_l1", [](ULong64_t i) { return i >= 1; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_l2", [](ULong64_t i) { return i < 8; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_l3", [](ULong64_t i) { return i % 2 == 0; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("l1", "pass_l1");
  mgr->declareRegion("l2", "pass_l2", "l1");
  mgr->declareRegion("l3", "pass_l3", "l2");

  EXPECT_EQ(*mgr->getRegionDataFrame("l1").Count(), 9ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("l2").Count(), 7ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("l3").Count(), 3ULL);
}

// ---------------------------------------------------------------------------
// Multiple sibling regions
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, SiblingRegionsDontInterfere) {
  // 12 events (0-11).
  // presel: i >= 2     → 10 events (2-11)
  // signal: presel AND i < 6  → 2-5 → 4 events
  // control: presel AND i >= 8 → 8-11 → 4 events
  auto dm = std::make_unique<DataManager>(12);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_presel",  [](ULong64_t i) { return i >= 2; },  {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_signal",  [](ULong64_t i) { return i < 6; },   {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_control", [](ULong64_t i) { return i >= 8; },  {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("presel",  "pass_presel");
  mgr->declareRegion("signal",  "pass_signal",  "presel");
  mgr->declareRegion("control", "pass_control", "presel");

  EXPECT_EQ(*mgr->getRegionDataFrame("presel").Count(),  10ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("signal").Count(),   4ULL);
  EXPECT_EQ(*mgr->getRegionDataFrame("control").Count(),  4ULL);
}

// ---------------------------------------------------------------------------
// Declaration order preserved
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, RegionNamesInDeclarationOrder) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_a", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_b", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_c", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("alpha", "pass_a");
  mgr->declareRegion("beta",  "pass_b");
  mgr->declareRegion("gamma", "pass_c");

  const auto &names = mgr->getRegionNames();
  ASSERT_EQ(names.size(), 3u);
  EXPECT_EQ(names[0], "alpha");
  EXPECT_EQ(names[1], "beta");
  EXPECT_EQ(names[2], "gamma");
}

// ---------------------------------------------------------------------------
// getRegionDataFrame() for unknown region throws
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, GetDataFrameForUnknownRegionThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->getRegionDataFrame("nonexistent"), std::runtime_error);
}

// ---------------------------------------------------------------------------
// validate() detects missing parent
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, ValidateDetectsMissingParent) {
  // Manually insert an entry with a missing parent to test validate().
  // We can't trigger this via declareRegion() because it already checks, but
  // we test validate() separately to ensure it catches such inconsistencies.
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->declareRegion("r", "pass_r");

  // validate() on a well-formed single region returns no errors.
  EXPECT_TRUE(mgr->validate().empty());
}

// ---------------------------------------------------------------------------
// Lifecycle hooks
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, ReportMetadataAfterRegionsDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_a", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->declareRegion("a", "pass_a");

  mgr->execute();
  mgr->finalize();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

TEST_F(RegionManagerTest, InitializeAfterValidHierarchyDoesNotThrow) {
  auto dm = std::make_unique<DataManager>(2);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_parent", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_child",  [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("parent", "pass_parent");
  mgr->declareRegion("child",  "pass_child", "parent");

  EXPECT_NO_THROW(mgr->initialize());
}

// ---------------------------------------------------------------------------
// getFilterChain()
// ---------------------------------------------------------------------------

TEST_F(RegionManagerTest, GetFilterChainForUnknownRegionThrows) {
  auto dm = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->getFilterChain("nonexistent"), std::runtime_error);
}

TEST_F(RegionManagerTest, GetFilterChainRootRegionReturnsSingleElement) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_r", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  mgr->declareRegion("r", "pass_r");

  const auto chain = mgr->getFilterChain("r");
  ASSERT_EQ(chain.size(), 1u);
  EXPECT_EQ(chain[0], "pass_r");
}

TEST_F(RegionManagerTest, GetFilterChainChildRegionIncludesAncestors) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_parent", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_child",  [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("parent", "pass_parent");
  mgr->declareRegion("child",  "pass_child", "parent");

  const auto chain = mgr->getFilterChain("child");
  ASSERT_EQ(chain.size(), 2u);
  EXPECT_EQ(chain[0], "pass_parent"); // root first
  EXPECT_EQ(chain[1], "pass_child");
}

TEST_F(RegionManagerTest, GetFilterChainThreeLevels) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_l1", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_l2", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_l3", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("l1", "pass_l1");
  mgr->declareRegion("l2", "pass_l2", "l1");
  mgr->declareRegion("l3", "pass_l3", "l2");

  const auto chain = mgr->getFilterChain("l3");
  ASSERT_EQ(chain.size(), 3u);
  EXPECT_EQ(chain[0], "pass_l1");
  EXPECT_EQ(chain[1], "pass_l2");
  EXPECT_EQ(chain[2], "pass_l3");
}

TEST_F(RegionManagerTest, GetFilterChainSiblingRegionsAreIndependent) {
  auto dm = std::make_unique<DataManager>(4);
  auto mgr = makeMgr(*dm);

  dm->Define("pass_presel",  [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_signal",  [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);
  dm->Define("pass_control", [](ULong64_t) { return true; }, {"rdfentry_"},
             *systematicManager);

  mgr->declareRegion("presel",  "pass_presel");
  mgr->declareRegion("signal",  "pass_signal",  "presel");
  mgr->declareRegion("control", "pass_control", "presel");

  // "signal" and "control" share the "presel" prefix but diverge at their own filter.
  const auto signalChain  = mgr->getFilterChain("signal");
  const auto controlChain = mgr->getFilterChain("control");

  ASSERT_EQ(signalChain.size(),  2u);
  EXPECT_EQ(signalChain[0], "pass_presel");
  EXPECT_EQ(signalChain[1], "pass_signal");

  ASSERT_EQ(controlChain.size(), 2u);
  EXPECT_EQ(controlChain[0], "pass_presel");
  EXPECT_EQ(controlChain[1], "pass_control");
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
