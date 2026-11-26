#include <ConfigurationManager.h>
#include <DataManager.h>
#include <InputCache.h>
#include <ROOT/RDataFrame.hxx>
#include <Math/Vector3D.h>
#include <TFile.h>
#include <TChainElement.h>
#include <TTree.h>
#include <TTreeCache.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <set>
#include <string>
#include <tuple>
#include <unistd.h>

#ifdef RDF_INPUT_CACHE_CLOCK_TEST
#include <atomic>
#include <TLeaf.h>

// Only the GNU/Linux instrumentation executable wraps this libstdc++ symbol.
// Shared ROOT libraries are not relinked; count framework object references.
namespace {
std::atomic<unsigned long> frameworkClockReads{0};
}
extern "C" std::chrono::steady_clock::time_point
__real__ZNSt6chrono3_V212steady_clock3nowEv();
extern "C" std::chrono::steady_clock::time_point
__wrap__ZNSt6chrono3_V212steady_clock3nowEv() {
  frameworkClockReads.fetch_add(1, std::memory_order_relaxed);
  return __real__ZNSt6chrono3_V212steady_clock3nowEv();
}
#endif

namespace {
class InputCacheTest : public ::testing::Test {
protected:
  std::filesystem::path dir;
  void SetUp() override {
    ROOT::DisableImplicitMT();
    dir = std::filesystem::temp_directory_path() /
          ("rdf-input-cache-" + std::to_string(getpid()));
    std::filesystem::create_directories(dir);
    for (int part = 0; part < 2; ++part) {
      TFile file((dir / (std::to_string(part) + ".root")).c_str(), "RECREATE");
      TTree tree("Events", "Events");
      float jet, eta = 2, muon = 3, met = 4, unused = 99;
      int n = 2;
      float values[2] = {1, 2};
      ROOT::Math::XYZVector position(1, 2, 3);
      tree.Branch("Jet_pt", &jet);
      tree.Branch("Jet_eta", &eta);
      tree.Branch("Muon_pt", &muon);
      tree.Branch("MET_pt", &met);
      tree.Branch("unused", &unused);
      tree.Branch("nJet", &n);
      tree.Branch("Jet_values", values, "Jet_values[nJet]/F");
      tree.Branch("position.", &position, 32000, 99);
      for (int entry = 0; entry < 20; ++entry) {
        jet = part * 20 + entry;
        tree.Fill();
      }
      tree.Write();
    }
  }
  void TearDown() override { std::filesystem::remove_all(dir); }
  ConfigurationManager config(const std::string &extra = "") {
    const auto path = dir / "config.txt";
    std::ofstream out(path);
    out << "threads=1\nbatch=True\ntreeList=Events\nfileList="
        << (dir / "0.root").string() << ',' << (dir / "1.root").string()
        << "\n" << extra;
    out.close();
    return ConfigurationManager(path.string());
  }
  TTreeCache *cache(DataManager &data) {
    auto &chain = data.getChain();
    return chain.GetTree()->GetReadCache(chain.GetCurrentFile(), false);
  }
  std::set<std::string> branches(DataManager &data) {
    std::set<std::string> names;
    auto *c = cache(data);
    if (c && c->GetCachedBranches())
      for (auto *b : *c->GetCachedBranches()) names.insert(b->GetName());
    return names;
  }
};

TEST_F(InputCacheTest, PhysicalDependenciesAndAsyncSurviveFileTransitions) {
  for (bool async : {false, true}) {
    auto cfg = config("treeCacheEnabled=1\ntreeCacheSizeMB=8\ntreeCacheAsyncPrefetch=" +
                      std::to_string(async) + "\n");
    DataManager data(cfg);
    auto df = data.getDataFrame()
                  .Define("x", [](float pt, float eta) { return pt + eta; }, {"Jet_pt", "Jet_eta"})
                  .Define("y", "x + Muon_pt")
                  .Alias("alias_met", "MET_pt")
                  .Filter("y > 0 && alias_met > 0");
    auto hist = df.Histo1D({"h", "h", 10, 0, 10}, "Muon_pt");
    std::set<int> files;
    df.Foreach([&](float) {
      const auto file = data.getChain().GetTreeNumber();
      if (!files.insert(file).second) return;
      ASSERT_NE(cache(data), nullptr);
      EXPECT_FALSE(cache(data)->IsLearning());
      EXPECT_EQ(cache(data)->IsEnablePrefetching(), async);
      if (async) EXPECT_GT(cache(data)->GetPrefetchedBlocks(), 0);
      EXPECT_EQ(branches(data), (std::set<std::string>{"Jet_pt", "Jet_eta", "Muon_pt", "MET_pt"}));
      EXPECT_FALSE(ROOT::IsImplicitMTEnabled());
      // Check metadata pointers were rebound to the new tree, not just names.
      for (auto *b : *cache(data)->GetCachedBranches())
        EXPECT_EQ(b, data.getChain().GetTree()->GetBranch(b->GetName()));
    }, {"y"});
    EXPECT_EQ(files, (std::set<int>{0, 1}));
    EXPECT_EQ(hist->GetEntries(), 40);
  }
}

TEST_F(InputCacheTest, StartupKeepsLaterFileMetadataLazy) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheStats=0\n");
  DataManager data(cfg);
  auto &chain = data.getChain();
  ASSERT_EQ(chain.GetTreeNumber(), 0);
  auto *second = dynamic_cast<TChainElement *>(chain.GetListOfFiles()->At(1));
  ASSERT_NE(second, nullptr);
  EXPECT_EQ(second->GetEntries(), TTree::kMaxEntries);
  EXPECT_EQ(*data.getDataFrame().Count(), 40);
  EXPECT_EQ(second->GetEntries(), 20);
}

TEST_F(InputCacheTest, EmptyFirstFileStillReachesNonemptyInput) {
  {
    TFile file((dir / "0.root").c_str(), "RECREATE");
    TTree tree("Events", "Events");
    float pt = 0;
    tree.Branch("Jet_pt", &pt);
    tree.Write();
  }
  auto cfg = config("treeCacheEnabled=1\ntreeCacheStats=0\n");
  DataManager data(cfg);
  EXPECT_EQ(*data.getDataFrame().Count(), 20);
  std::filesystem::remove(dir / "1.root");
  EXPECT_THROW(DataManager{cfg}, std::runtime_error);
}

TEST_F(InputCacheTest, MissingTreeAndUnreadableInputsStillFail) {
  for (const auto &name : {"0.root", "1.root"}) {
    TFile file((dir / name).c_str(), "RECREATE");
    TTree tree("WrongTree", "WrongTree");
    tree.Write();
  }
  auto cfg = config("treeCacheStats=0\n");
  EXPECT_THROW(DataManager{cfg}, std::runtime_error);
  std::filesystem::remove(dir / "0.root");
  std::filesystem::remove(dir / "1.root");
  EXPECT_THROW(DataManager{cfg}, std::runtime_error);
}

TEST_F(InputCacheTest, DefaultCacheBudgetsAndExplicitSizesSurviveFileTransitions) {
  const std::vector<std::tuple<std::string, Long64_t, int>> settings{
      {"", 32LL * 1024 * 1024, 32},
      {"treeCacheSizeMB=8\nxrdPrefetchCacheMB=64\n", 8LL * 1024 * 1024, 64},
  };
  for (const auto &[extra, rootBytes, xrdMiB] : settings) {
    auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=1\n" + extra);
    testing::internal::CaptureStdout();
    DataManager data(cfg);
    const auto setup = testing::internal::GetCapturedStdout();
    if (setup.find("ROOT input backend: XrdAdaptor") != std::string::npos)
      EXPECT_NE(setup.find("handoff cache=" + std::to_string(xrdMiB) + " MiB"),
                std::string::npos);
    EXPECT_EQ(data.getChain().GetCacheSize(), rootBytes);
    std::set<int> files;
    data.getDataFrame().Foreach([&](float) {
      const auto file = data.getChain().GetTreeNumber();
      if (!files.insert(file).second) return;
      ASSERT_NE(cache(data), nullptr);
      EXPECT_EQ(cache(data)->GetBufferSize(), rootBytes);
      EXPECT_TRUE(cache(data)->IsEnablePrefetching());
    }, {"Jet_pt"});
    EXPECT_EQ(files, (std::set<int>{0, 1}));
  }
}

TEST_F(InputCacheTest, SimpleAndTransitiveDependencies) {
  for (bool transitive : {false, true}) {
    auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
    DataManager data(cfg);
    auto df = data.getDataFrame().Define("x", "Jet_pt + Jet_eta");
    if (transitive) df = df.Define("y", "x + Muon_pt").Filter("y > 0");
    else df = df.Filter("x > 0");
    auto expected = std::set<std::string>{"Jet_pt", "Jet_eta"};
    if (transitive) expected.insert("Muon_pt");
    df.Foreach([&](float) { EXPECT_EQ(branches(data), expected); }, {"x"});
  }
}

TEST_F(InputCacheTest, RedefineAndCountLeafResolveToPhysicalInputs) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  auto df = data.getDataFrame().Alias("pt", "Jet_pt")
                .Redefine("Jet_pt", "pt + Muon_pt")
                .Define("total", "Jet_pt + Sum(Jet_values)");
  df.Foreach([&](float) {
    EXPECT_EQ(branches(data), (std::set<std::string>{"Jet_pt", "Muon_pt", "nJet", "Jet_values"}));
  }, {"total"});
}

TEST_F(InputCacheTest, RangeUsesEachFilesLocalCoordinates) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\nfirstEntry=12\nlastEntry=27\n");
  DataManager data(cfg);
  int count = 0;
  data.getDataFrame().Foreach([&](float) {
    ++count;
    auto *c = cache(data);
    ASSERT_NE(c, nullptr);
    const bool first = data.getChain().GetTreeNumber() == 0;
    EXPECT_EQ(c->GetEntryMin(), first ? 12 : 0);
    EXPECT_EQ(c->GetEntryMax(), first ? 20 : 7);
    EXPECT_FALSE(c->IsLearning());
  }, {"Jet_pt"});
  EXPECT_EQ(count, 15);
}

TEST_F(InputCacheTest, UnknownDependenciesRetainLearning) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  data.markInputDependenciesIncomplete("test opaque action");
  bool observed = false;
  data.getDataFrame().Range(1).Foreach([&](float) {
    observed = true;
    ASSERT_NE(cache(data), nullptr);
    EXPECT_TRUE(cache(data)->IsLearning());
  }, {"Jet_pt"});
  EXPECT_TRUE(observed);
}

TEST_F(InputCacheTest, ExplicitPolicyCanRetainLearning) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheExplicitBranches=0\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  data.getDataFrame().Range(1).Foreach([&](float) {
    ASSERT_NE(cache(data), nullptr);
    EXPECT_TRUE(cache(data)->IsLearning());
  }, {"Jet_pt"});
}

TEST_F(InputCacheTest, DisabledPreservesResultsAndOrdinaryChain) {
  double reference = -1;
  for (int mode : {0, 1}) {
    auto cfg = config("treeCacheEnabled=" + std::to_string(mode) +
                      "\ntreeCacheAsyncPrefetch=0\ntreeCacheStats=0\n");
    DataManager data(cfg);
    EXPECT_EQ(dynamic_cast<InputCacheChain *>(&data.getChain()) != nullptr, mode == 1);
    auto df = data.getDataFrame().Define("x", "Jet_pt * 2 + Muon_pt").Filter("x > 10");
    const double result = *df.Sum<float>("x");
    if (mode == 0) reference = result;
    else EXPECT_DOUBLE_EQ(result, reference);
  }
}

TEST_F(InputCacheTest, SnapshotAndLateActionsRegisterNewDependencies) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  auto df = data.getDataFrame();
  EXPECT_EQ(*df.Sum<float>("Jet_pt"), 780);
  auto out = dir / "snapshot.root";
  df.Define("derived", "Muon_pt + MET_pt").Snapshot("Events", out.string(), {"derived"});
  ROOT::RDataFrame result("Events", out.string());
  EXPECT_EQ(*result.Sum<float>("derived"), 280);
  EXPECT_TRUE(branches(data).count("Muon_pt"));
  EXPECT_TRUE(branches(data).count("MET_pt"));
  EXPECT_FALSE(branches(data).count("derived"));
}

TEST_F(InputCacheTest, ZeroSizeAndInvalidConfiguration) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheSizeMB=0\n");
  DataManager data(cfg);
  EXPECT_EQ(*data.getDataFrame().Sum<float>("Jet_pt"), 780);
  EXPECT_EQ(cache(data), nullptr);
  auto invalid = config("treeCacheEnabled=maybe\n");
  EXPECT_THROW(DataManager{invalid}, std::runtime_error);
  auto invalidAhead = config("treeCacheOpenAheadFiles=5\n");
  EXPECT_THROW(DataManager{invalidAhead}, std::runtime_error);
}

TEST_F(InputCacheTest, SplitBranchUsesRootMetadata) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  const std::string leaf = "position.fCoordinates.fX";
  data.getDataFrame().Foreach([&](double value) {
    EXPECT_EQ(value, 1);
    EXPECT_EQ(branches(data), (std::set<std::string>{leaf}));
    ASSERT_NE(cache(data), nullptr);
    EXPECT_FALSE(cache(data)->IsLearning());
  }, {leaf});
}

TEST_F(InputCacheTest, RegistrationFailureKeepsLearning) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheAsyncPrefetch=0\n");
  DataManager data(cfg);
  data.getChain().LoadTree(0);
  EXPECT_LT(data.getChain().AddBranchToCache("no_such_physical_branch", true), 0);
  data.getDataFrame().Range(1).Foreach([&](float) {
    ASSERT_NE(cache(data), nullptr);
    EXPECT_TRUE(cache(data)->IsLearning());
  }, {"Jet_pt"});
}

TEST_F(InputCacheTest, StatisticsDefaultOnWithoutOptimization) {
  auto cfg = config("treeCacheSizeMB=8\ntreeCacheAsyncPrefetch=0\n");
  testing::internal::CaptureStdout();
  {
    DataManager data(cfg);
    EXPECT_EQ(data.getChain().GetCacheSize(), 8LL * 1024 * 1024);
    EXPECT_EQ(*data.getDataFrame().Sum<float>("Jet_pt"), 780);
  }
  const auto log = testing::internal::GetCapturedStdout();
  EXPECT_NE(log.find("file_index=0"), std::string::npos);
  EXPECT_NE(log.find("file_index=1"), std::string::npos);
  EXPECT_NE(log.find("file_read_calls="), std::string::npos);
  EXPECT_NE(log.find("uncached_read_calls="), std::string::npos);
  EXPECT_NE(log.find("efficiency="), std::string::npos);
  EXPECT_NE(log.find("read_calls_per_wall_s="), std::string::npos);
  EXPECT_NE(log.find("prefetched blocks="), std::string::npos);
}

TEST_F(InputCacheTest, StatisticsCanBeDisabledIndependently) {
  auto cfg = config("treeCacheEnabled=1\ntreeCacheStats=0\n");
  testing::internal::CaptureStdout();
  {
    DataManager data(cfg);
    EXPECT_EQ(*data.getDataFrame().Sum<float>("Jet_pt"), 780);
  }
  const auto log = testing::internal::GetCapturedStdout();
  EXPECT_EQ(log.find("reading statistics"), std::string::npos);
  EXPECT_EQ(log.find("TreeCache statistics"), std::string::npos);
}

TEST_F(InputCacheTest, StatisticsWithNoCacheAndRangeEndingBeforeFileBoundary) {
  auto cfg = config("treeCacheSizeMB=0\nfirstEntry=3\nlastEntry=8\n");
  testing::internal::CaptureStdout();
  {
    DataManager data(cfg);
    EXPECT_EQ(*data.getDataFrame().Sum<float>("Jet_pt"), 25);
  }
  const auto log = testing::internal::GetCapturedStdout();
  EXPECT_NE(log.find("reason=chain-destruction"), std::string::npos);
  EXPECT_NE(log.find("cache unavailable or disabled"), std::string::npos);
  EXPECT_NE(log.find("file_bytes_read="), std::string::npos);
}

TEST_F(InputCacheTest, FriendChainsRetainResultsWithDefaultReporting) {
  const auto friendConfig = dir / "friends.yaml";
  {
    std::ofstream out(friendConfig);
    out << "friends:\n  - alias: calib\n    treeName: Events\n    fileList:\n"
        << "      - " << (dir / "0.root").string() << '\n'
        << "      - " << (dir / "1.root").string() << '\n';
  }
  auto cfg = config("friendConfig=" + friendConfig.string() + "\n");
  testing::internal::CaptureStdout();
  {
    DataManager data(cfg);
    EXPECT_EQ(*data.getDataFrame().Sum<float>("calib.Jet_pt"), 780);
  }
  const auto log = testing::internal::GetCapturedStdout();
  EXPECT_NE(log.find("file_index=1"), std::string::npos);
  EXPECT_NE(log.find("file_read_calls="), std::string::npos);
}

#ifdef RDF_INPUT_CACHE_CLOCK_TEST
TEST_F(InputCacheTest, EntryLocalCallsAvoidTransitionClocks) {
  for (const auto &extra : {
      std::string("treeCacheEnabled=0\ntreeCacheStats=1\n"),
      std::string("treeCacheEnabled=1\ntreeCacheStats=0\n")}) {
    auto cfg = config(extra);
    DataManager data(cfg);
    auto &chain = data.getChain();
    ASSERT_NE(dynamic_cast<InputCacheChain *>(&chain), nullptr);
    ASSERT_EQ(chain.LoadTree(0), 0);
    const auto before = frameworkClockReads.load(std::memory_order_relaxed);
    bool correctEntries = true;
    for (Long64_t entry = 1; entry < 20; ++entry)
      correctEntries = (chain.LoadTree(entry) == entry) && correctEntries;
    const auto after = frameworkClockReads.load(std::memory_order_relaxed);
    EXPECT_TRUE(correctEntries);
    EXPECT_EQ(after, before)
        << "In-file calls must not sample discarded transition timestamps";
    const auto beforeBoundary = frameworkClockReads.load(std::memory_order_relaxed);
    const auto local = chain.LoadTree(20);
    const auto afterBoundary = frameworkClockReads.load(std::memory_order_relaxed);
    EXPECT_EQ(local, 0);
    EXPECT_EQ(chain.GetTreeNumber(), 1);
    // Report/configuration clocks still work on actual file transitions.
    EXPECT_GT(afterBoundary, beforeBoundary);
  }
}

TEST_F(InputCacheTest, TransitionTimingAndValuesSurviveRandomAccess) {
  auto cfg = config("treeCacheEnabled=0\ntreeCacheStats=1\n");
  InputCacheChain chain("Events", cfg, false, false);
  chain.Add((dir / "0.root").c_str());
  chain.Add((dir / "1.root").c_str());
  ASSERT_EQ(chain.LoadTree(0), 0);
  testing::internal::CaptureStdout();
  const auto forward = chain.LoadTree(20);
  chain.GetEntry(20);
  const auto forwardValue = chain.GetLeaf("Jet_pt")->GetValue();
  const auto backward = chain.LoadTree(0);
  chain.GetEntry(0);
  const auto backwardValue = chain.GetLeaf("Jet_pt")->GetValue();
  const auto eof = chain.LoadTree(40);
  const auto log = testing::internal::GetCapturedStdout();
  EXPECT_EQ(forward, 0);
  EXPECT_EQ(backward, 0);
  EXPECT_EQ(forwardValue, 20.f);
  EXPECT_EQ(backwardValue, 0.f);
  EXPECT_LT(eof, 0);
  EXPECT_NE(log.find("from_index=0 expected_to_index=1 actual_to_index=1 requested_entry=20 result=0"),
            std::string::npos);
  // expected_to_index remains the existing next-file prediction for backwards
  // access; validate actual identity rather than silently changing that policy.
  EXPECT_NE(log.find("actual_to_index=0 requested_entry=0 result=0"), std::string::npos);
  EXPECT_NE(log.find("requested_entry=40 result="), std::string::npos);
  for (const auto *key : {"cache_report_ms=", "root_loadtree_ms=", "configure_tree_ms=",
                          "warm_release_ms=", "schedule_next_ms=", "callback_total_ms="}) {
    auto start = log.find(key);
    ASSERT_NE(start, std::string::npos) << key;
    for (; start != std::string::npos; start = log.find(key, start + 1))
      EXPECT_GE(std::stod(log.substr(start + std::string(key).size())), 0.) << key;
  }
}
#endif
}
