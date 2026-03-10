#include <gtest/gtest.h>

#include <analyzer.h>
#include <NDHistogramManager.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
std::string writeConfig(const std::string& path, const std::string& metaFile) {
  std::ofstream out(path);
  out << "enableCounters=true\n";
  out << "counterWeightBranch=genWeight\n";
  out << "counterIntWeightBranch=intCode\n";
  out << "metaFile=" << metaFile << "\n";
  out << "sample=TestSample\n";
  out.close();
  return path;
}
}

TEST(CounterServiceDedupTest, AnalyzerPlusNDHistogramManagerRunsCounterOnlyOnce) {
  const std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_dedup_config.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_dedup_meta.root";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());

  writeConfig(cfgPath, metaPath);

  Analyzer analyzer(cfgPath);
  auto histManager = std::make_unique<NDHistogramManager>(analyzer.getConfigurationProvider());
  analyzer.addPlugin("histogramManager", std::move(histManager));

  // define the branches used by CounterService
  analyzer.Define("intCode", [](ULong64_t entry) { return static_cast<Int_t>(entry % 3); }, {"rdfentry_"});
  analyzer.Define("genWeight", [](ULong64_t entry) { return (entry % 2 == 0) ? 1.0f : 2.0f; }, {"rdfentry_"});

  // trigger pre-filter notification
  analyzer.Filter("intCode", [](Int_t code) { return code != 1; }, {"intCode"});

  // capture stdout to count CounterService finalize calls
  testing::internal::CaptureStdout();
  analyzer.save();

  // call NDHistogramManager::saveHists (should NOT run CounterService again)
  std::vector<std::vector<histInfo>> fullHistList = {{histInfo("h", "genWeight", "lbl", "genWeight", 1, 0.0, 1.0)}};
  std::vector<std::vector<std::string>> regionNames = {{"all"}};
  analyzer.getPlugin<NDHistogramManager>("histogramManager")->saveHists(fullHistList, regionNames);

  std::string out = testing::internal::GetCapturedStdout();

  // count occurrences of the CounterService finalizing message
  size_t count = 0;
  std::string needle = "CounterService: Finalizing counter for sample";
  size_t pos = out.find(needle);
  while (pos != std::string::npos) {
    ++count;
    pos = out.find(needle, pos + 1);
  }

  EXPECT_EQ(count, 1u) << "CounterService finalized more than once";

  // verify counter histogram exists in meta file (sanity)
  TFile f(metaPath.c_str(), "READ");
  ASSERT_FALSE(f.IsZombie());
  auto* counterHist = dynamic_cast<TH1D*>(f.Get("counter_weightSignSum_TestSample"));
  ASSERT_NE(counterHist, nullptr);

  // also ensure the NDHistogramManager histogram we wrote is present somewhere in
  // the file (may be inside a directory). Search the key list for the name.
  bool foundND = false;
  if (auto keys = f.GetListOfKeys()) {
    keys->Rewind();
    TKey *k = nullptr;
    while ((k = (TKey*)keys->Next())) {
      if (std::string(k->GetName()) == "h") { foundND = true; break; }
    }
  }
  EXPECT_TRUE(foundND) << "ND histogram 'h' not found in meta file";
  f.Close();

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}
