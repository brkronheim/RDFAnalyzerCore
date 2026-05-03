#include <gtest/gtest.h>

#include <analyzer.h>
#include <NDHistogramManager.h>
#include <test_util.h>

#include <TFile.h>
#include <TH1D.h>
#include <TCollection.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
std::string writeConfig(const std::string& path, const std::string& metaFile) {
  std::ofstream out(path);
  out << "enableCounters=true\n";
  out << "counterIntWeightBranch=intCode\n";
  out << "metaFile=" << metaFile << "\n";
  out << "sample=TestSample\n";
  out << "directory=test_data_minimal\n";
  out << "saveTree=Events\n";
  out << "saveFile=test_output_dedup.root\n";
  out << "saveDirectory=test_output_dedup/\n";
  out << "threads=1\n";
  out.close();
  return path;
}
}

TEST(CounterServiceDedupTest, AnalyzerPlusNDHistogramManagerRunsCounterOnlyOnce) {
  ChangeToTestSourceDir();

  const std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_dedup_config.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_dedup_meta.root";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());

  writeConfig(cfgPath, metaPath);

  Analyzer analyzer(cfgPath);
  NDHistogramManager::create(analyzer);

  // define the branches used by CounterService
  analyzer.Define("intCode", [](ULong64_t entry) { return static_cast<Int_t>(entry % 3); }, {"rdfentry_"});
  analyzer.Define("genWeight", [](ULong64_t entry) { return (entry % 2 == 0) ? 1.0f : 2.0f; }, {"rdfentry_"});

  // trigger pre-filter notification
  analyzer.Filter("intCode", [](Int_t code) { return code != 1; }, {"intCode"});

  // capture stdout to count CounterService finalize calls
  testing::internal::CaptureStdout();
  analyzer.save();

  std::string out = testing::internal::GetCapturedStdout();

  // count occurrences of the CounterService finalize message
  size_t count = 0;
  std::string needle = "CounterService: sample=TestSample entries=";
  size_t pos = out.find(needle);
  while (pos != std::string::npos) {
    ++count;
    pos = out.find(needle, pos + 1);
  }

  EXPECT_EQ(count, 1u) << "CounterService finalized more than once";

  // verify counter histogram exists in meta file (sanity)
  TFile f(metaPath.c_str(), "READ");
  ASSERT_FALSE(f.IsZombie());
  auto* counterHist = dynamic_cast<TH1D*>(f.Get("counter_intWeightSum_TestSample"));
  ASSERT_NE(counterHist, nullptr) << "counter_intWeightSum_TestSample histogram not found";
  f.Close();

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}
