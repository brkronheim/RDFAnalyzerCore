#include <gtest/gtest.h>

#include <analyzer.h>
#include <NDHistogramManager.h>
#include <test_util.h>

#include <TFile.h>
#include <TH1D.h>
#include <TCollection.h>

#include <filesystem>
#include <unistd.h>
#include <cstdio>
#include <fstream>
#include <string>

namespace {
std::string writeConfig(const std::string& path, const std::string& metaFile) {
  std::ofstream out(path);
  out << "enableCounters: true\n";
  out << "counterIntWeightBranch: intCode\n";
  out << "metaFile: " << metaFile << "\n";
  out << "sample: TestSample\n";
  out << "directory: test_data_minimal\n";
  out << "saveTree: Events\n";
  out << "saveFile: test_output_dedup.root\n";
  out << "saveDirectory: test_output_dedup/\n";
  out << "threads: 1\n";
  out.close();
  return path;
}
}

TEST(CounterServiceDedupTest, AnalyzerPlusNDHistogramManagerRunsCounterOnlyOnce) {
  ChangeToTestSourceDir();

  const std::string cfgPath = testWorkspacePath().string() + "/aux/test_counter_dedup_config.yaml";
  const std::string metaPath = testWorkspacePath().string() + "/aux/test_counter_dedup_meta.root";

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
  analyzer.run();

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

TEST(CounterServiceDedupTest, InclusiveServicesUseTheSameNodeWithProviderFiltersAndNoFilters) {
  ChangeToTestSourceDir();
  for (bool filter : {false, true}) {
    const auto directory = std::filesystem::temp_directory_path() /
        ("rdf-inclusive-" + std::to_string(getpid()) + (filter ? "-filter" : "-inclusive"));
    std::filesystem::create_directories(directory);
    const auto meta = directory / "meta.root";
    const auto config = directory / "analysis.yaml";
    {
      std::ofstream out(config);
      out << "directory: test_data_minimal\nsaveTree: Events\nthreads: 1\n"
          << "enableCounters: yes\ncounterIntWeightBranch: code\ncounterIntWeightBranchNBins: 5\n"
          << "enableTaggerEfficiency: true\ntaggerEfficiencyColumns: code\n"
          << "taggerEfficiencyWeight: w\nsample: inclusive\nmetaFile: " << meta.string() << "\n";
    }
    {
      Analyzer analysis(config.string());
      analysis.Define("code", [] { return 2; }, {});
      analysis.Define("w", [] { return 3.f; }, {});
      auto& provider = analysis.getDataFrameProvider();
      if (filter) provider.Filter([](int code) { return code < 0; }, {"code"});
      auto node = provider.getDataFrame();
      auto count = node.Count();
      analysis.run();
      EXPECT_EQ(count.GetValue(), filter ? 0u : 1u);
      EXPECT_EQ(node.GetNRuns(), 1u);
      TFile output(meta.c_str(), "READ");
      auto* tagger = output.Get<TH1D>("taggerEff_code_inclusive");
      auto* counter = output.Get<TH1D>("counter_intWeightSum_inclusive");
      ASSERT_NE(tagger, nullptr);
      ASSERT_NE(counter, nullptr);
      EXPECT_DOUBLE_EQ(tagger->Integral(0, tagger->GetNbinsX() + 1), 3.);
      EXPECT_DOUBLE_EQ(counter->GetEntries(), 1.);
      EXPECT_EQ(counter->GetNbinsX(), 5);
    }
    std::filesystem::remove_all(directory);
  }
}
