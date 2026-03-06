#include <gtest/gtest.h>

#include <analyzer.h>

#include <TFile.h>
#include <TH1D.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
static std::string writeConfig(const std::string& path, const std::string& metaFile) {
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

TEST(AnalyzerManagerContextPersistence, CounterServiceCtxOutlivesCaller) {
  const std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/aux/test_analyzer_managercontext_config.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/test_analyzer_managercontext_meta.root";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());

  writeConfig(cfgPath, metaPath);

  Analyzer analyzer(cfgPath);

  // Define the same columns used by CounterService in the tests
  analyzer.Define("intCode", [](ULong64_t entry) { return static_cast<Int_t>(entry % 3); }, {"rdfentry_"});
  analyzer.Define("genWeight", [](ULong64_t entry) { return (entry % 2 == 0) ? 1.0f : 2.0f; }, {"rdfentry_"});

  // Add a pre-filter to trigger onPreFilter() behavior
  analyzer.Filter("intCode", [](Int_t code) { return code != 1; }, {"intCode"});

  // If Analyzer had passed a temporary ManagerContext to services, this would crash
  analyzer.save();

  TFile file(metaPath.c_str(), "READ");
  ASSERT_FALSE(file.IsZombie());

  auto* hist = dynamic_cast<TH1D*>(file.Get("counter_weightSignSum_TestSample"));
  ASSERT_NE(hist, nullptr);

  // basic sanity on histogram content (should be non-zero for our single-entry DF)
  EXPECT_GT(hist->GetBinContent(1), 0.0);

  file.Close();

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}
