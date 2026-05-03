#include <gtest/gtest.h>

#include <analyzer.h>
#include <test_util.h>

#include <TFile.h>
#include <TH1D.h>

#include <cstdio>
#include <fstream>
#include <string>

namespace {
static std::string writeConfig(const std::string& path, const std::string& metaFile) {
  std::ofstream out(path);
  out << "enableCounters=true\n";
  out << "metaFile=" << metaFile << "\n";
  out << "sample=TestSample\n";
  out << "fileList=test_data_minimal/dummy.root\n";
  out << "saveTree=Events\n";
  out << "saveFile=test_analyzer_managercontext_skim.root\n";
  out.close();
  return path;
}
}

TEST(AnalyzerManagerContextPersistence, CounterServiceCtxOutlivesCaller) {
  ChangeToTestSourceDir();

  const std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/aux/test_analyzer_managercontext_config.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/test_analyzer_managercontext_meta.root";
  const std::string skimPath = std::string(TEST_SOURCE_DIR) + "/test_analyzer_managercontext_skim.root";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
  std::remove(skimPath.c_str());

  writeConfig(cfgPath, metaPath);

  Analyzer analyzer(cfgPath);

  // Add a pre-filter to trigger onPreFilter() behavior
  analyzer.Define("keep_event", [](Int_t dummy) { return dummy == 1; }, {"dummy"});
  analyzer.Filter("keep_event", [](bool keepEvent) { return keepEvent; }, {"keep_event"});

  // If Analyzer had passed a temporary ManagerContext to services, this would crash
  EXPECT_NO_THROW(analyzer.save());

  TFile skimFile(skimPath.c_str(), "READ");
  ASSERT_FALSE(skimFile.IsZombie());
  ASSERT_NE(skimFile.Get("Events"), nullptr);
  skimFile.Close();

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
  std::remove(skimPath.c_str());
}
