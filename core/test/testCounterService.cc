#include <gtest/gtest.h>

#include <ConfigurationManager.h>
#include <CounterService.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <RootOutputSink.h>
#include <SystematicManager.h>

#include <TFile.h>
#include <TH1D.h>
#include <RtypesCore.h>

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
  out << "fileList=\n";  // Empty fileList to satisfy DataManager requirement
  out.close();
  return path;
}
}

TEST(CounterServiceTest, UsesPreFilterDataFrameForIntWeightHistogram) {
  const std::string cfgPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_config.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/test_counter_meta.root";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());

  writeConfig(cfgPath, metaPath);
  ConfigurationManager config(cfgPath);

  DataManager dataManager(6);
  SystematicManager systematicManager;
  DefaultLogger logger;
  NullOutputSink skimSink;
  RootOutputSink metaSink;

  auto df = dataManager.getDataFrame();
  dataManager.Define("intCode",
                     [](ULong64_t entry) { return static_cast<Int_t>(entry % 3); },
                     {"rdfentry_"},
                     systematicManager);
  dataManager.Define("genWeight",
                     [](ULong64_t entry) { return (entry % 2 == 0) ? 1.0f : 2.0f; },
                     {"rdfentry_"},
                     systematicManager);

  ManagerContext ctx{config, dataManager, systematicManager, logger, skimSink, metaSink};
  CounterService service;
  service.initialize(ctx);

  auto preFilterDf = dataManager.getDataFrame();
  service.onPreFilter(preFilterDf);

  dataManager.Filter([](Int_t code) { return code != 1; }, {"intCode"});

  auto filteredDf = dataManager.getDataFrame();
  service.finalize(filteredDf);

  TFile file(metaPath.c_str(), "READ");
  ASSERT_FALSE(file.IsZombie());

  auto* hist = dynamic_cast<TH1D*>(file.Get("counter_intWeightSum_TestSample"));
  ASSERT_NE(hist, nullptr);

  const int bin0 = hist->FindBin(0.0);
  const int bin1 = hist->FindBin(1.0);
  const int bin2 = hist->FindBin(2.0);

  EXPECT_DOUBLE_EQ(hist->GetBinContent(bin0), 3.0);
  EXPECT_DOUBLE_EQ(hist->GetBinContent(bin1), 3.0);
  EXPECT_DOUBLE_EQ(hist->GetBinContent(bin2), 3.0);

  file.Close();

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries() – returns service runtime settings
// ---------------------------------------------------------------------------
TEST(CounterServiceTest, CollectProvenanceEntriesContainsSampleName) {
  const std::string cfgPath  = std::string(TEST_SOURCE_DIR) + "/aux/counter_prov_cfg.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/counter_prov_meta.root";

  {
    std::ofstream out(cfgPath);
    out << "sample=MySample\n";
    out << "metaFile=" << metaPath << "\n";
    out << "fileList=\n";
  }

  ConfigurationManager config(cfgPath);
  DataManager          dataManager(3);
  SystematicManager    systematicManager;
  DefaultLogger        logger;
  NullOutputSink       skimSink;
  NullOutputSink       metaSink;

  ManagerContext ctx{config, dataManager, systematicManager, logger,
                     skimSink, metaSink};

  CounterService svc;
  svc.initialize(ctx);

  const auto entries = svc.collectProvenanceEntries();

  auto it_sample = entries.find("service.counter.sample");
  auto it_weight = entries.find("service.counter.weight_branch");

  ASSERT_NE(it_sample, entries.end())
      << "service.counter.sample key missing";
  ASSERT_NE(it_weight, entries.end())
      << "service.counter.weight_branch key missing";

  EXPECT_EQ(it_sample->second, "MySample");
  EXPECT_TRUE(it_weight->second.empty())
      << "weight_branch should be empty when not configured";

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}

TEST(CounterServiceTest, FallsBackToStitchIdWhenSampleMissing) {
  const std::string cfgPath  = std::string(TEST_SOURCE_DIR) + "/aux/counter_stitch_cfg.txt";
  const std::string metaPath = std::string(TEST_SOURCE_DIR) + "/aux/counter_stitch_meta.root";

  {
    std::ofstream out(cfgPath);
    out << "stitch_id=7\n";
    out << "dtype=mc\n";
    out << "metaFile=" << metaPath << "\n";
    out << "fileList=\n";
  }

  ConfigurationManager config(cfgPath);
  DataManager          dataManager(1);
  SystematicManager    systematicManager;
  DefaultLogger        logger;
  NullOutputSink       skimSink;
  NullOutputSink       metaSink;

  ManagerContext ctx{config, dataManager, systematicManager, logger,
                     skimSink, metaSink};

  CounterService svc;
  svc.initialize(ctx);

  const auto entries = svc.collectProvenanceEntries();
  auto it_sample = entries.find("service.counter.sample");

  ASSERT_NE(it_sample, entries.end());
  EXPECT_EQ(it_sample->second, "7");

  std::remove(cfgPath.c_str());
  std::remove(metaPath.c_str());
}
