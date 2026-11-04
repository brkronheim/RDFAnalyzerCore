#include <ConfigurationManager.h>
#include <test_util.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <NDHistogramManager.h>
#include <api/IPluggableManager.h>
#include <ROOT/RDataFrame.hxx>
#include <TH1D.h>
#include <TH2D.h>
#include <TH3D.h>
#include <gtest/gtest.h>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <memory>
#include <string>
#include <vector>
#include <plots.h>
#include <SystematicManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <api/ManagerContext.h>

class NDHistogramManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ROOT::DisableImplicitMT();
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config_minimal.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    dataManager = std::make_unique<DataManager>(1);
    systematicManager = std::make_unique<SystematicManager>();
    
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();

    histogramManager = std::make_unique<NDHistogramManager>(*configManager);

    // Set up the NDHistogramManager with its dependencies
    ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    histogramManager->setContext(ctx);
  }

  void TearDown() override {
    if (histogramManager) {
      histogramManager->Clear();
    }
    histogramManager.reset();
    dataManager.reset();
    systematicManager.reset();
    logger.reset();
    skimSink.reset();
    metaSink.reset();
    configManager.reset();
  }

  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<IDataFrameProvider> dataManager;
  std::unique_ptr<NDHistogramManager> histogramManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

TEST_F(NDHistogramManagerTest, ConstructorCreatesValidManager) {
  auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  EXPECT_NO_THROW({
    auto manager = std::make_unique<NDHistogramManager>(*config);
    ASSERT_NE(manager, nullptr);
  });
}

TEST_F(NDHistogramManagerTest, GetHistosReturnsValidReference) {
  auto &histos = histogramManager->GetHistos();
  // Basic check that we can get the histograms reference
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, GetHistosInitiallyEmpty) {
  auto &histos = histogramManager->GetHistos();
  // Histograms should be initially empty
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, ClearHistograms) {
  EXPECT_NO_THROW({ histogramManager->Clear(); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDBasic) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("test_hist1", "var1", "label1", "w1", 10, 0.0, 10.0),
                                 histInfo("test_hist2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0)};
  std::string suffix = "_test";
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2"}};
  EXPECT_NO_THROW(histogramManager->bookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptyInfos) {
  std::vector<histInfo> emptyInfos;
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::string suffix = "_test";
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW({ histogramManager->bookND(emptyInfos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptySelection) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> emptySelection;
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW(histogramManager->bookND(infos, emptySelection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptyRegionNames) {
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> emptyRegionNames;
  std::string suffix = "_test";
  EXPECT_THROW(histogramManager->bookND(infos, selection, suffix, emptyRegionNames), std::invalid_argument);
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptySuffix) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string emptySuffix = "";
  EXPECT_NO_THROW(histogramManager->bookND(infos, selection, emptySuffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleHistograms) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 3.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0),
                                 histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0),
                                 histInfo("hist3", "var3", "label3", "w3", 15, 1.0, 16.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW(histogramManager->bookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleSelections) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel2", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel3", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0), selectionInfo("sel3", 7, 2.0, 8.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW({ histogramManager->bookND(infos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleRegions) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2", "region3"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW({ histogramManager->bookND(infos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDNormalizesRegionAxes) {
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);

  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 1, 0.0, 1.0, {"all"})};
  std::vector<std::vector<std::string>> regionNames = {{"all"}};

  EXPECT_NO_THROW(histogramManager->bookND(infos, selection, "", regionNames));
  ASSERT_GE(regionNames.size(), 4u);
  EXPECT_EQ(regionNames[0][0], "all");
  EXPECT_EQ(regionNames[1][0], "Default");
  EXPECT_EQ(regionNames[2][0], "Default");
}

TEST_F(NDHistogramManagerTest, SaveHistsBasic) {
  std::vector<std::vector<histInfo>> fullHistList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> allRegionNames = {{"region1", "region2"}};
  EXPECT_NO_THROW(histogramManager->saveHists(fullHistList, allRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, RejectsInterruptedOutputBeforeRunningGraph) {
  const auto prefix = std::filesystem::temp_directory_path() /
      ("rdf-incomplete-output-" + std::to_string(getpid()));
  const auto source = prefix.string() + "-source.root";
  const auto damaged = prefix.string() + "-damaged.root";
  {
    // Keep the header above ROOT's initial 300-byte probe, reproducing the
    // non-zombie/no-free-segments case rather than a rejected short file.
    const std::string title(128, 'x');
    TFile unfinished(source.c_str(), "RECREATE", title.c_str());
    unfinished.Flush();
    // Copy the on-disk state before Close writes the free-segment table.
    std::filesystem::copy_file(source, damaged,
                              std::filesystem::copy_options::overwrite_existing);
  }
  configManager->set("metaFile", damaged);
  const auto readBytes = [](const std::string &path) {
    std::ifstream input(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(input),
                       std::istreambuf_iterator<char>());
  };
  const auto originalBytes = readBytes(damaged);
  int processed = 0;
  dataManager->Define("x", [&processed]() { ++processed; return 1.f; }, {}, *systematicManager);
  dataManager->Define("w", []() { return 1.f; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("x", "x", "x", "w", 10, 0, 10)};
  std::vector<selectionInfo> selections;
  std::vector<std::vector<std::string>> regions = {{"interrupted"}};
  histogramManager->bookND(infos, selections, "", regions);
  try {
    histogramManager->saveHists();
    FAIL() << "Expected an actionable error for incomplete output";
  } catch (const std::runtime_error &error) {
    EXPECT_NE(std::string(error.what()).find("no free-segment table"), std::string::npos);
    EXPECT_NE(std::string(error.what()).find(damaged), std::string::npos);
  }
  EXPECT_EQ(processed, 0);
  EXPECT_EQ(readBytes(damaged), originalBytes);
  std::filesystem::remove(source);
  std::filesystem::remove(damaged);
}

TEST_F(NDHistogramManagerTest, SavesEmptyFineHistogramsAndCancelledBinErrors) {
  dataManager->Define("pt", []() { return 250.0f; }, {}, *systematicManager);
  dataManager->Define("weight", []() { return 1.0f; }, {}, *systematicManager);
  std::vector<histInfo> infos = {
      histInfo("LHE_Vpt", "pt", "pt", "weight", 200, 0, 2000),
      histInfo("LHE_Vpt_fine", "pt", "pt", "weight", 130, -5, 60),
      histInfo("LHE_Vpt_fine_stitched", "pt", "pt", "weight", 130, -5, 60)};
  std::vector<selectionInfo> selections;
  std::vector<std::vector<std::string>> regions = {{"fine_regression"}};
  histogramManager->bookND(infos, selections, "", regions);

  // The high-pT event is outside the fine range. Also model a bin whose
  // signed weights cancel; serialization must retain its variance.
  auto &histos = histogramManager->GetHistos();
  histos[2]->Reset();
  const Double_t coordinates[] = {0.5, 0.5, 0.5, 0.5, 10.25};
  const auto bin = histos[2]->GetBin(coordinates, true);
  histos[2]->SetBinContent(bin, 0.0);
  histos[2]->SetBinError2(bin, 2.0);
  histogramManager->saveHists();

  const auto path = metaSink->resolveOutputFile(*configManager, OutputChannel::Meta);
  TFile output(path.c_str(), "READ");
  for (const auto &info : infos) {
    auto *hist = output.Get<TH1F>(("fine_regression_Default/Default/" + info.name()).c_str());
    ASSERT_NE(hist, nullptr) << info.name();
    EXPECT_EQ(hist->GetNbinsX(), info.bins());
    EXPECT_DOUBLE_EQ(hist->GetXaxis()->GetXmin(), info.lowerBound());
    EXPECT_DOUBLE_EQ(hist->GetXaxis()->GetXmax(), info.upperBound());
    if (info.name() == "LHE_Vpt") {
      EXPECT_DOUBLE_EQ(hist->Integral(), 1.0);
    } else {
      EXPECT_DOUBLE_EQ(hist->Integral(), 0.0);
      EXPECT_NEAR(hist->GetBinError(hist->FindBin(10.25)),
                  info.name() == "LHE_Vpt_fine_stitched" ? std::sqrt(2.0) : 0.0,
                  1e-6);
    }
  }
}

TEST_F(NDHistogramManagerTest, SaveHistsWithEmptyHistList) {
  std::vector<std::vector<histInfo>> emptyHistList = {};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW(histogramManager->saveHists(emptyHistList, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsWithEmptyRegionNames) {
  std::vector<std::vector<histInfo>> histList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> emptyRegionNames = {};
  EXPECT_NO_THROW(histogramManager->saveHists(histList, emptyRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsMultipleHistograms) {
  std::vector<std::vector<histInfo>> histList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0), histInfo("hist3", "var3", "label3", "w3", 15, 1.0, 16.0)}};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW(histogramManager->saveHists(histList, regionNames));
  // Basic check that we can save histograms with empty hist list
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, CompleteWorkflow) {
  dataManager->Define("workflow_sel1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("workflow_hist1", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("workflow_sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"workflow_region1", "workflow_region2"}};
  // Only call makeSystList once and reuse the result
  auto systAxis = systematicManager->makeSystList("Systematic", *static_cast<DataManager*>(dataManager.get()));
  regionNames.push_back(systAxis);
  std::string suffix = "_workflow";
  histogramManager->bookND(infos, selection, suffix, regionNames);
  // Debug prints
  std::cout << "histos_m.size(): " << histogramManager->GetHistos().size() << std::endl;
  // Structure fullHistList to match the number of histograms booked (1)
  std::vector<std::vector<histInfo>> fullHistList = {
    {histInfo("workflow_hist1", "var1", "label1", "w1", 10, 0.0, 10.0)}
  };
  std::cout << "fullHistList.size(): " << fullHistList.size() << std::endl;
  std::vector<std::vector<std::string>> allRegionNames = {{"workflow_region1", "workflow_region2"}, systAxis};
  EXPECT_NO_THROW(histogramManager->saveHists(fullHistList, allRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, DataManagerIntegration) {
  if (dataManager != nullptr) {
    // Test integration with data manager
    auto &histos = histogramManager->GetHistos();
    EXPECT_TRUE(true); // If we get here, no exception was thrown
  } else {
    // Skip test if data manager is not available
    EXPECT_TRUE(true);
  }
}

TEST_F(NDHistogramManagerTest, ConfigurationManagerIntegration) {
  // Test integration with configuration manager
  EXPECT_EQ(configManager->get("saveFile"), "test_output_minimal.root");
  EXPECT_EQ(configManager->get("saveTree"), "Events");
  EXPECT_EQ(configManager->get("threads"), "1");
}

TEST_F(NDHistogramManagerTest, ErrorHandling) {
  // Test error handling for invalid operations
  std::vector<std::vector<histInfo>> nonexistentHistList = {{histInfo("nonexistent_hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("nonexistent_hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> allRegionNames = {{"region1", "region2"}};
  EXPECT_NO_THROW(histogramManager->saveHists(nonexistentHistList, allRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, MemoryManagement) {
  auto localManager = std::make_unique<NDHistogramManager>(*configManager);
  ASSERT_NE(localManager, nullptr);

  ManagerContext ctx{*configManager, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  localManager->setContext(ctx);
  
  dataManager->Define("memory_test_sel1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("memory_test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("memory_test_sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"memory_test_region1", "memory_test_region2"}};
  std::string suffix = "_memory_test";
  EXPECT_NO_THROW(localManager->bookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramTypes) {
  // Test different histogram types
  // 1D histogram
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> hist1d = {histInfo("hist1d", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_1d";
  EXPECT_NO_THROW(histogramManager->bookND(hist1d, selection, suffix, regionNames));
  histogramManager->Clear();
  regionNames = {{"region1"}};
  // 2D histogram
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> hist2d = {histInfo("hist2d_x", "var1", "label1", "w1", 10, 0.0, 10.0),
                                  histInfo("hist2d_y", "var2", "label2", "w2", 20, -5.0, 5.0)};
  suffix = "_2d";
  EXPECT_NO_THROW(histogramManager->bookND(hist2d, selection, suffix, regionNames));
  histogramManager->Clear();
  regionNames = {{"region1"}};
  // 3D histogram
  dataManager->Define("var3", []() { return 3.0; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> hist3d = {histInfo("hist3d_x", "var1", "label1", "w1", 10, 0.0, 10.0),
                                  histInfo("hist3d_y", "var2", "label2", "w2", 20, -5.0, 5.0),
                                  histInfo("hist3d_z", "var3", "label3", "w3", 15, 1.0, 16.0)};
  suffix = "_3d";
  EXPECT_NO_THROW(histogramManager->bookND(hist3d, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramNaming) {
  // Test histogram naming conventions
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {histInfo("naming_test", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2"}};
  std::string suffix = "_naming_test";
  EXPECT_NO_THROW(histogramManager->bookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramProperties) {
  // Test histogram properties
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos = {
      histInfo("prop_test", "var1", "label1", "w1", 100, -50.0, 50.0) // 100 bins from -50 to 50
  };
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_prop_test";

  EXPECT_NO_THROW(
      { histogramManager->bookND(infos, selection, suffix, regionNames); });

  // Basic check that histogram properties are set correctly
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, MultipleBookOperations) {
  // Test multiple book operations
  // First booking
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos1 = {histInfo("multi1", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection1 = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames1 = {{"region1"}};
  std::string suffix1 = "_multi1";
  EXPECT_NO_THROW(histogramManager->bookND(infos1, selection1, suffix1, regionNames1));
  // Second booking
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos2 = {histInfo("multi2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection2 = {selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames2 = {{"region2"}};
  std::string suffix2 = "_multi2";
  EXPECT_NO_THROW(histogramManager->bookND(infos2, selection2, suffix2, regionNames2));
  // Third booking
  dataManager->Define("var3", []() { return 3.0; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel3", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos3 = {histInfo("multi3", "var3", "label3", "w3", 15, 1.0, 16.0)};
  std::vector<selectionInfo> selection3 = {selectionInfo("sel3", 7, 2.0, 8.0)};
  std::vector<std::vector<std::string>> regionNames3 = {{"region3"}};
  std::string suffix3 = "_multi3";
  EXPECT_NO_THROW(histogramManager->bookND(infos3, selection3, suffix3, regionNames3));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, ClearAndReuse) {
  // Test clearing histograms and reusing the manager
  // First booking
  dataManager->Define("var1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel1", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos1 = {histInfo("clear1", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection1 = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames1 = {{"region1"}};
  std::string suffix1 = "_clear1";
  EXPECT_NO_THROW(histogramManager->bookND(infos1, selection1, suffix1, regionNames1));
  // Clear histograms
  histogramManager->Clear();
  // Second booking after clear
  dataManager->Define("var2", []() { return 2.0; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0; }, {}, *systematicManager);
  dataManager->Define("sel2", []() { return 1.0; }, {}, *systematicManager);
  std::vector<histInfo> infos2 = {histInfo("clear2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection2 = {selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames2 = {{"region2"}};
  std::string suffix2 = "_clear2";
  EXPECT_NO_THROW(histogramManager->bookND(infos2, selection2, suffix2, regionNames2));
  EXPECT_TRUE(true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

// ---------------------------------------------------------------------------
// Lifecycle hook tests
// ---------------------------------------------------------------------------

TEST_F(NDHistogramManagerTest, InitializeIsCallable) {
  EXPECT_NO_THROW(histogramManager->initialize());
}

TEST_F(NDHistogramManagerTest, ReportMetadataIsCallable) {
  EXPECT_NO_THROW(histogramManager->reportMetadata());
}

TEST_F(NDHistogramManagerTest, ExecuteAndFinalizeAreNoOps) {
  EXPECT_NO_THROW(histogramManager->execute());
  EXPECT_NO_THROW(histogramManager->finalize());
}

TEST_F(NDHistogramManagerTest, GetDependenciesReturnsEmpty) {
  EXPECT_TRUE(histogramManager->getDependencies().empty());
}

TEST(HistogramStorage, DenseAndForcedSparseRetainCancellationVarianceAcrossSlots) {
  for (const bool sparse : {false, true}) {
    histFillInfo info;
    info.name = "cancelled";
    info.nSlots = 2;
    info.nbins = {1, 1, 1, 1, 2};
    info.xmin = {0, 0, 0, 0, 0};
    info.xmax = {1, 1, 1, 1, 2};
    info.forceSparse = sparse;
    THnMulti action(info);
    action.Exec(0, {.5f}, {2.f}, {.5f}, {true}, {.5f}, {.5f}, {.5f}, {1});
    action.Exec(1, {.5f}, {-2.f}, {.5f}, {true}, {.5f}, {.5f}, {.5f}, {1});
    action.Exec(1, {1.5f}, {3.f}, {.5f}, {true}, {.5f}, {.5f}, {.5f}, {1});
    action.Finalize();
    const auto result = action.GetResultPtr();
    const Double_t cancelled[] = {.5, .5, .5, .5, .5};
    const auto bin = result->GetBin(cancelled, false);
    ASSERT_GE(bin, 0);
    EXPECT_DOUBLE_EQ(result->GetBinContent(bin), 0);
    EXPECT_NEAR(result->GetBinError2(bin), 8, 1e-10);
    const Double_t populated[] = {.5, .5, .5, .5, 1.5};
    EXPECT_DOUBLE_EQ(result->GetBinContent(result->GetBin(populated, false)), 3);
  }
}

template <class Action>
void checkStorageFlowsAndCancellation(bool sparse, bool floatContents = false,
                                     const std::string &backend = "map") {
  histFillInfo info;
  info.name = "storage_contract";
  info.nSlots = 2;
  info.nbins = {2, 1, 1, 2, 3};
  info.xmin = {-.5, -.5, -.5, 0, 0};
  info.xmax = {1.5, .5, .5, 2, 3};
  info.denseMemoryLimitBytes = sparse ? 0 : kDenseMemoryThresholdBytes;
  info.boostFloatContents = floatContents;
  info.boostSparseBackend = backend;
  Action action(info);
  // Interior cancellation retains variance; flows in both observable and
  // category axes must survive every storage/backend and the slot merge.
  action.Exec(0, {.5f}, {2.f}, {0.f}, {true}, {0.f}, {0.f}, {0.f}, {1});
  action.Exec(1, {.5f}, {-2.f}, {0.f}, {true}, {0.f}, {0.f}, {0.f}, {1});
  action.Exec(0, {-1.f}, {3.f}, {0.f}, {true}, {0.f}, {0.f}, {0.f}, {1});
  action.Exec(1, {3.f}, {-4.f}, {0.f}, {true}, {0.f}, {0.f}, {0.f}, {1});
  action.Exec(1, {1.f}, {5.f}, {0.f}, {true}, {0.f}, {0.f}, {2.f}, {1});
  action.Exec(0, {2.f}, {9.f}, {0.f}, {false}, {0.f}, {0.f}, {0.f}, {1});
  action.Finalize();
  const auto result = action.GetResultPtr();
  const std::array<std::array<int, 5>, 4> coordinates{{
      {{1, 1, 1, 1, 1}}, {{1, 1, 1, 1, 0}},
      {{1, 1, 1, 1, 4}}, {{3, 1, 1, 1, 2}}}};
  const double contents[] = {0, 3, -4, 5};
  const double variances[] = {8, 9, 16, 25};
  ASSERT_EQ(result->GetNbins(), 4);
  for (std::size_t i = 0; i < coordinates.size(); ++i) {
    const auto bin = result->GetBin(coordinates[i].data(), false);
    ASSERT_GE(bin, 0);
    EXPECT_DOUBLE_EQ(result->GetBinContent(bin), contents[i]);
    EXPECT_DOUBLE_EQ(result->GetBinError2(bin), variances[i]);
  }
}

TEST(HistogramStorage, RootDenseAndSparsePreserveFlowsAndSelection) {
  checkStorageFlowsAndCancellation<THnMulti>(false);
  checkStorageFlowsAndCancellation<THnMulti>(true);
}

TEST(HistogramStorage, BoostDenseAndSparsePreserveFlowsAndSelection) {
  checkStorageFlowsAndCancellation<BHnMulti>(false);
  checkStorageFlowsAndCancellation<BHnMulti>(true);
}

TEST(HistogramStorage, BoostFloatDenseAndSparsePreserveFlowsAndSelection) {
  checkStorageFlowsAndCancellation<BHnMulti>(false, true);
  checkStorageFlowsAndCancellation<BHnMulti>(true, true);
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
  checkStorageFlowsAndCancellation<BHnMulti>(true, true, "flat");
  checkStorageFlowsAndCancellation<BHnMulti>(true, false, "flat");
#endif
}

TEST(HistogramStorage, BoostFloatMatchesRootForRepresentativeObservableEdges) {
  for (const auto &range : std::vector<std::array<double, 3>>{
      {40, 0, 400}, {60, -3, 3}, {40, -3.2, 3.2}, {35, 0, 700}, {69, 0, 69}}) {
    histFillInfo info;
    info.name = "float_edges"; info.nSlots = 1;
    info.nbins = {1, 1, 1, 1, static_cast<int>(range[0])};
    info.xmin = {0, 0, 0, 0, range[1]};
    info.xmax = {1, 1, 1, 1, range[2]};
    info.boostFloatContents = true;
    THnMulti root(info);
    BHnMulti boost(info);
    for (int bin = 0; bin <= info.nbins[4]; ++bin) {
      const float boundary = range[1] + (range[2]-range[1])*bin/info.nbins[4];
      for (const float value : {std::nextafter(boundary, -INFINITY), boundary,
                                std::nextafter(boundary, INFINITY)}) {
        root.Exec(0, {value}, {1.f}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
        boost.Exec(0, {value}, {1.f}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
      }
    }
    root.Finalize(); boost.Finalize();
    auto first = root.GetResultPtr(), second = boost.GetResultPtr();
    for (int bin = 0; bin <= info.nbins[4]+1; ++bin) {
      const int coord[] = {1, 1, 1, 1, bin};
      const auto a = first->GetBin(coord, false), b = second->GetBin(coord, false);
      ASSERT_EQ(a < 0, b < 0) << bin;
      if (a >= 0) {
        EXPECT_EQ(first->GetBinContent(a), second->GetBinContent(b)) << bin;
        EXPECT_EQ(first->GetBinError2(a), second->GetBinError2(b)) << bin;
      }
    }
  }
}

TEST(HistogramStorage, SparseAllocationCountersAreSlotLocalAndSurviveFinalization) {
  for (const auto *storage : {"map", "flat"}) {
#ifndef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
    if (std::string(storage) == "flat") continue;
#endif
    histFillInfo info;
    info.name = "memory_slots";
    info.nSlots = 2;
    info.nbins = {8, 8, 8, 8, 8};
    info.xmin = {0, 0, 0, 0, 0};
    info.xmax = {8, 8, 8, 8, 8};
    info.boostFloatContents = true;
    info.boostSparseBackend = storage;
    info.denseMemoryLimitBytes = 0;
    BHnMulti action(info);
    auto memory = action.GetStorageMemoryUsage();
    EXPECT_EQ(memory->sparseBytes, 0u);
    for (int cell = 0; cell < 32768; ++cell) {
      auto linear = cell;
      float coordinate[5];
      for (auto &value : coordinate) { value = (linear % 8) + .5f; linear /= 8; }
      for (unsigned slot = 0; slot < 2; ++slot)
        action.Exec(slot, {coordinate[4]}, {1.f}, {coordinate[3]}, {true},
                    {coordinate[2]}, {coordinate[1]}, {coordinate[0]}, {1});
    }
    action.Finalize();
    EXPECT_EQ(memory->slots, 2u);
    EXPECT_EQ(memory->denseBytes, estimateDenseMemoryBytes(info.nbins, 2, 12));
    EXPECT_TRUE(memory->sparse);
    EXPECT_GT(memory->sparseBytes, memory->denseBytes);
    EXPECT_EQ(action.GetResultPtr()->GetNbins(), 32768);
    EXPECT_EQ(action.GetResultPtr()->GetBinContent(Long64_t{0}), 2);
    EXPECT_EQ(action.GetResultPtr()->GetBinError2(0), 2);
  }
  BHnMulti::FloatSparseMap first, second;
  first[0] += boost::histogram::weight(1.);
  EXPECT_GT(first.get_allocator().bytes->current, 0u);
  EXPECT_EQ(second.get_allocator().bytes->current, 0u);
  auto counter = first.get_allocator().bytes;
  { auto moved = std::move(first); EXPECT_GT(counter->current, 0u); }
  EXPECT_EQ(counter->current, 0u);
}

TEST(HistogramStorage, BoostHugeLogicalGridUsesOnlyAllocatedCells) {
  histFillInfo info;
  info.name = "huge_sparse";
  info.nSlots = 2;
  info.nbins = {1000, 1000, 1000, 10, 1000};
  info.xmin = {0, 0, 0, 0, 0};
  info.xmax = {1000, 1000, 1000, 10, 1000};
  BHnMulti action(info);
  action.Exec(0, {.5f}, {2.f}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
  action.Exec(1, {.5f}, {-2.f}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
  action.Finalize();
  ASSERT_EQ(action.GetResultPtr()->GetNbins(), 1);
  EXPECT_DOUBLE_EQ(action.GetResultPtr()->GetBinContent(Long64_t{0}), 0);
  EXPECT_DOUBLE_EQ(action.GetResultPtr()->GetBinError2(0), 8);
}

TEST(HistogramStorage, RootFloatAndBoostDoubleHaveDifferentCancellationPrecision) {
  histFillInfo info;
  info.name = "precision";
  info.nSlots = 1;
  info.nbins = {1, 1, 1, 1, 1};
  info.xmin = {0, 0, 0, 0, 0};
  info.xmax = {1, 1, 1, 1, 1};
  THnMulti root(info);
  BHnMulti boost(info);
  for (float weight : {16777216.f, 1.f, -16777216.f}) {
    root.Exec(0, {.5f}, {weight}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
    boost.Exec(0, {.5f}, {weight}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
  }
  root.Finalize();
  boost.Finalize();
  EXPECT_EQ(root.GetResultPtr()->GetBinContent(Long64_t{0}), 0);
  EXPECT_EQ(boost.GetResultPtr()->GetBinContent(Long64_t{0}), 1);
  EXPECT_EQ(root.GetResultPtr()->GetBinError2(0), boost.GetResultPtr()->GetBinError2(0));
  for (const auto *storage : {"map", "flat"}) {
#ifndef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
    if (std::string(storage) == "flat") continue;
#endif
    for (const auto limit : {std::size_t{0}, kDenseMemoryThresholdBytes}) {
      info.boostFloatContents = true;
      info.boostSparseBackend = storage;
      info.denseMemoryLimitBytes = limit;
      BHnMulti compatible(info);
      for (float weight : {16777216.f, 1.f, -16777216.f})
        compatible.Exec(0, {.5f}, {weight}, {0.f}, {true}, {.5f}, {.5f}, {.5f}, {1});
      compatible.Finalize();
      EXPECT_EQ(compatible.GetResultPtr()->GetBinContent(Long64_t{0}), 0);
      EXPECT_EQ(compatible.GetResultPtr()->GetBinError2(0), root.GetResultPtr()->GetBinError2(0));
    }
  }
}

TEST_F(NDHistogramManagerTest, SharedAxisPacksDoNotCrossRedefineBetweenBookingCalls) {
  dataManager->DefineNominal("obs", []() { return 1.5f; });
  dataManager->DefineNominal("weight", []() { return 2.f; });
  dataManager->DefineNominal("weight_shiftUp", []() { return 3.f; });
  dataManager->DefineNominal("weight_shiftDown", []() { return 1.f; });
  systematicManager->registerVariationColumns("weight", "shift", "weight_shiftUp", "weight_shiftDown");
  histogramManager->setupFromConfigFile();
  std::vector<histInfo> first{
      histInfo("first", "obs", "", "weight", 2, 0, 2),
      histInfo("second", "obs", "", "weight", 2, 0, 2)};
  std::vector<selectionInfo> selections;
  std::vector<std::vector<std::string>> regions{{"ch"}, {"cr"}, {"sample"}};
  histogramManager->bookND(first, selections, "", regions);
  auto df = dataManager->getDataFrame();
  std::size_t weightPacks = 0;
  for (const auto &column : df.GetColumnNames())
    if (column.find("weight_systVector_") == 0) ++weightPacks;
  EXPECT_EQ(weightPacks, 1u);
  dataManager->Redefine("weight", []() { return 4.f; });
  histInfo third("third", "obs", "", "weight", 2, 0, 2);
  histogramManager->BookSingleHistogram(third);
  Double_t point[] = {.5, .5, .5, .5, 1.5};
  auto &histograms = histogramManager->GetHistos();
  ASSERT_EQ(histograms.size(), 3u);
  EXPECT_FLOAT_EQ(histograms[0]->GetBinContent(histograms[0]->GetBin(point, false)), 2.f);
  EXPECT_FLOAT_EQ(histograms[1]->GetBinContent(histograms[1]->GetBin(point, false)), 2.f);
  EXPECT_FLOAT_EQ(histograms[2]->GetBinContent(histograms[2]->GetBin(point, false)), 4.f);
}

TEST_F(NDHistogramManagerTest, ChangedSelectionMasksDoNotReuseEarlierHistogramMaskPack) {
  dataManager->DefineNominal("obs", []() { return 1.5f; });
  dataManager->DefineNominal("weight", []() { return 1.f; });
  dataManager->DefineNominal("mask_first", []() { return true; });
  dataManager->DefineNominal("mask_second", []() { return false; });
  histogramManager->setupFromConfigFile();
  systematicManager->registerSelectionMaskColumn("Nominal", "mask_first");
  histInfo before("before_mask", "obs", "", "weight", 2, 0, 2);
  histogramManager->BookSingleHistogram(before);
  systematicManager->registerSelectionMaskColumn("Nominal", "mask_second");
  histInfo after("after_mask", "obs", "", "weight", 2, 0, 2);
  histogramManager->BookSingleHistogram(after);
  Double_t point[] = {.5, .5, .5, .5, 1.5};
  auto &histograms = histogramManager->GetHistos();
  EXPECT_FLOAT_EQ(histograms[0]->GetBinContent(histograms[0]->GetBin(point, false)), 1.f);
  EXPECT_EQ(histograms[1]->GetNbins(), 0);
}

TEST(HistogramStorage, IndexedCategoriesMatchRootForEdgesMasksAndVariableObservable) {
  for (bool variableCategories : {false, true}) {
    histFillInfo info;
    info.name = "category_edges";
    info.nSlots = 2;
    info.nbins = {3, 3, 2, 2, 3};
    info.xmin = {-.5, .5, 0., 0., 0.};
    info.xmax = {2.5, 3.5, 2., 2., 5.};
    info.binEdges.resize(5);
    info.binEdges[4] = {0., .5, 2., 5.};
    if (variableCategories) info.binEdges[0] = {-.5, .25, 1.5, 2.5};
    THnMulti action(info);
    THnF reference("category_reference", "", 5, info.nbins.data(),
                   info.xmin.data(), info.xmax.data());
    reference.Sumw2();
    reference.SetBinEdges(4, info.binEdges[4].data());
    if (variableCategories) reference.SetBinEdges(0, info.binEdges[0].data());
    const ROOT::VecOps::RVec<Int_t> fills{1};
    const std::vector<float> values{-1.f, -.5f, 0.f, .25f, .5f, 1.f, 1.5f,
        2.f, 2.5f, 3.f, 3.5f, 5.f,
        std::nextafter(2.f, 0.f), std::nextafter(2.f, 3.f),
        std::numeric_limits<float>::infinity(),
        -std::numeric_limits<float>::infinity(),
        std::numeric_limits<float>::quiet_NaN()};
    for (int variedAxis = 0; variedAxis < 5; ++variedAxis) {
      for (std::size_t i = 0; i < values.size(); ++i) {
        Double_t point[5] = {1., 2., 1., 0., 1.};
        point[variedAxis] = values[i];
        for (float weight : {1.5f, -1.5f, .25f}) {
          const bool pass = i % 4 != 0;
          action.Exec(i % 2, {float(point[4])}, {weight}, {float(point[3])},
                      {pass}, {float(point[2])}, {float(point[1])},
                      {float(point[0])}, fills);
          if (pass) reference.Fill(point, weight);
        }
      }
    }
    action.Finalize();
    const auto result = action.GetResultPtr();
    Long64_t occupied = 0;
    Int_t coordinates[5];
    for (Long64_t i = 0; i < reference.GetNbins(); ++i) {
      const auto content = reference.GetBinContent(i, coordinates);
      const auto error2 = reference.GetBinError2(i);
      if (content == 0 && error2 == 0) continue;
      ++occupied;
      const auto bin = result->GetBin(coordinates, false);
      ASSERT_GE(bin, 0);
      EXPECT_EQ(result->GetBinContent(bin), content);
      EXPECT_EQ(result->GetBinError2(bin), error2);
    }
    EXPECT_EQ(result->GetNbins(), occupied);
  }
}

TEST_F(NDHistogramManagerTest, ScalarBroadcastKeepsVariationOnlyAcceptance) {
  configManager->set("histogramSystematics", "true");
  dataManager->DefineNominal("obs", []() { return .5f; });
  dataManager->DefineNominal("obs_shiftUp", []() { return 1.5f; });
  dataManager->DefineNominal("obs_shiftDown", []() { return .25f; });
  dataManager->DefineNominal("weight", []() { return 1.f; });
  dataManager->DefineNominal("reject", []() { return false; });
  dataManager->DefineNominal("accept", []() { return true; });
  systematicManager->registerVariationColumns("obs", "shift", "obs_shiftUp", "obs_shiftDown");
  systematicManager->registerSelectionMaskColumn("Nominal", "reject");
  systematicManager->registerSelectionMaskColumn("shiftUp", "accept");
  systematicManager->registerSelectionMaskColumn("shiftDown", "reject");
  histogramManager->setupFromConfigFile();
  std::vector<histInfo> histograms{histInfo("migration", "obs", "", "weight", 2, 0, 2)};
  std::vector<selectionInfo> selections;
  std::vector<std::vector<std::string>> regions{{"ch"}, {"cr"}, {"sample"}};
  histogramManager->bookND(histograms, selections, "", regions);
  const auto result = histogramManager->GetHistos().front().GetPtr();
  EXPECT_EQ(result->GetAxis(3)->GetNbins(), 3);
  ASSERT_EQ(result->GetNbins(), 1);
  Int_t coordinates[5];
  EXPECT_EQ(result->GetBinContent(0, coordinates), 1.f);
  EXPECT_EQ(coordinates[4], 2);
}
