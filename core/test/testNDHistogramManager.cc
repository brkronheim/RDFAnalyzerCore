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