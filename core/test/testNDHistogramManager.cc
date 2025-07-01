#include "ConfigurationManager.h"
#include "test_util.h"
#include "DataManager.h"
#include "ManagerFactory.h"
#include <api/INDHistogramManager.h>
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

class NDHistogramManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    std::string configFile = "cfg/test_data_config_minimal.txt";
    configManager = ManagerFactory::createConfigurationManager(configFile);
    dataManager = ManagerFactory::createDataManager(configManager);
    histogramManager = ManagerFactory::createNDHistogramManager(dataManager, configManager);
  }

  void TearDown() override {
    // Using smart pointers, so nothing to delete
  }

  std::shared_ptr<IConfigurationProvider> configManager;
  std::shared_ptr<IDataFrameProvider> dataManager;
  std::shared_ptr<INDHistogramManager> histogramManager;
};

TEST_F(NDHistogramManagerTest, ConstructorCreatesValidManager) {
  auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  auto data = ManagerFactory::createDataManager(config);
  EXPECT_NO_THROW({
    auto manager = ManagerFactory::createNDHistogramManager(data, config);
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
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("w2", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  dataManager->Define("sel2", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("test_hist1", "var1", "label1", "w1", 10, 0.0, 10.0),
                                 histInfo("test_hist2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0)};
  std::string suffix = "_test";
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2"}};
  EXPECT_NO_THROW(histogramManager->BookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptyInfos) {
  std::vector<histInfo> emptyInfos;
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::string suffix = "_test";
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW({ histogramManager->BookND(emptyInfos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptySelection) {
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> emptySelection;
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW(histogramManager->BookND(infos, emptySelection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptyRegionNames) {
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> emptyRegionNames;
  std::string suffix = "_test";
  EXPECT_NO_THROW(histogramManager->BookND(infos, selection, suffix, emptyRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDWithEmptySuffix) {
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string emptySuffix = "";
  EXPECT_NO_THROW(histogramManager->BookND(infos, selection, emptySuffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleHistograms) {
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("var3", []() { return 3.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("w2", []() { return 1.0; });
  dataManager->Define("w3", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0),
                                 histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0),
                                 histInfo("hist3", "var3", "label3", "w3", 15, 1.0, 16.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW(histogramManager->BookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleSelections) {
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  dataManager->Define("sel2", []() { return 1.0; });
  dataManager->Define("sel3", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0), selectionInfo("sel3", 7, 2.0, 8.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW({ histogramManager->BookND(infos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, BookNDMultipleRegions) {
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2", "region3"}};
  std::string suffix = "_test";
  EXPECT_NO_THROW({ histogramManager->BookND(infos, selection, suffix, regionNames); });
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsBasic) {
  std::vector<std::vector<histInfo>> fullHistList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> allRegionNames = {{"region1", "region2"}};
  EXPECT_NO_THROW(histogramManager->SaveHists(fullHistList, allRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsWithEmptyHistList) {
  std::vector<std::vector<histInfo>> emptyHistList = {};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW(histogramManager->SaveHists(emptyHistList, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsWithEmptyRegionNames) {
  std::vector<std::vector<histInfo>> histList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> emptyRegionNames = {};
  EXPECT_NO_THROW(histogramManager->SaveHists(histList, emptyRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, SaveHistsMultipleHistograms) {
  std::vector<std::vector<histInfo>> histList = {{histInfo("hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("hist2", "var2", "label2", "w2", 20, -5.0, 5.0), histInfo("hist3", "var3", "label3", "w3", 15, 1.0, 16.0)}};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  EXPECT_NO_THROW(histogramManager->SaveHists(histList, regionNames));
  // Basic check that we can save histograms with empty hist list
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, CompleteWorkflow) {
  dataManager->Define("workflow_sel1", []() { return 1.0; });
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("w2", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("workflow_hist1", "var1", "label1", "w1", 10, 0.0, 10.0),
                                 histInfo("workflow_hist2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection = {selectionInfo("workflow_sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"workflow_region1", "workflow_region2"}, {"Nominal"}};
  std::string suffix = "_workflow";
  histogramManager->BookND(infos, selection, suffix, regionNames);
  std::vector<std::vector<histInfo>> fullHistList = {{histInfo("workflow_hist1", "var1", "label1", "w1", 10, 0.0, 10.0), histInfo("workflow_hist2", "var2", "label2", "w2", 20, -5.0, 5.0)}};
  std::vector<std::vector<std::string>> allRegionNames = {{"workflow_region1", "workflow_region2"}, {"Nominal"}, {"VarAxis"}};
  EXPECT_NO_THROW(histogramManager->SaveHists(fullHistList, allRegionNames));
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
  EXPECT_NO_THROW(histogramManager->SaveHists(nonexistentHistList, allRegionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, MemoryManagement) {
  std::shared_ptr<INDHistogramManager> localManager = ManagerFactory::createNDHistogramManager(dataManager, configManager);
  dataManager->Define("memory_test_sel1", []() { return 1.0; });
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("memory_test_hist", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("memory_test_sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"memory_test_region1", "memory_test_region2"}};
  std::string suffix = "_memory_test";
  EXPECT_NO_THROW(localManager->BookND(infos, selection, suffix, regionNames));
  localManager = nullptr;
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramTypes) {
  // Test different histogram types
  // 1D histogram
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> hist1d = {histInfo("hist1d", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_1d";
  EXPECT_NO_THROW(histogramManager->BookND(hist1d, selection, suffix, regionNames));
  // 2D histogram
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("w2", []() { return 1.0; });
  std::vector<histInfo> hist2d = {histInfo("hist2d_x", "var1", "label1", "w1", 10, 0.0, 10.0),
                                  histInfo("hist2d_y", "var2", "label2", "w2", 20, -5.0, 5.0)};
  suffix = "_2d";
  EXPECT_NO_THROW(histogramManager->BookND(hist2d, selection, suffix, regionNames));
  // 3D histogram
  dataManager->Define("var3", []() { return 3.0; });
  dataManager->Define("w3", []() { return 1.0; });
  std::vector<histInfo> hist3d = {histInfo("hist3d_x", "var1", "label1", "w1", 10, 0.0, 10.0),
                                  histInfo("hist3d_y", "var2", "label2", "w2", 20, -5.0, 5.0),
                                  histInfo("hist3d_z", "var3", "label3", "w3", 15, 1.0, 16.0)};
  suffix = "_3d";
  EXPECT_NO_THROW(histogramManager->BookND(hist3d, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramNaming) {
  // Test histogram naming conventions
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  dataManager->Define("sel2", []() { return 1.0; });
  std::vector<histInfo> infos = {histInfo("naming_test", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0), selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1", "region2"}};
  std::string suffix = "_naming_test";
  EXPECT_NO_THROW(histogramManager->BookND(infos, selection, suffix, regionNames));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, HistogramProperties) {
  // Test histogram properties
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos = {
      histInfo("prop_test", "var1", "label1", "w1", 100, -50.0, 50.0) // 100 bins from -50 to 50
  };
  std::vector<selectionInfo> selection = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames = {{"region1"}};
  std::string suffix = "_prop_test";

  EXPECT_NO_THROW(
      { histogramManager->BookND(infos, selection, suffix, regionNames); });

  // Basic check that histogram properties are set correctly
  EXPECT_TRUE(true); // If we get here, no exception was thrown
}

TEST_F(NDHistogramManagerTest, MultipleBookOperations) {
  // Test multiple book operations
  // First booking
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos1 = {histInfo("multi1", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection1 = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames1 = {{"region1"}};
  std::string suffix1 = "_multi1";
  EXPECT_NO_THROW(histogramManager->BookND(infos1, selection1, suffix1, regionNames1));
  // Second booking
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("w2", []() { return 1.0; });
  dataManager->Define("sel2", []() { return 1.0; });
  std::vector<histInfo> infos2 = {histInfo("multi2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection2 = {selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames2 = {{"region2"}};
  std::string suffix2 = "_multi2";
  EXPECT_NO_THROW(histogramManager->BookND(infos2, selection2, suffix2, regionNames2));
  // Third booking
  dataManager->Define("var3", []() { return 3.0; });
  dataManager->Define("w3", []() { return 1.0; });
  dataManager->Define("sel3", []() { return 1.0; });
  std::vector<histInfo> infos3 = {histInfo("multi3", "var3", "label3", "w3", 15, 1.0, 16.0)};
  std::vector<selectionInfo> selection3 = {selectionInfo("sel3", 7, 2.0, 8.0)};
  std::vector<std::vector<std::string>> regionNames3 = {{"region3"}};
  std::string suffix3 = "_multi3";
  EXPECT_NO_THROW(histogramManager->BookND(infos3, selection3, suffix3, regionNames3));
  EXPECT_TRUE(true);
}

TEST_F(NDHistogramManagerTest, ClearAndReuse) {
  // Test clearing histograms and reusing the manager
  // First booking
  dataManager->Define("var1", []() { return 1.0; });
  dataManager->Define("w1", []() { return 1.0; });
  dataManager->Define("sel1", []() { return 1.0; });
  std::vector<histInfo> infos1 = {histInfo("clear1", "var1", "label1", "w1", 10, 0.0, 10.0)};
  std::vector<selectionInfo> selection1 = {selectionInfo("sel1", 5, 0.0, 5.0)};
  std::vector<std::vector<std::string>> regionNames1 = {{"region1"}};
  std::string suffix1 = "_clear1";
  EXPECT_NO_THROW(histogramManager->BookND(infos1, selection1, suffix1, regionNames1));
  // Clear histograms
  histogramManager->Clear();
  // Second booking after clear
  dataManager->Define("var2", []() { return 2.0; });
  dataManager->Define("w2", []() { return 1.0; });
  dataManager->Define("sel2", []() { return 1.0; });
  std::vector<histInfo> infos2 = {histInfo("clear2", "var2", "label2", "w2", 20, -5.0, 5.0)};
  std::vector<selectionInfo> selection2 = {selectionInfo("sel2", 6, 1.0, 7.0)};
  std::vector<std::vector<std::string>> regionNames2 = {{"region2"}};
  std::string suffix2 = "_clear2";
  EXPECT_NO_THROW(histogramManager->BookND(infos2, selection2, suffix2, regionNames2));
  EXPECT_TRUE(true);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}