#include <ConfigurationManager.h>
#include <test_util.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <NDHistogramManager.h>
#include <api/IPluggableManager.h>
#include <ROOT/RDataFrame.hxx>
#include <TH1D.h>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <fstream>
#include <plots.h>
#include <SystematicManager.h>
#include <DefaultLogger.h>
#include <NullOutputSink.h>
#include <api/ManagerContext.h>

class NDHistogramManagerConfigTest : public ::testing::Test {
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

TEST_F(NDHistogramManagerConfigTest, SetupFromConfigFileNoHistogramConfig) {
  // Test that setupFromConfigFile works when no histogram config is specified
  EXPECT_NO_THROW(histogramManager->setupFromConfigFile());
}

TEST_F(NDHistogramManagerConfigTest, SetupFromConfigFileWithHistogramConfig) {
  // Add histogram config to the configuration
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  
  // Setup should parse the config file without errors
  EXPECT_NO_THROW(histogramManager->setupFromConfigFile());
}

TEST_F(NDHistogramManagerConfigTest, BookConfigHistogramsNoConfig) {
  // Booking with no config loaded should not throw
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
}

TEST_F(NDHistogramManagerConfigTest, BookConfigHistogramsWithConfig) {
  // Set up necessary variables for the histograms
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 7.5f; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var4", []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("channel", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion", []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory", []() { return 2.5f; }, {}, *systematicManager);
  
  // Set histogram config and load it
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Book the histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // Verify histograms were booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_EQ(histos.size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, BookConfigHistogramsWithDefaults) {
  // Set up variables
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  
  // Set histogram config with only a simple histogram
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Book the histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // At least the first histogram should be booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_GE(histos.size(), 1u);
}

TEST_F(NDHistogramManagerConfigTest, ConfigHistogramsRespectFilters) {
  // Define variables and a filter
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("pass_filter", []() { return true; }, {}, *systematicManager);
  
  // Apply a filter
  auto passFunc = [](bool pass) { return pass; };
  dataManager->Filter(passFunc, {"pass_filter"});
  
  // Set histogram config and load it
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Book histograms after filter
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // Histograms should be booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_GE(histos.size(), 1u);
}

TEST_F(NDHistogramManagerConfigTest, ConfigHistogramsWorkWithSystematics) {
  // Define variables with systematics
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  
  // Register a systematic
  systematicManager->registerSystematic("TestSyst", {"var1"});
  dataManager->Define("var1_TestSyst", []() { return 5.5f; }, {}, *systematicManager);
  
  // Set histogram config
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Book histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // Histograms should be booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_GE(histos.size(), 1u);
}

TEST_F(NDHistogramManagerConfigTest, InvalidConfigFileThrows) {
  // Set invalid histogram config file
  configManager->set("histogramConfig", "nonexistent_file.txt");
  
  // Setup should throw an exception
  EXPECT_THROW(histogramManager->setupFromConfigFile(), std::exception);
}

TEST_F(NDHistogramManagerConfigTest, MissingRequiredFieldsThrows) {
  // Create a config file with missing required fields
  std::string tempFile = "/tmp/invalid_histogram_config.txt";
  std::ofstream out(tempFile);
  out << "name=test_hist variable=var1 bins=10\n";  // Missing weight, lowerBound, upperBound
  out.close();
  
  configManager->set("histogramConfig", tempFile);
  
  // Setup should throw
  EXPECT_THROW(histogramManager->setupFromConfigFile(), std::exception);
  
  // Clean up
  std::remove(tempFile.c_str());
}

TEST_F(NDHistogramManagerConfigTest, BookingWithoutVariablesDefinedDoesNotCrash) {
  // Set histogram config but don't define the variables
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Booking should not crash, but may log warnings
  // The actual booking will fail during dataframe execution
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
}

TEST_F(NDHistogramManagerConfigTest, MultipleBookCallsWork) {
  // Set up variables
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0f; }, {}, *systematicManager);
  
  // Set histogram config
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Get initial histogram count
  auto initialCount = histogramManager->GetHistos().size();
  
  // Book histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  auto countAfterFirstBook = histogramManager->GetHistos().size();
  
  // Book again - this should add more histograms (duplicate booking allowed)
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  auto countAfterSecondBook = histogramManager->GetHistos().size();
  
  // Verify histograms were booked each time
  EXPECT_EQ(countAfterFirstBook, initialCount + 4u);  // 4 histograms in test config
  EXPECT_EQ(countAfterSecondBook, countAfterFirstBook + 4u);  // Another 4 from second call
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
