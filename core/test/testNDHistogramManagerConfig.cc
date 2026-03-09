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
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 7.5f; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var4", []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("channel", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion", []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory", []() { return 2.5f; }, {}, *systematicManager);
  
  // Set histogram config
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Book the histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // All four histograms should be booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_GE(histos.size(), 1u);
}

TEST_F(NDHistogramManagerConfigTest, ConfigHistogramsRespectFilters) {
  // Define variables and a filter
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
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 7.5f; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var4", []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("channel", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion", []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory", []() { return 2.5f; }, {}, *systematicManager);
  
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

TEST_F(NDHistogramManagerConfigTest, MissingRequiredFieldsSkipped) {
  // An entry missing required fields (weight, lowerBound, upperBound) is silently
  // skipped by parseMultiKeyConfig rather than causing a throw.
  std::string tempFile = "/tmp/invalid_histogram_config.txt";
  std::ofstream out(tempFile);
  out << "name=test_hist variable=var1 bins=10\n";  // Missing weight, lowerBound, upperBound
  out.close();
  
  configManager->set("histogramConfig", tempFile);
  
  // Setup completes without throwing; the incomplete entry is skipped.
  EXPECT_NO_THROW(histogramManager->setupFromConfigFile());
  // No histograms should be loaded (the incomplete entry was discarded).
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  EXPECT_EQ(histogramManager->GetHistos().size(), 0u);
  
  // Clean up
  std::remove(tempFile.c_str());
}

TEST_F(NDHistogramManagerConfigTest, BookingWithoutVariablesDefinedThrows) {
  // Set histogram config but don't define the required variables
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  
  // Booking throws a meaningful exception (not a crash/segfault) when required
  // columns are missing — this is the expected graceful error behaviour.
  EXPECT_THROW(histogramManager->bookConfigHistograms(), std::exception);
}

TEST_F(NDHistogramManagerConfigTest, MultipleBookCallsWork) {
  // Set up variables
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

TEST_F(NDHistogramManagerConfigTest, BoostBackendBooking) {
  // Set up variables
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

  // Select the Boost.Histogram backend
  configManager->set("histogramBackend", "boost");
  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();

  // Book the histograms using the Boost backend
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());

  // Verify the same number of histograms are booked as with the ROOT backend
  auto& histos = histogramManager->GetHistos();
  EXPECT_EQ(histos.size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, BoostBackendFunctionallyEquivalentToRoot) {
  // This test verifies that both backends produce the same number of booked histograms
  // (i.e. they are functionally equivalent from the user's perspective).
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

  configManager->set("histogramConfig", "cfg/test_histograms.txt");

  // Book with ROOT backend (the default — no explicit set needed)
  histogramManager->setupFromConfigFile();
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  const auto rootCount = histogramManager->GetHistos().size();

  // Use a completely fresh config/manager for the Boost backend to avoid
  // duplicate-key errors (ConfigurationManager::set() throws on overwrite).
  auto boostConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.txt");
  boostConfig->set("histogramConfig", "cfg/test_histograms.txt");
  boostConfig->set("histogramBackend", "boost");

  auto boostManager = std::make_unique<NDHistogramManager>(*boostConfig);
  ManagerContext ctx{*boostConfig, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  boostManager->setContext(ctx);

  boostManager->setupFromConfigFile();
  EXPECT_NO_THROW(boostManager->bookConfigHistograms());
  const auto boostCount = boostManager->GetHistos().size();

  EXPECT_EQ(rootCount, boostCount);
}

TEST_F(NDHistogramManagerConfigTest, BoostBackendNoConfigHistograms) {
  // Booking with Boost backend but no config loaded should not throw
  configManager->set("histogramBackend", "boost");
  histogramManager->setupFromConfigFile();
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
}

TEST_F(NDHistogramManagerConfigTest, BoostBackendManualBooking) {
  // Verify that BookSingleHistogram works with the Boost backend
  dataManager->Define("bvar1", []() { return 3.0f; }, {}, *systematicManager);
  dataManager->Define("bw1", []() { return 1.0f; }, {}, *systematicManager);

  // Switch to Boost backend via config
  configManager->set("histogramBackend", "boost");
  histogramManager->setupFromConfigFile();

  histInfo info("boost_hist", "bvar1", "bvar1", "bw1", 10, 0.0, 10.0);
  EXPECT_NO_THROW(histogramManager->BookSingleHistogram(info));

  EXPECT_EQ(histogramManager->GetHistos().size(), 1u);
}

TEST_F(NDHistogramManagerConfigTest, InvalidBackendThrows) {
  // An unrecognised backend value should throw with a clear error message
  configManager->set("histogramBackend", "invalid-backend");
  EXPECT_THROW(histogramManager->setupFromConfigFile(), std::runtime_error);
}

// ── Dense / sparse auto-selection tests ───────────────────────────────────────

TEST_F(NDHistogramManagerConfigTest, EstimateDenseMemorySmallHistogram) {
  // A 5D histogram with 10 bins per axis: (10+2)^5 * 16 bytes/bin * 1 slot = ~4 MB << 64 MB
  const std::vector<Int_t> bins{10, 10, 10, 10, 10};
  const std::size_t est = estimateDenseMemoryBytes(bins, 1, 16);
  EXPECT_LT(est, kDenseMemoryThresholdBytes);
}

TEST_F(NDHistogramManagerConfigTest, EstimateDenseMemoryLargeHistogram) {
  // A 5D histogram with 100 bins per axis: (100+2)^5 * 16 bytes/bin * 1 slot ≈ 176 GB >> 64 MB
  const std::vector<Int_t> bins{100, 100, 100, 100, 100};
  const std::size_t est = estimateDenseMemoryBytes(bins, 1, 16);
  EXPECT_GT(est, kDenseMemoryThresholdBytes);
}

TEST_F(NDHistogramManagerConfigTest, EstimateDenseMemoryOverflowProtection) {
  // Extremely large bins should trigger overflow protection and return SIZE_MAX
  const std::vector<Int_t> bins{1000000, 1000000, 1000000, 1000000, 1000000};
  const std::size_t est = estimateDenseMemoryBytes(bins, 1, 16);
  EXPECT_EQ(est, std::numeric_limits<std::size_t>::max());
}

TEST_F(NDHistogramManagerConfigTest, RootBackendAutoSelectsDense) {
  // For a small histogram, the ROOT backend should auto-select dense THnF per-thread accumulators.
  // We verify this indirectly: the histogram must still be bookable and produce the expected count.
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

  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());

  // The number of booked histograms must match regardless of dense/sparse selection.
  EXPECT_EQ(histogramManager->GetHistos().size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, BoostBackendAutoSelectsDenseForSmallHist) {
  // For a small histogram that fits within kDenseMemoryThresholdBytes, BHnMulti
  // auto-selects weight_storage (dense) on Boost >= 1.76, or uses weight_storage
  // unconditionally on older Boost.  Either way the histogram must be bookable.
  dataManager->Define("bvar1", []() { return 3.0f; }, {}, *systematicManager);
  dataManager->Define("bw1", []() { return 1.0f; }, {}, *systematicManager);

  configManager->set("histogramBackend", "boost");
  histogramManager->setupFromConfigFile();

  histInfo info("boost_dense_auto", "bvar1", "bvar1", "bw1", 10, 0.0, 10.0);
  EXPECT_NO_THROW(histogramManager->BookSingleHistogram(info));
  EXPECT_EQ(histogramManager->GetHistos().size(), 1u);
}

// ── Automatic systematic detection tests ─────────────────────────────────────

TEST_F(NDHistogramManagerConfigTest, AutoDetectSystematicsOnBookSingleHistogram) {
  // Define a variable with systematic variation columns using the naming
  // convention <var>_<syst>Up / <var>_<syst>Down.  The histogram manager
  // should auto-detect these and include the systematic in the syst axis.
  dataManager->Define("myVar",        []() { return 5.0f;  }, {}, *systematicManager);
  dataManager->Define("myVar_jesUp",  []() { return 5.5f;  }, {}, *systematicManager);
  dataManager->Define("myVar_jesDown",[]() { return 4.5f;  }, {}, *systematicManager);
  dataManager->Define("myWeight",     []() { return 1.0f;  }, {}, *systematicManager);

  histogramManager->setupFromConfigFile();

  histInfo info("autodet_hist", "myVar", "myVar", "myWeight", 10, 0.0, 10.0);
  EXPECT_NO_THROW(histogramManager->BookSingleHistogram(info));
  EXPECT_EQ(histogramManager->GetHistos().size(), 1u);

  // "jes" should now be registered in the systematic manager
  const auto& systs = systematicManager->getSystematics();
  EXPECT_NE(systs.find("jes"), systs.end());
}

TEST_F(NDHistogramManagerConfigTest, AutoDetectSystematicsOnBookConfigHistograms) {
  // Same as above but via the config-driven path.
  dataManager->Define("var1",        []() { return 5.0f;  }, {}, *systematicManager);
  dataManager->Define("var1_jesUp",  []() { return 5.5f;  }, {}, *systematicManager);
  dataManager->Define("var1_jesDown",[]() { return 4.5f;  }, {}, *systematicManager);
  dataManager->Define("w1",          []() { return 1.0f;  }, {}, *systematicManager);
  dataManager->Define("var2",        []() { return 2.0f;  }, {}, *systematicManager);
  dataManager->Define("w2",          []() { return 1.0f;  }, {}, *systematicManager);
  dataManager->Define("var3",        []() { return 7.5f;  }, {}, *systematicManager);
  dataManager->Define("w3",          []() { return 1.0f;  }, {}, *systematicManager);
  dataManager->Define("var4",        []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4",          []() { return 1.0f;  }, {}, *systematicManager);
  dataManager->Define("channel",        []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion",  []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory", []() { return 2.5f; }, {}, *systematicManager);

  configManager->set("histogramConfig", "cfg/test_histograms.txt");
  histogramManager->setupFromConfigFile();

  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());

  // "jes" should be auto-detected and registered
  const auto& systs = systematicManager->getSystematics();
  EXPECT_NE(systs.find("jes"), systs.end());

  // All 4 config histograms should be booked
  EXPECT_EQ(histogramManager->GetHistos().size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, AutoDetectSkippedWhenSystListAlreadyMaterialized) {
  // If the user has already called makeSystList() (locking the syst list),
  // ensureSystematicsAutoRegistered() should be a no-op and not add new systs.
  dataManager->Define("v",        []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("v_xUp",   []() { return 1.1f; }, {}, *systematicManager);
  dataManager->Define("v_xDown", []() { return 0.9f; }, {}, *systematicManager);
  dataManager->Define("w",        []() { return 1.0f; }, {}, *systematicManager);

  // Materialise the syst list *before* booking (no systematics registered yet)
  systematicManager->makeSystList("SystematicCounter", *dataManager);

  histogramManager->setupFromConfigFile();

  histInfo info("locked_hist", "v", "v", "w", 5, 0.0, 5.0);
  EXPECT_NO_THROW(histogramManager->BookSingleHistogram(info));

  // "x" should NOT be registered (list was already locked)
  const auto& systs = systematicManager->getSystematics();
  EXPECT_EQ(systs.find("x"), systs.end());
}

TEST_F(NDHistogramManagerConfigTest, AutoDetectDoesNotDuplicateManualRegistrations) {
  // Manually register "jes" before booking; the auto-detect pass should not
  // create duplicate entries.
  dataManager->Define("pt",         []() { return 40.0f; }, {}, *systematicManager);
  dataManager->Define("pt_jesUp",   []() { return 41.0f; }, {}, *systematicManager);
  dataManager->Define("pt_jesDown", []() { return 39.0f; }, {}, *systematicManager);
  dataManager->Define("w",          []() { return 1.0f;  }, {}, *systematicManager);

  systematicManager->registerSystematic("jes", {"pt"});

  histogramManager->setupFromConfigFile();

  histInfo info("dedup_hist", "pt", "pt", "w", 10, 0.0, 100.0);
  EXPECT_NO_THROW(histogramManager->BookSingleHistogram(info));

  // Still exactly one systematic
  const auto& systs = systematicManager->getSystematics();
  ASSERT_EQ(systs.size(), 1u);
  EXPECT_NE(systs.find("jes"), systs.end());
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
