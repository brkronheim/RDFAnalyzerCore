#include <ConfigurationManager.h>
#include <test_util.h>
#include <DataManager.h>
#include <ManagerFactory.h>
#include <NDHistogramManager.h>
#include <RegionManager.h>
#include <api/IPluggableManager.h>
#include <ROOT/RDataFrame.hxx>
#include <TFile.h>
#include <TH1D.h>
#include <THnSparse.h>
#include <TNamed.h>
#include <TObjString.h>
#include <gtest/gtest.h>
#include <chrono>
#include <filesystem>
#include <memory>
#include <map>
#include <string>
#include <vector>
#include <fstream>
#include <unordered_map>
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
    std::string configFile = "cfg/test_data_config_minimal.yaml";
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

  void expectSparseMemoryWarning(const std::string &precision = "",
                                 const std::string &storage = "",
                                 const std::string &backend = "",
                                 const std::string &budget = "") {
    class RecordingLogger : public ILogger {
    public:
      std::vector<std::string> warnings;
      void log(Level level, const std::string &message) override {
        if (level == Level::Warn) warnings.push_back(message);
      }
    } recording;
    if (!precision.empty()) configManager->set("histogramBoostPrecision", precision);
    if (!storage.empty()) configManager->set("histogramBoostSparseBackend", storage);
    if (!backend.empty()) configManager->set("histogramBackend", backend);
    if (!budget.empty()) configManager->set("histogramDenseLimitMiB", budget);
    const auto output = (std::filesystem::temp_directory_path() /
        ("rdf-sparse-memory-" + std::to_string(getpid()) + ".root")).string();
    std::filesystem::remove(output);
    configManager->set("metaFile", output);
    ManagerContext context{*configManager, *dataManager, *systematicManager,
                           recording, *skimSink, *metaSink};
    histogramManager->setContext(context);
    // All 16^4 observable/category combinations, with nine weight variations,
    // occupy enough of the 5D grid to make either sparse map exceed dense.
    constexpr int bins = 16, cells = bins * bins * bins * bins;
    for (int axis = 0; axis < 4; ++axis) {
      dataManager->DefineNominal("coordinate" + std::to_string(axis), [axis, bins, cells]() {
        ROOT::VecOps::RVec<float> values(cells);
        int stride = 1;
        for (int i = 0; i < axis; ++i) stride *= bins;
        for (int i = 0; i < cells; ++i) values[i] = (i / stride) % bins + .5f;
        return values;
      });
    }
    dataManager->DefineNominal("memory_weight", [] { return 1.f; });
    for (int variation = 0; variation < 4; ++variation) {
      const auto name = "memory_scale" + std::to_string(variation);
      const auto up = "memory_weight_" + name + "Up";
      const auto down = "memory_weight_" + name + "Down";
      dataManager->DefineNominal(up, [] { return 1.f; });
      dataManager->DefineNominal(down, [] { return 1.f; });
      systematicManager->registerVariationColumns("memory_weight", name, up, down);
    }
    histogramManager->setupFromConfigFile();
    std::vector<histInfo> infos{
        histInfo("occupied", "coordinate0", "", "memory_weight", bins, 0, bins),
        histInfo("wide_sparse", "coordinate0", "", "memory_weight", 1000000, 0, 1000000)};
    std::vector<selectionInfo> selections{
        selectionInfo("coordinate1", bins, 0, bins), selectionInfo("coordinate2", bins, 0, bins),
        selectionInfo("coordinate3", bins, 0, bins)};
    std::vector<std::vector<std::string>> regions{{"channel"}, {"control"}, {"sample"}};
    histogramManager->bookND(infos, selections, "", regions);
    auto *histogram = histogramManager->GetHistos().front().GetPtr();
    EXPECT_TRUE(recording.warnings.empty()) << "Warning belongs to metadata saving, not finalization";
    ASSERT_NE(histogram, nullptr);
    EXPECT_EQ(histogram->GetNbins(), cells * 9);
    histogramManager->saveHists();
    const bool warn = backend != "root" && budget != "64";
    if (warn) {
      ASSERT_EQ(recording.warnings.size(), 1u);
      EXPECT_NE(recording.warnings[0].find("occupied_"), std::string::npos);
      EXPECT_NE(recording.warnings[0].find(output), std::string::npos);
      EXPECT_NE(recording.warnings[0].find("exceed the estimated dense arrays"), std::string::npos);
      EXPECT_NE(recording.warnings[0].find("histogramDenseLimitMiB"), std::string::npos);
    } else {
      EXPECT_TRUE(recording.warnings.empty());
    }
    histogramManager->saveHists();
    EXPECT_EQ(recording.warnings.size(), warn ? 1u : 0u) << "Repeated saves must not repeat warnings";
    {
      TFile saved(output.c_str(), "READ");
      auto *stored = saved.Get<THnSparseF>("nd_histograms_v1/occupied_");
      ASSERT_NE(stored, nullptr);
      EXPECT_EQ(stored->GetNbins(), cells * 9);
      const int point[] = {1, 1, 1, 1, 1};
      const auto bin = stored->GetBin(point, false);
      ASSERT_GE(bin, 0);
      EXPECT_EQ(stored->GetBinContent(bin), 1);
      EXPECT_EQ(stored->GetBinError2(bin), 1);
    }
    // Drop the context reference to the local logger before it is destroyed.
    ManagerContext original{*configManager, *dataManager, *systematicManager,
                            *logger, *skimSink, *metaSink};
    histogramManager->setContext(original);
    std::filesystem::remove(output);
  }

  void expectShapeVariationCanMigrateHistogramRegion(
      const std::string &backend = "") {
    dataManager->Define(
        "obs",
        []() { return ROOT::VecOps::RVec<Float_t>{1.0f, 2.0f}; }, {},
        *systematicManager);
    dataManager->Define("weight", []() { return 1.0f; }, {}, *systematicManager);
    dataManager->Define("channel", []() { return 0.5f; }, {}, *systematicManager);
    dataManager->Define("sampleCategory", []() { return 0.5f; }, {},
                        *systematicManager);
    dataManager->Define("controlRegion", []() { return 0.5f; }, {},
                        *systematicManager);
    dataManager->Define("controlRegion_jesUp", []() { return 1.5f; }, {},
                        *systematicManager);
    dataManager->Define("controlRegion_jesDown", []() { return 2.5f; }, {},
                        *systematicManager);
    systematicManager->registerVariationColumns(
        "controlRegion", "jes", "controlRegion_jesUp", "controlRegion_jesDown");

    if (!backend.empty()) {
      configManager->set("histogramBackend", backend);
    }
    histogramManager->setupFromConfigFile();
    histInfo info("region_migration", "obs", "obs", "weight", 5, 0.0f, 5.0f);
    EXPECT_NO_THROW(histogramManager->BookSingleHistogram(
        info, selectionInfo(),
        selectionInfo("controlRegion", 3, 0.0, 3.0, {"SR", "CR", "Top"}),
        selectionInfo("channel", 1, 0.0, 1.0, {"channel"})));

    ASSERT_EQ(histogramManager->GetHistos().size(), 1u);
    auto histogram = histogramManager->GetHistos().front().GetPtr();
    ASSERT_NE(histogram, nullptr);
    ASSERT_EQ(histogram->GetNdimensions(), 5);
    const auto variedRegions = dataManager->getDataFrame()
        .Take<ROOT::VecOps::RVec<Float_t>>(
            "controlRegion_systVector_region_migration_")
        .GetValue();
    ASSERT_EQ(variedRegions.size(), 1u);
    ASSERT_EQ(variedRegions.front().size(), 6u);
    EXPECT_FLOAT_EQ(variedRegions.front()[0], 0.5f);
    EXPECT_FLOAT_EQ(variedRegions.front()[1], 0.5f);
    EXPECT_FLOAT_EQ(variedRegions.front()[2], 1.5f);
    EXPECT_FLOAT_EQ(variedRegions.front()[3], 1.5f);
    EXPECT_FLOAT_EQ(variedRegions.front()[4], 2.5f);
    EXPECT_FLOAT_EQ(variedRegions.front()[5], 2.5f);

    // Axis 1 stores the control-region category and axis 3 the systematic.
    // ROOT's regular-bin coordinates start at one: Nominal, Up, and Down are
    // coordinates 1, 2, and 3 on the systematic axis.
    // The two synthetic fills each stay in SR nominally and move to CR / Top
    // under the Up / Down shape variations, respectively.
    std::map<std::pair<int, int>, float> regionYields;
    std::vector<Int_t> indices(histogram->GetNdimensions());
    for (Long64_t bin = 0; bin < histogram->GetNbins(); ++bin) {
      const float content = histogram->GetBinContent(bin, indices.data());
      if (content != 0.0f) {
        regionYields[{indices[1], indices[3]}] += content;
      }
    }
    EXPECT_FLOAT_EQ(regionYields.at(std::make_pair(1, 1)), 2.0f);
    EXPECT_FLOAT_EQ(regionYields.at(std::make_pair(2, 2)), 2.0f);
    EXPECT_FLOAT_EQ(regionYields.at(std::make_pair(3, 3)), 2.0f);
  }

  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<IDataFrameProvider> dataManager;
  std::unique_ptr<NDHistogramManager> histogramManager;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

namespace {

std::unordered_map<std::string, float> collectSystematicContents(
    THnSparseF &hist, const std::vector<std::string> &systList) {
  std::unordered_map<std::string, float> contents;
  std::vector<Int_t> indices(hist.GetNdimensions());

  for (Long64_t bin = 0; bin < hist.GetNbins(); ++bin) {
    const float content = hist.GetBinContent(bin, indices.data());
    if (content == 0.0f) {
      continue;
    }

    const int systematicIndex = indices[3] - 1;
    if (systematicIndex < 0 ||
        systematicIndex >= static_cast<int>(systList.size())) {
      continue;
    }

    contents[systList[systematicIndex]] += content;
  }

  return contents;
}

std::string findSingleColumnWithPrefix(ROOT::RDF::RNode df,
                                       const std::string &prefix) {
  const auto columns = df.GetColumnNames();
  std::string match;
  for (const auto &column : columns) {
    if (column.rfind(prefix, 0) != 0) {
      continue;
    }
    if (column.find("__defineVector_") != std::string::npos) {
      continue;
    }
    if (!match.empty()) {
      throw std::runtime_error("multiple columns matched prefix: " + prefix);
    }
    match = column;
  }
  if (match.empty()) {
    throw std::runtime_error("no column matched prefix: " + prefix);
  }
  return match;
}

void defineExplicitWeightVariationColumns(IDataFrameProvider &dataManager,
                                          SystematicManager &systematicManager) {
  dataManager.Define("obs", []() { return 5.0f; }, {}, systematicManager);
  dataManager.Define("weight_nominal", []() { return 1.0f; }, {},
                     systematicManager);
  dataManager.Define("weight_nominal_double_weight",
                     []() { return 2.0f; }, {}, systematicManager);
  dataManager.Define("weight_nominal_triple_weight",
                     []() { return 3.0f; }, {}, systematicManager);

  systematicManager.registerVariationColumn(
      "weight_nominal", "double_weight", "weight_nominal_double_weight");
  systematicManager.registerVariationColumn(
      "weight_nominal", "triple_weight", "weight_nominal_triple_weight");
}

void definePdfStyleWeightVariationColumns(IDataFrameProvider &dataManager,
                                          SystematicManager &systematicManager,
                                          int firstLhaId, int count) {
  dataManager.Define("pdf_obs", []() { return 5.0f; }, {}, systematicManager);
  dataManager.Define("pdf_weight_nominal", []() { return 1.0f; }, {},
                     systematicManager);

  for (int offset = 0; offset < count; ++offset) {
    const std::string label = "pdf_" + std::to_string(firstLhaId + offset);
    const std::string column = "pdf_weight_nominal_" + label;
    const float value = 1000.0f + static_cast<float>(offset);
    dataManager.Define(column, [value]() { return value; }, {},
                       systematicManager);
    systematicManager.registerVariationColumn("pdf_weight_nominal", label,
                                              column);
  }
}

void expectExplicitWeightVariationsAffectHistogram(
    NDHistogramManager &histogramManager, IDataFrameProvider &dataManager,
    SystematicManager &systematicManager) {
  histInfo info("weight_syst_hist", "obs", "obs", "weight_nominal", 10,
                0.0f, 10.0f);

  histogramManager.setupFromConfigFile();
  histogramManager.BookSingleHistogram(info);

  ASSERT_EQ(histogramManager.GetHistos().size(), 1u);

  const auto systList =
      systematicManager.makeSystList("SystematicCounter", dataManager);
  auto histPtr = histogramManager.GetHistos().front().GetPtr();
  ASSERT_NE(histPtr, nullptr);

  const auto contents = collectSystematicContents(*histPtr, systList);
  ASSERT_EQ(contents.size(), 3u);
  EXPECT_FLOAT_EQ(contents.at("Nominal"), 1.0f);
  EXPECT_FLOAT_EQ(contents.at("double_weight"), 2.0f);
  EXPECT_FLOAT_EQ(contents.at("triple_weight"), 3.0f);
}

void expectPdfStyleSystematicVectorsStaySequential(
    NDHistogramManager &histogramManager, IDataFrameProvider &dataManager,
    SystematicManager &systematicManager, int firstLhaId, int count) {
  histInfo info("pdf_diag_hist", "pdf_obs", "pdf_obs", "pdf_weight_nominal",
                10, 0.0f, 10.0f);

  histogramManager.setupFromConfigFile();
  histogramManager.BookSingleHistogram(info);

  auto df = dataManager.getDataFrame();
  const auto systValues =
      df.Take<ROOT::VecOps::RVec<Float_t>>("SystematicCounter_systVector")
          .GetValue();
  ASSERT_EQ(systValues.size(), 1u);
  ASSERT_EQ(systValues.front().size(), static_cast<std::size_t>(count + 1));

  for (int index = 0; index <= count; ++index) {
    EXPECT_FLOAT_EQ(systValues.front().at(index), static_cast<float>(index));
  }

  const std::string weightVectorColumn =
      findSingleColumnWithPrefix(df, "pdf_weight_nominal_systVector_");
  const auto weightValues =
      df.Take<ROOT::VecOps::RVec<Float_t>>(weightVectorColumn).GetValue();
  ASSERT_EQ(weightValues.size(), 1u);
  ASSERT_EQ(weightValues.front().size(), static_cast<std::size_t>(count + 1));
  EXPECT_FLOAT_EQ(weightValues.front().at(0), 1.0f);

  for (int offset = 0; offset < count; ++offset) {
    EXPECT_FLOAT_EQ(weightValues.front().at(offset + 1),
                    1000.0f + static_cast<float>(offset));
  }

  const auto systList =
      systematicManager.makeSystList("SystematicCounter", dataManager);
  ASSERT_EQ(systList.size(), static_cast<std::size_t>(count + 1));
  EXPECT_EQ(systList.at(22), "pdf_325321");
  EXPECT_EQ(systList.at(23), "pdf_325322");
  EXPECT_EQ(systList.at(51), "pdf_325350");
  EXPECT_EQ(systList.at(54), "pdf_325353");

  auto histPtr = histogramManager.GetHistos().front().GetPtr();
  ASSERT_NE(histPtr, nullptr);
  const auto contents = collectSystematicContents(*histPtr, systList);
  ASSERT_EQ(contents.size(), static_cast<std::size_t>(count + 1));
  EXPECT_FLOAT_EQ(contents.at("Nominal"), 1.0f);
  EXPECT_FLOAT_EQ(contents.at("pdf_325321"), 1021.0f);
  EXPECT_FLOAT_EQ(contents.at("pdf_325322"), 1022.0f);
  EXPECT_FLOAT_EQ(contents.at("pdf_325350"), 1050.0f);
  EXPECT_FLOAT_EQ(contents.at("pdf_325353"), 1053.0f);
}

} // namespace

TEST_F(NDHistogramManagerConfigTest, SetupFromConfigFileNoHistogramConfig) {
  // Test that setupFromConfigFile works when no histogram config is specified
  EXPECT_NO_THROW(histogramManager->setupFromConfigFile());
}

TEST_F(NDHistogramManagerConfigTest, SetupFromConfigFileWithHistogramConfig) {
  // Add histogram config to the configuration
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();
  
  // Book the histograms
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  
  // Verify histograms were booked
  auto& histos = histogramManager->GetHistos();
  EXPECT_EQ(histos.size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, HistogramSystematicsCanBeDisabled) {
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
  dataManager->Define("w1_weightUp", []() { return 2.0f; }, {},
                      *systematicManager);
  dataManager->Define("w1_weightDown", []() { return 0.5f; }, {},
                      *systematicManager);
  systematicManager->registerVariationColumn("w1", "weightUp", "w1_weightUp");
  systematicManager->registerVariationColumn("w1", "weightDown", "w1_weightDown");
  const auto allSystematics =
      systematicManager->makeSystList("SystematicCounter", *dataManager);
  ASSERT_EQ(allSystematics.size(), 3u);
  dataManager->DefineVector(
      "SystematicCounter_systVector",
      {"SystematicCounter", "SystematicCounter_weightUp",
       "SystematicCounter_weightDown"},
      "Float_t", *systematicManager);

  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  configManager->set("histogramSystematics", "false");
  histogramManager->setupFromConfigFile();
  histogramManager->bookConfigHistograms();

  ASSERT_EQ(histogramManager->GetHistos().size(), 4u);
  auto histogram = histogramManager->GetHistos().front().GetPtr();
  ASSERT_NE(histogram, nullptr);
  ASSERT_EQ(histogram->GetNdimensions(), 5);
  EXPECT_EQ(histogram->GetAxis(3)->GetNbins(), 1)
      << "The systematic axis should contain only the nominal bin";
  const auto nominalCounter = dataManager->getDataFrame()
      .Take<ROOT::VecOps::RVec<Float_t>>(
          "SystematicCounter_nominal_systVector")
      .GetValue();
  ASSERT_EQ(nominalCounter.size(), 1u);
  ASSERT_EQ(nominalCounter.front().size(), 1u);
  EXPECT_FLOAT_EQ(nominalCounter.front().front(), 0.0f);
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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

  configManager->set("histogramConfig", "cfg/test_histograms.yaml");

  // Book with the explicitly selected ROOT backend.
  configManager->set("histogramBackend", "root");
  histogramManager->setupFromConfigFile();
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  const auto rootCount = histogramManager->GetHistos().size();

  // Use a completely fresh config/manager for the Boost backend to avoid
  // duplicate-key errors (ConfigurationManager::set() throws on overwrite).
  auto boostConfig = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.yaml");
  boostConfig->set("histogramConfig", "cfg/test_histograms.yaml");
  boostConfig->set("histogramBackend", "boost");

  auto boostManager = std::make_unique<NDHistogramManager>(*boostConfig);
  ManagerContext ctx{*boostConfig, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  boostManager->setContext(ctx);

  boostManager->setupFromConfigFile();
  EXPECT_NO_THROW(boostManager->bookConfigHistograms());
  const auto boostCount = boostManager->GetHistos().size();

  EXPECT_EQ(rootCount, boostCount);
}

TEST_F(NDHistogramManagerConfigTest, DefaultBoostSparseWarnsOnlyWhenSavingOverfullHistogram) {
  expectSparseMemoryWarning();
}

TEST_F(NDHistogramManagerConfigTest, DoubleBoostSparseMemoryWarning) {
  expectSparseMemoryWarning("double");
}

#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
TEST_F(NDHistogramManagerConfigTest, FloatFlatSparseMemoryWarning) {
  expectSparseMemoryWarning("float", "flat");
}

TEST_F(NDHistogramManagerConfigTest, DoubleFlatSparseMemoryWarning) {
  expectSparseMemoryWarning("double", "flat");
}
#endif

TEST_F(NDHistogramManagerConfigTest, DenseBoostDoesNotProduceSparseMemoryWarning) {
  expectSparseMemoryWarning("float", "map", "boost", "64");
}

TEST_F(NDHistogramManagerConfigTest, RootDoesNotProduceBoostMemoryWarning) {
  expectSparseMemoryWarning("", "", "root");
}

TEST_F(NDHistogramManagerConfigTest, DefaultBoostUsesRootFloatCancellationArithmetic) {
  dataManager->DefineNominal("cancel_values", [] {
    return ROOT::VecOps::RVec<float>{.5f, .5f, .5f};
  });
  dataManager->DefineNominal("cancel_weights", [] {
    return ROOT::VecOps::RVec<float>{16777216.f, 1.f, -16777216.f};
  });
  histogramManager->setupFromConfigFile();
  histInfo info("cancel", "cancel_values", "", "cancel_weights", 1, 0, 1);
  histogramManager->BookSingleHistogram(info);
  auto *histogram = histogramManager->GetHistos().front().GetPtr();
  const int point[] = {1, 1, 1, 1, 1};
  const auto bin = histogram->GetBin(point, false);
  ASSERT_GE(bin, 0);
  EXPECT_EQ(histogram->GetBinContent(bin), 0);
  EXPECT_EQ(histogram->GetBinError2(bin), 2.0 * 16777216.0 * 16777216.0 + 1.0);
}

TEST_F(NDHistogramManagerConfigTest, DenseMemoryLimitRejectsInvalidValues) {
  for (const auto *value : {"-1", "1.5", "64MiB", "18446744073709551615"}) {
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.yaml");
    config->set("histogramDenseLimitMiB", value);
    NDHistogramManager manager(*config);
    ManagerContext context{*config, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    manager.setContext(context);
    EXPECT_THROW(manager.setupFromConfigFile(), std::runtime_error) << value;
  }
}

TEST_F(NDHistogramManagerConfigTest, BoostSelectionPreservesVariableEdgesThroughRootFallback) {
  dataManager->DefineNominal("variable_observable", []() { return 15.f; });
  dataManager->DefineNominal("variable_weight", []() { return 2.f; });
  configManager->set("histogramBackend", "boost");
  configManager->set("histogramBoostPrecision", "double");
  configManager->set("histogramDenseLimitMiB", "0");
  histogramManager->setupFromConfigFile();
  histInfo info("variable_edges", "variable_observable", "", "variable_weight",
                2, 0, 30, std::vector<Double_t>{0, 10, 30});
  histogramManager->BookSingleHistogram(info);
  auto histogram = histogramManager->GetHistos().front().GetPtr();
  EXPECT_EQ(histogram->GetAxis(4)->GetBinUpEdge(1), 10);
  EXPECT_EQ(histogram->GetAxis(4)->GetBinUpEdge(2), 30);
  const Int_t indices[] = {1, 1, 1, 1, 2};
  const auto bin = histogram->GetBin(indices, false);
  ASSERT_GE(bin, 0);
  EXPECT_EQ(histogram->GetBinContent(bin), 2);
  EXPECT_EQ(histogram->GetBinError2(bin), 4);
}

TEST_F(NDHistogramManagerConfigTest, BoostFloatSupportsVariableEdgesAtExactBoundary) {
  dataManager->DefineNominal("float_observable", []() { return 10.f; });
  dataManager->DefineNominal("float_weight", []() { return -2.f; });
  configManager->set("histogramBackend", "boost");
  configManager->set("histogramBoostPrecision", "float");
  configManager->set("histogramDenseLimitMiB", "0");
#ifdef RDF_HAS_BOOST_FLAT_HISTOGRAM_STORAGE
  configManager->set("histogramBoostSparseBackend", "flat");
#endif
  histogramManager->setupFromConfigFile();
  histInfo info("float_variable_edges", "float_observable", "", "float_weight",
                2, 0, 30, std::vector<Double_t>{0, 10, 30});
  histogramManager->BookSingleHistogram(info);
  auto histogram = histogramManager->GetHistos().front().GetPtr();
  EXPECT_EQ(histogram->GetAxis(4)->GetBinUpEdge(1), 10);
  EXPECT_EQ(histogram->GetAxis(4)->GetBinUpEdge(2), 30);
  const Int_t indices[] = {1, 1, 1, 1, 2};
  const auto bin = histogram->GetBin(indices, false);
  ASSERT_GE(bin, 0);
  EXPECT_EQ(histogram->GetBinContent(bin), -2);
  EXPECT_EQ(histogram->GetBinError2(bin), 4);
}

TEST_F(NDHistogramManagerConfigTest, ZeroDenseMemoryLimitIsSupportedForBothBackends) {
  for (const auto *backend : {"root", "boost"}) {
    auto config = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.yaml");
    config->set("histogramDenseLimitMiB", "0");
    config->set("histogramBackend", backend);
    NDHistogramManager manager(*config);
    ManagerContext context{*config, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
    manager.setContext(context);
    EXPECT_NO_THROW(manager.setupFromConfigFile());
  }
}

TEST_F(NDHistogramManagerConfigTest,
       ExplicitWeightVariationsChangeHistogramContent) {
  defineExplicitWeightVariationColumns(*dataManager, *systematicManager);
  expectExplicitWeightVariationsAffectHistogram(*histogramManager, *dataManager,
                                                *systematicManager);
}

TEST_F(NDHistogramManagerConfigTest,
       ExplicitWeightVariationsChangeHistogramContentWithBoostBackend) {
  defineExplicitWeightVariationColumns(*dataManager, *systematicManager);
  configManager->set("histogramBackend", "boost");
  expectExplicitWeightVariationsAffectHistogram(*histogramManager, *dataManager,
                                                *systematicManager);
}

TEST_F(NDHistogramManagerConfigTest,
    PdfStyleSystematicVectorsStaySequentialWithBoostBackend) {
  definePdfStyleWeightVariationColumns(*dataManager, *systematicManager,
                  325300, 103);
  configManager->set("histogramBackend", "boost");
  expectPdfStyleSystematicVectorsStaySequential(*histogramManager, *dataManager,
                     *systematicManager, 325300, 103);
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

  configManager->set("histogramBackend", "root");
  configManager->set("histogramDenseLimitMiB", "64");
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();
  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());

  // The number of booked histograms must match regardless of dense/sparse selection.
  EXPECT_EQ(histogramManager->GetHistos().size(), 4u);
}

TEST_F(NDHistogramManagerConfigTest, BoostBackendAutoSelectsDenseForSmallHist) {
  // An explicit nonzero budget permits dense storage for a small histogram.
  dataManager->Define("bvar1", []() { return 3.0f; }, {}, *systematicManager);
  dataManager->Define("bw1", []() { return 1.0f; }, {}, *systematicManager);

  configManager->set("histogramBackend", "boost");
  configManager->set("histogramDenseLimitMiB", "64");
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

  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
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

TEST_F(NDHistogramManagerConfigTest, ShapeVariationCanMigrateHistogramRegion) {
  expectShapeVariationCanMigrateHistogramRegion();
}

TEST_F(NDHistogramManagerConfigTest,
       ShapeVariationCanMigrateHistogramRegionWithBoostBackend) {
  expectShapeVariationCanMigrateHistogramRegion("boost");
}

TEST_F(NDHistogramManagerConfigTest, BookConfigHistogramsWithBoundRegionManager) {
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 7.5f; }, {}, *systematicManager);
  dataManager->Define("w3", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var4", []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4", []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion", []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory", []() { return 2.5f; }, {}, *systematicManager);
  dataManager->Define("pass_presel", []() { return true; }, {}, *systematicManager);
  dataManager->Define("pass_signal", []() { return true; }, {}, *systematicManager);

  RegionManager regionManager;
  ManagerContext regionCtx{*configManager, *dataManager, *systematicManager,
                           *logger, *skimSink, *metaSink};
  regionManager.setContext(regionCtx);
  regionManager.declareRegion("presel", "pass_presel");
  regionManager.declareRegion("signal", "pass_signal", "presel");

  histogramManager->bindToRegionManager(&regionManager);
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();

  EXPECT_NO_THROW(histogramManager->bookConfigHistograms());
  ASSERT_EQ(histogramManager->GetHistos().size(), 4u);

  auto histPtr = histogramManager->GetHistos().front().GetPtr();
  ASSERT_NE(histPtr, nullptr);
  EXPECT_GT(histPtr->GetEntries(), 0.0);
}

// ---------------------------------------------------------------------------
// YAML histogram config tests
// ---------------------------------------------------------------------------

TEST_F(NDHistogramManagerConfigTest, YamlHistogramConfigParsedFromTextMainConfig) {
  ConfigurationManager legacy("cfg/config.txt");
  legacy.set("histogramConfig", "cfg/test_histograms.yaml");
  NDHistogramManager manager(legacy);
  ManagerContext context{legacy, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  manager.setContext(context);
  EXPECT_NO_THROW(manager.setupFromConfigFile());
}

TEST_F(NDHistogramManagerConfigTest, HistogramYamlParsesSevenChannelRegions) {
  configManager->set("histogramConfig", "cfg/test_histograms_seven_regions.yaml");
  ASSERT_NO_THROW(histogramManager->setupFromConfigFile());

  const auto &histograms = histogramManager->getConfigHistograms();
  ASSERT_EQ(histograms.size(), 1u);
  const std::vector<std::string> expectedRegions = {
      "AtLeast1TightB_SubleadBelowLoose",
      "AtLeast1TightB_SubleadLooseToMedium",
      "AtLeast1TightB_SubleadMediumToTight",
      "AtLeast1TightB_SubleadTightToXTight",
      "AtLeast1TightB_SubleadXTightToXXTight",
      "AtLeast1TightB_SubleadAtLeastXXTight",
      "ZeroTightB_AnySubleadWP"};
  for (const auto &histogram : histograms) {
    EXPECT_EQ(histogram.channelVariable, "JetBTagRegion") << histogram.name;
    EXPECT_EQ(histogram.channelBins, 7) << histogram.name;
    EXPECT_FLOAT_EQ(histogram.channelLowerBound, -0.5f) << histogram.name;
    EXPECT_FLOAT_EQ(histogram.channelUpperBound, 6.5f) << histogram.name;
    EXPECT_EQ(histogram.channelRegions, expectedRegions) << histogram.name;
  }
}

TEST_F(NDHistogramManagerConfigTest, YamlHistogramConfigProducesSameResultAsText) {
  // Parse both the text and YAML histogram configs and verify they produce
  // identical histogram definitions.
  auto configManagerText = ManagerFactory::createConfigurationManager("cfg/test_data_config_minimal.yaml");
  auto histManagerText = std::make_unique<NDHistogramManager>(*configManagerText);
  ManagerContext ctxText{*configManagerText, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
  histManagerText->setContext(ctxText);
  configManagerText->set("histogramConfig", "cfg/test_histograms.txt");
  histManagerText->setupFromConfigFile();

  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();

  // Both should have loaded 4 histogram configurations
  auto& configHistsText = histManagerText->getConfigHistograms();
  auto& configHistsYaml = histogramManager->getConfigHistograms();
  ASSERT_EQ(configHistsText.size(), configHistsYaml.size());

  for (size_t i = 0; i < configHistsText.size(); ++i) {
    EXPECT_EQ(configHistsText[i].name,     configHistsYaml[i].name)     << "name mismatch at index " << i;
    EXPECT_EQ(configHistsText[i].variable, configHistsYaml[i].variable) << "variable mismatch at index " << i;
    EXPECT_EQ(configHistsText[i].weight,   configHistsYaml[i].weight)   << "weight mismatch at index " << i;
    EXPECT_EQ(configHistsText[i].bins,     configHistsYaml[i].bins)     << "bins mismatch at index " << i;
    EXPECT_FLOAT_EQ(configHistsText[i].lowerBound, configHistsYaml[i].lowerBound) << "lowerBound mismatch at index " << i;
    EXPECT_FLOAT_EQ(configHistsText[i].upperBound, configHistsYaml[i].upperBound) << "upperBound mismatch at index " << i;
  }
}

TEST_F(NDHistogramManagerConfigTest, BookConfigHistogramsTrackedForSaveHists) {
  // Regression test for bug: bookConfigHistograms() was not populating
  // trackedHistInfos_m, so saveHists() (no-args) returned early and
  // histograms were never written to the meta file.
  dataManager->Define("var1", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("w1",   []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var2", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("w2",   []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var3", []() { return 7.5f; }, {}, *systematicManager);
  dataManager->Define("w3",   []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("var4", []() { return 12.5f; }, {}, *systematicManager);
  dataManager->Define("w4",   []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("channel",       []() { return 1.0f; }, {}, *systematicManager);
  dataManager->Define("controlRegion", []() { return 1.5f; }, {}, *systematicManager);
  dataManager->Define("sampleCategory",[]() { return 2.5f; }, {}, *systematicManager);

  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();
  histogramManager->bookConfigHistograms();

  // After bookConfigHistograms(), trackedHistInfos_m must be non-empty so
  // that saveHists() does not return early.
  EXPECT_FALSE(histogramManager->GetTrackedHistInfos().empty())
      << "trackedHistInfos_m should be populated by bookConfigHistograms()";
}

TEST_F(NDHistogramManagerConfigTest, SaveHistsWritesSparseArrayAndSchema) {
  dataManager->Define("obs", []() { return 5.0f; }, {}, *systematicManager);
  dataManager->Define("weight", []() { return 2.0f; }, {}, *systematicManager);
  dataManager->Define("channel", []() { return 0.5f; }, {}, *systematicManager);
  dataManager->Define("control", []() { return 0.5f; }, {}, *systematicManager);
  dataManager->Define("sample", []() { return 0.5f; }, {}, *systematicManager);

  histInfo info("export_hist", "obs", "Observable", "weight", 2, 0.0f, 20.0f);
  histogramManager->BookSingleHistogram(
      info,
      selectionInfo("sample", 1, 0.0f, 1.0f, {"inclusive"}),
      selectionInfo("control", 1, 0.0f, 1.0f, {"default"}),
      selectionInfo("channel", 1, 0.0f, 1.0f, {"signal"}));

  const auto stamp = std::chrono::steady_clock::now().time_since_epoch().count();
  const auto outputPath = std::filesystem::temp_directory_path() /
                          ("rdfanalyzer_sparse_export_" +
                           std::to_string(stamp) + ".root");
  {
    TFile seed(outputPath.string().c_str(), "RECREATE");
    TNamed marker("seed", "seed");
    marker.Write();
  }
  configManager->set("metaFile", outputPath.string());

  std::vector<std::vector<histInfo>> histogramInfo = {{info}};
  std::vector<std::vector<std::string>> axisLabels = {
      {"signal"}, {"default"}, {"inclusive"}, {"Nominal"}};
  ASSERT_NO_THROW(histogramManager->saveHists(histogramInfo, axisLabels));

  TFile result(outputPath.string().c_str(), "READ");
  ASSERT_TRUE(result.IsOpen());
  auto *sparse = dynamic_cast<THnSparseF *>(
      result.Get("nd_histograms_v1/export_hist_"));
  ASSERT_NE(sparse, nullptr);
  EXPECT_EQ(std::string(sparse->GetAxis(0)->GetTitle()), "channel");
  EXPECT_EQ(std::string(sparse->GetAxis(0)->GetBinLabel(1)), "signal");
  EXPECT_EQ(std::string(sparse->GetAxis(3)->GetBinLabel(1)), "Nominal");
  Int_t coordinates[5] = {};
  ASSERT_EQ(sparse->GetNbins(), 1);
  EXPECT_FLOAT_EQ(sparse->GetBinContent(0, coordinates), 2.0f);

  auto *schema = dynamic_cast<TObjString *>(
      result.Get("__rdf_histogram_schema_v1"));
  ASSERT_NE(schema, nullptr);
  const std::string schemaText = schema->GetString().Data();
  EXPECT_NE(schemaText.find("\"name\":\"export_hist\""), std::string::npos);
  EXPECT_NE(schemaText.find("\"role\":\"observable\""), std::string::npos);
  result.Close();
  std::filesystem::remove(outputPath);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(NDHistogramManagerConfigTest, ExcludedDiagnosticsDoNotRequireTheirColumns) {
  configManager->set("histogramConfig", "cfg/test_histograms.yaml");
  histogramManager->setupFromConfigFile();
  dataManager->Define("var1", []() { return 5.f; }, {}, *systematicManager);
  dataManager->Define("w1", []() { return 1.f; }, {}, *systematicManager);
  // The excluded entries reference deliberately absent columns. Only the
  // selected entry may be booked, tracked, or evaluated.
  histogramManager->bookConfigHistograms({"test_hist2", "test_hist3", "test_hist4"});
  ASSERT_EQ(histogramManager->getConfigHistograms().size(), 1u);
  EXPECT_EQ(histogramManager->getConfigHistograms()[0].name, "test_hist1");
  ASSERT_EQ(histogramManager->GetHistos().size(), 1u);
  EXPECT_EQ(histogramManager->GetHistos()[0]->GetEntries(), 1);
  EXPECT_THROW(histogramManager->bookConfigHistograms({"test_hist1"}),
               std::runtime_error);
}

TEST_F(NDHistogramManagerConfigTest, PerHistogramNominalOnlyDoesNotDisableOtherSystematics) {
  const auto path = std::filesystem::temp_directory_path() /
      ("rdf_nominal_debug_" + std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()) + ".yaml");
  {
    std::ofstream out(path);
    out << "- name: debug\n  variable: value\n  weight: weight\n  bins: 10\n  lowerBound: 0\n  upperBound: 10\n  nominalOnly: true\n"
        << "- name: physics\n  variable: value\n  weight: weight\n  bins: 10\n  lowerBound: 0\n  upperBound: 10\n";
  }
  dataManager->Define("value", [](){return 5.f;}, {}, *systematicManager);
  dataManager->Define("weight", [](){return 1.f;}, {}, *systematicManager);
  dataManager->Define("weight_up", [](){return 2.f;}, {}, *systematicManager);
  dataManager->Define("weight_down", [](){return .5f;}, {}, *systematicManager);
  systematicManager->registerVariationColumn("weight","testUp","weight_up");
  systematicManager->registerVariationColumn("weight","testDown","weight_down");
  configManager->set("histogramConfig", path.string());
  configManager->set("histogramSystematics", "true");
  histogramManager->setupFromConfigFile();
  histogramManager->bookConfigHistograms();
  ASSERT_EQ(histogramManager->GetHistos().size(),2u);
  auto debug=histogramManager->GetHistos()[0].GetPtr();
  auto physics=histogramManager->GetHistos()[1].GetPtr();
  EXPECT_EQ(debug->GetAxis(3)->GetNbins(),1);
  EXPECT_EQ(physics->GetAxis(3)->GetNbins(),3);
  EXPECT_EQ(debug->GetEntries(),1);
  EXPECT_EQ(physics->GetEntries(),3);
  std::filesystem::remove(path);
}

TEST_F(NDHistogramManagerConfigTest, NominalSideBranchesKeepRejectedEntriesWithoutChangingSelection) {
  ROOT::RDataFrame frame(2);
  auto input=frame.Define("stage_value",[](){return 1.f;}).Define("stage_weight",[](){return 1.f;})
      .Define("stage_weight_up",[](){return 2.f;}).Define("stage_weight_down",[](){return .5f;});
  dataManager->setDataFrame(input);
  systematicManager->registerVariationColumn("stage_weight","shiftUp","stage_weight_up");
  systematicManager->registerVariationColumn("stage_weight","shiftDown","stage_weight_down");
  auto selected=input.Filter([](ULong64_t entry){return entry==0;},{"rdfentry_"});
  std::vector<std::pair<std::string,ROOT::RDF::RNode>> stages{{"before",input},{"after",selected}};
  for(const auto &stage:stages) {
    dataManager->setDataFrame(stage.second);
    std::vector<histInfo> histograms{histInfo(stage.first.c_str(),"stage_value","Unweighted entries","stage_weight",1,.5,1.5)};
    std::vector<selectionInfo> selections;
    std::vector<std::vector<std::string>> names{{"Default"},{"Default"},{"Default"}};
    histogramManager->bookND(histograms,selections,"",names,true);
  }
  ASSERT_EQ(histogramManager->GetHistos().size(),2u);
  const auto sum=[](const THnSparseF &hist){
    double result=0.;for(Long64_t i=0;i<hist.GetNbins();++i)result+=hist.GetBinContent(i);return result;
  };
  EXPECT_EQ(sum(*histogramManager->GetHistos()[0]),2);
  EXPECT_EQ(sum(*histogramManager->GetHistos()[1]),1);
  EXPECT_EQ(histogramManager->GetHistos()[0]->GetAxis(3)->GetNbins(),1);
  EXPECT_EQ(*selected.Count(),1u);
}
