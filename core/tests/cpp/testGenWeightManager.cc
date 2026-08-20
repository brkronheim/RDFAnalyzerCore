#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <GenWeightManager.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <test_util.h>

#include <ROOT/RVec.hxx>

#include <algorithm>
#include <memory>
#include <string>
#include <vector>

namespace {

using FloatVec = ROOT::VecOps::RVec<Float_t>;

ManagerContext makeContext(IConfigurationProvider &cfg, DataManager &dm,
                           SystematicManager &sm, DefaultLogger &log,
                           NullOutputSink &skim, NullOutputSink &meta) {
  return ManagerContext{cfg, dm, sm, log, skim, meta};
}

class GenWeightManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    config = ManagerFactory::createConfigurationManager(
        "cfg/test_data_config_minimal.txt");
    systematicManager = std::make_unique<SystematicManager>();
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();
  }

  std::unique_ptr<GenWeightManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<GenWeightManager>();
    auto ctx = makeContext(*config, dm, *systematicManager, *logger,
                           *skimSink, *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
  }

  std::unique_ptr<IConfigurationProvider> config;
  std::unique_ptr<SystematicManager> systematicManager;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

bool contains(const std::vector<std::string> &values, const std::string &needle) {
  return std::find(values.begin(), values.end(), needle) != values.end();
}

void defineBaseGenWeight(DataManager &dm, SystematicManager &sm,
                         float value = 1.0f) {
  dm.Define("genWeight", [value](ULong64_t) { return value; },
            {"rdfentry_"}, sm);
}

} // namespace

TEST_F(GenWeightManagerTest, TypeStringIsCorrect) {
  GenWeightManager mgr;
  EXPECT_EQ(mgr.type(), "GenWeightManager");
}

TEST_F(GenWeightManagerTest, MapsEightEntryLheScaleWeights) {
  config->set("genWeightEnablePs", "false");
  config->set("genWeightEnablePdf", "false");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("LHEScaleWeight",
            [](ULong64_t) {
              return FloatVec{0.55f, 1.05f, 2.05f, 0.75f,
                              2.15f, 0.85f, 1.25f, 2.25f};
            },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto values = dm.getDataFrame().Take<Float_t>(
      "genWeight_nominal_lhe_scale_muf2_mur1");
  EXPECT_FLOAT_EQ(values.GetValue().at(0), 2.15f);
}

TEST_F(GenWeightManagerTest, MapsNineEntryPowhegStyleLheScaleWeights) {
  config->set("genWeightEnablePs", "false");
  config->set("genWeightEnablePdf", "false");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("LHEScaleWeight",
            [](ULong64_t) {
              return FloatVec{0.55f, 1.05f, 2.05f, 0.75f, 1.0f,
                              2.15f, 0.85f, 1.25f, 2.25f};
            },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto values = dm.getDataFrame().Take<Float_t>(
      "genWeight_nominal_lhe_scale_muf2_mur1");
  EXPECT_FLOAT_EQ(values.GetValue().at(0), 2.15f);
}

TEST_F(GenWeightManagerTest, MapsPartonShowerWeights) {
  config->set("genWeightEnableLheScale", "false");
  config->set("genWeightEnablePdf", "false");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("PSWeight",
            [](ULong64_t) { return FloatVec{1.2f, 1.3f, 0.8f, 0.7f}; },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto values = dm.getDataFrame().Take<Float_t>("genWeight_nominal_ps_isr1_fsr0p5");
  EXPECT_FLOAT_EQ(values.GetValue().at(0), 0.7f);
}

TEST_F(GenWeightManagerTest, MapsConfiguredPdfIds) {
  config->set("genWeightEnableLheScale", "false");
  config->set("genWeightEnablePs", "false");
  config->set("genWeightPdfFirstLhaId", "325300");
  config->set("genWeightPdfCount", "3");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("LHEPdfWeight",
            [](ULong64_t) { return FloatVec{0.91f, 1.02f, 1.11f}; },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto first = dm.getDataFrame().Take<Float_t>("genWeight_nominal_pdf_325300");
  auto last = dm.getDataFrame().Take<Float_t>("genWeight_nominal_pdf_325302");
  EXPECT_FLOAT_EQ(first.GetValue().at(0), 0.91f);
  EXPECT_FLOAT_EQ(last.GetValue().at(0), 1.11f);
}

TEST_F(GenWeightManagerTest, UsesConfiguredBaseWeightColumn) {
  config->set("genWeightEnableLheScale", "false");
  config->set("genWeightEnablePs", "false");
  config->set("genWeightPdfFirstLhaId", "325300");
  config->set("genWeightPdfCount", "1");
  config->set("genWeightBaseWeightColumn", "weight_nominal");
  DataManager dm(1);
  dm.Define("weight_nominal", [](ULong64_t) { return 10.0f; },
            {"rdfentry_"}, *systematicManager);
  dm.Define("LHEPdfWeight", [](ULong64_t) { return FloatVec{0.91f}; },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto nominal = dm.getDataFrame().Take<Float_t>("genWeight_nominal");
  auto varied = dm.getDataFrame().Take<Float_t>("genWeight_nominal_pdf_325300");
  EXPECT_FLOAT_EQ(nominal.GetValue().at(0), 10.0f);
  EXPECT_FLOAT_EQ(varied.GetValue().at(0), 9.1f);
}

TEST_F(GenWeightManagerTest, ShortVectorsFallBackToUnity) {
  config->set("genWeightEnableLheScale", "false");
  config->set("genWeightEnablePdf", "false");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("PSWeight", [](ULong64_t) { return FloatVec{1.2f}; },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  auto values = dm.getDataFrame().Take<Float_t>("genWeight_nominal_ps_isr1_fsr0p5");
  EXPECT_FLOAT_EQ(values.GetValue().at(0), 1.0f);
}

TEST_F(GenWeightManagerTest, ExplicitLabelsAppearInSystematicList) {
  config->set("genWeightEnableLheScale", "false");
  config->set("genWeightEnablePs", "false");
  config->set("genWeightPdfCount", "1");
  DataManager dm(1);
  defineBaseGenWeight(dm, *systematicManager);
  dm.Define("LHEPdfWeight", [](ULong64_t) { return FloatVec{0.91f}; },
            {"rdfentry_"}, *systematicManager);

  auto mgr = makeMgr(dm);
  mgr->defineVariationWeights();

  const auto systList = systematicManager->makeSystList(
      ISystematicManager::CANONICAL_SYST_BRANCH_NAME, dm);
  EXPECT_TRUE(contains(systList, "pdf_325300"));
  EXPECT_EQ(systematicManager->getVariationColumnName(
                "genWeight_nominal", "pdf_325300"),
            "genWeight_nominal_pdf_325300");
}
