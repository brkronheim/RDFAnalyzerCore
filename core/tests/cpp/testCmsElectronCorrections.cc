#include <gtest/gtest.h>
#include <analyzer.h>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ElectronEnergyScaleManager.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <CmsCorrections.h>
#include <CmsWeights.h>
#include <CmsYearConfig.h>
#include <correction.h>
#include <algorithm>
#include <cmath>
#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

namespace {
using Vec = ROOT::VecOps::RVec<float>;

std::unique_ptr<Analyzer> makeAnalyzer(bool isData = false) {
  auto config = std::make_unique<ConfigurationManager>(
      std::string(TEST_SOURCE_DIR) + "/cfg/test_cms_electron.txt");
  config->set("dtype", isData ? "data" : "mc");
  auto an = std::make_unique<Analyzer>(
      std::move(config), std::make_unique<DataManager>(1),
      std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>{},
      std::make_unique<SystematicManager>(), std::make_unique<DefaultLogger>(),
      std::make_unique<NullOutputSink>(), std::make_unique<NullOutputSink>());
  CorrectionManager::create(*an);
  return an;
}

const Vec kPt{15.0f, 19.999f, 20.0f, 74.999f, 75.0f, 100.0f};
void defineSelectedElectrons(Analyzer &an, bool phi = true) {
  an.Define("SelectedElectron_pt", []() { return kPt; }, {})
      ->Define("SelectedElectron_scEta", []() { return Vec(kPt.size(), 0.5f); }, {});
  if (phi) an.Define("SelectedElectron_phi", []() { return Vec(kPt.size(), 0.7f); }, {});
}

struct Era {
  const char *canonical;
  const char *token;
  bool run2;
};
class ElectronSfPayloadTest : public ::testing::TestWithParam<Era> {};

TEST_P(ElectronSfPayloadTest, FrameworkMatchesOfficialPayloadIncludingBoundaries) {
  const auto era = GetParam();
  cms::CmsYearRegistry registry;
  registry.setYear(era.canonical);
  const auto ys = registry.load();
  EXPECT_EQ(ys.electronYearToken, era.token);
  if (!std::filesystem::exists(ys.electronFile)) GTEST_SKIP() << "CVMFS unavailable";
  auto an = makeAnalyzer();
  defineSelectedElectrons(*an);
  cms::CmsWeightConfig cfg;
  cfg.yearSettings = &ys;
  cfg.applyElectronScaleFactors = true;
  ASSERT_NO_THROW(cms::defineCmsElectronScaleFactors(*an, cfg));
  auto payload = correction::CorrectionSet::from_file(ys.electronFile);
  const auto direct = payload->at(ys.electronCorrection);
  for (const auto &variation : {std::pair<const char *, const char *>{"sf", "nominal"},
                               {"sfup", "up"}, {"sfdown", "down"}}) {
    for (const std::string family : {"id", "reco"}) {
      const auto column = "electron_" + family + "_sf_" + variation.second + "_vec";
      const auto result = an->getDataFrameProvider().getDataFrame()
                              .Take<Vec>(column).GetValue().at(0);
      ASSERT_EQ(result.size(), kPt.size());
      for (std::size_t i = 0; i < kPt.size(); ++i) {
        const std::string wp = family == "id" ? "wp80iso"
            : kPt[i] < 20 ? "RecoBelow20"
            : era.run2 ? "RecoAbove20"
            : kPt[i] < 75 ? "Reco20to75" : "RecoAbove75";
        std::vector<correction::Variable::Type> args{
            std::string(era.token), std::string(variation.first), wp, 0.5,
            static_cast<double>(kPt[i])};
        if (direct->inputs().size() == 6) args.emplace_back(static_cast<double>(0.7f));
        EXPECT_NEAR(result[i], direct->evaluate(args), 1e-6)
            << era.canonical << " " << column << " pT=" << kPt[i];
      }
    }
  }
}

INSTANTIATE_TEST_SUITE_P(AllYears, ElectronSfPayloadTest, ::testing::Values(
    Era{"2016_PreVFP", "2016preVFP", true},
    Era{"2016_PostVFP", "2016postVFP", true}, Era{"2017", "2017", true},
    Era{"2018", "2018", true}, Era{"2022_preEE", "2022Re-recoBCD", false},
    Era{"2022_postEE", "2022Re-recoE+PromptFG", false},
    Era{"2023_preBPix", "2023PromptC", false},
    Era{"2023_postBPix", "2023PromptD", false}, Era{"2024", "2024Prompt", false}));

TEST(ElectronSfConfiguration, InvalidYearAndWorkingPointFailDuringSetup) {
  cms::CmsYearRegistry registry;
  registry.setYear("22pre");
  auto ys = registry.load();
  if (!std::filesystem::exists(ys.electronFile)) GTEST_SKIP() << "CVMFS unavailable";
  for (bool invalidYear : {false, true}) {
    auto an = makeAnalyzer();
    defineSelectedElectrons(*an);
    auto invalid = ys;
    if (invalidYear) invalid.electronYearToken = "2022_preEE";
    else invalid.electronWorkingPoint = "missing_WP";
    cms::CmsWeightConfig cfg;
    cfg.yearSettings = &invalid;
    cfg.applyElectronScaleFactors = true;
    EXPECT_THROW(cms::defineCmsElectronScaleFactors(*an, cfg), std::runtime_error);
  }
}

TEST(ElectronSfConfiguration, PhiIsRequiredWhenDeclaredByPayload) {
  cms::CmsYearRegistry registry;
  registry.setYear("2023_preBPix");
  const auto ys = registry.load();
  if (!std::filesystem::exists(ys.electronFile)) GTEST_SKIP() << "CVMFS unavailable";
  auto an = makeAnalyzer();
  defineSelectedElectrons(*an, false);
  cms::CmsWeightConfig cfg;
  cfg.yearSettings = &ys;
  cfg.applyElectronScaleFactors = true;
  EXPECT_THROW(cms::defineCmsElectronScaleFactors(*an, cfg), std::runtime_error);
}

TEST(ElectronRecoBoundaries, BothContainerTypesUseHalfOpenBins) {
  const Vec pt{19.999f, 20, 74.999f, 75, 75.001f};
  const auto result = cms::selectElectronRecoScaleFactors(
      pt, Vec(5, 0.8f), Vec(5, 0.9f), Vec(5, 1.1f));
  EXPECT_EQ(std::vector<float>(result.begin(), result.end()),
            (std::vector<float>{0.8f, 0.9f, 0.9f, 1.1f, 1.1f}));
  const auto standard = cms::selectElectronRecoScaleFactors(
      std::vector<float>(pt.begin(), pt.end()), std::vector<float>(5, 0.8f),
      std::vector<float>(5, 0.9f), std::vector<float>(5, 1.1f));
  EXPECT_EQ(standard, (std::vector<float>{0.8f, 0.9f, 0.9f, 1.1f, 1.1f}));
}

class ElectronEnergyRecipeTest : public ::testing::TestWithParam<bool> {};
TEST_P(ElectronEnergyRecipeTest, DataAndMcMatchTheSelectedRecipe) {
  const bool etDependent = GetParam();
  cms::CmsYearRegistry registry;
  registry.setYear("22pre");
  auto ys = registry.load();
  ys.electronScaleSmearingEtDependent = etDependent;
  if (!etDependent) {
    ys.electronScaleSmearingFile =
        "/cvmfs/cms-griddata.cern.ch/cat/metadata/EGM/"
        "Run3-22CDSep23-Summer22-NanoAODv12/2024-03-04/electronSS.json.gz";
    ys.electronSmearingCorrection = "Smearing";
  }
  if (!std::filesystem::exists(ys.electronScaleSmearingFile)) GTEST_SKIP() << "CVMFS unavailable";
  auto payload = correction::CorrectionSet::from_file(ys.electronScaleSmearingFile);
  for (bool isData : {false, true}) {
    auto an = makeAnalyzer(isData);
    ElectronEnergyScaleManager::create(*an);
    an->Define("Electron_pt", []() { return Vec{45}; }, {})
        ->Define("Electron_eta", []() { return Vec{0.5}; }, {})
        ->Define("Electron_deltaEtaSC", []() { return Vec{0}; }, {})
        ->Define("Electron_phi", []() { return Vec{0.7}; }, {})
        ->Define("Electron_mass", []() { return Vec{0.000511}; }, {})
        ->Define("Electron_r9", []() { return Vec{0.98}; }, {})
        ->Define("Electron_seedGain", []() { return ROOT::VecOps::RVec<UChar_t>{12}; }, {})
        ->Define("run", []() { return UInt_t{355800}; }, {})
        ->Define("luminosityBlock", []() { return UInt_t{1}; }, {})
        ->Define("event", []() { return ULong64_t{1}; }, {});
    cms::CmsCorrectionConfig cfg;
    cfg.yearSettings = &ys;
    cfg.applyElectronCorrections = true;
    cms::defineCmsCorrectionInputs(*an, cfg);
    ASSERT_NO_THROW(cms::configureCmsCorrectionWorkflow(*an, cfg));
    auto df = an->getDataFrameProvider().getDataFrame();
    const auto value = [&](const std::string &column) {
      return df.Take<Vec>(column).GetValue().at(0).at(0);
    };
    const double r9 = static_cast<double>(0.98f);
    if (isData) {
      const double scale = etDependent
          ? payload->compound().at("Scale")->evaluate({std::string("scale"),
                355800.0, 0.5, r9, 45.0, 12.0})
          : payload->at("Scale")->evaluate({std::string("total_correction"),
                12, 355800.0, 0.5, r9, 45.0});
      EXPECT_NEAR(value("Electron_pt_corr_nominal"), 45 * scale, 1e-5);
    } else {
      const auto smear = payload->at(ys.electronSmearingCorrection);
      const double sigma = etDependent
          ? smear->evaluate({std::string("smear"), 45.0, r9, 0.5})
          : smear->evaluate({std::string("rho"), 0.5, r9});
      EXPECT_NEAR(value("Electron_smearing_sigma"), sigma, 1e-7);
      const auto random = value("Electron_smearing_random");
      EXPECT_NEAR(value("Electron_pt_corr_nominal"), 45 * (1 + sigma * random), 1e-5);
      const double error = etDependent ? 0.0
          : smear->evaluate({std::string("err_rho"), 0.5, r9});
      if (!etDependent) {
        EXPECT_NEAR(value("Electron_smearing_sigma_up"), sigma + error, 1e-7);
        EXPECT_NEAR(value("Electron_smearing_sigma_down"), std::max(0.0, sigma - error), 1e-7);
      }
      EXPECT_NEAR(value("Electron_pt_scale_up"), value("Electron_pt_corr_nominal") *
                      (1 + value("Electron_scale_uncertainty")), 1e-5);
    }
  }
}
INSTANTIATE_TEST_SUITE_P(Recipes, ElectronEnergyRecipeTest, ::testing::Bool());
} // namespace
