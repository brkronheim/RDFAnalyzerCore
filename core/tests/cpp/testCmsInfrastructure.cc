//==============================================================================
// testCmsInfrastructure.cc — Unit tests for Generic CMS Analysis Infrastructure
//
// Tests for CmsYearConfig, CmsNanoAodCompat, CmsCorrections, and CmsWeights.
//==============================================================================

#include <gtest/gtest.h>
#include <test_util.h>
#include <analyzer.h>

#include <CmsYearConfig.h>
#include <CmsNanoAodCompat.h>
#include <CmsCorrections.h>
#include <CmsWeights.h>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <JetEnergyScaleManager.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <correction.h>

#include <string>
#include <sstream>
#include <vector>
#include <unordered_map>
#include <atomic>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace cms;

// ===========================================================================
// CmsYearConfig tests
// ===========================================================================

TEST(CmsYearConfigTest, NormalizeYearAcceptsAllAliases) {
  CmsYearRegistry registry;

  // 2016 aliases
  EXPECT_EQ(registry.normalizeYear("2016"), "2016_PostVFP");
  EXPECT_EQ(registry.normalizeYear("2016pre"), "2016_PreVFP");
  EXPECT_EQ(registry.normalizeYear("2016_PreVFP"), "2016_PreVFP");
  EXPECT_EQ(registry.normalizeYear("2016post"), "2016_PostVFP");
  EXPECT_EQ(registry.normalizeYear("2016_postVFP"), "2016_PostVFP");
  EXPECT_EQ(registry.normalizeYear("2016preVFP"), "2016_PreVFP");
  EXPECT_EQ(registry.normalizeYear("2016postVFP"), "2016_PostVFP");

  // 2017
  EXPECT_EQ(registry.normalizeYear("2017"), "2017");

  // 2018
  EXPECT_EQ(registry.normalizeYear("2018"), "2018");

  // 2022 aliases
  EXPECT_EQ(registry.normalizeYear("2022"), "2022_postEE");
  EXPECT_EQ(registry.normalizeYear("22pre"), "2022_preEE");
  EXPECT_EQ(registry.normalizeYear("22post"), "2022_postEE");
  EXPECT_EQ(registry.normalizeYear("2022_preEE"), "2022_preEE");
  EXPECT_EQ(registry.normalizeYear("2022_postEE"), "2022_postEE");

  // 2023 aliases
  EXPECT_EQ(registry.normalizeYear("2023"), "2023_postBPix");
  EXPECT_EQ(registry.normalizeYear("23pre"), "2023_preBPix");
  EXPECT_EQ(registry.normalizeYear("23post"), "2023_postBPix");
  EXPECT_EQ(registry.normalizeYear("2023_preBPix"), "2023_preBPix");
  EXPECT_EQ(registry.normalizeYear("2023_postBPix"), "2023_postBPix");

  // 2024
  EXPECT_EQ(registry.normalizeYear("2024"), "2024");
  EXPECT_EQ(registry.normalizeYear("24"), "2024");

  // Invalid year
  EXPECT_THROW(registry.normalizeYear("invalid_year"), std::runtime_error);
  EXPECT_THROW(registry.normalizeYear("2015"), std::runtime_error);
  EXPECT_THROW(registry.normalizeYear("2025"), std::runtime_error);
}

TEST(CmsYearConfigTest, SetYearNormalizesCorrectly) {
  CmsYearRegistry registry;
  registry.setYear("22pre");
  EXPECT_EQ(registry.year(), "2022_preEE");

  registry.setYear("2016post");
  EXPECT_EQ(registry.year(), "2016_PostVFP");

  registry.setYear("2024");
  EXPECT_EQ(registry.year(), "2024");
}

TEST(CmsYearConfigTest, LoadReturnsCorrectDefaultsFor2016PreVFP) {
  CmsYearRegistry registry;
  registry.setYear("2016_PreVFP");
  auto s = registry.load();

  EXPECT_EQ(s.canonicalYear, "2016_PreVFP");
  EXPECT_EQ(s.electronYearToken, "2016preVFP");
  EXPECT_EQ(s.rhoBranch, "fixedGridRhoFastjetAll");
  EXPECT_EQ(s.muonCorrection, "NUM_MediumID_DEN_TrackerMuons");
  EXPECT_EQ(s.electronCorrection, "UL-Electron-ID-SF");
  EXPECT_EQ(s.electronWorkingPoint, "wp80iso");

  // Check that JEC/JER names are correct (updated to latest)
  EXPECT_EQ(s.jecCompoundCorrection, "Summer19UL16APV_V7_MC_L1L2L3Res_AK4PFchs");
  EXPECT_EQ(s.jesTotalCorrection, "Summer19UL16APV_V7_MC_Total_AK4PFchs");

  // Triggers
  EXPECT_GT(s.doubleMuonTriggers.size(), 0);
  EXPECT_GT(s.singleMuonTriggers.size(), 0);

  // Golden JSON should be non-empty
  EXPECT_FALSE(s.goldenJsonFile.empty());
}

TEST(CmsYearConfigTest, LoadReturnsCorrectDefaultsFor2023PostBPix) {
  CmsYearRegistry registry;
  registry.setYear("2023_postBPix");
  auto s = registry.load();

  EXPECT_EQ(s.canonicalYear, "2023_postBPix");
  EXPECT_EQ(s.electronYearToken, "2023PromptD");
  EXPECT_EQ(s.electronCorrection, "Electron-ID-SF");
  EXPECT_TRUE(s.electronScaleSmearingEtDependent);

  // Run 3 uses Puppi jets
  EXPECT_NE(s.jecCompoundCorrection.find("AK4PFPuppi"), std::string::npos);
  EXPECT_FALSE(s.electronScaleSmearingFile.empty());
  EXPECT_FALSE(s.muonScaleResolutionFile.empty());
}

TEST(CmsYearConfigTest, SupportedYearsAreComplete) {
  auto years = CmsYearRegistry::supportedYears();
  EXPECT_EQ(years.size(), 9);
  EXPECT_NE(std::find(years.begin(), years.end(), "2016_PreVFP"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2016_PostVFP"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2017"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2018"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2022_preEE"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2022_postEE"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2023_preBPix"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2023_postBPix"), years.end());
  EXPECT_NE(std::find(years.begin(), years.end(), "2024"), years.end());
}

TEST(CmsYearConfigTest, PathResolutionWorksWithVersionToken) {
  CmsYearRegistry registry;
  registry.setCorrectionsBase("/cvmfs/cms-griddata.cern.ch/cat/metadata");
  registry.setVersionOverride("v1");

  const std::string resolved = registry.resolvePath(
      "LUM/Run2-2016preVFP-UL-NanoAODv9/{version}/puWeights.json.gz");
  EXPECT_EQ(resolved,
      "/cvmfs/cms-griddata.cern.ch/cat/metadata/LUM/Run2-2016preVFP-UL-NanoAODv9/v1/puWeights.json.gz");
}

TEST(CmsYearConfigTest, PathResolutionWithoutVersionUsesLatest) {
  CmsYearRegistry registry;
  registry.setCorrectionsBase("/cvmfs/cms-griddata.cern.ch/cat/metadata");

  const std::string resolved = registry.resolvePath(
      "JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/{version}/jet_jerc.json.gz");
  EXPECT_EQ(resolved,
      "/cvmfs/cms-griddata.cern.ch/cat/metadata/JME/Run3-24CDEReprocessingFGHIPrompt-Summer24-NanoAODv15/latest/jet_jerc.json.gz");
}

TEST(CmsYearConfigTest, AbsolutePathIsUsedAsIs) {
  CmsYearRegistry registry;
  registry.setCorrectionsBase("/cvmfs/base");

  const std::string resolved = registry.resolvePath(
      "/eos/user/c/cmsdqm/www/CAF/certification/Cert_golden.json");
  EXPECT_EQ(resolved, "/eos/user/c/cmsdqm/www/CAF/certification/Cert_golden.json");
}

TEST(CmsYearConfigTest, OverrideChangesField) {
  CmsYearRegistry registry;
  registry.setYear("2022_preEE");
  registry.override("muonCorrection", "NUM_TightID_DEN_TrackerMuons");
  auto s = registry.load();
  EXPECT_EQ(s.muonCorrection, "NUM_TightID_DEN_TrackerMuons");
}

TEST(CmsYearConfigTest, OverrideListChangesTriggers) {
  CmsYearRegistry registry;
  registry.setYear("2022_preEE");
  registry.overrideList("singleMuonTriggers", {"CustomTrigger"});
  auto s = registry.load();
  EXPECT_EQ(s.singleMuonTriggers.size(), 1);
  EXPECT_EQ(s.singleMuonTriggers[0], "HLT_CustomTrigger");
}

TEST(CmsYearConfigTest, LoadWithOverridesAppliesConfigMap) {
  CmsYearRegistry registry;
  registry.setYear("2024");

  std::unordered_map<std::string, std::string> configMap = {
    {"cms_pileupFile", "/custom/path/pu.json.gz"},
    {"cms_muonCorrection", "NUM_TightID_DEN_TrackerMuons"},
  };

  auto s = registry.loadWithOverrides(configMap, "cms");
  EXPECT_EQ(s.pileupFile, "/custom/path/pu.json.gz");
  EXPECT_EQ(s.muonCorrection, "NUM_TightID_DEN_TrackerMuons");
}

TEST(CmsYearConfigTest, DefaultBTagBranchesAreCorrectForTaggers) {
  // ParticleNet (default)
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("PNet", "2022_preEE"), "Jet_btagPNetB");
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("DeepFlav", "2022_preEE"), "Jet_btagDeepFlavB");
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("RobustParT", "2022_preEE"), "Jet_btagRobustParTAK4B");
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("UParT", "2024"), "Jet_btagUParTAK4B");

  // Run 2 fallback
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("", "2018"), "Jet_btagDeepFlavB");
}

TEST(CmsYearConfigTest, DefaultJesSourcesArePopulated) {
  CmsYearRegistry registry;
  registry.setYear("2022_preEE");
  auto s = registry.load();

  // Should have JES sources
  EXPECT_GT(s.jesSources.size(), 0);

  // Should contain FlavorQCD
  EXPECT_NE(std::find(s.jesSources.begin(), s.jesSources.end(), "FlavorQCD"),
            s.jesSources.end());
}

// ===========================================================================
// Helper function tests
// ===========================================================================

TEST(CmsHelpersTest, MergeBTagScaleFactors) {
  std::vector<int> flavour = {0, 4, 5, 1, 21};
  std::vector<float> heavy = {2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
  std::vector<float> light = {0.9f, 0.95f, 1.0f, 1.05f, 1.1f};

  auto result = mergeBTagScaleFactors(flavour, heavy, light);
  ASSERT_EQ(result.size(), 5);
  EXPECT_FLOAT_EQ(result[0], 0.9f);   // flavour 0 -> light[0]
  EXPECT_FLOAT_EQ(result[1], 3.0f);   // flavour 4 -> heavy[1]
  EXPECT_FLOAT_EQ(result[2], 4.0f);   // flavour 5 -> heavy[2]
  EXPECT_FLOAT_EQ(result[3], 1.05f);  // flavour 1 -> light[3]
  EXPECT_FLOAT_EQ(result[4], 1.1f);   // flavour 21 -> light[4]
}

TEST(CmsHelpersTest, MergeCTagScaleFactors) {
  std::vector<int> flavour = {0, 4, 5, 1, 21};
  std::vector<float> light = {0.9f, 0.95f, 1.0f, 1.05f, 1.1f};
  std::vector<float> wc = {2.0f, 3.0f, 4.0f, 5.0f, 6.0f};
  std::vector<float> tnp = {7.0f, 8.0f, 9.0f, 10.0f, 11.0f};

  auto result = mergeCTagScaleFactors(flavour, light, wc, tnp);
  ASSERT_EQ(result.size(), 5);
  EXPECT_FLOAT_EQ(result[0], 0.9f);   // flavour 0 -> light
  EXPECT_FLOAT_EQ(result[1], 3.0f);   // flavour 4 -> wc
  EXPECT_FLOAT_EQ(result[2], 9.0f);   // flavour 5 -> tnp
  EXPECT_FLOAT_EQ(result[3], 1.05f);  // flavour 1 -> light
  EXPECT_FLOAT_EQ(result[4], 1.1f);   // flavour 21 -> light
}

TEST(CmsHelpersTest, SelectElectronRecoScaleFactors) {
  std::vector<float> pt = {15.0f, 50.0f, 100.0f};
  std::vector<float> below20 = {0.9f, 0.95f, 1.0f};
  std::vector<float> between20And75 = {1.0f, 1.02f, 1.05f};
  std::vector<float> above75 = {0.98f, 0.99f, 1.01f};

  auto result = selectElectronRecoScaleFactors(pt, below20, between20And75, above75);
  ASSERT_EQ(result.size(), 3);
  EXPECT_FLOAT_EQ(result[0], 0.9f);   // 15 GeV -> below20
  EXPECT_FLOAT_EQ(result[1], 1.02f);  // 50 GeV -> between20And75
  EXPECT_FLOAT_EQ(result[2], 1.01f);  // 100 GeV -> above75
}

TEST(CmsHelpersTest, ProductOrOne) {
  std::vector<float> weights = {1.0f, 2.0f, 3.0f};
  float result = productOrOne(weights);
  EXPECT_FLOAT_EQ(result, 6.0f);
}

TEST(CmsHelpersTest, SafeWeightRatio) {
  EXPECT_FLOAT_EQ(safeWeightRatio(2.0f, 1.0f), 2.0f);
  EXPECT_FLOAT_EQ(safeWeightRatio(1.0f, 0.0f), 1.0f);  // div by zero -> 1
  EXPECT_FLOAT_EQ(safeWeightRatio(1.0f, -1.0f), -1.0f);
}

// ===========================================================================
// Weight pattern tests
// ===========================================================================

TEST(CmsWeightsTest, DefaultSourceNamesArePopulated) {
  auto btagNames = defaultBTagSourceNames();
  EXPECT_GT(btagNames.size(), 0);
  EXPECT_NE(std::find(btagNames.begin(), btagNames.end(), "hf"), btagNames.end());
  EXPECT_NE(std::find(btagNames.begin(), btagNames.end(), "lf"), btagNames.end());
  EXPECT_NE(std::find(btagNames.begin(), btagNames.end(), "cferr1"), btagNames.end());

  auto ctagLight = defaultCTagLightSourceNames();
  EXPECT_EQ(ctagLight.size(), 2);

  auto ctagWc = defaultCTagWcSourceNames();
  EXPECT_EQ(ctagWc.size(), 2);

  auto ctagTnp = defaultCTagTnpSourceNames();
  EXPECT_GT(ctagTnp.size(), 0);
}

// ===========================================================================
// CmsYearConfig branch name tests
// ===========================================================================

TEST(CmsYearConfigTest, DefaultBTagBranchFor2016IsDeepFlav) {
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("", "2016_PreVFP"), "Jet_btagDeepFlavB");
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("", "2016_PostVFP"), "Jet_btagDeepFlavB");
}

TEST(CmsYearConfigTest, DefaultBTagBranchFor2022IsPNet) {
  EXPECT_EQ(CmsYearRegistry::defaultBTagBranch("PNet", "2022_preEE"), "Jet_btagPNetB");
}

TEST(CmsYearConfigTest, DefaultCvLBranchForUParTIsCorrect) {
  EXPECT_EQ(CmsYearRegistry::defaultCvLBranch("UParT", "2024"), "Jet_btagUParTAK4CvL");
}

TEST(CmsYearConfigTest, DefaultCvBBranchForDeepFlavIsCorrect) {
  EXPECT_EQ(CmsYearRegistry::defaultCvBBranch("DeepFlav", "2018"), "Jet_btagDeepFlavCvB");
}

// ===========================================================================
// Correction config tests
// ===========================================================================

TEST(CmsCorrectionsTest, ConfigKeyPrefixIsConfigurable) {
  CmsCorrectionConfig cfg;
  EXPECT_EQ(cfg.configKeyPrefix, "cms");

  CmsCorrectionConfig cfgCustom;
  cfgCustom.configKeyPrefix = "vhqq";
  EXPECT_EQ(cfgCustom.configKeyPrefix, "vhqq");
}

TEST(CmsCorrectionsTest, DefaultJesModeIsReduced) {
  CmsCorrectionConfig cfg;
  EXPECT_EQ(cfg.jesMode, CmsCorrectionConfig::Reduced);
}

TEST(CmsCorrectionsTest, CorrectionEnableFlagsDefaultToFalse) {
  CmsCorrectionConfig cfg;
  EXPECT_FALSE(cfg.applyJetCorrections);
  EXPECT_FALSE(cfg.applyElectronCorrections);
  EXPECT_FALSE(cfg.applyMuonCorrections);
  EXPECT_FALSE(cfg.applyBtagCorrections);
  EXPECT_FALSE(cfg.applyCtagCorrections);
}

TEST(CmsCorrectionsTest, PrintIncludesConfiguredSummaryFields) {
  CmsCorrectionConfig cfg;
  cfg.jetTagger = "DeepFlav";
  cfg.applyTauCorrections = true;
  std::ostringstream output;

  cfg.print(output);

  EXPECT_NE(output.str().find("jetTagger: DeepFlav"), std::string::npos);
  EXPECT_NE(output.str().find("applyTauCorrections: yes"), std::string::npos);
}

TEST(CmsWeightsTest, PrintIncludesConfiguredSummaryFields) {
  CmsWeightConfig cfg;
  cfg.isMC = false;
  cfg.applyPileupWeights = true;
  std::ostringstream output;

  cfg.print(output);

  EXPECT_NE(output.str().find("isMC: no"), std::string::npos);
  EXPECT_NE(output.str().find("applyPileupWeights: yes"), std::string::npos);
}

// ===========================================================================
// CmsYearConfig defaultJesReducedSources tests
// ===========================================================================

TEST(CmsYearConfigTest, ReducedJesSourcesContainCoreSources) {
  auto sources = CmsYearRegistry::defaultJesReducedSources("2022_preEE");
  EXPECT_NE(std::find(sources.begin(), sources.end(), "FlavorQCD"), sources.end());
  EXPECT_NE(std::find(sources.begin(), sources.end(), "RelativeBal"), sources.end());
  EXPECT_NE(std::find(sources.begin(), sources.end(), "Absolute"), sources.end());
}

TEST(CmsYearConfigTest, ReducedJesSourcesContainYearTaggedSources) {
  auto sources2022 = CmsYearRegistry::defaultJesReducedSources("2022_preEE");
  EXPECT_NE(std::find(sources2022.begin(), sources2022.end(), "Absolute_2022"), sources2022.end());

  auto sources2022EE = CmsYearRegistry::defaultJesReducedSources("2022_postEE");
  EXPECT_NE(std::find(sources2022EE.begin(), sources2022EE.end(), "Absolute_2022EE"), sources2022EE.end());
  EXPECT_EQ(std::find(sources2022EE.begin(), sources2022EE.end(), "Absolute_2022"), sources2022EE.end());

  auto sources2018 = CmsYearRegistry::defaultJesReducedSources("2018");
  EXPECT_NE(std::find(sources2018.begin(), sources2018.end(), "Absolute_2018"), sources2018.end());
}

TEST(CmsYearConfigTest, FullJesSourcesAreMoreNumerous) {
  auto reduced = CmsYearRegistry::defaultJesReducedSources("2022_preEE");
  auto full = CmsYearRegistry::defaultJesFullSources("2022_preEE");
  EXPECT_GT(full.size(), reduced.size());
}

// ===========================================================================
// JER per-bin sources
// ===========================================================================

TEST(CmsYearConfigTest, JerPerBinSourcesAreCorrect) {
  auto sources = CmsYearRegistry::defaultJerPerBinSources("2022_preEE");
  EXPECT_EQ(sources.size(), 6);
  EXPECT_EQ(sources[0], "eta0to1p93_pt0toInf");
  EXPECT_EQ(sources[1], "eta1p93to2p5_pt0toInf");
  EXPECT_EQ(sources[2], "eta2p5to3p0_pt0to50");
  EXPECT_EQ(sources[3], "eta2p5to3p0_pt50toInf");
  EXPECT_EQ(sources[4], "eta3to5_pt0to50");
  EXPECT_EQ(sources[5], "eta3to5_pt50toInf");
}

// ===========================================================================
// Trigger helper tests
// ===========================================================================

TEST(CmsYearConfigTest, TriggerTagForYear) {
  EXPECT_EQ(triggerTagForYear("2022_preEE"), "22");
  EXPECT_EQ(triggerTagForYear("2022_postEE"), "22EE");
  EXPECT_EQ(triggerTagForYear("2024"), "24");
}

// ===========================================================================
// c-tag naming resolution
// ===========================================================================

TEST(CmsWeightsTest, CtagCorrectionNameUsesLightWcTnpForRun3) {
  // Run 3 uses modern _light/_wc/_tnp naming
  const std::string name = ctagCorrectionName("particleNet", "2022_preEE", "light");
  EXPECT_EQ(name, "particleNet_light");
}

TEST(CmsWeightsTest, CtagCorrectionNameUsesDeepJetForRun2) {
  // Run 2 uses legacy deepJet_wp naming
  const std::string name = ctagCorrectionName("particleNet", "2018", "light");
  EXPECT_EQ(name, "deepJet_light");
}

TEST(CmsWeightsTest, CtagCorrectionNameForDeepFlavRun2) {
  const std::string name = ctagCorrectionName("deepJet", "2017", "wc");
  EXPECT_EQ(name, "deepJet_wc");
}

// ===========================================================================
// Jet tagger base name resolution
// ===========================================================================

TEST(CmsWeightsTest, JetTaggerBaseResolvesCorrectly) {
  // The jetTaggerBase is an internal helper in CmsWeights.cc
  // We test it indirectly through the correction name resolution
  // which uses similar logic.
}

TEST(CmsWeightsTest, ColdCorrectionCacheSupportsConcurrentEvaluation) {
  const auto source = std::filesystem::path(TEST_SOURCE_DIR) /
                      "aux" / "correction.json";
  const auto copied = std::filesystem::temp_directory_path() /
      ("rdfanalyzer-cold-correction-" + std::to_string(getpid()) + ".json");
  std::filesystem::copy_file(
      source, copied, std::filesystem::copy_options::overwrite_existing);

  constexpr std::size_t workers = 16;
  std::atomic<bool> start{false};
  std::vector<float> results(workers, 0.0f);
  std::vector<std::string> errors(workers);
  std::vector<std::thread> threads;
  threads.reserve(workers);
  for (std::size_t i = 0; i < workers; ++i) {
    threads.emplace_back([&, i]() {
      while (!start.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      try {
        results[i] = evalCorr3(copied.string(), "test_correction_real_pt",
                               0.5f, 1.0f, "A");
      } catch (const std::exception& error) {
        errors[i] = error.what();
      } catch (...) {
        errors[i] = "unknown exception";
      }
    });
  }
  start.store(true, std::memory_order_release);
  for (auto& thread : threads) thread.join();

  for (std::size_t i = 0; i < workers; ++i) {
    EXPECT_TRUE(errors[i].empty()) << errors[i];
    const auto result = results[i];
    EXPECT_FLOAT_EQ(result, 0.1f);
  }
  std::filesystem::remove(copied);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST(CmsCorrectionsTest, JecInputsFollowCampaignPayloadSchema) {
  const std::vector<std::string> legacy{"JetArea", "JetEta", "JetPt", "Rho"};
  EXPECT_EQ(cms::detail::jecCompoundInputColumns(legacy, "raw_pt", "rho"),
            (std::vector<std::string>{"Jet_area", "Jet_eta", "raw_pt", "rho"}));
  const std::vector<std::string> modern{
      "JetA", "JetEta", "JetPt", "Rho", "JetPhi", "run"};
  EXPECT_EQ(cms::detail::jecCompoundInputColumns(modern, "fat_raw", "rho", "FatJet"),
            (std::vector<std::string>{"FatJet_area", "FatJet_eta", "fat_raw",
                                      "rho", "FatJet_phi", "run"}));
  EXPECT_THROW(cms::detail::jecCompoundInputColumns(
                   std::vector<std::string>{"unknown"}, "pt", "rho"),
               std::runtime_error);
}

namespace {
using JetVec = ROOT::VecOps::RVec<float>;

std::unique_ptr<Analyzer> jetTestAnalyzer(bool data, bool variations = true) {
  auto config = std::make_unique<ConfigurationManager>(
      std::string(TEST_SOURCE_DIR) + "/cfg/test_cms_electron.txt");
  config->set("dtype", data ? "data" : "mc");
  auto an = std::make_unique<Analyzer>(
      std::move(config), std::make_unique<DataManager>(1),
      std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>{},
      std::make_unique<SystematicManager>(), std::make_unique<DefaultLogger>(),
      std::make_unique<NullOutputSink>(), std::make_unique<NullOutputSink>());
  an->getSystematicManager().setEnabled(variations && !data);
  CorrectionManager::create(*an);
  JetEnergyScaleManager::create(*an);
  an->Define("Jet_pt", []() { return JetVec{100}; }, {})
      ->Define("Jet_mass", []() { return JetVec{10}; }, {})
      ->Define("Jet_eta", []() { return JetVec{.4f}; }, {})
      ->Define("Jet_phi", []() { return JetVec{0}; }, {})
      ->Define("Jet_area", []() { return JetVec{.5f}; }, {})
      ->Define("Jet_rawFactor", []() { return JetVec{.1f}; }, {})
      ->Define("Jet_genMatchedPt", []() { return JetVec{90}; }, {})
      ->Define("rho", []() { return 20.0f; }, {})
      ->Define("run", []() { return UInt_t{360000}; }, {})
      ->Define("event", []() { return ULong64_t{1}; }, {})
      ->Define("PuppiMET_pt", []() { return 100.0f; }, {})
      ->Define("PuppiMET_phi", []() { return 0.0f; }, {});
  return an;
}

CmsYearSettings jetTestSettings() {
  CmsYearRegistry registry;
  registry.setYear("22pre");
  registry.override("jmeFile", std::string(TEST_SOURCE_DIR) + "/aux/mock_cms_jme.json");
  registry.override("jecCompoundCorrection", "Mock_MC_L1L2L3Res_AK4PFPuppi");
  registry.override("jerPtResolutionCorrection", "resolution");
  registry.override("jerScaleFactorCorrection", "Mock_MC_ScaleFactor_AK4PFPuppi");
  registry.override("rhoBranch", "rho");
  return registry.load();
}

CmsCorrectionConfig jetTestConfig(const CmsYearSettings &settings) {
  CmsCorrectionConfig cfg;
  cfg.yearSettings = &settings;
  cfg.applyJetCorrections = true;
  cfg.applyMetType1 = true;
  cfg.applyJesSystematics = false;
  return cfg;
}

float jetValue(Analyzer &an, const std::string &column) {
  return an.getDataFrameProvider().getDataFrame().Take<JetVec>(column).GetValue()[0][0];
}
} // namespace

TEST(CmsJetWorkflowTest, DataUsesRunDependentDataCompoundAfterRawFactorRemoval) {
  auto an = jetTestAnalyzer(true);
  const auto settings = jetTestSettings();
  configureCmsCorrectionWorkflow(*an, jetTestConfig(settings));
  EXPECT_NEAR(jetValue(*an, "Jet_pt_corr_nominal"), 90.f * 1.21f, 1e-5);
  EXPECT_EQ(an->getConfigurationProvider().get("cmsJecCompoundCorrection"),
            "Mock_DATA_L1L2L3Res_AK4PFPuppi");
  EXPECT_FALSE(an->HasColumn("Jet_pt_jer_up"));
  const auto met = an->getDataFrameProvider().getDataFrame()
                       .Take<float>("PuppiMET_pt_corr_nominal").GetValue()[0];
  EXPECT_NEAR(met, 100.f - (90.f * 1.21f - 100.f), 1e-5);
}

TEST(CmsJetWorkflowTest, SeparateJerUncertaintyChangesJetsMassAndMet) {
  auto an = jetTestAnalyzer(false);
  const auto settings = jetTestSettings();
  EXPECT_EQ(settings.jerScaleFactorUncertaintyCorrection,
            "Mock_MC_SFUncertainty_AK4PFPuppi");
  configureCmsCorrectionWorkflow(*an, jetTestConfig(settings));
  // JEC gives 99 GeV and the matched gen jet is 90 GeV. Smearing adds
  // (SF-1)*(99-90), with SF=1.2 nominal and 1.3/1.1 up/down.
  EXPECT_NEAR(jetValue(*an, "Jet_pt_corr_jec"), 99, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_pt_corr_nominal"), 100.8, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_pt_jer_up"), 101.7, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_pt_jer_down"), 99.9, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_mass_jer_up"), 10.17, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_mass_jer_down"), 9.99, 1e-5);
  for (const auto &direction : {"up", "down"}) {
    const auto jet = jetValue(*an, "Jet_pt_jer_" + std::string(direction));
    const auto met = an->getDataFrameProvider().getDataFrame()
        .Take<float>("PuppiMET_pt_jer_" + std::string(direction)).GetValue()[0];
    EXPECT_NEAR(jet + met, 200, 2e-5);
  }
  EXPECT_EQ(an->getSystematicManager().getVariationColumnName("Jet_pt_corr_nominal", "jerUp"),
            "Jet_pt_jer_up");
}

TEST(CmsJetWorkflowTest, LegacyCategoricalJerStillUsesItsUpAndDownValues) {
  auto an = jetTestAnalyzer(false);
  auto settings = jetTestSettings();
  settings.jerScaleFactorCorrection = "legacy_sf";
  settings.jerScaleFactorInputs = {"Jet_eta"};
  settings.jerScaleFactorUncertaintyCorrection = "not_present";
  configureCmsCorrectionWorkflow(*an, jetTestConfig(settings));
  EXPECT_NEAR(jetValue(*an, "Jet_pt_jer_up"), 101.7, 1e-5);
  EXPECT_NEAR(jetValue(*an, "Jet_pt_jer_down"), 99.9, 1e-5);
}

TEST(CmsJetWorkflowTest, MissingSeparateJerUncertaintyCannotSilentlyProduceIdenticalVariations) {
  for (const std::string name : {"", "not_present"}) {
    auto an = jetTestAnalyzer(false);
    auto settings = jetTestSettings();
    settings.jerScaleFactorUncertaintyCorrection = name;
    EXPECT_THROW(configureCmsCorrectionWorkflow(*an, jetTestConfig(settings)), std::runtime_error);
  }
  auto an = jetTestAnalyzer(false, false);
  auto settings = jetTestSettings();
  settings.jerScaleFactorUncertaintyCorrection = "not_present";
  EXPECT_NO_THROW(configureCmsCorrectionWorkflow(*an, jetTestConfig(settings)));
  EXPECT_NEAR(jetValue(*an, "Jet_pt_corr_nominal"), 100.8, 1e-5);
  EXPECT_FALSE(an->HasColumn("Jet_pt_jer_up"));
}

TEST(CmsJetWorkflowTest, ExplicitJecCompoundOverrideIsRespected) {
  auto an = jetTestAnalyzer(true);
  const auto settings = jetTestSettings();
  auto cfg = jetTestConfig(settings);
  cfg.jesCompoundCorrectionOverride = "Mock_MC_L1L2L3Res_AK4PFPuppi";
  configureCmsCorrectionWorkflow(*an, cfg);
  EXPECT_NEAR(jetValue(*an, "Jet_pt_corr_nominal"), 99, 1e-5);
}

TEST(CmsJetWorkflowTest, Run3DefaultsResolveRealDataAndJerUncertaintyPayloads) {
  for (const auto &era : {"2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix", "2024"}) {
    CmsYearRegistry registry;
    registry.setYear(era);
    const auto settings = registry.load();
    if (!std::filesystem::exists(settings.jmeFile)) GTEST_SKIP() << "CVMFS unavailable";
    const auto payload = correction::CorrectionSet::from_file(settings.jmeFile);
    const auto dataName = detail::jecCompoundForSample(settings.jecCompoundCorrection, true);
    EXPECT_NE(dataName.find("_DATA_"), std::string::npos) << era;
    EXPECT_NO_THROW(payload->compound().at(dataName)) << era;
    EXPECT_NO_THROW(payload->at(settings.jerScaleFactorUncertaintyCorrection)) << era;
    EXPECT_EQ(settings.jerScaleFactorUncertaintyInputs, settings.jerScaleFactorInputs);
  }
}

TEST(CmsJetWorkflowTest, FullJesLoadsUnregroupedPayloadsAndPropagatesShifts) {
  for (const auto &era : {"2022_preEE", "2022_postEE", "2023_preBPix", "2023_postBPix", "2024"}) {
    SCOPED_TRACE(era);
    CmsYearRegistry registry;
    registry.setYear(era);
    auto settings = registry.load();
    if (!std::filesystem::exists(settings.jmeFile)) GTEST_SKIP() << "CVMFS unavailable";
    settings.rhoBranch = "rho";
    auto an = jetTestAnalyzer(false);
    auto cfg = jetTestConfig(settings);
    cfg.applyJesSystematics = true;
    cfg.jesMode = CmsCorrectionConfig::Full;
    configureCmsCorrectionWorkflow(*an, cfg);
    EXPECT_EQ(an->getConfigurationProvider().get("cmsJesSystematicsMode"), "full");
    const auto sources = CmsYearRegistry::defaultJesFullSources(era);
    ASSERT_EQ(sources.size(), 27u);
    for (const auto &source : sources) {
      EXPECT_TRUE(an->HasColumn("Jet_pt_jes_" + source + "_up"));
      EXPECT_TRUE(an->HasColumn("Jet_pt_jes_" + source + "_down"));
    }
    EXPECT_FALSE(an->HasColumn("Jet_pt_jes_Absolute_up"));
    const auto nominal = jetValue(*an, "Jet_pt_corr_nominal");
    const auto payload = correction::CorrectionSet::from_file(settings.jmeFile);
    auto fullTotal = settings.jesTotalCorrection;
    const auto regrouped = fullTotal.find("Regrouped_");
    ASSERT_NE(regrouped, std::string::npos);
    fullTotal.erase(regrouped, std::string("Regrouped_").size());
    // Cover both an untagged source and a year-tagged variation whose payload
    // name has no year suffix.
    for (const auto &source : {std::string("FlavorQCD"), sources.at(19)}) {
      auto payloadSource = source;
      const auto suffix = payloadSource.find('_');
      if (suffix != std::string::npos) payloadSource.erase(suffix);
      auto name = fullTotal;
      name.replace(name.find("Total"), 5, payloadSource);
      const auto uncertainty = payload->at(name)->evaluate({double(.4f), double(nominal)});
      for (const auto &direction : {std::string("up"), std::string("down")}) {
        const auto sign = direction == "up" ? 1 : -1;
        const auto varied = jetValue(*an, "Jet_pt_jes_" + source + "_" + direction);
        EXPECT_NEAR(varied, nominal * (1 + sign * uncertainty), 2e-5);
        const auto met = an->getDataFrameProvider().getDataFrame()
            .Take<float>("PuppiMET_pt_jes_" + source + "_" + direction).GetValue()[0];
        EXPECT_NEAR(varied + met, 200, 2e-5);
      }
    }
  }
}
