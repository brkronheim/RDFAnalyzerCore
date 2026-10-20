#include <gtest/gtest.h>
#include <CmsMetCorrections.h>
#include <CmsCorrections.h>
#include <CmsYearConfig.h>
#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <JetEnergyScaleManager.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <analyzer.h>
#include <cmath>
#include <filesystem>

namespace {
using Vec = ROOT::VecOps::RVec<float>;
using Mask = ROOT::VecOps::RVec<bool>;
std::unique_ptr<Analyzer> makeAnalyzer(bool systematics = true, bool isMC = false) {
  auto config = std::make_unique<ConfigurationManager>(std::string(TEST_SOURCE_DIR) + "/cfg/test_cms_electron.txt");
  config->set("dtype", isMC ? "mc" : "data");
  auto sm = std::make_unique<SystematicManager>();
  sm->setEnabled(systematics);
  auto an = std::make_unique<Analyzer>(std::move(config), std::make_unique<DataManager>(1),
      std::unordered_map<std::string, std::shared_ptr<IPluggableManager>>{},
      std::move(sm), std::make_unique<DefaultLogger>(),
      std::make_unique<NullOutputSink>(), std::make_unique<NullOutputSink>());
  CorrectionManager::create(*an);
  JetEnergyScaleManager::create(*an);
  an->getSystematicManager().setEnabled(systematics);
  return an;
}
void v(Analyzer &an, const std::string &name, Vec value) {
  an.Define(name, [value]() { return value; }, {});
}
void inputs(Analyzer &an) {
  an.Define("RawPuppiMET_pt", []() { return 100.f; }, {})
      ->Define("RawPuppiMET_phi", []() { return 0.f; }, {})
      ->Define("PuppiMET_pt", []() { return 999.f; }, {}) // must be ignored
      ->Define("PuppiMET_phi", []() { return 0.f; }, {})
      ->Define("PuppiMET_ptUnclusteredUp", []() { return 1004.f; }, {})
      ->Define("PuppiMET_phiUnclusteredUp", []() { return 0.f; }, {})
      ->Define("PuppiMET_ptUnclusteredDown", []() { return 994.f; }, {})
      ->Define("PuppiMET_phiUnclusteredDown", []() { return 0.f; }, {})
      ->Define("PFMET_pt", []() { return 73.f; }, {})
      ->Define("run", []() { return UInt_t(382000); }, {})
      ->Define("Rho_fixedGridRhoFastjetAll", []() { return 25.f; }, {});
  v(an, "Jet_pt", {50.f, 50.f}); v(an, "Jet_mass", {5.f, 5.f});
  v(an, "Jet_rawFactor", {.2f, .2f}); v(an, "Jet_eta", {.5f, .5f});
  v(an, "Jet_phi", {0.f, 0.f}); v(an, "Jet_area", {.5f, .5f});
  v(an, "Jet_muonSubtrFactor", {.2f, 0.f});
  v(an, "Jet_muonSubtrDeltaEta", {0.f, 0.f});
  v(an, "Jet_muonSubtrDeltaPhi", {0.f, 0.f});
  v(an, "Jet_chEmEF", {.1f, .6f}); v(an, "Jet_neEmEF", {.1f, .4f});
  v(an, "CorrT1METJet_rawPt", {14.f}); v(an, "CorrT1METJet_area", {.5f});
  v(an, "CorrT1METJet_eta", {.5f}); v(an, "CorrT1METJet_phi", {float(M_PI/2.)});
  v(an, "CorrT1METJet_muonSubtrFactor", {0.f});
  v(an, "CorrT1METJet_muonSubtrDeltaEta", {0.f});
  v(an, "CorrT1METJet_muonSubtrDeltaPhi", {0.f});
  v(an, "CorrT1METJet_EmEF", {.2f});
  v(an, "Muon_pt", {30.f, 30.f}); v(an, "Muon_pt_corr_nominal", {33.f, 60.f});
  v(an, "Muon_phi", {0.f, 0.f});
  an.Define("metMuons", []() { return Mask{true, false}; }, {});
  v(an, "Electron_pt", {40.f}); v(an, "Electron_pt_corr_nominal", {42.f});
  v(an, "Electron_pt_scale_up", {44.f}); v(an, "Electron_pt_scale_down", {40.f});
  v(an, "Electron_phi", {float(M_PI/2.)});
  an.Define("metElectrons", []() { return Mask{true}; }, {});
  an.getSystematicManager().registerVariationColumns("Electron_pt_corr_nominal",
      "ElectronScale", "Electron_pt_scale_up", "Electron_pt_scale_down");
}
}

TEST(CmsMetCorrections, Type1UsesMuonSubtractedDirectionEmVetoAndCorrectedThreshold) {
  // A low-pT jet migrates into the sum; a second migrates out; the third
  // is EM-dominated; the last loses half its raw pT to a muon.
  const auto sum = cms::detail::puppiType1JetSum(
      {14.f, 20.f, 20.f, 40.f}, {16.f, 14.f, 30.f, 60.f},
      {0.f, 0.f, 0.f, 0.f}, {0.f, 0.f, 0.f, 0.f},
      {0.f, 0.f, 0.f, .5f}, {0.f, 0.f, 0.f, 0.f},
      {0.f, 0.f, 0.f, float(M_PI/2.)}, {.2f, .2f, .95f, .2f});
  EXPECT_NEAR(sum[0], 2.f, 1e-5);
  EXPECT_NEAR(sum[1], 10.f, 1e-5);
  EXPECT_THROW(cms::detail::puppiType1JetSum({10.f}, {}, {}, {}, {}, {}, {}, {}), std::runtime_error);
}

TEST(CmsMetCorrections, FullWorkflowStartsFromRawIncludesLowJetsAndPropagatesLeptonAndJesVariations) {
  cms::CmsYearRegistry registry; registry.setYear("2024"); const auto ys = registry.load();
  if (!std::filesystem::exists(ys.jmeFile)) GTEST_SKIP() << "CVMFS unavailable";
  for (bool systematics : {false, true}) for (bool isMC : {false, true}) {
  SCOPED_TRACE(std::string(isMC ? "MC" : "data") + (systematics ? " varied" : " nominal"));
  auto an = makeAnalyzer(systematics, isMC); inputs(*an);
  if (isMC) {
    an->Define("event", []() { return ULong64_t(1234); }, {});
    v(*an, "GenJet_pt", {45.f}); v(*an, "GenJet_eta", {.5f}); v(*an, "GenJet_phi", {0.f});
    v(*an, "Jet_genMatchedPt", {45.f, 45.f});
  }
  cms::CmsCorrectionConfig cfg; cfg.yearSettings = &ys;
  cfg.applyJetCorrections = true; cfg.applyMetType1 = true;
  cfg.rebuildPuppiMetFromRaw = true; cfg.jesMode = cms::CmsCorrectionConfig::Total;
  cfg.metMuonMaskColumn = "metMuons"; cfg.metElectronMaskColumn = "metElectrons";
  cms::configureCmsCorrectionWorkflow(*an, cfg);
  auto df = an->getDataFrameProvider().getDataFrame();
  const auto jetPt = df.Take<Vec>("Jet_pt_corr_nominal").GetValue().at(0);
  const auto lowPt = df.Take<Vec>("CorrT1METJet_pt_corr_nominal").GetValue().at(0);
  ASSERT_GT(lowPt.at(0), 15.f);
  const float expectedX = 100.f - .8f * (jetPt[0] - 40.f) - 3.f;
  const float expectedY = -(lowPt[0] - 14.f) - 2.f;
  EXPECT_NEAR(df.Take<float>("PuppiMET_pt_corr_nominal").GetValue().at(0),
              std::hypot(expectedX, expectedY), 2e-5);
  EXPECT_NEAR(df.Take<float>("PuppiMET_phi_corr_nominal").GetValue().at(0),
              std::atan2(expectedY, expectedX), 2e-5);
  EXPECT_FLOAT_EQ(df.Take<float>("PFMET_pt").GetValue().at(0), 73.f);
  if (!systematics) continue;
  auto &sm = an->getSystematicManager();
  const auto electronUp = sm.getVariationColumnName("PuppiMET_pt_corr_nominal", "ElectronScaleUp");
  EXPECT_NE(electronUp, "PuppiMET_pt_corr_nominal");
  EXPECT_NEAR(df.Take<float>(electronUp).GetValue().at(0),
              std::hypot(expectedX, expectedY - 2.f), 2e-5);
  const auto jesUp = sm.getVariationColumnName("PuppiMET_pt_corr_nominal", "totalUp");
  const auto unclusteredUp = sm.getVariationColumnName("PuppiMET_pt_corr_nominal", "UnclusteredMETUp");
  EXPECT_NEAR(df.Take<float>(unclusteredUp).GetValue().at(0),
              std::hypot(expectedX + 5.f, expectedY), 2e-5);
  const auto variedJets = df.Take<Vec>("Jet_pt_jes_total_up").GetValue().at(0);
  const auto variedLow = df.Take<Vec>("CorrT1METJet_pt_jes_total_up").GetValue().at(0);
  EXPECT_NEAR(df.Take<float>(jesUp).GetValue().at(0),
      std::hypot(100.f - .8f * (variedJets[0] - 40.f) - 3.f,
                 -(variedLow[0] - 14.f) - 2.f), 2e-5);
  if (isMC) {
    const auto jerUp = sm.getVariationColumnName("PuppiMET_pt_corr_nominal", "jerUp");
    EXPECT_NE(jerUp, "PuppiMET_pt_corr_nominal");
    EXPECT_TRUE(std::isfinite(df.Take<float>(jerUp).GetValue().at(0)));
  }
  }
}

TEST(CmsMetCorrections, JerMatchingRejectsDistantStoredAssociationsAndWrapsPhi) {
  auto an = makeAnalyzer(false, true);
  an->Define("Jet_genJetIdx_int", []() { return std::vector<int>{0, 1}; }, {});
  v(*an,"Jet_eta",{0.f,0.f}); v(*an,"Jet_phi",{0.f,3.13f});
  v(*an,"GenJet_pt",{40.f,50.f}); v(*an,"GenJet_eta",{.3f,0.f});
  v(*an,"GenJet_phi",{0.f,-3.13f});
  an->Define("FatJet_genJetAK8Idx_int", []() { return ROOT::VecOps::RVec<int>{0,1}; }, {});
  v(*an,"FatJet_eta",{0.f,0.f}); v(*an,"FatJet_phi",{0.f,3.13f});
  v(*an,"GenJetAK8_pt",{100.f,200.f}); v(*an,"GenJetAK8_eta",{.5f,0.f});
  v(*an,"GenJetAK8_phi",{0.f,-3.13f});
  cms::CmsCorrectionConfig cfg; cfg.applyJetCorrections=true; cfg.applyFatJetCorrections=true;
  cms::defineCmsCorrectionInputs(*an,cfg);
  auto df=an->getDataFrameProvider().getDataFrame();
  EXPECT_TRUE(ROOT::VecOps::All(df.Take<Vec>("Jet_genMatchedPt").GetValue().at(0) == Vec{-1.f,50.f}));
  EXPECT_TRUE(ROOT::VecOps::All(df.Take<Vec>("FatJet_genMatchedPt").GetValue().at(0) == Vec{-1.f,200.f}));
}

TEST(CmsMetCorrections, MissingRebuildInputsFailInsteadOfReturningNominalMet) {
  auto an = makeAnalyzer();
  cms::CmsYearRegistry registry; registry.setYear("2024"); const auto ys = registry.load();
  cms::CmsCorrectionConfig cfg; cfg.yearSettings = &ys; cfg.applyJetCorrections = true;
  EXPECT_THROW(cms::defineCmsRebuiltPuppiMet(*an, cfg), std::runtime_error);
}
