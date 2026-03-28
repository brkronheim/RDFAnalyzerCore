#include <ConfigurationManager.h>
#include <CorrectionManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ElectronEnergyScaleManager.h>
#include <JetEnergyScaleManager.h>
#include <ManagerFactory.h>
#include <MuonRochesterManager.h>
#include <NullOutputSink.h>
#include <PhotonEnergyScaleManager.h>
#include <PhysicsObjectCollection.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <test_util.h>

#include "../plugins/CorrectedObjectCollectionManagers/CorrectedObjectCollectionManagers.h"

namespace {

ManagerContext makeContext(IConfigurationProvider &cfg,
                           DataManager &dm,
                           SystematicManager &sm,
                           DefaultLogger &log,
                           NullOutputSink &skim,
                           NullOutputSink &meta) {
  return ManagerContext{cfg, dm, sm, log, skim, meta};
}

void defineRVecColumn(DataManager &dm,
                      const std::string &name,
                      float value,
                      SystematicManager &sm) {
  dm.Define(
      name,
      [value](ULong64_t) -> ROOT::VecOps::RVec<Float_t> { return {value}; },
      {"rdfentry_"}, sm);
}

    void defineIntRVecColumn(DataManager &dm,
             const std::string &name,
             int value,
             SystematicManager &sm) {
      dm.Define(
      name,
      [value](ULong64_t) -> ROOT::VecOps::RVec<Int_t> { return {value}; },
      {"rdfentry_"}, sm);
    }

    void defineScalarColumn(DataManager &dm,
            const std::string &name,
            float value,
            SystematicManager &sm) {
      dm.Define(
      name,
      [value](ULong64_t) -> Float_t { return value; },
      {"rdfentry_"}, sm);
    }

void defineCollectionPtProbe(DataManager &dm,
                             SystematicManager &sm,
                             const std::string &outputColumn,
                             const std::string &collectionColumn) {
  dm.Define(
      outputColumn,
      [](const PhysicsObjectCollection &objects) -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> pts;
        pts.reserve(objects.size());
        for (std::size_t index = 0; index < objects.size(); ++index) {
          pts.push_back(static_cast<Float_t>(objects.at(index).Pt()));
        }
        return pts;
      },
      {collectionColumn}, sm);
}

class CorrectedObjectCollectionManagersTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    config = ManagerFactory::createConfigurationManager(
        "cfg/test_data_config_minimal.txt");
    systematics = std::make_unique<SystematicManager>();
    logger = std::make_unique<DefaultLogger>();
    skimSink = std::make_unique<NullOutputSink>();
    metaSink = std::make_unique<NullOutputSink>();
  }

  ManagerContext context(DataManager &dm) {
    return makeContext(*config, dm, *systematics, *logger, *skimSink, *metaSink);
  }

  std::unique_ptr<IConfigurationProvider> config;
  std::unique_ptr<SystematicManager> systematics;
  std::unique_ptr<DefaultLogger> logger;
  std::unique_ptr<NullOutputSink> skimSink;
  std::unique_ptr<NullOutputSink> metaSink;
};

TEST_F(CorrectedObjectCollectionManagersTest, JetWrapperPropagatesMassVariations) {
  config->set("correctedJetCollectionConfig", "cfg/test_corrected_jets.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Jet_pt", 100.0f, *systematics);
  defineRVecColumn(dm, "Jet_eta", 1.2f, *systematics);
  defineRVecColumn(dm, "Jet_phi", 0.4f, *systematics);
  defineRVecColumn(dm, "Jet_mass", 20.0f, *systematics);
  defineRVecColumn(dm, "Jet_pt_corr_nominal", 110.0f, *systematics);
  defineRVecColumn(dm, "Jet_mass_corr_nominal", 22.0f, *systematics);
  defineRVecColumn(dm, "Jet_pt_jes_total_up", 120.0f, *systematics);
  defineRVecColumn(dm, "Jet_pt_jes_total_down", 100.0f, *systematics);
  defineRVecColumn(dm, "Jet_mass_jes_total_up", 24.0f, *systematics);
  defineRVecColumn(dm, "Jet_mass_jes_total_down", 20.0f, *systematics);

  JetEnergyScaleManager jetManager;
  auto jetContext = context(dm);
  jetManager.setContext(jetContext);
  jetManager.setupFromConfigFile();
  jetManager.setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  jetManager.addVariation("jes_total", "Jet_pt_jes_total_up", "Jet_pt_jes_total_down",
                          "Jet_mass_jes_total_up", "Jet_mass_jes_total_down");

  CorrectedJetCollectionManager wrapper(jetManager);
  wrapper.setContext(jetContext);
  wrapper.setupFromConfigFile();

  jetManager.execute();
  wrapper.execute();

  defineCollectionPtProbe(dm, *systematics, "SelectedJetPts", "CorrectedJets");

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets_jes_totalUp");
  auto down = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets_jes_totalDown");
  auto selectedNominal = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedJetPts");
  auto selectedUp = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedJetPts_jes_totalUp");
  auto selectedDown = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedJetPts_jes_totalDown");

  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 110.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).M()), 22.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 120.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).M()), 24.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(down.GetValue()[0].at(0).Pt()), 100.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(down.GetValue()[0].at(0).M()), 20.0f, 0.01f);
  EXPECT_NEAR(selectedNominal.GetValue()[0][0], 110.0f, 0.01f);
  EXPECT_NEAR(selectedUp.GetValue()[0][0], 120.0f, 0.01f);
  EXPECT_NEAR(selectedDown.GetValue()[0][0], 100.0f, 0.01f);

  const auto &affected = systematics->getVariablesForSystematic("jes_totalUp");
  EXPECT_NE(affected.find("CorrectedJets"), affected.end());
}

TEST_F(CorrectedObjectCollectionManagersTest, ElectronWrapperRegistersCollectionSystematics) {
  config->set("correctedElectronCollectionConfig", "cfg/test_corrected_electrons.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Electron_pt", 50.0f, *systematics);
  defineRVecColumn(dm, "Electron_eta", 0.2f, *systematics);
  defineRVecColumn(dm, "Electron_phi", 1.0f, *systematics);
  defineRVecColumn(dm, "Electron_mass", 0.0005f, *systematics);
  defineRVecColumn(dm, "Electron_pt_corr_nominal", 51.0f, *systematics);
  defineRVecColumn(dm, "Electron_pt_ees_up", 52.0f, *systematics);
  defineRVecColumn(dm, "Electron_pt_ees_down", 50.0f, *systematics);

  ElectronEnergyScaleManager electronManager;
  auto electronContext = context(dm);
  electronManager.setContext(electronContext);
  electronManager.setupFromConfigFile();
  electronManager.setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "Electron_mass");
  electronManager.addVariation("ees", "Electron_pt_ees_up", "Electron_pt_ees_down");

  CorrectedElectronCollectionManager wrapper(electronManager);
  wrapper.setContext(electronContext);
  wrapper.setupFromConfigFile();

  electronManager.execute();
  wrapper.execute();

  defineCollectionPtProbe(dm, *systematics, "SelectedElectronPts", "CorrectedElectrons");

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedElectrons");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedElectrons_eesUp");
  auto variationMap = dm.getDataFrame().Take<PhysicsObjectVariationMap>("CorrectedElectrons_variations");
  auto selectedNominal = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedElectronPts");
  auto selectedUp = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedElectronPts_eesUp");
  auto selectedDown = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedElectronPts_eesDown");

  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 51.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 52.0f, 0.01f);
  EXPECT_NEAR(selectedNominal.GetValue()[0][0], 51.0f, 0.01f);
  EXPECT_NEAR(selectedUp.GetValue()[0][0], 52.0f, 0.01f);
  EXPECT_NEAR(selectedDown.GetValue()[0][0], 50.0f, 0.01f);
  EXPECT_EQ(variationMap.GetValue()[0].count("nominal"), 1u);
  EXPECT_EQ(variationMap.GetValue()[0].count("eesUp"), 1u);
  EXPECT_EQ(variationMap.GetValue()[0].count("eesDown"), 1u);

  const auto &affected = systematics->getVariablesForSystematic("eesUp");
  EXPECT_NE(affected.find("CorrectedElectrons"), affected.end());
}

TEST_F(CorrectedObjectCollectionManagersTest, MuonWrapperRegistersCollectionSystematics) {
  config->set("correctedMuonCollectionConfig", "cfg/test_corrected_muons.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Muon_pt", 20.0f, *systematics);
  defineRVecColumn(dm, "Muon_eta", -0.4f, *systematics);
  defineRVecColumn(dm, "Muon_phi", 0.9f, *systematics);
  defineRVecColumn(dm, "Muon_mass", 0.105f, *systematics);
  defineRVecColumn(dm, "Muon_pt_corr_nominal", 20.3f, *systematics);
  defineRVecColumn(dm, "Muon_pt_stat_up", 20.5f, *systematics);
  defineRVecColumn(dm, "Muon_pt_stat_down", 20.1f, *systematics);

  MuonRochesterManager muonManager;
  auto muonContext = context(dm);
  muonManager.setContext(muonContext);
  muonManager.setupFromConfigFile();
  muonManager.setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  muonManager.addVariation("stat", "Muon_pt_stat_up", "Muon_pt_stat_down");

  CorrectedMuonCollectionManager wrapper(muonManager);
  wrapper.setContext(muonContext);
  wrapper.setupFromConfigFile();

  muonManager.execute();
  wrapper.execute();

  defineCollectionPtProbe(dm, *systematics, "SelectedMuonPts", "CorrectedMuons");

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedMuons");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedMuons_statUp");
  auto down = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedMuons_statDown");
  auto selectedNominal = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedMuonPts");
  auto selectedUp = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedMuonPts_statUp");
  auto selectedDown = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedMuonPts_statDown");

  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 20.3f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 20.5f, 0.01f);
  EXPECT_NEAR(static_cast<float>(down.GetValue()[0].at(0).Pt()), 20.1f, 0.01f);
  EXPECT_NEAR(selectedNominal.GetValue()[0][0], 20.3f, 0.01f);
  EXPECT_NEAR(selectedUp.GetValue()[0][0], 20.5f, 0.01f);
  EXPECT_NEAR(selectedDown.GetValue()[0][0], 20.1f, 0.01f);

  const auto &affected = systematics->getVariablesForSystematic("statUp");
  EXPECT_NE(affected.find("CorrectedMuons"), affected.end());
}

TEST_F(CorrectedObjectCollectionManagersTest, PhotonWrapperRegistersCollectionSystematics) {
  config->set("correctedPhotonCollectionConfig", "cfg/test_corrected_photons.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Photon_pt", 40.0f, *systematics);
  defineRVecColumn(dm, "Photon_eta", 0.7f, *systematics);
  defineRVecColumn(dm, "Photon_phi", -0.2f, *systematics);
  defineRVecColumn(dm, "Photon_mass", 0.0f, *systematics);
  defineRVecColumn(dm, "Photon_pt_corr_nominal", 40.4f, *systematics);
  defineRVecColumn(dm, "Photon_pt_pes_up", 41.0f, *systematics);
  defineRVecColumn(dm, "Photon_pt_pes_down", 39.0f, *systematics);

  PhotonEnergyScaleManager photonManager;
  auto photonContext = context(dm);
  photonManager.setContext(photonContext);
  photonManager.setupFromConfigFile();
  photonManager.setObjectColumns("Photon_pt", "Photon_eta", "Photon_phi", "Photon_mass");
  photonManager.addVariation("pes", "Photon_pt_pes_up", "Photon_pt_pes_down");

  CorrectedPhotonCollectionManager wrapper(photonManager);
  wrapper.setContext(photonContext);
  wrapper.setupFromConfigFile();

  photonManager.execute();
  wrapper.execute();

  defineCollectionPtProbe(dm, *systematics, "SelectedPhotonPts", "CorrectedPhotons");

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedPhotons");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedPhotons_pesUp");
  auto variationMap = dm.getDataFrame().Take<PhysicsObjectVariationMap>("CorrectedPhotons_variations");
  auto selectedNominal = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedPhotonPts");
  auto selectedUp = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedPhotonPts_pesUp");
  auto selectedDown = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("SelectedPhotonPts_pesDown");

  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 40.4f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 41.0f, 0.01f);
  EXPECT_NEAR(selectedNominal.GetValue()[0][0], 40.4f, 0.01f);
  EXPECT_NEAR(selectedUp.GetValue()[0][0], 41.0f, 0.01f);
  EXPECT_NEAR(selectedDown.GetValue()[0][0], 39.0f, 0.01f);
  EXPECT_EQ(variationMap.GetValue()[0].count("nominal"), 1u);
  EXPECT_EQ(variationMap.GetValue()[0].count("pesUp"), 1u);
  EXPECT_EQ(variationMap.GetValue()[0].count("pesDown"), 1u);

  const auto &affected = systematics->getVariablesForSystematic("pesUp");
  EXPECT_NE(affected.find("CorrectedPhotons"), affected.end());
}

TEST_F(CorrectedObjectCollectionManagersTest, JetWrapperReadsWorkflowConfig) {
  config->set("correctedJetCollectionConfig", "cfg/test_corrected_jets_workflow.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Jet_pt", 100.0f, *systematics);
  defineRVecColumn(dm, "Jet_eta", 1.2f, *systematics);
  defineRVecColumn(dm, "Jet_phi", 0.0f, *systematics);
  defineRVecColumn(dm, "Jet_mass", 20.0f, *systematics);
  defineRVecColumn(dm, "Jet_rawFactor", 0.1f, *systematics);
  defineRVecColumn(dm, "jet_nominal_sf", 1.1f, *systematics);
  defineRVecColumn(dm, "Jet_pt_jes_total_up", 104.0f, *systematics);
  defineRVecColumn(dm, "Jet_pt_jes_total_down", 96.0f, *systematics);
  defineRVecColumn(dm, "Jet_mass_jes_total_up", 20.8f, *systematics);
  defineRVecColumn(dm, "Jet_mass_jes_total_down", 19.2f, *systematics);
  defineScalarColumn(dm, "MET_pt", 50.0f, *systematics);
  defineScalarColumn(dm, "MET_phi", 0.0f, *systematics);

  JetEnergyScaleManager jetManager;
  auto jetContext = context(dm);
  jetManager.setContext(jetContext);
  jetManager.setupFromConfigFile();

  CorrectedJetCollectionManager wrapper(jetManager);
  wrapper.setContext(jetContext);
  wrapper.setupFromConfigFile();

  jetManager.execute();
  wrapper.execute();

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets_jes_totalUp");
  auto down = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedJets_jes_totalDown");
  auto metNominal = dm.getDataFrame().Take<float>("MET_pt_corr_nominal");
  auto metUp = dm.getDataFrame().Take<float>("MET_pt_jes_total_up");
  auto metDown = dm.getDataFrame().Take<float>("MET_pt_jes_total_down");

  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 99.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).M()), 19.8f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 104.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(down.GetValue()[0].at(0).Pt()), 96.0f, 0.01f);
  EXPECT_NEAR(metNominal.GetValue()[0], 51.0f, 0.01f);
  EXPECT_NEAR(metUp.GetValue()[0], 46.0f, 0.01f);
  EXPECT_NEAR(metDown.GetValue()[0], 54.0f, 0.01f);
}

TEST_F(CorrectedObjectCollectionManagersTest, ElectronWrapperWorkflowCanDriveCorrectionManager) {
  config->set("type", "mc_test");
  config->set("correctedElectronCollectionConfig",
              "cfg/test_corrected_electrons_workflow.txt");

  DataManager dm(1);
  defineRVecColumn(dm, "Electron_pt", 50.0f, *systematics);
  defineRVecColumn(dm, "Electron_eta", 0.2f, *systematics);
  defineRVecColumn(dm, "Electron_phi", 1.0f, *systematics);
  defineRVecColumn(dm, "Electron_mass", 0.0005f, *systematics);
  defineRVecColumn(dm, "Electron_pt_corr_nominal", 50.0f, *systematics);
  defineRVecColumn(dm, "Electron_corr_arg", 0.5f, *systematics);
  defineIntRVecColumn(dm, "Electron_bin", 1, *systematics);

  ElectronEnergyScaleManager electronManager;
  auto electronContext = context(dm);
  electronManager.setContext(electronContext);
  electronManager.setupFromConfigFile();

  auto correctionManager = std::make_unique<CorrectionManager>(*config);
  correctionManager->setContext(electronContext);

  CorrectedElectronCollectionManager wrapper(electronManager, correctionManager.get());
  wrapper.setContext(electronContext);
  wrapper.setupFromConfigFile();

  electronManager.execute();
  wrapper.execute();

  auto nominal = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedElectrons");
  auto up = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedElectrons_electron_scaleUp");
  auto down = dm.getDataFrame().Take<PhysicsObjectCollection>("CorrectedElectrons_electron_scaleDown");
  auto uncertainty = dm.getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>(
      "vhqq_electron_scale_uncertainty");

  EXPECT_NEAR(uncertainty.GetValue()[0][0], 0.1f, 1e-6f);
  EXPECT_NEAR(static_cast<float>(nominal.GetValue()[0].at(0).Pt()), 50.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(up.GetValue()[0].at(0).Pt()), 55.0f, 0.01f);
  EXPECT_NEAR(static_cast<float>(down.GetValue()[0].at(0).Pt()), 45.0f, 0.01f);

  const auto &affected = systematics->getVariablesForSystematic("electron_scaleUp");
  EXPECT_NE(affected.find("CorrectedElectrons"), affected.end());
}

} // namespace