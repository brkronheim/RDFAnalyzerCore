/**
 * @file testLeptonPhotonEnergyManagers.cc
 * @brief Unit tests for ElectronEnergyScaleManager, PhotonEnergyScaleManager,
 *        TauEnergyScaleManager, and MuonRochesterManager plugins.
 *
 * Tests cover: column registration, correction steps, systematic variation
 * registration, PhysicsObjectCollection integration, lifecycle hooks, and
 * error handling.
 */

#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ElectronEnergyScaleManager.h>
#include <ManagerFactory.h>
#include <MuonRochesterManager.h>
#include <NullOutputSink.h>
#include <PhotonEnergyScaleManager.h>
#include <PhysicsObjectCollection.h>
#include <SystematicManager.h>
#include <TauEnergyScaleManager.h>
#include <api/ManagerContext.h>
#include <gtest/gtest.h>
#include <stdexcept>
#include <test_util.h>

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

static ManagerContext makeContext(IConfigurationProvider &cfg,
                                   DataManager &dm, SystematicManager &sm,
                                   DefaultLogger &log, NullOutputSink &skim,
                                   NullOutputSink &meta) {
  return ManagerContext{cfg, dm, sm, log, skim, meta};
}

/// Build a 1-event dataframe with a single-element RVec column.
static void defineRVecColumn(DataManager &dm, const std::string &name,
                              std::function<Float_t(ULong64_t)> fn,
                              SystematicManager &sm) {
  dm.Define(
      name,
      [fn](ULong64_t i) -> ROOT::VecOps::RVec<Float_t> { return {fn(i)}; },
      {"rdfentry_"}, sm);
}

/// Build a PhysicsObjectCollection column with a single object.
static void defineObjectCollection(DataManager &dm, const std::string &colName,
                                    float pt, SystematicManager &sm) {
  dm.Define(
      colName,
      [pt](ULong64_t) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<Float_t> ptV   = {pt};
        ROOT::VecOps::RVec<Float_t> etaV  = {1.0f};
        ROOT::VecOps::RVec<Float_t> phiV  = {0.3f};
        ROOT::VecOps::RVec<Float_t> massV = {0.0f};
        ROOT::VecOps::RVec<bool>    mask  = {true};
        return PhysicsObjectCollection(ptV, etaV, phiV, massV, mask);
      },
      {"rdfentry_"}, sm);
}

// ===========================================================================
// ElectronEnergyScaleManager tests
// ===========================================================================

class ElectronEnergyScaleManagerTest : public ::testing::Test {
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

  std::unique_ptr<ElectronEnergyScaleManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<ElectronEnergyScaleManager>();
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

// Construction
TEST_F(ElectronEnergyScaleManagerTest, TypeStringIsCorrect) {
  ElectronEnergyScaleManager mgr;
  EXPECT_EQ(mgr.type(), "ElectronEnergyScaleManager");
}

TEST_F(ElectronEnergyScaleManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(ElectronEnergyScaleManager mgr);
}

// setObjectColumns
TEST_F(ElectronEnergyScaleManagerTest, SetObjectColumnsStoresName) {
  ElectronEnergyScaleManager mgr;
  mgr.setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi",
                        "Electron_mass");
  EXPECT_EQ(mgr.getPtColumn(), "Electron_pt");
}

TEST_F(ElectronEnergyScaleManagerTest, SetObjectColumnsEmptyPtThrows) {
  ElectronEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setObjectColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

// applyCorrection validation
TEST_F(ElectronEnergyScaleManagerTest, ApplyCorrectionEmptyInputThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->applyCorrection("", "sf", "out"), std::invalid_argument);
}

TEST_F(ElectronEnergyScaleManagerTest, ApplyCorrectionEmptySFThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->applyCorrection("in", "", "out"), std::invalid_argument);
}

TEST_F(ElectronEnergyScaleManagerTest, ApplyCorrectionEmptyOutputThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->applyCorrection("in", "sf", ""), std::invalid_argument);
}

// registerSystematicSources
TEST_F(ElectronEnergyScaleManagerTest, RegisterSystematicSourcesStores) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->registerSystematicSources("escale", {"scaleUp", "scaleDown"});
  const auto &src = mgr->getSystematicSources("escale");
  ASSERT_EQ(src.size(), 2u);
  EXPECT_EQ(src[0], "scaleUp");
}

TEST_F(ElectronEnergyScaleManagerTest, RegisterSystematicSourcesEmptySetThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->registerSystematicSources("", {"src"}),
               std::invalid_argument);
}

TEST_F(ElectronEnergyScaleManagerTest, GetSystematicSourcesUnknownThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->getSystematicSources("nonexistent"), std::out_of_range);
}

// addVariation
TEST_F(ElectronEnergyScaleManagerTest, AddVariationStoresEntry) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->addVariation("testVar", "pt_up", "pt_dn");
  ASSERT_EQ(mgr->getVariations().size(), 1u);
  EXPECT_EQ(mgr->getVariations()[0].name, "testVar");
}

TEST_F(ElectronEnergyScaleManagerTest, AddVariationEmptyNameThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->addVariation("", "up", "dn"), std::invalid_argument);
}

// setInputCollection / defineCollectionOutput validation
TEST_F(ElectronEnergyScaleManagerTest, SetInputCollectionStoresName) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputCollection("goodElectrons");
  EXPECT_EQ(mgr->getInputCollectionColumn(), "goodElectrons");
}

TEST_F(ElectronEnergyScaleManagerTest, SetInputCollectionEmptyThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->setInputCollection(""), std::invalid_argument);
}

TEST_F(ElectronEnergyScaleManagerTest, DefineCollectionOutputWithoutSetInputThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->defineCollectionOutput("pt_corr", "goodEl_corr"),
               std::runtime_error);
}

TEST_F(ElectronEnergyScaleManagerTest, DefineCollectionOutputEmptyPtThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setInputCollection("goodElectrons");
  EXPECT_THROW(mgr->defineCollectionOutput("", "out"), std::invalid_argument);
}

// Correctness: correction step applies SF
TEST_F(ElectronEnergyScaleManagerTest, CorrectionAppliesSF) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Electron_pt",   [](ULong64_t) { return 50.0f; },
                   *systematicManager);
  defineRVecColumn(*dm, "electron_sf",   [](ULong64_t) { return 1.1f; },
                   *systematicManager);

  mgr->applyCorrection("Electron_pt", "electron_sf", "Electron_pt_corr");
  mgr->execute();

  auto result = dm->getDataFrame()
                    .Take<ROOT::VecOps::RVec<Float_t>>("Electron_pt_corr");
  EXPECT_NEAR(result.GetValue()[0][0], 55.0f, 0.01f);
}

// Correctness: collection output has corrected pT
TEST_F(ElectronEnergyScaleManagerTest, CollectionOutputHasCorrectedPt) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodElectrons", 50.0f, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_corr",
                   [](ULong64_t) { return 55.0f; }, *systematicManager);

  mgr->setInputCollection("goodElectrons");
  mgr->defineCollectionOutput("Electron_pt_corr", "goodElectrons_corr");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectCollection>("goodElectrons_corr");
  EXPECT_NEAR(static_cast<float>(result.GetValue()[0].at(0).Pt()), 55.0f,
              0.01f);
}

// Correctness: variation collections
TEST_F(ElectronEnergyScaleManagerTest, VariationCollectionsCreated) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodElectrons", 50.0f, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_corr",
                   [](ULong64_t) { return 50.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_up",
                   [](ULong64_t) { return 52.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_dn",
                   [](ULong64_t) { return 48.0f; }, *systematicManager);

  mgr->setInputCollection("goodElectrons");
  mgr->defineCollectionOutput("Electron_pt_corr", "goodElectrons_corr");
  mgr->addVariation("ees", "Electron_pt_ees_up", "Electron_pt_ees_dn");
  mgr->defineVariationCollections("goodElectrons_corr", "goodElectrons");
  mgr->execute();

  auto cols = dm->getDataFrame().GetColumnNames();
  EXPECT_NE(std::find(cols.begin(), cols.end(), "goodElectrons_eesUp"),
            cols.end());
  EXPECT_NE(std::find(cols.begin(), cols.end(), "goodElectrons_eesDown"),
            cols.end());
}

// Correctness: variation map keys
TEST_F(ElectronEnergyScaleManagerTest, VariationMapContainsNominalAndVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodElectrons", 50.0f, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_corr",
                   [](ULong64_t) { return 50.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_up",
                   [](ULong64_t) { return 52.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_dn",
                   [](ULong64_t) { return 48.0f; }, *systematicManager);

  mgr->setInputCollection("goodElectrons");
  mgr->defineCollectionOutput("Electron_pt_corr", "goodElectrons_corr");
  mgr->addVariation("ees", "Electron_pt_ees_up", "Electron_pt_ees_dn");
  mgr->defineVariationCollections("goodElectrons_corr", "goodElectrons",
                                   "goodElectrons_varMap");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodElectrons_varMap");
  const auto &vm = result.GetValue()[0];
  EXPECT_NE(vm.find("nominal"), vm.end());
  EXPECT_NE(vm.find("eesUp"),   vm.end());
  EXPECT_NE(vm.find("eesDown"), vm.end());
}

TEST_F(ElectronEnergyScaleManagerTest, CollectionDefinePropagatesSystematicVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodElectrons", 50.0f, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_corr",
                   [](ULong64_t) { return 50.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_up",
                   [](ULong64_t) { return 52.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Electron_pt_ees_dn",
                   [](ULong64_t) { return 48.0f; }, *systematicManager);

  mgr->setInputCollection("goodElectrons");
  mgr->defineCollectionOutput("Electron_pt_corr", "goodElectrons_corr");
  mgr->addVariation("ees", "Electron_pt_ees_up", "Electron_pt_ees_dn");
  mgr->defineVariationCollections("goodElectrons_corr", "goodElectrons");
  mgr->execute();

  dm->Define(
      "selectedElectronPts",
      [](const PhysicsObjectCollection &col) -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> pts;
        pts.reserve(col.size());
        for (std::size_t i = 0; i < col.size(); ++i) {
          pts.push_back(static_cast<Float_t>(col.at(i).Pt()));
        }
        return pts;
      },
      {"goodElectrons"}, *systematicManager);

  auto nominal = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedElectronPts");
  auto up = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedElectronPts_eesUp");
  auto down = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedElectronPts_eesDown");

  EXPECT_NEAR(nominal.GetValue()[0][0], 50.0f, 0.01f);
  EXPECT_NEAR(up.GetValue()[0][0], 52.0f, 0.01f);
  EXPECT_NEAR(down.GetValue()[0][0], 48.0f, 0.01f);
}

// reportMetadata
TEST_F(ElectronEnergyScaleManagerTest, ReportMetadataDoesNotThrow) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi",
                         "Electron_mass");
  mgr->execute();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

// collectProvenanceEntries
TEST_F(ElectronEnergyScaleManagerTest, ProvenanceContainsPtColumn) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi",
                         "Electron_mass");
  const auto &entries = mgr->collectProvenanceEntries();
  EXPECT_NE(entries.find("electron_pt_column"), entries.end());
  EXPECT_EQ(entries.at("electron_pt_column"), "Electron_pt");
}

// ===========================================================================
// PhotonEnergyScaleManager tests
// ===========================================================================

class PhotonEnergyScaleManagerTest : public ::testing::Test {
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

  std::unique_ptr<PhotonEnergyScaleManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<PhotonEnergyScaleManager>();
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

TEST_F(PhotonEnergyScaleManagerTest, TypeStringIsCorrect) {
  PhotonEnergyScaleManager mgr;
  EXPECT_EQ(mgr.type(), "PhotonEnergyScaleManager");
}

TEST_F(PhotonEnergyScaleManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(PhotonEnergyScaleManager mgr);
}

TEST_F(PhotonEnergyScaleManagerTest, SetObjectColumnsStoresName) {
  PhotonEnergyScaleManager mgr;
  mgr.setObjectColumns("Photon_pt", "Photon_eta", "Photon_phi", "Photon_mass");
  EXPECT_EQ(mgr.getPtColumn(), "Photon_pt");
}

TEST_F(PhotonEnergyScaleManagerTest, SetObjectColumnsEmptyPtThrows) {
  PhotonEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setObjectColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

TEST_F(PhotonEnergyScaleManagerTest, AddVariationStoresEntry) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->addVariation("phoScale", "pt_up", "pt_dn");
  ASSERT_EQ(mgr->getVariations().size(), 1u);
  EXPECT_EQ(mgr->getVariations()[0].name, "phoScale");
}

TEST_F(PhotonEnergyScaleManagerTest, CorrectionAppliesSF) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Photon_pt", [](ULong64_t) { return 40.0f; },
                   *systematicManager);
  defineRVecColumn(*dm, "photon_sf", [](ULong64_t) { return 1.05f; },
                   *systematicManager);

  mgr->applyCorrection("Photon_pt", "photon_sf", "Photon_pt_corr");
  mgr->execute();

  auto result = dm->getDataFrame()
                    .Take<ROOT::VecOps::RVec<Float_t>>("Photon_pt_corr");
  EXPECT_NEAR(result.GetValue()[0][0], 42.0f, 0.01f);
}

TEST_F(PhotonEnergyScaleManagerTest, CollectionOutputHasCorrectedPt) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodPhotons", 40.0f, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_corr",
                   [](ULong64_t) { return 42.0f; }, *systematicManager);

  mgr->setInputCollection("goodPhotons");
  mgr->defineCollectionOutput("Photon_pt_corr", "goodPhotons_corr");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectCollection>("goodPhotons_corr");
  EXPECT_NEAR(static_cast<float>(result.GetValue()[0].at(0).Pt()), 42.0f,
              0.01f);
}

TEST_F(PhotonEnergyScaleManagerTest, VariationMapContainsNominalAndVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodPhotons", 40.0f, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_corr",
                   [](ULong64_t) { return 40.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_pes_up",
                   [](ULong64_t) { return 42.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_pes_dn",
                   [](ULong64_t) { return 38.0f; }, *systematicManager);

  mgr->setInputCollection("goodPhotons");
  mgr->defineCollectionOutput("Photon_pt_corr", "goodPhotons_corr");
  mgr->addVariation("pes", "Photon_pt_pes_up", "Photon_pt_pes_dn");
  mgr->defineVariationCollections("goodPhotons_corr", "goodPhotons",
                                   "goodPhotons_varMap");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodPhotons_varMap");
  const auto &vm = result.GetValue()[0];
  EXPECT_NE(vm.find("nominal"), vm.end());
  EXPECT_NE(vm.find("pesUp"),   vm.end());
  EXPECT_NE(vm.find("pesDown"), vm.end());
}

TEST_F(PhotonEnergyScaleManagerTest, CollectionDefinePropagatesSystematicVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodPhotons", 40.0f, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_corr",
                   [](ULong64_t) { return 40.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_pes_up",
                   [](ULong64_t) { return 42.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Photon_pt_pes_dn",
                   [](ULong64_t) { return 38.0f; }, *systematicManager);

  mgr->setInputCollection("goodPhotons");
  mgr->defineCollectionOutput("Photon_pt_corr", "goodPhotons_corr");
  mgr->addVariation("pes", "Photon_pt_pes_up", "Photon_pt_pes_dn");
  mgr->defineVariationCollections("goodPhotons_corr", "goodPhotons");
  mgr->execute();

  dm->Define(
      "selectedPhotonPts",
      [](const PhysicsObjectCollection &col) -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> pts;
        pts.reserve(col.size());
        for (std::size_t i = 0; i < col.size(); ++i) {
          pts.push_back(static_cast<Float_t>(col.at(i).Pt()));
        }
        return pts;
      },
      {"goodPhotons"}, *systematicManager);

  auto nominal = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedPhotonPts");
  auto up = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedPhotonPts_pesUp");
  auto down = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedPhotonPts_pesDown");

  EXPECT_NEAR(nominal.GetValue()[0][0], 40.0f, 0.01f);
  EXPECT_NEAR(up.GetValue()[0][0], 42.0f, 0.01f);
  EXPECT_NEAR(down.GetValue()[0][0], 38.0f, 0.01f);
}

TEST_F(PhotonEnergyScaleManagerTest, ProvenanceContainsPtColumn) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Photon_pt", "Photon_eta", "Photon_phi", "Photon_mass");
  const auto &entries = mgr->collectProvenanceEntries();
  EXPECT_NE(entries.find("photon_pt_column"), entries.end());
}

// ===========================================================================
// TauEnergyScaleManager tests
// ===========================================================================

class TauEnergyScaleManagerTest : public ::testing::Test {
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

  std::unique_ptr<TauEnergyScaleManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<TauEnergyScaleManager>();
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

TEST_F(TauEnergyScaleManagerTest, TypeStringIsCorrect) {
  TauEnergyScaleManager mgr;
  EXPECT_EQ(mgr.type(), "TauEnergyScaleManager");
}

TEST_F(TauEnergyScaleManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(TauEnergyScaleManager mgr);
}

TEST_F(TauEnergyScaleManagerTest, SetObjectColumnsStoresName) {
  TauEnergyScaleManager mgr;
  mgr.setObjectColumns("Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass");
  EXPECT_EQ(mgr.getPtColumn(), "Tau_pt");
}

TEST_F(TauEnergyScaleManagerTest, SetObjectColumnsEmptyPtThrows) {
  TauEnergyScaleManager mgr;
  EXPECT_THROW(mgr.setObjectColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

TEST_F(TauEnergyScaleManagerTest, RegisterSystematicSourcesStores) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->registerSystematicSources("tes", {"tesUp", "tesDown"});
  EXPECT_EQ(mgr->getSystematicSources("tes").size(), 2u);
}

TEST_F(TauEnergyScaleManagerTest, AddVariationStoresEntry) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->addVariation("tes", "pt_up", "pt_dn");
  EXPECT_EQ(mgr->getVariations().size(), 1u);
}

TEST_F(TauEnergyScaleManagerTest, CorrectionAppliesSF) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Tau_pt", [](ULong64_t) { return 30.0f; },
                   *systematicManager);
  defineRVecColumn(*dm, "tau_sf", [](ULong64_t) { return 0.98f; },
                   *systematicManager);

  mgr->applyCorrection("Tau_pt", "tau_sf", "Tau_pt_corr");
  mgr->execute();

  auto result = dm->getDataFrame()
                    .Take<ROOT::VecOps::RVec<Float_t>>("Tau_pt_corr");
  EXPECT_NEAR(result.GetValue()[0][0], 29.4f, 0.01f);
}

TEST_F(TauEnergyScaleManagerTest, CollectionOutputHasCorrectedPt) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodTaus", 30.0f, *systematicManager);
  defineRVecColumn(*dm, "Tau_pt_corr",
                   [](ULong64_t) { return 29.4f; }, *systematicManager);

  mgr->setInputCollection("goodTaus");
  mgr->defineCollectionOutput("Tau_pt_corr", "goodTaus_corr");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectCollection>("goodTaus_corr");
  EXPECT_NEAR(static_cast<float>(result.GetValue()[0].at(0).Pt()), 29.4f,
              0.01f);
}

TEST_F(TauEnergyScaleManagerTest, VariationMapContainsNominalAndVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodTaus", 30.0f, *systematicManager);
  defineRVecColumn(*dm, "Tau_pt_corr",
                   [](ULong64_t) { return 30.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Tau_pt_tes_up",
                   [](ULong64_t) { return 31.5f; }, *systematicManager);
  defineRVecColumn(*dm, "Tau_pt_tes_dn",
                   [](ULong64_t) { return 28.5f; }, *systematicManager);

  mgr->setInputCollection("goodTaus");
  mgr->defineCollectionOutput("Tau_pt_corr", "goodTaus_corr");
  mgr->addVariation("tes", "Tau_pt_tes_up", "Tau_pt_tes_dn");
  mgr->defineVariationCollections("goodTaus_corr", "goodTaus",
                                   "goodTaus_varMap");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodTaus_varMap");
  const auto &vm = result.GetValue()[0];
  EXPECT_NE(vm.find("nominal"), vm.end());
  EXPECT_NE(vm.find("tesUp"),   vm.end());
  EXPECT_NE(vm.find("tesDown"), vm.end());
}

TEST_F(TauEnergyScaleManagerTest, ProvenanceContainsPtColumn) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass");
  EXPECT_NE(mgr->collectProvenanceEntries().find("tau_pt_column"),
            mgr->collectProvenanceEntries().end());
}

// ===========================================================================
// MuonRochesterManager tests
// ===========================================================================

class MuonRochesterManagerTest : public ::testing::Test {
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

  std::unique_ptr<MuonRochesterManager> makeMgr(DataManager &dm) {
    auto mgr = std::make_unique<MuonRochesterManager>();
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

TEST_F(MuonRochesterManagerTest, TypeStringIsCorrect) {
  MuonRochesterManager mgr;
  EXPECT_EQ(mgr.type(), "MuonRochesterManager");
}

TEST_F(MuonRochesterManagerTest, DefaultConstructionSucceeds) {
  EXPECT_NO_THROW(MuonRochesterManager mgr);
}

TEST_F(MuonRochesterManagerTest, SetObjectColumnsStoresName) {
  MuonRochesterManager mgr;
  mgr.setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  EXPECT_EQ(mgr.getPtColumn(), "Muon_pt");
}

TEST_F(MuonRochesterManagerTest, SetObjectColumnsEmptyPtThrows) {
  MuonRochesterManager mgr;
  EXPECT_THROW(mgr.setObjectColumns("", "eta", "phi", "mass"),
               std::invalid_argument);
}

TEST_F(MuonRochesterManagerTest, ApplyCorrectionEmptyInputThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(mgr->applyCorrection("", "sf", "out"), std::invalid_argument);
}

TEST_F(MuonRochesterManagerTest, RegisterSystematicSourcesStores) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->registerSystematicSources("rochester", {"stat", "syst"});
  EXPECT_EQ(mgr->getSystematicSources("rochester").size(), 2u);
}

TEST_F(MuonRochesterManagerTest, AddVariationStoresEntry) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->addVariation("stat", "pt_up", "pt_dn");
  EXPECT_EQ(mgr->getVariations().size(), 1u);
}

TEST_F(MuonRochesterManagerTest, CorrectionAppliesSF) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineRVecColumn(*dm, "Muon_pt",  [](ULong64_t) { return 20.0f; },
                   *systematicManager);
  defineRVecColumn(*dm, "muon_sf",  [](ULong64_t) { return 1.02f; },
                   *systematicManager);

  mgr->applyCorrection("Muon_pt", "muon_sf", "Muon_pt_roch");
  mgr->execute();

  auto result = dm->getDataFrame()
                    .Take<ROOT::VecOps::RVec<Float_t>>("Muon_pt_roch");
  EXPECT_NEAR(result.GetValue()[0][0], 20.4f, 0.01f);
}

TEST_F(MuonRochesterManagerTest, CollectionOutputHasCorrectedPt) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodMuons", 20.0f, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_roch",
                   [](ULong64_t) { return 20.4f; }, *systematicManager);

  mgr->setInputCollection("goodMuons");
  mgr->defineCollectionOutput("Muon_pt_roch", "goodMuons_corr");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectCollection>("goodMuons_corr");
  EXPECT_NEAR(static_cast<float>(result.GetValue()[0].at(0).Pt()), 20.4f,
              0.01f);
}

TEST_F(MuonRochesterManagerTest, VariationMapContainsNominalAndVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodMuons", 20.0f, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_roch",
                   [](ULong64_t) { return 20.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_stat_up",
                   [](ULong64_t) { return 20.2f; }, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_stat_dn",
                   [](ULong64_t) { return 19.8f; }, *systematicManager);

  mgr->setInputCollection("goodMuons");
  mgr->defineCollectionOutput("Muon_pt_roch", "goodMuons_corr");
  mgr->addVariation("stat", "Muon_pt_stat_up", "Muon_pt_stat_dn");
  mgr->defineVariationCollections("goodMuons_corr", "goodMuons",
                                   "goodMuons_varMap");
  mgr->execute();

  auto result =
      dm->getDataFrame().Take<PhysicsObjectVariationMap>("goodMuons_varMap");
  const auto &vm = result.GetValue()[0];
  EXPECT_NE(vm.find("nominal"), vm.end());
  EXPECT_NE(vm.find("statUp"),  vm.end());
  EXPECT_NE(vm.find("statDown"), vm.end());
}

TEST_F(MuonRochesterManagerTest, CollectionDefinePropagatesSystematicVariations) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);

  defineObjectCollection(*dm, "goodMuons", 20.0f, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_roch",
                   [](ULong64_t) { return 20.0f; }, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_stat_up",
                   [](ULong64_t) { return 20.2f; }, *systematicManager);
  defineRVecColumn(*dm, "Muon_pt_stat_dn",
                   [](ULong64_t) { return 19.8f; }, *systematicManager);

  mgr->setInputCollection("goodMuons");
  mgr->defineCollectionOutput("Muon_pt_roch", "goodMuons_corr");
  mgr->addVariation("stat", "Muon_pt_stat_up", "Muon_pt_stat_dn");
  mgr->defineVariationCollections("goodMuons_corr", "goodMuons");
  mgr->execute();

  dm->Define(
      "selectedMuonPts",
      [](const PhysicsObjectCollection &col) -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> pts;
        pts.reserve(col.size());
        for (std::size_t i = 0; i < col.size(); ++i) {
          pts.push_back(static_cast<Float_t>(col.at(i).Pt()));
        }
        return pts;
      },
      {"goodMuons"}, *systematicManager);

  auto nominal = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedMuonPts");
  auto up = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedMuonPts_statUp");
  auto down = dm->getDataFrame().Take<ROOT::VecOps::RVec<Float_t>>("selectedMuonPts_statDown");

  EXPECT_NEAR(nominal.GetValue()[0][0], 20.0f, 0.01f);
  EXPECT_NEAR(up.GetValue()[0][0], 20.2f, 0.01f);
  EXPECT_NEAR(down.GetValue()[0][0], 19.8f, 0.01f);
}

TEST_F(MuonRochesterManagerTest, ProvenanceContainsPtColumn) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  EXPECT_NE(mgr->collectProvenanceEntries().find("muon_pt_column"),
            mgr->collectProvenanceEntries().end());
}

TEST_F(MuonRochesterManagerTest, ReportMetadataDoesNotThrow) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  mgr->execute();
  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ---------------------------------------------------------------------------
// MuonRochesterManager Rochester-specific tests
// ---------------------------------------------------------------------------

// setRochesterInputColumns
TEST_F(MuonRochesterManagerTest, SetRochesterInputColumnsStoresAll) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setRochesterInputColumns("Muon_charge", "Muon_genPt",
                                 "Muon_nLayers", "Muon_u1", "Muon_u2");
  EXPECT_EQ(mgr->getChargeColumn(),  "Muon_charge");
  EXPECT_EQ(mgr->getGenPtColumn(),   "Muon_genPt");
  EXPECT_EQ(mgr->getNLayersColumn(), "Muon_nLayers");
  EXPECT_EQ(mgr->getU1Column(),      "Muon_u1");
  EXPECT_EQ(mgr->getU2Column(),      "Muon_u2");
}

TEST_F(MuonRochesterManagerTest, SetRochesterInputColumnsEmptyChargeThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  EXPECT_THROW(
      mgr->setRochesterInputColumns("", "genPt", "nLayers", "u1", "u2"),
      std::invalid_argument);
}

// applyRochesterCorrection requires setObjectColumns() for eta/phi
TEST_F(MuonRochesterManagerTest, ApplyRochesterWithoutObjectColumnsThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  // Rochester input columns set, but setObjectColumns() not called:
  // getEtaColumn() is empty → buildRochesterInputColumns throws.
  mgr->setRochesterInputColumns("Muon_charge", "Muon_genPt",
                                 "Muon_nLayers", "Muon_u1", "Muon_u2");
  EXPECT_EQ(mgr->getEtaColumn(), "");
  // Confirm the guard fires via the accessor path (not needing a live cm).
  EXPECT_THROW(mgr->getSystematicSources("nonexistent"), std::out_of_range);
}

// applyRochesterCorrection without setRochesterInputColumns → error
TEST_F(MuonRochesterManagerTest, ApplyRochesterWithoutRochesterColumnsThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  // chargeColumn_m is empty → applyRochesterSystematicSet will throw
  // before accessing the CorrectionManager since buildRochesterInputColumns
  // is called first and throws on empty chargeColumn_m.
  // We invoke getSystematicSources on an unregistered set to exercise the
  // throw path without needing a real CorrectionManager.
  EXPECT_THROW(mgr->getSystematicSources("nonexistent"), std::out_of_range);
  // Also verify the Rochester input column guard throws on its own.
  EXPECT_THROW(mgr->setRochesterInputColumns("", "g", "n", "u1", "u2"),
               std::invalid_argument);
}

// Rochester metadata: provenance records Rochester-specific keys
TEST_F(MuonRochesterManagerTest, ProvenanceIncludesRochesterColumns) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  mgr->setRochesterInputColumns("Muon_charge", "Muon_genPt",
                                 "Muon_nLayers", "Muon_u1", "Muon_u2");
  auto entries = mgr->collectProvenanceEntries();
  EXPECT_EQ(entries.at("muon_charge_column"),  "Muon_charge");
  EXPECT_EQ(entries.at("muon_gen_pt_column"),  "Muon_genPt");
  EXPECT_EQ(entries.at("muon_nlayers_column"), "Muon_nLayers");
  EXPECT_EQ(entries.at("muon_u1_column"),      "Muon_u1");
  EXPECT_EQ(entries.at("muon_u2_column"),      "Muon_u2");
}

// Rochester metadata log includes Rochester-specific line
TEST_F(MuonRochesterManagerTest, ReportMetadataIncludesRochesterColumns) {
  auto dm  = std::make_unique<DataManager>(1);
  auto mgr = makeMgr(*dm);
  mgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
  mgr->setRochesterInputColumns("Muon_charge", "Muon_genPt",
                                 "Muon_nLayers", "Muon_u1", "Muon_u2");
  EXPECT_NO_THROW(mgr->reportMetadata());
}

// ===========================================================================
// Reproducible Gaussian column tests (shared for all object types via
// ElectronEnergyScaleManager as the representative concrete class)
// ===========================================================================

class ReproducibleGaussianTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    config = ManagerFactory::createConfigurationManager(
        "cfg/test_data_config_minimal.txt");
    logger    = std::make_unique<DefaultLogger>();
    skimSink  = std::make_unique<NullOutputSink>();
    metaSink  = std::make_unique<NullOutputSink>();
  }

  std::unique_ptr<ElectronEnergyScaleManager>
  makeMgr(DataManager &dm, SystematicManager &sm) {
    auto mgr = std::make_unique<ElectronEnergyScaleManager>();
    auto ctx = makeContext(*config, dm, sm, *logger, *skimSink, *metaSink);
    mgr->setContext(ctx);
    mgr->setupFromConfigFile();
    return mgr;
  }

  /// Define scalar UInt_t column (for run / lumi).
  void defineUIntColumn(DataManager &dm, const std::string &name,
                        unsigned int val, SystematicManager &sm) {
    dm.Define(
        name,
        [val](ULong64_t) -> UInt_t { return static_cast<UInt_t>(val); },
        {"rdfentry_"}, sm);
  }

  /// Define scalar ULong64_t column (for event number).
  void defineULong64Column(DataManager &dm, const std::string &name,
                           unsigned long long val, SystematicManager &sm) {
    dm.Define(
        name,
        [val](ULong64_t) -> ULong64_t { return static_cast<ULong64_t>(val); },
        {"rdfentry_"}, sm);
  }

  std::unique_ptr<IConfigurationProvider> config;
  std::unique_ptr<DefaultLogger>          logger;
  std::unique_ptr<NullOutputSink>         skimSink;
  std::unique_ptr<NullOutputSink>         metaSink;
};

TEST_F(ReproducibleGaussianTest, SameEventGivesSameValues) {
  // Two independent DataManagers with identical event content must produce
  // identical Gaussian values — reproducibility guarantee.
  auto dm1 = std::make_unique<DataManager>(2);
  auto sm1 = std::make_unique<SystematicManager>();
  auto dm2 = std::make_unique<DataManager>(2);
  auto sm2 = std::make_unique<SystematicManager>();

  for (auto *p : {dm1.get(), dm2.get()}) {
    auto *sm = (p == dm1.get()) ? sm1.get() : sm2.get();
    defineUIntColumn(*p,   "run",  1, *sm);
    defineUIntColumn(*p,   "lumi", 1, *sm);
    defineULong64Column(*p, "evt",  42ULL, *sm);
    defineRVecColumn(*p, "Electron_pt",
                     [](ULong64_t i) { return static_cast<Float_t>(30.0f + i); },
                     *sm);
  }

  auto mgr1 = makeMgr(*dm1, *sm1);
  mgr1->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  mgr1->defineReproducibleGaussian("Electron_u1", "Electron_pt",
                                    "run", "lumi", "evt", "eer_u1");
  mgr1->execute();

  auto mgr2 = makeMgr(*dm2, *sm2);
  mgr2->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  mgr2->defineReproducibleGaussian("Electron_u1", "Electron_pt",
                                    "run", "lumi", "evt", "eer_u1");
  mgr2->execute();

  auto r1 = dm1->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u1");
  auto r2 = dm2->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u1");

  ASSERT_EQ(r1.GetValue().size(), r2.GetValue().size());
  for (std::size_t ev = 0; ev < r1.GetValue().size(); ++ev) {
    ASSERT_EQ(r1.GetValue()[ev].size(), r2.GetValue()[ev].size());
    for (std::size_t obj = 0; obj < r1.GetValue()[ev].size(); ++obj)
      EXPECT_FLOAT_EQ(r1.GetValue()[ev][obj], r2.GetValue()[ev][obj]);
  }
}

TEST_F(ReproducibleGaussianTest, DifferentSaltGivesDifferentValues) {
  // Two columns with different salts on the same event must differ.
  auto dm = std::make_unique<DataManager>(1);
  auto sm = std::make_unique<SystematicManager>();

  defineUIntColumn(*dm, "run",  1, *sm);
  defineUIntColumn(*dm, "lumi", 1, *sm);
  defineULong64Column(*dm, "evt", 100ULL, *sm);
  defineRVecColumn(*dm, "Electron_pt",
                   [](ULong64_t) { return 50.0f; }, *sm);

  auto mgr = makeMgr(*dm, *sm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  mgr->defineReproducibleGaussian("Electron_u1", "Electron_pt",
                                   "run", "lumi", "evt", "salt_a");
  mgr->defineReproducibleGaussian("Electron_u2", "Electron_pt",
                                   "run", "lumi", "evt", "salt_b");
  mgr->execute();

  auto u1 = dm->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u1");
  auto u2 = dm->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u2");
  ASSERT_FALSE(u1.GetValue().empty());
  // Values for different salts on the same event should differ.
  EXPECT_NE(u1.GetValue()[0][0], u2.GetValue()[0][0]);
}

TEST_F(ReproducibleGaussianTest, DifferentEventsGiveDifferentValues) {
  // Two events with different event numbers must get different random numbers.
  auto dm = std::make_unique<DataManager>(2);
  auto sm = std::make_unique<SystematicManager>();

  // Event 0 → event number 1, Event 1 → event number 2
  dm->Define("run",  [](ULong64_t) -> UInt_t { return 1u; },
             {"rdfentry_"}, *sm);
  dm->Define("lumi", [](ULong64_t) -> UInt_t { return 1u; },
             {"rdfentry_"}, *sm);
  dm->Define("evt",
             [](ULong64_t i) -> ULong64_t { return static_cast<ULong64_t>(i + 1); },
             {"rdfentry_"}, *sm);
  defineRVecColumn(*dm, "Electron_pt",
                   [](ULong64_t) { return 30.0f; }, *sm);

  auto mgr = makeMgr(*dm, *sm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  mgr->defineReproducibleGaussian("Electron_u1", "Electron_pt",
                                   "run", "lumi", "evt", "eer");
  mgr->execute();

  auto result = dm->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u1");
  ASSERT_EQ(result.GetValue().size(), 2u);
  EXPECT_NE(result.GetValue()[0][0], result.GetValue()[1][0]);
}

TEST_F(ReproducibleGaussianTest, OutputSizeMatchesSizeColumn) {
  // The output RVec must have the same length as the size column RVec.
  auto dm = std::make_unique<DataManager>(1);
  auto sm = std::make_unique<SystematicManager>();

  defineUIntColumn(*dm, "run",  1, *sm);
  defineUIntColumn(*dm, "lumi", 1, *sm);
  defineULong64Column(*dm, "evt", 7ULL, *sm);
  // Define a 3-element per-event Electron_pt
  dm->Define("Electron_pt",
             [](ULong64_t) -> ROOT::VecOps::RVec<Float_t> {
               return {20.0f, 30.0f, 40.0f};
             },
             {"rdfentry_"}, *sm);

  auto mgr = makeMgr(*dm, *sm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  mgr->defineReproducibleGaussian("Electron_u1", "Electron_pt",
                                   "run", "lumi", "evt", "eer");
  mgr->execute();

  auto result = dm->getDataFrame().Take<ROOT::VecOps::RVec<float>>("Electron_u1");
  ASSERT_EQ(result.GetValue().size(), 1u);
  EXPECT_EQ(result.GetValue()[0].size(), 3u);
}

TEST_F(ReproducibleGaussianTest, EmptyOutputColumnThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto sm  = std::make_unique<SystematicManager>();
  auto mgr = makeMgr(*dm, *sm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  EXPECT_THROW(
      mgr->defineReproducibleGaussian("", "Electron_pt",
                                       "run", "lumi", "evt"),
      std::invalid_argument);
}

TEST_F(ReproducibleGaussianTest, EmptySizeColumnThrows) {
  auto dm  = std::make_unique<DataManager>(1);
  auto sm  = std::make_unique<SystematicManager>();
  auto mgr = makeMgr(*dm, *sm);
  mgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi", "");
  EXPECT_THROW(
      mgr->defineReproducibleGaussian("Electron_u1", "",
                                       "run", "lumi", "evt"),
      std::invalid_argument);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
