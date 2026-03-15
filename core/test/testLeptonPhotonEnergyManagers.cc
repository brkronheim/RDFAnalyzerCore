/**
 * @file testLepton_PhotonEnergyScaleManagers.cc
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

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
