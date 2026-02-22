/**
 * @file testKinematicFitManager.cc
 * @brief Unit tests for the KinematicFitManager plugin and the KinematicFit
 *        algorithm.
 *
 * Tests cover:
 *  - Construction and configuration loading
 *  - Fit config retrieval and error handling
 *  - The KinematicFit algorithm (convergence, chi2, mass constraints)
 *  - Integration with RDataFrame via applyFit / applyAllFits
 */

#include <KinematicFit.h>
#include <KinematicFitManager.h>
#include <ConfigurationManager.h>
#include <DataManager.h>
#include <DefaultLogger.h>
#include <ManagerFactory.h>
#include <NullOutputSink.h>
#include <SystematicManager.h>
#include <api/ManagerContext.h>
#include <test_util.h>

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RVec.hxx>
#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif
#include <cmath>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

// ─── helpers ──────────────────────────────────────────────────────────────────

/// Compute invariant mass of two 4-momenta given (pT, eta, phi, m).
static double invMass(double pt1, double eta1, double phi1, double m1,
                      double pt2, double eta2, double phi2, double m2) {
  const double E1  = std::sqrt(pt1 * pt1 * std::cosh(eta1) * std::cosh(eta1) + m1 * m1);
  const double E2  = std::sqrt(pt2 * pt2 * std::cosh(eta2) * std::cosh(eta2) + m2 * m2);
  const double px1 = pt1 * std::cos(phi1), py1 = pt1 * std::sin(phi1),
               pz1 = pt1 * std::sinh(eta1);
  const double px2 = pt2 * std::cos(phi2), py2 = pt2 * std::sin(phi2),
               pz2 = pt2 * std::sinh(eta2);
  const double m2val = (E1 + E2) * (E1 + E2) -
                       (px1 + px2) * (px1 + px2) -
                       (py1 + py2) * (py1 + py2) -
                       (pz1 + pz2) * (pz1 + pz2);
  return (m2val > 0) ? std::sqrt(m2val) : 0.0;
}

// ─── KinematicFit algorithm tests ────────────────────────────────────────────

class KinematicFitTest : public ::testing::Test {};

TEST_F(KinematicFitTest, EmptyFitReturnsZeroChi2) {
  KinematicFit fitter;
  const auto result = fitter.fit();
  EXPECT_EQ(result.chi2, 0.0);
  EXPECT_TRUE(result.converged);
}

TEST_F(KinematicFitTest, NoConstraintsReturnsZeroChi2) {
  KinematicFit fitter;
  fitter.addParticle({45.0, 0.5, 1.0, 0.106, 0.02, 0.001, 0.001});
  fitter.addParticle({40.0, -0.5, -2.0, 0.106, 0.02, 0.001, 0.001});
  const auto result = fitter.fit();
  EXPECT_EQ(result.chi2, 0.0);
  EXPECT_TRUE(result.converged);
  EXPECT_EQ(result.fittedParticles.size(), 2u);
}

TEST_F(KinematicFitTest, AlreadySatisfiedConstraintHasSmallChi2) {
  // Build two muons whose invariant mass already equals the Z mass (~91.2 GeV)
  const double mZ = 91.2;
  KinematicFit fitter;
  // Symmetric Z -> mu+mu- in the lab
  fitter.addParticle({mZ / 2.0, 0.0, 0.0, 0.106, 0.02, 0.001, 0.001});
  fitter.addParticle({mZ / 2.0, 0.0, M_PI, 0.106, 0.02, 0.001, 0.001});
  fitter.addMassConstraint(0, 1, mZ);
  const auto result = fitter.fit();
  EXPECT_TRUE(result.converged);
  EXPECT_LT(result.chi2, 1e-4);  // essentially zero
}

TEST_F(KinematicFitTest, SingleConstraintFitConvergesAndSatisfiesConstraint) {
  const double mZ = 91.2;
  // Two muons with invariant mass ~100 GeV – fit should pull them towards Z
  KinematicFit fitter;
  fitter.addParticle({52.0, 0.3, 0.5, 0.106, 0.05, 0.01, 0.01});
  fitter.addParticle({52.0, -0.3, 0.5 + M_PI, 0.106, 0.05, 0.01, 0.01});
  fitter.addMassConstraint(0, 1, mZ);
  const auto result = fitter.fit();
  EXPECT_TRUE(result.converged);
  EXPECT_GE(result.chi2, 0.0);

  // Check that the fitted mass is close to the constraint
  const auto &fp = result.fittedParticles;
  const double fittedMass = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                                    fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fittedMass, mZ, 1.0); // within 1 GeV
}

TEST_F(KinematicFitTest, TwoConstraintFitDileptonDijet) {
  const double mZ = 91.2;
  const double mH = 125.0;

  KinematicFit fitter;
  // Dilepton (muons)
  fitter.addParticle({48.0,  0.4,  1.0, 0.106, 0.03, 0.005, 0.005});
  fitter.addParticle({48.0, -0.4,  1.0 + M_PI, 0.106, 0.03, 0.005, 0.005});
  // Dijets
  fitter.addParticle({65.0,  1.0,  0.0, 4.18, 0.10, 0.05, 0.05});
  fitter.addParticle({65.0, -1.0,  M_PI, 4.18, 0.10, 0.05, 0.05});

  fitter.addMassConstraint(0, 1, mZ);
  fitter.addMassConstraint(2, 3, mH);

  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  // Fitted masses should be close to target values (within a few GeV)
  const auto &fp = result.fittedParticles;
  const double fittedDilepton = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                                        fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  const double fittedDijet    = invMass(fp[2].pt, fp[2].eta, fp[2].phi, fp[2].mass,
                                        fp[3].pt, fp[3].eta, fp[3].phi, fp[3].mass);
  EXPECT_NEAR(fittedDilepton, mZ, 5.0);
  EXPECT_NEAR(fittedDijet,    mH, 5.0);
}

TEST_F(KinematicFitTest, FittedMomenta_SatisfyMassConstraint_Tightly) {
  // Use particles whose initial mass is exactly 80 GeV, constrain to 91.2
  const double mZ = 91.2;
  KinematicFit fitter;
  // pT chosen so that m(p1+p2) ~ 80 GeV
  const double pt = 40.0;
  fitter.addParticle({pt, 0.0,  0.0, 0.106, 0.02, 0.001, 0.001});
  fitter.addParticle({pt, 0.0,  M_PI, 0.106, 0.02, 0.001, 0.001});
  fitter.addMassConstraint(0, 1, mZ);
  const auto result = fitter.fit(100, 1e-8);
  const auto &fp = result.fittedParticles;
  const double fm = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                             fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fm, mZ, 0.5);
}

// ─── KinematicFitManager configuration tests ─────────────────────────────────

class KinematicFitManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    ChangeToTestSourceDir();
    configManager = ManagerFactory::createConfigurationManager(
        "cfg/test_data_config.txt");
    fitManager = std::make_unique<KinematicFitManager>(*configManager);

    systematicManager = std::make_unique<SystematicManager>();
    dataManager       = std::make_unique<DataManager>(4);
    logger            = std::make_unique<DefaultLogger>();
    skimSink          = std::make_unique<NullOutputSink>();
    metaSink          = std::make_unique<NullOutputSink>();

    ManagerContext ctx{*configManager, *dataManager, *systematicManager,
                       *logger, *skimSink, *metaSink};
    fitManager->setContext(ctx);
  }

  /// Helper – define all particle columns needed by the test fits.
  void defineParticleColumns() {
    // lepton 1
    dataManager->Define("lep1_pt",   [](ULong64_t) -> float { return 46.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep1_eta",  [](ULong64_t) -> float { return  0.5f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep1_phi",  [](ULong64_t) -> float { return  1.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep1_mass", [](ULong64_t) -> float { return  0.106f; }, {"rdfentry_"}, *systematicManager);
    // lepton 2
    dataManager->Define("lep2_pt",   [](ULong64_t) -> float { return 46.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep2_eta",  [](ULong64_t) -> float { return -0.5f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep2_phi",  [](ULong64_t) -> float { return  1.0f + static_cast<float>(M_PI); }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("lep2_mass", [](ULong64_t) -> float { return  0.106f; }, {"rdfentry_"}, *systematicManager);
    // jet 1
    dataManager->Define("jet1_pt",   [](ULong64_t) -> float { return 65.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet1_eta",  [](ULong64_t) -> float { return  1.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet1_phi",  [](ULong64_t) -> float { return  0.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet1_mass", [](ULong64_t) -> float { return  4.18f; }, {"rdfentry_"}, *systematicManager);
    // jet 2
    dataManager->Define("jet2_pt",   [](ULong64_t) -> float { return 65.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet2_eta",  [](ULong64_t) -> float { return -1.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet2_phi",  [](ULong64_t) -> float { return static_cast<float>(M_PI); }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("jet2_mass", [](ULong64_t) -> float { return  4.18f; }, {"rdfentry_"}, *systematicManager);
  }

  std::unique_ptr<IConfigurationProvider> configManager;
  std::unique_ptr<KinematicFitManager>    fitManager;
  std::unique_ptr<SystematicManager>      systematicManager;
  std::unique_ptr<DataManager>            dataManager;
  std::unique_ptr<DefaultLogger>          logger;
  std::unique_ptr<NullOutputSink>         skimSink;
  std::unique_ptr<NullOutputSink>         metaSink;
};

TEST_F(KinematicFitManagerTest, ConstructorCreatesValidManager) {
  EXPECT_NO_THROW({
    auto cfg = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
    auto mgr = std::make_unique<KinematicFitManager>(*cfg);
  });
}

TEST_F(KinematicFitManagerTest, GetAllFitNames_ReturnsConfiguredFits) {
  const auto names = fitManager->getAllFitNames();
  EXPECT_EQ(names.size(), 2u);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "zhFit") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "wwFit") != names.end());
}

TEST_F(KinematicFitManagerTest, GetFitConfig_Valid) {
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("zhFit");
    EXPECT_NEAR(cfg.dileptonMassConstraint, 91.2, 1e-6);
    EXPECT_NEAR(cfg.dijetMassConstraint,   125.0, 1e-6);
    EXPECT_EQ(cfg.maxIterations, 50);
    EXPECT_EQ(cfg.lepton1Pt,   "lep1_pt");
    EXPECT_EQ(cfg.jet1Pt,      "jet1_pt");
  });
}

TEST_F(KinematicFitManagerTest, GetFitConfig_Invalid_Throws) {
  EXPECT_THROW(fitManager->getFitConfig("nonexistent"), std::runtime_error);
}

TEST_F(KinematicFitManagerTest, Type_ReturnsCorrectString) {
  EXPECT_EQ(fitManager->type(), "KinematicFitManager");
}

TEST_F(KinematicFitManagerTest, ApplyFit_WithoutContext_Throws) {
  // Manager with no context set
  auto cfg = ManagerFactory::createConfigurationManager("cfg/test_data_config.txt");
  auto mgr = std::make_unique<KinematicFitManager>(*cfg);
  EXPECT_THROW(mgr->applyFit("zhFit"), std::runtime_error);
}

TEST_F(KinematicFitManagerTest, ApplyFit_InvalidName_Throws) {
  EXPECT_THROW(fitManager->applyFit("nonexistent"), std::runtime_error);
}

TEST_F(KinematicFitManagerTest, ApplyFit_DefinesOutputColumns) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("zhFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };

  EXPECT_TRUE(hasCol("zhFit_chi2"));
  EXPECT_TRUE(hasCol("zhFit_converged"));
  EXPECT_TRUE(hasCol("zhFit_l1Pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_l1Eta_fitted"));
  EXPECT_TRUE(hasCol("zhFit_l1Phi_fitted"));
  EXPECT_TRUE(hasCol("zhFit_l2Pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_j1Pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_j2Pt_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_Chi2IsNonNegative) {
  defineParticleColumns();
  fitManager->applyFit("zhFit");

  auto df     = dataManager->getDataFrame();
  auto chi2s  = df.Take<Float_t>("zhFit_chi2");
  for (const float c : *chi2s) {
    EXPECT_GE(c, 0.0f);
  }
}

TEST_F(KinematicFitManagerTest, ApplyFit_FittedPtIsPositive) {
  defineParticleColumns();
  fitManager->applyFit("zhFit");

  auto df = dataManager->getDataFrame();
  auto l1pts = df.Take<Float_t>("zhFit_l1Pt_fitted");
  auto l2pts = df.Take<Float_t>("zhFit_l2Pt_fitted");
  auto j1pts = df.Take<Float_t>("zhFit_j1Pt_fitted");
  auto j2pts = df.Take<Float_t>("zhFit_j2Pt_fitted");
  for (std::size_t i = 0; i < l1pts->size(); ++i) {
    EXPECT_GT((*l1pts)[i], 0.0f);
    EXPECT_GT((*l2pts)[i], 0.0f);
    EXPECT_GT((*j1pts)[i], 0.0f);
    EXPECT_GT((*j2pts)[i], 0.0f);
  }
}

TEST_F(KinematicFitManagerTest, ApplyAllFits_DefinesColumnsForAllFits) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyAllFits());

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };
  EXPECT_TRUE(hasCol("zhFit_chi2"));
  EXPECT_TRUE(hasCol("wwFit_chi2"));
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
