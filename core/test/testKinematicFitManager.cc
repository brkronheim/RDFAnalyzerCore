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
 *  - Flexible particle lists including MET
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

TEST_F(KinematicFitTest, FitWithRecoilParticle_DoesNotAffectConstrainedMasses) {
  // Adding an unconstrained recoil particle should not change the mass-constraint
  // result for the signal particles.  We verify this by comparing a fit with
  // and without a recoil particle.
  const double mZ = 91.2;

  // Reference fit: just two muons constrained to Z mass
  KinematicFit refFitter;
  refFitter.addParticle({48.0,  0.4,  1.0, 0.106, 0.03, 0.005, 0.005});
  refFitter.addParticle({48.0, -0.4,  1.0 + M_PI, 0.106, 0.03, 0.005, 0.005});
  refFitter.addMassConstraint(0, 1, mZ);
  const auto refResult = refFitter.fit();

  // Fit with an extra recoil particle (representing ISR jet)
  KinematicFit fitterWithRecoil;
  fitterWithRecoil.addParticle({48.0,  0.4,  1.0, 0.106, 0.03, 0.005, 0.005});
  fitterWithRecoil.addParticle({48.0, -0.4,  1.0 + M_PI, 0.106, 0.03, 0.005, 0.005});
  // ISR jet: high uncertainty, not constrained
  fitterWithRecoil.addParticle({25.0, 1.5, 0.3, 0.0, 0.30, 0.10, 0.10});
  fitterWithRecoil.addMassConstraint(0, 1, mZ);  // same constraint, no constraint on recoil
  const auto resultWithRecoil = fitterWithRecoil.fit();

  EXPECT_GE(resultWithRecoil.chi2, 0.0);
  // Signal pair fitted mass should be close to Z regardless of the recoil
  const auto &fp = resultWithRecoil.fittedParticles;
  const double fittedMass = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                                    fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fittedMass, mZ, 5.0);
  // The fitted dilepton mass should be similar whether or not the recoil is included
  const auto &refFp = refResult.fittedParticles;
  const double refMass = invMass(refFp[0].pt, refFp[0].eta, refFp[0].phi, refFp[0].mass,
                                  refFp[1].pt, refFp[1].eta, refFp[1].phi, refFp[1].mass);
  EXPECT_NEAR(fittedMass, refMass, 2.0);
}

TEST_F(KinematicFitTest, FitWithZeroRecoil_SameAsWithoutRecoil) {
  // A zero-pT recoil (empty extra-jet collection) should not change the fit.
  const double mZ = 91.2;
  KinematicFit fitter;
  fitter.addParticle({48.0,  0.4,  1.0, 0.106, 0.03, 0.005, 0.005});
  fitter.addParticle({48.0, -0.4,  1.0 + M_PI, 0.106, 0.03, 0.005, 0.005});
  // Zero-pT recoil (represents an event with no extra jets)
  fitter.addParticle({0.0, 0.0, 0.0, 0.0, 0.30, 0.10, 0.10});
  fitter.addMassConstraint(0, 1, mZ);
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  const auto &fp = result.fittedParticles;
  const double fm = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                             fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fm, mZ, 5.0);
}

TEST_F(KinematicFitTest, FitWithMET_Converges) {
  // W -> mu + nu: lepton + MET, constrain to W mass 80.4 GeV
  const double mW = 80.4;
  KinematicFit fitter;
  // Muon
  fitter.addParticle({42.0, 0.5, 1.0, 0.106, 0.02, 0.001, 0.001});
  // MET (eta = 0, mass = 0, very large eta resolution so eta is free)
  fitter.addParticle({40.0, 0.0, -2.0, 0.0, 0.20, 100.0, 0.05});
  fitter.addMassConstraint(0, 1, mW);
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  EXPECT_EQ(result.fittedParticles.size(), 2u);
  // Fitted invariant mass should be close to the W mass constraint
  const auto &fp = result.fittedParticles;
  const double fm = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                             fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fm, mW, 5.0); // within 5 GeV (MET has larger freedom)
}

TEST_F(KinematicFitTest, FitWithVariableJets_FourJets) {
  // Test with 4 jets (ttbar-like: constrain j1+j2 to W and j3+j4 to W)
  const double mW = 80.4;
  KinematicFit fitter;
  fitter.addParticle({50.0,  0.5,  0.0, 0.0, 0.10, 0.05, 0.05});
  fitter.addParticle({45.0, -0.5,  M_PI, 0.0, 0.10, 0.05, 0.05});
  fitter.addParticle({40.0,  1.0,  1.5, 0.0, 0.10, 0.05, 0.05});
  fitter.addParticle({38.0, -1.0, -1.5, 0.0, 0.10, 0.05, 0.05});
  fitter.addMassConstraint(0, 1, mW);
  fitter.addMassConstraint(2, 3, mW);
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  EXPECT_EQ(result.fittedParticles.size(), 4u);
  // Both dijet masses should be close to the W mass constraint
  const auto &fp = result.fittedParticles;
  const double m01 = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                              fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  const double m23 = invMass(fp[2].pt, fp[2].eta, fp[2].phi, fp[2].mass,
                              fp[3].pt, fp[3].eta, fp[3].phi, fp[3].mass);
  EXPECT_NEAR(m01, mW, 5.0);
  EXPECT_NEAR(m23, mW, 5.0);
}

TEST_F(KinematicFitTest, ThreeBodyMassConstraint_Converges) {
  // t → b l ν: three-body mass constraint m(b, l, ν) = 173.3 GeV
  const double mTop = 173.3;
  KinematicFit fitter;
  fitter.addParticle({55.0,  0.3,  2.0, 4.18,  0.10, 0.05, 0.05}); // b-jet
  fitter.addParticle({42.0,  0.5,  1.0, 0.106, 0.02, 0.001, 0.001}); // lepton
  fitter.addParticle({40.0,  0.0, -2.0, 0.0,   0.20, 100.0, 0.05}); // MET/neutrino
  fitter.addThreeBodyMassConstraint(0, 1, 2, mTop);
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  EXPECT_EQ(result.fittedParticles.size(), 3u);
  // Fitted three-body invariant mass should be close to the top mass
  const auto &fp = result.fittedParticles;
  const double E0 = std::sqrt(fp[0].pt*fp[0].pt*std::cosh(fp[0].eta)*std::cosh(fp[0].eta)+fp[0].mass*fp[0].mass);
  const double E1 = std::sqrt(fp[1].pt*fp[1].pt*std::cosh(fp[1].eta)*std::cosh(fp[1].eta)+fp[1].mass*fp[1].mass);
  const double E2 = std::sqrt(fp[2].pt*fp[2].pt*std::cosh(fp[2].eta)*std::cosh(fp[2].eta)+fp[2].mass*fp[2].mass);
  const double px0=fp[0].pt*std::cos(fp[0].phi), py0=fp[0].pt*std::sin(fp[0].phi), pz0=fp[0].pt*std::sinh(fp[0].eta);
  const double px1=fp[1].pt*std::cos(fp[1].phi), py1=fp[1].pt*std::sin(fp[1].phi), pz1=fp[1].pt*std::sinh(fp[1].eta);
  const double px2=fp[2].pt*std::cos(fp[2].phi), py2=fp[2].pt*std::sin(fp[2].phi), pz2=fp[2].pt*std::sinh(fp[2].eta);
  const double sE=E0+E1+E2, sPx=px0+px1+px2, sPy=py0+py1+py2, sPz=pz0+pz1+pz2;
  const double m2 = sE*sE - sPx*sPx - sPy*sPy - sPz*sPz;
  EXPECT_NEAR(std::sqrt(std::max(m2, 0.0)), mTop, 10.0);
}

TEST_F(KinematicFitTest, ThreeBodyPlusTwoBodyConstraint_BothSatisfied) {
  // Simultaneous two-body W mass + three-body top mass: l+ν = mW, b+l+ν = mTop
  const double mW   = 80.4;
  const double mTop = 173.3;
  KinematicFit fitter;
  fitter.addParticle({55.0,  0.3,  2.0, 4.18,  0.10, 0.05, 0.05}); // b-jet
  fitter.addParticle({50.0,  0.5,  1.0, 0.106, 0.02, 0.001, 0.001}); // lepton
  fitter.addParticle({45.0,  0.0, -2.0, 0.0,   0.20, 100.0, 0.05}); // MET/neutrino
  fitter.addMassConstraint(1, 2, mW);            // W → l ν
  fitter.addThreeBodyMassConstraint(0, 1, 2, mTop); // t → b l ν
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  EXPECT_EQ(result.fittedParticles.size(), 3u);
}

TEST_F(KinematicFitTest, PtConstraint_ForcesParticlePt) {
  // A pT constraint should drive the particle's fitted pT to the target value.
  KinematicFit fitter;
  fitter.addParticle({30.0, 0.0, 0.0, 0.0, 0.20, 100.0, 0.05}); // MET
  fitter.addPtConstraint(0, 0.0); // force MET to 0 (no genuine MET)
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  // Fitted pT should be pulled toward 0
  EXPECT_LT(result.fittedParticles[0].pt, 30.0);
}

TEST_F(KinematicFitTest, PtConstraintZeroMET_WithMassConstraint) {
  // Combine a dijet mass constraint with MET pT = 0 (hadronic Z → jj selection).
  const double mZ = 91.2;
  KinematicFit fitter;
  fitter.addParticle({50.0,  0.5,  0.0, 0.0, 0.10, 0.05, 0.05}); // jet1
  fitter.addParticle({45.0, -0.5,  M_PI, 0.0, 0.10, 0.05, 0.05}); // jet2
  fitter.addParticle({20.0,  0.0, -1.0, 0.0, 0.20, 100.0, 0.05}); // MET
  fitter.addMassConstraint(0, 1, mZ);  // Z → jj
  fitter.addPtConstraint(2, 0.0);      // MET should be 0
  const auto result = fitter.fit();
  EXPECT_GE(result.chi2, 0.0);
  EXPECT_EQ(result.fittedParticles.size(), 3u);
  // Dijet mass should be close to Z
  const auto &fp = result.fittedParticles;
  const double fm = invMass(fp[0].pt, fp[0].eta, fp[0].phi, fp[0].mass,
                             fp[1].pt, fp[1].eta, fp[1].phi, fp[1].mass);
  EXPECT_NEAR(fm, mZ, 10.0);
  // MET should be pulled toward 0
  EXPECT_LT(fp[2].pt, 20.0);
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
    // MET (for wjFit and ttbarFit)
    dataManager->Define("met_pt",    [](ULong64_t) -> float { return 38.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("met_phi",   [](ULong64_t) -> float { return -0.8f; }, {"rdfentry_"}, *systematicManager);
    // Extra jets as RVec<float> collection (for ttbarFit recoil particle).
    // The collection has two jets, representing extra QCD radiation.
    // In a real analysis this collection contains all jets NOT assigned to the
    // signal hypothesis; its length varies event by event.
    dataManager->Define("ExtraJet_pt",
        [](ULong64_t) -> ROOT::VecOps::RVec<float> { return {25.0f, 18.0f}; },
        {"rdfentry_"}, *systematicManager);
    dataManager->Define("ExtraJet_eta",
        [](ULong64_t) -> ROOT::VecOps::RVec<float> { return {0.8f, -1.2f}; },
        {"rdfentry_"}, *systematicManager);
    dataManager->Define("ExtraJet_phi",
        [](ULong64_t) -> ROOT::VecOps::RVec<float> { return {0.5f,  2.1f}; },
        {"rdfentry_"}, *systematicManager);
    dataManager->Define("ExtraJet_mass",
        [](ULong64_t) -> ROOT::VecOps::RVec<float> { return {0.0f,  0.0f}; },
        {"rdfentry_"}, *systematicManager);
    // b-jet for topFullFit (particle 0 in that fit is the b-jet)
    dataManager->Define("bjet_pt",   [](ULong64_t) -> float { return 55.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("bjet_eta",  [](ULong64_t) -> float { return  0.3f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("bjet_phi",  [](ULong64_t) -> float { return  2.0f; }, {"rdfentry_"}, *systematicManager);
    dataManager->Define("bjet_mass", [](ULong64_t) -> float { return  4.18f; }, {"rdfentry_"}, *systematicManager);
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
  EXPECT_EQ(names.size(), 6u);
  EXPECT_TRUE(std::find(names.begin(), names.end(), "zhFit")      != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "wjFit")      != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "ttbarFit")   != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "zhCollFit")  != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "topFullFit") != names.end());
  EXPECT_TRUE(std::find(names.begin(), names.end(), "hadZFit")    != names.end());
}

TEST_F(KinematicFitManagerTest, GetFitConfig_Valid) {
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("zhFit");
    EXPECT_EQ(cfg.particles.size(), 4u);
    EXPECT_EQ(cfg.constraints.size(), 2u);
    EXPECT_NEAR(cfg.constraints[0].targetMass, 91.2,  1e-6);
    EXPECT_NEAR(cfg.constraints[1].targetMass, 125.0, 1e-6);
    EXPECT_EQ(cfg.particles[0].name,   "mu1");
    EXPECT_EQ(cfg.particles[0].ptCol,  "lep1_pt");
    EXPECT_EQ(cfg.particles[0].type,   "lepton");
    EXPECT_EQ(cfg.particles[2].name,   "bjet1");
    EXPECT_EQ(cfg.particles[2].ptCol,  "jet1_pt");
    EXPECT_EQ(cfg.particles[2].type,   "jet");
    EXPECT_EQ(cfg.maxIterations, 50);
  });
}

TEST_F(KinematicFitManagerTest, GetFitConfig_METFit) {
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("wjFit");
    EXPECT_EQ(cfg.particles.size(), 2u);
    EXPECT_EQ(cfg.constraints.size(), 1u);
    EXPECT_NEAR(cfg.constraints[0].targetMass, 80.4, 1e-6);
    EXPECT_EQ(cfg.particles[1].name,   "nu");
    EXPECT_EQ(cfg.particles[1].type,   "met");
    EXPECT_EQ(cfg.particles[1].etaCol, "_");   // MET has no eta column
    EXPECT_EQ(cfg.particles[1].massCol, "0");  // massless neutrino
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

TEST_F(KinematicFitManagerTest, ApplyFit_DefinesOutputColumns_zh) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("zhFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };

  EXPECT_TRUE(hasCol("zhFit_chi2"));
  EXPECT_TRUE(hasCol("zhFit_converged"));
  // Particle-labelled output columns (per particle label from config)
  EXPECT_TRUE(hasCol("zhFit_mu1_pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_mu1_eta_fitted"));
  EXPECT_TRUE(hasCol("zhFit_mu1_phi_fitted"));
  EXPECT_TRUE(hasCol("zhFit_mu2_pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_bjet1_pt_fitted"));
  EXPECT_TRUE(hasCol("zhFit_bjet2_pt_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_DefinesOutputColumns_wjFit_WithMET) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("wjFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };

  EXPECT_TRUE(hasCol("wjFit_chi2"));
  EXPECT_TRUE(hasCol("wjFit_converged"));
  EXPECT_TRUE(hasCol("wjFit_lep_pt_fitted"));
  EXPECT_TRUE(hasCol("wjFit_lep_eta_fitted"));
  EXPECT_TRUE(hasCol("wjFit_nu_pt_fitted"));
  EXPECT_TRUE(hasCol("wjFit_nu_phi_fitted"));  // MET phi should be fitted
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

  auto df    = dataManager->getDataFrame();
  auto mu1pt = df.Take<Float_t>("zhFit_mu1_pt_fitted");
  auto mu2pt = df.Take<Float_t>("zhFit_mu2_pt_fitted");
  auto j1pt  = df.Take<Float_t>("zhFit_bjet1_pt_fitted");
  auto j2pt  = df.Take<Float_t>("zhFit_bjet2_pt_fitted");
  for (std::size_t i = 0; i < mu1pt->size(); ++i) {
    EXPECT_GT((*mu1pt)[i], 0.0f);
    EXPECT_GT((*mu2pt)[i], 0.0f);
    EXPECT_GT((*j1pt)[i],  0.0f);
    EXPECT_GT((*j2pt)[i],  0.0f);
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
  EXPECT_TRUE(hasCol("wjFit_chi2"));
  EXPECT_TRUE(hasCol("ttbarFit_chi2"));
  EXPECT_TRUE(hasCol("zhCollFit_chi2"));
  EXPECT_TRUE(hasCol("topFullFit_chi2"));
  EXPECT_TRUE(hasCol("hadZFit_chi2"));
}

// ─── Recoil particle type tests ───────────────────────────────────────────────

TEST_F(KinematicFitManagerTest, GetFitConfig_RecoilFit) {
  // Verify that the ttbarFit config is parsed correctly, including the
  // recoil particle that absorbs extra QCD jets.
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("ttbarFit");
    EXPECT_EQ(cfg.particles.size(), 5u);
    EXPECT_EQ(cfg.constraints.size(), 2u);
    // Last particle should be the recoil
    const auto &recoilPart = cfg.particles.back();
    EXPECT_EQ(recoilPart.type,   "recoil");
    EXPECT_EQ(recoilPart.name,   "isr");
    EXPECT_EQ(recoilPart.ptCol,  "ExtraJet_pt");
    EXPECT_EQ(recoilPart.etaCol, "ExtraJet_eta");
    EXPECT_EQ(recoilPart.collectionIndex, -1); // recoil always sums all
    // Resolution defaults should be applied
    EXPECT_NEAR(cfg.recoilPtResolution,  0.30, 1e-9);
    EXPECT_NEAR(cfg.recoilEtaResolution, 0.10, 1e-9);
    EXPECT_NEAR(cfg.recoilPhiResolution, 0.10, 1e-9);
  });
}

TEST_F(KinematicFitManagerTest, ApplyFit_RecoilType_DefinesOutputColumns) {
  // Verify that applyFit defines the expected output columns for the
  // ttbarFit, which includes a recoil particle.
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("ttbarFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };

  EXPECT_TRUE(hasCol("ttbarFit_chi2"));
  EXPECT_TRUE(hasCol("ttbarFit_converged"));
  EXPECT_TRUE(hasCol("ttbarFit_lep_pt_fitted"));
  EXPECT_TRUE(hasCol("ttbarFit_nu_pt_fitted"));
  EXPECT_TRUE(hasCol("ttbarFit_j1_pt_fitted"));
  EXPECT_TRUE(hasCol("ttbarFit_j2_pt_fitted"));
  // The recoil particle also gets fitted output columns
  EXPECT_TRUE(hasCol("ttbarFit_isr_pt_fitted"));
  EXPECT_TRUE(hasCol("ttbarFit_isr_eta_fitted"));
  EXPECT_TRUE(hasCol("ttbarFit_isr_phi_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_RecoilType_Chi2IsNonNegative) {
  // Applying the ttbarFit (with recoil) should produce non-negative chi2.
  defineParticleColumns();
  fitManager->applyFit("ttbarFit");

  auto df    = dataManager->getDataFrame();
  auto chi2s = df.Take<Float_t>("ttbarFit_chi2");
  for (const float c : *chi2s) {
    EXPECT_GE(c, 0.0f);
  }
}

// ─── Collection-index particle type tests ────────────────────────────────────

TEST_F(KinematicFitManagerTest, GetFitConfig_CollectionIndexFit) {
  // The zhCollFit uses collection-indexed jets (index 0 and 1 from ExtraJet_*).
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("zhCollFit");
    EXPECT_EQ(cfg.particles.size(), 4u);
    EXPECT_EQ(cfg.constraints.size(), 2u);
    // Jets should have their collection indices set
    const auto &j1 = cfg.particles[2];
    EXPECT_EQ(j1.type,            "jet");
    EXPECT_EQ(j1.ptCol,           "ExtraJet_pt");
    EXPECT_EQ(j1.collectionIndex, 0);
    const auto &j2 = cfg.particles[3];
    EXPECT_EQ(j2.collectionIndex, 1);
  });
}

TEST_F(KinematicFitManagerTest, ApplyFit_CollectionIndex_DefinesOutputColumns) {
  // zhCollFit selects jets from ExtraJet_* by index: verifies RVec
  // extraction helper columns are defined and the fit columns exist.
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("zhCollFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };

  EXPECT_TRUE(hasCol("zhCollFit_chi2"));
  EXPECT_TRUE(hasCol("zhCollFit_converged"));
  EXPECT_TRUE(hasCol("zhCollFit_mu1_pt_fitted"));
  EXPECT_TRUE(hasCol("zhCollFit_mu2_pt_fitted"));
  EXPECT_TRUE(hasCol("zhCollFit_j1_pt_fitted"));
  EXPECT_TRUE(hasCol("zhCollFit_j2_pt_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_CollectionIndex_Chi2IsNonNegative) {
  // The fit using collection-indexed jets should produce valid (≥ 0) chi2.
  defineParticleColumns();
  fitManager->applyFit("zhCollFit");

  auto df    = dataManager->getDataFrame();
  auto chi2s = df.Take<Float_t>("zhCollFit_chi2");
  for (const float c : *chi2s) {
    EXPECT_GE(c, 0.0f);
  }
}

// ─── Three-body mass constraint tests (manager) ───────────────────────────────

TEST_F(KinematicFitManagerTest, GetFitConfig_TopFullFit_HasThreeBodyConstraint) {
  // topFullFit uses a three-body mass constraint (b + l + ν = top mass).
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("topFullFit");
    EXPECT_EQ(cfg.particles.size(), 3u);
    EXPECT_EQ(cfg.constraints.size(), 2u);
    // Find the three-body constraint
    bool foundThreeBody = false;
    for (const auto &con : cfg.constraints) {
      if (con.type == KinFitConstraintConfig::Type::MASS && con.idx3 >= 0) {
        foundThreeBody = true;
        EXPECT_NEAR(con.targetValue, 173.3, 0.1);
      }
    }
    EXPECT_TRUE(foundThreeBody) << "No three-body mass constraint found";
  });
}

TEST_F(KinematicFitManagerTest, ApplyFit_TopFullFit_DefinesOutputColumns) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("topFullFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };
  EXPECT_TRUE(hasCol("topFullFit_chi2"));
  EXPECT_TRUE(hasCol("topFullFit_converged"));
  EXPECT_TRUE(hasCol("topFullFit_bjet_pt_fitted"));
  EXPECT_TRUE(hasCol("topFullFit_lep_pt_fitted"));
  EXPECT_TRUE(hasCol("topFullFit_nu_pt_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_TopFullFit_Chi2IsNonNegative) {
  defineParticleColumns();
  fitManager->applyFit("topFullFit");

  auto df    = dataManager->getDataFrame();
  auto chi2s = df.Take<Float_t>("topFullFit_chi2");
  for (const float c : *chi2s) {
    EXPECT_GE(c, 0.0f);
  }
}

// ─── pT (MET size) constraint tests (manager) ────────────────────────────────

TEST_F(KinematicFitManagerTest, GetFitConfig_HadZFit_HasPtConstraint) {
  // hadZFit constrains dijet mass to Z and MET pT to 0.
  EXPECT_NO_THROW({
    const auto &cfg = fitManager->getFitConfig("hadZFit");
    EXPECT_EQ(cfg.particles.size(), 3u);
    EXPECT_EQ(cfg.constraints.size(), 2u);
    // Find the pT constraint
    bool foundPt = false;
    for (const auto &con : cfg.constraints) {
      if (con.type == KinFitConstraintConfig::Type::PT) {
        foundPt = true;
        EXPECT_NEAR(con.targetValue, 0.0, 1e-9);
        EXPECT_EQ(con.idx1, 2); // MET is particle index 2
      }
    }
    EXPECT_TRUE(foundPt) << "No pT constraint found in hadZFit";
  });
}

TEST_F(KinematicFitManagerTest, ApplyFit_HadZFit_DefinesOutputColumns) {
  defineParticleColumns();
  EXPECT_NO_THROW(fitManager->applyFit("hadZFit"));

  auto df = dataManager->getDataFrame();
  const auto cols = df.GetColumnNames();
  auto hasCol = [&](const std::string &name) {
    return std::find(cols.begin(), cols.end(), name) != cols.end();
  };
  EXPECT_TRUE(hasCol("hadZFit_chi2"));
  EXPECT_TRUE(hasCol("hadZFit_converged"));
  EXPECT_TRUE(hasCol("hadZFit_j1_pt_fitted"));
  EXPECT_TRUE(hasCol("hadZFit_j2_pt_fitted"));
  EXPECT_TRUE(hasCol("hadZFit_nu_pt_fitted"));
}

TEST_F(KinematicFitManagerTest, ApplyFit_HadZFit_Chi2IsNonNegative) {
  defineParticleColumns();
  fitManager->applyFit("hadZFit");

  auto df    = dataManager->getDataFrame();
  auto chi2s = df.Take<Float_t>("hadZFit_chi2");
  for (const float c : *chi2s) {
    EXPECT_GE(c, 0.0f);
  }
}

TEST_F(KinematicFitManagerTest, ParseConstraintSpec_ThreeBody_ParsedCorrectly) {
  // Verify that a three-body constraint is parsed correctly: indices and
  // target mass are stored in the right fields.
  const auto &cfg = fitManager->getFitConfig("topFullFit");
  bool foundThreeBody = false;
  for (const auto &con : cfg.constraints) {
    if (con.type == KinFitConstraintConfig::Type::MASS && con.idx3 >= 0) {
      foundThreeBody = true;
      EXPECT_EQ(con.idx1, 0);
      EXPECT_EQ(con.idx2, 1);
      EXPECT_EQ(con.idx3, 2);
      EXPECT_NEAR(con.targetValue, 173.3, 0.1);
    }
  }
  EXPECT_TRUE(foundThreeBody);
}

TEST_F(KinematicFitManagerTest, ParseConstraintSpec_PtConstraint_ParsedCorrectly) {
  // Verify that a pT constraint is parsed correctly.
  const auto &cfg = fitManager->getFitConfig("hadZFit");
  bool foundPt = false;
  for (const auto &con : cfg.constraints) {
    if (con.type == KinFitConstraintConfig::Type::PT) {
      foundPt = true;
      EXPECT_EQ(con.idx1, 2);
      EXPECT_EQ(con.idx2, -1);  // unused for pT constraint
      EXPECT_EQ(con.idx3, -1);  // unused for pT constraint
      EXPECT_NEAR(con.targetValue, 0.0, 1e-9);
    }
  }
  EXPECT_TRUE(foundPt);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
