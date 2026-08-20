//==============================================================================
// CmsCorrections — Generic CMS correction workflow setup
//
// Implements the generic CMS correction workflow functions.
// Extracted from VHqqCorrections.cc (VHbb STXS analysis) and generalized.
//==============================================================================

#include "CmsCorrections.h"

// Plugin headers (needed for ::create calls and programmatic API).
#include "CorrectedObjectCollectionManagers.h"
#include "CorrectionManager.h"
#include "ElectronEnergyScaleManager.h"
#include "GenWeightManager.h"
#include "GoldenJsonManager.h"
#include "JetEnergyScaleManager.h"
#include "JetVetoMapManager.h"
#include "MuonRochesterManager.h"
#include "TaggerWorkingPointManager.h"
#include "WeightManager.h"

#include "analyzer.h"

#include <algorithm>
#include <cctype>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <vector>

namespace cms {

namespace {

/// Convert string to lower case.
std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

/// Join a vector of strings with comma separators.
std::string joinCsv(const std::vector<std::string> &values) {
  std::ostringstream out;
  for (size_t i = 0; i < values.size(); ++i) {
    if (i != 0) out << ',';
    out << values[i];
  }
  return out.str();
}

/// Set a config value only if not already set (do not override user config).
void setWorkflowConfigValue(IConfigurationProvider &config,
                            const std::string &key,
                            const std::string &value) {
  const auto existing = config.get(key);
  if (existing.empty()) {
    config.set(key, value);
  }
}

/// Derive the per-source JES correction name from the total correction name.
std::string jesCorrectionNameForSource(const std::string &totalCorrectionName,
                                       const std::string &source) {
  const auto totalPos = totalCorrectionName.find("Total");
  if (totalPos == std::string::npos) {
    throw std::runtime_error(
        "CmsCorrections: cannot derive JES source correction name from '" +
        totalCorrectionName + "': no 'Total' token found.");
  }
  std::string out = totalCorrectionName;
  out.replace(totalPos, std::string("Total").size(), source);
  return out;
}

} // namespace

// ===========================================================================
// Tagger working-point configuration helpers
// ===========================================================================

/// Register b-tag working points from a BTagWorkingPoints struct.
/// Only non-zero thresholds are registered.  WP names: L, M, T, XT, XXT.
/// The tagger manager must have already been created (e.g. by
/// setupCmsFrameworkPlugins()).
///
/// @param an      The Analyzer instance.
/// @param btWp    B-tag working-point thresholds.
void configureBtagWorkingPoints(Analyzer &an, const BTagWorkingPoints &btWp) {
  auto btagMgr = an.getPlugin<TaggerWorkingPointManager>("btagManager");
  if (!btagMgr) {
    std::cerr << "WARNING: configureBtagWorkingPoints — btagManager not found.\n";
    return;
  }
  if (btWp.loose != 0.0f)   btagMgr->addWorkingPoint("L", btWp.loose);
  if (btWp.medium != 0.0f)  btagMgr->addWorkingPoint("M", btWp.medium);
  if (btWp.tight != 0.0f)   btagMgr->addWorkingPoint("T", btWp.tight);
  if (btWp.xtight != 0.0f)  btagMgr->addWorkingPoint("XT", btWp.xtight);
  if (btWp.xxtight != 0.0f) btagMgr->addWorkingPoint("XXT", btWp.xxtight);
}

/// Register c-tag working points from a CTagWorkingPoints struct.
/// Each WP is a pair {CvL, CvB}.  Only pairs with at least one non-zero
/// threshold are registered.  WP names: L, M, T.
/// The tagger manager must have already been created.
///
/// @param an      The Analyzer instance.
/// @param ctWp    C-tag working-point thresholds (CvL + CvB per WP).
void configureCtagWorkingPoints(Analyzer &an, const CTagWorkingPoints &ctWp) {
  auto ctagMgr = an.getPlugin<TaggerWorkingPointManager>("ctagManager");
  if (!ctagMgr) {
    std::cerr << "WARNING: configureCtagWorkingPoints — ctagManager not found.\n";
    return;
  }
  if (ctWp.cvlLoose != 0.0f || ctWp.cvbLoose != 0.0f)
    ctagMgr->addWorkingPoint("L", {ctWp.cvlLoose, ctWp.cvbLoose});
  if (ctWp.cvlMedium != 0.0f || ctWp.cvbMedium != 0.0f)
    ctagMgr->addWorkingPoint("M", {ctWp.cvlMedium, ctWp.cvbMedium});
  if (ctWp.cvlTight != 0.0f || ctWp.cvbTight != 0.0f)
    ctagMgr->addWorkingPoint("T", {ctWp.cvlTight, ctWp.cvbTight});
}

/// Configure the b-tag TaggerWorkingPointManager with object columns,
/// tagger branch, input collection, and working-point thresholds.
/// This is a convenience that bundles all setup steps needed before
/// defineCmsBTagScaleFactors() can be called.
///
/// @param an      The Analyzer instance.
/// @param ys      Year settings (provides btagBranch, etc.).
/// @param btWp    B-tag working-point thresholds (all zero = skip WPs).
void setupBtagTaggerManager(Analyzer &an, const CmsYearSettings &ys,
                             const BTagWorkingPoints &btWp) {
  auto btagMgr = an.getPlugin<TaggerWorkingPointManager>("btagManager");
  if (!btagMgr) return;
  btagMgr->setObjectColumns("Jet_pt_corr_nominal", "Jet_eta",
                            "Jet_phi", "Jet_mass_corr_nominal");
  btagMgr->setTaggerColumn(ys.btagBranch);
  btagMgr->setInputObjectCollection("TaggerJets");
  configureBtagWorkingPoints(an, btWp);
}

/// Configure the c-tag TaggerWorkingPointManager with object columns,
/// tagger branches (CvL + CvB), input collection, and working-point
/// thresholds.  This is a convenience that bundles all setup steps needed
/// before defineCmsCTagScaleFactors() can be called.
///
/// @param an      The Analyzer instance.
/// @param ys      Year settings (provides cvlBranch, cvbBranch, etc.).
/// @param ctWp    C-tag working-point thresholds (all zero = skip WPs).
void setupCtagTaggerManager(Analyzer &an, const CmsYearSettings &ys,
                             const CTagWorkingPoints &ctWp) {
  auto ctagMgr = an.getPlugin<TaggerWorkingPointManager>("ctagManager");
  if (!ctagMgr) return;
  ctagMgr->setObjectColumns("Jet_pt_corr_nominal_ctag", "Jet_eta",
                            "Jet_phi", "Jet_mass_corr_nominal");
  ctagMgr->setTaggerColumns({ys.cvlBranch, ys.cvbBranch});
  ctagMgr->setInputObjectCollection("TaggerJets");
  configureCtagWorkingPoints(an, ctWp);
}

// ===========================================================================
// Plugin setup — correction-related plugins only
// ===========================================================================

void setupCmsFrameworkPlugins(Analyzer &an,
                               const CmsCorrectionConfig &cfg) {
  CorrectionManager::create(an, "correctionManager");
  WeightManager::create(an, "weightManager");
  GenWeightManager::create(an, "genWeightManager");
  TaggerWorkingPointManager::create(an, "btagManager");
  TaggerWorkingPointManager::create(an, "ctagManager");
  JetEnergyScaleManager::create(an, "jetEnergyScaleManager");
  ElectronEnergyScaleManager::create(an, "electronEnergyScaleManager");
  MuonRochesterManager::create(an, "muonRochesterManager");
  CorrectedJetCollectionManager::create(an, "correctedJetCollectionManager");
  CorrectedElectronCollectionManager::create(an, "correctedElectronCollectionManager");
  CorrectedMuonCollectionManager::create(an, "correctedMuonCollectionManager");
  GoldenJsonManager::create(an, "goldenJsonManager");
  JetVetoMapManager::create(an, "jetVetoMapManager");

  // Configure tagger working-point managers if thresholds are provided.
  if (cfg.yearSettings) {
    const auto &ys = *cfg.yearSettings;
    const bool hasBtagWp = (cfg.btagWorkingPoints.loose != 0.0f ||
                            cfg.btagWorkingPoints.medium != 0.0f ||
                            cfg.btagWorkingPoints.tight != 0.0f ||
                            cfg.btagWorkingPoints.xtight != 0.0f ||
                            cfg.btagWorkingPoints.xxtight != 0.0f);
    if (hasBtagWp) {
      setupBtagTaggerManager(an, ys, cfg.btagWorkingPoints);
    }
    const bool hasCtagWp = (cfg.ctagWorkingPoints.cvlLoose != 0.0f ||
                            cfg.ctagWorkingPoints.cvlMedium != 0.0f ||
                            cfg.ctagWorkingPoints.cvlTight != 0.0f ||
                            cfg.ctagWorkingPoints.cvbLoose != 0.0f ||
                            cfg.ctagWorkingPoints.cvbMedium != 0.0f ||
                            cfg.ctagWorkingPoints.cvbTight != 0.0f);
    if (hasCtagWp) {
      setupCtagTaggerManager(an, ys, cfg.ctagWorkingPoints);
    }
  }
}

// ===========================================================================
// Correction input columns
// ===========================================================================

void defineCmsCorrectionInputs(Analyzer &an) {
  // Electron_scEta: supercluster eta from electron eta + deltaEtaSC
  an.Define("Electron_scEta",
            [](const ROOT::VecOps::RVec<float> &eta,
               const ROOT::VecOps::RVec<float> &deltaEtaSC) {
              ROOT::VecOps::RVec<float> scEta(eta.size(), 0.0f);
              for (size_t i = 0; i < eta.size(); ++i) {
                scEta[i] = eta[i] + deltaEtaSC[i];
              }
              return scEta;
            },
            {"Electron_eta", "Electron_deltaEtaSC"});

  // Jet_genMatchedPt: matched GenJet pT for JER smearing
  an.Define("Jet_genMatchedPt",
            [](const ROOT::VecOps::RVec<int> &genJetIdx,
               const ROOT::VecOps::RVec<float> &genJetPt) {
              ROOT::VecOps::RVec<float> matched(genJetIdx.size(), -1.0f);
              for (size_t i = 0; i < genJetIdx.size(); ++i) {
                const int idx = genJetIdx[i];
                if (idx >= 0 && static_cast<size_t>(idx) < genJetPt.size()) {
                  matched[i] = genJetPt[idx];
                }
              }
              return matched;
            },
            {"Jet_genJetIdx_int", "GenJet_pt"});
}

// ===========================================================================
// Correction workflow configuration
//
// Uses the modern programmatic JetEnergyScaleManager API instead of the
// legacy config-key-based slot system.  Supports three JES uncertainty
// granularities (Total, Reduced, Full) plus JER per-bin uncertainties.
// ===========================================================================

void configureCmsCorrectionWorkflow(Analyzer &an,
                                     const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings) {
    throw std::runtime_error(
        "CmsCorrections: yearSettings must be set in CmsCorrectionConfig.");
  }

  auto &config = an.getConfigurationProvider();
  const auto &ys = *cfg.yearSettings;
  const auto prefix = cfg.configKeyPrefix;

  // -- Basic enable/disable flags --
  setWorkflowConfigValue(config, prefix + "ApplyJetCorrections",
                         cfg.applyJetCorrections ? "true" : "false");
  setWorkflowConfigValue(config, prefix + "ApplyElectronCorrections",
                         cfg.applyElectronCorrections ? "true" : "false");
  setWorkflowConfigValue(config, prefix + "ApplyMuonCorrections",
                         cfg.applyMuonCorrections ? "true" : "false");

  // -- JME file and correction names --
  setWorkflowConfigValue(config, prefix + "JmeFile", ys.jmeFile);
  setWorkflowConfigValue(config, prefix + "RhoBranch", ys.rhoBranch);

  const std::string jecCompound = cfg.jesCompoundCorrectionOverride.empty()
                                      ? ys.jecCompoundCorrection
                                      : cfg.jesCompoundCorrectionOverride;
  setWorkflowConfigValue(config, prefix + "JecCompoundCorrection", jecCompound);

  // JER corrections (always needed for JER smearing, regardless of JES mode)
  setWorkflowConfigValue(config, prefix + "JerPtResolutionCorrection",
                         ys.jerPtResolutionCorrection);
  setWorkflowConfigValue(config, prefix + "JerScaleFactorCorrection",
                         ys.jerScaleFactorCorrection);
  setWorkflowConfigValue(config, prefix + "JerScaleFactorInputs",
                         joinCsv(ys.jerScaleFactorInputs));

  // -- Electron scale/smearing --
  setWorkflowConfigValue(config, prefix + "ElectronScaleSmearingFile",
                         ys.electronScaleSmearingFile);
  setWorkflowConfigValue(config, prefix + "ElectronScaleCorrection",
                         ys.electronScaleCorrection);
  setWorkflowConfigValue(config, prefix + "ElectronSmearingCorrection",
                         ys.electronSmearingCorrection);

  // -- Muon scale/resolution --
  setWorkflowConfigValue(config, prefix + "MuonScaleResolutionFile",
                         ys.muonScaleResolutionFile);

  // -- JES systematic mode (used by fallback logic) --
  const char *jesModeStr = "reduced";
  switch (cfg.jesMode) {
    case CmsCorrectionConfig::Total:    jesModeStr = "total"; break;
    case CmsCorrectionConfig::Reduced:  jesModeStr = "reduced"; break;
    case CmsCorrectionConfig::Full:     jesModeStr = "full"; break;
  }
  setWorkflowConfigValue(config, prefix + "JesSystematicsMode", jesModeStr);

  // -- Programmatic JetEnergyScaleManager setup --
  if (!cfg.applyJetCorrections) return;

  auto jesMgr = an.getPlugin<JetEnergyScaleManager>("jetEnergyScaleManager");
  if (!jesMgr) return;

  // Register JES uncertainties with the CorrectionManager
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  if (!corrMgr) return;

  // Register the JES uncertainty correction
  const std::string jesCorrectionName = prefix + "_jes_unc";
  corrMgr->registerCorrection(
      jesCorrectionName, ys.jmeFile, ys.jesTotalCorrection,
      {"Jet_pt", "Jet_eta", "Jet_phi"});

  // Select and register the systematic source set based on mode
  std::vector<std::string> jesSources;
  switch (cfg.jesMode) {
    case CmsCorrectionConfig::Total:
      jesSources = {"Total"};
      break;
    case CmsCorrectionConfig::Reduced:
      jesSources = CmsYearRegistry::defaultJesReducedSources(ys.canonicalYear);
      break;
    case CmsCorrectionConfig::Full:
      jesSources = CmsYearRegistry::defaultJesFullSources(ys.canonicalYear);
      break;
  }

  if (!jesSources.empty()) {
    jesMgr->registerSystematicSources(jesModeStr, jesSources);
    jesMgr->applySystematicSet(*corrMgr, jesCorrectionName,
                                jesModeStr, "Jet_pt_corr_jec",
                                prefix + "_Jet_pt_jes");
  }

  // -- JER per-bin uncertainties --
  // Additionally register JER total variation
  jesMgr->setJERSmearingColumns("Jet_genMatchedPt",
                                 ys.rhoBranch, "event");
  // JER uncertainties are handled within the JER smearing step itself.
  // The JetEnergyScaleManager will create JER up/down columns automatically
  // when the correction payloads contain systematic variations.
}

// ===========================================================================
// Correction fallback columns
// ===========================================================================

void defineCmsCorrectionFallbacks(Analyzer &an,
                                   const CmsCorrectionConfig &cfg) {
  // -- Jet correction fallbacks --
  if (!cfg.applyJetCorrections) {
    an.Define("Jet_pt_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"Jet_pt"})
        ->Define("Jet_mass_corr_nominal",
                 [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                 {"Jet_mass"})
        ->Define("PuppiMET_pt_corr_nominal",
                 [](float pt) { return pt; },
                 {"PuppiMET_pt"})
        ->Define("PuppiMET_phi_corr_nominal",
                 [](float phi) { return phi; },
                 {"PuppiMET_phi"});

    // JES variation fallbacks (identity for each source)
    const auto &jesSources = cfg.yearSettings
                                 ? cfg.yearSettings->jesSources
                                 : std::vector<std::string>{"Total"};
    for (const auto &source : jesSources) {
      const std::string label = (source == "Total") ? "total" : source;
      an.Define("Jet_pt_jes_" + label + "_up",
                [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                {"Jet_pt_corr_nominal"})
          ->Define("Jet_pt_jes_" + label + "_down",
                   [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                   {"Jet_pt_corr_nominal"})
          ->Define("Jet_mass_jes_" + label + "_up",
                   [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                   {"Jet_mass_corr_nominal"})
          ->Define("Jet_mass_jes_" + label + "_down",
                   [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                   {"Jet_mass_corr_nominal"});
    }

    // JER variation fallbacks
    an.Define("Jet_pt_jer_up",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"Jet_pt_corr_nominal"})
        ->Define("Jet_pt_jer_down",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Jet_pt_corr_nominal"})
        ->Define("Jet_mass_jer_up",
                 [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                 {"Jet_mass_corr_nominal"})
        ->Define("Jet_mass_jer_down",
                 [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                 {"Jet_mass_corr_nominal"});

    // CorrectedJets collection (identity)
    an.Define("CorrectedJets",
              [](const ROOT::VecOps::RVec<float> &pt,
                 const ROOT::VecOps::RVec<float> &eta,
                 const ROOT::VecOps::RVec<float> &phi,
                 const ROOT::VecOps::RVec<float> &mass) {
                ROOT::VecOps::RVec<bool> mask(pt.size(), true);
                return PhysicsObjectCollection(pt, eta, phi, mass, mask);
              },
              {"Jet_pt_corr_nominal", "Jet_eta", "Jet_phi", "Jet_mass_corr_nominal"});
  }

  // -- Electron correction fallbacks --
  if (!cfg.applyElectronCorrections) {
    an.Define("Electron_pt_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"Electron_pt"})
        ->Define("Electron_pt_scale_up",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Electron_pt"})
        ->Define("Electron_pt_scale_down",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Electron_pt"})
        ->Define("Electron_pt_smear_up",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Electron_pt"})
        ->Define("Electron_pt_smear_down",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Electron_pt"})
        ->Define("CorrectedElectrons",
                 [](const ROOT::VecOps::RVec<float> &pt,
                    const ROOT::VecOps::RVec<float> &eta,
                    const ROOT::VecOps::RVec<float> &phi,
                    const ROOT::VecOps::RVec<float> &mass) {
                   ROOT::VecOps::RVec<bool> mask(pt.size(), true);
                   return PhysicsObjectCollection(pt, eta, phi, mass, mask);
                 },
                 {"Electron_pt_corr_nominal", "Electron_eta", "Electron_phi", "Electron_mass"});
  }

  // -- Muon correction fallbacks --
  if (!cfg.applyMuonCorrections) {
    an.Define("Muon_pt_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"Muon_pt"})
        ->Define("Muon_pt_scale_up",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Muon_pt"})
        ->Define("Muon_pt_scale_down",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Muon_pt"})
        ->Define("Muon_pt_smear_up",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Muon_pt"})
        ->Define("Muon_pt_smear_down",
                 [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                 {"Muon_pt"})
        ->Define("CorrectedMuons",
                 [](const ROOT::VecOps::RVec<float> &pt,
                    const ROOT::VecOps::RVec<float> &eta,
                    const ROOT::VecOps::RVec<float> &phi,
                    const ROOT::VecOps::RVec<float> &mass) {
                   ROOT::VecOps::RVec<bool> mask(pt.size(), true);
                   return PhysicsObjectCollection(pt, eta, phi, mass, mask);
                 },
                 {"Muon_pt_corr_nominal", "Muon_eta", "Muon_phi", "Muon_mass"});
  }
}

// ===========================================================================
// HEM issue filter (2018)
// ===========================================================================

void applyCmsHemIssueFilter(Analyzer &an, const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings) return;
  const auto &ys = *cfg.yearSettings;

  // Only relevant for 2018
  if (ys.canonicalYear != "2018") return;

  // HEM-affected regions (from CMS JME recommendations):
  //   Region 1: -1.57 < phi < -0.87, -2.5 < eta < -1.3
  //   Region 2: -1.57 < phi < -0.87, -3.0 < eta < -2.5
  constexpr float kPhiMin = -1.57f;
  constexpr float kPhiMax = -0.87f;
  constexpr float kEtaMin1 = -2.5f;
  constexpr float kEtaMax1 = -1.3f;
  constexpr float kEtaMin2 = -3.0f;
  constexpr float kEtaMax2 = -2.5f;

  constexpr float kPtMin = 15.0f;

  // Check if required columns exist
  const auto cols = an.GetColumnNames();
  const bool hasJetId = std::find(cols.begin(), cols.end(), "Jet_jetId_i") != cols.end();
  const bool hasChEm = std::find(cols.begin(), cols.end(), "Jet_chEmEF") != cols.end();
  const bool hasNeEm = std::find(cols.begin(), cols.end(), "Jet_neEmEF") != cols.end();

  if (!hasJetId || !hasChEm || !hasNeEm) {
    std::cerr << "WARNING: applyCmsHemIssueFilter — required Jet columns "
              << "(Jet_jetId_i, Jet_chEmEF, Jet_neEmEF) not found. "
              << "HEM filter skipped.\n";
    return;
  }

  if (!cfg.applyHemIssueFilter) return;

  const double lumiFrac = cfg.hemIssueLumiFraction;

  // Define a column that identifies HEM-vetoed jets (idempotent: only defines
  // if not already present)
  const bool hasVetoJet = std::find(cols.begin(), cols.end(),
                                     "hem_veto_jet") != cols.end();
  if (!hasVetoJet) {
    an.Define("hem_veto_jet",
              [](const ROOT::VecOps::RVec<float> &jetPt,
                 const ROOT::VecOps::RVec<float> &jetEta,
                 const ROOT::VecOps::RVec<float> &jetPhi,
                 const ROOT::VecOps::RVec<int> &jetId,
                 const ROOT::VecOps::RVec<float> &chEmEF,
                 const ROOT::VecOps::RVec<float> &neEmEF) -> bool {
                for (size_t i = 0; i < jetPt.size(); ++i) {
                  if (jetPt[i] < kPtMin) continue;
                  if (!(jetId[i] & 4)) continue;  // tightLepVeto
                  if ((chEmEF[i] + neEmEF[i]) >= 0.9f) continue;
                  const float eta = jetEta[i];
                  const float phi = jetPhi[i];
                  if (phi < kPhiMin || phi > kPhiMax) continue;
                  const bool inRegion1 = (eta > kEtaMin1 && eta < kEtaMax1);
                  const bool inRegion2 = (eta > kEtaMin2 && eta < kEtaMax2);
                  if (inRegion1 || inRegion2) return true;
                }
                return false;
              },
              {"Jet_pt", "Jet_eta", "Jet_phi", "Jet_jetId_i",
               "Jet_chEmEF", "Jet_neEmEF"});
  }

  const bool hasPass = std::find(cols.begin(), cols.end(),
                                  "hem_veto_pass") != cols.end();
  if (!hasPass) {
    if (lumiFrac > 0.0 && lumiFrac < 1.0) {
      // MC: veto a fraction of events matching the lumi fraction
      an.Define("hem_veto_pass",
                [lumiFrac](ULong64_t event, bool hasVetoJet) -> bool {
                  if (!hasVetoJet) return true;
                  const double frac =
                      static_cast<double>(event % 10000) / 10000.0;
                  if (frac < lumiFrac) return false;
                  return true;
                },
                {"event", "hem_veto_jet"});
    } else {
      // Data or full MC veto: veto events with HEM-affected jets
      an.Define("hem_veto_pass",
                [](bool hasVetoJet) -> bool { return !hasVetoJet; },
                {"hem_veto_jet"});
    }
  }

  an.Filter("hem_veto_pass", [](bool pass) { return pass; },
            {"hem_veto_pass"});
}

// ===========================================================================
// JetVetoMap integration
// ===========================================================================

void applyCmsJetVetoMap(Analyzer &an, const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings) return;

  auto jvm = an.getPlugin<JetVetoMapManager>("jetVetoMapManager");
  if (!jvm) {
    std::cerr << "WARNING: applyCmsJetVetoMap — JetVetoMapManager not found. "
              << "Call setupCmsFrameworkPlugins() first.\n";
    return;
  }

  // Auto-configure the JetVetoMap config if not already set
  // The jetvetomaps.json.gz lives alongside the JME JSON file
  const auto &ys = *cfg.yearSettings;
  if (!ys.jmeFile.empty()) {
    // Derive jetvetomap path from JME file path
    // e.g. .../JME/Run3-...-NanoAODv12/latest/jet_jerc.json.gz
    //  -> .../JME/Run3-...-NanoAODv12/latest/jetvetomaps.json.gz
    std::string mapPath = ys.jmeFile;
    const auto lastSlash = mapPath.rfind('/');
    if (lastSlash != std::string::npos) {
      mapPath = mapPath.substr(0, lastSlash + 1) + "jetvetomaps.json.gz";
    }
    auto &config = an.getConfigurationProvider();
    const std::string existing = config.get("jetVetoMapConfig");
    if (existing.empty()) {
      config.set("jetVetoMapConfig", mapPath);
    }
  }

  // Apply the JetVetoMap
  jvm->applyJetVetoMap();
}

// ===========================================================================
// GenWeights integration
// ===========================================================================

void setupCmsGenWeights(Analyzer &an) {
  auto genWm = an.getPlugin<GenWeightManager>("genWeightManager");
  if (!genWm) {
    std::cerr << "WARNING: setupCmsGenWeights — GenWeightManager not found. "
              << "Call setupCmsFrameworkPlugins() first.\n";
    return;
  }

  // Configure from Analyzer config keys
  genWm->setupFromConfigFile();

  // Define LHE scale, PDF, and PS variation weights
  genWm->defineVariationWeights();

  // Register generator weight variations with WeightManager
  auto wm = an.getPlugin<WeightManager>("weightManager");
  if (wm) {
    // Register genWeight as a scale factor component for the nominal weight
    wm->addScaleFactor("genWeight", genWm->nominalColumn());

    // Register systematic variations from GenWeightManager
    for (const auto &var : genWm->variations()) {
      wm->addWeightVariation(var.label, "genWeight",
                              var.column, var.column);
    }
  }
}

} // namespace cms
