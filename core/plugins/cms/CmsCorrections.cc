//==============================================================================
// CmsCorrections — Generic CMS correction workflow setup
//
// Implements the generic CMS correction workflow functions.
// Extracted from VHqqCorrections.cc (VHbb STXS analysis) and generalized.
//==============================================================================

#include "CmsCorrections.h"
#include "CmsWeights.h"

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

std::string deriveJetVetoMapPath(const CmsYearSettings &ys) {
  if (ys.jmeFile.empty()) return "";
  std::string mapPath = ys.jmeFile;
  const auto lastSlash = mapPath.rfind('/');
  if (lastSlash == std::string::npos) return "";
  return mapPath.substr(0, lastSlash + 1) + "jetvetomaps.json.gz";
}

void ensureCmsJetVetoMapConfig(Analyzer &an,
                               const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings) return;
  auto &config = an.getConfigurationProvider();
  if (!config.get("jetVetoMapConfig").empty()) return;

  const std::string mapPath = deriveJetVetoMapPath(*cfg.yearSettings);
  if (!mapPath.empty()) config.set("jetVetoMapConfig", mapPath);
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

void CmsCorrectionAudit::add(const std::string &category,
                              const std::string &reason,
                              const std::string &detail,
                              const std::string &remediation) {
  entries.push_back({category, reason, detail, remediation});
}

void CmsCorrectionAudit::print(std::ostream &os) const {
  if (entries.empty()) {
    os << "CmsCorrectionAudit: No issues recorded.\n";
    return;
  }
  os << "CmsCorrectionAudit: " << entries.size() << " issue(s).\n";
  for (const auto &entry : entries) {
    os << "  [" << entry.category << "] " << entry.reason << ": "
       << entry.detail;
    if (!entry.remediation.empty()) os << " Remediation: " << entry.remediation;
    os << "\n";
  }
}

void CmsCorrectionAudit::printSummary(std::ostream &os) const {
  if (entries.empty()) {
    os << "CmsCorrectionAudit: All corrections available.\n";
    return;
  }
  os << "CmsCorrectionAudit: " << entries.size() << " issue(s):";
  for (const auto &entry : entries) os << " " << entry.category;
  os << "\n";
}

bool CmsCorrectionAudit::hasCategory(const std::string &category) const {
  return std::any_of(entries.begin(), entries.end(),
                     [&category](const CmsCorrectionAuditEntry &entry) {
                       return entry.category == category;
                     });
}

bool CmsCorrectionAudit::hasReason(const std::string &reason) const {
  return std::any_of(entries.begin(), entries.end(),
                     [&reason](const CmsCorrectionAuditEntry &entry) {
                       return entry.reason == reason;
                     });
}

CmsCorrectionAudit validateCmsCorrectionStack(
    const CmsCorrectionConfig &corrCfg,
    const CmsWeightConfig *weightCfg) {
  CmsCorrectionAudit audit;
  if (!corrCfg.yearSettings) {
    audit.add("config", "missing_year_settings",
              "No CmsYearSettings were supplied.",
              "Load a year through CmsYearRegistry before configuring corrections.");
    return audit;
  }

  const auto &ys = *corrCfg.yearSettings;
  const auto check = [&audit](const std::string &category,
                              const std::string &file,
                              const std::string &detail,
                              const std::string &remediation) {
    if (file.empty() || !std::filesystem::exists(file)) {
      audit.add(category, "payload_not_found", detail + " Path: " + file,
                remediation);
    }
  };

  if (corrCfg.applyJetCorrections) {
    check("jec", ys.jmeFile, "AK4 JME payload is unavailable.",
          "Set the 2024 JME payload path or disable jet corrections.");
  }
  if (corrCfg.applyFatJetCorrections) {
    check("fatjet_jec", ys.fatJetJmeFile,
          "AK8 JME payload is unavailable.",
          "Set the fat-jet JME payload path or disable fat-jet corrections.");
  }
  if (corrCfg.applyMuonCorrections) {
    check("muon_scale", ys.muonScaleResolutionFile,
          "Muon scale and resolution payload is unavailable.",
          "Set the 2024 muon scale/resolution payload or disable muon corrections.");
  }
  if (corrCfg.applyBtagCorrections) {
    check("btag", ys.btvBTagFile, "BTV b-tag payload is unavailable.",
          "Set the 2024 BTV payload or disable b-tag corrections.");
  }

  if (weightCfg && weightCfg->isMC) {
    if (weightCfg->applyPileupWeights) {
      check("pileup", ys.pileupFile, "Pileup payload is unavailable.",
            "Set the 2024 pileup payload or disable pileup weights.");
    }
    if (weightCfg->applyMuonScaleFactors) {
      check("muon_sf", ys.muonFile, "Muon ID/isolation payload is unavailable.",
            "Set the 2024 muon SF payload or disable muon scale factors.");
    }
    if (weightCfg->applyBTagWeights || weightCfg->applyBTagScaleFactors) {
      check("btag", ys.btvBTagFile, "BTV b-tag SF payload is unavailable.",
            "Set the 2024 BTV payload or disable b-tag weights.");
      if (weightCfg->btagMethod == BTagMethod::Shape) {
        const std::string shapeFile = weightCfg->btagShapeFile.empty()
                                          ? ys.btvBTagFile
                                          : weightCfg->btagShapeFile;
        check("btag_shape", shapeFile,
          "Continuous b-tag shape payload is unavailable.",
          "Set btagShapeFile to a payload containing the continuous correction.");
        if (weightCfg->btagShapeCorrection.empty()) {
          audit.add("btag_shape", "correction_name_missing",
            "No continuous b-tag correction name is configured.",
            "Set btagShapeCorrection, for example UParTAK4_shape.");
        }
      } else {
        check("btag_efficiency", weightCfg->btagEfficiencyFile,
          "MC b-tag efficiency payload is unavailable.",
          "Set btagEfficiencyFile and btagEfficiencyCorrection.");
        if (weightCfg->btagEfficiencyCorrection.empty()) {
          audit.add("btag_efficiency", "correction_name_missing",
            "No MC b-tag efficiency correction name is configured.",
            "Set btagEfficiencyCorrection.");
        }
      }
    }
    if (weightCfg->applyTriggerScaleFactors) {
      check("trigger", weightCfg->triggerSfFile,
        "Path-specific dimuon trigger SF payload is unavailable.",
        "Set triggerSfFile and triggerSfCorrection for the requested HLT.");
      if (weightCfg->triggerSfCorrection.empty()) {
        audit.add("trigger", "correction_name_missing",
          "No path-specific trigger SF correction name is configured.",
          "Set triggerSfCorrection for the requested HLT.");
      }
    }
  }
  return audit;
}

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
  const auto &workingPoints = btagMgr->getWorkingPoints();
  const auto addIfMissing = [&btagMgr, &workingPoints](const std::string &name,
                                                        float threshold) {
    if (threshold == 0.0f) return;
    const auto existing = std::find_if(
        workingPoints.begin(), workingPoints.end(),
        [&name](const WorkingPointEntry &workingPoint) {
          return workingPoint.name == name;
        });
    if (existing == workingPoints.end()) {
      btagMgr->addWorkingPoint(name, threshold);
    }
  };
  addIfMissing("L", btWp.loose);
  addIfMissing("M", btWp.medium);
  addIfMissing("T", btWp.tight);
  addIfMissing("XT", btWp.xtight);
  addIfMissing("XXT", btWp.xxtight);
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
  const auto &workingPoints = ctagMgr->getWorkingPoints();
  const auto addIfMissing = [&ctagMgr, &workingPoints](
                                const std::string &name,
                                const std::vector<float> &thresholds) {
    if (thresholds[0] == 0.0f && thresholds[1] == 0.0f) return;
    const auto existing = std::find_if(
        workingPoints.begin(), workingPoints.end(),
        [&name](const WorkingPointEntry &workingPoint) {
          return workingPoint.name == name;
        });
    if (existing == workingPoints.end()) {
      ctagMgr->addWorkingPoint(name, thresholds);
    }
  };
  addIfMissing("L", {ctWp.cvlLoose, ctWp.cvbLoose});
  addIfMissing("M", {ctWp.cvlMedium, ctWp.cvbMedium});
  addIfMissing("T", {ctWp.cvlTight, ctWp.cvbTight});
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
  ensureCmsJetVetoMapConfig(an, cfg);
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

void defineCmsCorrectionInputs(Analyzer &an,
                                const CmsCorrectionConfig &cfg) {
  const bool isData = toLowerCopy(an.getConfigurationProvider().get("dtype")) ==
                      "data";

  if (cfg.applyElectronCorrections) {
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
  }

  if (cfg.applyJetCorrections && !isData) {
    // Jet_genMatchedPt: matched GenJet pT for JER smearing
    an.Define("Jet_genMatchedPt",
            [](const std::vector<int> &genJetIdx,
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
  const bool isData = toLowerCopy(config.get("dtype")) == "data";

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

  if (cfg.applyMuonCorrections && !ys.muonScaleResolutionFile.empty()) {
    auto muonMgr = an.getPlugin<MuonRochesterManager>("muonRochesterManager");
    if (!muonMgr) {
      throw std::runtime_error(
          "CmsCorrections: muonRochesterManager is required for muon corrections.");
    }
    muonMgr->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
    muonMgr->setRochesterInputColumns(
        "Muon_charge", "", "Muon_nTrackerLayers", "", "");
    muonMgr->setScaleResolutionEventColumns("luminosityBlock", "event");
    const bool isData = config.get("dtype") == "data";
    muonMgr->applyScaleAndResolution(
        ys.muonScaleResolutionFile, isData, "Muon_pt",
        "Muon_pt_corr_nominal");
  }

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

  jesMgr->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
  std::string rawPtColumn = "Jet_pt_raw";
  std::string rawMassColumn = "Jet_mass_raw";
  if (cfg.jetRawPtColumnOverride.empty()) {
    jesMgr->removeExistingCorrections("Jet_rawFactor");
  } else {
    jesMgr->setRawPtColumn(cfg.jetRawPtColumnOverride);
    rawPtColumn = cfg.jetRawPtColumnOverride;
    if (!cfg.jetRawMassColumnOverride.empty()) {
      rawMassColumn = cfg.jetRawMassColumnOverride;
    }
  }

  const std::vector<std::string> jecInputs{
    "Jet_area", "Jet_eta", rawPtColumn, ys.rhoBranch, "Jet_phi"};
  const std::string jecCorrectionName = prefix + "_jec_nominal";
  corrMgr->registerCorrection(jecCorrectionName, ys.jmeFile, jecCompound,
                              jecInputs);
  jesMgr->applyCorrectionlib(*corrMgr, jecCorrectionName, {}, rawPtColumn,
                              "Jet_pt_corr_jec", true, rawMassColumn,
                              "Jet_mass_corr_jec", jecInputs);

  if (!isData) {
    const std::string jerResolutionName = prefix + "_jer_resolution";
    const std::string jerScaleFactorName = prefix + "_jer_scale_factor";
    const std::vector<std::string> jerResolutionInputs{
      "Jet_eta", "Jet_pt_corr_jec", ys.rhoBranch};
    corrMgr->registerCorrection(jerResolutionName, ys.jmeFile,
                                ys.jerPtResolutionCorrection,
                                jerResolutionInputs);
    corrMgr->registerCorrection(jerScaleFactorName, ys.jmeFile,
                                ys.jerScaleFactorCorrection,
                                ys.jerScaleFactorInputs);
    jesMgr->setJERSmearingColumns("Jet_genMatchedPt", ys.rhoBranch, "event");
    jesMgr->applyJERSmearing(*corrMgr, jerResolutionName, jerScaleFactorName,
                             "Jet_pt_corr_jec", "Jet_pt_corr_nominal", "nom",
                             true, "Jet_mass_corr_jec",
                             "Jet_mass_corr_nominal", jerResolutionInputs,
                             ys.jerScaleFactorInputs);
  }

  jesMgr->execute();

  if (isData) {
    an.Define("Jet_pt_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"Jet_pt_corr_jec"});
    an.Define("Jet_mass_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
              {"Jet_mass_corr_jec"});
  }

  if (cfg.applyMetType1) {
    jesMgr->setMETColumns("PuppiMET_pt", "PuppiMET_phi");
    jesMgr->propagateMET(
        "PuppiMET_pt", "PuppiMET_phi", "Jet_pt", "Jet_pt_corr_nominal",
        "PuppiMET_pt_corr_nominal", "PuppiMET_phi_corr_nominal");
  }

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

  const std::string jesTotalCorrection =
      cfg.jesTotalCorrectionOverride.empty()
          ? ys.jesTotalCorrection
          : cfg.jesTotalCorrectionOverride;
  if (!jesSources.empty()) {
    jesMgr->registerSystematicSources(jesModeStr, jesSources);
    const std::vector<std::string> jesInputs{"Jet_eta", "Jet_pt_corr_nominal"};
    for (const auto &source : jesSources) {
      const std::string correctionlibName =
          source == "Total" ? jesTotalCorrection
                             : jesCorrectionNameForSource(jesTotalCorrection,
                                                          source);
      const std::string correctionName = prefix + "_jes_" + source;
      corrMgr->registerCorrection(correctionName, ys.jmeFile,
                                  correctionlibName, jesInputs);
      const std::string label = source == "Total" ? "total" : source;
      const std::string upPtColumn = "Jet_pt_jes_" + label + "_up";
      const std::string downPtColumn = "Jet_pt_jes_" + label + "_down";
      const std::string upMassColumn = "Jet_mass_jes_" + label + "_up";
      const std::string downMassColumn = "Jet_mass_jes_" + label + "_down";
      jesMgr->applyCorrectionlib(
          *corrMgr, correctionName, {}, "Jet_pt_corr_nominal", upPtColumn,
          true, "Jet_mass_corr_nominal", upMassColumn, jesInputs, true, 1.0f);
      jesMgr->applyCorrectionlib(
          *corrMgr, correctionName, {}, "Jet_pt_corr_nominal", downPtColumn,
          true, "Jet_mass_corr_nominal", downMassColumn, jesInputs, true,
          -1.0f);
      jesMgr->addVariation(label, upPtColumn, downPtColumn, upMassColumn,
                           downMassColumn);
      an.getSystematicManager().registerVariationColumns(
          "Jet_pt_corr_nominal", label, upPtColumn, downPtColumn);
      an.getSystematicManager().registerVariationColumns(
          "Jet_mass_corr_nominal", label, upMassColumn, downMassColumn);
      if (cfg.applyMetType1) {
        const std::string upMetPtColumn =
            "PuppiMET_pt_jes_" + label + "_up";
        const std::string downMetPtColumn =
            "PuppiMET_pt_jes_" + label + "_down";
        const std::string upMetPhiColumn =
            "PuppiMET_phi_jes_" + label + "_up";
        const std::string downMetPhiColumn =
            "PuppiMET_phi_jes_" + label + "_down";
        jesMgr->propagateMET(
            "PuppiMET_pt_corr_nominal", "PuppiMET_phi_corr_nominal",
            "Jet_pt_corr_nominal", upPtColumn, upMetPtColumn,
            upMetPhiColumn);
        jesMgr->propagateMET(
            "PuppiMET_pt_corr_nominal", "PuppiMET_phi_corr_nominal",
            "Jet_pt_corr_nominal", downPtColumn, downMetPtColumn,
            downMetPhiColumn);
        an.getSystematicManager().registerVariationColumns(
            "PuppiMET_pt_corr_nominal", label, upMetPtColumn,
            downMetPtColumn);
        an.getSystematicManager().registerVariationColumns(
            "PuppiMET_phi_corr_nominal", label, upMetPhiColumn,
            downMetPhiColumn);
      }
    }
  }

  jesMgr->execute();
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
// MET event-cleaning filters
// ===========================================================================

void applyCmsMetFilters(Analyzer &an) {
  const std::vector<std::string> names{
      "Flag_HBHENoiseFilter",
      "Flag_HBHENoiseIsoFilter",
      "Flag_CSCTightHaloFilter",
      "Flag_goodVertices",
      "Flag_eeBadScFilter",
      "Flag_EcalDeadCellTriggerPrimitiveFilter",
      "Flag_BadPFMuonFilter",
      "Flag_BadChargedCandidateFilter",
      "Flag_ecalBadCalibFilter"};

  const auto columns = an.GetColumnNames();
  std::vector<std::string> filterColumns;
  filterColumns.reserve(names.size());
  for (const auto &name : names) {
    const std::string output = "cms_met_filter_" + name.substr(5);
    filterColumns.push_back(output);
    if (std::find(columns.begin(), columns.end(), name) == columns.end()) {
      an.Define(output, []() { return true; }, {});
    } else {
      an.Define(output, [](bool value) { return value; }, {name});
    }
  }

  an.Filter(
      "cms_met_filters",
      [](bool hbhe, bool hbheIso, bool halo, bool vertices, bool eeBadSc,
         bool deadCell, bool badMuon, bool badCharged, bool badCalib) {
        return hbhe && hbheIso && halo && vertices && eeBadSc && deadCell &&
               badMuon && badCharged && badCalib;
      },
      filterColumns);
}

// ===========================================================================
// JetVetoMap integration
// ===========================================================================

void applyCmsJetVetoMap(Analyzer &an, const CmsCorrectionConfig &cfg) {
  if (!cfg.yearSettings) return;

  ensureCmsJetVetoMapConfig(an, cfg);

  auto jvm = an.getPlugin<JetVetoMapManager>("jetVetoMapManager");
  if (!jvm) {
    std::cerr << "WARNING: applyCmsJetVetoMap — JetVetoMapManager not found. "
              << "Call setupCmsFrameworkPlugins() first.\n";
    return;
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
