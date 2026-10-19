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
#include <utility>
#include <vector>

namespace cms {

namespace {

/// Convert string to lower case.
std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

bool hasColumn(Analyzer &analyzer, const std::string &name) {
  const auto columns = analyzer.GetColumnNames();
  return std::find(columns.begin(), columns.end(), name) != columns.end();
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
  const auto &values = config.getConfigMap();
  const auto it = values.find(key);
  if (it == values.end() || it->second.empty()) {
    config.set(key, value);
  }
}

void defineElectronIdentityCorrections(Analyzer &an) {
  an.Define("Electron_pt_corr_nominal",
            [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
            {"Electron_pt"})
      ->Define("Electron_mass_corr_nominal",
               [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
               {"Electron_mass"});
}

void defineElectronIdentityVariations(Analyzer &an,
                                      const std::string &nominalPtColumn,
                                      const std::string &nominalMassColumn) {
  an.Define("Electron_pt_scale_up",
            [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
            {nominalPtColumn})
      ->Define("Electron_pt_scale_down",
               [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
               {nominalPtColumn})
      ->Define("Electron_pt_smear_up",
               [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
               {nominalPtColumn})
      ->Define("Electron_pt_smear_down",
               [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
               {nominalPtColumn})
      ->Define("Electron_mass_scale_up",
               [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
               {nominalMassColumn})
      ->Define("Electron_mass_scale_down",
               [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
               {nominalMassColumn})
      ->Define("Electron_mass_smear_up",
               [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
               {nominalMassColumn})
      ->Define("Electron_mass_smear_down",
               [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
               {nominalMassColumn});
}

void registerElectronVariationMappings(Analyzer &an) {
  auto &systematics = an.getSystematicManager();
  systematics.registerVariationColumns(
      "Electron_pt_corr_nominal", "ElectronScale", "Electron_pt_scale_up",
      "Electron_pt_scale_down");
  systematics.registerVariationColumns(
      "Electron_mass_corr_nominal", "ElectronScale",
      "Electron_mass_scale_up", "Electron_mass_scale_down");
  systematics.registerVariationColumns(
      "Electron_pt_corr_nominal", "ElectronSmear", "Electron_pt_smear_up",
      "Electron_pt_smear_down");
  systematics.registerVariationColumns(
      "Electron_mass_corr_nominal", "ElectronSmear",
      "Electron_mass_smear_up", "Electron_mass_smear_down");
}

void configureElectronScaleSmearing(Analyzer &an,
                                    const CmsCorrectionConfig &cfg,
                                    const CmsYearSettings &ys,
                                    bool isData) {
  if (!cfg.applyElectronCorrections) return;

  if (ys.electronScaleSmearingFile.empty()) {
    // Run 2 NanoAOD already includes the EGM scale and smearing corrections.
    defineElectronIdentityCorrections(an);
    if (cfg.applyLeptonScaleSmearSystematics &&
        an.getSystematicManager().isEnabled()) {
      defineElectronIdentityVariations(an, "Electron_pt_corr_nominal",
                                       "Electron_mass_corr_nominal");
      registerElectronVariationMappings(an);
    }
    return;
  }
  if (ys.electronScaleCorrection.empty() ||
      ys.electronSmearingCorrection.empty()) {
    throw std::runtime_error(
        "CmsCorrections: electron scale/smearing file is configured, but its "
        "correction names are empty.");
  }

  auto electronMgr =
      an.getPlugin<ElectronEnergyScaleManager>("electronEnergyScaleManager");
  auto corrMgr = an.getPlugin<CorrectionManager>("correctionManager");
  if (!electronMgr || !corrMgr) {
    throw std::runtime_error(
        "CmsCorrections: electron corrections require both "
        "electronEnergyScaleManager and correctionManager.");
  }

  electronMgr->setObjectColumns("Electron_pt", "Electron_eta", "Electron_phi",
                                "Electron_mass");

  const std::string prefix =
      cfg.configKeyPrefix.empty() ? "cms" : cfg.configKeyPrefix;
  const std::string scaleName = prefix + "_electron_scale";
  const std::string smearName = prefix + "_electron_smearing";
  const bool etDependent = ys.electronScaleSmearingEtDependent;
  const bool includeVariations = cfg.applyLeptonScaleSmearSystematics &&
                                 an.getSystematicManager().isEnabled();
  const std::vector<std::string> smearInputs = etDependent
      ? std::vector<std::string>{"Electron_pt", "Electron_r9", "Electron_scEta"}
      : std::vector<std::string>{"Electron_scEta", "Electron_r9"};
  const std::vector<std::string> scaleInputs = etDependent
      ? std::vector<std::string>{"Electron_run", "Electron_scEta", "Electron_r9",
                                 "Electron_pt", "Electron_seedGain"}
      : std::vector<std::string>{"Electron_seedGain", "Electron_run",
                                 "Electron_scEta", "Electron_r9", "Electron_pt"};

  if (isData || (!etDependent && includeVariations)) {
    // Correctionlib's EGM scale compound takes a per-electron run number.
    // Materialize it as an RVec to keep all correction inputs object-aligned.
    an.Define("Electron_run",
              [](UInt_t run, const ROOT::VecOps::RVec<float> &pt) {
                return ROOT::VecOps::RVec<UInt_t>(pt.size(), run);
              },
              {"run", "Electron_pt"});
  }
  if (isData) {
    corrMgr->registerCorrection(scaleName, ys.electronScaleSmearingFile,
                                ys.electronScaleCorrection, scaleInputs);
    electronMgr->applyCorrectionlib(
        *corrMgr, scaleName, {etDependent ? "scale" : "total_correction"}, "Electron_pt",
        "Electron_pt_corr_nominal", true, "Electron_mass",
        "Electron_mass_corr_nominal", scaleInputs);
    electronMgr->execute();
    if (cfg.applyLeptonScaleSmearSystematics &&
        an.getSystematicManager().isEnabled()) {
      defineElectronIdentityVariations(an, "Electron_pt_corr_nominal",
                                       "Electron_mass_corr_nominal");
      registerElectronVariationMappings(an);
    }
    return;
  }

  corrMgr->registerCorrection(smearName, ys.electronScaleSmearingFile,
                              ys.electronSmearingCorrection, smearInputs);
  const std::string randomColumn = "Electron_smearing_random";
  electronMgr->defineReproducibleGaussian(
      randomColumn, "Electron_pt", "run", "luminosityBlock", "event",
      "cms_egm_electron_smearing");
  // Define the deterministic Gaussian input before scheduling the columns
  // that consume it.
  electronMgr->execute();

  corrMgr->applyCorrectionVec(smearName, {etDependent ? "smear" : "rho"},
                              smearInputs, "Electron_smearing_sigma");
  if (includeVariations && etDependent) {
    corrMgr->applyCorrectionVec(smearName, {"smear_up"}, smearInputs,
                                "Electron_smearing_sigma_up");
    corrMgr->applyCorrectionVec(smearName, {"smear_down"}, smearInputs,
                                "Electron_smearing_sigma_down");
    corrMgr->applyCorrectionVec(smearName, {"escale"}, smearInputs,
                                "Electron_scale_uncertainty");
  } else if (includeVariations) {
    corrMgr->applyCorrectionVec(smearName, {"err_rho"}, smearInputs,
                                "Electron_smearing_sigma_error");
    for (const auto &direction : {std::pair<const char *, float>{"up", 1.0f},
                                 {"down", -1.0f}}) {
      an.Define(std::string("Electron_smearing_sigma_") + direction.first,
          [sign = direction.second](const ROOT::VecOps::RVec<float> &sigma,
                                    const ROOT::VecOps::RVec<float> &error) {
            ROOT::VecOps::RVec<float> result(sigma.size());
            for (std::size_t i = 0; i < sigma.size(); ++i) {
              result[i] = std::max(0.0f, sigma[i] + sign * error.at(i));
            }
            return result;
          }, {"Electron_smearing_sigma", "Electron_smearing_sigma_error"});
    }
    corrMgr->registerCorrection(scaleName, ys.electronScaleSmearingFile,
                                ys.electronScaleCorrection, scaleInputs);
    corrMgr->applyCorrectionVec(scaleName, {"total_uncertainty"}, scaleInputs,
                                "Electron_scale_uncertainty");
  }

  an.Define("Electron_smearing_factor",
            [](const ROOT::VecOps::RVec<float> &sigma,
               const ROOT::VecOps::RVec<float> &random) {
              ROOT::VecOps::RVec<float> factor(sigma.size());
              for (std::size_t i = 0; i < sigma.size(); ++i) {
                factor[i] = 1.0f + sigma[i] * random[i];
              }
              return factor;
            },
            {"Electron_smearing_sigma", randomColumn});
  if (includeVariations) an.Define("Electron_smearing_factor_up",
            [](const ROOT::VecOps::RVec<float> &sigma,
               const ROOT::VecOps::RVec<float> &random) {
              ROOT::VecOps::RVec<float> factor(sigma.size());
              for (std::size_t i = 0; i < sigma.size(); ++i) {
                factor[i] = 1.0f + sigma[i] * random[i];
              }
              return factor;
            },
            {"Electron_smearing_sigma_up", randomColumn});
  if (includeVariations) an.Define("Electron_smearing_factor_down",
            [](const ROOT::VecOps::RVec<float> &sigma,
               const ROOT::VecOps::RVec<float> &random) {
              ROOT::VecOps::RVec<float> factor(sigma.size());
              for (std::size_t i = 0; i < sigma.size(); ++i) {
                factor[i] = 1.0f + sigma[i] * random[i];
              }
              return factor;
            },
            {"Electron_smearing_sigma_down", randomColumn});
  if (includeVariations) an.Define("Electron_scale_factor_up",
            [](const ROOT::VecOps::RVec<float> &uncertainty) {
              ROOT::VecOps::RVec<float> factor(uncertainty.size());
              for (std::size_t i = 0; i < uncertainty.size(); ++i) {
                factor[i] = 1.0f + uncertainty[i];
              }
              return factor;
            },
            {"Electron_scale_uncertainty"});
  if (includeVariations) an.Define("Electron_scale_factor_down",
            [](const ROOT::VecOps::RVec<float> &uncertainty) {
              ROOT::VecOps::RVec<float> factor(uncertainty.size());
              for (std::size_t i = 0; i < uncertainty.size(); ++i) {
                factor[i] = 1.0f - uncertainty[i];
              }
              return factor;
            },
            {"Electron_scale_uncertainty"});

  electronMgr->applyCorrection(
      "Electron_pt", "Electron_smearing_factor",
      "Electron_pt_corr_nominal", true, "Electron_mass",
      "Electron_mass_corr_nominal");
  if (includeVariations) {
    electronMgr->applyCorrection(
        "Electron_pt", "Electron_smearing_factor_up", "Electron_pt_smear_up",
        true, "Electron_mass", "Electron_mass_smear_up");
    electronMgr->applyCorrection(
        "Electron_pt", "Electron_smearing_factor_down",
        "Electron_pt_smear_down", true, "Electron_mass",
        "Electron_mass_smear_down");
    electronMgr->addVariation("ElectronSmear", "Electron_pt_smear_up",
                              "Electron_pt_smear_down",
                              "Electron_mass_smear_up",
                              "Electron_mass_smear_down");
    electronMgr->applyCorrection(
        "Electron_pt_corr_nominal", "Electron_scale_factor_up",
        "Electron_pt_scale_up", true, "Electron_mass_corr_nominal",
        "Electron_mass_scale_up");
    electronMgr->applyCorrection(
        "Electron_pt_corr_nominal", "Electron_scale_factor_down",
        "Electron_pt_scale_down", true, "Electron_mass_corr_nominal",
        "Electron_mass_scale_down");
    electronMgr->addVariation("ElectronScale", "Electron_pt_scale_up",
                              "Electron_pt_scale_down",
                              "Electron_mass_scale_up",
                              "Electron_mass_scale_down");
  }
  electronMgr->execute();
  if (includeVariations) registerElectronVariationMappings(an);
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
                                       const std::string &source,
                                       bool fullSplit = false) {
  std::string out = totalCorrectionName;
  std::string payloadSource = source;
  if (fullSplit) {
    // Full maps are unregrouped and have no year suffix in the payload.
    // Keep the suffix on the variation label to preserve its correlations.
    const auto regrouped = out.find("_Regrouped_");
    if (regrouped != std::string::npos) {
      out.replace(regrouped, std::string("_Regrouped_").size(), "_");
    }
    const auto suffix = payloadSource.rfind('_');
    if (suffix != std::string::npos &&
        payloadSource.compare(suffix + 1, 2, "20") == 0) {
      payloadSource.erase(suffix);
    }
  }
  const auto totalPos = out.find("Total");
  if (totalPos == std::string::npos) {
    throw std::runtime_error(
        "CmsCorrections: cannot derive JES source correction name from '" +
        totalCorrectionName + "': no 'Total' token found.");
  }
  out.replace(totalPos, std::string("Total").size(), payloadSource);
  return out;
}

} // namespace

void CmsCorrectionConfig::print(std::ostream &os) const {
  const auto yesNo = [](bool value) { return value ? "yes" : "no"; };
  const char *jesModeName = "Reduced";
  switch (jesMode) {
  case Total:
    jesModeName = "Total";
    break;
  case Reduced:
    jesModeName = "Reduced";
    break;
  case Full:
    jesModeName = "Full";
    break;
  }

  os << "=== CmsCorrectionConfig ===\n"
     << "  year: "
     << (yearSettings ? yearSettings->canonicalYear : "<unset>") << '\n'
     << "  jetTagger: " << jetTagger << '\n'
     << "  jesMode: " << jesModeName << '\n'
     << "  applyJetCorrections: " << yesNo(applyJetCorrections) << '\n'
     << "  applyFatJetCorrections: " << yesNo(applyFatJetCorrections) << '\n'
     << "  applyElectronCorrections: " << yesNo(applyElectronCorrections)
     << '\n'
     << "  applyJesSystematics: " << yesNo(applyJesSystematics) << '\n'
     << "  applyJerSystematics: " << yesNo(applyJerSystematics) << '\n'
     << "  applyLeptonScaleSmearSystematics: "
     << yesNo(applyLeptonScaleSmearSystematics) << '\n'
     << "  applyMuonCorrections: " << yesNo(applyMuonCorrections) << '\n'
     << "  applyBtagCorrections: " << yesNo(applyBtagCorrections) << '\n'
     << "  applyCtagCorrections: " << yesNo(applyCtagCorrections) << '\n'
     << "  applyTauCorrections: " << yesNo(applyTauCorrections) << '\n'
     << "  applyMetType1: " << yesNo(applyMetType1) << '\n'
     << "  maxJesSlots: " << maxJesSlots << '\n'
     << "  correctionNamePrefix: " << correctionNamePrefix << '\n';
}

std::string detail::jecCompoundForSample(const std::string &defaultCompound,
                                        bool isData) {
  std::string selected = defaultCompound;
  const auto token = selected.find("_MC_");
  if (isData && token != std::string::npos) {
    selected.replace(token, 4, "_DATA_");
  }
  return selected;
}

std::vector<std::string> detail::jecCompoundInputColumns(
    bool isData, const std::string &rawPtColumn, const std::string &rhoBranch,
    const std::string &collectionPrefix, bool includePhi) {
  std::vector<std::string> inputs{
      collectionPrefix + "_area", collectionPrefix + "_eta", rawPtColumn,
      rhoBranch};
  if (includePhi) inputs.push_back(collectionPrefix + "_phi");
  if (isData) inputs.emplace_back("run");
  return inputs;
}

std::vector<std::string> detail::jecCompoundInputColumns(
    const std::vector<std::string> &schemaInputs, const std::string &rawPtColumn,
    const std::string &rhoBranch, const std::string &collectionPrefix) {
  std::vector<std::string> columns;
  columns.reserve(schemaInputs.size());
  for (const auto &name : schemaInputs) {
    if (name == "JetA" || name == "JetArea") columns.push_back(collectionPrefix + "_area");
    else if (name == "JetEta") columns.push_back(collectionPrefix + "_eta");
    else if (name == "JetPt") columns.push_back(rawPtColumn);
    else if (name == "Rho") columns.push_back(rhoBranch);
    else if (name == "JetPhi") columns.push_back(collectionPrefix + "_phi");
    else if (name == "run") columns.push_back("run");
    else throw std::runtime_error("CmsCorrections: unsupported JEC compound input '" + name + "'");
  }
  return columns;
}

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
      const bool hasDimuonLegPayload =
          !weightCfg->dimuonTriggerMu17File.empty() ||
          !weightCfg->dimuonTriggerMu8File.empty();
      if (hasDimuonLegPayload) {
        check("trigger", weightCfg->dimuonTriggerMu17File,
              "Mu17 dimuon trigger SF payload is unavailable.",
              "Set dimuonTriggerMu17File and its correction name.");
        check("trigger", weightCfg->dimuonTriggerMu8File,
              "Mu8 dimuon trigger SF payload is unavailable.",
              "Set dimuonTriggerMu8File and its correction name.");
        if (weightCfg->dimuonTriggerMu17Correction.empty()) {
          audit.add("trigger", "correction_name_missing",
                    "No Mu17 trigger SF correction name is configured.",
                    "Set dimuonTriggerMu17Correction.");
        }
        if (weightCfg->dimuonTriggerMu8Correction.empty()) {
          audit.add("trigger", "correction_name_missing",
                    "No Mu8 trigger SF correction name is configured.",
                    "Set dimuonTriggerMu8Correction.");
        }
      } else {
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
  if (cfg.applyFatJetCorrections) {
    JetEnergyScaleManager::create(an, "fatJetEnergyScaleManager");
    CorrectedFatJetCollectionManager::create(
        an, "correctedFatJetCollectionManager", "fatJetEnergyScaleManager");
  }
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

  if (cfg.applyFatJetCorrections && !isData) {
    if (!hasColumn(an, "FatJet_genJetAK8Idx_int") ||
        !hasColumn(an, "GenJetAK8_pt")) {
      throw std::runtime_error(
          "CmsCorrections: AK8 JER requires FatJet_genJetAK8Idx and "
          "GenJetAK8_pt branches.");
    }
    an.Define("FatJet_genMatchedPt",
              [](const ROOT::VecOps::RVec<Int_t> &genJetIdx,
                 const ROOT::VecOps::RVec<float> &genJetPt) {
                ROOT::VecOps::RVec<float> matched(genJetIdx.size(), -1.0f);
                for (size_t i = 0; i < genJetIdx.size(); ++i) {
                  const Int_t idx = genJetIdx[i];
                  if (idx >= 0 && static_cast<size_t>(idx) < genJetPt.size()) {
                    matched[i] = genJetPt[static_cast<size_t>(idx)];
                  }
                }
                return matched;
              },
              {"FatJet_genJetAK8Idx_int", "GenJetAK8_pt"});
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
  setWorkflowConfigValue(config, prefix + "ApplyFatJetCorrections",
                         cfg.applyFatJetCorrections ? "true" : "false");
  setWorkflowConfigValue(config, prefix + "ApplyElectronCorrections",
                         cfg.applyElectronCorrections ? "true" : "false");
  setWorkflowConfigValue(config, prefix + "ApplyMuonCorrections",
                         cfg.applyMuonCorrections ? "true" : "false");

  // -- JME file and correction names --
  setWorkflowConfigValue(config, prefix + "JmeFile", ys.jmeFile);
  setWorkflowConfigValue(config, prefix + "RhoBranch", ys.rhoBranch);

  const std::string jecCompound = cfg.jesCompoundCorrectionOverride.empty()
                                      ? detail::jecCompoundForSample(
                                            ys.jecCompoundCorrection, isData)
                                      : cfg.jesCompoundCorrectionOverride;
  setWorkflowConfigValue(config, prefix + "JecCompoundCorrection", jecCompound);

  // JER corrections (always needed for JER smearing, regardless of JES mode)
  setWorkflowConfigValue(config, prefix + "JerPtResolutionCorrection",
                         ys.jerPtResolutionCorrection);
  setWorkflowConfigValue(config, prefix + "JerScaleFactorCorrection",
                         ys.jerScaleFactorCorrection);
  setWorkflowConfigValue(config, prefix + "JerScaleFactorInputs",
                         joinCsv(ys.jerScaleFactorInputs));
  setWorkflowConfigValue(config, prefix + "JerScaleFactorUncertaintyCorrection",
                         ys.jerScaleFactorUncertaintyCorrection);
  setWorkflowConfigValue(config, prefix + "JerScaleFactorUncertaintyInputs",
                         joinCsv(ys.jerScaleFactorUncertaintyInputs));
  const std::string fatJecCompound = detail::jecCompoundForSample(
      ys.fatJetJecCompoundCorrection, isData);
  setWorkflowConfigValue(config, prefix + "FatJetJmeFile", ys.fatJetJmeFile);
  setWorkflowConfigValue(config, prefix + "FatJetJecCompoundCorrection",
                         fatJecCompound);
  setWorkflowConfigValue(config, prefix + "FatJetJesTotalCorrection",
                         ys.fatJetJesTotalCorrection);
  setWorkflowConfigValue(config, prefix + "FatJetJerPtResolutionCorrection",
                         ys.fatJetJerPtResolutionCorrection);
  setWorkflowConfigValue(config, prefix + "FatJetJerScaleFactorCorrection",
                         ys.fatJetJerScaleFactorCorrection);
  setWorkflowConfigValue(config, prefix + "FatJetJerScaleFactorInputs",
                         joinCsv(ys.fatJetJerScaleFactorInputs));
  setWorkflowConfigValue(
      config, prefix + "FatJetJerScaleFactorUncertaintyCorrection",
      ys.fatJetJerScaleFactorUncertaintyCorrection);
  setWorkflowConfigValue(
      config, prefix + "FatJetJerScaleFactorUncertaintyInputs",
      joinCsv(ys.fatJetJerScaleFactorUncertaintyInputs));

  // -- Electron scale/smearing --
  setWorkflowConfigValue(config, prefix + "ElectronScaleSmearingFile",
                         ys.electronScaleSmearingFile);
  setWorkflowConfigValue(config, prefix + "ElectronScaleCorrection",
                         ys.electronScaleCorrection);
  setWorkflowConfigValue(config, prefix + "ElectronSmearingCorrection",
                         ys.electronSmearingCorrection);

  configureElectronScaleSmearing(an, cfg, ys, isData);

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
    if (cfg.applyLeptonScaleSmearSystematics &&
        an.getSystematicManager().isEnabled()) {
      if (!isData) {
        muonMgr->applyScaleAndResolution(
            ys.muonScaleResolutionFile, false, "Muon_pt", "Muon_pt_scale_up",
            "up", "nom");
        muonMgr->applyScaleAndResolution(
            ys.muonScaleResolutionFile, false, "Muon_pt", "Muon_pt_scale_down",
            "down", "nom");
        muonMgr->applyScaleAndResolution(
            ys.muonScaleResolutionFile, false, "Muon_pt", "Muon_pt_smear_up",
            "nom", "up");
        muonMgr->applyScaleAndResolution(
            ys.muonScaleResolutionFile, false, "Muon_pt", "Muon_pt_smear_down",
            "nom", "down");
        muonMgr->addVariation("MuonScale", "Muon_pt_scale_up",
                              "Muon_pt_scale_down");
        muonMgr->addVariation("MuonSmear", "Muon_pt_smear_up",
                              "Muon_pt_smear_down");
        muonMgr->execute();
      } else {
        an.Define("Muon_pt_scale_up",
                  [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                  {"Muon_pt_corr_nominal"})
            ->Define("Muon_pt_scale_down",
                     [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                     {"Muon_pt_corr_nominal"})
            ->Define("Muon_pt_smear_up",
                     [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                     {"Muon_pt_corr_nominal"})
            ->Define("Muon_pt_smear_down",
                     [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                     {"Muon_pt_corr_nominal"});
      }
      an.getSystematicManager().registerVariationColumns(
          "Muon_pt_corr_nominal", "MuonScale", "Muon_pt_scale_up",
          "Muon_pt_scale_down");
      an.getSystematicManager().registerVariationColumns(
          "Muon_pt_corr_nominal", "MuonSmear", "Muon_pt_smear_up",
          "Muon_pt_smear_down");
    }
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

  const std::string jecCorrectionName = prefix + "_jec_nominal";
  // Register without default columns: the explicit inputs below follow the
  // payload schema, which differs between campaigns (notably JetPhi in 2024).
  corrMgr->registerCorrection(jecCorrectionName, ys.jmeFile, jecCompound, {});
  std::vector<std::string> jecSchema;
  for (const auto &input : corrMgr->getCompoundCorrection(jecCorrectionName)->inputs()) {
    jecSchema.push_back(input.name());
  }
  const auto jecInputs = detail::jecCompoundInputColumns(
      jecSchema, rawPtColumn, ys.rhoBranch);
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
    const bool includeJerSystematics = cfg.applyJerSystematics &&
                                       an.getSystematicManager().isEnabled();
    if (includeJerSystematics) {
      // Older payloads select nom/up/down via a string. Modern Run 3
      // payloads store the central SF and its absolute uncertainty separately.
      const auto sfCorrection = corrMgr->getCorrection(jerScaleFactorName);
      const bool hasVariationInput = std::any_of(
          sfCorrection->inputs().begin(), sfCorrection->inputs().end(),
          [](const auto &input) { return input.typeStr() == "string"; });
      std::string uncertaintyName;
      if (!hasVariationInput) {
        if (ys.jerScaleFactorUncertaintyCorrection.empty()) {
          throw std::runtime_error(
              "CmsCorrections: JER SF has no variation input; configure "
              "jerScaleFactorUncertaintyCorrection to avoid identical JER variations.");
        }
        uncertaintyName = prefix + "_jer_scale_factor_uncertainty";
        corrMgr->registerCorrection(
            uncertaintyName, ys.jmeFile,
            ys.jerScaleFactorUncertaintyCorrection,
            ys.jerScaleFactorUncertaintyInputs);
      }
      for (const auto &direction : {std::string("up"), std::string("down")}) {
        jesMgr->applyJERSmearing(
            *corrMgr, jerResolutionName, jerScaleFactorName,
            "Jet_pt_corr_jec", "Jet_pt_jer_" + direction,
            hasVariationInput ? direction : "nom", true,
            "Jet_mass_corr_jec", "Jet_mass_jer_" + direction,
            jerResolutionInputs, ys.jerScaleFactorInputs, uncertaintyName,
            ys.jerScaleFactorUncertaintyInputs, direction == "up" ? 1.0f : -1.0f);
      }
      jesMgr->addVariation("jer", "Jet_pt_jer_up", "Jet_pt_jer_down",
                           "Jet_mass_jer_up", "Jet_mass_jer_down");
      an.getSystematicManager().registerVariationColumns(
          "Jet_pt_corr_nominal", "jer", "Jet_pt_jer_up", "Jet_pt_jer_down");
      an.getSystematicManager().registerVariationColumns(
          "Jet_mass_corr_nominal", "jer", "Jet_mass_jer_up",
          "Jet_mass_jer_down");
    }
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
  if (!isData && cfg.applyMetType1 && cfg.applyJerSystematics &&
      an.getSystematicManager().isEnabled()) {
    for (const auto &direction : {std::string("up"), std::string("down")}) {
      const auto metPt = "PuppiMET_pt_jer_" + direction;
      const auto metPhi = "PuppiMET_phi_jer_" + direction;
      jesMgr->propagateMET(
          "PuppiMET_pt_corr_nominal", "PuppiMET_phi_corr_nominal",
          "Jet_pt_corr_nominal", "Jet_pt_jer_" + direction, metPt, metPhi);
    }
    an.getSystematicManager().registerVariationColumns(
        "PuppiMET_pt_corr_nominal", "jer", "PuppiMET_pt_jer_up",
        "PuppiMET_pt_jer_down");
    an.getSystematicManager().registerVariationColumns(
        "PuppiMET_phi_corr_nominal", "jer", "PuppiMET_phi_jer_up",
        "PuppiMET_phi_jer_down");
  }

  JetEnergyScaleManager *fatJesMgr = nullptr;
  if (cfg.applyFatJetCorrections) {
    if (ys.fatJetJmeFile.empty() || ys.fatJetJecCompoundCorrection.empty()) {
      throw std::runtime_error(
          "CmsCorrections: AK8 corrections are enabled, but the fat-jet JME "
          "payload or compound JEC correction is not configured.");
    }
    auto fatJesPlugin = an.getPlugin<JetEnergyScaleManager>(
        "fatJetEnergyScaleManager");
    if (!fatJesPlugin) {
      throw std::runtime_error(
          "CmsCorrections: fatJetEnergyScaleManager is required when AK8 "
          "corrections are enabled.");
    }
    fatJesMgr = fatJesPlugin.get();
    fatJesMgr->setJetColumns("FatJet_pt", "FatJet_eta", "FatJet_phi",
                             "FatJet_mass");
    fatJesMgr->removeExistingCorrections("FatJet_rawFactor");
    if (!isData) {
      fatJesMgr->setJERSmearingColumns("FatJet_genMatchedPt", ys.rhoBranch,
                                       "event");
    }

    const std::string fatJecName = prefix + "_fatjet_jec_nominal";
    corrMgr->registerCorrection(fatJecName, ys.fatJetJmeFile,
                                fatJecCompound, {});
    std::vector<std::string> fatJecSchema;
    for (const auto &input : corrMgr->getCompoundCorrection(fatJecName)->inputs()) {
      fatJecSchema.push_back(input.name());
    }
    const auto fatJecInputs = detail::jecCompoundInputColumns(
        fatJecSchema, "FatJet_pt_raw", ys.rhoBranch, "FatJet");
    fatJesMgr->applyCorrectionlib(
        *corrMgr, fatJecName, {}, "FatJet_pt_raw", "FatJet_pt_corr_jec", true,
        "FatJet_mass_raw", "FatJet_mass_corr_jec", fatJecInputs);

    // Make the JEC columns available before registering JER inputs that use
    // FatJet_pt_corr_jec. This also ensures the data nominal aliases below
    // reference a column already present in the dataframe.
    fatJesMgr->execute();

    if (!isData) {
      if (ys.fatJetJerPtResolutionCorrection.empty() ||
          ys.fatJetJerScaleFactorCorrection.empty() ||
          ys.fatJetJerScaleFactorInputs.empty()) {
        throw std::runtime_error(
            "CmsCorrections: AK8 MC corrections require both JER payload "
            "correction names and their scale-factor inputs.");
      }
      const std::string resolutionName = prefix + "_fatjet_jer_resolution";
      const std::string scaleFactorName = prefix + "_fatjet_jer_scale_factor";
      const std::vector<std::string> resolutionInputs{
          "FatJet_eta", "FatJet_pt_corr_jec", ys.rhoBranch};
      corrMgr->registerCorrection(resolutionName, ys.fatJetJmeFile,
                                  ys.fatJetJerPtResolutionCorrection,
                                  resolutionInputs);
      corrMgr->registerCorrection(scaleFactorName, ys.fatJetJmeFile,
                                  ys.fatJetJerScaleFactorCorrection,
                                  ys.fatJetJerScaleFactorInputs);
      fatJesMgr->applyJERSmearing(
          *corrMgr, resolutionName, scaleFactorName, "FatJet_pt_corr_jec",
          "FatJet_pt_corr_nominal", "nom", true, "FatJet_mass_corr_jec",
          "FatJet_mass_corr_nominal", resolutionInputs,
          ys.fatJetJerScaleFactorInputs);
      if (cfg.applyJerSystematics && an.getSystematicManager().isEnabled() &&
          !ys.fatJetJerScaleFactorUncertaintyCorrection.empty()) {
        const std::string uncertaintyName =
            prefix + "_fatjet_jer_scale_factor_uncertainty";
        corrMgr->registerCorrection(
            uncertaintyName, ys.fatJetJmeFile,
            ys.fatJetJerScaleFactorUncertaintyCorrection,
            ys.fatJetJerScaleFactorUncertaintyInputs);
        for (const auto &direction : {std::string("up"), std::string("down")}) {
          const float uncertaintyDirection = direction == "up" ? 1.0f : -1.0f;
          const std::string variedPt = "FatJet_pt_jer_" + direction;
          const std::string variedMass = "FatJet_mass_jer_" + direction;
          fatJesMgr->applyJERSmearing(
              *corrMgr, resolutionName, scaleFactorName,
              "FatJet_pt_corr_jec", variedPt, "nom", true,
              "FatJet_mass_corr_jec", variedMass, resolutionInputs,
              ys.fatJetJerScaleFactorInputs, uncertaintyName,
              ys.fatJetJerScaleFactorUncertaintyInputs, uncertaintyDirection);
        }
        fatJesMgr->addVariation("jer", "FatJet_pt_jer_up",
                                "FatJet_pt_jer_down", "FatJet_mass_jer_up",
                                "FatJet_mass_jer_down");
        an.getSystematicManager().registerVariationColumns(
            "FatJet_pt_corr_nominal", "jer", "FatJet_pt_jer_up",
            "FatJet_pt_jer_down");
        an.getSystematicManager().registerVariationColumns(
            "FatJet_mass_corr_nominal", "jer", "FatJet_mass_jer_up",
            "FatJet_mass_jer_down");
      }
    } else {
      an.Define("FatJet_pt_corr_nominal",
                [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
                {"FatJet_pt_corr_jec"});
      an.Define("FatJet_mass_corr_nominal",
                [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                {"FatJet_mass_corr_jec"});
    }
  }

  // Materialize the nominal AK8 JER result before JES variations are added
  // below, since those variations use FatJet_pt_corr_nominal as an input.
  if (fatJesMgr) fatJesMgr->execute();

  // Select and register the systematic source set based on mode
  // Nominal JEC/JER corrections have already been registered above.  Avoid
  // loading any JES uncertainty corrections when the global systematic
  // manager is disabled; those columns cannot be consumed in that mode.
  if (!an.getSystematicManager().isEnabled()) {
    // The nominal MET propagation was queued after the first execute() call
    // above, so flush it even when no JES variations will be registered.
    jesMgr->execute();
    if (fatJesMgr) fatJesMgr->execute();
    return;
  }

  std::vector<std::string> jesSources;
  if (cfg.applyJesSystematics) {
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
                                                          source,
                                                          cfg.jesMode == CmsCorrectionConfig::Full);
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

      if (fatJesMgr) {
        const std::string fatJesCorrectionName = prefix + "_fatjet_jes_" + source;
        const std::string fatJesCorrectionlibName =
            source == "Total"
                ? ys.fatJetJesTotalCorrection
                : jesCorrectionNameForSource(ys.fatJetJesTotalCorrection,
                                             source,
                                             cfg.jesMode == CmsCorrectionConfig::Full);
        if (fatJesCorrectionlibName.empty()) {
          throw std::runtime_error(
              "CmsCorrections: AK8 JES uncertainty correction is not "
              "configured for source '" + source + "'.");
        }
        const std::vector<std::string> fatJesInputs{
            "FatJet_eta", "FatJet_pt_corr_nominal"};
        corrMgr->registerCorrection(fatJesCorrectionName,
                                    ys.fatJetJmeFile,
                                    fatJesCorrectionlibName, fatJesInputs);
        const std::string fatUpPt = "FatJet_pt_jes_" + label + "_up";
        const std::string fatDownPt = "FatJet_pt_jes_" + label + "_down";
        const std::string fatUpMass = "FatJet_mass_jes_" + label + "_up";
        const std::string fatDownMass = "FatJet_mass_jes_" + label + "_down";
        fatJesMgr->applyCorrectionlib(
            *corrMgr, fatJesCorrectionName, {}, "FatJet_pt_corr_nominal",
            fatUpPt, true, "FatJet_mass_corr_nominal", fatUpMass,
            fatJesInputs, true, 1.0f);
        fatJesMgr->applyCorrectionlib(
            *corrMgr, fatJesCorrectionName, {}, "FatJet_pt_corr_nominal",
            fatDownPt, true, "FatJet_mass_corr_nominal", fatDownMass,
            fatJesInputs, true, -1.0f);
        fatJesMgr->addVariation(label, fatUpPt, fatDownPt, fatUpMass,
                                fatDownMass);
        an.getSystematicManager().registerVariationColumns(
            "FatJet_pt_corr_nominal", label, fatUpPt, fatDownPt);
        an.getSystematicManager().registerVariationColumns(
            "FatJet_mass_corr_nominal", label, fatUpMass, fatDownMass);
      }
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
  if (fatJesMgr) fatJesMgr->execute();
}

// ===========================================================================
// Correction fallback columns
// ===========================================================================

void defineCmsCorrectionFallbacks(Analyzer &an,
                                   const CmsCorrectionConfig &cfg) {
  if (!cfg.applyFatJetCorrections && hasColumn(an, "FatJet_pt")) {
    an.Define("FatJet_pt_corr_nominal",
              [](const ROOT::VecOps::RVec<float> &pt) { return pt; },
              {"FatJet_pt"});
    if (hasColumn(an, "FatJet_mass")) {
      an.Define("FatJet_mass_corr_nominal",
                [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                {"FatJet_mass"});
    }
  }
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
        ->Define("Electron_mass_corr_nominal",
                 [](const ROOT::VecOps::RVec<float> &mass) { return mass; },
                 {"Electron_mass"})
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
