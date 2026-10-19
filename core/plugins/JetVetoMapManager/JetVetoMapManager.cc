//==============================================================================
// JetVetoMapManager — CMS JetVetoMap event filter
//
// Implements the CMS JetVetoMap recommendations using correctionlib JSON
// payloads.  The map identifies (eta, phi) regions with detector issues;
// events with jets in those regions are vetoed.
//
// The maps are centrally produced by the JetMET POG and stored under:
//   /cvmfs/cms-griddata.cern.ch/cat/metadata/JME/<era>/latest/jetvetomaps.json.gz
//==============================================================================

#include "JetVetoMapManager.h"
#include "analyzer.h"
#include "api/ILogger.h"

#include <ROOT/RVec.hxx>
#include <RtypesCore.h>

#include <correction.h>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <fstream>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

std::shared_ptr<JetVetoMapManager> JetVetoMapManager::create(
    Analyzer& an, const std::string& role) {
  auto plugin = std::make_shared<JetVetoMapManager>();
  an.addPlugin(role, plugin);
  return plugin;
}

// ---------------------------------------------------------------------------
// IContextAware
// ---------------------------------------------------------------------------

void JetVetoMapManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

void JetVetoMapManager::loadConfigFile(const std::string &path) {
  const auto isJsonPayload = [&path](const std::string &suffix) {
    return path.size() >= suffix.size() &&
           path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0;
  };
  if (isJsonPayload(".json") || isJsonPayload(".json.gz")) {
    vetoMapFiles_m.push_back(path);
    return;
  }

  std::ifstream input(path);
  if (!input) {
    throw std::runtime_error(
        "JetVetoMapManager: cannot open config file '" + path + "'.");
  }

  std::string line;
  while (std::getline(input, line)) {
    // Strip comments
    const auto commentPos = line.find('#');
    const std::string trimmed = (commentPos != std::string::npos)
                                    ? line.substr(0, commentPos)
                                    : line;
    if (trimmed.empty()) continue;

    // Trim whitespace
    const auto first = trimmed.find_first_not_of(" \t");
    const auto last = trimmed.find_last_not_of(" \t");
    if (first == std::string::npos) continue;
    const std::string path = trimmed.substr(first, last - first + 1);

    vetoMapFiles_m.push_back(path);
  }
}

void JetVetoMapManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("JetVetoMapManager: ConfigManager not set");
  }

  try {
    const std::string configPath = configManager_m->get("jetVetoMapConfig");
    if (configPath.empty()) {
      if (logger_m) {
        logger_m->log(ILogger::Level::Warn,
                      "JetVetoMapManager: 'jetVetoMapConfig' key is empty. "
                      "No JetVetoMap filtering will be applied.");
      }
      return;
    }
    loadConfigFile(configPath);

    if (vetoMapFiles_m.empty()) {
      if (logger_m) {
        logger_m->log(ILogger::Level::Warn,
                      "JetVetoMapManager: no JetVetoMap files loaded from '" +
                      configPath + "'.");
      }
      return;
    }

    active_m = true;
    if (logger_m) {
      logger_m->log(ILogger::Level::Info,
                    "JetVetoMapManager: loaded " +
                    std::to_string(vetoMapFiles_m.size()) +
                    " JetVetoMap file(s).");
    }
  } catch (const std::exception &e) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Warn,
                    std::string("JetVetoMapManager: ") + e.what());
    }
  }

  // Optional type override
  try {
    const std::string typeOverride = configManager_m->get("jetVetoMapType");
    if (!typeOverride.empty()) vetoMapType_m = typeOverride;
  } catch (...) {}

  // Optional jet-ID column override, used when NanoAOD does not store the
  // Jet_jetId branch and the analysis constructs the recommended ID itself.
  try {
    const std::string jetIdColumn =
        configManager_m->get("jetVetoMapJetIdColumn");
    if (!jetIdColumn.empty()) jetIdColumn_m = jetIdColumn;
  } catch (...) {}
}

void JetVetoMapManager::initialize() {
  if (!active_m) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Info,
                    "JetVetoMapManager: not active — no filtering.");
    }
    return;
  }

  if (logger_m) {
    logger_m->log(ILogger::Level::Info,
                  "JetVetoMapManager: active with " +
                  std::to_string(vetoMapFiles_m.size()) +
                  " map file(s), type='" + vetoMapType_m + "'.");
  }
}

// ===========================================================================
// Main filter: applyJetVetoMap
// ===========================================================================

void JetVetoMapManager::applyJetVetoMap() {
  if (!dataManager_m || !configManager_m) {
    throw std::runtime_error(
        "JetVetoMapManager: DataManager or ConfigManager not set");
  }

  if (!active_m || vetoMapFiles_m.empty()) {
    // Define pass-through column and no filter
    auto df = dataManager_m->getDataFrame();
    const auto cols = df.GetColumnNames();
    if (std::find(cols.begin(), cols.end(), "jet_veto_pass") == cols.end()) {
      dataManager_m->setDataFrame(df.Define("jet_veto_pass", []() { return true; }));
    }
    return;
  }

  // ---- Define the JetVetoMap filter column ----
  //
  // For each jet in the event, check if it falls in a vetoed region.
  // Jet selection criteria (CMS recommendations):
  //   - pT > 15 GeV
  //   - tightLepVeto jet ID (bit 2 set: Jet_jetId_i & 4)
  //   - (charged EM fraction + neutral EM fraction) < 0.9
  //
  // For Run 2, also check PU jet ID for pT < 50 GeV and muon overlap.
  //
  // The correctionlib map returns non-zero for vetoed (eta, phi).
  //
  // We evaluate the first loaded map file using correctionlib directly
  // since CorrectionManager's applyCorrection is per-event scalar.

  const std::string &mapFile = vetoMapFiles_m[0];
  const std::string vetoType = vetoMapType_m;

  // Load the correction set
  auto corrSet = correction::CorrectionSet::from_file(mapFile);
  // Capture in shared_ptr to keep the correction set alive across invocations
  auto sharedSet = std::shared_ptr<const correction::CorrectionSet>(std::move(corrSet));
  if (sharedSet->size() == 0) {
    throw std::runtime_error(
        "JetVetoMapManager: payload contains no corrections: " + mapFile);
  }
  const auto mapCorrection = sharedSet->begin()->second;

  auto df = dataManager_m->getDataFrame();
  const auto cols = df.GetColumnNames();

  // Check if required columns exist; if not, define pass-through
  const bool hasJetId =
      std::find(cols.begin(), cols.end(), jetIdColumn_m) != cols.end();
  bool hasChEm = std::find(cols.begin(), cols.end(), "Jet_chEmEF") != cols.end();
  bool hasNeEm = std::find(cols.begin(), cols.end(), "Jet_neEmEF") != cols.end();

  if (!hasJetId || !hasChEm || !hasNeEm) {
    // Missing columns — define pass-through
    if (std::find(cols.begin(), cols.end(), "jet_veto_pass") == cols.end()) {
      dataManager_m->setDataFrame(
          df.Define("jet_veto_pass", []() { return true; }));
    }
    if (logger_m) {
      logger_m->log(ILogger::Level::Warn,
                    "JetVetoMapManager: required columns missing "
                    "(" + jetIdColumn_m + ", Jet_chEmEF, Jet_neEmEF). "
                    "Skipping JetVetoMap filter.");
    }
    return;
  }

  if (std::find(cols.begin(), cols.end(), "jet_veto_pass") == cols.end()) {
    const std::vector<std::string> mapColumns{
        "Jet_pt", "Jet_eta", "Jet_phi", jetIdColumn_m,
        "Jet_chEmEF", "Jet_neEmEF"};
    df = df.Define("jet_veto_pass",
                   [mapCorrection, vetoType](const ROOT::VecOps::RVec<float> &jetPt,
                                         const ROOT::VecOps::RVec<float> &jetEta,
                                         const ROOT::VecOps::RVec<float> &jetPhi,
                                         const ROOT::VecOps::RVec<int> &jetId,
                                         const ROOT::VecOps::RVec<float> &chEmEF,
                                         const ROOT::VecOps::RVec<float> &neEmEF) {
                     for (size_t i = 0; i < jetPt.size(); ++i) {
                       // Minimal selection (CMS recommendations)
                       if (jetPt[i] <= 15.0f) continue;
                       if (!(jetId[i] & 4)) continue;  // tightLepVeto
                       if ((chEmEF[i] + neEmEF[i]) >= 0.9f) continue;

                       const double veto = mapCorrection->evaluate(
                           {vetoType, static_cast<double>(jetEta[i]),
                            static_cast<double>(jetPhi[i])});
                       if (std::fabs(veto) > 1.0e-6) {
                         return false;  // vetoed
                       }
                     }
                     return true;  // pass
                   },
                   mapColumns);
  }

  dataManager_m->setDataFrame(df.Filter("jet_veto_pass"));
}

// ===========================================================================
// Metadata reporting
// ===========================================================================

void JetVetoMapManager::reportMetadata() {
  if (!logger_m) return;
  if (active_m) {
    logger_m->log(ILogger::Level::Info,
                  "JetVetoMapManager: active with " +
                  std::to_string(vetoMapFiles_m.size()) + " map file(s).");
  } else {
    logger_m->log(ILogger::Level::Info,
                  "JetVetoMapManager: not active — no filtering.");
  }
}
