#include <JetEnergyScaleManager.h>
#include <ROOT/RVec.hxx>
#include <api/ILogger.h>
#include <set>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// setContext
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

// ---------------------------------------------------------------------------
// Jet column configuration
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::setJetColumns(const std::string &ptColumn,
                                           const std::string &etaColumn,
                                           const std::string &phiColumn,
                                           const std::string &massColumn) {
  if (ptColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setJetColumns: ptColumn must not be empty");
  ptColumn_m = ptColumn;
  etaColumn_m = etaColumn;
  phiColumn_m = phiColumn;
  massColumn_m = massColumn;
}

// ---------------------------------------------------------------------------
// Removing existing corrections
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::removeExistingCorrections(
    const std::string &rawFactorColumn) {
  if (rawFactorColumn.empty())
    throw std::invalid_argument("JetEnergyScaleManager::removeExistingCorrections: "
                                "rawFactorColumn must not be empty");
  if (!rawPtColumn_m.empty())
    throw std::runtime_error("JetEnergyScaleManager::removeExistingCorrections: "
                             "raw pT column already set via setRawPtColumn()");
  rawFactorColumn_m = rawFactorColumn;
  rawPtColumn_m = ptColumn_m + "_raw";
  rawMassColumn_m = massColumn_m.empty() ? "" : massColumn_m + "_raw";
}

void JetEnergyScaleManager::setRawPtColumn(const std::string &rawPtColumn) {
  if (rawPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setRawPtColumn: rawPtColumn must not be empty");
  if (!rawFactorColumn_m.empty())
    throw std::runtime_error("JetEnergyScaleManager::setRawPtColumn: "
                             "removeExistingCorrections() was already called");
  rawPtColumn_m = rawPtColumn;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

std::string JetEnergyScaleManager::deriveMassColumnName(
    const std::string &ptColName) const {
  if (massColumn_m.empty() || ptColumn_m.empty())
    return "";
  // Replace the ptColumn_m prefix with massColumn_m in ptColName.
  if (ptColName.size() >= ptColumn_m.size() &&
      ptColName.substr(0, ptColumn_m.size()) == ptColumn_m) {
    return massColumn_m + ptColName.substr(ptColumn_m.size());
  }
  return "";
}

// ---------------------------------------------------------------------------
// Applying corrections
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::applyCorrection(
    const std::string &inputPtColumn, const std::string &sfColumn,
    const std::string &outputPtColumn, bool applyToMass,
    const std::string &inputMassColumn,
    const std::string &outputMassColumn) {
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyCorrection: inputPtColumn must not be empty");
  if (sfColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyCorrection: sfColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyCorrection: outputPtColumn must not be empty");

  CorrectionStep step;
  step.inputPtColumn = inputPtColumn;
  step.sfColumn = sfColumn;
  step.outputPtColumn = outputPtColumn;

  if (applyToMass) {
    step.inputMassColumn = inputMassColumn.empty()
                               ? deriveMassColumnName(inputPtColumn)
                               : inputMassColumn;
    step.outputMassColumn = outputMassColumn.empty()
                                ? deriveMassColumnName(outputPtColumn)
                                : outputMassColumn;
  }

  correctionSteps_m.push_back(std::move(step));
}

void JetEnergyScaleManager::applyCorrectionlib(
    CorrectionManager &cm, const std::string &correctionName,
    const std::vector<std::string> &stringArgs,
    const std::string &inputPtColumn, const std::string &outputPtColumn,
    bool applyToMass, const std::string &inputMassColumn,
    const std::string &outputMassColumn,
    const std::vector<std::string> &inputColumns) {
  // Ask the CorrectionManager to evaluate the correctionlib formula and store
  // per-jet scale factors in a new RVec<Float_t> column.
  cm.applyCorrectionVec(correctionName, stringArgs, inputColumns);

  // The SF column name is derived by the same convention used in
  // CorrectionManager::applyCorrectionVec() / makeBranchName():
  //   correctionName + "_" + stringArgs[0] + "_" + stringArgs[1] + ...
  std::string sfColumn = correctionName;
  for (const auto &arg : stringArgs)
    sfColumn += "_" + arg;

  // Schedule the element-wise pT (and mass) multiplication for execute().
  applyCorrection(inputPtColumn, sfColumn, outputPtColumn, applyToMass,
                  inputMassColumn, outputMassColumn);
}

// ---------------------------------------------------------------------------
// Systematic variation registration
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::addVariation(const std::string &systematicName,
                                          const std::string &upPtColumn,
                                          const std::string &downPtColumn,
                                          const std::string &upMassColumn,
                                          const std::string &downMassColumn) {
  if (systematicName.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::addVariation: systematicName must not be empty");
  if (upPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::addVariation: upPtColumn must not be empty");
  if (downPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::addVariation: downPtColumn must not be empty");

  JESVariationEntry entry;
  entry.name = systematicName;
  entry.upPtColumn = upPtColumn;
  entry.downPtColumn = downPtColumn;
  entry.upMassColumn = upMassColumn;
  entry.downMassColumn = downMassColumn;
  variations_m.push_back(std::move(entry));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const std::string &JetEnergyScaleManager::getRawPtColumn() const {
  return rawPtColumn_m;
}

const std::string &JetEnergyScaleManager::getPtColumn() const {
  return ptColumn_m;
}

const std::string &JetEnergyScaleManager::getMassColumn() const {
  return massColumn_m;
}

const std::vector<JESVariationEntry> &
JetEnergyScaleManager::getVariations() const {
  return variations_m;
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error("JetEnergyScaleManager::execute: context not set");

  // 1. Define raw-pT and raw-mass columns (if removeExistingCorrections was called).
  if (!rawFactorColumn_m.empty()) {
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string ptCol = ptColumn_m;
      const std::string rawFactor = rawFactorColumn_m;
      const std::string rawPtCol = rawPtColumn_m;
      auto newDf = df.Define(
          rawPtCol,
          [](const ROOT::VecOps::RVec<Float_t> &pt,
             const ROOT::VecOps::RVec<Float_t> &rawFactor) {
            return pt * (1.0f - rawFactor);
          },
          {ptCol, rawFactor});
      dataManager_m->setDataFrame(newDf);
    }
    if (!massColumn_m.empty() && !rawMassColumn_m.empty()) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string massCol = massColumn_m;
      const std::string rawFactor = rawFactorColumn_m;
      const std::string rawMassCol = rawMassColumn_m;
      auto newDf = df.Define(
          rawMassCol,
          [](const ROOT::VecOps::RVec<Float_t> &mass,
             const ROOT::VecOps::RVec<Float_t> &rawFactor) {
            return mass * (1.0f - rawFactor);
          },
          {massCol, rawFactor});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // 2. Apply each registered correction step.
  for (const auto &step : correctionSteps_m) {
    // Corrected pT.
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputPt = step.inputPtColumn;
      const std::string sf = step.sfColumn;
      const std::string outputPt = step.outputPtColumn;
      auto newDf = df.Define(
          outputPt,
          [](const ROOT::VecOps::RVec<Float_t> &pt,
             const ROOT::VecOps::RVec<Float_t> &sf) { return pt * sf; },
          {inputPt, sf});
      dataManager_m->setDataFrame(newDf);
    }
    // Corrected mass (if applicable).
    if (!step.inputMassColumn.empty() && !step.outputMassColumn.empty()) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputMass = step.inputMassColumn;
      const std::string sf = step.sfColumn;
      const std::string outputMass = step.outputMassColumn;
      auto newDf = df.Define(
          outputMass,
          [](const ROOT::VecOps::RVec<Float_t> &mass,
             const ROOT::VecOps::RVec<Float_t> &sf) { return mass * sf; },
          {inputMass, sf});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // 3. Register systematic variations with the ISystematicManager.
  for (const auto &var : variations_m) {
    {
      std::set<std::string> affectedUp = {var.upPtColumn};
      if (!var.upMassColumn.empty())
        affectedUp.insert(var.upMassColumn);
      systematicManager_m->registerSystematic(var.name + "Up", affectedUp);
    }
    {
      std::set<std::string> affectedDown = {var.downPtColumn};
      if (!var.downMassColumn.empty())
        affectedDown.insert(var.downMassColumn);
      systematicManager_m->registerSystematic(var.name + "Down",
                                               affectedDown);
    }
  }
}

// ---------------------------------------------------------------------------
// reportMetadata()
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::reportMetadata() {
  if (!logger_m)
    return;

  std::ostringstream ss;
  ss << "JetEnergyScaleManager: configuration summary\n";

  if (!ptColumn_m.empty()) {
    ss << "  Jet columns:"
       << " pt="   << ptColumn_m
       << " eta="  << etaColumn_m
       << " phi="  << phiColumn_m
       << " mass=" << massColumn_m << "\n";
  }

  if (!rawFactorColumn_m.empty()) {
    ss << "  Raw pT: " << rawPtColumn_m
       << " (stripped via raw-factor column \"" << rawFactorColumn_m << "\")\n";
    if (!rawMassColumn_m.empty())
      ss << "  Raw mass: " << rawMassColumn_m << "\n";
  } else if (!rawPtColumn_m.empty()) {
    ss << "  Raw pT: " << rawPtColumn_m << " (provided directly)\n";
  }

  if (!correctionSteps_m.empty()) {
    ss << "  Correction steps (" << correctionSteps_m.size() << "):\n";
    for (const auto &step : correctionSteps_m) {
      ss << "    " << step.inputPtColumn
         << " × " << step.sfColumn
         << " → "  << step.outputPtColumn;
      if (!step.outputMassColumn.empty())
        ss << "  (mass: " << step.inputMassColumn
           << " → " << step.outputMassColumn << ")";
      ss << "\n";
    }
  }

  if (!variations_m.empty()) {
    ss << "  Systematic variations (" << variations_m.size() << "):\n";
    for (const auto &var : variations_m) {
      ss << "    " << var.name
         << ": up="   << var.upPtColumn
         << "  down=" << var.downPtColumn;
      if (!var.upMassColumn.empty())
        ss << "  (mass up=" << var.upMassColumn
           << " down=" << var.downMassColumn << ")";
      ss << "\n";
    }
  }

  logger_m->log(ILogger::Level::Info, ss.str());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
JetEnergyScaleManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  if (!ptColumn_m.empty()) {
    entries["jet_pt_column"] = ptColumn_m;
    entries["jet_eta_column"] = etaColumn_m;
    entries["jet_phi_column"] = phiColumn_m;
    entries["jet_mass_column"] = massColumn_m;
  }

  if (!rawFactorColumn_m.empty()) {
    entries["raw_factor_column"] = rawFactorColumn_m;
    entries["raw_pt_column"] = rawPtColumn_m;
  } else if (!rawPtColumn_m.empty()) {
    entries["raw_pt_column"] = rawPtColumn_m;
  }

  if (!correctionSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < correctionSteps_m.size(); ++i) {
      if (i > 0)
        ss << ',';
      ss << correctionSteps_m[i].inputPtColumn
         << "->" << correctionSteps_m[i].outputPtColumn
         << "(sf:" << correctionSteps_m[i].sfColumn << ')';
    }
    entries["correction_steps"] = ss.str();
  }

  if (!variations_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < variations_m.size(); ++i) {
      if (i > 0)
        ss << ',';
      ss << variations_m[i].name
         << "(up:" << variations_m[i].upPtColumn
         << ",dn:" << variations_m[i].downPtColumn << ')';
    }
    entries["variations"] = ss.str();
  }

  return entries;
}
