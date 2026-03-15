#include <ElectronEnergyScaleManager.h>
#include <ROOT/RVec.hxx>
#include <api/ILogger.h>
#include <set>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// setContext
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

// ---------------------------------------------------------------------------
// Object column configuration
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::setObjectColumns(
    const std::string &ptColumn, const std::string &etaColumn,
    const std::string &phiColumn, const std::string &massColumn) {
  if (ptColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::setObjectColumns: ptColumn must not be empty");
  ptColumn_m = ptColumn;
  etaColumn_m = etaColumn;
  phiColumn_m = phiColumn;
  massColumn_m = massColumn;
}

// ---------------------------------------------------------------------------
// Applying corrections
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::applyCorrection(
    const std::string &inputPtColumn, const std::string &sfColumn,
    const std::string &outputPtColumn) {
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applyCorrection: inputPtColumn must not be empty");
  if (sfColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applyCorrection: sfColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applyCorrection: outputPtColumn must not be empty");

  CorrectionStep step;
  step.inputPtColumn = inputPtColumn;
  step.sfColumn = sfColumn;
  step.outputPtColumn = outputPtColumn;
  correctionSteps_m.push_back(std::move(step));
}

void ElectronEnergyScaleManager::applyCorrectionlib(
    CorrectionManager &cm, const std::string &correctionName,
    const std::vector<std::string> &stringArgs,
    const std::string &inputPtColumn, const std::string &outputPtColumn,
    const std::vector<std::string> &inputColumns) {
  cm.applyCorrectionVec(correctionName, stringArgs, inputColumns);

  std::string sfColumn = correctionName;
  for (const auto &arg : stringArgs)
    sfColumn += "_" + arg;

  applyCorrection(inputPtColumn, sfColumn, outputPtColumn);
}

// ---------------------------------------------------------------------------
// Systematic source sets
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::registerSystematicSources(
    const std::string &setName, const std::vector<std::string> &sources) {
  if (setName.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::registerSystematicSources: setName must not be empty");
  if (sources.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::registerSystematicSources: sources must not be empty");
  for (const auto &s : sources) {
    if (s.empty())
      throw std::invalid_argument(
          "ElectronEnergyScaleManager::registerSystematicSources: "
          "source names must not be empty");
  }
  systematicSets_m[setName] = sources;
}

const std::vector<std::string> &
ElectronEnergyScaleManager::getSystematicSources(
    const std::string &setName) const {
  auto it = systematicSets_m.find(setName);
  if (it == systematicSets_m.end())
    throw std::out_of_range(
        "ElectronEnergyScaleManager::getSystematicSources: set \"" + setName +
        "\" is not registered");
  return it->second;
}

void ElectronEnergyScaleManager::applySystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName, const std::string &inputPtColumn,
    const std::string &outputPtPrefix,
    const std::vector<std::string> &inputColumns) {
  if (correctionName.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applySystematicSet: correctionName must not be empty");
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applySystematicSet: inputPtColumn must not be empty");
  if (outputPtPrefix.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::applySystematicSet: outputPtPrefix must not be empty");

  const auto &sources = getSystematicSources(setName);

  for (const auto &source : sources) {
    const std::string upPtCol = outputPtPrefix + "_" + source + "_up";
    const std::string dnPtCol = outputPtPrefix + "_" + source + "_down";

    const std::string sfUpCol = correctionName + "_" + source + "_up";
    const std::string sfDnCol = correctionName + "_" + source + "_down";
    cm.applyCorrectionVec(correctionName, {source, "up"}, inputColumns, sfUpCol);
    cm.applyCorrectionVec(correctionName, {source, "down"}, inputColumns, sfDnCol);

    applyCorrection(inputPtColumn, sfUpCol, upPtCol);
    applyCorrection(inputPtColumn, sfDnCol, dnPtCol);

    addVariation(source, upPtCol, dnPtCol);
  }
}

// ---------------------------------------------------------------------------
// Direct variation registration
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::addVariation(const std::string &systematicName,
                                               const std::string &upPtColumn,
                                               const std::string &downPtColumn) {
  if (systematicName.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::addVariation: systematicName must not be empty");
  if (upPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::addVariation: upPtColumn must not be empty");
  if (downPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::addVariation: downPtColumn must not be empty");

  EESVariationEntry entry;
  entry.name = systematicName;
  entry.upPtColumn = upPtColumn;
  entry.downPtColumn = downPtColumn;
  variations_m.push_back(std::move(entry));
}

// ---------------------------------------------------------------------------
// PhysicsObjectCollection integration
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::setInputCollection(
    const std::string &collectionColumn) {
  if (collectionColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::setInputCollection: "
        "collectionColumn must not be empty");
  inputCollectionColumn_m = collectionColumn;
}

void ElectronEnergyScaleManager::defineCollectionOutput(
    const std::string &correctedPtColumn,
    const std::string &outputCollectionColumn) {
  if (correctedPtColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::defineCollectionOutput: "
        "correctedPtColumn must not be empty");
  if (outputCollectionColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::defineCollectionOutput: "
        "outputCollectionColumn must not be empty");
  if (inputCollectionColumn_m.empty())
    throw std::runtime_error(
        "ElectronEnergyScaleManager::defineCollectionOutput: "
        "call setInputCollection() before defineCollectionOutput()");

  CollectionOutputStep step;
  step.correctedPtColumn = correctedPtColumn;
  step.outputCollectionColumn = outputCollectionColumn;
  collectionOutputSteps_m.push_back(std::move(step));
}

void ElectronEnergyScaleManager::defineVariationCollections(
    const std::string &nominalCollectionColumn,
    const std::string &collectionPrefix,
    const std::string &variationMapColumn) {
  if (nominalCollectionColumn.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::defineVariationCollections: "
        "nominalCollectionColumn must not be empty");
  if (collectionPrefix.empty())
    throw std::invalid_argument(
        "ElectronEnergyScaleManager::defineVariationCollections: "
        "collectionPrefix must not be empty");
  if (inputCollectionColumn_m.empty())
    throw std::runtime_error(
        "ElectronEnergyScaleManager::defineVariationCollections: "
        "call setInputCollection() before defineVariationCollections()");

  VariationCollectionsStep step;
  step.nominalCollectionColumn = nominalCollectionColumn;
  step.collectionPrefix = collectionPrefix;
  step.variationMapColumn = variationMapColumn;
  variationCollectionsSteps_m.push_back(std::move(step));
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const std::string &ElectronEnergyScaleManager::getPtColumn() const {
  return ptColumn_m;
}

const std::string &ElectronEnergyScaleManager::getInputCollectionColumn() const {
  return inputCollectionColumn_m;
}

const std::vector<EESVariationEntry> &
ElectronEnergyScaleManager::getVariations() const {
  return variations_m;
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error("ElectronEnergyScaleManager::execute: context not set");

  // 1. Define corrected pT columns.
  for (const auto &step : correctionSteps_m) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string inputPt = step.inputPtColumn;
    const std::string sf      = step.sfColumn;
    const std::string outputPt = step.outputPtColumn;
    auto newDf = df.Define(
        outputPt,
        [](const ROOT::VecOps::RVec<Float_t> &pt,
           const ROOT::VecOps::RVec<Float_t> &sf) { return pt * sf; },
        {inputPt, sf});
    dataManager_m->setDataFrame(newDf);
  }

  // 2. Define PhysicsObjectCollection output columns.
  for (const auto &colStep : collectionOutputSteps_m) {
    const std::string inputCol  = inputCollectionColumn_m;
    const std::string corrPtCol = colStep.correctedPtColumn;
    const std::string outputCol = colStep.outputCollectionColumn;

    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    auto newDf = df.Define(
        outputCol,
        [](const PhysicsObjectCollection &col,
           const ROOT::VecOps::RVec<Float_t> &corrPt)
            -> PhysicsObjectCollection {
          return col.withCorrectedPt(corrPt);
        },
        {inputCol, corrPtCol});
    dataManager_m->setDataFrame(newDf);
  }

  // 3. Define per-variation collection columns and optional variation map.
  for (const auto &varColStep : variationCollectionsSteps_m) {
    const std::string inputCol = inputCollectionColumn_m;
    const std::string nomCol   = varColStep.nominalCollectionColumn;
    const std::string prefix   = varColStep.collectionPrefix;
    const std::string mapCol   = varColStep.variationMapColumn;

    std::vector<std::string> varNames;
    std::vector<std::string> upColNames;
    std::vector<std::string> dnColNames;

    for (const auto &var : variations_m) {
      const std::string upCol = prefix + "_" + var.name + "Up";
      const std::string dnCol = prefix + "_" + var.name + "Down";

      {
        const std::string varUpPt = var.upPtColumn;
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            upCol,
            [](const PhysicsObjectCollection &col,
               const ROOT::VecOps::RVec<Float_t> &corrPt)
                -> PhysicsObjectCollection {
              return col.withCorrectedPt(corrPt);
            },
            {inputCol, varUpPt});
        dataManager_m->setDataFrame(newDf);
      }

      {
        const std::string varDnPt = var.downPtColumn;
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            dnCol,
            [](const PhysicsObjectCollection &col,
               const ROOT::VecOps::RVec<Float_t> &corrPt)
                -> PhysicsObjectCollection {
              return col.withCorrectedPt(corrPt);
            },
            {inputCol, varDnPt});
        dataManager_m->setDataFrame(newDf);
      }

      varNames.push_back(var.name);
      upColNames.push_back(upCol);
      dnColNames.push_back(dnCol);
    }

    if (!mapCol.empty()) {
      {
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            mapCol,
            [](const PhysicsObjectCollection &nominalCol)
                -> PhysicsObjectVariationMap {
              PhysicsObjectVariationMap m;
              m.emplace("nominal", nominalCol);
              return m;
            },
            {nomCol});
        dataManager_m->setDataFrame(newDf);
      }

      for (std::size_t i = 0; i < varNames.size(); ++i) {
        const std::string keyUp = varNames[i] + "Up";
        const std::string keyDn = varNames[i] + "Down";

        {
          ROOT::RDF::RNode df = dataManager_m->getDataFrame();
          auto newDf = df.Redefine(
              mapCol,
              [keyUp](PhysicsObjectVariationMap m,
                      const PhysicsObjectCollection &upCol)
                  -> PhysicsObjectVariationMap {
                m.emplace(keyUp, upCol);
                return m;
              },
              {mapCol, upColNames[i]});
          dataManager_m->setDataFrame(newDf);
        }

        {
          ROOT::RDF::RNode df = dataManager_m->getDataFrame();
          auto newDf = df.Redefine(
              mapCol,
              [keyDn](PhysicsObjectVariationMap m,
                      const PhysicsObjectCollection &dnCol)
                  -> PhysicsObjectVariationMap {
                m.emplace(keyDn, dnCol);
                return m;
              },
              {mapCol, dnColNames[i]});
          dataManager_m->setDataFrame(newDf);
        }
      }
    }
  }

  // 4. Register systematic variations with ISystematicManager.
  for (const auto &var : variations_m) {
    systematicManager_m->registerSystematic(var.name + "Up",
                                            {var.upPtColumn});
    systematicManager_m->registerSystematic(var.name + "Down",
                                            {var.downPtColumn});
  }
}

// ---------------------------------------------------------------------------
// reportMetadata()
// ---------------------------------------------------------------------------

void ElectronEnergyScaleManager::reportMetadata() {
  if (!logger_m)
    return;

  std::ostringstream ss;
  ss << "ElectronEnergyScaleManager: configuration summary\n";

  if (!ptColumn_m.empty()) {
    ss << "  Electron columns:"
       << " pt=" << ptColumn_m
       << " eta=" << etaColumn_m
       << " phi=" << phiColumn_m
       << " mass=" << massColumn_m << "\n";
  }

  if (!correctionSteps_m.empty()) {
    ss << "  Correction steps (" << correctionSteps_m.size() << "):\n";
    for (const auto &step : correctionSteps_m) {
      ss << "    " << step.inputPtColumn
         << " x " << step.sfColumn
         << " -> " << step.outputPtColumn << "\n";
    }
  }

  if (!systematicSets_m.empty()) {
    ss << "  Systematic source sets:\n";
    for (const auto &kv : systematicSets_m)
      ss << "    \"" << kv.first << "\": " << kv.second.size() << " sources\n";
  }

  if (!variations_m.empty()) {
    ss << "  Systematic variations (" << variations_m.size() << "):\n";
    for (const auto &var : variations_m)
      ss << "    " << var.name
         << ": up=" << var.upPtColumn
         << " down=" << var.downPtColumn << "\n";
  }

  if (!inputCollectionColumn_m.empty()) {
    ss << "  Input collection: " << inputCollectionColumn_m << "\n";
  }

  if (!collectionOutputSteps_m.empty()) {
    ss << "  Collection output steps (" << collectionOutputSteps_m.size() << "):\n";
    for (const auto &step : collectionOutputSteps_m)
      ss << "    " << step.correctedPtColumn << " -> "
         << step.outputCollectionColumn << "\n";
  }

  if (!variationCollectionsSteps_m.empty()) {
    ss << "  Variation collection steps ("
       << variationCollectionsSteps_m.size() << "):\n";
    for (const auto &step : variationCollectionsSteps_m) {
      ss << "    nominal=\"" << step.nominalCollectionColumn
         << "\" prefix=\"" << step.collectionPrefix << "\"";
      if (!step.variationMapColumn.empty())
        ss << " map=\"" << step.variationMapColumn << "\"";
      ss << "\n";
    }
  }

  logger_m->log(ILogger::Level::Info, ss.str());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
ElectronEnergyScaleManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  if (!ptColumn_m.empty()) {
    entries["electron_pt_column"] = ptColumn_m;
    entries["electron_eta_column"] = etaColumn_m;
    entries["electron_phi_column"] = phiColumn_m;
    entries["electron_mass_column"] = massColumn_m;
  }

  if (!correctionSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < correctionSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << correctionSteps_m[i].inputPtColumn
         << "->" << correctionSteps_m[i].outputPtColumn
         << "(sf:" << correctionSteps_m[i].sfColumn << ')';
    }
    entries["correction_steps"] = ss.str();
  }

  if (!systematicSets_m.empty()) {
    std::ostringstream ss;
    bool firstSet = true;
    for (const auto &kv : systematicSets_m) {
      if (!firstSet) ss << ';';
      ss << kv.first << ":[";
      for (std::size_t i = 0; i < kv.second.size(); ++i) {
        if (i > 0) ss << ',';
        ss << kv.second[i];
      }
      ss << ']';
      firstSet = false;
    }
    entries["systematic_sets"] = ss.str();
  }

  if (!variations_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < variations_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << variations_m[i].name
         << "(up:" << variations_m[i].upPtColumn
         << ",dn:" << variations_m[i].downPtColumn << ')';
    }
    entries["variations"] = ss.str();
  }

  if (!inputCollectionColumn_m.empty())
    entries["input_collection_column"] = inputCollectionColumn_m;

  if (!collectionOutputSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < collectionOutputSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << collectionOutputSteps_m[i].correctedPtColumn
         << "->" << collectionOutputSteps_m[i].outputCollectionColumn;
    }
    entries["collection_output_steps"] = ss.str();
  }

  if (!variationCollectionsSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < variationCollectionsSteps_m.size(); ++i) {
      if (i > 0) ss << ';';
      ss << variationCollectionsSteps_m[i].collectionPrefix
         << "(nom:" << variationCollectionsSteps_m[i].nominalCollectionColumn;
      if (!variationCollectionsSteps_m[i].variationMapColumn.empty())
        ss << ",map:" << variationCollectionsSteps_m[i].variationMapColumn;
      ss << ')';
    }
    entries["variation_collection_steps"] = ss.str();
  }

  return entries;
}
