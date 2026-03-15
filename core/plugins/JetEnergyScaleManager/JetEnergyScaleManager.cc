#include <JetEnergyScaleManager.h>
#include <ROOT/RVec.hxx>
#include <api/ILogger.h>
#include <cmath>
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
// MET column configuration
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::setMETColumns(const std::string &metPtColumn,
                                           const std::string &metPhiColumn) {
  if (metPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setMETColumns: metPtColumn must not be empty");
  if (metPhiColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setMETColumns: metPhiColumn must not be empty");
  metPtColumn_m = metPtColumn;
  metPhiColumn_m = metPhiColumn;
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
  // per-jet scale factors in a new RVec<Float_t> column immediately.
  cm.applyCorrectionVec(correctionName, stringArgs, inputColumns);

  // The SF column name follows CorrectionManager::makeBranchName convention:
  //   correctionName + "_" + stringArgs[0] + "_" + stringArgs[1] + ...
  std::string sfColumn = correctionName;
  for (const auto &arg : stringArgs)
    sfColumn += "_" + arg;

  // Schedule the element-wise pT (and mass) multiplication for execute().
  applyCorrection(inputPtColumn, sfColumn, outputPtColumn, applyToMass,
                  inputMassColumn, outputMassColumn);
}

// ---------------------------------------------------------------------------
// CMS systematic source sets
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::registerSystematicSources(
    const std::string &setName, const std::vector<std::string> &sources) {
  if (setName.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::registerSystematicSources: setName must not be empty");
  if (sources.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::registerSystematicSources: sources must not be empty");
  for (const auto &s : sources) {
    if (s.empty())
      throw std::invalid_argument(
          "JetEnergyScaleManager::registerSystematicSources: "
          "source names must not be empty");
  }
  systematicSets_m[setName] = sources;
}

const std::vector<std::string> &
JetEnergyScaleManager::getSystematicSources(const std::string &setName) const {
  auto it = systematicSets_m.find(setName);
  if (it == systematicSets_m.end())
    throw std::out_of_range(
        "JetEnergyScaleManager::getSystematicSources: set \"" + setName +
        "\" is not registered");
  return it->second;
}

void JetEnergyScaleManager::applySystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName, const std::string &inputPtColumn,
    const std::string &outputPtPrefix, bool applyToMass,
    const std::vector<std::string> &inputColumns,
    const std::string &inputMassColumn) {
  if (correctionName.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applySystematicSet: correctionName must not be empty");
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applySystematicSet: inputPtColumn must not be empty");
  if (outputPtPrefix.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applySystematicSet: outputPtPrefix must not be empty");

  const auto &sources = getSystematicSources(setName); // throws if not found

  for (const auto &source : sources) {
    // Output pT column names for this source.
    const std::string upPtCol = outputPtPrefix + "_" + source + "_up";
    const std::string dnPtCol = outputPtPrefix + "_" + source + "_down";

    // Apply up and down corrections via CorrectionManager (SF column defined now).
    // String argument order: {source, "up"} / {source, "down"} as per CMS convention.
    // The explicit outputBranch parameter is used to keep naming consistent.
    const std::string sfUpCol = correctionName + "_" + source + "_up";
    const std::string sfDnCol = correctionName + "_" + source + "_down";
    cm.applyCorrectionVec(correctionName, {source, "up"}, inputColumns, sfUpCol);
    cm.applyCorrectionVec(correctionName, {source, "down"}, inputColumns, sfDnCol);

    // Schedule pT (and optionally mass) correction for execute().
    // Derive explicit mass column names so they follow the same pattern.
    const std::string inMass = inputMassColumn.empty()
                                   ? deriveMassColumnName(inputPtColumn)
                                   : inputMassColumn;
    const std::string upMasCol = applyToMass ? deriveMassColumnName(upPtCol) : "";
    const std::string dnMasCol = applyToMass ? deriveMassColumnName(dnPtCol) : "";

    applyCorrection(inputPtColumn, sfUpCol, upPtCol, applyToMass,
                    inMass, upMasCol);
    applyCorrection(inputPtColumn, sfDnCol, dnPtCol, applyToMass,
                    inMass, dnMasCol);

    // Register the variation (mass columns may be empty when applyToMass is
    // false or when the prefix substitution cannot be made).
    addVariation(source, upPtCol, dnPtCol,
                 applyToMass ? upMasCol : "",
                 applyToMass ? dnMasCol : "");
  }
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
// MET propagation
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::propagateMET(
    const std::string &baseMETPtColumn, const std::string &baseMETPhiColumn,
    const std::string &nominalJetPtColumn, const std::string &variedJetPtColumn,
    const std::string &outputMETPtColumn, const std::string &outputMETPhiColumn,
    float jetPtThreshold) {
  if (baseMETPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: baseMETPtColumn must not be empty");
  if (baseMETPhiColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: baseMETPhiColumn must not be empty");
  if (nominalJetPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: nominalJetPtColumn must not be empty");
  if (variedJetPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: variedJetPtColumn must not be empty");
  if (outputMETPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: outputMETPtColumn must not be empty");
  if (outputMETPhiColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::propagateMET: outputMETPhiColumn must not be empty");
  if (phiColumn_m.empty())
    throw std::runtime_error(
        "JetEnergyScaleManager::propagateMET: jet phi column not set. "
        "Call setJetColumns() before propagateMET().");

  METPropagationStep step;
  step.baseMETPtColumn = baseMETPtColumn;
  step.baseMETPhiColumn = baseMETPhiColumn;
  step.nominalJetPtColumn = nominalJetPtColumn;
  step.variedJetPtColumn = variedJetPtColumn;
  step.outputMETPtColumn = outputMETPtColumn;
  step.outputMETPhiColumn = outputMETPhiColumn;
  step.jetPtThreshold = jetPtThreshold;
  metPropagationSteps_m.push_back(std::move(step));
}

// ---------------------------------------------------------------------------
// PhysicsObjectCollection integration
// ---------------------------------------------------------------------------

void JetEnergyScaleManager::setInputJetCollection(
    const std::string &collectionColumn) {
  if (collectionColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setInputJetCollection: "
        "collectionColumn must not be empty");
  inputJetCollectionColumn_m = collectionColumn;
}

void JetEnergyScaleManager::defineCollectionOutput(
    const std::string &correctedPtColumn,
    const std::string &outputCollectionColumn,
    const std::string &correctedMassColumn) {
  if (correctedPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::defineCollectionOutput: "
        "correctedPtColumn must not be empty");
  if (outputCollectionColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::defineCollectionOutput: "
        "outputCollectionColumn must not be empty");
  if (inputJetCollectionColumn_m.empty())
    throw std::runtime_error(
        "JetEnergyScaleManager::defineCollectionOutput: "
        "call setInputJetCollection() before defineCollectionOutput()");

  CollectionOutputStep step;
  step.correctedPtColumn = correctedPtColumn;
  step.correctedMassColumn = correctedMassColumn;
  step.outputCollectionColumn = outputCollectionColumn;
  collectionOutputSteps_m.push_back(std::move(step));
}

void JetEnergyScaleManager::defineVariationCollections(
    const std::string &nominalCollectionColumn,
    const std::string &collectionPrefix,
    const std::string &variationMapColumn) {
  if (nominalCollectionColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::defineVariationCollections: "
        "nominalCollectionColumn must not be empty");
  if (collectionPrefix.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::defineVariationCollections: "
        "collectionPrefix must not be empty");
  if (inputJetCollectionColumn_m.empty())
    throw std::runtime_error(
        "JetEnergyScaleManager::defineVariationCollections: "
        "call setInputJetCollection() before defineVariationCollections()");

  VariationCollectionsStep step;
  step.nominalCollectionColumn = nominalCollectionColumn;
  step.collectionPrefix = collectionPrefix;
  step.variationMapColumn = variationMapColumn;
  variationCollectionsSteps_m.push_back(std::move(step));
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

const std::string &JetEnergyScaleManager::getMETPtColumn() const {
  return metPtColumn_m;
}

const std::string &JetEnergyScaleManager::getMETPhiColumn() const {
  return metPhiColumn_m;
}

const std::string &JetEnergyScaleManager::getInputJetCollectionColumn() const {
  return inputJetCollectionColumn_m;
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

  // 3. Apply each MET propagation step.
  //    Strategy: define an intermediate column holding [metX, metY], then
  //    derive MET pT and phi from it to avoid computing the sum twice.
  for (const auto &step : metPropagationSteps_m) {
    const std::string tmpCol =
        "_jesmet_tmp_" + step.outputMETPtColumn;
    const std::string basePt  = step.baseMETPtColumn;
    const std::string basePhi = step.baseMETPhiColumn;
    const std::string nomPt   = step.nominalJetPtColumn;
    const std::string varPt   = step.variedJetPtColumn;
    const std::string jetPhi  = phiColumn_m;
    const float threshold     = step.jetPtThreshold;

    // Intermediate: RVec<float>{newMET_x, newMET_y}
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(
          tmpCol,
          [threshold](Float_t baseMETPt, Float_t baseMETPhiArg,
                      const ROOT::VecOps::RVec<Float_t> &nomJetPt,
                      const ROOT::VecOps::RVec<Float_t> &varJetPt,
                      const ROOT::VecOps::RVec<Float_t> &jPhi)
              -> ROOT::VecOps::RVec<float> {
            float metX = baseMETPt * std::cos(baseMETPhiArg);
            float metY = baseMETPt * std::sin(baseMETPhiArg);
            for (std::size_t i = 0; i < nomJetPt.size(); ++i) {
              if (nomJetPt[i] > threshold) {
                float dpt = varJetPt[i] - nomJetPt[i];
                metX -= dpt * std::cos(jPhi[i]);
                metY -= dpt * std::sin(jPhi[i]);
              }
            }
            return ROOT::VecOps::RVec<float>{metX, metY};
          },
          {basePt, basePhi, nomPt, varPt, jetPhi});
      dataManager_m->setDataFrame(newDf);
    }

    // Output MET pT.
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string outPt = step.outputMETPtColumn;
      auto newDf = df.Define(
          outPt,
          [](const ROOT::VecOps::RVec<float> &xy) -> Float_t {
            return std::sqrt(xy[0] * xy[0] + xy[1] * xy[1]);
          },
          {tmpCol});
      dataManager_m->setDataFrame(newDf);
    }

    // Output MET phi.
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string outPhi = step.outputMETPhiColumn;
      auto newDf = df.Define(
          outPhi,
          [](const ROOT::VecOps::RVec<float> &xy) -> Float_t {
            return std::atan2(xy[1], xy[0]);
          },
          {tmpCol});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // 4. Define PhysicsObjectCollection output columns.
  for (const auto &colStep : collectionOutputSteps_m) {
    const std::string inputCol  = inputJetCollectionColumn_m;
    const std::string corrPtCol = colStep.correctedPtColumn;
    const std::string outputCol = colStep.outputCollectionColumn;

    if (!colStep.correctedMassColumn.empty()) {
      // Pt + mass correction: use withCorrectedKinematics.
      // Eta and phi are unchanged by JES/JER; reconstruct full-size arrays
      // from the collection's stored 4-vectors.  Entries for jets that were
      // filtered out of the input collection (i.e. not stored in the
      // PhysicsObjectCollection) remain at their initialised value of 0.0f.
      // withCorrectedKinematics only reads entries at the indices that are
      // present in the collection, so the zero values for absent jets are
      // never used in the output.
      const std::string corrMasCol = colStep.correctedMassColumn;
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(
          outputCol,
          [](const PhysicsObjectCollection &col,
             const ROOT::VecOps::RVec<Float_t> &corrPt,
             const ROOT::VecOps::RVec<Float_t> &corrMass)
              -> PhysicsObjectCollection {
            // Reconstruct full-size eta/phi arrays (unchanged by JES/JER).
            const std::size_t n = corrPt.size();
            ROOT::VecOps::RVec<Float_t> etaFull(n, 0.0f);
            ROOT::VecOps::RVec<Float_t> phiFull(n, 0.0f);
            for (std::size_t i = 0; i < col.size(); ++i) {
              Int_t idx = col.index(i);
              if (idx >= 0 && static_cast<std::size_t>(idx) < n) {
                etaFull[idx] = static_cast<Float_t>(col.at(i).Eta());
                phiFull[idx] = static_cast<Float_t>(col.at(i).Phi());
              }
            }
            return col.withCorrectedKinematics(corrPt, etaFull, phiFull,
                                               corrMass);
          },
          {inputCol, corrPtCol, corrMasCol});
      dataManager_m->setDataFrame(newDf);
    } else {
      // Pt-only correction: use withCorrectedPt.
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
  }

  // 5. Define per-variation collection columns and optional variation map.
  for (const auto &varColStep : variationCollectionsSteps_m) {
    const std::string inputCol = inputJetCollectionColumn_m;
    const std::string nomCol   = varColStep.nominalCollectionColumn;
    const std::string prefix   = varColStep.collectionPrefix;
    const std::string mapCol   = varColStep.variationMapColumn;

    std::vector<std::string> varNames;
    std::vector<std::string> upColNames;
    std::vector<std::string> dnColNames;

    for (const auto &var : variations_m) {
      const std::string upCol = prefix + "_" + var.name + "Up";
      const std::string dnCol = prefix + "_" + var.name + "Down";

      // Define up-variation collection.
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

      // Define down-variation collection.
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

    // Build the PhysicsObjectVariationMap column (if requested).
    // Strategy: define the column with "nominal" first, then Redefine
    // it to accumulate each variation (up then down) one at a time.
    // Listing the column name itself in the input cols of Redefine passes
    // the current value of the column to the lambda, enabling accumulation.
    if (!mapCol.empty()) {
      // Step A: initialise the map with just the nominal collection.
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

      // Step B: fold in each variation's up and down collections.
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

  // 6. Register systematic variations with the ISystematicManager.
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

  if (!metPtColumn_m.empty()) {
    ss << "  MET columns: pt=" << metPtColumn_m
       << " phi=" << metPhiColumn_m << "\n";
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
         << " x " << step.sfColumn
         << " -> " << step.outputPtColumn;
      if (!step.outputMassColumn.empty())
        ss << "  (mass: " << step.inputMassColumn
           << " -> " << step.outputMassColumn << ")";
      ss << "\n";
    }
  }

  if (!systematicSets_m.empty()) {
    ss << "  Registered systematic source sets:\n";
    for (const auto &kv : systematicSets_m) {
      ss << "    \"" << kv.first << "\": " << kv.second.size() << " sources\n";
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

  if (!metPropagationSteps_m.empty()) {
    ss << "  MET propagation steps (" << metPropagationSteps_m.size() << "):\n";
    for (const auto &step : metPropagationSteps_m) {
      ss << "    " << step.nominalJetPtColumn
         << " -> " << step.variedJetPtColumn
         << " : " << step.baseMETPtColumn
         << " -> " << step.outputMETPtColumn
         << " (threshold=" << step.jetPtThreshold << " GeV)\n";
    }
  }

  if (!inputJetCollectionColumn_m.empty()) {
    ss << "  Input jet collection: " << inputJetCollectionColumn_m << "\n";
  }

  if (!collectionOutputSteps_m.empty()) {
    ss << "  Collection output steps (" << collectionOutputSteps_m.size() << "):\n";
    for (const auto &step : collectionOutputSteps_m) {
      ss << "    " << step.correctedPtColumn
         << " -> " << step.outputCollectionColumn;
      if (!step.correctedMassColumn.empty())
        ss << " (mass: " << step.correctedMassColumn << ")";
      ss << "\n";
    }
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
JetEnergyScaleManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  if (!ptColumn_m.empty()) {
    entries["jet_pt_column"] = ptColumn_m;
    entries["jet_eta_column"] = etaColumn_m;
    entries["jet_phi_column"] = phiColumn_m;
    entries["jet_mass_column"] = massColumn_m;
  }

  if (!metPtColumn_m.empty()) {
    entries["met_pt_column"] = metPtColumn_m;
    entries["met_phi_column"] = metPhiColumn_m;
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

  if (!metPropagationSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < metPropagationSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << metPropagationSteps_m[i].nominalJetPtColumn
         << "->" << metPropagationSteps_m[i].variedJetPtColumn
         << ":" << metPropagationSteps_m[i].outputMETPtColumn;
    }
    entries["met_propagation_steps"] = ss.str();
  }

  if (!inputJetCollectionColumn_m.empty()) {
    entries["input_jet_collection_column"] = inputJetCollectionColumn_m;
  }

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
