#include <PhotonEnergyScaleManager.h>
#include <ROOT/RVec.hxx>
#include <api/ILogger.h>
#include <cmath>
#include <set>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// setContext
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

// ---------------------------------------------------------------------------
// Object column configuration
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::setObjectColumns(
    const std::string &ptColumn, const std::string &etaColumn,
    const std::string &phiColumn, const std::string &massColumn) {
  if (ptColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::setObjectColumns: ptColumn must not be empty");
  ptColumn_m = ptColumn;
  etaColumn_m = etaColumn;
  phiColumn_m = phiColumn;
  massColumn_m = massColumn;
}

// ---------------------------------------------------------------------------
// MET column configuration
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::setMETColumns(
    const std::string &metPtColumn, const std::string &metPhiColumn) {
  if (metPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::setMETColumns: metPtColumn must not be empty");
  if (metPhiColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::setMETColumns: metPhiColumn must not be empty");
  metPtColumn_m = metPtColumn;
  metPhiColumn_m = metPhiColumn;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

std::string PhotonEnergyScaleManager::deriveMassColumnName(
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

void PhotonEnergyScaleManager::applyCorrection(
    const std::string &inputPtColumn, const std::string &sfColumn,
    const std::string &outputPtColumn, bool applyToMass,
    const std::string &inputMassColumn, const std::string &outputMassColumn) {
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyCorrection: inputPtColumn must not be empty");
  if (sfColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyCorrection: sfColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyCorrection: outputPtColumn must not be empty");

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

void PhotonEnergyScaleManager::applyCorrectionlib(
    CorrectionManager &cm, const std::string &correctionName,
    const std::vector<std::string> &stringArgs,
    const std::string &inputPtColumn, const std::string &outputPtColumn,
    bool applyToMass, const std::string &inputMassColumn,
    const std::string &outputMassColumn,
    const std::vector<std::string> &inputColumns) {
  cm.applyCorrectionVec(correctionName, stringArgs, inputColumns);

  std::string sfColumn = correctionName;
  for (const auto &arg : stringArgs)
    sfColumn += "_" + arg;

  applyCorrection(inputPtColumn, sfColumn, outputPtColumn, applyToMass,
                  inputMassColumn, outputMassColumn);
}

// ---------------------------------------------------------------------------
// Resolution smearing
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::applyResolutionSmearing(
    const std::string &inputPtColumn, const std::string &sigmaColumn,
    const std::string &randomColumn, const std::string &outputPtColumn) {
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyResolutionSmearing: "
        "inputPtColumn must not be empty");
  if (sigmaColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyResolutionSmearing: "
        "sigmaColumn must not be empty");
  if (randomColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyResolutionSmearing: "
        "randomColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applyResolutionSmearing: "
        "outputPtColumn must not be empty");

  SmearingStep step;
  step.inputPtColumn = inputPtColumn;
  step.sigmaColumn = sigmaColumn;
  step.randomColumn = randomColumn;
  step.outputPtColumn = outputPtColumn;
  smearingSteps_m.push_back(std::move(step));
}

// ---------------------------------------------------------------------------
// Systematic source sets
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::registerSystematicSources(
    const std::string &setName, const std::vector<std::string> &sources) {
  if (setName.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::registerSystematicSources: "
        "setName must not be empty");
  if (sources.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::registerSystematicSources: "
        "sources must not be empty");
  for (const auto &s : sources) {
    if (s.empty())
      throw std::invalid_argument(
          "PhotonEnergyScaleManager::registerSystematicSources: "
          "source names must not be empty");
  }
  systematicSets_m[setName] = sources;
}

const std::vector<std::string> &
PhotonEnergyScaleManager::getSystematicSources(
    const std::string &setName) const {
  auto it = systematicSets_m.find(setName);
  if (it == systematicSets_m.end())
    throw std::out_of_range(
        "PhotonEnergyScaleManager::getSystematicSources: set \"" + setName +
        "\" is not registered");
  return it->second;
}

void PhotonEnergyScaleManager::applySystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName, const std::string &inputPtColumn,
    const std::string &outputPtPrefix, bool applyToMass,
    const std::vector<std::string> &inputColumns,
    const std::string &inputMassColumn) {
  if (correctionName.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applySystematicSet: "
        "correctionName must not be empty");
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applySystematicSet: "
        "inputPtColumn must not be empty");
  if (outputPtPrefix.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::applySystematicSet: "
        "outputPtPrefix must not be empty");

  const auto &sources = getSystematicSources(setName);

  for (const auto &source : sources) {
    const std::string upPtCol = outputPtPrefix + "_" + source + "_up";
    const std::string dnPtCol = outputPtPrefix + "_" + source + "_down";

    const std::string sfUpCol = correctionName + "_" + source + "_up";
    const std::string sfDnCol = correctionName + "_" + source + "_down";
    cm.applyCorrectionVec(correctionName, {source, "up"}, inputColumns, sfUpCol);
    cm.applyCorrectionVec(correctionName, {source, "down"}, inputColumns, sfDnCol);

    const std::string inMass = inputMassColumn.empty()
                                   ? deriveMassColumnName(inputPtColumn)
                                   : inputMassColumn;
    const std::string upMasCol = applyToMass ? deriveMassColumnName(upPtCol) : "";
    const std::string dnMasCol = applyToMass ? deriveMassColumnName(dnPtCol) : "";

    applyCorrection(inputPtColumn, sfUpCol, upPtCol, applyToMass,
                    inMass, upMasCol);
    applyCorrection(inputPtColumn, sfDnCol, dnPtCol, applyToMass,
                    inMass, dnMasCol);

    addVariation(source, upPtCol, dnPtCol,
                 applyToMass ? upMasCol : "",
                 applyToMass ? dnMasCol : "");
  }
}

// ---------------------------------------------------------------------------
// Direct variation registration
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::addVariation(
    const std::string &systematicName, const std::string &upPtColumn,
    const std::string &downPtColumn, const std::string &upMassColumn,
    const std::string &downMassColumn) {
  if (systematicName.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::addVariation: "
        "systematicName must not be empty");
  if (upPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::addVariation: upPtColumn must not be empty");
  if (downPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::addVariation: downPtColumn must not be empty");

  PESVariationEntry entry;
  entry.name = systematicName;
  entry.upPtColumn = upPtColumn;
  entry.downPtColumn = downPtColumn;
  entry.upMassColumn = upMassColumn;
  entry.downMassColumn = downMassColumn;
  variations_m.push_back(std::move(entry));
}

// ---------------------------------------------------------------------------
// Type-1 MET propagation
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::propagateMET(
    const std::string &baseMETPtColumn, const std::string &baseMETPhiColumn,
    const std::string &nominalPtColumn, const std::string &variedPtColumn,
    const std::string &outputMETPtColumn, const std::string &outputMETPhiColumn,
    float ptThreshold) {
  if (baseMETPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "baseMETPtColumn must not be empty");
  if (baseMETPhiColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "baseMETPhiColumn must not be empty");
  if (nominalPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "nominalPtColumn must not be empty");
  if (variedPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "variedPtColumn must not be empty");
  if (outputMETPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "outputMETPtColumn must not be empty");
  if (outputMETPhiColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::propagateMET: "
        "outputMETPhiColumn must not be empty");
  if (phiColumn_m.empty())
    throw std::runtime_error(
        "PhotonEnergyScaleManager::propagateMET: "
        "object phi column not set. Call setObjectColumns() before propagateMET().");

  METPropagationStep step;
  step.baseMETPtColumn = baseMETPtColumn;
  step.baseMETPhiColumn = baseMETPhiColumn;
  step.nominalPtColumn = nominalPtColumn;
  step.variedPtColumn = variedPtColumn;
  step.outputMETPtColumn = outputMETPtColumn;
  step.outputMETPhiColumn = outputMETPhiColumn;
  step.ptThreshold = ptThreshold;
  metPropagationSteps_m.push_back(std::move(step));
}

// ---------------------------------------------------------------------------
// PhysicsObjectCollection integration
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::setInputCollection(
    const std::string &collectionColumn) {
  if (collectionColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::setInputCollection: "
        "collectionColumn must not be empty");
  inputCollectionColumn_m = collectionColumn;
}

void PhotonEnergyScaleManager::defineCollectionOutput(
    const std::string &correctedPtColumn,
    const std::string &outputCollectionColumn,
    const std::string &correctedMassColumn) {
  if (correctedPtColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::defineCollectionOutput: "
        "correctedPtColumn must not be empty");
  if (outputCollectionColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::defineCollectionOutput: "
        "outputCollectionColumn must not be empty");
  if (inputCollectionColumn_m.empty())
    throw std::runtime_error(
        "PhotonEnergyScaleManager::defineCollectionOutput: "
        "call setInputCollection() before defineCollectionOutput()");

  CollectionOutputStep step;
  step.correctedPtColumn = correctedPtColumn;
  step.correctedMassColumn = correctedMassColumn;
  step.outputCollectionColumn = outputCollectionColumn;
  collectionOutputSteps_m.push_back(std::move(step));
}

void PhotonEnergyScaleManager::defineVariationCollections(
    const std::string &nominalCollectionColumn,
    const std::string &collectionPrefix,
    const std::string &variationMapColumn) {
  if (nominalCollectionColumn.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::defineVariationCollections: "
        "nominalCollectionColumn must not be empty");
  if (collectionPrefix.empty())
    throw std::invalid_argument(
        "PhotonEnergyScaleManager::defineVariationCollections: "
        "collectionPrefix must not be empty");
  if (inputCollectionColumn_m.empty())
    throw std::runtime_error(
        "PhotonEnergyScaleManager::defineVariationCollections: "
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

const std::string &PhotonEnergyScaleManager::getPtColumn() const {
  return ptColumn_m;
}

const std::string &PhotonEnergyScaleManager::getMassColumn() const {
  return massColumn_m;
}

const std::string &PhotonEnergyScaleManager::getMETPtColumn() const {
  return metPtColumn_m;
}

const std::string &PhotonEnergyScaleManager::getMETPhiColumn() const {
  return metPhiColumn_m;
}

const std::string &PhotonEnergyScaleManager::getInputCollectionColumn() const {
  return inputCollectionColumn_m;
}

const std::vector<PESVariationEntry> &
PhotonEnergyScaleManager::getVariations() const {
  return variations_m;
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error(
        "PhotonEnergyScaleManager::execute: context not set");

  // 1. Apply scale correction steps (pT × SF, optionally mass × SF).
  for (const auto &step : correctionSteps_m) {
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputPt  = step.inputPtColumn;
      const std::string sf       = step.sfColumn;
      const std::string outputPt = step.outputPtColumn;
      auto newDf = df.Define(
          outputPt,
          [](const ROOT::VecOps::RVec<Float_t> &pt,
             const ROOT::VecOps::RVec<Float_t> &sf) { return pt * sf; },
          {inputPt, sf});
      dataManager_m->setDataFrame(newDf);
    }
    if (!step.inputMassColumn.empty() && !step.outputMassColumn.empty()) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputMass  = step.inputMassColumn;
      const std::string sf         = step.sfColumn;
      const std::string outputMass = step.outputMassColumn;
      auto newDf = df.Define(
          outputMass,
          [](const ROOT::VecOps::RVec<Float_t> &mass,
             const ROOT::VecOps::RVec<Float_t> &sf) { return mass * sf; },
          {inputMass, sf});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // 2. Apply resolution smearing steps (pT + sigma * u).
  for (const auto &step : smearingSteps_m) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string inputPt  = step.inputPtColumn;
    const std::string sigma    = step.sigmaColumn;
    const std::string rnd      = step.randomColumn;
    const std::string outputPt = step.outputPtColumn;
    auto newDf = df.Define(
        outputPt,
        [](const ROOT::VecOps::RVec<Float_t> &pt,
           const ROOT::VecOps::RVec<Float_t> &sig,
           const ROOT::VecOps::RVec<Float_t> &u) { return pt + sig * u; },
        {inputPt, sigma, rnd});
    dataManager_m->setDataFrame(newDf);
  }

  // 3. Apply MET propagation steps.
  //    Intermediate column holds {newMET_x, newMET_y} to avoid double computation.
  for (const auto &step : metPropagationSteps_m) {
    const std::string tmpCol =
        "_pesmet_tmp_" + step.outputMETPtColumn;
    const std::string basePt  = step.baseMETPtColumn;
    const std::string basePhi = step.baseMETPhiColumn;
    const std::string nomPt   = step.nominalPtColumn;
    const std::string varPt   = step.variedPtColumn;
    const std::string objPhi  = phiColumn_m;
    const float threshold     = step.ptThreshold;

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(
          tmpCol,
          [threshold](Float_t baseMETPt, Float_t baseMETPhiArg,
                      const ROOT::VecOps::RVec<Float_t> &nomObjPt,
                      const ROOT::VecOps::RVec<Float_t> &varObjPt,
                      const ROOT::VecOps::RVec<Float_t> &oPhi)
              -> ROOT::VecOps::RVec<float> {
            float metX = baseMETPt * std::cos(baseMETPhiArg);
            float metY = baseMETPt * std::sin(baseMETPhiArg);
            for (std::size_t i = 0; i < nomObjPt.size(); ++i) {
              if (nomObjPt[i] > threshold) {
                float dpt = varObjPt[i] - nomObjPt[i];
                metX -= dpt * std::cos(oPhi[i]);
                metY -= dpt * std::sin(oPhi[i]);
              }
            }
            return ROOT::VecOps::RVec<float>{metX, metY};
          },
          {basePt, basePhi, nomPt, varPt, objPhi});
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
    const std::string inputCol  = inputCollectionColumn_m;
    const std::string corrPtCol = colStep.correctedPtColumn;
    const std::string outputCol = colStep.outputCollectionColumn;

    if (!colStep.correctedMassColumn.empty()) {
      const std::string corrMasCol = colStep.correctedMassColumn;
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(
          outputCol,
          [](const PhysicsObjectCollection &col,
             const ROOT::VecOps::RVec<Float_t> &corrPt,
             const ROOT::VecOps::RVec<Float_t> &corrMass)
              -> PhysicsObjectCollection {
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

  // 6. Register systematic variations with ISystematicManager.
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
      systematicManager_m->registerSystematic(var.name + "Down", affectedDown);
    }
  }
}

// ---------------------------------------------------------------------------
// reportMetadata()
// ---------------------------------------------------------------------------

void PhotonEnergyScaleManager::reportMetadata() {
  if (!logger_m)
    return;

  std::ostringstream ss;
  ss << "PhotonEnergyScaleManager: configuration summary\n";

  if (!ptColumn_m.empty()) {
    ss << "  Photon columns:"
       << " pt="   << ptColumn_m
       << " eta="  << etaColumn_m
       << " phi="  << phiColumn_m
       << " mass=" << massColumn_m << "\n";
  }

  if (!metPtColumn_m.empty())
    ss << "  MET columns: pt=" << metPtColumn_m
       << " phi=" << metPhiColumn_m << "\n";

  if (!correctionSteps_m.empty()) {
    ss << "  Scale correction steps (" << correctionSteps_m.size() << "):\n";
    for (const auto &step : correctionSteps_m) {
      ss << "    " << step.inputPtColumn
         << " x " << step.sfColumn
         << " -> " << step.outputPtColumn;
      if (!step.outputMassColumn.empty())
        ss << " [+ mass -> " << step.outputMassColumn << "]";
      ss << "\n";
    }
  }

  if (!smearingSteps_m.empty()) {
    ss << "  Resolution smearing steps (" << smearingSteps_m.size() << "):\n";
    for (const auto &step : smearingSteps_m)
      ss << "    " << step.inputPtColumn
         << " + " << step.sigmaColumn
         << " * " << step.randomColumn
         << " -> " << step.outputPtColumn << "\n";
  }

  if (!metPropagationSteps_m.empty()) {
    ss << "  MET propagation steps (" << metPropagationSteps_m.size() << "):\n";
    for (const auto &step : metPropagationSteps_m)
      ss << "    (" << step.baseMETPtColumn << "," << step.baseMETPhiColumn
         << ") nom=" << step.nominalPtColumn
         << " var=" << step.variedPtColumn
         << " -> (" << step.outputMETPtColumn << ","
         << step.outputMETPhiColumn << ")\n";
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

  if (!inputCollectionColumn_m.empty())
    ss << "  Input collection: " << inputCollectionColumn_m << "\n";

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
PhotonEnergyScaleManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  if (!ptColumn_m.empty()) {
    entries["photon_pt_column"]   = ptColumn_m;
    entries["photon_eta_column"]  = etaColumn_m;
    entries["photon_phi_column"]  = phiColumn_m;
    entries["photon_mass_column"] = massColumn_m;
  }

  if (!metPtColumn_m.empty()) {
    entries["met_pt_column"]  = metPtColumn_m;
    entries["met_phi_column"] = metPhiColumn_m;
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

  if (!smearingSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < smearingSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << smearingSteps_m[i].inputPtColumn
         << "+" << smearingSteps_m[i].sigmaColumn
         << "*" << smearingSteps_m[i].randomColumn
         << "->" << smearingSteps_m[i].outputPtColumn;
    }
    entries["smearing_steps"] = ss.str();
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
      ss << metPropagationSteps_m[i].nominalPtColumn
         << "->" << metPropagationSteps_m[i].variedPtColumn
         << ":" << metPropagationSteps_m[i].outputMETPtColumn;
    }
    entries["met_propagation_steps"] = ss.str();
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
