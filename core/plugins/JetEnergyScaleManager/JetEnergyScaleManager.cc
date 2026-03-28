#include <JetEnergyScaleManager.h>
#include <analyzer.h>
#include <ROOT/RVec.hxx>
#include <api/ILogger.h>
#include <cmath>
#include <cstdint>
#include <sstream>
#include <stdexcept>

namespace {

std::string buildPackedInputExpression(const std::vector<std::string> &inputColumns) {
  std::string expr =
      "ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> _out(" +
      inputColumns[0] + ".size());\n";
  for (const auto &column : inputColumns) {
    expr += "for (size_t _i = 0; _i < _out.size(); ++_i)"
            " _out[_i].push_back(static_cast<double>(" +
            column + "[_i]));\n";
  }
  expr += "return _out;";
  return expr;
}

uint64_t splitmix64(uint64_t value) {
  value += 0x9e3779b97f4a7c15ULL;
  value = (value ^ (value >> 30U)) * 0xbf58476d1ce4e5b9ULL;
  value = (value ^ (value >> 27U)) * 0x94d049bb133111ebULL;
  return value ^ (value >> 31U);
}

float normalFromSeed(uint64_t seed) {
  constexpr double invTwoTo53 = 1.0 / static_cast<double>(1ULL << 53U);
  const double u1 = std::max(
      static_cast<double>((splitmix64(seed) >> 11U)) * invTwoTo53,
      1e-12);
  const double u2 = static_cast<double>((splitmix64(seed ^ 0x9e3779b97f4a7c15ULL) >> 11U)) *
                    invTwoTo53;
  return static_cast<float>(std::sqrt(-2.0 * std::log(u1)) *
                            std::cos(2.0 * M_PI * u2));
}

} // namespace

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

void JetEnergyScaleManager::setJERSmearingColumns(
    const std::string &genJetPtColumn, const std::string &rhoColumn,
    const std::string &eventColumn) {
  if (genJetPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setJERSmearingColumns: genJetPtColumn must not be empty");
  if (rhoColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setJERSmearingColumns: rhoColumn must not be empty");
  if (eventColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::setJERSmearingColumns: eventColumn must not be empty");
  genJetPtColumn_m = genJetPtColumn;
  rhoColumn_m = rhoColumn;
  eventColumn_m = eventColumn;
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

void JetEnergyScaleManager::applyJERSmearing(
    CorrectionManager &cm, const std::string &ptResolutionCorrection,
    const std::string &scaleFactorCorrection, const std::string &inputPtColumn,
    const std::string &outputPtColumn, const std::string &systematic,
    bool applyToMass, const std::string &inputMassColumn,
    const std::string &outputMassColumn,
    const std::vector<std::string> &ptResolutionInputs,
    const std::vector<std::string> &scaleFactorInputs) {
  if (ptResolutionCorrection.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyJERSmearing: ptResolutionCorrection must not be empty");
  if (scaleFactorCorrection.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyJERSmearing: scaleFactorCorrection must not be empty");
  if (inputPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyJERSmearing: inputPtColumn must not be empty");
  if (outputPtColumn.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyJERSmearing: outputPtColumn must not be empty");
  if (systematic.empty())
    throw std::invalid_argument(
        "JetEnergyScaleManager::applyJERSmearing: systematic must not be empty");
  if (genJetPtColumn_m.empty() || rhoColumn_m.empty() || eventColumn_m.empty())
    throw std::runtime_error(
        "JetEnergyScaleManager::applyJERSmearing: call setJERSmearingColumns() before scheduling JER smearing");

  JERSmearingStep step;
  step.ptResolutionCorrection = cm.getCorrection(ptResolutionCorrection);
  step.scaleFactorCorrection = cm.getCorrection(scaleFactorCorrection);
  step.ptResolutionInputs = ptResolutionInputs.empty()
                                ? cm.getCorrectionFeatures(ptResolutionCorrection)
                                : ptResolutionInputs;
  step.scaleFactorInputs = scaleFactorInputs.empty()
                               ? cm.getCorrectionFeatures(scaleFactorCorrection)
                               : scaleFactorInputs;
  step.systematic = systematic;
  step.inputPtColumn = inputPtColumn;
  step.outputPtColumn = outputPtColumn;
  if (applyToMass) {
    step.inputMassColumn = inputMassColumn.empty()
                               ? deriveMassColumnName(inputPtColumn)
                               : inputMassColumn;
    step.outputMassColumn = outputMassColumn.empty()
                                ? deriveMassColumnName(outputPtColumn)
                                : outputMassColumn;
  }
  jerSmearingSteps_m.push_back(std::move(step));
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
    throw std::runtime_error(
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

  // 3. Apply each registered JER smearing step.
  for (const auto &step : jerSmearingSteps_m) {
    const std::string resInputCol = "_jer_inputs_res_" + step.outputPtColumn;
    const std::string sfInputCol = "_jer_inputs_sf_" + step.outputPtColumn;
    const std::string jerResCol = "_jer_resolution_" + step.outputPtColumn;
    const std::string jerSfCol = "_jer_scale_factor_" + step.outputPtColumn;
    const std::string smearCol = "_jer_smear_" + step.outputPtColumn;

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(resInputCol,
                             buildPackedInputExpression(step.ptResolutionInputs));
      dataManager_m->setDataFrame(newDf);
    }

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(sfInputCol,
                             buildPackedInputExpression(step.scaleFactorInputs));
      dataManager_m->setDataFrame(newDf);
    }

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const auto resolution = step.ptResolutionCorrection;
      auto newDf = df.Define(
          jerResCol,
          [resolution](const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> &inputMatrix)
              -> ROOT::VecOps::RVec<Float_t> {
            ROOT::VecOps::RVec<Float_t> result(inputMatrix.size());
            for (std::size_t i = 0; i < inputMatrix.size(); ++i) {
              std::vector<correction::Variable::Type> values;
              values.reserve(inputMatrix[i].size());
              for (double value : inputMatrix[i])
                values.emplace_back(value);
              result[i] = static_cast<Float_t>(resolution->evaluate(values));
            }
            return result;
          },
          {resInputCol});
      dataManager_m->setDataFrame(newDf);
    }

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const auto scaleFactor = step.scaleFactorCorrection;
      const auto systematic = step.systematic;
      auto newDf = df.Define(
          jerSfCol,
          [scaleFactor, systematic](const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> &inputMatrix)
              -> ROOT::VecOps::RVec<Float_t> {
            ROOT::VecOps::RVec<Float_t> result(inputMatrix.size());
            for (std::size_t i = 0; i < inputMatrix.size(); ++i) {
              std::vector<correction::Variable::Type> values;
              values.reserve(inputMatrix[i].size() + 1U);
              std::size_t numericIndex = 0;
              for (const auto &input : scaleFactor->inputs()) {
                if (input.type() == correction::Variable::VarType::string) {
                  values.emplace_back(systematic);
                } else {
                  values.emplace_back(inputMatrix[i].at(numericIndex));
                  ++numericIndex;
                }
              }
              result[i] = static_cast<Float_t>(scaleFactor->evaluate(values));
            }
            return result;
          },
          {sfInputCol});
      dataManager_m->setDataFrame(newDf);
    }

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputPt = step.inputPtColumn;
      const std::string etaCol = etaColumn_m;
      const std::string genJetPtCol = genJetPtColumn_m;
      const std::string eventCol = eventColumn_m;
      auto newDf = df.Define(
          smearCol,
          [](const ROOT::VecOps::RVec<Float_t> &pt,
             const ROOT::VecOps::RVec<Float_t> &eta,
             const ROOT::VecOps::RVec<Float_t> &genJetPt,
             ULong64_t eventId,
             const ROOT::VecOps::RVec<Float_t> &resolution,
             const ROOT::VecOps::RVec<Float_t> &scaleFactor)
              -> ROOT::VecOps::RVec<Float_t> {
            ROOT::VecOps::RVec<Float_t> smear(pt.size(), 1.0f);
            for (std::size_t i = 0; i < pt.size(); ++i) {
              const float ptValue = pt[i];
              if (ptValue <= 0.0f) {
                smear[i] = 1.0f;
                continue;
              }
              const float sf = i < scaleFactor.size() ? scaleFactor[i] : 1.0f;
              const float res = i < resolution.size() ? resolution[i] : 0.0f;
              const float genPt = i < genJetPt.size() ? genJetPt[i] : -1.0f;
              const bool matched =
                  genPt > 0.0f && std::fabs(ptValue - genPt) < 3.0f * ptValue * res;
              float smearFactor = 1.0f;
              if (matched) {
                smearFactor += (sf - 1.0f) * (ptValue - genPt) / ptValue;
              } else {
                const float stochastic = std::sqrt(std::max(sf * sf - 1.0f, 0.0f));
                const uint64_t etaBits = static_cast<uint64_t>(std::llround((eta[i] + 10.0f) * 10000.0f));
                const uint64_t seed = splitmix64(static_cast<uint64_t>(eventId) ^
                                                (static_cast<uint64_t>(i) << 32U) ^
                                                etaBits);
                smearFactor += stochastic * res * normalFromSeed(seed);
              }
              if (!std::isfinite(smearFactor) || smearFactor <= 0.0f)
                smearFactor = 1.0f;
              smear[i] = smearFactor;
            }
            return smear;
          },
          {inputPt, etaCol, genJetPtCol, eventCol, jerResCol, jerSfCol});
      dataManager_m->setDataFrame(newDf);
    }

    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputPt = step.inputPtColumn;
      const std::string outputPt = step.outputPtColumn;
      auto newDf = df.Define(
          outputPt,
          [](const ROOT::VecOps::RVec<Float_t> &pt,
             const ROOT::VecOps::RVec<Float_t> &smear)
              -> ROOT::VecOps::RVec<Float_t> {
            return pt * smear;
          },
          {inputPt, smearCol});
      dataManager_m->setDataFrame(newDf);
    }

    if (!step.inputMassColumn.empty() && !step.outputMassColumn.empty()) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string inputMass = step.inputMassColumn;
      const std::string outputMass = step.outputMassColumn;
      auto newDf = df.Define(
          outputMass,
          [](const ROOT::VecOps::RVec<Float_t> &mass,
             const ROOT::VecOps::RVec<Float_t> &smear)
              -> ROOT::VecOps::RVec<Float_t> {
            return mass * smear;
          },
          {inputMass, smearCol});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // 4. Apply each MET propagation step.
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

  // 5. Register explicit variation mappings for corrected collection inputs.
  for (const auto &colStep : collectionOutputSteps_m) {
    for (const auto &var : variations_m) {
      systematicManager_m->registerVariationColumns(
          colStep.correctedPtColumn, var.name, var.upPtColumn, var.downPtColumn);
      if (!colStep.correctedMassColumn.empty() && !var.upMassColumn.empty() &&
          !var.downMassColumn.empty()) {
        systematicManager_m->registerVariationColumns(
            colStep.correctedMassColumn, var.name, var.upMassColumn,
            var.downMassColumn);
      }
    }
  }

  // 6. Define PhysicsObjectCollection output columns.
  for (const auto &colStep : collectionOutputSteps_m) {
    const std::string inputCol  = inputJetCollectionColumn_m;
    const std::string corrPtCol = colStep.correctedPtColumn;
    const std::string outputCol = colStep.outputCollectionColumn;

    if (!colStep.correctedMassColumn.empty()) {
      const std::string corrMasCol = colStep.correctedMassColumn;
      dataManager_m->Define(
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
          {inputCol, corrPtCol, corrMasCol}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          outputCol,
          [](const PhysicsObjectCollection &col,
             const ROOT::VecOps::RVec<Float_t> &corrPt)
              -> PhysicsObjectCollection {
            return col.withCorrectedPt(corrPt);
          },
          {inputCol, corrPtCol}, *systematicManager_m);
    }
  }

  // 7. Define per-variation collection aliases and optional variation map.
  for (const auto &varColStep : variationCollectionsSteps_m) {
    const std::string nomCol   = varColStep.nominalCollectionColumn;
    const std::string prefix   = varColStep.collectionPrefix;
    const std::string mapCol   = varColStep.variationMapColumn;

    std::vector<std::string> varNames;
    std::vector<std::string> upColNames;
    std::vector<std::string> dnColNames;

    for (const auto &var : variations_m) {
      const std::string sourceUpCol =
          systematicManager_m->getVariationColumnName(nomCol, var.name + "Up");
      const std::string sourceDnCol =
          systematicManager_m->getVariationColumnName(nomCol, var.name + "Down");
      const std::string upCol = prefix + "_" + var.name + "Up";
      const std::string dnCol = prefix + "_" + var.name + "Down";

      if (upCol != sourceUpCol) {
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            upCol,
            [](const PhysicsObjectCollection &col) -> PhysicsObjectCollection {
              return col;
            },
            {sourceUpCol});
        dataManager_m->setDataFrame(newDf);
      }

      if (dnCol != sourceDnCol) {
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            dnCol,
            [](const PhysicsObjectCollection &col) -> PhysicsObjectCollection {
              return col;
            },
            {sourceDnCol});
        dataManager_m->setDataFrame(newDf);
      }

      systematicManager_m->registerVariationColumns(prefix, var.name, upCol,
                                                    dnCol);

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

  for (const auto &var : variations_m) {
    std::set<std::string> affected = {var.upPtColumn, var.downPtColumn};
    if (!var.upMassColumn.empty()) {
      affected.insert(var.upMassColumn);
    }
    if (!var.downMassColumn.empty()) {
      affected.insert(var.downMassColumn);
    }
    systematicManager_m->registerSystematic(var.name, affected);
  }

  correctionSteps_m.clear();
  jerSmearingSteps_m.clear();
  metPropagationSteps_m.clear();
  collectionOutputSteps_m.clear();
  variationCollectionsSteps_m.clear();
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

  if (!jerSmearingSteps_m.empty()) {
    ss << "  JER smearing steps (" << jerSmearingSteps_m.size() << "):\n";
    for (const auto &step : jerSmearingSteps_m) {
      ss << "    " << step.inputPtColumn
         << " -> " << step.outputPtColumn
         << " (systematic=" << step.systematic << ")";
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

  if (!genJetPtColumn_m.empty()) {
    entries["jer_gen_pt_column"] = genJetPtColumn_m;
    entries["jer_rho_column"] = rhoColumn_m;
    entries["jer_event_column"] = eventColumn_m;
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

  if (!jerSmearingSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < jerSmearingSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << jerSmearingSteps_m[i].inputPtColumn
         << "->" << jerSmearingSteps_m[i].outputPtColumn
         << "(syst:" << jerSmearingSteps_m[i].systematic << ')';
    }
    entries["jer_smearing_steps"] = ss.str();
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

std::shared_ptr<JetEnergyScaleManager> JetEnergyScaleManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<JetEnergyScaleManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
