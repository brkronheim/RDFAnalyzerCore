#include <TaggerWorkingPointManager.h>
#include <NullOutputSink.h>
#include <WeightManager.h>
#include <analyzer.h>
#include <ROOT/RVec.hxx>
#include <TFile.h>
#include <TH1D.h>
#include <api/ILogger.h>
#include <cmath>
#include <set>
#include <sstream>
#include <stdexcept>

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

std::shared_ptr<TaggerWorkingPointManager>
TaggerWorkingPointManager::create(Analyzer &an, const std::string &role) {
  auto plugin = std::make_shared<TaggerWorkingPointManager>();
  an.addPlugin(role, plugin);
  return plugin;
}

// ---------------------------------------------------------------------------
// setContext
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setContext(ManagerContext &ctx) {
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
  configManager_m = &ctx.config;
  metaSink_m = &ctx.metaSink;
}

// ---------------------------------------------------------------------------
// setObjectColumns
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setObjectColumns(const std::string &ptColumn,
                                                    const std::string &etaColumn,
                                                    const std::string &phiColumn,
                                                    const std::string &massColumn) {
  if (ptColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::setObjectColumns: ptColumn must not be empty");
  ptColumn_m = ptColumn;
  etaColumn_m = etaColumn;
  phiColumn_m = phiColumn;
  massColumn_m = massColumn;
}

// ---------------------------------------------------------------------------
// setTaggerColumn / setTaggerColumns
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setTaggerColumn(
    const std::string &taggerScoreColumn) {
  setTaggerColumns({taggerScoreColumn});
}

void TaggerWorkingPointManager::setTaggerColumns(
    const std::vector<std::string> &taggerScoreColumns) {
  if (taggerScoreColumns.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::setTaggerColumns: list must not be empty");
  for (const auto &col : taggerScoreColumns) {
    if (col.empty())
      throw std::invalid_argument(
          "TaggerWorkingPointManager::setTaggerColumns: column names must not "
          "be empty");
  }
  taggerColumns_m = taggerScoreColumns;
}

// ---------------------------------------------------------------------------
// addWorkingPoint
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::addWorkingPoint(const std::string &name,
                                                     float threshold) {
  addWorkingPoint(name, std::vector<float>{threshold});
}

void TaggerWorkingPointManager::addWorkingPoint(
    const std::string &name, const std::vector<float> &thresholds) {
  if (name.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addWorkingPoint: name must not be empty");
  if (thresholds.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addWorkingPoint: thresholds must not be "
        "empty");
  for (const auto &wp : workingPoints_m) {
    if (wp.name == name)
      throw std::invalid_argument(
          "TaggerWorkingPointManager::addWorkingPoint: duplicate WP name '" +
          name + "'");
  }
  // Enforce ascending order only for single-score WPs (multi-score ordering
  // is the user's responsibility).
  if (thresholds.size() == 1 && !workingPoints_m.empty() &&
      workingPoints_m.back().thresholds.size() == 1 &&
      thresholds[0] <= workingPoints_m.back().thresholds[0])
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addWorkingPoint: threshold must be "
        "strictly greater than the previous WP threshold");
  workingPoints_m.push_back({name, thresholds});
}

// ---------------------------------------------------------------------------
// setInputObjectCollection
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setInputObjectCollection(
    const std::string &collectionColumn) {
  if (collectionColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::setInputObjectCollection: column must not "
        "be empty");
  inputObjectCollectionColumn_m = collectionColumn;
}

// ---------------------------------------------------------------------------
// setFractionCorrection
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setFractionCorrection(
    CorrectionManager &cm, const std::string &fractionCorrectionName,
    const std::vector<std::string> &inputColumns) {
  if (fractionCorrectionName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::setFractionCorrection: "
        "fractionCorrectionName must not be empty");
  fractionCorrectionName_m = fractionCorrectionName;
  fractionInputColumns_m = inputColumns;
  hasFractionCorrection_m = true;

  // Apply the fraction correction immediately so the SF column is available
  // in the dataframe when execute() runs the weight steps.
  cm.applyCorrectionVec(fractionCorrectionName, {}, inputColumns);
}

// ---------------------------------------------------------------------------
// applyCorrectionlib
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::applyCorrectionlib(
    CorrectionManager &cm, const std::string &correctionName,
    const std::vector<std::string> &stringArgs,
    const std::vector<std::string> &inputColumns) {
  if (correctionName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::applyCorrectionlib: correctionName "
        "must not be empty");
  if (inputObjectCollectionColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::applyCorrectionlib: call "
        "setInputObjectCollection() first");

  // Evaluate the correctionlib to get a per-object SF column.
  cm.applyCorrectionVec(correctionName, stringArgs, inputColumns);

  // Derive the SF column name following CorrectionManager convention.
  std::string sfColumn = correctionName;
  for (const auto &arg : stringArgs)
    sfColumn += "_" + arg;

  // Build the output weight column name.
  std::string weightColumn = sfColumn + "_weight";

  weightSteps_m.push_back({sfColumn, weightColumn});
}

// ---------------------------------------------------------------------------
// registerSystematicSources / getSystematicSources
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::registerSystematicSources(
    const std::string &setName, const std::vector<std::string> &sources) {
  if (setName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::registerSystematicSources: setName "
        "must not be empty");
  if (sources.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::registerSystematicSources: sources "
        "must not be empty");
  for (const auto &s : sources) {
    if (s.empty())
      throw std::invalid_argument(
          "TaggerWorkingPointManager::registerSystematicSources: source "
          "name must not be empty");
  }
  systematicSets_m[setName] = sources;
}

const std::vector<std::string> &
TaggerWorkingPointManager::getSystematicSources(
    const std::string &setName) const {
  auto it = systematicSets_m.find(setName);
  if (it == systematicSets_m.end())
    throw std::out_of_range(
        "TaggerWorkingPointManager::getSystematicSources: unknown set '" +
        setName + "'");
  return it->second;
}

// ---------------------------------------------------------------------------
// applySystematicSet
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::applySystematicSet(
    CorrectionManager &cm, const std::string &correctionName,
    const std::string &setName,
    const std::vector<std::string> &inputColumns) {
  if (correctionName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::applySystematicSet: correctionName "
        "must not be empty");
  if (setName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::applySystematicSet: setName "
        "must not be empty");

  const auto &sources = getSystematicSources(setName);

  for (const auto &source : sources) {
    // Up variation
    cm.applyCorrectionVec(correctionName, {source, "up"}, inputColumns);
    const std::string sfUp = correctionName + "_" + source + "_up";
    const std::string weightUp = sfUp + "_weight";

    // Down variation
    cm.applyCorrectionVec(correctionName, {source, "down"}, inputColumns);
    const std::string sfDown = correctionName + "_" + source + "_down";
    const std::string weightDown = sfDown + "_weight";

    weightSteps_m.push_back({sfUp,   weightUp});
    weightSteps_m.push_back({sfDown, weightDown});

    // Register variation entry (stores SF columns for provenance/metadata).
    // Weight columns are tracked via weightSteps_m.
    variations_m.push_back({source, sfUp, sfDown, weightUp, weightDown});
  }
}

// ---------------------------------------------------------------------------
// addVariation
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::addVariation(
    const std::string &systematicName,
    const std::string &upSFColumn,
    const std::string &downSFColumn,
    const std::string &upWeightColumn,
    const std::string &downWeightColumn) {
  if (systematicName.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addVariation: systematicName must not "
        "be empty");
  if (upSFColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addVariation: upSFColumn must not be "
        "empty");
  if (downSFColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addVariation: downSFColumn must not "
        "be empty");

  const std::string upWt   = upWeightColumn.empty()   ? upSFColumn   + "_weight" : upWeightColumn;
  const std::string downWt = downWeightColumn.empty() ? downSFColumn + "_weight" : downWeightColumn;

  variations_m.push_back({systematicName, upSFColumn, downSFColumn, upWt, downWt});

  // Only schedule weight steps if they haven't already been added (e.g. by
  // applySystematicSet).
  auto alreadyScheduled = [&](const std::string &wCol) {
    for (const auto &ws : weightSteps_m)
      if (ws.outputWeightColumn == wCol) return true;
    return false;
  };

  if (!alreadyScheduled(upWt))
    weightSteps_m.push_back({upSFColumn, upWt});
  if (!alreadyScheduled(downWt))
    weightSteps_m.push_back({downSFColumn, downWt});
}

// ---------------------------------------------------------------------------
// defineWorkingPointCollection
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineWorkingPointCollection(
    const std::string &selection, const std::string &outputCollectionColumn) {
  if (selection.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineWorkingPointCollection: "
        "selection must not be empty");
  if (outputCollectionColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineWorkingPointCollection: "
        "outputCollectionColumn must not be empty");
  if (inputObjectCollectionColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineWorkingPointCollection: call "
        "setInputObjectCollection() first");

  wpCollectionSteps_m.push_back({parseSelection(selection), outputCollectionColumn});
}

// ---------------------------------------------------------------------------
// defineVariationCollections
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineVariationCollections(
    const std::string &nominalCollectionColumn,
    const std::string &collectionPrefix,
    const std::string &variationMapColumn) {
  if (nominalCollectionColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineVariationCollections: "
        "nominalCollectionColumn must not be empty");
  if (collectionPrefix.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineVariationCollections: "
        "collectionPrefix must not be empty");
  if (inputObjectCollectionColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineVariationCollections: call "
        "setInputObjectCollection() first");
  variationCollectionsSteps_m.push_back(
      {nominalCollectionColumn, collectionPrefix, variationMapColumn});
}

// ---------------------------------------------------------------------------
// defineUnfilteredCollection
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineUnfilteredCollection(
    const std::string &outputCollectionColumn) {
  if (outputCollectionColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineUnfilteredCollection: "
        "outputCollectionColumn must not be empty");
  if (inputObjectCollectionColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineUnfilteredCollection: call "
        "setInputObjectCollection() first");

  WPCollectionSelection sel;
  sel.type = WPCollectionSelection::Type::AllObjects;
  wpCollectionSteps_m.push_back({sel, outputCollectionColumn});
}

// ---------------------------------------------------------------------------
// setupFromConfigFile
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setupFromConfigFile() {
  if (!configManager_m)
    throw std::runtime_error(
        "TaggerWorkingPointManager::setupFromConfigFile: context not set");

  // Prefer role-specific key, fall back to generic "taggerConfig".
  std::string configFile = configManager_m->get(role_m + "Config");
  if (configFile.empty())
    configFile = configManager_m->get("taggerConfig");
  if (configFile.empty())
    return; // config is optional

  const auto entries =
      configManager_m->parseMultiKeyConfig(configFile, {"type"});

  for (const auto &entry : entries) {
    const std::string &blockType = entry.at("type");

    if (blockType == "working_points") {
      if (entry.count("taggerColumns")) {
        const auto cols =
            configManager_m->splitString(entry.at("taggerColumns"), ",");
        setTaggerColumns(cols);
      }
      if (entry.count("wpNames") && entry.count("wpThresholds")) {
        const auto names =
            configManager_m->splitString(entry.at("wpNames"), ",");
        const auto threshStrs =
            configManager_m->splitString(entry.at("wpThresholds"), ",");
        for (std::size_t i = 0;
             i < names.size() && i < threshStrs.size(); ++i) {
          const auto parts =
              configManager_m->splitString(threshStrs[i], ":");
          if (parts.size() == 1) {
            addWorkingPoint(names[i], std::stof(parts[0]));
          } else {
            std::vector<float> thresholds;
            for (const auto &p : parts)
              thresholds.push_back(std::stof(p));
            addWorkingPoint(names[i], thresholds);
          }
        }
      }
    } else if (blockType == "correction") {
      ConfiguredCorrection cc;
      if (entry.count("correctionName"))
        cc.correctionName = entry.at("correctionName");
      if (entry.count("stringArgs"))
        cc.stringArgs =
            configManager_m->splitString(entry.at("stringArgs"), ",");
      if (entry.count("inputColumns"))
        cc.inputColumns =
            configManager_m->splitString(entry.at("inputColumns"), ",");
      configuredCorrections_m.push_back(std::move(cc));
    } else if (blockType == "systematics") {
      if (entry.count("setName") && entry.count("sources")) {
        const auto sources =
            configManager_m->splitString(entry.at("sources"), ",");
        registerSystematicSources(entry.at("setName"), sources);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// applyConfiguredCorrections
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::applyConfiguredCorrections(
    CorrectionManager &cm) {
  for (const auto &cc : configuredCorrections_m) {
    applyCorrectionlib(cm, cc.correctionName, cc.stringArgs, cc.inputColumns);
  }
}

// ---------------------------------------------------------------------------
// registerWeightsWithWeightManager
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::registerWeightsWithWeightManager(
    WeightManager &wm, const std::string &nominalSFName,
    const std::string &nominalWeightColumn, bool registerVariations) {
  wm.addScaleFactor(nominalSFName, nominalWeightColumn);
  if (registerVariations) {
    for (const auto &var : variations_m) {
      wm.addWeightVariation(var.name, var.upWeightColumn,
                            var.downWeightColumn);
    }
  }
}

// ---------------------------------------------------------------------------
// defineFractionHistograms
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineFractionHistograms(
    const std::string &outputPrefix,
    const std::vector<float> &ptBinEdges,
    const std::vector<float> &etaBinEdges,
    const std::string &flavourColumn) {
  if (outputPrefix.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineFractionHistograms: outputPrefix "
        "must not be empty");
  if (ptBinEdges.size() < 2)
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineFractionHistograms: ptBinEdges "
        "must have at least 2 elements");
  if (etaBinEdges.size() < 2)
    throw std::invalid_argument(
        "TaggerWorkingPointManager::defineFractionHistograms: etaBinEdges "
        "must have at least 2 elements");
  if (taggerColumns_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineFractionHistograms: call "
        "setTaggerColumn(s)() first");
  if (ptColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineFractionHistograms: call "
        "setObjectColumns() first");
  if (inputObjectCollectionColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineFractionHistograms: call "
        "setInputObjectCollection() first");

  fractionHistogramConfigs_m.push_back(
      {outputPrefix, ptBinEdges, etaBinEdges, flavourColumn});
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

std::size_t TaggerWorkingPointManager::wpIndex(
    const std::string &name) const {
  for (std::size_t i = 0; i < workingPoints_m.size(); ++i) {
    if (workingPoints_m[i].name == name)
      return i;
  }
  throw std::invalid_argument(
      "TaggerWorkingPointManager: unknown working point '" + name + "'");
}

std::string TaggerWorkingPointManager::wpCategoryColumn() const {
  return ptColumn_m + "_wp_category";
}

std::string TaggerWorkingPointManager::getWPCategoryColumn() const {
  return wpCategoryColumn();
}

WPCollectionSelection TaggerWorkingPointManager::parseSelection(
    const std::string &selection) const {
  WPCollectionSelection sel;

  // "pass_<wp>" or "pass_<wp1>_fail_<wp2>"
  if (selection.size() > 5 && selection.substr(0, 5) == "pass_") {
    const std::string rest = selection.substr(5);
    const auto failPos = rest.find("_fail_");
    if (failPos != std::string::npos) {
      // "pass_<wp1>_fail_<wp2>"
      const std::string wp1 = rest.substr(0, failPos);
      const std::string wp2 = rest.substr(failPos + 6);
      wpIndex(wp1);  // validate
      wpIndex(wp2);  // validate
      const std::size_t idx1 = wpIndex(wp1);
      const std::size_t idx2 = wpIndex(wp2);
      if (idx1 >= idx2)
        throw std::invalid_argument(
            "TaggerWorkingPointManager: in 'pass_" + wp1 + "_fail_" + wp2 +
            "', '" + wp1 + "' must have a lower threshold than '" + wp2 + "'");
      sel.type = WPCollectionSelection::Type::PassRangeWP;
      sel.wpNameLower = wp1;
      sel.wpNameUpper = wp2;
    } else {
      // "pass_<wp>"
      wpIndex(rest);  // validate
      sel.type = WPCollectionSelection::Type::PassWP;
      sel.wpNameLower = rest;
    }
    return sel;
  }

  // "fail_<wp>"
  if (selection.size() > 5 && selection.substr(0, 5) == "fail_") {
    const std::string wpName = selection.substr(5);
    wpIndex(wpName);  // validate
    sel.type = WPCollectionSelection::Type::FailWP;
    sel.wpNameLower = wpName;
    return sel;
  }

  throw std::invalid_argument(
      "TaggerWorkingPointManager: unrecognised selection '" + selection +
      "'. Expected 'pass_<wp>', 'fail_<wp>', or 'pass_<wp1>_fail_<wp2>'.");
}

// ---------------------------------------------------------------------------
// defineWeightColumn — private helper called from execute()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineWeightColumn(
    const std::string &perObjectSFColumn,
    const std::string &outputWeightColumn) {
  ROOT::RDF::RNode df = dataManager_m->getDataFrame();

  if (!hasFractionCorrection_m) {
    // Simple product of per-object SFs over objects in the input collection.
    const std::string collectionCol = inputObjectCollectionColumn_m;
    auto newDf = df.Define(
        outputWeightColumn,
        [](const PhysicsObjectCollection &jets,
           const ROOT::VecOps::RVec<Float_t> &sf) -> Float_t {
          Float_t weight = 1.0f;
          for (std::size_t i = 0; i < jets.size(); ++i) {
            const Int_t idx = jets.index(i);
            if (idx >= 0 && static_cast<std::size_t>(idx) < sf.size()) {
              weight *= sf[static_cast<std::size_t>(idx)];
            }
          }
          return weight;
        },
        {collectionCol, perObjectSFColumn});
    dataManager_m->setDataFrame(newDf);
  } else {
    // Fraction-weighted: divide each per-object SF by the MC fraction for the
    // jet's WP category so the total weight reflects the generator-level
    // distribution.
    const std::string collectionCol = inputObjectCollectionColumn_m;
    // The fraction column was registered under fractionCorrectionName_m
    // (no string args appended since we called with empty stringArgs).
    const std::string fracCol = fractionCorrectionName_m;
    auto newDf = df.Define(
        outputWeightColumn,
        [](const PhysicsObjectCollection &jets,
           const ROOT::VecOps::RVec<Float_t> &sf,
           const ROOT::VecOps::RVec<Float_t> &fraction) -> Float_t {
          Float_t weight = 1.0f;
          for (std::size_t i = 0; i < jets.size(); ++i) {
            const Int_t idx = jets.index(i);
            if (idx >= 0 && static_cast<std::size_t>(idx) < sf.size()) {
              const std::size_t j = static_cast<std::size_t>(idx);
              const Float_t frac = (j < fraction.size() && fraction[j] > 0.0f)
                                       ? fraction[j]
                                       : 1.0f;
              weight *= sf[j] / frac;
            }
          }
          return weight;
        },
        {collectionCol, perObjectSFColumn, fracCol});
    dataManager_m->setDataFrame(newDf);
  }
}

// ---------------------------------------------------------------------------
// defineWPCategoryColumn() — private helper called from execute()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::defineWPCategoryColumn() {
  const std::string catCol = wpCategoryColumn();
  const std::vector<WorkingPointEntry> wps = workingPoints_m;

  if (taggerColumns_m.size() == 1) {
    // -----------------------------------------------------------------------
    // Single-score path (fast path — existing logic, no intermediate columns).
    // -----------------------------------------------------------------------
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string taggerCol = taggerColumns_m[0];

    auto newDf = df.Define(
        catCol,
        [wps](const ROOT::VecOps::RVec<Float_t> &score)
            -> ROOT::VecOps::RVec<Int_t> {
          ROOT::VecOps::RVec<Int_t> cat(score.size(), 0);
          for (std::size_t i = 0; i < score.size(); ++i) {
            Int_t c = 0;
            for (const auto &wp : wps) {
              if (score[i] >= wp.thresholds[0])
                ++c;
              else
                break;
            }
            cat[i] = c;
          }
          return cat;
        },
        {taggerCol});
    dataManager_m->setDataFrame(newDf);

  } else {
    // -----------------------------------------------------------------------
    // Multi-score path: pack all discriminant scores per object into a
    // RVec<RVec<Float_t>> intermediate column, then compute category.
    //
    // The packed column is named "_twm_packed_<catCol>" so multiple instances
    // of the plugin don't collide.
    // -----------------------------------------------------------------------
    const std::string packedCol = "_twm_packed_" + catCol;
    const std::size_t numScores = taggerColumns_m.size();

    // Step A: initialise packed column from first tagger score.
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string firstCol = taggerColumns_m[0];
      auto newDf = df.Define(
          packedCol,
          [](const ROOT::VecOps::RVec<Float_t> &s)
              -> ROOT::VecOps::RVec<ROOT::VecOps::RVec<Float_t>> {
            ROOT::VecOps::RVec<ROOT::VecOps::RVec<Float_t>> result(s.size());
            for (std::size_t i = 0; i < s.size(); ++i)
              result[i] = ROOT::VecOps::RVec<Float_t>{s[i]};
            return result;
          },
          {firstCol});
      dataManager_m->setDataFrame(newDf);
    }

    // Step B: append each additional tagger score column.
    for (std::size_t k = 1; k < taggerColumns_m.size(); ++k) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string scoreCol = taggerColumns_m[k];
      auto newDf = df.Redefine(
          packedCol,
          [](ROOT::VecOps::RVec<ROOT::VecOps::RVec<Float_t>> packed,
             const ROOT::VecOps::RVec<Float_t> &s)
              -> ROOT::VecOps::RVec<ROOT::VecOps::RVec<Float_t>> {
            // Both columns must have the same number of objects per event.
            // Size mismatches indicate a configuration error (mismatched
            // tagger columns or incorrectly sized branches).
            if (packed.size() != s.size())
              throw std::runtime_error(
                  "TaggerWorkingPointManager: tagger score columns have "
                  "different lengths per event — ensure all setTaggerColumns() "
                  "columns have one entry per object");
            for (std::size_t i = 0; i < packed.size(); ++i)
              packed[i].push_back(s[i]);
            return packed;
          },
          {packedCol, scoreCol});
      dataManager_m->setDataFrame(newDf);
    }

    // Step C: compute category from packed scores.
    {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      auto newDf = df.Define(
          catCol,
          [wps, numScores](
              const ROOT::VecOps::RVec<ROOT::VecOps::RVec<Float_t>> &packed)
              -> ROOT::VecOps::RVec<Int_t> {
            ROOT::VecOps::RVec<Int_t> cat(packed.size(), 0);
            for (std::size_t i = 0; i < packed.size(); ++i) {
              const auto &scores = packed[i];
              Int_t c = 0;
              for (const auto &wp : wps) {
                bool passes = true;
                for (std::size_t k = 0; k < numScores; ++k) {
                  if (k >= scores.size() ||
                      scores[k] < wp.thresholds[k]) {
                    passes = false;
                    break;
                  }
                }
                if (passes)
                  ++c;
                else
                  break;
              }
              cat[i] = c;
            }
            return cat;
          },
          {packedCol});
      dataManager_m->setDataFrame(newDf);
    }
  }
}

// ---------------------------------------------------------------------------
// execute()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error(
        "TaggerWorkingPointManager::execute: context not set");

  // -------------------------------------------------------------------------
  // 1. Define per-object WP category column.
  // -------------------------------------------------------------------------
  if (!taggerColumns_m.empty() && !workingPoints_m.empty()) {
    defineWPCategoryColumn();
  }

  // -------------------------------------------------------------------------
  // 2. Define per-event weight columns for each scheduled SF step.
  // -------------------------------------------------------------------------
  for (const auto &step : weightSteps_m) {
    defineWeightColumn(step.perObjectSFColumn, step.outputWeightColumn);
  }

  // -------------------------------------------------------------------------
  // 3. Define WP-filtered PhysicsObjectCollection columns.
  // -------------------------------------------------------------------------
  for (const auto &step : wpCollectionSteps_m) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string inputCol = inputObjectCollectionColumn_m;
    const std::string outputCol = step.outputCollectionColumn;
    const WPCollectionSelection sel = step.selection;
    const std::string catCol = wpCategoryColumn();

    if (sel.type == WPCollectionSelection::Type::PassWP) {
      // Pass: category >= minCat (1-based index of the named WP)
      // The mask is built over the collection's jets (using their original
      // indices to look up the category in the full-branch array).
      const std::size_t minCat = wpIndex(sel.wpNameLower) + 1;
      auto newDf = df.Define(
          outputCol,
          [minCat](const PhysicsObjectCollection &jets,
                   const ROOT::VecOps::RVec<Int_t> &category)
              -> PhysicsObjectCollection {
            ROOT::VecOps::RVec<bool> mask(jets.size(), false);
            for (std::size_t i = 0; i < jets.size(); ++i) {
              const Int_t idx = jets.index(i);
              if (idx >= 0 && static_cast<std::size_t>(idx) < category.size())
                mask[i] = static_cast<std::size_t>(
                               category[static_cast<std::size_t>(idx)]) >= minCat;
            }
            return jets.withFilter(mask);
          },
          {inputCol, catCol});
      dataManager_m->setDataFrame(newDf);

    } else if (sel.type == WPCollectionSelection::Type::FailWP) {
      // Fail: category < minCat (strictly fails the named WP)
      const std::size_t minCat = wpIndex(sel.wpNameLower) + 1;
      auto newDf = df.Define(
          outputCol,
          [minCat](const PhysicsObjectCollection &jets,
                   const ROOT::VecOps::RVec<Int_t> &category)
              -> PhysicsObjectCollection {
            ROOT::VecOps::RVec<bool> mask(jets.size(), false);
            for (std::size_t i = 0; i < jets.size(); ++i) {
              const Int_t idx = jets.index(i);
              if (idx >= 0 && static_cast<std::size_t>(idx) < category.size())
                mask[i] = static_cast<std::size_t>(
                               category[static_cast<std::size_t>(idx)]) < minCat;
            }
            return jets.withFilter(mask);
          },
          {inputCol, catCol});
      dataManager_m->setDataFrame(newDf);

    } else if (sel.type == WPCollectionSelection::Type::PassRangeWP) {
      // PassRangeWP: pass lower WP, fail upper WP
      const std::size_t minCat = wpIndex(sel.wpNameLower) + 1;
      const std::size_t maxCat = wpIndex(sel.wpNameUpper); // exclusive upper bound
      auto newDf = df.Define(
          outputCol,
          [minCat, maxCat](const PhysicsObjectCollection &jets,
                           const ROOT::VecOps::RVec<Int_t> &category)
              -> PhysicsObjectCollection {
            ROOT::VecOps::RVec<bool> mask(jets.size(), false);
            for (std::size_t i = 0; i < jets.size(); ++i) {
              const Int_t idx = jets.index(i);
              if (idx >= 0 && static_cast<std::size_t>(idx) < category.size()) {
                const auto c = static_cast<std::size_t>(
                    category[static_cast<std::size_t>(idx)]);
                mask[i] = (c >= minCat && c <= maxCat);
              }
            }
            return jets.withFilter(mask);
          },
          {inputCol, catCol});
      dataManager_m->setDataFrame(newDf);

    } else { // AllObjects: copy input collection unchanged
      auto newDf = df.Define(
          outputCol,
          [](const PhysicsObjectCollection &col) -> PhysicsObjectCollection {
            return col;
          },
          {inputCol});
      dataManager_m->setDataFrame(newDf);
    }
  }

  // -------------------------------------------------------------------------
  // 4. Define variation collection columns + variation map.
  // -------------------------------------------------------------------------
  for (const auto &vstep : variationCollectionsSteps_m) {
    const std::string nomCol = vstep.nominalCollectionColumn;
    const std::string prefix = vstep.collectionPrefix;
    const std::string mapCol = vstep.variationMapColumn;

    // For tagger WP corrections, jet kinematics don't change.
    // Variation collections are aliases of the nominal collection;
    // only the event weight differs.
    std::vector<std::string> varNames;
    std::vector<std::string> upColNames;
    std::vector<std::string> dnColNames;

    for (const auto &var : variations_m) {
      const std::string upCol = prefix + "_" + var.name + "Up";
      const std::string dnCol = prefix + "_" + var.name + "Down";

      // Define as aliases of the nominal collection.
      {
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            upCol,
            [](const PhysicsObjectCollection &col) -> PhysicsObjectCollection {
              return col;
            },
            {nomCol});
        dataManager_m->setDataFrame(newDf);
      }
      {
        ROOT::RDF::RNode df = dataManager_m->getDataFrame();
        auto newDf = df.Define(
            dnCol,
            [](const PhysicsObjectCollection &col) -> PhysicsObjectCollection {
              return col;
            },
            {nomCol});
        dataManager_m->setDataFrame(newDf);
      }

      // Tell ISystematicManager about the explicit column names.
      systematicManager_m->registerVariationColumns(
          prefix, var.name, upCol, dnCol);

      varNames.push_back(var.name);
      upColNames.push_back(upCol);
      dnColNames.push_back(dnCol);
    }

    // Build the PhysicsObjectVariationMap using the same fold-redefine
    // pattern as JetEnergyScaleManager.
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

  // -------------------------------------------------------------------------
  // 5. Register all systematic variations with ISystematicManager.
  // -------------------------------------------------------------------------
  for (const auto &var : variations_m) {
    std::set<std::string> affected;
    affected.insert(var.upWeightColumn);
    affected.insert(var.downWeightColumn);
    // Also include the per-object SF columns so downstream consumers can track them.
    affected.insert(var.upSFColumn);
    affected.insert(var.downSFColumn);
    systematicManager_m->registerSystematic(var.name, affected);
  }

  // -------------------------------------------------------------------------
  // 6. Book fraction histograms (pre-processing mode).
  // -------------------------------------------------------------------------
  for (const auto &cfg : fractionHistogramConfigs_m) {
    const std::size_t nPt  = cfg.ptBinEdges.size() - 1;
    const std::size_t nEta = cfg.etaBinEdges.size() - 1;
    const bool hasFlavour = !cfg.flavourColumn.empty();

    // Flavour labels and integer codes (hadronFlavour: 5=b, 4=c, else=light).
    const std::vector<std::string> flavourLabels =
        hasFlavour ? std::vector<std::string>{"b", "c", "light"}
                   : std::vector<std::string>{""};

    for (std::size_t iPt = 0; iPt < nPt; ++iPt) {
      for (std::size_t iEta = 0; iEta < nEta; ++iEta) {
        for (std::size_t iFl = 0; iFl < flavourLabels.size(); ++iFl) {
          const float ptLow   = cfg.ptBinEdges[iPt];
          const float ptHigh  = cfg.ptBinEdges[iPt + 1];
          const float etaLow  = cfg.etaBinEdges[iEta];
          const float etaHigh = cfg.etaBinEdges[iEta + 1];
          // flavTarget: 5=b, 4=c, -1=light (anything else)
          const int flavTarget = hasFlavour
              ? (iFl == 0 ? 5 : (iFl == 1 ? 4 : -1))
              : 0;

          // Build histogram name.
          std::ostringstream hname;
          hname << cfg.outputPrefix << "_pt" << iPt << "_eta" << iEta;
          if (hasFlavour) hname << "_" << flavourLabels[iFl];
          const std::string histName = hname.str();

          // Build an intermediate column with the selected jet scores.
          // For multi-score WPs, use the first tagger column for the fraction
          // histogram (the primary discriminant).
          const std::string scoreSelCol = "_frac_score_" + histName;
          const std::string taggerCol   = taggerColumns_m[0];
          const std::string collectionCol = inputObjectCollectionColumn_m;
          const std::string flavCol     = cfg.flavourColumn;

          if (!hasFlavour) {
            ROOT::RDF::RNode df = dataManager_m->getDataFrame();
            auto newDf = df.Define(
                scoreSelCol,
                [ptLow, ptHigh, etaLow, etaHigh](
                    const PhysicsObjectCollection &jets,
                    const ROOT::VecOps::RVec<Float_t> &tagger)
                    -> ROOT::VecOps::RVec<Float_t> {
                  ROOT::VecOps::RVec<Float_t> scores;
                  for (std::size_t i = 0; i < jets.size(); ++i) {
                    const Float_t pt  = static_cast<Float_t>(jets.at(i).Pt());
                    const Float_t eta = static_cast<Float_t>(
                        std::fabs(jets.at(i).Eta()));
                    if (pt < ptLow || pt >= ptHigh) continue;
                    if (eta < etaLow || eta >= etaHigh) continue;
                    const Int_t idx = jets.index(i);
                    if (idx >= 0 &&
                        static_cast<std::size_t>(idx) < tagger.size())
                      scores.push_back(
                          tagger[static_cast<std::size_t>(idx)]);
                  }
                  return scores;
                },
                {collectionCol, taggerCol});
            dataManager_m->setDataFrame(newDf);
          } else {
            ROOT::RDF::RNode df = dataManager_m->getDataFrame();
            auto newDf = df.Define(
                scoreSelCol,
                [ptLow, ptHigh, etaLow, etaHigh, flavTarget](
                    const PhysicsObjectCollection &jets,
                    const ROOT::VecOps::RVec<Float_t> &tagger,
                    const ROOT::VecOps::RVec<Int_t> &flavour)
                    -> ROOT::VecOps::RVec<Float_t> {
                  ROOT::VecOps::RVec<Float_t> scores;
                  for (std::size_t i = 0; i < jets.size(); ++i) {
                    const Float_t pt  = static_cast<Float_t>(jets.at(i).Pt());
                    const Float_t eta = static_cast<Float_t>(
                        std::fabs(jets.at(i).Eta()));
                    if (pt < ptLow || pt >= ptHigh) continue;
                    if (eta < etaLow || eta >= etaHigh) continue;
                    const Int_t idx = jets.index(i);
                    if (idx < 0 ||
                        static_cast<std::size_t>(idx) >= tagger.size())
                      continue;
                    const std::size_t j = static_cast<std::size_t>(idx);
                    const Int_t fl = (j < flavour.size()) ? flavour[j] : 0;
                    const bool matchFlavour =
                        (flavTarget == 5  && fl == 5) ||
                        (flavTarget == 4  && fl == 4) ||
                        (flavTarget == -1 && fl != 5 && fl != 4);
                    if (!matchFlavour) continue;
                    scores.push_back(tagger[j]);
                  }
                  return scores;
                },
                {collectionCol, taggerCol, flavCol});
            dataManager_m->setDataFrame(newDf);
          }

          // Book a 1D histogram of tagger scores in [0, 1].
          ROOT::RDF::RNode dfHist = dataManager_m->getDataFrame();
          auto model = ROOT::RDF::TH1DModel(
              histName.c_str(),
              (histName + ";tagger score;jets").c_str(),
              100, 0.0, 1.0);
          auto hPtr = dfHist.Histo1D(model, scoreSelCol);
          fractionHistResults_m.push_back({histName, std::move(hPtr)});

          // Book a 1D histogram of WP category (0..N) — always, when WPs are defined.
          if (!workingPoints_m.empty()) {
            const std::string catHistName =
                cfg.outputPrefix + "_cat_pt" + std::to_string(iPt) +
                "_eta" + std::to_string(iEta) +
                (hasFlavour ? "_" + flavourLabels[iFl] : "");
            const std::string catSelCol = "_frac_cat_" + catHistName;
            const std::string catColName = wpCategoryColumn();

            if (!hasFlavour) {
              ROOT::RDF::RNode dfCat = dataManager_m->getDataFrame();
              auto newDfCat = dfCat.Define(
                  catSelCol,
                  [ptLow, ptHigh, etaLow, etaHigh](
                      const PhysicsObjectCollection &jets,
                      const ROOT::VecOps::RVec<Int_t> &category)
                      -> ROOT::VecOps::RVec<Float_t> {
                    ROOT::VecOps::RVec<Float_t> cats;
                    for (std::size_t i = 0; i < jets.size(); ++i) {
                      const Float_t pt =
                          static_cast<Float_t>(jets.at(i).Pt());
                      const Float_t eta = static_cast<Float_t>(
                          std::fabs(jets.at(i).Eta()));
                      if (pt < ptLow || pt >= ptHigh) continue;
                      if (eta < etaLow || eta >= etaHigh) continue;
                      const Int_t idx = jets.index(i);
                      if (idx >= 0 &&
                          static_cast<std::size_t>(idx) < category.size())
                        cats.push_back(static_cast<Float_t>(
                            category[static_cast<std::size_t>(idx)]));
                    }
                    return cats;
                  },
                  {collectionCol, catColName});
              dataManager_m->setDataFrame(newDfCat);
            } else {
              ROOT::RDF::RNode dfCat = dataManager_m->getDataFrame();
              auto newDfCat = dfCat.Define(
                  catSelCol,
                  [ptLow, ptHigh, etaLow, etaHigh, flavTarget](
                      const PhysicsObjectCollection &jets,
                      const ROOT::VecOps::RVec<Int_t> &category,
                      const ROOT::VecOps::RVec<Int_t> &flavour)
                      -> ROOT::VecOps::RVec<Float_t> {
                    ROOT::VecOps::RVec<Float_t> cats;
                    for (std::size_t i = 0; i < jets.size(); ++i) {
                      const Float_t pt =
                          static_cast<Float_t>(jets.at(i).Pt());
                      const Float_t eta = static_cast<Float_t>(
                          std::fabs(jets.at(i).Eta()));
                      if (pt < ptLow || pt >= ptHigh) continue;
                      if (eta < etaLow || eta >= etaHigh) continue;
                      const Int_t idx = jets.index(i);
                      if (idx < 0 ||
                          static_cast<std::size_t>(idx) >= category.size())
                        continue;
                      const std::size_t j = static_cast<std::size_t>(idx);
                      const Int_t fl =
                          (j < flavour.size()) ? flavour[j] : 0;
                      const bool matchFlavour =
                          (flavTarget == 5  && fl == 5) ||
                          (flavTarget == 4  && fl == 4) ||
                          (flavTarget == -1 && fl != 5 && fl != 4);
                      if (!matchFlavour) continue;
                      cats.push_back(static_cast<Float_t>(
                          category[static_cast<std::size_t>(idx)]));
                    }
                    return cats;
                  },
                  {collectionCol, catColName, flavCol});
              dataManager_m->setDataFrame(newDfCat);
            }

            // N+1 bins from 0 to N+1 to cover categories 0..N.
            const int nCatBins =
                static_cast<int>(workingPoints_m.size()) + 1;
            ROOT::RDF::RNode dfCatHist = dataManager_m->getDataFrame();
            auto catModel = ROOT::RDF::TH1DModel(
                catHistName.c_str(),
                (catHistName + ";WP category;jets").c_str(),
                nCatBins, 0.0, static_cast<double>(nCatBins));
            auto catHPtr = dfCatHist.Histo1D(catModel, catSelCol);
            fractionHistResults_m.push_back({catHistName, std::move(catHPtr)});
          }
        }
      }
    }
  }
}

// ---------------------------------------------------------------------------
// finalize()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::finalize() {
  if (fractionHistResults_m.empty()) return;

  // Skip writing if running against a NullOutputSink (e.g. unit tests).
  if (dynamic_cast<NullOutputSink *>(metaSink_m) != nullptr) return;

  if (!configManager_m) return;
  const std::string fileName =
      metaSink_m->resolveOutputFile(*configManager_m, OutputChannel::Meta);
  if (fileName.empty()) return;

  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Error,
                    "TaggerWorkingPointManager: failed to open meta output "
                    "file: " + fileName);
    }
    return;
  }

  // Write each fraction histogram.
  TDirectory *fragDir = outFile.mkdir("tagger_fractions", "Tagger fraction histograms");
  if (!fragDir) fragDir = outFile.GetDirectory("tagger_fractions");

  for (auto &entry : fractionHistResults_m) {
    const TH1D &histRef = entry.result.GetValue();
    // Clone to set directory ownership without modifying the cached result.
    TH1D *histClone = static_cast<TH1D *>(histRef.Clone(entry.name.c_str()));
    if (histClone) {
      histClone->SetDirectory(fragDir);
      fragDir->cd();
      histClone->Write(entry.name.c_str(), TObject::kOverwrite);
      delete histClone;
    }
  }
  outFile.Close();
}

// ---------------------------------------------------------------------------
// reportMetadata()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::reportMetadata() {
  if (!logger_m) return;

  std::ostringstream ss;
  ss << "TaggerWorkingPointManager configuration:\n";

  if (!ptColumn_m.empty()) {
    ss << "  Object columns: pt=" << ptColumn_m
       << " eta=" << etaColumn_m
       << " phi=" << phiColumn_m
       << " mass=" << massColumn_m << "\n";
  }

  if (!taggerColumns_m.empty()) {
    if (taggerColumns_m.size() == 1) {
      ss << "  Tagger column: " << taggerColumns_m[0] << "\n";
    } else {
      ss << "  Tagger columns (" << taggerColumns_m.size() << "):\n";
      for (const auto &col : taggerColumns_m)
        ss << "    " << col << "\n";
    }
  }

  if (!workingPoints_m.empty()) {
    ss << "  Working points (" << workingPoints_m.size() << "):\n";
    for (std::size_t i = 0; i < workingPoints_m.size(); ++i) {
      ss << "    [" << i + 1 << "] " << workingPoints_m[i].name
         << " thresholds={";
      for (std::size_t k = 0; k < workingPoints_m[i].thresholds.size(); ++k) {
        if (k > 0) ss << ", ";
        ss << workingPoints_m[i].thresholds[k];
      }
      ss << "}\n";
    }
    ss << "  WP category column: " << wpCategoryColumn() << "\n";
  }

  if (!inputObjectCollectionColumn_m.empty())
    ss << "  Input object collection: " << inputObjectCollectionColumn_m << "\n";

  if (hasFractionCorrection_m)
    ss << "  Fraction correction: " << fractionCorrectionName_m << "\n";

  if (!weightSteps_m.empty()) {
    ss << "  Weight columns (" << weightSteps_m.size() << "):\n";
    for (const auto &ws : weightSteps_m) {
      ss << "    " << ws.perObjectSFColumn << " -> " << ws.outputWeightColumn << "\n";
    }
  }

  if (!systematicSets_m.empty()) {
    ss << "  Systematic source sets:\n";
    for (const auto &kv : systematicSets_m) {
      ss << "    \"" << kv.first << "\": " << kv.second.size() << " sources\n";
    }
  }

  if (!variations_m.empty()) {
    ss << "  Systematic variations (" << variations_m.size() << "):\n";
    for (const auto &var : variations_m) {
      ss << "    " << var.name
         << ": up=" << var.upSFColumn
         << "  down=" << var.downSFColumn << "\n";
    }
  }

  if (!wpCollectionSteps_m.empty()) {
    ss << "  WP-filtered collections (" << wpCollectionSteps_m.size() << "):\n";
    for (const auto &step : wpCollectionSteps_m) {
      ss << "    -> " << step.outputCollectionColumn << "\n";
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

  if (!fractionHistogramConfigs_m.empty()) {
    ss << "  Fraction histogram configs ("
       << fractionHistogramConfigs_m.size() << "):\n";
    for (const auto &cfg : fractionHistogramConfigs_m) {
      ss << "    prefix=\"" << cfg.outputPrefix << "\""
         << " pt_bins=" << (cfg.ptBinEdges.size() - 1)
         << " eta_bins=" << (cfg.etaBinEdges.size() - 1);
      if (!cfg.flavourColumn.empty())
        ss << " flavour=" << cfg.flavourColumn;
      ss << "\n";
    }
  }

  logger_m->log(ILogger::Level::Info, ss.str());
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
TaggerWorkingPointManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  if (!ptColumn_m.empty()) {
    entries["pt_column"]   = ptColumn_m;
    entries["eta_column"]  = etaColumn_m;
    entries["phi_column"]  = phiColumn_m;
    entries["mass_column"] = massColumn_m;
  }

  if (!taggerColumns_m.empty()) {
    if (taggerColumns_m.size() == 1) {
      entries["tagger_column"] = taggerColumns_m[0];
    } else {
      std::ostringstream ss;
      for (std::size_t k = 0; k < taggerColumns_m.size(); ++k) {
        if (k > 0) ss << ',';
        ss << taggerColumns_m[k];
      }
      entries["tagger_columns"] = ss.str();
    }
  }

  if (!workingPoints_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < workingPoints_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << workingPoints_m[i].name << ":{";
      for (std::size_t k = 0; k < workingPoints_m[i].thresholds.size(); ++k) {
        if (k > 0) ss << ";";
        ss << workingPoints_m[i].thresholds[k];
      }
      ss << "}";
    }
    entries["working_points"] = ss.str();
  }

  if (!inputObjectCollectionColumn_m.empty())
    entries["input_object_collection_column"] = inputObjectCollectionColumn_m;

  if (hasFractionCorrection_m)
    entries["fraction_correction"] = fractionCorrectionName_m;

  if (!variations_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < variations_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << variations_m[i].name
         << "(up:" << variations_m[i].upSFColumn
         << ",dn:" << variations_m[i].downSFColumn << ')';
    }
    entries["variations"] = ss.str();
  }

  if (!wpCollectionSteps_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < wpCollectionSteps_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << wpCollectionSteps_m[i].outputCollectionColumn;
    }
    entries["wp_collection_steps"] = ss.str();
  }

  return entries;
}
