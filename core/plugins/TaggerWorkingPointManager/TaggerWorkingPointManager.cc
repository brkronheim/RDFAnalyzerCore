#include <TaggerWorkingPointManager.h>
#include <NullOutputSink.h>
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
// setTaggerColumn
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::setTaggerColumn(
    const std::string &taggerScoreColumn) {
  if (taggerScoreColumn.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::setTaggerColumn: column must not be empty");
  taggerColumn_m = taggerScoreColumn;
}

// ---------------------------------------------------------------------------
// addWorkingPoint
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::addWorkingPoint(const std::string &name,
                                                     float threshold) {
  if (name.empty())
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addWorkingPoint: name must not be empty");
  for (const auto &wp : workingPoints_m) {
    if (wp.name == name)
      throw std::invalid_argument(
          "TaggerWorkingPointManager::addWorkingPoint: duplicate WP name '" +
          name + "'");
  }
  if (!workingPoints_m.empty() && threshold <= workingPoints_m.back().threshold)
    throw std::invalid_argument(
        "TaggerWorkingPointManager::addWorkingPoint: threshold must be "
        "strictly greater than the previous WP threshold");
  workingPoints_m.push_back({name, threshold});
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
  if (taggerColumn_m.empty())
    throw std::runtime_error(
        "TaggerWorkingPointManager::defineFractionHistograms: call "
        "setTaggerColumn() first");
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
// execute()
// ---------------------------------------------------------------------------

void TaggerWorkingPointManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error(
        "TaggerWorkingPointManager::execute: context not set");

  // -------------------------------------------------------------------------
  // 1. Define per-object WP category column.
  // -------------------------------------------------------------------------
  if (!taggerColumn_m.empty() && !workingPoints_m.empty()) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string taggerCol = taggerColumn_m;
    const std::vector<WorkingPointEntry> wps = workingPoints_m;
    const std::string catCol = wpCategoryColumn();

    auto newDf = df.Define(
        catCol,
        [wps](const ROOT::VecOps::RVec<Float_t> &score)
            -> ROOT::VecOps::RVec<Int_t> {
          ROOT::VecOps::RVec<Int_t> cat(score.size(), 0);
          for (std::size_t i = 0; i < score.size(); ++i) {
            Int_t c = 0;
            for (const auto &wp : wps) {
              if (score[i] >= wp.threshold)
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

    } else { // PassRangeWP: pass lower WP, fail upper WP
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
          const std::string scoreSelCol = "_frac_score_" + histName;
          const std::string taggerCol   = taggerColumn_m;
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
    ss << "  Jet columns: pt=" << ptColumn_m
       << " eta=" << etaColumn_m
       << " phi=" << phiColumn_m
       << " mass=" << massColumn_m << "\n";
  }

  if (!taggerColumn_m.empty())
    ss << "  Tagger column: " << taggerColumn_m << "\n";

  if (!workingPoints_m.empty()) {
    ss << "  Working points (" << workingPoints_m.size() << "):\n";
    for (std::size_t i = 0; i < workingPoints_m.size(); ++i) {
      ss << "    [" << i + 1 << "] " << workingPoints_m[i].name
         << " threshold=" << workingPoints_m[i].threshold << "\n";
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
    entries["jet_pt_column"]   = ptColumn_m;
    entries["jet_eta_column"]  = etaColumn_m;
    entries["jet_phi_column"]  = phiColumn_m;
    entries["jet_mass_column"] = massColumn_m;
  }

  if (!taggerColumn_m.empty())
    entries["tagger_column"] = taggerColumn_m;

  if (!workingPoints_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < workingPoints_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << workingPoints_m[i].name << ':' << workingPoints_m[i].threshold;
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
