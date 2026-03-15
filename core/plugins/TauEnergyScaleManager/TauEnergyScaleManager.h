#ifndef TAUENERGYMANAGER_H_INCLUDED
#define TAUENERGYMANAGER_H_INCLUDED

#include <CorrectionManager.h>
#include <PhysicsObjectCollection.h>
#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>
#include <map>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Record of a single tau energy systematic variation.
 */
struct TESVariationEntry {
  std::string name;           ///< Systematic name (e.g. "escale", "esmear")
  std::string upPtColumn;     ///< Corrected-pT column for the up shift
  std::string downPtColumn;   ///< Corrected-pT column for the down shift
};

/**
 * @class TauEnergyScaleManager
 * @brief Plugin for applying CMS Tau Energy Scale (TES) and Resolution
 *        (EER) corrections to tau collections, with full support for
 *        systematic variations and PhysicsObjectCollection integration.
 *
 * @see docs/ELECTRON_ENERGY_CORRECTIONS.md for the full user-facing reference.
 *
 * ## Features
 *  - **Correctionlib-based scale and smearing**: apply TES/EER corrections
 *    to a per-tau pT column via CorrectionManager.
 *  - **Systematic variations**: register named up/down variations with
 *    ISystematicManager.
 *  - **PhysicsObjectCollection integration**: consume a
 *    PhysicsObjectCollection of taus and produce corrected nominal and
 *    per-variation collections plus a PhysicsObjectVariationMap.
 *
 * ## Typical CMS NanoAOD usage
 * @code
 *   auto* ees = analyzer->getPlugin<TauEnergyScaleManager>("ees");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare tau column names.
 *   ees->setObjectColumns("Tau_pt", "Tau_eta",
 *                         "Tau_phi", "Tau_mass");
 *
 *   // 2. Apply nominal TES via correctionlib.
 *   ees->applyCorrectionlib(*cm, "tauScale", {"nom"},
 *                           "Tau_pt", "Tau_pt_escale");
 *
 *   // 3. Register and apply systematic set.
 *   ees->registerSystematicSources("escale", {"scaleStatUp",
 *                                             "scaleStatDown",
 *                                             "scaleSystUp",
 *                                             "scaleSystDown"});
 *   ees->applySystematicSet(*cm, "tauScale", "escale",
 *                           "Tau_pt", "Tau_pt_ees");
 *
 *   // 4. PhysicsObjectCollection integration.
 *   ees->setInputCollection("goodTaus");
 *   ees->defineCollectionOutput("Tau_pt_escale", "goodTaus_corr");
 *   ees->defineVariationCollections("goodTaus_corr", "goodTaus",
 *                                   "goodTaus_variations");
 * @endcode
 */
class TauEnergyScaleManager : public IPluggableManager {
public:
  TauEnergyScaleManager() = default;

  // -------------------------------------------------------------------------
  // Object column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the input tau kinematic column names.
   *
   * @param ptColumn   Name of the per-tau pT column (RVec<Float_t>).
   * @param etaColumn  Name of the per-tau η column.
   * @param phiColumn  Name of the per-tau φ column.
   * @param massColumn Name of the per-tau mass column; empty = skip mass.
   *
   * @throws std::invalid_argument if ptColumn is empty.
   */
  void setObjectColumns(const std::string &ptColumn,
                        const std::string &etaColumn,
                        const std::string &phiColumn,
                        const std::string &massColumn);

  // -------------------------------------------------------------------------
  // Applying corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule a correction using an existing per-tau SF column.
   *
   * In execute():
   * @code
   *   outputPtColumn = inputPtColumn × sfColumn
   * @endcode
   *
   * @throws std::invalid_argument if any required column name is empty.
   */
  void applyCorrection(const std::string &inputPtColumn,
                       const std::string &sfColumn,
                       const std::string &outputPtColumn);

  /**
   * @brief Apply a correctionlib correction and schedule the pT multiplication.
   *
   * Evaluates @p correctionName with @p stringArgs via @p cm, then calls
   * applyCorrection().
   *
   * @param cm             CorrectionManager to use.
   * @param correctionName Name of the correctionlib correction.
   * @param stringArgs     String arguments passed after numeric inputs.
   * @param inputPtColumn  Input pT column.
   * @param outputPtColumn Output pT column.
   * @param inputColumns   Numeric input columns for the correction evaluator.
   */
  void applyCorrectionlib(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::vector<std::string> &stringArgs,
                          const std::string &inputPtColumn,
                          const std::string &outputPtColumn,
                          const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Systematic source sets
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named list of TES/EER systematic source names.
   *
   * @throws std::invalid_argument if setName or any source is empty.
   */
  void registerSystematicSources(const std::string &setName,
                                  const std::vector<std::string> &sources);

  /**
   * @brief Return the source list for a registered set.
   *
   * @throws std::out_of_range if setName is not registered.
   */
  const std::vector<std::string> &
  getSystematicSources(const std::string &setName) const;

  /**
   * @brief Apply all sources in a registered set via correctionlib.
   *
   * For each source S in setName:
   *  - Evaluates correctionlib with args {S, "up"} → <outputPtPrefix>_S_up
   *  - Evaluates correctionlib with args {S, "down"} → <outputPtPrefix>_S_down
   *  - Registers variation S with up/down columns.
   *
   * @throws std::runtime_error    if setName is not registered.
   * @throws std::invalid_argument if any required argument is empty.
   */
  void applySystematicSet(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::string &setName,
                          const std::string &inputPtColumn,
                          const std::string &outputPtPrefix,
                          const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Direct variation registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named systematic variation directly (no correctionlib).
   *
   * @throws std::invalid_argument if any argument is empty.
   */
  void addVariation(const std::string &systematicName,
                    const std::string &upPtColumn,
                    const std::string &downPtColumn);

  // -------------------------------------------------------------------------
  // PhysicsObjectCollection integration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the RDF column holding the input tau
   *        PhysicsObjectCollection.
   *
   * @throws std::invalid_argument if collectionColumn is empty.
   */
  void setInputCollection(const std::string &collectionColumn);

  /**
   * @brief Schedule definition of a corrected PhysicsObjectCollection column.
   *
   * In execute(), defines @p outputCollectionColumn with pT values taken from
   * @p correctedPtColumn, eta/phi/mass preserved from the input collection.
   *
   * @throws std::invalid_argument if either column name is empty.
   * @throws std::runtime_error    if setInputCollection() was not called.
   */
  void defineCollectionOutput(const std::string &correctedPtColumn,
                              const std::string &outputCollectionColumn);

  /**
   * @brief Schedule per-variation collection columns and an optional
   *        PhysicsObjectVariationMap column for all registered variations.
   *
   * Column naming: @c <collectionPrefix>_<variationName>Up / Down.
   *
   * @throws std::invalid_argument if nominalCollectionColumn or
   *         collectionPrefix is empty.
   * @throws std::runtime_error    if setInputCollection() was not called.
   */
  void defineVariationCollections(const std::string &nominalCollectionColumn,
                                  const std::string &collectionPrefix,
                                  const std::string &variationMapColumn = "");

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Return the pT column name (from setObjectColumns).
  const std::string &getPtColumn() const;

  /// Return the input collection column name (from setInputCollection).
  const std::string &getInputCollectionColumn() const;

  /// Return all registered systematic variations.
  const std::vector<TESVariationEntry> &getVariations() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "TauEnergyScaleManager"; }

  void setContext(ManagerContext &ctx) override;

  void setupFromConfigFile() override {}

  /**
   * @brief Define all correction/collection output columns on the dataframe.
   *
   * Execution order:
   *  1. Each scheduled correction step.
   *  2. Each collection output step (defineCollectionOutput).
   *  3. Per-variation collection columns (defineVariationCollections).
   *  4. Register all systematic variations with ISystematicManager.
   */
  void execute() override;

  void finalize() override {}

  void reportMetadata() override;

  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Object column names ------------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- Correction steps ---------------------------------------------------
  struct CorrectionStep {
    std::string inputPtColumn;
    std::string sfColumn;
    std::string outputPtColumn;
  };
  std::vector<CorrectionStep> correctionSteps_m;

  // ---- Systematic variations ----------------------------------------------
  std::vector<TESVariationEntry> variations_m;

  // ---- Named systematic source sets ---------------------------------------
  std::map<std::string, std::vector<std::string>> systematicSets_m;

  // ---- PhysicsObjectCollection integration --------------------------------
  std::string inputCollectionColumn_m;

  struct CollectionOutputStep {
    std::string correctedPtColumn;
    std::string outputCollectionColumn;
  };
  std::vector<CollectionOutputStep> collectionOutputSteps_m;

  struct VariationCollectionsStep {
    std::string nominalCollectionColumn;
    std::string collectionPrefix;
    std::string variationMapColumn;
  };
  std::vector<VariationCollectionsStep> variationCollectionsSteps_m;

  // ---- Context ------------------------------------------------------------
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;
};

#endif // TAUENERGYMANAGER_H_INCLUDED
