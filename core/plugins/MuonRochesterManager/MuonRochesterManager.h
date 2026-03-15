#ifndef MUONROCHESTERMANAGER_H_INCLUDED
#define MUONROCHESTERMANAGER_H_INCLUDED

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
 * @brief Record of a single muon Rochester systematic variation.
 */
struct RochesterVariationEntry {
  std::string name;         ///< Systematic name (e.g. "stat", "syst")
  std::string upPtColumn;   ///< Corrected-pT column for the up shift
  std::string downPtColumn; ///< Corrected-pT column for the down shift
};

/**
 * @class MuonRochesterManager
 * @brief Plugin for applying CMS muon Rochester momentum corrections and
 *        associated systematic variations to muon collections.
 *
 * @see docs/MUON_ROCHESTER_CORRECTIONS.md for the full user-facing reference.
 *
 * ## Features
 *  - **Rochester nominal correction**: apply the nominal Rochester scale
 *    factor column to a per-muon pT column via correctionlib or a
 *    pre-computed SF column.
 *  - **Systematic variations**: stat + syst uncertainties registered with
 *    ISystematicManager.
 *  - **PhysicsObjectCollection integration**: consume a
 *    PhysicsObjectCollection of muons and produce a corrected nominal
 *    collection, per-variation collections, and a PhysicsObjectVariationMap.
 *
 * ## CMS Rochester correctionlib calling convention
 *
 * The CMS correctionlib implementation of Rochester corrections expects the
 * following numeric inputs in this order:
 *  1. `charge` (Int_t per muon, must be passed as Float_t/RVec<Float_t>)
 *  2. `eta`
 *  3. `phi`
 *  4. `pt`
 *  5. `genPt` (for MC; set to 0 or -1 for data or when unavailable)
 *  6. `nl`    (number of tracker layers, Int_t cast to Float_t)
 *  7. `u1`    (random number for smearing, Float_t; 0 for data)
 *  8. `u2`    (second random number; 0 for data)
 *
 * String arguments: the variation direction ("nom", "up", or "down") and the
 * systematic type ("stat" or "syst") depending on the correction.
 *
 * The user is responsible for preparing the numeric input columns and passing
 * them via @c inputColumns in the applyCorrection / applyCorrectionlib calls.
 *
 * ## Typical CMS NanoAOD usage
 * @code
 *   auto* roch = analyzer->getPlugin<MuonRochesterManager>("rochester");
 *   auto* cm   = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare muon column names.
 *   roch->setObjectColumns("Muon_pt", "Muon_eta", "Muon_phi", "Muon_mass");
 *
 *   // 2. Apply nominal Rochester correction via correctionlib.
 *   //    inputColumns are: charge, eta, phi, pt, genPt, nl, u1, u2
 *   roch->applyCorrectionlib(
 *       *cm, "rochester", {"nom"},
 *       "Muon_pt", "Muon_pt_roch",
 *       {"Muon_charge_f", "Muon_eta", "Muon_phi", "Muon_pt",
 *        "Muon_genPt", "Muon_nTrackerLayers_f", "Muon_u1", "Muon_u2"});
 *
 *   // 3. Apply statistical and systematic uncertainties.
 *   roch->registerSystematicSources("rochester", {"stat", "syst"});
 *   roch->applySystematicSet(
 *       *cm, "rochester", "rochester",
 *       "Muon_pt", "Muon_pt_roch",
 *       {"Muon_charge_f", "Muon_eta", "Muon_phi", "Muon_pt",
 *        "Muon_genPt", "Muon_nTrackerLayers_f", "Muon_u1", "Muon_u2"});
 *
 *   // 4. PhysicsObjectCollection integration.
 *   roch->setInputCollection("goodMuons");
 *   roch->defineCollectionOutput("Muon_pt_roch", "goodMuons_corr");
 *   roch->defineVariationCollections("goodMuons_corr", "goodMuons",
 *                                    "goodMuons_variations");
 * @endcode
 */
class MuonRochesterManager : public IPluggableManager {
public:
  MuonRochesterManager() = default;

  // -------------------------------------------------------------------------
  // Object column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the input muon kinematic column names.
   *
   * @param ptColumn   Name of the per-muon pT column (RVec<Float_t>).
   * @param etaColumn  Name of the per-muon η column.
   * @param phiColumn  Name of the per-muon φ column.
   * @param massColumn Name of the per-muon mass column; empty = skip.
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
   * @brief Schedule a correction using an existing per-muon SF column.
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
   * For the CMS Rochester correctionlib format @p stringArgs contains the
   * variation direction, e.g. @c {"nom"} for nominal, @c {"stat", "up"} or
   * @c {"syst", "down"} for systematics.
   *
   * @p inputColumns should contain all numeric input columns required by the
   * correction (typically charge, eta, phi, pt, genPt, nl, u1, u2 for
   * CMS Rochester).
   *
   * @param cm             CorrectionManager to use.
   * @param correctionName Name of the correctionlib correction.
   * @param stringArgs     String arguments for the correctionlib evaluator.
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
   * @brief Register a named list of Rochester systematic source names.
   *
   * For CMS Rochester corrections the standard sources are:
   *  - @c "stat"  — statistical uncertainty (MC only)
   *  - @c "syst"  — systematic uncertainty
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
   *  - Registers variation S with ISystematicManager.
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
   * @brief Declare the RDF column holding the input muon
   *        PhysicsObjectCollection.
   *
   * @throws std::invalid_argument if collectionColumn is empty.
   */
  void setInputCollection(const std::string &collectionColumn);

  /**
   * @brief Schedule definition of a corrected PhysicsObjectCollection column.
   *
   * @throws std::invalid_argument if either column name is empty.
   * @throws std::runtime_error    if setInputCollection() was not called.
   */
  void defineCollectionOutput(const std::string &correctedPtColumn,
                              const std::string &outputCollectionColumn);

  /**
   * @brief Schedule per-variation collection columns and an optional
   *        PhysicsObjectVariationMap column.
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
  const std::vector<RochesterVariationEntry> &getVariations() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "MuonRochesterManager"; }

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
  std::vector<RochesterVariationEntry> variations_m;

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

#endif // MUONROCHESTERMANAGER_H_INCLUDED
