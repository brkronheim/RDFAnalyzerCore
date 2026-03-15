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
 * @brief Record of a single tau energy scale/resolution systematic
 *        variation.
 */
struct TESVariationEntry {
  std::string name;            ///< Systematic name (e.g. "escale", "esmear")
  std::string upPtColumn;      ///< Corrected-pT column for the up shift
  std::string downPtColumn;    ///< Corrected-pT column for the down shift
  std::string upMassColumn;    ///< Corrected-mass column for the up shift (may be empty)
  std::string downMassColumn;  ///< Corrected-mass column for the down shift (may be empty)
};

/**
 * @class TauEnergyScaleManager
 * @brief Plugin for applying CMS Tau Energy Scale (TES) and Energy
 *        Resolution (EER) corrections to tau collections, with full
 *        support for systematic variations, Type-1 MET propagation, and
 *        PhysicsObjectCollection integration.
 *
 * @see docs/ELECTRON_ENERGY_CORRECTIONS.md for the full user-facing reference.
 *
 * ## Features
 *  - **Scale corrections**: correctionlib-based multiplicative pT scale factors.
 *  - **Resolution smearing**: additive Gaussian smear via
 *    applyResolutionSmearing(), supporting both correctionlib-derived σ
 *    columns and pre-computed per-tau σ columns.
 *  - **Mass propagation**: optionally scale/smear the mass column together
 *    with pT via applyCorrection().
 *  - **Systematic source sets**: register named lists of TES/EER source
 *    names and apply them in bulk with applySystematicSet().
 *  - **Type-1 MET propagation**: propagateMET() updates MET pT and φ when
 *    tau pT changes, using the same vector-sum formula as
 *    JetEnergyScaleManager.
 *  - **PhysicsObjectCollection integration**: produce corrected nominal and
 *    per-variation collections plus a PhysicsObjectVariationMap.
 *
 * ## Typical CMS NanoAOD usage
 * @code
 *   auto* ees = analyzer->getPlugin<TauEnergyScaleManager>("ees");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare tau and MET column names.
 *   ees->setObjectColumns("Tau_pt", "Tau_eta",
 *                         "Tau_phi", "Tau_mass");
 *   ees->setMETColumns("MET_pt", "MET_phi");
 *
 *   // 2. Apply nominal TES via correctionlib (multiplicative SF).
 *   ees->applyCorrectionlib(*cm, "tauSS", {"nom", "scale"},
 *                           "Tau_pt", "Tau_pt_escale");
 *
 *   // 3. Apply nominal EER resolution smearing.
 *   //    sigma column can come from correctionlib or be pre-defined.
 *   ees->applyResolutionSmearing("Tau_pt_escale",
 *                                "Tau_dEsigma",
 *                                "Tau_u1",
 *                                "Tau_pt_corr");
 *
 *   // 4. Propagate nominal correction to MET.
 *   ees->propagateMET("MET_pt", "MET_phi",
 *                     "Tau_pt", "Tau_pt_corr",
 *                     "MET_pt_ees", "MET_phi_ees");
 *
 *   // 5. Register and apply systematic set.
 *   ees->registerSystematicSources("escale",
 *       {"scaleStatUp", "scaleStatDown", "scaleSystUp", "scaleSystDown"});
 *   ees->applySystematicSet(*cm, "tauSS", "escale",
 *                           "Tau_pt", "Tau_pt_ees");
 *
 *   // 6. PhysicsObjectCollection integration.
 *   ees->setInputCollection("goodTaus");
 *   ees->defineCollectionOutput("Tau_pt_corr", "goodTaus_corr");
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
   * The phi column is also used as the source of tau directions for
   * MET propagation (tau φ does not change with TES/EER).
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
  // MET column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the base Missing Transverse Energy (MET) columns.
   *
   * Stored for provenance and metadata; the actual input MET column names
   * for each propagation step are passed directly to propagateMET().
   *
   * @param metPtColumn  Name of the scalar MET-pT column (Float_t).
   * @param metPhiColumn Name of the scalar MET-φ column (Float_t).
   *
   * @throws std::invalid_argument if either argument is empty.
   */
  void setMETColumns(const std::string &metPtColumn,
                     const std::string &metPhiColumn);

  // -------------------------------------------------------------------------
  // Applying corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule a multiplicative scale correction.
   *
   * In execute():
   * @code
   *   outputPtColumn   = inputPtColumn   × sfColumn
   *   outputMassColumn = inputMassColumn × sfColumn  [if applyToMass]
   * @endcode
   *
   * Mass column names are auto-derived by replacing the @c ptColumn_m prefix
   * with @c massColumn_m when @p inputMassColumn / @p outputMassColumn are
   * empty and @c massColumn_m is non-empty.
   *
   * @throws std::invalid_argument if any mandatory column name is empty.
   */
  void applyCorrection(const std::string &inputPtColumn,
                       const std::string &sfColumn,
                       const std::string &outputPtColumn,
                       bool applyToMass = false,
                       const std::string &inputMassColumn = "",
                       const std::string &outputMassColumn = "");

  /**
   * @brief Schedule an additive Gaussian energy resolution smear.
   *
   * In execute():
   * @code
   *   outputPtColumn = inputPtColumn + sigmaColumn × randomColumn
   * @endcode
   *
   * Typical CMS usage:
   *  - @p sigmaColumn   = per-tau resolution σ (from correctionlib or
   *    pre-computed, in GeV).
   *  - @p randomColumn  = per-tau Gaussian random number column
   *    (0 for data; Gaussian-distributed for MC smearing).
   *  - For systematic up/down use ±1σ random columns and call addVariation().
   *
   * @throws std::invalid_argument if any column name is empty.
   */
  void applyResolutionSmearing(const std::string &inputPtColumn,
                                const std::string &sigmaColumn,
                                const std::string &randomColumn,
                                const std::string &outputPtColumn);

  /**
   * @brief Apply a correctionlib correction and schedule the pT multiplication.
   *
   * Evaluates @p correctionName with @p stringArgs via @p cm to produce a
   * per-tau SF column, then calls applyCorrection().
   *
   * The SF column name is @c correctionName + "_" + joined(@p stringArgs, "_").
   *
   * @param cm             CorrectionManager to use.
   * @param correctionName Name of the correctionlib correction.
   * @param stringArgs     String arguments for the evaluator.
   * @param inputPtColumn  Input pT column.
   * @param outputPtColumn Output pT column.
   * @param applyToMass    Also define a corrected-mass column (default: false).
   * @param inputMassColumn  Input mass column; auto-derived if empty.
   * @param outputMassColumn Output mass column; auto-derived if empty.
   * @param inputColumns   Numeric input columns for the correction evaluator.
   */
  void applyCorrectionlib(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::vector<std::string> &stringArgs,
                          const std::string &inputPtColumn,
                          const std::string &outputPtColumn,
                          bool applyToMass = false,
                          const std::string &inputMassColumn = "",
                          const std::string &outputMassColumn = "",
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
   *  1. Evaluates correctionlib with args @c {S, "up"}   → SF column,
   *     schedules @c outputPtPrefix_S_up.
   *  2. Evaluates correctionlib with args @c {S, "down"} → SF column,
   *     schedules @c outputPtPrefix_S_down.
   *  3. Calls addVariation(S, up, down).
   *
   * @param applyToMass    Also define corrected-mass columns (default: false).
   * @param inputMassColumn  Explicit input mass column; auto-derived if empty.
   *
   * @throws std::runtime_error    if setName is not registered.
   * @throws std::invalid_argument if any mandatory argument is empty.
   */
  void applySystematicSet(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::string &setName,
                          const std::string &inputPtColumn,
                          const std::string &outputPtPrefix,
                          bool applyToMass = false,
                          const std::vector<std::string> &inputColumns = {},
                          const std::string &inputMassColumn = "");

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
                    const std::string &downPtColumn,
                    const std::string &upMassColumn = "",
                    const std::string &downMassColumn = "");

  // -------------------------------------------------------------------------
  // Type-1 MET propagation
  // -------------------------------------------------------------------------

  /**
   * @brief Propagate an tau pT change into MET (Type-1 style).
   *
   * Computes:
   * @code
   *   MET_x_new = MET_x_base − Σ_i (variedPt_i − nominalPt_i) · cos(φ_i)
   *   MET_y_new = MET_y_base − Σ_i (variedPt_i − nominalPt_i) · sin(φ_i)
   * @endcode
   * where the sum runs over all taus (no pT threshold by default since
   * taus are already selected objects — pass @p ptThreshold > 0 to
   * restrict to a sub-range).
   *
   * @param baseMETPtColumn     Input scalar MET-pT column (Float_t).
   * @param baseMETPhiColumn    Input scalar MET-φ column (Float_t).
   * @param nominalPtColumn     Nominal per-tau pT column (RVec<Float_t>).
   * @param variedPtColumn      Varied per-tau pT column (RVec<Float_t>).
   * @param outputMETPtColumn   Output scalar MET-pT column name.
   * @param outputMETPhiColumn  Output scalar MET-φ column name.
   * @param ptThreshold         Only propagate taus with nominal pT above
   *                            this threshold (default: 0, i.e. all taus).
   *
   * @throws std::invalid_argument if any required column name is empty.
   * @throws std::runtime_error    if setObjectColumns() was not called first
   *                               (phi column needed).
   */
  void propagateMET(const std::string &baseMETPtColumn,
                    const std::string &baseMETPhiColumn,
                    const std::string &nominalPtColumn,
                    const std::string &variedPtColumn,
                    const std::string &outputMETPtColumn,
                    const std::string &outputMETPhiColumn,
                    float ptThreshold = 0.0f);

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
   * When @p correctedMassColumn is provided, the output collection is built
   * with both corrected pT and corrected mass via withCorrectedKinematics();
   * otherwise only pT is updated via withCorrectedPt().
   *
   * @throws std::invalid_argument if correctedPtColumn or
   *         outputCollectionColumn is empty.
   * @throws std::runtime_error    if setInputCollection() was not called.
   */
  void defineCollectionOutput(const std::string &correctedPtColumn,
                              const std::string &outputCollectionColumn,
                              const std::string &correctedMassColumn = "");

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

  /// Return the mass column name (from setObjectColumns).
  const std::string &getMassColumn() const;

  /// Return the MET pT column name (from setMETColumns).
  const std::string &getMETPtColumn() const;

  /// Return the MET phi column name (from setMETColumns).
  const std::string &getMETPhiColumn() const;

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
   * @brief Define all correction/collection/MET output columns on the
   *        dataframe and register systematics.
   *
   * Execution order:
   *  1. Scale correction steps (applyCorrection / applyCorrectionlib).
   *  2. Resolution smearing steps (applyResolutionSmearing).
   *  3. MET propagation steps (propagateMET).
   *  4. Collection output steps (defineCollectionOutput).
   *  5. Per-variation collection columns (defineVariationCollections).
   *  6. Register all systematic variations with ISystematicManager.
   */
  void execute() override;

  void finalize() override {}

  void reportMetadata() override;

  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Helpers ------------------------------------------------------------
  /// Derive the mass column name corresponding to a pT column name, by
  /// substituting the ptColumn_m prefix with massColumn_m.
  std::string deriveMassColumnName(const std::string &ptColName) const;

  // ---- Object column names ------------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- MET column names ---------------------------------------------------
  std::string metPtColumn_m;
  std::string metPhiColumn_m;

  // ---- Scale correction steps ---------------------------------------------
  struct CorrectionStep {
    std::string inputPtColumn;
    std::string sfColumn;
    std::string outputPtColumn;
    std::string inputMassColumn;   ///< empty = skip mass
    std::string outputMassColumn;  ///< empty = skip mass
  };
  std::vector<CorrectionStep> correctionSteps_m;

  // ---- Resolution smearing steps ------------------------------------------
  struct SmearingStep {
    std::string inputPtColumn;
    std::string sigmaColumn;
    std::string randomColumn;
    std::string outputPtColumn;
  };
  std::vector<SmearingStep> smearingSteps_m;

  // ---- MET propagation steps ----------------------------------------------
  struct METPropagationStep {
    std::string baseMETPtColumn;
    std::string baseMETPhiColumn;
    std::string nominalPtColumn;
    std::string variedPtColumn;
    std::string outputMETPtColumn;
    std::string outputMETPhiColumn;
    float ptThreshold = 0.0f;
  };
  std::vector<METPropagationStep> metPropagationSteps_m;

  // ---- Systematic variations ----------------------------------------------
  std::vector<TESVariationEntry> variations_m;

  // ---- Named systematic source sets ---------------------------------------
  std::map<std::string, std::vector<std::string>> systematicSets_m;

  // ---- PhysicsObjectCollection integration --------------------------------
  std::string inputCollectionColumn_m;

  struct CollectionOutputStep {
    std::string correctedPtColumn;
    std::string correctedMassColumn;
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
