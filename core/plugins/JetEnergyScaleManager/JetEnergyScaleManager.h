#ifndef JETENERYSCALEMANAGER_H_INCLUDED
#define JETENERYSCALEMANAGER_H_INCLUDED

#include <CorrectionManager.h>
#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Record of a single JES/JER systematic variation.
 *
 * Populated by addVariation() and used in execute() (systematic registration)
 * and reportMetadata() (logging).
 */
struct JESVariationEntry {
  std::string name;           ///< Systematic name (e.g. "jesTotal")
  std::string upPtColumn;     ///< Corrected-pT column for the up shift
  std::string downPtColumn;   ///< Corrected-pT column for the down shift
  std::string upMassColumn;   ///< Corrected-mass column for the up shift (may be empty)
  std::string downMassColumn; ///< Corrected-mass column for the down shift (may be empty)
};

/**
 * @class JetEnergyScaleManager
 * @brief Plugin for applying Jet Energy Scale (JES) and Jet Energy Resolution
 *        (JER) corrections to jet collections.
 *
 * Supports:
 *  - **Removing existing JEC**: strip NanoAOD-level corrections using a raw
 *    factor column (CMS convention: @c Jet_pt_raw = Jet_pt × (1 − Jet_rawFactor)).
 *  - **Applying new corrections**: using per-jet scale factor columns that are
 *    already in the dataframe, or via CMS-style Correctionlib evaluations
 *    performed through a CorrectionManager instance.
 *  - **Systematic variation tracking**: register named up/down variations with
 *    the framework's ISystematicManager and emit a human-readable summary via
 *    reportMetadata().
 *
 * ## Typical CMS NanoAOD usage
 * @code
 *   auto* jes = analyzer->getPlugin<JetEnergyScaleManager>("jes");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare input jet columns.
 *   jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
 *
 *   // 2. Strip existing NanoAOD JEC to obtain raw pT and mass.
 *   //    Jet_pt_raw  = Jet_pt  * (1 - Jet_rawFactor)
 *   //    Jet_mass_raw = Jet_mass * (1 - Jet_rawFactor)
 *   jes->removeExistingCorrections("Jet_rawFactor");
 *
 *   // 3a. Apply new L1L2L3 JEC via CorrectionManager (CMS correctionlib).
 *   jes->applyCorrectionlib(*cm, "jec_l1l2l3", {"L3Residual"},
 *                           "Jet_pt_raw", "Jet_pt_jec");
 *   // Creates columns Jet_pt_jec and Jet_mass_jec.
 *
 *   // 3b. Apply total JES uncertainty variations.
 *   jes->applyCorrectionlib(*cm, "jes_unc_total", {"up"},
 *                           "Jet_pt_jec", "Jet_pt_jes_total_up");
 *   jes->applyCorrectionlib(*cm, "jes_unc_total", {"down"},
 *                           "Jet_pt_jec", "Jet_pt_jes_total_dn");
 *
 *   // 4. Register the systematic for framework tracking.
 *   jes->addVariation("jesTotal",
 *                     "Jet_pt_jes_total_up", "Jet_pt_jes_total_dn");
 *
 *   // 5. Retrieve output column names for downstream use.
 *   std::string nominalPt = "Jet_pt_jec";
 *   std::string rawPt     = jes->getRawPtColumn(); // "Jet_pt_raw"
 * @endcode
 *
 * ## General (non-correctionlib) usage
 * @code
 *   // Scale factor column already present in the dataframe.
 *   jes->applyCorrection("Jet_pt_raw", "my_jes_sf", "Jet_pt_jec");
 * @endcode
 */
class JetEnergyScaleManager : public IPluggableManager {
public:
  JetEnergyScaleManager() = default;

  // -------------------------------------------------------------------------
  // Jet column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the input jet kinematic column names.
   *
   * Must be called before execute().  All four columns are required; an empty
   * string for @p massColumn disables automatic mass correction propagation.
   *
   * @param ptColumn   Name of the per-jet pT column (RVec<Float_t>).
   * @param etaColumn  Name of the per-jet η column (RVec<Float_t>).
   * @param phiColumn  Name of the per-jet φ column (RVec<Float_t>).
   * @param massColumn Name of the per-jet mass column (RVec<Float_t>);
   *                   pass an empty string to skip mass corrections.
   *
   * @throws std::invalid_argument if @p ptColumn is empty.
   */
  void setJetColumns(const std::string &ptColumn,
                     const std::string &etaColumn,
                     const std::string &phiColumn,
                     const std::string &massColumn);

  // -------------------------------------------------------------------------
  // Removing existing corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Strip existing JEC by computing a raw-pT (and raw-mass) column.
   *
   * Schedules the following definitions for execute():
   * @code
   *   <ptColumn>_raw   = <ptColumn>   × (1 − rawFactorColumn)   [element-wise]
   *   <massColumn>_raw = <massColumn> × (1 − rawFactorColumn)   [element-wise]
   * @endcode
   *
   * CMS NanoAOD convention: @p rawFactorColumn = @c "Jet_rawFactor"
   * (= 1 − pt_raw / pt_jec).
   *
   * @param rawFactorColumn Name of the per-jet @c RVec<Float_t> column that
   *        holds the raw factor (= 1 − pt_raw / pt_corrected).
   *
   * @throws std::invalid_argument if @p rawFactorColumn is empty.
   * @throws std::runtime_error    if setRawPtColumn() was already called.
   */
  void removeExistingCorrections(const std::string &rawFactorColumn);

  /**
   * @brief Declare an existing raw-pT column (already present in the dataframe).
   *
   * Use this when the raw pT is already available as a branch rather than
   * being derived from a raw factor.  No new column is defined in execute().
   *
   * Mutually exclusive with removeExistingCorrections().
   *
   * @param rawPtColumn Name of the existing per-jet raw-pT column.
   *
   * @throws std::invalid_argument if @p rawPtColumn is empty.
   * @throws std::runtime_error    if removeExistingCorrections() was already called.
   */
  void setRawPtColumn(const std::string &rawPtColumn);

  // -------------------------------------------------------------------------
  // Applying corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule a correction using an existing per-jet scale factor column.
   *
   * In execute(), defines:
   * @code
   *   outputPtColumn   = inputPtColumn   × sfColumn   [element-wise]
   *   outputMassColumn = inputMassColumn × sfColumn   [element-wise, if applyToMass]
   * @endcode
   *
   * When @p inputMassColumn or @p outputMassColumn are empty and
   * @p applyToMass is @c true, they are derived by replacing the pt column
   * prefix (@c ptColumn_m) with the mass column prefix (@c massColumn_m) in
   * the respective pT column name.  If the substitution cannot be made (e.g.
   * the pT column name does not start with @c ptColumn_m), the mass
   * correction for that step is skipped.
   *
   * @param inputPtColumn    Per-jet pT column to correct.
   * @param sfColumn         Per-jet @c RVec<Float_t> scale factor column.
   * @param outputPtColumn   Name for the corrected-pT output column.
   * @param applyToMass      Also create a corrected-mass column (default: true).
   * @param inputMassColumn  Input mass column; derived from @p inputPtColumn
   *                         when empty and @p applyToMass is @c true.
   * @param outputMassColumn Output mass column name; derived from
   *                         @p outputPtColumn when empty and @p applyToMass
   *                         is @c true.
   *
   * @throws std::invalid_argument if any of the mandatory string arguments is empty.
   */
  void applyCorrection(const std::string &inputPtColumn,
                       const std::string &sfColumn,
                       const std::string &outputPtColumn,
                       bool applyToMass = true,
                       const std::string &inputMassColumn = "",
                       const std::string &outputMassColumn = "");

  /**
   * @brief Apply a CMS correctionlib-based JES/JER correction via CorrectionManager.
   *
   * Calls @p cm.applyCorrectionVec() to evaluate the correctionlib formula
   * and store per-jet scale factors in an intermediate RVec column, then
   * schedules the pT and (optionally) mass correction for execute() exactly
   * as applyCorrection() does.
   *
   * The intermediate SF column is named @p correctionName followed by each
   * element of @p stringArgs joined with underscores (the same convention
   * used by CorrectionManager::applyCorrectionVec()).
   *
   * @param cm               CorrectionManager that holds the registered correction.
   * @param correctionName   Name of the correction in @p cm.
   * @param stringArgs       Constant string arguments for the correctionlib
   *                         evaluation (e.g. @c {"nominal"}, @c {"up"}).
   * @param inputPtColumn    Per-jet pT column to correct.
   * @param outputPtColumn   Name for the corrected-pT output column.
   * @param applyToMass      Also create a corrected-mass column (default: true).
   * @param inputMassColumn  Input mass column (derived if empty).
   * @param outputMassColumn Output mass column name (derived if empty).
   * @param inputColumns     Override for the correctionlib numeric input
   *                         columns (uses the columns registered in @p cm
   *                         when empty).
   */
  void applyCorrectionlib(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::vector<std::string> &stringArgs,
                          const std::string &inputPtColumn,
                          const std::string &outputPtColumn,
                          bool applyToMass = true,
                          const std::string &inputMassColumn = "",
                          const std::string &outputMassColumn = "",
                          const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Systematic variation registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a JES/JER systematic variation for framework tracking.
   *
   * Associates @p systematicName with the supplied up and down pT columns
   * (and, optionally, mass columns).  In execute(), registers each variation
   * direction with the ISystematicManager so that downstream histogramming and
   * validation tools can propagate the systematic.
   *
   * The up/down columns should already have been created via applyCorrection()
   * or applyCorrectionlib() calls before execute() runs.
   *
   * @param systematicName  Label for the systematic (e.g. @c "jesTotal").
   * @param upPtColumn      Corrected-pT column for the up variation.
   * @param downPtColumn    Corrected-pT column for the down variation.
   * @param upMassColumn    Corrected-mass column for the up variation (optional).
   * @param downMassColumn  Corrected-mass column for the down variation (optional).
   *
   * @throws std::invalid_argument if @p systematicName, @p upPtColumn, or
   *         @p downPtColumn is empty.
   */
  void addVariation(const std::string &systematicName,
                    const std::string &upPtColumn,
                    const std::string &downPtColumn,
                    const std::string &upMassColumn = "",
                    const std::string &downMassColumn = "");

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /**
   * @brief Return the raw-pT column name.
   *
   * Non-empty after removeExistingCorrections() or setRawPtColumn() has been
   * called; empty otherwise.
   */
  const std::string &getRawPtColumn() const;

  /**
   * @brief Return the original pT column name (as set by setJetColumns()).
   */
  const std::string &getPtColumn() const;

  /**
   * @brief Return the original mass column name (as set by setJetColumns()).
   */
  const std::string &getMassColumn() const;

  /**
   * @brief Return all registered systematic variations.
   *
   * Entries are populated by addVariation() calls and are available
   * immediately (not deferred to execute()).
   */
  const std::vector<JESVariationEntry> &getVariations() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "JetEnergyScaleManager"; }

  void setContext(ManagerContext &ctx) override;

  /** No-op: corrections are registered programmatically. */
  void setupFromConfigFile() override {}

  /**
   * @brief Define all correction output columns on the dataframe.
   *
   * Called immediately before the RDataFrame computation is triggered.
   * Performs:
   *  1. Defines the raw-pT and raw-mass columns (if removeExistingCorrections()
   *     was called).
   *  2. Defines each scheduled correction output column (pT and mass).
   *  3. Registers all declared systematic variations with the
   *     ISystematicManager.
   */
  void execute() override;

  /** No-op: no deferred results to retrieve. */
  void finalize() override {}

  /**
   * @brief Log a human-readable JES/JER configuration summary.
   *
   * Reports the jet column configuration, raw-pT computation, correction
   * steps, and registered systematic variations via the analysis logger.
   */
  void reportMetadata() override;

  /**
   * @brief Contribute structured provenance metadata for this plugin.
   *
   * Returns entries for jet columns, raw-pT column, correction steps, and
   * registered systematic variations.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Jet column names ---------------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- Raw pT computation -------------------------------------------------
  std::string rawFactorColumn_m; ///< Column used to compute raw pT (non-empty if
                                 ///< removeExistingCorrections() was called)
  std::string rawPtColumn_m;    ///< Raw-pT column name (derived or user-supplied)
  std::string rawMassColumn_m;  ///< Raw-mass column name (derived from rawPtColumn_m)

  // ---- Correction steps ---------------------------------------------------
  struct CorrectionStep {
    std::string inputPtColumn;
    std::string sfColumn;
    std::string outputPtColumn;
    std::string inputMassColumn;  ///< empty → no mass correction for this step
    std::string outputMassColumn; ///< empty → no mass correction for this step
  };
  std::vector<CorrectionStep> correctionSteps_m;

  // ---- Systematic variations ----------------------------------------------
  std::vector<JESVariationEntry> variations_m;

  // ---- Context ------------------------------------------------------------
  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;

  // ---- Internal helpers ---------------------------------------------------

  /**
   * @brief Derive the mass column name corresponding to a pT column name.
   *
   * Replaces the @c ptColumn_m prefix in @p ptColName with @c massColumn_m.
   * Returns an empty string if the substitution cannot be made (pT column
   * does not start with @c ptColumn_m, or @c massColumn_m is empty).
   */
  std::string deriveMassColumnName(const std::string &ptColName) const;
};

#endif // JETENERYSCALEMANAGER_H_INCLUDED
