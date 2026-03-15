#ifndef OBJECTENERGYMANAGERBASE_H_INCLUDED
#define OBJECTENERGYMANAGERBASE_H_INCLUDED

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
 * @brief Record of a single energy scale/resolution systematic variation,
 *        shared by all ObjectEnergyManagerBase concrete subclasses.
 */
struct EnergyVariationEntry {
  std::string name;            ///< Systematic name (e.g. "escale", "esmear")
  std::string upPtColumn;      ///< Corrected-pT column for the up shift
  std::string downPtColumn;    ///< Corrected-pT column for the down shift
  std::string upMassColumn;    ///< Corrected-mass column for the up shift (may be empty)
  std::string downMassColumn;  ///< Corrected-mass column for the down shift (may be empty)
};

/**
 * @class ObjectEnergyManagerBase
 * @brief Abstract base class providing the complete implementation for CMS
 *        object energy scale, energy resolution smearing, Type-1 MET
 *        propagation, and PhysicsObjectCollection integration.
 *
 * Concrete subclasses need only override `type()` (the plugin type string)
 * and `objectName()` (the physics-object name, e.g. "Electron", "Photon",
 * "Tau", "Muon").  All data members and algorithms live here; the subclasses
 * are thin shells.
 *
 * ## Feature set
 *  - **Scale corrections**: correctionlib-based multiplicative pT/mass scale
 *    factors, scheduled in advance and applied atomically in execute().
 *  - **Resolution smearing**: additive Gaussian smear
 *    @c pt_out = pt_in + σ × u, where @c σ is a per-object resolution column
 *    and @c u is a Gaussian random-number column.
 *  - **Mass propagation**: optional simultaneous correction of the mass column
 *    using the same scale factor (via applyCorrection()).
 *  - **Systematic source sets**: bulk application of named uncertainty sources
 *    via applySystematicSet(), matching the JetEnergyScaleManager API.
 *  - **Type-1 MET propagation**: propagateMET() uses the same vector-sum
 *    formula as JetEnergyScaleManager.
 *  - **PhysicsObjectCollection integration**: nominal corrected collection,
 *    per-variation up/down collections, and a PhysicsObjectVariationMap.
 *
 * @see ElectronEnergyScaleManager, PhotonEnergyScaleManager,
 *      TauEnergyScaleManager, MuonRochesterManager for concrete subclasses.
 * @see docs/JET_ENERGY_CORRECTIONS.md for the companion jet documentation.
 */
class ObjectEnergyManagerBase : public IPluggableManager {
public:
  ObjectEnergyManagerBase() = default;
  ~ObjectEnergyManagerBase() override = default;

  // -------------------------------------------------------------------------
  // Object column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the input object kinematic column names.
   *
   * The phi column is also used as the direction source for MET propagation
   * (phi does not change with energy scale/resolution corrections).
   *
   * @param ptColumn   Per-object pT column (RVec<Float_t>).
   * @param etaColumn  Per-object η column.
   * @param phiColumn  Per-object φ column.
   * @param massColumn Per-object mass column; empty = skip mass corrections.
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
   * @brief Declare the base Missing Transverse Energy (MET) columns for
   *        provenance and metadata tracking.
   *
   * The column names passed here are stored for reporting only; each call to
   * propagateMET() accepts its own base-MET column arguments.
   *
   * @param metPtColumn  Scalar MET-pT column (Float_t).
   * @param metPhiColumn Scalar MET-φ column (Float_t).
   *
   * @throws std::invalid_argument if either argument is empty.
   */
  void setMETColumns(const std::string &metPtColumn,
                     const std::string &metPhiColumn);

  // -------------------------------------------------------------------------
  // Applying scale corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule a multiplicative scale correction.
   *
   * In execute():
   * @code
   *   outputPtColumn   = inputPtColumn   × sfColumn
   *   outputMassColumn = inputMassColumn × sfColumn   [if applyToMass]
   * @endcode
   *
   * Mass column names are auto-derived by replacing the @c ptColumn_m prefix
   * with @c massColumn_m when the explicit arguments are empty.
   *
   * @throws std::invalid_argument if any mandatory argument is empty.
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
   * @param inputPtColumn  Input pT column.
   * @param sigmaColumn    Per-object resolution σ (in GeV), from correctionlib
   *                       or pre-computed.
   * @param randomColumn   Per-object Gaussian random-number column
   *                       (0 for data; N(0,1)-distributed for MC smearing).
   * @param outputPtColumn Output smeared pT column name.
   *
   * @throws std::invalid_argument if any argument is empty.
   */
  void applyResolutionSmearing(const std::string &inputPtColumn,
                                const std::string &sigmaColumn,
                                const std::string &randomColumn,
                                const std::string &outputPtColumn);

  /**
   * @brief Apply a correctionlib correction and schedule the pT multiplication.
   *
   * Evaluates @p correctionName with @p stringArgs via @p cm to produce a
   * per-object SF column, then calls applyCorrection().
   *
   * The SF column name is: @c correctionName + "_" + join(@p stringArgs, "_").
   *
   * @param cm               CorrectionManager holding the correction.
   * @param correctionName   Correction name in @p cm.
   * @param stringArgs       String arguments for the correctionlib evaluator.
   * @param inputPtColumn    Input pT column.
   * @param outputPtColumn   Output pT column.
   * @param applyToMass      Also correct the mass column (default: false).
   * @param inputMassColumn  Explicit input mass column; auto-derived if empty.
   * @param outputMassColumn Explicit output mass column; auto-derived if empty.
   * @param inputColumns     Numeric input columns for the evaluator.
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
   * @brief Register a named list of systematic source names.
   *
   * @throws std::invalid_argument if setName or any source is empty, or if
   *         sources is empty.
   */
  void registerSystematicSources(const std::string &setName,
                                  const std::vector<std::string> &sources);

  /**
   * @brief Return the source list for a registered set.
   *
   * @throws std::out_of_range if setName has not been registered.
   */
  const std::vector<std::string> &
  getSystematicSources(const std::string &setName) const;

  /**
   * @brief Apply all sources in a registered set via correctionlib.
   *
   * For each source S in the set:
   *  1. Evaluates correctionlib with args @c {S,"up"}   → @c outputPtPrefix_S_up.
   *  2. Evaluates correctionlib with args @c {S,"down"} → @c outputPtPrefix_S_down.
   *  3. Calls addVariation(S, up, down).
   *
   * @param applyToMass     Also define corrected-mass columns (default: false).
   * @param inputMassColumn Explicit input mass column; auto-derived if empty.
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
   * @throws std::invalid_argument if systematicName, upPtColumn, or
   *         downPtColumn is empty.
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
   * @brief Propagate an object pT change into MET (Type-1 vector-sum formula).
   *
   * In execute(), defines two new scalar columns for the updated MET:
   * @code
   *   MET_x_new = MET_x_base − Σ_i (variedPt_i − nominalPt_i)·cos(φ_i)
   *   MET_y_new = MET_y_base − Σ_i (variedPt_i − nominalPt_i)·sin(φ_i)
   * @endcode
   * where the sum runs over all objects with nominal pT > @p ptThreshold.
   *
   * @param baseMETPtColumn    Input scalar MET-pT column (Float_t).
   * @param baseMETPhiColumn   Input scalar MET-φ column (Float_t).
   * @param nominalPtColumn    Nominal per-object pT column (RVec<Float_t>).
   * @param variedPtColumn     Varied per-object pT column (RVec<Float_t>).
   * @param outputMETPtColumn  Name for the output MET-pT column.
   * @param outputMETPhiColumn Name for the output MET-φ column.
   * @param ptThreshold        Only propagate objects with nominal pT above
   *                           this value (default: 0, i.e. all objects).
   *
   * @throws std::invalid_argument if any required column name is empty.
   * @throws std::runtime_error    if setObjectColumns() was not called first.
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
   * @brief Declare the RDF column holding the input PhysicsObjectCollection.
   *
   * @throws std::invalid_argument if collectionColumn is empty.
   */
  void setInputCollection(const std::string &collectionColumn);

  /**
   * @brief Schedule a corrected PhysicsObjectCollection output column.
   *
   * When @p correctedMassColumn is non-empty the output is built with both
   * corrected pT and mass via withCorrectedKinematics(); otherwise only pT
   * is updated via withCorrectedPt().
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

  /// pT column name (from setObjectColumns).
  const std::string &getPtColumn() const;

  /// Mass column name (from setObjectColumns; empty if not set).
  const std::string &getMassColumn() const;

  /// MET pT column name (from setMETColumns; empty if not set).
  const std::string &getMETPtColumn() const;

  /// MET phi column name (from setMETColumns; empty if not set).
  const std::string &getMETPhiColumn() const;

  /// Input collection column name (from setInputCollection; empty if not set).
  const std::string &getInputCollectionColumn() const;

  /// All registered systematic variations.
  const std::vector<EnergyVariationEntry> &getVariations() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  /// Must be overridden: return the concrete plugin type string.
  std::string type() const override = 0;

  void setContext(ManagerContext &ctx) override;
  void setupFromConfigFile() override {}

  /**
   * @brief Apply all scheduled corrections and register all systematics.
   *
   * Execution order:
   *  1. Scale correction steps.
   *  2. Resolution smearing steps.
   *  3. MET propagation steps.
   *  4. Collection output steps.
   *  5. Variation collection steps (+ optional variation map).
   *  6. Systematic registration with ISystematicManager.
   */
  void execute() override;

  void finalize() override {}
  void reportMetadata() override;

  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

protected:
  /**
   * @brief Return the physics-object name used in error messages and
   *        provenance keys (e.g. "Electron", "Photon", "Tau", "Muon").
   *
   * Must be overridden by concrete subclasses.
   */
  virtual std::string objectName() const = 0;

private:
  // ---- Helper -------------------------------------------------------------
  /// Derive a mass column name corresponding to a given pT column name.
  std::string deriveMassColumnName(const std::string &ptColName) const;

  /// Return a lower-case copy of @p s.
  static std::string toLower(const std::string &s);

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
  std::vector<EnergyVariationEntry> variations_m;

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
  IDataFrameProvider     *dataManager_m = nullptr;
  ISystematicManager     *systematicManager_m = nullptr;
  ILogger                *logger_m = nullptr;
  IOutputSink            *metaSink_m = nullptr;
};

#endif // OBJECTENERGYMANAGERBASE_H_INCLUDED
