#ifndef JETENERGYSCALEMANAGER_H_INCLUDED
#define JETENERGYSCALEMANAGER_H_INCLUDED

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
#include <memory>
#include <correction.h>

class Analyzer;

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
 *        (JER) corrections to jet collections, with full support for CMS-style
 *        systematic uncertainty sets, Type-1 MET propagation, and
 *        PhysicsObjectCollection integration.
 *
 * @see docs/JET_ENERGY_CORRECTIONS.md for the full user-facing reference.
 *
 * ## Features
 *  - **Removing existing JEC**: strip NanoAOD-level corrections using a raw
 *    factor column (CMS: @c Jet_pt_raw = Jet_pt × (1 − Jet_rawFactor)).
 *  - **Applying new corrections**: per-jet scale factor columns or via
 *    CMS-style Correctionlib evaluations through a CorrectionManager.
 *  - **Systematic sets**: register named source lists (e.g. "full", "reduced",
 *    or "individual") and apply all sources at once with applySystematicSet().
 *  - **Systematic variation tracking**: register named up/down variations with
 *    ISystematicManager; reportMetadata() logs a human-readable summary.
 *  - **MET propagation**: Type-1 MET propagation that updates MET pT and φ
 *    when jet pT changes due to corrections or systematic variations.
 *  - **PhysicsObjectCollection integration**: accept a per-event
 *    PhysicsObjectCollection of jets, produce a corrected nominal collection,
 *    per-variation up/down collections, and a PhysicsObjectVariationMap.
 *
 * ## Typical CMS NanoAOD usage (full workflow)
 * @code
 *   auto* jes = analyzer->getPlugin<JetEnergyScaleManager>("jes");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare jet and MET columns.
 *   jes->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
 *   jes->setMETColumns("MET_pt", "MET_phi");
 *
 *   // 2. Strip existing NanoAOD JEC.
 *   jes->removeExistingCorrections("Jet_rawFactor");
 *   // → defines Jet_pt_raw and Jet_mass_raw
 *
 *   // 3. Apply new L1L2L3 JEC via correctionlib.
 *   jes->applyCorrectionlib(*cm, "jec_l1l2l3", {"L3Residual"},
 *                           "Jet_pt_raw", "Jet_pt_jec");
 *   // → defines Jet_pt_jec and Jet_mass_jec
 *
 *   // 4. Register systematic source sets.
 *   jes->registerSystematicSources("full", {
 *       "AbsoluteCal", "AbsoluteScale", "AbsoluteMPFBias",
 *       "FlavorQCD", "Fragmentation", "PileUpDataMC",
 *       "PileUpPtRef", "RelativeFSR", "RelativeJEREC1",
 *       "RelativeJEREC2", "RelativeJERHF",
 *       "RelativePtBB", "RelativePtEC1", "RelativePtEC2",
 *       "RelativePtHF", "RelativeBal", "RelativeSample"
 *   });
 *   jes->registerSystematicSources("reduced", {"Total"});
 *
 *   // 5. Apply all sources in a set (creates up/down pT columns + registers
 *   //    all variations with ISystematicManager in execute()).
 *   jes->applySystematicSet(*cm, "jes_unc", "reduced",
 *                           "Jet_pt_jec", "Jet_pt_jes");
 *
 *   // 6. Propagate the nominal JEC correction to MET.
 *   jes->propagateMET("MET_pt", "MET_phi",
 *                     "Jet_pt_raw", "Jet_pt_jec",
 *                     "MET_pt_jec", "MET_phi_jec");
 *
 *   // 7. Propagate a JES variation to MET.
 *   jes->propagateMET("MET_pt_jec", "MET_phi_jec",
 *                     "Jet_pt_jec", "Jet_pt_jes_Total_up",
 *                     "MET_pt_jes_Total_up", "MET_phi_jes_Total_up");
 *
 *   // 8. PhysicsObjectCollection integration.
 *   jes->setInputJetCollection("goodJets");
 *   jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
 *   jes->defineVariationCollections("goodJets_jec", "goodJets",
 *                                   "goodJets_variations");
 *   // → defines goodJets_jec, goodJets_TotalUp, goodJets_TotalDown,
 *   //   goodJets_variations (PhysicsObjectVariationMap)
 * @endcode
 *
 * ## Applying a single CMS JES uncertainty source
 * @code
 *   jes->applyCorrectionlib(*cm, "jes_unc", {"AbsoluteCal", "up"},
 *                           "Jet_pt_jec", "Jet_pt_AbsoluteCal_up");
 *   jes->applyCorrectionlib(*cm, "jes_unc", {"AbsoluteCal", "down"},
 *                           "Jet_pt_jec", "Jet_pt_AbsoluteCal_dn");
 *   jes->addVariation("AbsoluteCal",
 *                     "Jet_pt_AbsoluteCal_up", "Jet_pt_AbsoluteCal_dn");
 * @endcode
 *
 * ## General (non-correctionlib) usage
 * @code
 *   jes->applyCorrection("Jet_pt_raw", "my_jes_sf", "Jet_pt_jec");
 * @endcode
 */
class JetEnergyScaleManager : public IPluggableManager {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<JetEnergyScaleManager> create(
      Analyzer& an, const std::string& role = "jetEnergyScaleManager");

  JetEnergyScaleManager() = default;

  // -------------------------------------------------------------------------
  // Jet column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the input jet kinematic column names.
   *
   * Must be called before execute().  An empty string for @p massColumn
   * disables automatic mass correction propagation.
   *
   * @param ptColumn   Name of the per-jet pT column (RVec<Float_t>).
   * @param etaColumn  Name of the per-jet η column (RVec<Float_t>).
   * @param phiColumn  Name of the per-jet φ column (RVec<Float_t>).
   *                   Also used as the source of jet directions for MET
   *                   propagation (jet φ does not change with JES/JER).
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
  // MET column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the base Missing Transverse Energy (MET) columns.
   *
   * These names are stored for reference and for use in reportMetadata().
   * They do not need to match the columns passed to propagateMET() – that
   * method accepts its own base MET column names – but it is conventional to
   * call this with the "starting" MET columns (e.g. @c "MET_pt", @c "MET_phi").
   *
   * @param metPtColumn  Name of the scalar MET-pT column (Float_t).
   * @param metPhiColumn Name of the scalar MET-φ column (Float_t).
   *
   * @throws std::invalid_argument if either argument is empty.
   */
  void setMETColumns(const std::string &metPtColumn,
                     const std::string &metPhiColumn);

  // -------------------------------------------------------------------------
  // Removing existing corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Strip existing JEC by computing a raw-pT (and raw-mass) column.
   *
   * Schedules in execute():
   * @code
   *   <ptColumn>_raw   = <ptColumn>   × (1 − rawFactorColumn)
   *   <massColumn>_raw = <massColumn> × (1 − rawFactorColumn)
   * @endcode
   *
   * CMS NanoAOD convention: @p rawFactorColumn = @c "Jet_rawFactor"
   * (= 1 − pt_raw / pt_jec).
   *
   * @throws std::invalid_argument if @p rawFactorColumn is empty.
   * @throws std::runtime_error    if setRawPtColumn() was already called.
   */
  void removeExistingCorrections(const std::string &rawFactorColumn);

  /**
   * @brief Declare an existing raw-pT column (already present in the dataframe).
   *
   * Mutually exclusive with removeExistingCorrections().
   *
   * @throws std::invalid_argument if @p rawPtColumn is empty.
   * @throws std::runtime_error    if removeExistingCorrections() was already called.
   */
  void setRawPtColumn(const std::string &rawPtColumn);

  /**
   * @brief Declare the auxiliary columns needed for JER smearing.
   *
   * @param genJetPtColumn Matched generator-jet pT per reco jet (0 or -1 if unmatched).
   * @param rhoColumn      Per-event rho column used by the JER payload.
   * @param eventColumn    Event identifier used to derive reproducible random numbers.
   *
   * @throws std::invalid_argument if any argument is empty.
   */
  void setJERSmearingColumns(const std::string &genJetPtColumn,
                             const std::string &rhoColumn,
                             const std::string &eventColumn);

  // -------------------------------------------------------------------------
  // Applying corrections
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule a correction using an existing per-jet scale factor column.
   *
   * In execute(), defines:
   * @code
   *   outputPtColumn   = inputPtColumn   × sfColumn
   *   outputMassColumn = inputMassColumn × sfColumn  [if applyToMass]
   * @endcode
   *
   * Mass column names are derived by replacing the @c ptColumn_m prefix with
   * @c massColumn_m when @p inputMassColumn / @p outputMassColumn are empty.
   *
   * @throws std::invalid_argument if any mandatory string argument is empty.
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
   * and store per-jet scale factors, then schedules the pT/mass update.
   *
   * The intermediate SF column name follows CorrectionManager's convention:
   * @c correctionName + "_" + joined(@p stringArgs, "_").
   *
   * @param cm               CorrectionManager that holds the registered correction.
   * @param correctionName   Name of the correction in @p cm.
   * @param stringArgs       Constant string arguments for correctionlib evaluation.
   *                         For CMS JES uncertainty sources use:
   *                         @c {"SourceName", "up"} or @c {"SourceName", "down"}.
   * @param inputPtColumn    Per-jet pT column to correct.
   * @param outputPtColumn   Name for the corrected-pT output column.
   * @param applyToMass      Also create a corrected-mass column (default: true).
   * @param inputMassColumn  Input mass column (derived if empty).
   * @param outputMassColumn Output mass column name (derived if empty).
   * @param inputColumns     Override for correctionlib numeric input columns.
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

  /**
   * @brief Schedule CMS JER smearing from correctionlib resolution and scale-factor payloads.
   *
   * The nominal or shifted JER scale factor is evaluated at execute time after
   * any preceding JEC columns already exist in the dataframe.
   *
   * @param cm                      CorrectionManager holding the registered corrections.
   * @param ptResolutionCorrection  Correction name for the pt-resolution payload.
   * @param scaleFactorCorrection   Correction name for the scale-factor payload.
   * @param inputPtColumn           Corrected jet pT to smear.
   * @param outputPtColumn          Output smeared jet pT column.
   * @param systematic              Scale-factor variation string, usually one of
   *                                "nom", "up", or "down".
   * @param applyToMass            Also smear the jet mass with the same factor.
   * @param inputMassColumn        Explicit input mass column; auto-derived if empty.
   * @param outputMassColumn       Explicit output mass column; auto-derived if empty.
   * @param ptResolutionInputs     Optional override for pt-resolution numeric inputs.
   * @param scaleFactorInputs      Optional override for scale-factor numeric inputs.
   */
  void applyJERSmearing(CorrectionManager &cm,
                        const std::string &ptResolutionCorrection,
                        const std::string &scaleFactorCorrection,
                        const std::string &inputPtColumn,
                        const std::string &outputPtColumn,
                        const std::string &systematic = "nom",
                        bool applyToMass = true,
                        const std::string &inputMassColumn = "",
                        const std::string &outputMassColumn = "",
                        const std::vector<std::string> &ptResolutionInputs = {},
                        const std::vector<std::string> &scaleFactorInputs = {});

  // -------------------------------------------------------------------------
  // CMS systematic source sets
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named list of CMS JES/JER uncertainty source names.
   *
   * Source sets allow bulk application of multiple uncertainty sources via
   * applySystematicSet().  Typical CMS sets are:
   *  - @b full: all individual JES uncertainty sources (e.g. AbsoluteCal,
   *    AbsoluteScale, FlavorQCD, …).
   *  - @b reduced: a smaller combined set (e.g. just "Total").
   *  - @b individual: any single-source list.
   *
   * Multiple calls with the same @p setName replace the previous list.
   *
   * @param setName  User-chosen label (e.g. @c "full", @c "reduced").
   * @param sources  Ordered list of CMS source names that will be passed as
   *                 the first string argument to the correctionlib evaluation,
   *                 followed by @c "up" or @c "down".
   *
   * @throws std::invalid_argument if @p setName or any element of @p sources
   *         is empty, or if @p sources is empty.
   */
  void registerSystematicSources(const std::string &setName,
                                  const std::vector<std::string> &sources);

  /**
   * @brief Return the list of source names registered under @p setName.
   *
   * @throws std::out_of_range if @p setName has not been registered.
   */
  const std::vector<std::string> &
  getSystematicSources(const std::string &setName) const;

  /**
   * @brief Apply all sources in a named systematic set via correctionlib.
   *
   * For each source @c S in the registered set @p setName, this method:
   *  1. Calls @c cm.applyCorrectionVec(correctionName, {S, "up"}, inputColumns)
   *     → SF column @c correctionName_S_up.
   *  2. Calls @c cm.applyCorrectionVec(correctionName, {S, "down"}, inputColumns)
   *     → SF column @c correctionName_S_down.
   *  3. Schedules corrected-pT columns:
   *     @c outputPtPrefix_S_up and @c outputPtPrefix_S_down.
   *  4. Registers the variation @c S with up = @c outputPtPrefix_S_up,
   *     down = @c outputPtPrefix_S_down (and mass columns when @p applyToMass
   *     is @c true).
   *
   * The correctionlib string argument order for each source is
   * @c {S, "up"} / @c {S, "down"}.  If your correction uses a different
   * argument order, call applyCorrectionlib() directly for each source.
   *
   * @param cm               CorrectionManager that holds the registered correction.
   * @param correctionName   Name of the correction in @p cm.
   * @param setName          Name of the previously registered source set.
   * @param inputPtColumn    Per-jet pT input column.
   * @param outputPtPrefix   Prefix for the per-source output pT column names.
   *                         Output columns will be named
   *                         @c <outputPtPrefix>_<source>_up and
   *                         @c <outputPtPrefix>_<source>_down.
   * @param applyToMass      Also define corrected-mass columns (default: true).
   * @param inputColumns     Override for correctionlib numeric input columns.
   * @param inputMassColumn  Explicit input mass column; auto-derived if empty.
   *
   * @throws std::runtime_error    if @p setName is not registered.
   * @throws std::invalid_argument if any mandatory argument is empty.
   */
  void applySystematicSet(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::string &setName,
                          const std::string &inputPtColumn,
                          const std::string &outputPtPrefix,
                          bool applyToMass = true,
                          const std::vector<std::string> &inputColumns = {},
                          const std::string &inputMassColumn = "");

  // -------------------------------------------------------------------------
  // Systematic variation registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a JES/JER systematic variation for framework tracking.
   *
   * In execute(), registers each direction with ISystematicManager so that
   * downstream histogramming and validation tools propagate the systematic.
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
  // Type-1 MET propagation
  // -------------------------------------------------------------------------

  /**
   * @brief Register a Type-1 MET propagation step.
   *
   * When jet energies change (due to a correction or a systematic variation),
   * the MET must be updated to compensate.  This method registers a deferred
   * propagation step that is executed in execute().
   *
   * The propagation formula for each jet @c j above @p jetPtThreshold is:
   * @code
   *   dMET_x = -sum_j [(variedPt_j - nominalPt_j) * cos(phi_j)]
   *   dMET_y = -sum_j [(variedPt_j - nominalPt_j) * sin(phi_j)]
   *   new_MET_x = baseMET_x + dMET_x
   *   new_MET_y = baseMET_y + dMET_y
   * @endcode
   *
   * The jet φ column used is the one declared in setJetColumns().
   *
   * @param baseMETPtColumn     Input scalar MET-pT column (Float_t).
   * @param baseMETPhiColumn    Input scalar MET-φ column (Float_t).
   * @param nominalJetPtColumn  Jet pT column representing the "before" state.
   * @param variedJetPtColumn   Jet pT column representing the "after" state.
   * @param outputMETPtColumn   Output MET-pT column name.
   * @param outputMETPhiColumn  Output MET-φ column name.
   * @param jetPtThreshold      Only jets above this pT (GeV, applied to
   *                            @p nominalJetPtColumn) enter the propagation
   *                            (default: 15 GeV, CMS standard).
   *
   * @throws std::invalid_argument if any column name is empty.
   * @throws std::runtime_error    if setJetColumns() has not been called
   *                               (phi column required for propagation).
   */
  void propagateMET(const std::string &baseMETPtColumn,
                    const std::string &baseMETPhiColumn,
                    const std::string &nominalJetPtColumn,
                    const std::string &variedJetPtColumn,
                    const std::string &outputMETPtColumn,
                    const std::string &outputMETPhiColumn,
                    float jetPtThreshold = 15.0f);

  // -------------------------------------------------------------------------
  // PhysicsObjectCollection integration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the name of the RDF column holding the input jet
   *        PhysicsObjectCollection (one collection per event).
   *
   * This call is the entry point for the collection-based workflow.  The
   * named column must be of type @c PhysicsObjectCollection and must be
   * present in the dataframe before execute() is called.
   *
   * @param collectionColumn  RDF column name that produces a
   *                          @c PhysicsObjectCollection per event.
   *
   * @throws std::invalid_argument if @p collectionColumn is empty.
   */
  void setInputJetCollection(const std::string &collectionColumn);

  /**
   * @brief Schedule definition of a corrected PhysicsObjectCollection column.
   *
   * In execute(), defines a new RDF column @p outputCollectionColumn of type
   * @c PhysicsObjectCollection whose per-jet pT values are taken from
   * @p correctedPtColumn (a full-size RVec column), and optionally whose mass
   * values are taken from @p correctedMassColumn.  Eta and phi are preserved
   * from the input collection.
   *
   * Requires setInputJetCollection() to have been called beforehand.
   *
   * @code
   *   // After applying the nominal JEC:
   *   jes->setInputJetCollection("goodJets");
   *   jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
   *   // After execute(), the dataframe has a "goodJets_jec" column of type
   *   // PhysicsObjectCollection with each jet's pT updated by the JEC.
   * @endcode
   *
   * @param correctedPtColumn       Full-size per-jet pT RVec column (all jets
   *                                in the original collection, not just
   *                                selected ones).
   * @param outputCollectionColumn  Output column name.
   * @param correctedMassColumn     Optional full-size per-jet mass RVec column.
   *                                When empty, mass values from the input
   *                                collection are preserved.
   *
   * @throws std::invalid_argument if @p correctedPtColumn or
   *         @p outputCollectionColumn is empty.
   * @throws std::runtime_error    if setInputJetCollection() was not called.
   */
  void defineCollectionOutput(const std::string &correctedPtColumn,
                              const std::string &outputCollectionColumn,
                              const std::string &correctedMassColumn = "");

  /**
   * @brief Schedule definition of up/down variation collection columns and an
   *        optional PhysicsObjectVariationMap column for all registered
   *        variations.
   *
   * For each variation registered via addVariation() this method schedules
   * (in execute()) the definition of two PhysicsObjectCollection columns:
   *  - @c <collectionPrefix>_<variationName>Up: jets with upPtColumn pT values.
   *  - @c <collectionPrefix>_<variationName>Down: jets with downPtColumn pT values.
   *
   * The names for the individual variation columns follow the pattern:
   *  - @c <collectionPrefix>_<variationName>Up
   *  - @c <collectionPrefix>_<variationName>Down
   *
   * If @p variationMapColumn is non-empty an additional column of type
   * @c PhysicsObjectVariationMap is defined, containing:
   *  - Key @c "nominal" → @p nominalCollectionColumn
   *  - Key @c "<variationName>Up" / @c "<variationName>Down" for each variation
   *
   * Requires setInputJetCollection() to have been called beforehand.
   *
   * @code
   *   jes->setInputJetCollection("goodJets");
   *   jes->defineCollectionOutput("Jet_pt_jec", "goodJets_jec");
   *   jes->defineVariationCollections("goodJets_jec",
   *                                   "goodJets",
   *                                   "goodJets_variations");
   *   // After execute():
   *   //   "goodJets_AbsoluteCal_up" : PhysicsObjectCollection
   *   //   "goodJets_AbsoluteCal_down" : PhysicsObjectCollection
   *   //   "goodJets_variations" : PhysicsObjectVariationMap
   *   //       ["nominal"]        → goodJets_jec collection
   *   //       ["AbsoluteCalUp"]  → goodJets_AbsoluteCal_up collection
   *   //       ...
   * @endcode
   *
   * @param nominalCollectionColumn  The nominal (already-corrected) collection
   *                                 column name; used as the "nominal" key in
   *                                 the variation map.
   * @param collectionPrefix         Prefix for output variation column names
   *                                 (e.g. "goodJets" → "goodJets_TotalUp").
   * @param variationMapColumn       If non-empty, name of the output
   *                                 PhysicsObjectVariationMap column.
   *
   * @throws std::runtime_error    if setInputJetCollection() was not called.
   * @throws std::invalid_argument if @p nominalCollectionColumn or
   *         @p collectionPrefix is empty.
   */
  void defineVariationCollections(const std::string &nominalCollectionColumn,
                                  const std::string &collectionPrefix,
                                  const std::string &variationMapColumn = "");

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Return the raw-pT column name (non-empty after removeExistingCorrections() or setRawPtColumn()).
  const std::string &getRawPtColumn() const;

  /// Return the original pT column name (as set by setJetColumns()).
  const std::string &getPtColumn() const;

  /// Return the original mass column name (as set by setJetColumns()).
  const std::string &getMassColumn() const;

  /// Return the base MET-pT column name (as set by setMETColumns()).
  const std::string &getMETPtColumn() const;

  /// Return the base MET-φ column name (as set by setMETColumns()).
  const std::string &getMETPhiColumn() const;

  /// Return the input jet collection column name (as set by setInputJetCollection()).
  const std::string &getInputJetCollectionColumn() const;

  /// Return all registered systematic variations.
  const std::vector<JESVariationEntry> &getVariations() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "JetEnergyScaleManager"; }

  void setContext(ManagerContext &ctx) override;

  /** No-op: corrections are registered programmatically. */
  void setupFromConfigFile() override {}

  /**
   * @brief Define all correction/propagation/collection output columns.
   *
   * Execution order:
   *  1. Raw-pT and raw-mass columns (if removeExistingCorrections() was called).
   *  2. Each scheduled correction step (applyCorrection / applyCorrectionlib).
   *  3. Each MET propagation step (propagateMET).
   *  4. Each collection output step (defineCollectionOutput).
   *  5. Each variation-collection step (defineVariationCollections).
   *  6. Register all systematic variations with ISystematicManager.
   */
  void execute() override;

  /** No-op: no deferred results to retrieve. */
  void finalize() override {}

  /**
   * @brief Log a human-readable JES/JER + MET configuration summary.
   */
  void reportMetadata() override;

  /**
   * @brief Contribute structured provenance metadata for this plugin.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Jet column names ---------------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- MET column names ---------------------------------------------------
  std::string metPtColumn_m;
  std::string metPhiColumn_m;

  // ---- Raw pT computation -------------------------------------------------
  std::string rawFactorColumn_m;
  std::string rawPtColumn_m;
  std::string rawMassColumn_m;
  std::string genJetPtColumn_m;
  std::string rhoColumn_m;
  std::string eventColumn_m;

  // ---- Correction steps ---------------------------------------------------
  struct CorrectionStep {
    std::string inputPtColumn;
    std::string sfColumn;
    std::string outputPtColumn;
    std::string inputMassColumn;
    std::string outputMassColumn;
  };
  std::vector<CorrectionStep> correctionSteps_m;

  struct JERSmearingStep {
    correction::Correction::Ref ptResolutionCorrection;
    correction::Correction::Ref scaleFactorCorrection;
    std::vector<std::string> ptResolutionInputs;
    std::vector<std::string> scaleFactorInputs;
    std::string systematic;
    std::string inputPtColumn;
    std::string outputPtColumn;
    std::string inputMassColumn;
    std::string outputMassColumn;
  };
  std::vector<JERSmearingStep> jerSmearingSteps_m;

  // ---- Systematic variations ----------------------------------------------
  std::vector<JESVariationEntry> variations_m;

  // ---- Named systematic source sets ---------------------------------------
  std::map<std::string, std::vector<std::string>> systematicSets_m;

  // ---- MET propagation steps (deferred to execute()) ----------------------
  struct METPropagationStep {
    std::string baseMETPtColumn;
    std::string baseMETPhiColumn;
    std::string nominalJetPtColumn;
    std::string variedJetPtColumn;
    std::string outputMETPtColumn;
    std::string outputMETPhiColumn;
    float jetPtThreshold;
  };
  std::vector<METPropagationStep> metPropagationSteps_m;

  // ---- PhysicsObjectCollection integration --------------------------------
  std::string inputJetCollectionColumn_m; ///< Input POC column (set by setInputJetCollection).

  /// Deferred step: produce a corrected PhysicsObjectCollection column.
  struct CollectionOutputStep {
    std::string correctedPtColumn;
    std::string correctedMassColumn; ///< may be empty
    std::string outputCollectionColumn;
  };
  std::vector<CollectionOutputStep> collectionOutputSteps_m;

  /// Deferred step: produce per-variation collection columns + optional map.
  struct VariationCollectionsStep {
    std::string nominalCollectionColumn;
    std::string collectionPrefix;
    std::string variationMapColumn; ///< may be empty
  };
  std::vector<VariationCollectionsStep> variationCollectionsSteps_m;

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
   * Replaces @c ptColumn_m prefix in @p ptColName with @c massColumn_m.
   * Returns empty string when substitution is not possible.
   */
  std::string deriveMassColumnName(const std::string &ptColName) const;
};



#endif // JETENERGYSCALEMANAGER_H_INCLUDED
