#ifndef TAGGERWORKINGPOINTMANAGER_H_INCLUDED
#define TAGGERWORKINGPOINTMANAGER_H_INCLUDED

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

#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <TH1D.h>

class Analyzer;

// ---------------------------------------------------------------------------
// Supporting structs
// ---------------------------------------------------------------------------

/**
 * @brief A single working-point definition for a tagger discriminator.
 *
 * Working points are ordered (ascending), allowing selection categories such as
 * "pass medium but fail tight".  For multi-score WPs (e.g. charm-tagging with
 * CvsL and CvsB), @p thresholds holds one threshold per discriminant column
 * in the same order as `setTaggerColumns()`.  For single-score WPs there is
 * exactly one element.
 */
struct WorkingPointEntry {
  std::string name;                ///< Human-readable label (e.g. "loose", "medium", "tight")
  std::vector<float> thresholds;  ///< Per-discriminant score thresholds; an object passes this WP
                                   ///< only if ALL scores meet their respective thresholds.
};

/**
 * @brief Record of a per-object SF column pair for a tagger systematic variation.
 *
 * Populated by addVariation() and used in execute() for systematic registration
 * and variation-collection definition.
 */
struct TaggingVariationEntry {
  std::string name;            ///< Systematic name (e.g. "btagHF")
  std::string upSFColumn;      ///< Per-object SF column for the up shift   (RVec<Float_t>)
  std::string downSFColumn;    ///< Per-object SF column for the down shift (RVec<Float_t>)
  std::string upWeightColumn;  ///< Per-event weight column for the up shift   (Float_t)
  std::string downWeightColumn;///< Per-event weight column for the down shift (Float_t)
};

// ---------------------------------------------------------------------------
// WP selection category
// ---------------------------------------------------------------------------

/**
 * @brief Describes a working-point-based selection for filtered collections.
 *
 * The selection determines which objects are kept in a PhysicsObjectCollection:
 *
 * | Type            | Meaning                                          |
 * |-----------------|--------------------------------------------------|
 * | PassWP          | object passes at least one named WP              |
 * | FailWP          | object fails a named WP (below its threshold)    |
 * | PassRangeWP     | object passes lower WP but fails upper WP        |
 *
 * @see TaggerWorkingPointManager::defineWorkingPointCollection
 */
struct WPCollectionSelection {
  enum class Type { PassWP, FailWP, PassRangeWP };
  Type type;
  std::string wpNameLower;  ///< Name of the lower (or single) WP
  std::string wpNameUpper;  ///< Name of the upper WP (only for PassRangeWP)
};

// ---------------------------------------------------------------------------
// TaggerWorkingPointManager
// ---------------------------------------------------------------------------

/**
 * @class TaggerWorkingPointManager
 * @brief Plugin for applying working-point-based tagger corrections and
 *        systematics to any PhysicsObjectCollection.
 *
 * ## Overview
 *
 * This plugin handles CMS-style working-point (WP) tagger scale factors for
 * **any physics object** with a tagger discriminator — including jets, taus,
 * and fat-jets.  Common use cases include:
 *  - **Jets**: b-tagging (DeepJet, RobustParTAK4, ParticleNet)
 *  - **Jets (multi-score)**: charm-tagging (CvsL + CvsB simultaneously)
 *  - **Taus**: DeepTau ID (VSe, VSmu, VSjet discriminators)
 *  - **Fat jets**: Xbb / Xcc boosted taggers
 *
 * The plugin supports:
 *  - **Working-point definitions**: declare any number of named WPs with
 *    score thresholds (e.g. loose/medium/tight).
 *  - **Multi-score WPs**: a single WP can require passing thresholds on
 *    *multiple* discriminants simultaneously (e.g. charm-tagging uses both
 *    CvsL and CvsB; see setTaggerColumns() and the vector overload of
 *    addWorkingPoint()).
 *  - **WP category column**: per-object integer encoding which WP range an
 *    object falls into (0 = fail all, 1 = pass lowest, …, N = pass all).
 *  - **Correctionlib scale factors**: apply per-object SFs from a correctionlib
 *    payload to produce per-event weight columns compatible with WeightManager.
 *  - **Generator-level fraction correction**: optionally accept a second
 *    correctionlib payload encoding the MC fraction of objects in each WP
 *    category at given (pt, η).  These fractions reweight the per-object SF
 *    contributions to properly reflect the generator-level object distributions
 *    when making WP-based selections.
 *  - **Systematic source sets**: bulk application of named uncertainty sources
 *    via applySystematicSet(), matching the JetEnergyScaleManager API.
 *  - **WP-filtered PhysicsObjectCollections**: produce collections of objects
 *    passing specific WP criteria (pass_<wp>, fail_<wp>,
 *    pass_<wp1>_fail_<wp2>).
 *  - **Variation collections and map**: per-variation up/down object collections
 *    and a PhysicsObjectVariationMap for downstream systematic propagation.
 *  - **Fraction histogram utility**: book per-(pt, η, flavour) tagger-score
 *    histograms in a dedicated pre-processing run so the fraction payload can
 *    be constructed from MC data.
 *
 * ## Typical usage — Jets (b-tagging with DeepJet)
 * @code
 *   auto* twm = analyzer->getPlugin<TaggerWorkingPointManager>("btagManager");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare kinematic column names.
 *   twm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
 *
 *   // 2. Declare the tagger discriminator score column.
 *   twm->setTaggerColumn("Jet_btagDeepFlavB");
 *
 *   // 3. Register working points in order of increasing threshold.
 *   twm->addWorkingPoint("loose",  0.0521f);
 *   twm->addWorkingPoint("medium", 0.3033f);
 *   twm->addWorkingPoint("tight",  0.7489f);
 *
 *   // 4. Set the input PhysicsObjectCollection column.
 *   twm->setInputObjectCollection("goodJets");
 *
 *   // 5. Apply a correctionlib SF payload.
 *   twm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
 *                           {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
 *                            "Jet_btagDeepFlavB"});
 *   // → "deepjet_sf_central_weight"
 *
 *   // 6. Register systematic sources and apply in one call.
 *   twm->registerSystematicSources("standard",
 *                                   {"hf", "lf", "hfstats1", "hfstats2",
 *                                    "lfstats1", "lfstats2", "cferr1", "cferr2"});
 *   twm->applySystematicSet(*cm, "deepjet_sf", "standard",
 *                           {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
 *                            "Jet_btagDeepFlavB"});
 *
 *   // 7. Define WP-filtered object collections.
 *   twm->defineWorkingPointCollection("pass_medium", "goodJets_bmedium");
 *   twm->defineWorkingPointCollection("fail_loose",  "goodJets_bfail");
 *   twm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
 *                                    "goodJets_btag_variations");
 * @endcode
 *
 * ## Typical usage — Jets (charm-tagging with CvsL + CvsB)
 * @code
 *   auto* ctwm = analyzer->getPlugin<TaggerWorkingPointManager>("ctagManager");
 *
 *   // 1. Declare kinematic columns.
 *   ctwm->setObjectColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
 *
 *   // 2. Declare BOTH charm-tagger score columns (order matters).
 *   ctwm->setTaggerColumns({"Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});
 *
 *   // 3. Register 2D working points: {CvsL threshold, CvsB threshold}.
 *   //    An object passes a WP only if BOTH its CvsL and CvsB scores are
 *   //    at or above the respective thresholds.
 *   ctwm->addWorkingPoint("loose",  {0.042f, 0.135f});
 *   ctwm->addWorkingPoint("medium", {0.108f, 0.285f});
 *   ctwm->addWorkingPoint("tight",  {0.274f, 0.605f});
 *   // → defines "Jet_pt_wp_category" encoding category 0..3
 *
 *   // 4. Set input collection and apply SFs as usual.
 *   ctwm->setInputObjectCollection("goodJets");
 *   ctwm->applyCorrectionlib(*cm, "ctag_sf", {"central"},
 *                            {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
 *                             "Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});
 * @endcode
 *
 * ## Typical usage — Taus (DeepTau ID)
 * @code
 *   auto* tauTwm = analyzer->getPlugin<TaggerWorkingPointManager>("tauIdManager");
 *
 *   // 1. Declare tau kinematic column names.
 *   tauTwm->setObjectColumns("Tau_pt", "Tau_eta", "Tau_phi", "Tau_mass");
 *
 *   // 2. Set the DeepTau VSjet discriminator score column.
 *   tauTwm->setTaggerColumn("Tau_rawDeepTau2018v2p5VSjet");
 *
 *   // 3. Register DeepTau VSjet WPs (Run 3 working points).
 *   tauTwm->addWorkingPoint("VVVLoose", 0.05f);
 *   tauTwm->addWorkingPoint("Medium",   0.49f);
 *   tauTwm->addWorkingPoint("Tight",    0.75f);
 *
 *   // 4. Set the input tau collection.
 *   tauTwm->setInputObjectCollection("goodTaus");
 *
 *   // 5. Apply tau ID SF from correctionlib.
 *   tauTwm->applyCorrectionlib(*cm, "tau_id_sf", {"pt_binned", "central"},
 *                              {"Tau_pt", "Tau_decayMode",
 *                               "Tau_genPartFlav"});
 *   // → "tau_id_sf_pt_binned_central_weight"
 *
 *   // 6. Apply systematic variations.
 *   tauTwm->registerSystematicSources("tau_unc", {"stat0", "stat1", "syst"});
 *   tauTwm->applySystematicSet(*cm, "tau_id_sf", "tau_unc",
 *                              {"Tau_pt", "Tau_decayMode", "Tau_genPartFlav"});
 *
 *   // 7. WP-filtered tau collections.
 *   tauTwm->defineWorkingPointCollection("pass_Medium", "goodTaus_medium");
 * @endcode
 *
 * ## Working-point categories
 *
 * With WPs [loose=0.05, medium=0.3, tight=0.75] the per-object category column
 * `<ptColumn>_wp_category` is:
 * | category | meaning                                   |
 * |----------|-------------------------------------------|
 * |    0     | score < 0.05  (fail all WPs)              |
 * |    1     | 0.05 ≤ score < 0.3  (pass loose, fail medium) |
 * |    2     | 0.3  ≤ score < 0.75 (pass medium, fail tight) |
 * |    3     | score ≥ 0.75 (pass all WPs / pass tight)  |
 *
 * For multi-score WPs (e.g. charm tagging), an object passes WP `i` only if
 * **all** its discriminant scores meet the respective thresholds for WP `i`.
 * The category is still the count of consecutive WPs passed (break on first
 * failure), so WPs should be defined in globally nested order if possible.
 *
 * Selection expressions for defineWorkingPointCollection():
 * | string                       | objects kept                      |
 * |------------------------------|-----------------------------------|
 * | `"pass_loose"`               | category ≥ 1                      |
 * | `"pass_medium"`              | category ≥ 2                      |
 * | `"fail_loose"`               | category = 0                      |
 * | `"pass_medium_fail_tight"`   | category = 2 (pass M, fail T)     |
 * | `"pass_loose_fail_medium"`   | category = 1 (pass L, fail M)     |
 *
 * ## Generator-level fraction reweighting
 *
 * When setFractionCorrection() is called the per-event weight becomes
 * a fraction-weighted sum rather than a product of per-object SFs.  For each
 * object @c j, the contribution is:
 * @code
 *   weight_j = SF(pt_j, η_j, …) / mc_fraction(pt_j, η_j, category_j)
 * @endcode
 * This ensures that the selected object distribution matches the generator-level
 * expectation when SFs differ across WP categories.
 *
 * @see docs/TAGGER_WORKING_POINTS.md for the complete reference.
 * @see JetEnergyScaleManager for JES/JER corrections.
 * @see WeightManager for applying the resulting weight columns.
 */
class TaggerWorkingPointManager : public IPluggableManager {
public:

  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------

  /**
   * @brief Create a TaggerWorkingPointManager, register it with @p an
   *        under @p role, and return the owning shared_ptr.
   */
  static std::shared_ptr<TaggerWorkingPointManager> create(
      Analyzer &an, const std::string &role = "btagManager");

  // -------------------------------------------------------------------------
  // Jet column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the object kinematic column names.
   *
   * @param ptColumn   Per-object pT column (RVec<Float_t>).
   * @param etaColumn  Per-object η column.
   * @param phiColumn  Per-object φ column.
   * @param massColumn Per-object mass column (may be empty to skip mass handling).
   *
   * @throws std::invalid_argument if @p ptColumn is empty.
   */
  void setObjectColumns(const std::string &ptColumn,
                     const std::string &etaColumn,
                     const std::string &phiColumn,
                     const std::string &massColumn);

  // -------------------------------------------------------------------------
  // Tagger discriminator configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the per-object tagger discriminator score column (single-score).
   *
   * Shorthand for `setTaggerColumns({taggerScoreColumn})`.  The column must be
   * of type `RVec<Float_t>` with one entry per object.
   *
   * @param taggerScoreColumn  RDF column name.
   * @throws std::invalid_argument if empty.
   */
  void setTaggerColumn(const std::string &taggerScoreColumn);

  /**
   * @brief Set multiple per-object tagger discriminator score columns.
   *
   * Use this for multi-score WPs such as CMS charm-tagging where both CvsL and
   * CvsB are required.  Each column must be of type `RVec<Float_t>` with one
   * entry per object.  The order of columns here must match the order of
   * threshold values passed to the vector overload of addWorkingPoint().
   *
   * @code
   *   ctwm->setTaggerColumns({"Jet_btagDeepFlavCvL", "Jet_btagDeepFlavCvB"});
   * @endcode
   *
   * @param taggerScoreColumns  Ordered list of RDF column names (all non-empty).
   * @throws std::invalid_argument if the list is empty or any element is empty.
   */
  void setTaggerColumns(const std::vector<std::string> &taggerScoreColumns);

  // -------------------------------------------------------------------------
  // Working-point registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named working point with a single-score threshold.
   *
   * Shorthand for `addWorkingPoint(name, {threshold})`.  Working points must
   * be added in strictly *increasing* threshold order:
   * @code
   *   twm->addWorkingPoint("loose",  0.0521f);
   *   twm->addWorkingPoint("medium", 0.3033f);
   *   twm->addWorkingPoint("tight",  0.7489f);
   * @endcode
   *
   * @param name       Human-readable WP label (must be unique, non-empty).
   * @param threshold  Score threshold; objects with score ≥ threshold pass.
   *
   * @throws std::invalid_argument if @p name is empty, already registered,
   *         or if the threshold is not strictly greater than the previously
   *         registered single-score WP threshold.
   */
  void addWorkingPoint(const std::string &name, float threshold);

  /**
   * @brief Register a named working point with per-discriminant thresholds.
   *
   * For multi-score WPs the @p thresholds vector must have one entry for each
   * tagger column registered via setTaggerColumns() (if setTaggerColumns() has
   * already been called; otherwise this is validated lazily in execute()).
   * An object passes this WP only if **all** its discriminant scores are ≥ the
   * respective threshold.
   *
   * @code
   *   // CvsL threshold = 0.108, CvsB threshold = 0.285
   *   ctwm->addWorkingPoint("medium", {0.108f, 0.285f});
   * @endcode
   *
   * For single-score WPs (thresholds.size() == 1), the same ascending-order
   * constraint is enforced as for the single-float overload.  For multi-score
   * WPs the ordering check is not enforced (user responsibility).
   *
   * @param name        Human-readable WP label (must be unique, non-empty).
   * @param thresholds  Per-discriminant score thresholds (must be non-empty).
   *
   * @throws std::invalid_argument if @p name is empty, already registered, or
   *         if @p thresholds is empty.
   */
  void addWorkingPoint(const std::string &name,
                       const std::vector<float> &thresholds);

  // -------------------------------------------------------------------------
  // Input collection
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the RDF column holding the input PhysicsObjectCollection.
   *
   * This enables the WP-filtered collection API (defineWorkingPointCollection,
   * defineVariationCollections).  The named column must be present in the
   * dataframe before execute() is called.
   *
   * @param collectionColumn  RDF column name of type PhysicsObjectCollection.
   * @throws std::invalid_argument if empty.
   */
  void setInputObjectCollection(const std::string &collectionColumn);

  // -------------------------------------------------------------------------
  // Scale-factor application
  // -------------------------------------------------------------------------

  /**
   * @brief Apply a correctionlib-based per-object SF and produce a per-event
   *        weight column.
   *
   * The correctionlib payload is evaluated once per object per event via
   * CorrectionManager::applyCorrectionVec().  The resulting per-object
   * RVec<Float_t> SF column is then reduced to a per-event scalar weight
   * (product of all per-object SFs for objects in the input collection) stored in
   * the column `<correctionName>_<stringArgs...>_weight`.
   *
   * If setFractionCorrection() has been called, each per-object contribution
   * is divided by the MC fraction for that jet's WP category, normalising
   * the weight to the generator-level distribution.
   *
   * @param cm               CorrectionManager holding the registered correction.
   * @param correctionName   Name of the correction in @p cm.
   * @param stringArgs       Constant string arguments passed to the correctionlib
   *                         evaluator (e.g. @c {"central"}).  Each argument is
   *                         also appended to the output weight column name.
   * @param inputColumns     Ordered list of numeric input columns for the
   *                         correctionlib evaluator (RVec columns for per-object
   *                         quantities).  When empty, uses the columns
   *                         registered with the correction.
   *
   * @throws std::invalid_argument if @p correctionName is empty.
   * @throws std::runtime_error    if setInputObjectCollection() was not called.
   */
  void applyCorrectionlib(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::vector<std::string> &stringArgs,
                          const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Generator-level fraction correction
  // -------------------------------------------------------------------------

  /**
   * @brief Set a correctionlib payload encoding MC generator-level jet
   *        fractions per WP category.
   *
   * The fraction payload evaluates to the MC fraction of objects at a given
   * (pt, η, …) that fall in a given WP category.  Typical inputs are
   * `{"Jet_pt", "Jet_eta", "Jet_pt_wp_category"}` (after setTaggerColumn and
   * addWorkingPoint have been called and execute() has defined the category
   * column).
   *
   * When this method is called, the per-event weight produced by
   * applyCorrectionlib() is divided by the MC fraction for each jet,
   * reflecting the proper generator-level jet distribution.
   *
   * @param cm                    CorrectionManager holding the correction.
   * @param fractionCorrectionName  Name of the fraction correction in @p cm.
   * @param inputColumns          Ordered list of numeric input columns (RVec
   *                              columns, one entry per object per event).  Must
   *                              include the WP category column
   *                              `<ptColumn>_wp_category` produced by execute().
   *
   * @throws std::invalid_argument if @p fractionCorrectionName is empty.
   */
  void setFractionCorrection(CorrectionManager &cm,
                             const std::string &fractionCorrectionName,
                             const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Systematic source sets
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named list of tagger systematic source names.
   *
   * Mirrors the JetEnergyScaleManager API.  Source sets allow bulk application
   * of multiple uncertainty sources via applySystematicSet().
   *
   * @param setName  User-chosen label (e.g. @c "standard", @c "reduced").
   * @param sources  Ordered list of CMS source names that are passed as the
   *                 first string argument to the correctionlib evaluation,
   *                 followed by @c "up" or @c "down".
   *
   * @throws std::invalid_argument if @p setName or any element of @p sources
   *         is empty, or if @p sources is empty.
   */
  void registerSystematicSources(const std::string &setName,
                                  const std::vector<std::string> &sources);

  /**
   * @brief Return the source names registered under @p setName.
   * @throws std::out_of_range if @p setName has not been registered.
   */
  const std::vector<std::string> &
  getSystematicSources(const std::string &setName) const;

  /**
   * @brief Apply all sources in a named set via correctionlib.
   *
   * For each source @c S in the set, two weight columns are produced:
   *  - `<correctionName>_<S>_up_weight`
   *  - `<correctionName>_<S>_down_weight`
   *
   * Each variation is also registered via addVariation() so it is tracked
   * by ISystematicManager and propagated to downstream WP-filtered collections.
   *
   * @param cm              CorrectionManager holding the correction.
   * @param correctionName  Name of the correction in @p cm.
   * @param setName         Name of the previously registered source set.
   * @param inputColumns    Numeric input columns for the evaluator.
   *
   * @throws std::runtime_error    if @p setName is not registered.
   * @throws std::invalid_argument if any mandatory argument is empty.
   */
  void applySystematicSet(CorrectionManager &cm,
                          const std::string &correctionName,
                          const std::string &setName,
                          const std::vector<std::string> &inputColumns = {});

  // -------------------------------------------------------------------------
  // Manual variation registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a tagger systematic variation for framework tracking.
   *
   * In execute(), registers both directions with ISystematicManager.
   * The @p upSFColumn and @p downSFColumn must be per-object RVec<Float_t>
   * columns already defined in the dataframe (e.g. via applyCorrectionlib
   * or applySystematicSet).
   *
   * @param systematicName  Base name (e.g. @c "btagHF").
   * @param upSFColumn      Per-object SF column for the up variation.
   * @param downSFColumn    Per-object SF column for the down variation.
   * @param upWeightColumn  Corresponding per-event weight column for up.
   * @param downWeightColumn Corresponding per-event weight column for down.
   *
   * @throws std::invalid_argument if any argument is empty.
   */
  void addVariation(const std::string &systematicName,
                    const std::string &upSFColumn,
                    const std::string &downSFColumn,
                    const std::string &upWeightColumn = "",
                    const std::string &downWeightColumn = "");

  // -------------------------------------------------------------------------
  // WP-filtered PhysicsObjectCollection definitions
  // -------------------------------------------------------------------------

  /**
   * @brief Schedule definition of a WP-filtered PhysicsObjectCollection.
   *
   * In execute(), defines a new RDF column @p outputCollectionColumn of type
   * PhysicsObjectCollection containing only objects that satisfy @p selection.
   *
   * Supported selection strings (names must match those passed to addWorkingPoint):
   *  - `"pass_<wp>"` — keeps objects with WP category ≥ index(<wp>), i.e. objects
   *    that pass <wp> and all lower WPs.
   *  - `"fail_<wp>"` — keeps objects with WP category < index(<wp>), i.e. objects
   *    that fail <wp> (fail all WPs with equal or higher threshold).
   *  - `"pass_<wp1>_fail_<wp2>"` — keeps objects in the WP range [wp1, wp2),
   *    i.e. passing wp1 but failing wp2 (wp1 < wp2 in threshold order).
   *
   * @param selection               Selection expression (see above).
   * @param outputCollectionColumn  Output RDF column name.
   *
   * @throws std::invalid_argument if the selection string is malformed or
   *         references an unknown WP name, or if either argument is empty.
   * @throws std::runtime_error    if setInputObjectCollection() was not called.
   */
  void defineWorkingPointCollection(const std::string &selection,
                                    const std::string &outputCollectionColumn);

  /**
   * @brief Schedule up/down variation collection columns and an optional
   *        PhysicsObjectVariationMap for all registered variations.
   *
   * For each variation registered via addVariation() / applySystematicSet(),
   * the input collection @p nominalCollectionColumn is re-filtered using the
   * same WP selection while ISystematicManager marks the nominal collection as
   * affected.  The variation collections are produced by re-running the WP
   * category computation with the nominal object collection, so the selected objects
   * are the same as the nominal — only the event weight differs.  The nominal
   * collection itself is stored under the key @c "nominal" in the variation map.
   *
   * @param nominalCollectionColumn  The nominal WP-filtered collection column.
   * @param collectionPrefix         Prefix for output variation column names.
   * @param variationMapColumn       If non-empty, name for the output
   *                                 PhysicsObjectVariationMap column.
   *
   * @throws std::runtime_error    if setInputObjectCollection() was not called.
   * @throws std::invalid_argument if @p nominalCollectionColumn or
   *         @p collectionPrefix is empty.
   */
  void defineVariationCollections(const std::string &nominalCollectionColumn,
                                  const std::string &collectionPrefix,
                                  const std::string &variationMapColumn = "");

  // -------------------------------------------------------------------------
  // Fraction histogram calculation utility
  // -------------------------------------------------------------------------

  /**
   * @brief Book per-(pt, η, flavour) tagger-score fraction histograms.
   *
   * This method is intended for a **dedicated pre-processing run** whose sole
   * purpose is to compute generator-level object fractions.  The resulting
   * histograms can be used to build the fraction correctionlib payload consumed
   * by setFractionCorrection().
   *
   * For each combination of (pt-bin, η-bin) a TH1D named
   * `<outputPrefix>_pt<I>_eta<J>[_<flavourLabel>]` is booked in the metadata
   * output ROOT file.  The x-axis spans [0, 1] and records the tagger score
   * distribution.  From these histograms, the fraction of objects in each WP
   * category can be read off after running over the full MC sample.
   *
   * @param outputPrefix   Prefix for all histogram names.
   * @param ptBinEdges     Bin edges for the object-pT dimension (must have ≥ 2
   *                       elements; entries are in GeV).
   * @param etaBinEdges    Bin edges for the object-|η| dimension (must have ≥ 2
   *                       elements).
   * @param flavourColumn  Optional per-object hadron-flavour column (RVec<Int_t>).
   *                       When non-empty, separate histograms are produced for
   *                       flavour labels "b", "c", "light" (hadron flavour
   *                       5, 4, other respectively).  When empty, a single
   *                       inclusive histogram is produced per (pt, η) bin.
   *
   * @throws std::invalid_argument if @p outputPrefix is empty, or if either
   *         bin-edge vector has fewer than 2 elements.
   * @throws std::runtime_error    if setTaggerColumn(s)() or setObjectColumns()
   *         was not called, or if setInputObjectCollection() was not called.
   */
  void defineFractionHistograms(const std::string &outputPrefix,
                                 const std::vector<float> &ptBinEdges,
                                 const std::vector<float> &etaBinEdges,
                                 const std::string &flavourColumn = "");

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Return the first (or only) tagger score column name.
  const std::string &getTaggerColumn() const {
    static const std::string empty;
    return taggerColumns_m.empty() ? empty : taggerColumns_m[0];
  }

  /// Return all tagger score column names.
  const std::vector<std::string> &getTaggerColumns() const { return taggerColumns_m; }

  /// Return the registered working points.
  const std::vector<WorkingPointEntry> &getWorkingPoints() const { return workingPoints_m; }

  /// Return the input object collection column name.
  const std::string &getInputObjectCollectionColumn() const { return inputObjectCollectionColumn_m; }

  /// Return all registered variations.
  const std::vector<TaggingVariationEntry> &getVariations() const { return variations_m; }

  /// Return the per-object WP category column name.
  std::string getWPCategoryColumn() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "TaggerWorkingPointManager"; }

  void setContext(ManagerContext &ctx) override;

  /** No-op: corrections are registered programmatically. */
  void setupFromConfigFile() override {}

  /**
   * @brief Define all WP-category, weight, and filtered-collection columns.
   *
   * Execution order:
   *  1. Per-object WP category column (`<ptColumn>_wp_category`).
   *     For multi-score WPs, an intermediate packed-scores column is created.
   *  2. Each scheduled SF weight column (from applyCorrectionlib / applySystematicSet).
   *  3. Each WP-filtered PhysicsObjectCollection column.
   *  4. Each variation-collection step.
   *  5. Register all systematic variations with ISystematicManager.
   */
  void execute() override;

  /** No-op: no deferred results to retrieve. */
  void finalize() override;

  /**
   * @brief Log a human-readable tagger WP configuration summary.
   */
  void reportMetadata() override;

  /**
   * @brief Contribute structured provenance metadata for this plugin.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  // ---- Context pointers ---------------------------------------------------
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  IConfigurationProvider *configManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;

  // ---- Object kinematic columns -------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- Tagger columns (one for single-score, two+ for multi-score) --------
  std::vector<std::string> taggerColumns_m;

  // ---- Working points (ordered ascending) ---------------------------------
  std::vector<WorkingPointEntry> workingPoints_m;

  // ---- Input object collection --------------------------------------------
  std::string inputObjectCollectionColumn_m;

  // ---- Fraction correction ------------------------------------------------
  std::string fractionCorrectionName_m;
  std::vector<std::string> fractionInputColumns_m;
  bool hasFractionCorrection_m = false;

  // ---- Per-event weight steps ---------------------------------------------
  struct WeightStep {
    std::string perObjectSFColumn;   ///< RVec<Float_t> from correctionlib
    std::string outputWeightColumn; ///< scalar Float_t per-event weight
  };
  std::vector<WeightStep> weightSteps_m;

  // ---- Systematic source sets ---------------------------------------------
  std::unordered_map<std::string, std::vector<std::string>> systematicSets_m;

  // ---- Variation entries --------------------------------------------------
  std::vector<TaggingVariationEntry> variations_m;

  // ---- WP-filtered collection steps ---------------------------------------
  struct WPCollectionStep {
    WPCollectionSelection selection;
    std::string outputCollectionColumn;
  };
  std::vector<WPCollectionStep> wpCollectionSteps_m;

  // ---- Variation collection steps -----------------------------------------
  struct VariationCollectionsStep {
    std::string nominalCollectionColumn;
    std::string collectionPrefix;
    std::string variationMapColumn; ///< may be empty
  };
  std::vector<VariationCollectionsStep> variationCollectionsSteps_m;

  // ---- Fraction histogram booking -----------------------------------------
  struct FractionHistogramConfig {
    std::string outputPrefix;
    std::vector<float> ptBinEdges;
    std::vector<float> etaBinEdges;
    std::string flavourColumn;
  };
  std::vector<FractionHistogramConfig> fractionHistogramConfigs_m;

  // ---- Fraction histogram results (populated in execute, used in finalize) -
  struct FractionHistResult {
    std::string name;
    ROOT::RDF::RResultPtr<TH1D> result;
  };
  std::vector<FractionHistResult> fractionHistResults_m;

  // ---- Internal helpers ---------------------------------------------------

  /// Parse a selection expression into a WPCollectionSelection.
  WPCollectionSelection parseSelection(const std::string &selection) const;

  /// Return the index of a WP by name, or throw if not found.
  std::size_t wpIndex(const std::string &name) const;

  /// Return the column name for per-object WP category.
  std::string wpCategoryColumn() const;

  /// Build a per-event weight column from a per-object SF column.
  /// If fraction correction is configured, divides each per-object SF by the
  /// MC fraction for the object's WP category.
  void defineWeightColumn(const std::string &perObjectSFColumn,
                          const std::string &outputWeightColumn);

  /// Define the per-object WP category column (handles single- and multi-score).
  void defineWPCategoryColumn();
};

#endif // TAGGERWORKINGPOINTMANAGER_H_INCLUDED
