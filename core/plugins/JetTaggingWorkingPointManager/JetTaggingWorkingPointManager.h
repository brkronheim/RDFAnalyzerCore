#ifndef JETTAGGINGWORKINGPOINTMANAGER_H_INCLUDED
#define JETTAGGINGWORKINGPOINTMANAGER_H_INCLUDED

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
 * Working points are ordered by threshold (ascending), allowing selection
 * categories such as "pass medium but fail tight".
 */
struct WorkingPointEntry {
  std::string name;    ///< Human-readable label (e.g. "loose", "medium", "tight")
  float threshold;     ///< Discriminator score threshold (score >= threshold = pass)
};

/**
 * @brief Record of a per-jet SF column pair for a tagger systematic variation.
 *
 * Populated by addVariation() and used in execute() for systematic registration
 * and variation-collection definition.
 */
struct TaggingVariationEntry {
  std::string name;            ///< Systematic name (e.g. "btagHF")
  std::string upSFColumn;      ///< Per-jet SF column for the up shift   (RVec<Float_t>)
  std::string downSFColumn;    ///< Per-jet SF column for the down shift (RVec<Float_t>)
  std::string upWeightColumn;  ///< Per-event weight column for the up shift   (Float_t)
  std::string downWeightColumn;///< Per-event weight column for the down shift (Float_t)
};

// ---------------------------------------------------------------------------
// WP selection category
// ---------------------------------------------------------------------------

/**
 * @brief Describes a working-point-based selection for filtered collections.
 *
 * The selection determines which jets are kept in a PhysicsObjectCollection:
 *
 * | Type            | Meaning                                          |
 * |-----------------|--------------------------------------------------|
 * | PassWP          | jet passes at least one named WP                 |
 * | FailWP          | jet fails a named WP (below its threshold)       |
 * | PassRangeWP     | jet passes lower WP but fails upper WP           |
 *
 * @see JetTaggingWorkingPointManager::defineWorkingPointCollection
 */
struct WPCollectionSelection {
  enum class Type { PassWP, FailWP, PassRangeWP };
  Type type;
  std::string wpNameLower;  ///< Name of the lower (or single) WP
  std::string wpNameUpper;  ///< Name of the upper WP (only for PassRangeWP)
};

// ---------------------------------------------------------------------------
// JetTaggingWorkingPointManager
// ---------------------------------------------------------------------------

/**
 * @class JetTaggingWorkingPointManager
 * @brief Plugin for applying working-point-based tagger corrections and
 *        systematics to Jet and FatJet physics collections.
 *
 * ## Overview
 *
 * This plugin handles CMS-style working-point (WP) tagger scale factors —
 * e.g. for b-tagging algorithms such as DeepJet, ParticleNet, RobustParTAK4
 * — with full support for:
 *  - **Working-point definitions**: declare any number of named WPs with
 *    score thresholds (e.g. loose/medium/tight).
 *  - **WP category column**: per-jet integer encoding which WP range a jet
 *    falls into (0 = fail all, 1 = pass lowest, …, N = pass all).
 *  - **Correctionlib scale factors**: apply per-jet SFs from a correctionlib
 *    payload to produce per-event weight columns compatible with WeightManager.
 *  - **Generator-level jet fraction correction**: optionally accept a second
 *    correctionlib payload encoding the MC fraction of jets in each WP
 *    category at given (pt, η).  These fractions reweight the per-jet SF
 *    contributions to properly reflect the generator-level jet distributions
 *    when making WP-based selections.
 *  - **Systematic source sets**: bulk application of named uncertainty sources
 *    via applySystematicSet(), matching the JetEnergyScaleManager API.
 *  - **WP-filtered PhysicsObjectCollections**: produce collections of jets
 *    passing specific WP criteria (pass_<wp>, fail_<wp>,
 *    pass_<wp1>_fail_<wp2>).
 *  - **Variation collections and map**: per-variation up/down jet collections
 *    and a PhysicsObjectVariationMap for downstream systematic propagation.
 *  - **Fraction histogram utility**: book per-(pt, η, flavour) tagger-score
 *    histograms in a dedicated pre-processing run so the fraction payload can
 *    be constructed from data.
 *
 * ## Typical usage
 * @code
 *   auto* jtm = analyzer->getPlugin<JetTaggingWorkingPointManager>("btagManager");
 *   auto* cm  = analyzer->getPlugin<CorrectionManager>("corrections");
 *
 *   // 1. Declare jet kinematic column names.
 *   jtm->setJetColumns("Jet_pt", "Jet_eta", "Jet_phi", "Jet_mass");
 *
 *   // 2. Declare the tagger discriminator score column.
 *   jtm->setTaggerColumn("Jet_btagDeepFlavB");
 *
 *   // 3. Register working points in order of increasing threshold.
 *   jtm->addWorkingPoint("loose",  0.0521f);
 *   jtm->addWorkingPoint("medium", 0.3033f);
 *   jtm->addWorkingPoint("tight",  0.7489f);
 *
 *   // 4. Set the input jet PhysicsObjectCollection column.
 *   jtm->setInputJetCollection("goodJets");
 *
 *   // 5. Apply a correctionlib payload that returns per-jet SFs.
 *   //    The correctionlib inputs typically include: systematic type,
 *   //    jet flavour, eta, pt.  String arguments (like "central") are
 *   //    passed via stringArgs; numeric columns via inputColumns.
 *   jtm->applyCorrectionlib(*cm, "deepjet_sf", {"central"},
 *                           {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
 *                            "Jet_btagDeepFlavB"});
 *   // Defines per-event weight column "deepjet_sf_central_weight".
 *
 *   // 6. (Optional) Set a fraction correctionlib for generator-level
 *   //    distribution reweighting.
 *   jtm->setFractionCorrection(*cm, "deepjet_fractions",
 *                              {"Jet_pt", "Jet_eta", "Jet_pt_wp_category"});
 *
 *   // 7. Register systematic source sets and apply them in one call.
 *   jtm->registerSystematicSources("standard",
 *                                   {"hf", "lf", "hfstats1", "hfstats2",
 *                                    "lfstats1", "lfstats2", "cferr1", "cferr2"});
 *   jtm->applySystematicSet(*cm, "deepjet_sf", "standard",
 *                           {"Jet_hadronFlavour", "Jet_eta", "Jet_pt",
 *                            "Jet_btagDeepFlavB"});
 *
 *   // 8. Define WP-filtered jet collections.
 *   jtm->defineWorkingPointCollection("pass_medium",      "goodJets_bmedium");
 *   jtm->defineWorkingPointCollection("fail_loose",       "goodJets_bfail");
 *   jtm->defineWorkingPointCollection("pass_tight_fail_medium", "goodJets_btight");
 *
 *   // 9. Build systematic variation collections + variation map.
 *   jtm->defineVariationCollections("goodJets_bmedium", "goodJets_btag",
 *                                    "goodJets_btag_variations");
 *
 *   // 10. (Pre-processing only) book fraction histograms.
 *   //     Run with a dedicated minimal analysis to collect these first.
 *   // jtm->defineFractionHistograms("btag_frac",
 *   //                               {20,30,50,70,100,200,500},
 *   //                               {-2.4f,-1.5f,0.f,1.5f,2.4f},
 *   //                               "Jet_hadronFlavour");
 * @endcode
 *
 * ## Working-point categories
 *
 * With WPs [loose=0.05, medium=0.3, tight=0.75] the per-jet category column
 * `Jet_wp_category` is:
 * | category | meaning                                   |
 * |----------|-------------------------------------------|
 * |    0     | score < 0.05  (fail all WPs)              |
 * |    1     | 0.05 ≤ score < 0.3  (pass loose, fail medium) |
 * |    2     | 0.3  ≤ score < 0.75 (pass medium, fail tight) |
 * |    3     | score ≥ 0.75 (pass all WPs / pass tight)  |
 *
 * Selection expressions for defineWorkingPointCollection():
 * | string                     | jets kept                         |
 * |----------------------------|-----------------------------------|
 * | `"pass_loose"`             | category ≥ 1                      |
 * | `"pass_medium"`            | category ≥ 2                      |
 * | `"fail_loose"`             | category = 0                      |
 * | `"pass_medium_fail_tight"` | category = 2 (pass M, fail T)     |
 * | `"pass_loose_fail_medium"` | category = 1 (pass L, fail M)     |
 *
 * ## Generator-level fraction reweighting
 *
 * When setFractionCorrection() is called the per-event weight becomes
 * a fraction-weighted sum rather than a product of per-jet SFs.  For each
 * jet @c j, the contribution is:
 * @code
 *   weight_j = SF(pt_j, η_j, …) / mc_fraction(pt_j, η_j, category_j)
 * @endcode
 * This ensures that the selected jet distribution matches the generator-level
 * expectation when SFs differ across WP categories.
 *
 * @see docs/JET_TAGGING_WORKING_POINTS.md for the complete reference.
 * @see JetEnergyScaleManager for JES/JER corrections.
 * @see WeightManager for applying the resulting weight columns.
 */
class JetTaggingWorkingPointManager : public IPluggableManager {
public:

  // -------------------------------------------------------------------------
  // Factory
  // -------------------------------------------------------------------------

  /**
   * @brief Create a JetTaggingWorkingPointManager, register it with @p an
   *        under @p role, and return the owning shared_ptr.
   */
  static std::shared_ptr<JetTaggingWorkingPointManager> create(
      Analyzer &an, const std::string &role = "btagManager");

  // -------------------------------------------------------------------------
  // Jet column configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the jet kinematic column names.
   *
   * @param ptColumn   Per-jet pT column (RVec<Float_t>).
   * @param etaColumn  Per-jet η column.
   * @param phiColumn  Per-jet φ column.
   * @param massColumn Per-jet mass column (may be empty to skip mass handling).
   *
   * @throws std::invalid_argument if @p ptColumn is empty.
   */
  void setJetColumns(const std::string &ptColumn,
                     const std::string &etaColumn,
                     const std::string &phiColumn,
                     const std::string &massColumn);

  // -------------------------------------------------------------------------
  // Tagger discriminator configuration
  // -------------------------------------------------------------------------

  /**
   * @brief Set the per-jet tagger discriminator score column.
   *
   * The column must be of type RVec<Float_t> with one entry per jet.
   * It is used in execute() to build the per-jet WP category column
   * (`<ptColumn>_wp_category`).
   *
   * @param taggerScoreColumn  RDF column name.
   * @throws std::invalid_argument if empty.
   */
  void setTaggerColumn(const std::string &taggerScoreColumn);

  // -------------------------------------------------------------------------
  // Working-point registration
  // -------------------------------------------------------------------------

  /**
   * @brief Register a named working point with its score threshold.
   *
   * Working points must be added in order of *increasing* threshold so that
   * the category encoding is well-defined:
   * @code
   *   jtm->addWorkingPoint("loose",  0.0521f);  // threshold 0
   *   jtm->addWorkingPoint("medium", 0.3033f);  // threshold 1
   *   jtm->addWorkingPoint("tight",  0.7489f);  // threshold 2
   * @endcode
   *
   * @param name       Human-readable WP label (must be unique, non-empty).
   * @param threshold  Score threshold; jets with score ≥ threshold pass.
   *
   * @throws std::invalid_argument if @p name is empty, already registered,
   *         or if the threshold is not strictly greater than the previously
   *         registered WP threshold.
   */
  void addWorkingPoint(const std::string &name, float threshold);

  // -------------------------------------------------------------------------
  // Input collection
  // -------------------------------------------------------------------------

  /**
   * @brief Declare the RDF column holding the input jet PhysicsObjectCollection.
   *
   * This enables the WP-filtered collection API (defineWorkingPointCollection,
   * defineVariationCollections).  The named column must be present in the
   * dataframe before execute() is called.
   *
   * @param collectionColumn  RDF column name of type PhysicsObjectCollection.
   * @throws std::invalid_argument if empty.
   */
  void setInputJetCollection(const std::string &collectionColumn);

  // -------------------------------------------------------------------------
  // Scale-factor application
  // -------------------------------------------------------------------------

  /**
   * @brief Apply a correctionlib-based per-jet SF and produce a per-event
   *        weight column.
   *
   * The correctionlib payload is evaluated once per jet per event via
   * CorrectionManager::applyCorrectionVec().  The resulting per-jet
   * RVec<Float_t> SF column is then reduced to a per-event scalar weight
   * (product of all per-jet SFs for jets in the input collection) stored in
   * the column `<correctionName>_<stringArgs...>_weight`.
   *
   * If setFractionCorrection() has been called, each per-jet contribution
   * is divided by the MC fraction for that jet's WP category, normalising
   * the weight to the generator-level distribution.
   *
   * @param cm               CorrectionManager holding the registered correction.
   * @param correctionName   Name of the correction in @p cm.
   * @param stringArgs       Constant string arguments passed to the correctionlib
   *                         evaluator (e.g. @c {"central"}).  Each argument is
   *                         also appended to the output weight column name.
   * @param inputColumns     Ordered list of numeric input columns for the
   *                         correctionlib evaluator (RVec columns for per-jet
   *                         quantities).  When empty, uses the columns
   *                         registered with the correction.
   *
   * @throws std::invalid_argument if @p correctionName is empty.
   * @throws std::runtime_error    if setInputJetCollection() was not called.
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
   * The fraction payload evaluates to the MC fraction of jets at a given
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
   *                              columns, one entry per jet per event).  Must
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
   * The @p upSFColumn and @p downSFColumn must be per-jet RVec<Float_t>
   * columns already defined in the dataframe (e.g. via applyCorrectionlib
   * or applySystematicSet).
   *
   * @param systematicName  Base name (e.g. @c "btagHF").
   * @param upSFColumn      Per-jet SF column for the up variation.
   * @param downSFColumn    Per-jet SF column for the down variation.
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
   * PhysicsObjectCollection containing only jets that satisfy @p selection.
   *
   * Supported selection strings (names must match those passed to addWorkingPoint):
   *  - `"pass_<wp>"` — keeps jets with WP category ≥ index(<wp>), i.e. jets
   *    that pass <wp> and all lower WPs.
   *  - `"fail_<wp>"` — keeps jets with WP category < index(<wp>), i.e. jets
   *    that fail <wp> (fail all WPs with equal or higher threshold).
   *  - `"pass_<wp1>_fail_<wp2>"` — keeps jets in the WP range [wp1, wp2),
   *    i.e. passing wp1 but failing wp2 (wp1 < wp2 in threshold order).
   *
   * @param selection               Selection expression (see above).
   * @param outputCollectionColumn  Output RDF column name.
   *
   * @throws std::invalid_argument if the selection string is malformed or
   *         references an unknown WP name, or if either argument is empty.
   * @throws std::runtime_error    if setInputJetCollection() was not called.
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
   * category computation with the nominal jet collection, so the selected jets
   * are the same as the nominal — only the event weight differs.  The nominal
   * collection itself is stored under the key @c "nominal" in the variation map.
   *
   * @param nominalCollectionColumn  The nominal WP-filtered collection column.
   * @param collectionPrefix         Prefix for output variation column names.
   * @param variationMapColumn       If non-empty, name for the output
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
  // Fraction histogram calculation utility
  // -------------------------------------------------------------------------

  /**
   * @brief Book per-(pt, η, flavour) tagger-score fraction histograms.
   *
   * This method is intended for a **dedicated pre-processing run** whose sole
   * purpose is to compute generator-level jet fractions.  The resulting
   * histograms can be used to build the fraction correctionlib payload consumed
   * by setFractionCorrection().
   *
   * For each combination of (pt-bin, η-bin) a TH1D named
   * `<outputPrefix>_pt<I>_eta<J>[_<flavourLabel>]` is booked in the metadata
   * output ROOT file.  The x-axis spans [0, 1] and records the tagger score
   * distribution.  From these histograms, the fraction of jets in each WP
   * category can be read off after running over the full MC sample.
   *
   * @param outputPrefix   Prefix for all histogram names.
   * @param ptBinEdges     Bin edges for the jet-pT dimension (must have ≥ 2
   *                       elements; entries are in GeV).
   * @param etaBinEdges    Bin edges for the jet-|η| dimension (must have ≥ 2
   *                       elements).
   * @param flavourColumn  Optional per-jet hadron-flavour column (RVec<Int_t>).
   *                       When non-empty, separate histograms are produced for
   *                       flavour labels "b", "c", "light" (hadron flavour
   *                       5, 4, other respectively).  When empty, a single
   *                       inclusive histogram is produced per (pt, η) bin.
   *
   * @throws std::invalid_argument if @p outputPrefix is empty, or if either
   *         bin-edge vector has fewer than 2 elements.
   * @throws std::runtime_error    if setTaggerColumn() or setJetColumns() was
   *         not called, or if setInputJetCollection() was not called.
   */
  void defineFractionHistograms(const std::string &outputPrefix,
                                 const std::vector<float> &ptBinEdges,
                                 const std::vector<float> &etaBinEdges,
                                 const std::string &flavourColumn = "");

  // -------------------------------------------------------------------------
  // Accessors
  // -------------------------------------------------------------------------

  /// Return the tagger score column name.
  const std::string &getTaggerColumn() const { return taggerColumn_m; }

  /// Return the registered working points.
  const std::vector<WorkingPointEntry> &getWorkingPoints() const { return workingPoints_m; }

  /// Return the input jet collection column name.
  const std::string &getInputJetCollectionColumn() const { return inputJetCollectionColumn_m; }

  /// Return all registered variations.
  const std::vector<TaggingVariationEntry> &getVariations() const { return variations_m; }

  /// Return the per-jet WP category column name.
  std::string getWPCategoryColumn() const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "JetTaggingWorkingPointManager"; }

  void setContext(ManagerContext &ctx) override;

  /** No-op: corrections are registered programmatically. */
  void setupFromConfigFile() override {}

  /**
   * @brief Define all WP-category, weight, and filtered-collection columns.
   *
   * Execution order:
   *  1. Per-jet WP category column (`<ptColumn>_wp_category`).
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

  // ---- Jet kinematic columns ----------------------------------------------
  std::string ptColumn_m;
  std::string etaColumn_m;
  std::string phiColumn_m;
  std::string massColumn_m;

  // ---- Tagger column ------------------------------------------------------
  std::string taggerColumn_m;

  // ---- Working points (ordered ascending by threshold) --------------------
  std::vector<WorkingPointEntry> workingPoints_m;

  // ---- Input jet collection -----------------------------------------------
  std::string inputJetCollectionColumn_m;

  // ---- Fraction correction ------------------------------------------------
  std::string fractionCorrectionName_m;
  std::vector<std::string> fractionInputColumns_m;
  bool hasFractionCorrection_m = false;

  // ---- Per-event weight steps ---------------------------------------------
  struct WeightStep {
    std::string perJetSFColumn;   ///< RVec<Float_t> from correctionlib
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

  /// Return the column name for per-jet WP category.
  std::string wpCategoryColumn() const;

  /// Build a per-event weight column from a per-jet SF column.
  /// If fraction correction is configured, divides each per-jet SF by the
  /// MC fraction for the jet's WP category.
  void defineWeightColumn(const std::string &perJetSFColumn,
                          const std::string &outputWeightColumn);
};

#endif // JETTAGGINGWORKINGPOINTMANAGER_H_INCLUDED
