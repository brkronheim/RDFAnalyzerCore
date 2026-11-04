#ifndef CORRECTIONMANAGER_H_INCLUDED
#define CORRECTIONMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <correction.h>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

class Analyzer;

/**
 * @class CorrectionManager
 * @brief Handles loading and applying corrections (correctionlib).
 *
 * This manager encapsulates the logic for managing corrections using correctionlib,
 * including loading from configuration, storing, and applying them to data.
 * Inherits from NamedObjectManager which provides IPluggableManager functionality.
 *
 * Corrections can be registered either from a config file (via the constructor
 * or setupFromConfigFile()) or directly from C++ using registerCorrection().
 * During evaluation, each range/category fallback writes a warning to
 * stderr with the correction name, exception, inputs, and fallback value.
 */
class CorrectionManager
    : public NamedObjectManager<correction::Correction::Ref> {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<CorrectionManager> create(
      Analyzer& an, const std::string& role = "correctionManager");

  /**
   * @brief Construct a new CorrectionManager object
   * @param configProvider Reference to the configuration provider
   */
  CorrectionManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Register a correction directly from C++ (without a config file).
   *
   * This is the programmatic equivalent of a config-file entry.  Use this
   * when you want to set up corrections in C++ code rather than through an
   * external configuration file.
   *
   * @param name       Name to give the correction (used in applyCorrection calls).
   * @param file       Path to the correctionlib JSON file.
   * @param correctionlibName  Name of the correction inside the JSON file.
   * @param inputVariables     Ordered list of RDF column names that supply
   *                           the numeric inputs to the correction.
   *
   * @throws std::runtime_error if the file cannot be opened or the
   *         correction name is not found inside it, or if a correction
   *         with the given @p name is already registered.
   * @throws std::invalid_argument if any string argument passed to
   *         applyCorrection() later contains characters that are unsafe for
   *         ROOT branch names (see validateBranchComponent).
   *
   * @code{.cpp}
   * correctionManager.registerCorrection(
   *     "muon_sf", "muon_sf.json", "NUM_TightID_DEN_genTracks",
   *     {"muon_eta", "muon_pt"});
   * correctionManager.applyCorrection("muon_sf", {"nominal"});
   * // Dataframe column "muon_sf_nominal" is now available.
   * @endcode
   */
  void registerCorrection(const std::string &name,
                          const std::string &file,
                          const std::string &correctionlibName,
                          const std::vector<std::string> &inputVariables);

  /**
   * @brief Apply a scalar correction to the dataframe.
   *
   * The output column is named @p correctionName with each string argument
   * appended (separated by underscores).  For example, calling with
   * @p correctionName = "muon_sf" and @p stringArguments = {"nominal"} creates
   * a column named "muon_sf_nominal".  Calling again with {"syst_up"} creates
   * "muon_sf_syst_up".  This lets the same correction be applied multiple
   * times for different systematic variations.
   *
   * @param correctionName  Name of the correction (must be registered).
   * @param stringArguments String arguments consumed by the correction in the
   *        order string-typed inputs appear in the correctionlib JSON.  Each
   *        argument is also appended to the output column name (joined with
   *        underscores).
   * @param inputColumns    Optional override for the RDF input column names.
   *        When non-empty these are used instead of the columns registered with
   *        the correction, allowing the same registered correction to be applied
   *        to different branches.  When empty the configured columns are used.
   * @param outputBranch    Optional explicit name for the output RDF column.
   *        When non-empty this name is used as-is instead of the auto-derived
   *        name (@p correctionName + "_" + joined @p stringArguments).
   */
  void applyCorrection(const std::string &correctionName,
                       const std::vector<std::string> &stringArguments,
                       const std::vector<std::string> &inputColumns = {},
                       const std::string &outputBranch = "");

  /**
   * @brief Apply a correction over a vector of objects (e.g., all jets in an
   *        event) and store the per-object results as an RVec column.
   *
   * Use this method when the input variables registered for the correction are
   * RVec columns (one entry per object per event) rather than plain scalars.
   * For each event, the correction is evaluated once per object and the results
   * are collected into a @c ROOT::VecOps::RVec<Float_t> output column whose
   * name is @p correctionName with each string argument appended (separated by
   * underscores), e.g. "jet_sf_nominal".
   *
   * The @p stringArguments list works exactly as in @c applyCorrection: string
   * inputs declared in the correctionlib JSON are taken from this list (in
   * order) and the same value is used for every object in the collection.
   * Each argument is also appended to the output column name (joined with
   * underscores) so that the same correction can be applied with different
   * variations without column name collisions.
   *
   * Only scalars broadcast. All vector inputs must have equal lengths;
   * malformed arrays and non-finite numeric inputs/results throw. Every
   * payload range/category fallback reports the correction, ordered input
   * values/names and original error to stderr, while retaining unity.
   *
   * @param correctionName  Name of the correction (must have been loaded via
   *        the configuration or registerCorrection()).
   * @param stringArguments Constant string arguments consumed by the
   *        correction (in the order the string inputs appear in the JSON).
   * @param inputColumns    Optional override for the RDF input column names
   *        (RVec columns, one entry per object per event).  When non-empty
   *        these are used instead of the columns registered with the correction.
   * @param outputBranch    Optional explicit name for the output RDF column.
   *        When non-empty this name is used instead of the auto-derived name.
   *
   * @throws std::runtime_error if the DataManager or SystematicManager have
   *         not been set, if @p correctionName is not registered, or if any
   *         required input column is missing from the dataframe.
   *
   * @code{.cpp}
   * // Suppose "jet_sf" is configured with inputVariables=jet_pt,jet_eta
   * // and both columns are RVec<double> (one entry per jet).
   * correctionManager.applyCorrectionVec("jet_sf", {"nominal"});
   * // The dataframe now contains a column "jet_sf_nominal" of type RVec<Float_t>
   * // with one scale factor per jet.
   * correctionManager.applyCorrectionVec("jet_sf", {"syst_up"});
   * // Creates an additional column "jet_sf_syst_up".
   *
   * // Override input branches at call time (no config change needed):
   * correctionManager.applyCorrectionVec(
   *     "jet_sf", {"nominal"}, {"jet_pt_raw", "jet_eta"}, "jet_sf_raw_nominal");
   * @endcode
   */
  void applyCorrectionVec(const std::string &correctionName,
                          const std::vector<std::string> &stringArguments,
                          const std::vector<std::string> &inputColumns = {},
                          const std::string &outputBranch = "");

  struct VectorCorrectionCase {
    std::string correctionName;
    std::vector<std::string> stringArguments;
  };

  /** Evaluate exactly one case per object, selected before calling correctionlib.
   * selectorColumn is an aligned RVec<int> of zero-based case indices. Numeric
   * inputs share the same ordered columns across cases; scalars broadcast and
   * arrays must align. Invalid selectors/inputs throw. Range/category failures
   * retain the logged unity fallback. Input/selector systematics propagate.
   */
  void applySelectedCorrectionsVec(
      const std::vector<VectorCorrectionCase> &cases,
      const std::string &selectorColumn,
      const std::vector<std::string> &inputColumns,
      const std::string &outputBranch,
      bool propagateSystematics = true);

  /// Selected outputs sharing numeric inputs and a selector use one pack per
  /// input variation. Sharing lasts only for this call, preserving Redefine.
  void applySelectedCorrectionsVecBatch(
      const std::vector<std::vector<VectorCorrectionCase>> &caseSets,
      const std::string &selectorColumn,
      const std::vector<std::string> &inputColumns,
      const std::vector<std::string> &outputBranches,
      const std::vector<bool> &propagateSystematics = {});

  /// Scalar correction gated by a bool column. Inactive events return unity
  /// without calling correctionlib; nominal-only outputs can opt out of shapes.
  void applyConditionalCorrection(
      const std::string &correctionName,
      const std::vector<std::string> &stringArguments,
      const std::vector<std::string> &inputColumns,
      const std::string &conditionColumn,
      const std::string &outputBranch,
      bool propagateSystematics = true);

  /// Gated scalar variants share a numeric pack and consume the bool condition
  /// directly, without allocating selector or result vectors for each output.
  void applyConditionalCorrections(
      const std::string &correctionName,
      const std::vector<std::vector<std::string>> &argumentSets,
      const std::vector<std::string> &inputColumns,
      const std::string &conditionColumn,
      const std::vector<std::string> &outputBranches,
      const std::vector<bool> &propagateSystematics = {});

  /** Fuse numeric input preparation with gated scalar evaluation. The callback
   * runs on a private copy of the ordered numeric inputs, only for active
   * events. It must preserve input count/order and be safe for concurrent RDF
   * slots. The shared raw pack and other outputs are never modified. */
  void applyConditionalCorrections(
      const std::string &correctionName,
      const std::vector<std::vector<std::string>> &argumentSets,
      const std::vector<std::string> &inputColumns,
      const std::string &conditionColumn,
      const std::vector<std::string> &outputBranches,
      const std::vector<bool> &propagateSystematics,
      std::function<void(ROOT::VecOps::RVec<double> &)> prepareInputs);

  /// Apply several string-argument variants with one shared numeric input
  /// pack. Output branches must match argumentSets in length, or be empty to
  /// derive the names. Sharing is scoped to this call, so subsequent Redefine
  /// operations cannot reuse stale inputs. Scalar input systematics propagate
  /// exactly as in applyCorrection; the vector path retains applyCorrectionVec
  /// semantics. An optional boolean per output disables shape propagation for
  /// explicitly varied terminal outputs; the default propagates all outputs.
  void applyCorrections(const std::string &correctionName,
                        const std::vector<std::vector<std::string>> &argumentSets,
                        const std::vector<std::string> &inputColumns = {},
                        const std::vector<std::string> &outputBranches = {},
                        const std::vector<bool> &propagateSystematics = {});
  void applyCorrectionsVec(const std::string &correctionName,
                           const std::vector<std::vector<std::string>> &argumentSets,
                           const std::vector<std::string> &inputColumns = {},
                           const std::vector<std::string> &outputBranches = {},
                           const std::vector<bool> &propagateSystematics = {});

  /**
   * @brief Get a correction object by key
   * @param key Correction key
   * @return Correction reference
   */
  correction::Correction::Ref getCorrection(const std::string &key) const;

  /**
   * @brief Get a compound correction object by key.
   * @param key Correction key
   * @return Compound correction reference
   */
  correction::CompoundCorrection::Ref
  getCompoundCorrection(const std::string &key) const;

  /**
   * @brief Get the features for a correction by key
   * @param key Correction key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &
  getCorrectionFeatures(const std::string &key) const;

  /**
   * @brief Return the type of the manager
   */
  std::string type() const override { return "CorrectionManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs loaded correction names.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports the list of loaded corrections to the logger.
   */
  void reportMetadata() override;

private:
  void applyCorrectionImpl(const std::string &correctionName,
                           const std::vector<std::string> &stringArguments,
                           const std::vector<std::string> &inputColumns,
                           const std::string &outputBranch,
                           const std::string &packedInput,
                           bool propagateSystematics = true);
  void applyCorrectionVecImpl(const std::string &correctionName,
                              const std::vector<std::string> &stringArguments,
                              const std::vector<std::string> &inputColumns,
                              const std::string &outputBranch,
                              const std::string &packedInput,
                           bool propagateSystematics = true);
  void applyCorrectionBatch(bool vector,
                            const std::string &correctionName,
                            const std::vector<std::vector<std::string>> &argumentSets,
                            const std::vector<std::string> &inputColumns,
                            const std::vector<std::string> &outputBranches,
                            const std::vector<bool> &propagateSystematics);
  /**
   * @brief Register corrections from correctionlib using the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerCorrectionlib(const IConfigurationProvider &configProvider);

  std::shared_ptr<const correction::CorrectionSet>
  loadCorrectionSet(const std::string &file);

  std::unordered_map<std::string, correction::CompoundCorrection::Ref> compoundObjects_m;
  std::unordered_map<std::string,
                     std::shared_ptr<const correction::CorrectionSet>>
      correctionSets_m;

  std::size_t batchInputIndex_m = 0;
  bool initialized_m = false;
};



#endif // CORRECTIONMANAGER_H_INCLUDED
