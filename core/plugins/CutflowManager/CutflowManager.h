#ifndef CUTFLOWMANAGER_H_INCLUDED
#define CUTFLOWMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>
#include <ROOT/RDataFrame.hxx>
#include <ROOT/RResultPtr.hxx>
#include <RtypesCore.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

class Analyzer;

// Forward declaration to avoid circular includes.
class RegionManager;

/**
 * @class CutflowManager
 * @brief Plugin that computes sequential cutflow and N-1 event count tables.
 *
 * Cuts are registered programmatically via addCut(). Each addCut() call
 * captures the current dataframe state (before the cut filter is applied)
 * and applies the cut as a filter on the main analysis dataframe.
 *
 * **Important**: all boolean columns for *every* cut that will be registered
 * must be defined on the dataframe *before* the first addCut() call.  This
 * ensures the base node (captured at the first addCut) carries every column
 * needed for the N-1 computation.
 *
 * ### Region-aware mode
 *
 * Call bindToRegionManager() after all cuts and regions have been declared.
 * When a RegionManager is bound:
 *  - execute() books per-region count actions in addition to the global ones.
 *    All actions (global + per-region) share the same underlying RDataFrame
 *    computation graph, so a **single** event-loop pass covers everything.
 *  - finalize() writes a 2-D histogram **regions × cuts** to the meta output
 *    file alongside the existing global cutflow histograms.
 *  - Per-region results are accessible via getRegionCutflowCounts(),
 *    getRegionNMinusOneCounts(), and getRegionTotalCount().
 *
 * Typical usage:
 * @code
 *   // 1. Define all boolean cut columns upfront.
 *   analyzer->Define("pass_ptCut",  [](float pt)  { return pt > 30.f; },         {"pt"});
 *   analyzer->Define("pass_etaCut", [](float eta) { return std::abs(eta) < 2.4f; }, {"eta"});
 *
 *   // 2. Register cuts with the manager (also applies each filter).
 *   auto* cfm = analyzer->getPlugin<CutflowManager>("cutflow");
 *   cfm->addCut("ptCut",  "pass_ptCut");
 *   cfm->addCut("etaCut", "pass_etaCut");
 *
 *   // 3. (Optional) bind to a RegionManager for per-region cutflows.
 *   cfm->bindToRegionManager(analyzer->getPlugin<RegionManager>("regions"));
 *
 *   // 4. Run the analysis.
 *   analyzer->run();
 *
 *   // 5. Inspect results (populated after run()).
 *   for (auto& [name, count] : cfm->getCutflowCounts())  { ... }
 *   for (auto& [name, count] : cfm->getNMinusOneCounts()) { ... }
 *   // Per-region:
 *   for (auto& [name, count] : cfm->getRegionCutflowCounts("signal")) { ... }
 * @endcode
 *
 * Results are also written as TH1D histograms ("cutflow" and
 * "cutflow_nminus1") to the meta output ROOT file and logged via the
 * analysis logger.  When regions are bound a TH2D histogram
 * "cutflow_regions" is additionally written.
 */
class CutflowManager : public IPluggableManager {
public:
  CutflowManager() = default;

  /**
   * @brief Register a cut by name and the name of its boolean column.
   *
   * Captures the current dataframe state before applying this cut's filter.
   * All boolean columns for all cuts that will ever be registered must
   * already be defined on the dataframe when this method is first called.
   *
   * @param name       Human-readable label for the cut.
   * @param boolColumn Name of the boolean column representing the cut result.
   */
  void addCut(const std::string &name, const std::string &boolColumn);

  /**
   * @brief Bind this manager to a RegionManager for per-region cutflows.
   *
   * Must be called *before* execute() (i.e. before the event loop starts).
   * The RegionManager must have already had all its regions declared.
   *
   * When bound, execute() books count actions on every region's filtered
   * RDataFrame. Because all those nodes share the same computation graph as
   * the main dataframe, a single event-loop pass covers global and per-region
   * cutflows simultaneously.
   *
   * @param rm  Pointer to the RegionManager.  Passing nullptr is a no-op.
   */
  void bindToRegionManager(RegionManager *rm);

  /**
   * @brief Return the sequential cutflow counts (populated after run()).
   * @return Vector of (cut_label, event_count) in registration order.
   */
  const std::vector<std::pair<std::string, ULong64_t>> &
  getCutflowCounts() const;

  /**
   * @brief Return the N-1 counts (populated after run()).
   * @return Vector of (cut_label, event_count): events passing all cuts
   *         except the named one.
   */
  const std::vector<std::pair<std::string, ULong64_t>> &
  getNMinusOneCounts() const;

  /**
   * @brief Return the total event count before any registered cuts.
   * @return Event count (populated after run()).
   */
  ULong64_t getTotalCount() const { return totalEventCount_m; }

  /**
   * @brief Return per-region sequential cutflow counts (populated after run()).
   *
   * Only available when bindToRegionManager() has been called.
   *
   * @param regionName  Declared region name.
   * @return Vector of (cut_label, event_count) in registration order.
   * @throws std::runtime_error if @p regionName is unknown.
   */
  const std::vector<std::pair<std::string, ULong64_t>> &
  getRegionCutflowCounts(const std::string &regionName) const;

  /**
   * @brief Return per-region N-1 counts (populated after run()).
   *
   * Only available when bindToRegionManager() has been called.
   *
   * @param regionName  Declared region name.
   * @return Vector of (cut_label, event_count).
   * @throws std::runtime_error if @p regionName is unknown.
   */
  const std::vector<std::pair<std::string, ULong64_t>> &
  getRegionNMinusOneCounts(const std::string &regionName) const;

  /**
   * @brief Return total event count before any cuts for the named region.
   *
   * Only available when bindToRegionManager() has been called.
   *
   * @param regionName  Declared region name.
   * @throws std::runtime_error if @p regionName is unknown.
   */
  ULong64_t getRegionTotalCount(const std::string &regionName) const;

  std::string type() const override { return "CutflowManager"; }

  void setContext(ManagerContext &ctx) override;

  /**
   * @brief No-op: cuts are registered programmatically via addCut().
   */
  void setupFromConfigFile() override {}

  /**
   * @brief Validate region bindings; warn about any unknown region names.
   */
  void initialize() override;

  /**
   * @brief Book all lazy count actions on the RDataFrame.
   *
   * Called by the framework immediately before the event loop is triggered.
   * When a RegionManager is bound, per-region count actions are also booked
   * here so that global and per-region results are computed in a single pass.
   */
  void execute() override;

  /**
   * @brief Retrieve count results and write tables to the meta output file.
   *
   * Called by the framework after the event loop completes.
   * When regions are bound, a region×cuts TH2D histogram is also written.
   */
  void finalize() override;

  /**
   * @brief Log the cutflow and N-1 tables to the analysis logger.
   */
  void reportMetadata() override;

  /**
   * @brief Contribute structured provenance metadata for this plugin.
   *
   * Returns:
   *  - "cuts": comma-separated "name:boolColumn" pairs for each registered cut
   *  - "num_cuts": number of registered cuts
   *
   * The Analyzer automatically computes "plugin.<role>.config_hash" from
   * these entries.
   */
  std::unordered_map<std::string, std::string>
  collectProvenanceEntries() const override;

private:
  struct CutEntry {
    std::string name;
    std::string column;
    ROOT::RDF::RNode dfNode; ///< DF state before this cut's filter was applied
  };

  std::vector<CutEntry> cuts_m;

  // Lazy RDataFrame count results (booked in execute(), read in finalize()).
  ROOT::RDF::RResultPtr<ULong64_t> totalCountResult_m;
  std::vector<ROOT::RDF::RResultPtr<ULong64_t>> cutflowCountResults_m;
  std::vector<ROOT::RDF::RResultPtr<ULong64_t>> nMinusOneCountResults_m;

  // Final values populated in finalize().
  ULong64_t totalEventCount_m = 0;
  std::vector<std::pair<std::string, ULong64_t>> cutflowCounts_m;
  std::vector<std::pair<std::string, ULong64_t>> nMinusOneCounts_m;

  // -------------------------------------------------------------------------
  // Region-aware state
  // -------------------------------------------------------------------------

  RegionManager *regionManager_m = nullptr;

  struct RegionCutflowPending {
    ROOT::RDF::RResultPtr<ULong64_t> totalCount;
    std::vector<ROOT::RDF::RResultPtr<ULong64_t>> cutflowCounts;
    std::vector<ROOT::RDF::RResultPtr<ULong64_t>> nMinusOneCounts;
  };

  struct RegionCutflowResult {
    ULong64_t totalCount = 0;
    std::vector<std::pair<std::string, ULong64_t>> cutflowCounts;
    std::vector<std::pair<std::string, ULong64_t>> nMinusOneCounts;
  };

  std::unordered_map<std::string, RegionCutflowPending> regionPending_m;
  std::unordered_map<std::string, RegionCutflowResult>  regionResults_m;

  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<CutflowManager> create(
      Analyzer& an, const std::string& role = "cutflowManager");
};



#endif // CUTFLOWMANAGER_H_INCLUDED
