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
#include <utility>
#include <vector>

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
 *   // 3. Run the analysis.
 *   analyzer->run();
 *
 *   // 4. Inspect results (populated after run()).
 *   for (auto& [name, count] : cfm->getCutflowCounts())  { ... }
 *   for (auto& [name, count] : cfm->getNMinusOneCounts()) { ... }
 * @endcode
 *
 * Results are also written as TH1D histograms ("cutflow" and
 * "cutflow_nminus1") to the meta output ROOT file and logged via the
 * analysis logger.
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

  std::string type() const override { return "CutflowManager"; }

  void setContext(ManagerContext &ctx) override;

  /**
   * @brief No-op: cuts are registered programmatically via addCut().
   */
  void setupFromConfigFile() override {}

  /**
   * @brief Book all lazy count actions on the RDataFrame.
   *
   * Called by the framework immediately before the event loop is triggered.
   */
  void execute() override;

  /**
   * @brief Retrieve count results and write tables to the meta output file.
   *
   * Called by the framework after the event loop completes.
   */
  void finalize() override;

  /**
   * @brief Log the cutflow and N-1 tables to the analysis logger.
   */
  void reportMetadata() override;

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

  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;
};

#endif // CUTFLOWMANAGER_H_INCLUDED
