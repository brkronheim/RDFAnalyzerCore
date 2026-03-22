#ifndef REGIONMANAGER_H_INCLUDED
#define REGIONMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <api/ManagerContext.h>
#include <ROOT/RDataFrame.hxx>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class RegionManager
 * @brief Plugin that manages named analysis regions with optional hierarchy.
 *
 * Regions are declared programmatically via declareRegion(). Each region
 * has a unique name, a boolean filter column that selects events belonging to
 * that region, and an optional parent region name.
 *
 * When a region has a parent, its associated RDataFrame node is built by
 * starting from the parent's filtered node and applying this region's own
 * filter on top. This allows arbitrarily deep region hierarchies where child
 * regions are strict subsets of their parents.
 *
 * Regions without a parent are applied to the analysis dataframe as it exists
 * at the time declareRegion() is first called. All root regions therefore
 * share the same base node (capturing any global preselection applied before
 * the first declareRegion() call).
 *
 * The main analysis dataframe is **not modified** by RegionManager. It builds
 * its own filtered branches independently, which other plugins (histogram and
 * cutflow managers) can retrieve via getRegionDataFrame().
 *
 * Typical usage:
 * @code
 *   // 1. Define boolean columns for all regions upfront.
 *   analyzer->Define("pass_presel",  [](float pt){ return pt > 20.f; }, {"pt"});
 *   analyzer->Define("pass_signal",  [](float mva){ return mva > 0.8f; }, {"mva"});
 *   analyzer->Define("pass_control", [](float mva){ return mva < 0.4f; }, {"mva"});
 *
 *   // 2. Declare regions (parent before child).
 *   auto* rm = analyzer->getPlugin<RegionManager>("regions");
 *   rm->declareRegion("presel",  "pass_presel");
 *   rm->declareRegion("signal",  "pass_signal",  "presel");
 *   rm->declareRegion("control", "pass_control", "presel");
 *
 *   // 3. Retrieve per-region DataFrames for histogramming.
 *   ROOT::RDF::RNode signalDf  = rm->getRegionDataFrame("signal");
 *   ROOT::RDF::RNode controlDf = rm->getRegionDataFrame("control");
 * @endcode
 *
 * initialize() validates the region hierarchy (no cycles, no missing parents,
 * no duplicate names) and throws std::runtime_error on failure. finalize()
 * writes a region-summary TNamed to the meta output ROOT file.
 */
class RegionManager : public IPluggableManager {
public:
  RegionManager() = default;

  /**
   * @brief Declare a named region.
   *
   * All boolean columns used by any region must already be defined on the
   * dataframe when this method is first called, because the base node is
   * captured at that point.
   *
   * @param name         Unique region name (must not already be registered).
   * @param filterColumn Name of the boolean column that selects events in
   *                     this region.
   * @param parent       Name of an already-declared parent region, or empty
   *                     string for a root region (no parent).
   *
   * @throws std::runtime_error if context has not been set, if @p name is
   *         already declared, or if @p parent is specified but has not yet
   *         been declared.
   */
  void declareRegion(const std::string &name,
                     const std::string &filterColumn,
                     const std::string &parent = "");

  /**
   * @brief Return the filtered RDataFrame node for a declared region.
   *
   * The returned node has all filters from the full ancestor chain applied
   * (root to this region inclusive).
   *
   * @param name  Name of the declared region.
   * @return The filtered RDataFrame node for that region.
   * @throws std::runtime_error if @p name has not been declared.
   */
  ROOT::RDF::RNode getRegionDataFrame(const std::string &name) const;

  /**
   * @brief Return the names of all declared regions in declaration order.
   */
  const std::vector<std::string> &getRegionNames() const;

  /**
   * @brief Validate the region hierarchy without throwing.
   *
   * Checks for: duplicate names, missing parent references, and cycles.
   *
   * @return List of validation error strings. An empty list means the
   *         hierarchy is valid.
   */
  std::vector<std::string> validate() const;

  /**
   * @brief Return the ordered chain of boolean filter columns from the root
   *        region down to (and including) the named region.
   *
   * For a root region "presel" with filterColumn "pass_presel" the chain is
   * @c {"pass_presel"}.  For a child region "signal" whose parent is "presel"
   * the chain is @c {"pass_presel", "pass_signal"}.
   *
   * The chain can be used to reconstruct the full membership condition for a
   * region without holding a reference to a filtered RDataFrame node.
   *
   * @param name  Name of the declared region.
   * @return Ordered list of filter column names (root → this region).
   * @throws std::runtime_error if @p name has not been declared.
   */
  std::vector<std::string> getFilterChain(const std::string &name) const;

  // -------------------------------------------------------------------------
  // IPluggableManager interface
  // -------------------------------------------------------------------------

  std::string type() const override { return "RegionManager"; }

  void setContext(ManagerContext &ctx) override;

  /**
   * @brief No-op: regions are registered programmatically via declareRegion().
   */
  void setupFromConfigFile() override {}

  /**
   * @brief Validate the region hierarchy; throw on error.
   *
   * Called after all plugins have been wired. This is the earliest point at
   * which all dependencies are guaranteed to be registered, but note that
   * initialize() is called before any declareRegion() invocations in user
   * code. For this reason, re-validation is also performed in finalize().
   */
  void initialize() override;

  /**
   * @brief No-op for the event loop; regions are fully built at declaration.
   */
  void execute() override {}

  /**
   * @brief Validate regions and write a summary to the meta output ROOT file.
   */
  void finalize() override;

  /**
   * @brief Log the region hierarchy to the analysis logger.
   */
  void reportMetadata() override;

private:
  struct RegionEntry {
    std::string name;
    std::string filterColumn;
    std::string parent;             ///< empty = root region
    ROOT::RDF::RNode dfNode;        ///< DF filtered by this region's full chain
  };

  // Declaration-order list of names (stable for iteration).
  std::vector<std::string> regionOrder_m;
  // Map from name to entry (for O(1) lookup).
  std::unordered_map<std::string, RegionEntry> regions_m;

  // Base DF captured on the first declareRegion() call.
  // All root regions are filtered from this node so they share the same
  // baseline (any global preselection applied before the first region
  // declaration is automatically included).
  bool baseCaptured_m = false;
  ROOT::RDF::RNode baseDf_m{ROOT::RDataFrame(0)};

  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ILogger *logger_m = nullptr;
  IOutputSink *metaSink_m = nullptr;
};


// ---------------------------------------------------------------------------
// Helper: create, register with analyzer, and return as shared_ptr
// ---------------------------------------------------------------------------
#include <memory>
class Analyzer;
std::shared_ptr<RegionManager> makeRegionManager(
    Analyzer& an, const std::string& role = "regionManager");

#endif // REGIONMANAGER_H_INCLUDED
