#ifndef NDHISTOGRAMMANAGER_H_INCLUDED
#define NDHISTOGRAMMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/ManagerContext.h>
#include <plots.h>
#include <ROOT/RDataFrame.hxx>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <string>
#include <vector>

class Analyzer;

// Forward declaration to avoid circular includes.
class RegionManager;

/**
 * @class NDHistogramManager
 * @brief Manages booking, storage, and saving of N-dimensional histograms.
 *
 * This manager encapsulates the logic for creating, storing, and writing
 * N-dimensional histograms (THnSparseD) using ROOT's RDataFrame. It is designed
 * to be used by the Analyzer class and centralizes all histogram-related
 * operations. Implements the INDHistogramManager interface for dependency injection.
 *
 * ### Region-aware mode
 *
 * Call bindToRegionManager() with a configured RegionManager before calling
 * bookConfigHistograms().  When a RegionManager is bound:
 *
 *  - A `__rm_region_membership__` multi-fill column is defined on the
 *    dataframe.  For each event, it contains the 1-indexed float index of
 *    every declared region the event belongs to (empty entries are 0.0 and
 *    fall to the underflow bin, which is not included in the output).
 *  - Each config histogram is booked **once** as a single, large
 *    multi-dimensional THnSparse that has an additional *region axis*.
 *    The region axis has @em N bins (one per declared region), so the total
 *    number of stored histogram objects does not grow with the region count.
 *    This satisfies the single-execution / large-multi-dim requirement.
 *  - All histogram bookings share the same underlying RDataFrame computation
 *    graph, so the event loop is executed **exactly once**.
 *
 * @note Only scalar (non-collection) base variables are supported in the
 *       region-aware booking path.  For per-object (RVec) variables use the
 *       standard bookND() interface.
 */
class NDHistogramManager : public IPluggableManager {
public:
  /**
   * @brief Construct a new NDHistogramManager object
   */
  NDHistogramManager(const IConfigurationProvider& configProvider);

  /**
   * @brief Virtual destructor for proper cleanup
   */
  virtual ~NDHistogramManager() = default;

  /**
   * @brief Book a single histogram
   * @param info Histogram info object
   * @param sampleCategoryInfo Selection info object for sample category
   * @param controlRegionInfo Selection info object for control region
   * @param channelInfo Selection info object for channel
   * @param suffix Suffix to append to histogram names
   */
   void BookSingleHistogram(histInfo &info, selectionInfo &&sampleCategoryInfo=selectionInfo(),
     selectionInfo &&controlRegionInfo=selectionInfo(), 
     selectionInfo &&channelInfo=selectionInfo(),
      std::string suffix=""); 
    
  /**
   * @brief Save histograms to file
   * @param fullHistList Vector of vectors of histogram info objects
   * @param allRegionNames Vector of vectors of region name vectors
   */
  void SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                 std::vector<std::vector<std::string>> &allRegionNames,
                 const IConfigurationProvider &configProvider);

  /**
   * @brief Book N-dimensional histograms (wrapper for analyzer compatibility)
   * @param infos Vector of histogram info objects
   * @param selection Vector of selection info objects
   * @param suffix Suffix to append to histogram names
   * @param allRegionNames Vector of region name vectors
   */
  void bookND(std::vector<histInfo> &infos,
              std::vector<selectionInfo> &selection,
              const std::string &suffix,
              std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Save histograms to file (wrapper for analyzer compatibility)
   * @param fullHistList Vector of vectors of histogram info objects
   * @param allRegionNames Vector of vectors of region name vectors
   */
  void saveHists(std::vector<std::vector<histInfo>> &fullHistList,
                 std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Save all histograms booked via bookND without supplying lists again.
   *
   * Histogram metadata and region names are accumulated internally each time
   * bookND() is called. This overload replays that stored information so the
   * caller does not need to manage separate tracking vectors.
   */
  void saveHists();

  /**
   * @brief Get the vector of histogram result pointers
   * @return Reference to the vector of RResultPtr<THnSparseD>
   */
  std::vector<ROOT::RDF::RResultPtr<THnSparseF>> &GetHistos();

  /**
   * @brief Clear all stored histograms
   */
  void Clear();

  std::string type() const override {
    return "NDHistogramManager";
  }

  void setContext(ManagerContext& ctx) override {
    configManager_m = &ctx.config;
    dataManager_m = &ctx.data;
    systematicManager_m = &ctx.systematics;
    logger_m = &ctx.logger;
    skimSink_m = &ctx.skimSink;
    metaSink_m = &ctx.metaSink;
  }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs the number of config histograms.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports booked histogram counts to the logger.
   */
  void reportMetadata() override;

  /**
   * @brief Book histograms defined in config file
   * 
   * This method books all histograms that were loaded from the config file
   * during setupFromConfigFile(). It should be called after all filters and
   * defines have been applied to the dataframe, typically right before save().
   *
   * When a RegionManager has been bound via bindToRegionManager(), each
   * configured histogram is booked with an additional *region axis* encoded
   * as the channel dimension of the THnSparse.  A single histogram object
   * covers all declared regions, keeping the output compact and the event
   * loop to a single execution.
   */
  void bookConfigHistograms();

  /**
   * @brief Bind this manager to a RegionManager for automatic region-aware
   *        histogram booking.
   *
   * Must be called *before* bookConfigHistograms().  The RegionManager must
   * have already had all its regions declared.
   *
   * When bound, bookConfigHistograms() defines a multi-fill region membership
   * column on the dataframe and books each config histogram with the region
   * as an extra axis (a single large THnSparse per config entry instead of
   * N separate histograms).  Because all bookings are lazy and share the same
   * computation graph, the event loop runs **exactly once**.
   *
   * @param rm  Pointer to the RegionManager.  Passing nullptr is a no-op.
   */
  void bindToRegionManager(RegionManager *rm);

  /**
   * @brief Structure to hold parsed histogram configuration.
   *
   * Exposed as a public struct so that tests (and user code) can inspect the
   * loaded configuration via getConfigHistograms().
   */
  struct HistogramConfig {
    std::string name;
    std::string variable;
    std::string label;
    std::string weight;
    int bins;
    float lowerBound;
    float upperBound;
    std::string suffix;
    std::string channelVariable;
    int channelBins;
    float channelLowerBound;
    float channelUpperBound;
    std::vector<std::string> channelRegions;
    std::string controlRegionVariable;
    int controlRegionBins;
    float controlRegionLowerBound;
    float controlRegionUpperBound;
    std::vector<std::string> controlRegionRegions;
    std::string sampleCategoryVariable;
    int sampleCategoryBins;
    float sampleCategoryLowerBound;
    float sampleCategoryUpperBound;
    std::vector<std::string> sampleCategoryRegions;
  };

  /**
   * @brief Get the tracked histogram infos (for testing).
   * @return Const reference to the tracked histogram info batches.
   */
  const std::vector<std::vector<histInfo>>& GetTrackedHistInfos() const {
    return trackedHistInfos_m;
  }

  /**
   * @brief Get the parsed config histogram definitions (for testing).
   * @return Const reference to the config histogram vector.
   */
  const std::vector<HistogramConfig>& getConfigHistograms() const {
    return configHistograms_m;
  }

private:
  void BookSingleHistogramWithSystList(histInfo &info,
                                       selectionInfo &&sampleCategoryInfo,
                                       selectionInfo &&controlRegionInfo,
                                       selectionInfo &&channelInfo,
                                       std::string suffix,
                                       const std::vector<std::string> &systList,
                                       const std::string &baseRefVector = "");

  /**
   * @brief Automatically detect and register systematic variations from the
   *        current dataframe columns before the first histogram booking.
   *
   * Scans all available dataframe columns for `baseVar_systUp` /
   * `baseVar_systDown` pairs and registers them with the SystematicManager.
   * Incomplete pairs (one direction missing) are reported via the logger.
   *
   * This method is a no-op if the canonical SystematicCounter branch has
   * already been materialised (i.e. makeSystList() was already called),
   * because the cached syst list cannot be updated after the fact.
   */
  void ensureSystematicsAutoRegistered();

  /**
   * @brief Ensure the region membership column is defined on the dataframe.
   *
   * Defines `__rm_region_membership__` as an RVec<Float_t> whose elements are
   * the 1-indexed float IDs of all regions the event belongs to.  Values of
   * 0.0 (event not in that region) are mapped to the underflow bin and are
   * therefore not counted in the histogram output.
   *
   * @return The column name of the membership vector.
   */
  std::string ensureRegionMembershipColumn();

  /**
   * @brief Vector of histogram result pointers.
   */
  std::vector<ROOT::RDF::RNode> histNodes_m;
  std::vector<ROOT::RDF::RResultPtr<THnSparseF>> histos_m;
  std::vector<HistogramConfig> configHistograms_m;
  // Accumulated metadata from each bookND() call — used by the no-args saveHists()
  std::vector<std::vector<histInfo>> trackedHistInfos_m;
  std::vector<std::vector<std::string>> trackedRegionNames_m;
  IConfigurationProvider* configManager_m = nullptr;
  IDataFrameProvider* dataManager_m = nullptr;
  ISystematicManager* systematicManager_m = nullptr;
  ILogger* logger_m = nullptr;
  IOutputSink* skimSink_m = nullptr;
  IOutputSink* metaSink_m = nullptr;
  bool countersFinalized_m = false;
  std::string histogramBackend_m = "root";
  RegionManager* regionManager_m = nullptr;

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<NDHistogramManager> create(
      Analyzer& an, const std::string& role = "histogramManager");
};



#endif // NDHISTOGRAMMANAGER_H_INCLUDED 