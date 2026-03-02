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

/**
 * @class NDHistogramManager
 * @brief Manages booking, storage, and saving of N-dimensional histograms.
 *
 * This manager encapsulates the logic for creating, storing, and writing
 * N-dimensional histograms (THnSparseD) using ROOT's RDataFrame. It is designed
 * to be used by the Analyzer class and centralizes all histogram-related
 * operations. Implements the INDHistogramManager interface for dependency injection.
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
   * @brief Book histograms defined in config file
   * 
   * This method books all histograms that were loaded from the config file
   * during setupFromConfigFile(). It should be called after all filters and
   * defines have been applied to the dataframe, typically right before save().
   */
  void bookConfigHistograms();

private:
  void BookSingleHistogramWithSystList(histInfo &info,
                                       selectionInfo &&sampleCategoryInfo,
                                       selectionInfo &&controlRegionInfo,
                                       selectionInfo &&channelInfo,
                                       std::string suffix,
                                       const std::vector<std::string> &systList);

  /**
   * @brief Structure to hold parsed histogram configuration
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
   * @brief Vector of histogram result pointers.
   */
  std::vector<ROOT::RDF::RNode> histNodes_m;
  std::vector<ROOT::RDF::RResultPtr<THnSparseF>> histos_m;
  std::vector<HistogramConfig> configHistograms_m;
  IConfigurationProvider* configManager_m = nullptr;
  IDataFrameProvider* dataManager_m = nullptr;
  ISystematicManager* systematicManager_m = nullptr;
  ILogger* logger_m = nullptr;
  IOutputSink* skimSink_m = nullptr;
  IOutputSink* metaSink_m = nullptr;
  bool countersFinalized_m = false;
  std::string histogramBackend_m = "root";
};

#endif // NDHISTOGRAMMANAGER_H_INCLUDED 