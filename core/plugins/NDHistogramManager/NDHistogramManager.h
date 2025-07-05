#ifndef NDHISTOGRAMMANAGER_H_INCLUDED
#define NDHISTOGRAMMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/IPluggableManager.h>
#include <plots.h>
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
   * @brief Book N-dimensional histograms
   * @param infos Vector of histogram info objects
   * @param selection Vector of selection info objects
   * @param suffix Suffix to append to histogram names
   * @param allRegionNames Vector of region name vectors
   */
  void BookND(std::vector<histInfo> &infos,
              std::vector<selectionInfo> &selection, const std::string &suffix,
              std::vector<std::vector<std::string>> &allRegionNames,
              IDataFrameProvider &dataFrameProvider,
              ISystematicManager &systematicManager);

  /**
   * @brief Save histograms to file
   * @param fullHistList Vector of vectors of histogram info objects
   * @param allRegionNames Vector of vectors of region name vectors
   */
  void SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                 std::vector<std::vector<std::string>> &allRegionNames,
                 const IConfigurationProvider &configProvider);

  /**
   * @brief Get the vector of histogram result pointers
   * @return Reference to the vector of RResultPtr<THnSparseD>
   */
  std::vector<ROOT::RDF::RResultPtr<THnSparseD>> &GetHistos();

  /**
   * @brief Clear all stored histograms
   */
  void Clear();

  std::string type() const override {
    return "NDHistogramManager";
  }

  void setConfigManager(IConfigurationProvider* configManager) override {
    configManager_m = configManager;
  }
  void setDataManager(IDataFrameProvider* dataManager) override {
    dataManager_m = dataManager;
  }
  void setSystematicManager(ISystematicManager* systematicManager) override {
    systematicManager_m = systematicManager;
  }

private:
  /**
   * @brief Vector of histogram result pointers.
   */
  std::vector<ROOT::RDF::RResultPtr<THnSparseD>> histos_m;
  IConfigurationProvider* configManager_m;
  IDataFrameProvider* dataManager_m;
  ISystematicManager* systematicManager_m;
};

#endif // NDHISTOGRAMMANAGER_H_INCLUDED 