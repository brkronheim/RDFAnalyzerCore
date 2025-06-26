#ifndef NDHISTOGRAMMANAGER_H_INCLUDED
#define NDHISTOGRAMMANAGER_H_INCLUDED

#include <plots.h>
#include <string>
#include <vector>

class DataManager;
class ConfigurationManager;

/**
 * @class NDHistogramManager
 * @brief Manages booking, storage, and saving of N-dimensional histograms.
 *
 * This manager encapsulates the logic for creating, storing, and writing
 * N-dimensional histograms (THnSparseD) using ROOT's RDataFrame. It is designed
 * to be used by the Analyzer class and centralizes all histogram-related
 * operations.
 */
class NDHistogramManager {
public:
  /**
   * @brief Construct a new NDHistogramManager object
   * @param dataManager Reference to the DataManager
   * @param configManager Reference to the ConfigurationManager
   */
  NDHistogramManager(DataManager &dataManager,
                     ConfigurationManager &configManager);

  /**
   * @brief Book N-dimensional histograms
   * @param infos Vector of histogram info objects
   * @param selection Vector of selection info objects
   * @param suffix Suffix to append to histogram names
   * @param allRegionNames Vector of region name vectors
   */
  void BookND(std::vector<histInfo> &infos,
              std::vector<selectionInfo> &selection, const std::string &suffix,
              std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Save histograms to file
   * @param fullHistList Vector of vectors of histogram info objects
   * @param allRegionNames Vector of vectors of region name vectors
   */
  void SaveHists(std::vector<std::vector<histInfo>> &fullHistList,
                 std::vector<std::vector<std::string>> &allRegionNames);

  /**
   * @brief Get the vector of histogram result pointers
   * @return Reference to the vector of RResultPtr<THnSparseD>
   */
  std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseD>> &GetHistos();

  /**
   * @brief Clear all stored histograms
   */
  void Clear();

private:
  DataManager &dataManager_m;
  ConfigurationManager &configManager_m;
  std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseD>> histos_m;
};

#endif // NDHISTOGRAMMANAGER_H_INCLUDED