#ifndef GOLDENJSONMANAGER_H_INCLUDED
#define GOLDENJSONMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <api/ManagerContext.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

/**
 * @class GoldenJsonManager
 * @brief Filters data events against one or more CMS golden JSON files.
 *
 * This manager loads CMS-format golden JSON files that specify which
 * (run, luminosity section) pairs are certified as good for physics analysis.
 * Multiple files can be specified to cover different eras processed together.
 * When applied, it filters out any event whose (run, luminosityBlock) pair
 * is not present in the valid set.  The filter is only applied when the
 * sample type (config key "type") equals "data".
 *
 * The golden JSON file format is:
 * @code
 * {"355100": [[1, 100], [150, 200]], "355101": [[1, 50]]}
 * @endcode
 *
 * Configuration:
 *   - goldenJsonConfig: path to a text file listing golden JSON file paths,
 *     one per line (comments starting with '#' are ignored).
 */
class GoldenJsonManager : public IPluggableManager {
public:
  /**
   * @brief Default constructor.
   */
  GoldenJsonManager() = default;

  /**
   * @brief Apply the golden JSON filter to the dataframe.
   *
   * Events whose (run, luminosityBlock) pair is not listed in any of the
   * loaded golden JSON files are removed.  The filter is skipped when the
   * sample type is not "data".
   */
  void applyGoldenJson();

  /**
   * @brief Check whether a (run, luminosityBlock) pair is valid.
   * @param run       Run number.
   * @param lumi      Luminosity section number.
   * @return true if the pair falls within a certified range, false otherwise.
   */
  bool isValid(unsigned int run, unsigned int lumi) const;

  /**
   * @brief Return the number of certified run entries loaded.
   * @return Number of runs in the validity map.
   */
  std::size_t numRuns() const { return validLumis_m.size(); }

  std::string type() const override { return "GoldenJsonManager"; }

  void setContext(ManagerContext &ctx) override;

  /**
   * @brief Load golden JSON files listed under the "goldenJsonConfig" key.
   */
  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs the number of loaded run entries.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports the number of certified runs to the logger.
   */
  void reportMetadata() override;

private:
  /// run number -> list of [lumi_start, lumi_end] inclusive ranges
  std::unordered_map<unsigned int,
                     std::vector<std::pair<unsigned int, unsigned int>>>
      validLumis_m;

  IConfigurationProvider *configManager_m = nullptr;
  IDataFrameProvider *dataManager_m = nullptr;
  ISystematicManager *systematicManager_m = nullptr;
  ILogger *logger_m = nullptr;

  /**
   * @brief Parse one golden JSON file and merge its contents into
   *        validLumis_m.
   * @param filename Path to the JSON file.
   */
  void loadJsonFile(const std::string &filename);
};


// ---------------------------------------------------------------------------
// Helper: create, register with analyzer, and return as shared_ptr
// ---------------------------------------------------------------------------
#include <memory>
class Analyzer;
std::shared_ptr<GoldenJsonManager> makeGoldenJsonManager(
    Analyzer& an, const std::string& role = "goldenJsonManager");

#endif // GOLDENJSONMANAGER_H_INCLUDED
