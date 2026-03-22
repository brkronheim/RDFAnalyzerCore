#ifndef TRIGGERMANAGER_H_INCLUDED
#define TRIGGERMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <NamedObjectManager.h>
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>

class Analyzer;

/**
 * @class TriggerManager
 * @brief Handles loading, storing, and applying trigger groups and vetoes.
 *
 * This manager encapsulates the logic for managing trigger groups and vetoes,
 * including loading from configuration, storing, and providing access to them.
 * Implements the ITriggerManager interface for dependency injection.
 */
class TriggerManager : public NamedObjectManager<std::vector<std::string>> {
public:

  // -------------------------------------------------------------------------
  // Factory: create, register with an Analyzer, and return as shared_ptr.
  // -------------------------------------------------------------------------
  static std::shared_ptr<TriggerManager> create(
      Analyzer& an, const std::string& role = "triggerManager");

  /**
   * @brief Construct a new TriggerManager object
   * @param configProvider Reference to the configuration provider
   */
  TriggerManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Register triggers from configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerTriggers(const IConfigurationProvider &configProvider);

  /**
   * @brief Get the triggers for a given group
   * @param group Name of the trigger group
   * @return Reference to the vector of trigger names
   */
  const std::vector<std::string> &getTriggers(const std::string &group) const;

  /**
   * @brief Get the vetoes for a given group
   * @param group Name of the trigger group
   * @return Reference to the vector of veto names
   */
  const std::vector<std::string> &getVetoes(const std::string &group) const;

  /**
   * @brief Get the group for a given sample
   * @param sample Name of the sample
   * @return Name of the group
   */
  std::string getGroupForSample(const std::string &sample) const;

  /**
   * @brief Get all groups
   * @return Vector of all group names
   */
  std::vector<std::string> getAllGroups() const;

  /**
   * @brief Apply all triggers for the current sample type
   */
  void applyAllTriggers();

  std::string type() const override {
    return "TriggerManager";
  }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs loaded trigger groups.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports trigger groups to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Map from group name to vetoes.
   */
  std::unordered_map<std::string, std::vector<std::string>> vetoes_m;
  /**
   * @brief Map from sample name to group name.
   */
  std::unordered_map<std::string, std::string> sampleToGroup_m;
};



#endif // TRIGGERMANAGER_H_INCLUDED 