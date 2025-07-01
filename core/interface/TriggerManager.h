#ifndef TRIGGERMANAGER_H_INCLUDED
#define TRIGGERMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/ITriggerManager.h>
#include <NamedObjectManager.h>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class TriggerManager
 * @brief Handles loading, storing, and applying trigger groups and vetoes.
 *
 * This manager encapsulates the logic for managing trigger groups and vetoes,
 * including loading from configuration, storing, and providing access to them.
 * Implements the ITriggerManager interface for dependency injection.
 */
class TriggerManager : public NamedObjectManager<std::vector<std::string>>,
                      public ITriggerManager {
public:
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
  const std::vector<std::string> &getTriggers(const std::string &group) const override;

  /**
   * @brief Get the vetoes for a given group
   * @param group Name of the trigger group
   * @return Reference to the vector of veto names
   */
  const std::vector<std::string> &getVetoes(const std::string &group) const override;

  /**
   * @brief Get the group for a given sample
   * @param sample Name of the sample
   * @return Name of the group
   */
  std::string getGroupForSample(const std::string &sample) const override;

  /**
   * @brief Get all groups
   * @return Vector of all group names
   */
  std::vector<std::string> getAllGroups() const override;

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