#ifndef TRIGGERMANAGER_H_INCLUDED
#define TRIGGERMANAGER_H_INCLUDED

#include <NamedObjectManager.h>
#include <string>
#include <unordered_map>
#include <vector>

class ConfigurationManager;

/**
 * @brief TriggerManager: Handles loading, storing, and applying trigger groups
 * and vetoes.
 */
class TriggerManager : public NamedObjectManager<std::vector<std::string>> {
public:
  /**
   * @brief Construct a new TriggerManager object
   * @param configManager Reference to the ConfigurationManager
   */
  TriggerManager(const ConfigurationManager &configManager);

  /**
   * @brief Register triggers from configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerTriggers(const ConfigurationManager &configManager);

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

private:
  std::unordered_map<std::string, std::vector<std::string>> vetoes_m;
  std::unordered_map<std::string, std::string> sampleToGroup_m;
};

#endif // TRIGGERMANAGER_H_INCLUDED