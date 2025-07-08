#ifndef NAMEDOBJECTMANAGER_H_INCLUDED
#define NAMEDOBJECTMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @brief Template base class for managing named objects and their input
 * features.
 * @tparam ObjectType The type of object to manage (e.g., Correction,
 * FastForest)
 */
template <typename ObjectType> class NamedObjectManager : public IPluggableManager {
public:
  /**
   * @brief Get an object by key
   * @param key Object key
   * @return Reference to the object
   */
  const ObjectType &getObject(const std::string &key) const {
    auto it = objects_m.find(key);
    if (it != objects_m.end()) {
      return it->second;
    }
    throw std::runtime_error("Object not found: " + key);
  }

  /**
   * @brief Get the features for an object by key
   * @param key Object key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &getFeatures(const std::string &key) const {
    auto it = features_m.find(key);
    if (it != features_m.end()) {
      return it->second;
    }
    throw std::runtime_error("Features not found: " + key);
  }

  std::string type() const override {
    return "NamedObjectManager";
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

  void setupFromConfigFile() override {
    // Do nothing in base class
  }

protected:
  std::unordered_map<std::string, ObjectType> objects_m;
  std::unordered_map<std::string, std::vector<std::string>> features_m;
  IConfigurationProvider* configManager_m;
  IDataFrameProvider* dataManager_m;
  ISystematicManager* systematicManager_m;
};

#endif // NAMEDOBJECTMANAGER_H_INCLUDED 