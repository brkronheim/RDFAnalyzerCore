#ifndef NAMEDOBJECTMANAGER_H_INCLUDED
#define NAMEDOBJECTMANAGER_H_INCLUDED

#include <api/IPluggableManager.h>
#include <api/ILogger.h>
#include <api/ManagerContext.h>
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

  void setContext(ManagerContext& ctx) override {
    configManager_m = &ctx.config;
    dataManager_m = &ctx.data;
    systematicManager_m = &ctx.systematics;
    logger_m = &ctx.logger;
  }

  void setupFromConfigFile() override {
    // Do nothing in base class
  }

protected:
  std::unordered_map<std::string, ObjectType> objects_m;
  std::unordered_map<std::string, std::vector<std::string>> features_m;
  IConfigurationProvider* configManager_m = nullptr;
  IDataFrameProvider* dataManager_m = nullptr;
  ISystematicManager* systematicManager_m = nullptr;
  ILogger* logger_m = nullptr;
};

#endif // NAMEDOBJECTMANAGER_H_INCLUDED 