#ifndef NAMEDOBJECTMANAGER_H_INCLUDED
#define NAMEDOBJECTMANAGER_H_INCLUDED

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
template <typename ObjectType> class NamedObjectManager {
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

protected:
  std::unordered_map<std::string, ObjectType> objects_m;
  std::unordered_map<std::string, std::vector<std::string>> features_m;
};

#endif // NAMEDOBJECTMANAGER_H_INCLUDED