#ifndef CORRECTIONMANAGER_H_INCLUDED
#define CORRECTIONMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ICorrectionManager.h>
#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <correction.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

/**
 * @class CorrectionManager
 * @brief Handles loading and applying corrections (correctionlib).
 *
 * This manager encapsulates the logic for managing corrections using correctionlib,
 * including loading from configuration, storing, and applying them to data.
 * Implements the ICorrectionManager interface for dependency injection.
 */
class CorrectionManager
    : public NamedObjectManager<correction::Correction::Ref>,
      public ICorrectionManager {
public:
  /**
   * @brief Construct a new CorrectionManager object
   * @param configProvider Reference to the configuration provider
   */
  CorrectionManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply a correction to the given dataframe provider
   * @param dataFrameProvider Reference to the dataframe provider
   * @param correctionName Name of the correction
   * @param stringArguments String arguments
   */
  void applyCorrection(IDataFrameProvider& dataFrameProvider,
                      const std::string &correctionName,
                      const std::vector<std::string> &stringArguments,
                      ISystematicManager &systematicManager);

  /**
   * @brief Get a correction object by key
   * @param key Correction key
   * @return Correction reference
   */
  correction::Correction::Ref getCorrection(const std::string &key) const override;

  /**
   * @brief Get the features for a correction by key
   * @param key Correction key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &
  getCorrectionFeatures(const std::string &key) const override;

private:
  /**
   * @brief Register corrections from correctionlib using the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerCorrectionlib(const IConfigurationProvider &configProvider);
};

#endif // CORRECTIONMANAGER_H_INCLUDED