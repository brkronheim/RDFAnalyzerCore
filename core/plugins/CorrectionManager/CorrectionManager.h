#ifndef CORRECTIONMANAGER_H_INCLUDED
#define CORRECTIONMANAGER_H_INCLUDED

#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
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
 * Inherits from NamedObjectManager which provides IPluggableManager functionality.
 */
class CorrectionManager
    : public NamedObjectManager<correction::Correction::Ref> {
public:
  /**
   * @brief Construct a new CorrectionManager object
   * @param configProvider Reference to the configuration provider
   */
  CorrectionManager(const IConfigurationProvider &configProvider);

  /**
   * @brief Apply a correction to the dataframe provider
   * @param correctionName Name of the correction
   * @param stringArguments String arguments
   */
  void applyCorrection(const std::string &correctionName,
                      const std::vector<std::string> &stringArguments);

  /**
   * @brief Apply a correction over a vector of objects (e.g., all jets in an
   *        event) and store the per-object results as an RVec column.
   *
   * Use this method when the input variables registered for the correction are
   * RVec columns (one entry per object per event) rather than plain scalars.
   * For each event, the correction is evaluated once per object and the results
   * are collected into a @c ROOT::VecOps::RVec<Float_t> output column whose
   * name equals @p correctionName.
   *
   * The @p stringArguments list works exactly as in @c applyCorrection: string
   * inputs declared in the correctionlib JSON are taken from this list (in
   * order) and the same value is used for every object in the collection.
   *
   * @param correctionName Name of the correction (must have been loaded via
   *        the configuration)
   * @param stringArguments Constant string arguments consumed by the
   *        correction (in the order the string inputs appear in the JSON)
   *
   * @throws std::runtime_error if the DataManager or SystematicManager have
   *         not been set, if @p correctionName is not registered, or if any
   *         required input column is missing from the dataframe.
   *
   * @code{.cpp}
   * // Suppose "jet_sf" is configured with inputVariables=jet_pt,jet_eta
   * // and both columns are RVec<double> (one entry per jet).
   * correctionManager.applyCorrectionVec("jet_sf", {"nominal"});
   * // The dataframe now contains a column "jet_sf" of type RVec<Float_t>
   * // with one scale factor per jet.
   * @endcode
   */
  void applyCorrectionVec(const std::string &correctionName,
                          const std::vector<std::string> &stringArguments);

  /**
   * @brief Get a correction object by key
   * @param key Correction key
   * @return Correction reference
   */
  correction::Correction::Ref getCorrection(const std::string &key) const;

  /**
   * @brief Get the features for a correction by key
   * @param key Correction key
   * @return Reference to the vector of feature names
   */
  const std::vector<std::string> &
  getCorrectionFeatures(const std::string &key) const;

  /**
   * @brief Return the type of the manager
   */
  std::string type() const override { return "CorrectionManager"; }

  void setupFromConfigFile() override;

  /**
   * @brief Post-wiring initialization: logs loaded correction names.
   */
  void initialize() override;

  /**
   * @brief Metadata hook: reports the list of loaded corrections to the logger.
   */
  void reportMetadata() override;

private:
  /**
   * @brief Register corrections from correctionlib using the configuration
   * @param configProvider Reference to the configuration provider
   */
  void registerCorrectionlib(const IConfigurationProvider &configProvider);

  bool initialized_m = false;
};

#endif // CORRECTIONMANAGER_H_INCLUDED 