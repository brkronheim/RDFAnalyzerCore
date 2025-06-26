#ifndef CORRECTIONMANAGER_H_INCLUDED
#define CORRECTIONMANAGER_H_INCLUDED

#include <NamedObjectManager.h>
#include <ROOT/RVec.hxx>
#include <RtypesCore.h>
#include <correction.h>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

class ConfigurationManager;

/**
 * @brief CorrectionManager: Handles loading and applying corrections
 * (correctionlib).
 */
class CorrectionManager
    : public NamedObjectManager<correction::Correction::Ref> {
public:
  /**
   * @brief Construct a new CorrectionManager object
   * @param configManager Reference to the ConfigurationManager
   */
  CorrectionManager(const ConfigurationManager &configManager);

  /**
   * @brief Apply a correction to a set of input features
   * @param correctionName Name of the correction
   * @param stringArguments String arguments for the correction
   * @param inputFeatures Input features for the correction
   * @param defineVector Function to define a vector
   * @param define Function to define the correction
   */
  template <typename DefineVectorFunc, typename DefineFunc>
  void applyCorrection(const std::string &correctionName,
                       const std::vector<std::string> &stringArguments,
                       const std::vector<std::string> &inputFeatures,
                       DefineVectorFunc defineVector, DefineFunc define) {
    defineVector("input_" + correctionName, inputFeatures, "double");
    auto correction = this->objects_m.at(correctionName);
    auto stringArgs = stringArguments;
    auto correctionLambda =
        [correction,
         stringArgs](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
      std::vector<std::variant<int, double, std::string>> values;
      auto stringArgIt = stringArgs.begin();
      auto doubleArgIt = inputVector.begin();
      for (const auto &varType : correction->inputs()) {
        if (varType.typeStr() == "string") {
          values.push_back(*stringArgIt);
          ++stringArgIt;
        } else if (varType.typeStr() == "int") {
          values.push_back(int(*doubleArgIt));
          ++doubleArgIt;
        } else {
          values.push_back(*doubleArgIt);
          ++doubleArgIt;
        }
      }
      return correction->evaluate(values);
    };
    define(correctionName, correctionLambda, {"input_" + correctionName});
  }

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

private:
  /**
   * @brief Register corrections from correctionlib using the configuration
   * @param configManager Reference to the ConfigurationManager
   */
  void registerCorrectionlib(const ConfigurationManager &configManager);
};

#endif // CORRECTIONMANAGER_H_INCLUDED