#include "CorrectedObjectCollectionManagers.h"

#include <PhysicsObjectCollection.h>
#include <analyzer.h>

#include <ROOT/RVec.hxx>

#include <algorithm>
#include <cctype>
#include <cstdlib>
#include <sstream>
#include <stdexcept>

namespace {

using ActionMap = std::unordered_map<std::string, std::string>;

bool inferIsMC(IConfigurationProvider &configManager);

void defineRelativeUncertaintyScaleFactors(IDataFrameProvider &dataManager,
                                           ISystematicManager &systematicManager,
                                           const std::string &inputColumn,
                                           const std::string &upColumn,
                                           const std::string &downColumn);

bool isTruthy(const std::string &value) {
  return value == "1" || value == "true" || value == "True" ||
         value == "yes" || value == "Yes";
}

bool hasValue(const std::string &value) {
  return !value.empty();
}

bool hasAnyValue(std::initializer_list<std::string> values) {
  for (const auto &value : values) {
    if (!value.empty()) {
      return true;
    }
  }
  return false;
}

std::string toLower(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
  return value;
}

std::string requireValue(const std::unordered_map<std::string, std::string> &config,
                         const std::string &key,
                         const std::string &typeName,
                         const std::string &configFile) {
  const auto it = config.find(key);
  if (it == config.end() || it->second.empty()) {
    throw std::runtime_error(typeName + ": missing required key '" + key +
                             "' in config file '" + configFile + "'");
  }
  return it->second;
}

std::string getValue(const std::unordered_map<std::string, std::string> &config,
                     const std::string &key,
                     const std::string &defaultValue = "") {
  const auto it = config.find(key);
  if (it == config.end()) {
    return defaultValue;
  }
  return it->second;
}

std::string resolvePlaceholders(const std::string &input,
                                IConfigurationProvider &configManager,
                                const ActionMap &localValues,
                                const std::string &owner) {
  std::string output;
  output.reserve(input.size());

  std::size_t cursor = 0;
  while (cursor < input.size()) {
    const std::size_t start = input.find("${", cursor);
    if (start == std::string::npos) {
      output.append(input.substr(cursor));
      break;
    }

    output.append(input.substr(cursor, start - cursor));
    const std::size_t end = input.find('}', start + 2);
    if (end == std::string::npos) {
      throw std::runtime_error(owner + ": unterminated placeholder in value '" +
                               input + "'");
    }

    const std::string key = input.substr(start + 2, end - start - 2);
    auto localIt = localValues.find(key);
    if (localIt != localValues.end()) {
      output.append(localIt->second);
    } else {
      const std::string configValue = configManager.get(key);
      if (configValue.empty()) {
        throw std::runtime_error(owner + ": unresolved placeholder '${" + key +
                                 "}' in value '" + input + "'");
      }
      output.append(configValue);
    }
    cursor = end + 1;
  }

  return output;
}

std::string getResolvedValue(const ActionMap &action,
                             const std::string &key,
                             IConfigurationProvider &configManager,
                             const ActionMap &localValues,
                             const std::string &owner,
                             const std::string &defaultValue = "") {
  const auto it = action.find(key);
  if (it == action.end()) {
    return defaultValue;
  }
  return resolvePlaceholders(it->second, configManager, localValues, owner);
}

std::string requireResolvedValue(const ActionMap &action,
                                 const std::string &key,
                                 IConfigurationProvider &configManager,
                                 const ActionMap &localValues,
                                 const std::string &owner) {
  const auto it = action.find(key);
  if (it == action.end() || it->second.empty()) {
    throw std::runtime_error(owner + ": missing required action key '" + key + "'");
  }
  return resolvePlaceholders(it->second, configManager, localValues, owner);
}

std::string getResolvedAliasedValue(const ActionMap &action,
                                    const std::vector<std::string> &keys,
                                    IConfigurationProvider &configManager,
                                    const ActionMap &localValues,
                                    const std::string &owner,
                                    const std::string &defaultValue = "") {
  for (const auto &key : keys) {
    const auto it = action.find(key);
    if (it != action.end() && !it->second.empty()) {
      return resolvePlaceholders(it->second, configManager, localValues, owner);
    }
  }
  return defaultValue;
}

std::string requireResolvedAliasedValue(const ActionMap &action,
                                        const std::vector<std::string> &keys,
                                        IConfigurationProvider &configManager,
                                        const ActionMap &localValues,
                                        const std::string &owner) {
  const std::string value =
      getResolvedAliasedValue(action, keys, configManager, localValues, owner);
  if (value.empty()) {
    std::ostringstream ss;
    for (std::size_t index = 0; index < keys.size(); ++index) {
      if (index != 0) {
        ss << ", ";
      }
      ss << "'" << keys[index] << "'";
    }
    throw std::runtime_error(owner + ": missing required action key one of [" +
                             ss.str() + "]");
  }
  return value;
}

std::vector<std::string> getResolvedList(const ActionMap &action,
                                         const std::string &key,
                                         IConfigurationProvider &configManager,
                                         const ActionMap &localValues,
                                         const std::string &owner) {
  const std::string rawValue = getResolvedValue(action, key, configManager, localValues,
                                                owner);
  if (rawValue.empty()) {
    return {};
  }
  return configManager.splitString(rawValue, ",");
}

bool getResolvedBool(const ActionMap &action,
                     const std::string &key,
                     IConfigurationProvider &configManager,
                     const ActionMap &localValues,
                     const std::string &owner,
                     bool defaultValue) {
  const auto it = action.find(key);
  if (it == action.end()) {
    return defaultValue;
  }
  return isTruthy(resolvePlaceholders(it->second, configManager, localValues, owner));
}

float getResolvedFloat(const ActionMap &action,
                       const std::string &key,
                       IConfigurationProvider &configManager,
                       const ActionMap &localValues,
                       const std::string &owner,
                       float defaultValue = 0.0f) {
  const auto it = action.find(key);
  if (it == action.end() || it->second.empty()) {
    return defaultValue;
  }
  return std::stof(resolvePlaceholders(it->second, configManager, localValues, owner));
}

int getResolvedInt(const ActionMap &action,
                   const std::string &key,
                   IConfigurationProvider &configManager,
                   const ActionMap &localValues,
                   const std::string &owner,
                   int defaultValue = 0) {
  const auto it = action.find(key);
  if (it == action.end() || it->second.empty()) {
    return defaultValue;
  }
  return std::stoi(resolvePlaceholders(it->second, configManager, localValues, owner));
}

std::string getResolvedAliasedOrDefault(const ActionMap &action,
                                        const std::vector<std::string> &keys,
                                        IConfigurationProvider &configManager,
                                        const ActionMap &localValues,
                                        const std::string &owner,
                                        const std::string &defaultValue = "") {
  return getResolvedAliasedValue(action, keys, configManager, localValues, owner,
                                 defaultValue);
}

std::string appendDirectionalSuffix(const std::string &prefix,
                                    const std::string &direction) {
  if (prefix.empty()) {
    return "";
  }
  return prefix + "_" + direction;
}

std::string composeIndexedKey(const std::string &prefix,
                              int index,
                              const std::string &suffix) {
  std::ostringstream ss;
  ss << prefix << index << suffix;
  return ss.str();
}

std::string getConfigValue(IConfigurationProvider &configManager,
                           const std::string &key) {
  return configManager.get(key);
}

bool getConfigBool(IConfigurationProvider &configManager,
                   const std::string &key,
                   bool defaultValue = false) {
  const std::string value = configManager.get(key);
  if (value.empty()) {
    return defaultValue;
  }
  return isTruthy(value);
}










bool inferIsMC(IConfigurationProvider &configManager) {
  const std::string primaryDataset = toLower(configManager.get("primaryDataset"));
  if (primaryDataset == "mc") {
    return true;
  }

  const std::string sampleType = toLower(configManager.get("type"));
  if (sampleType == "mc" || sampleType.rfind("mc_", 0) == 0) {
    return true;
  }
  if (sampleType == "data") {
    return false;
  }

  return false;
}

bool actionAppliesToSample(const ActionMap &action,
                           IConfigurationProvider &configManager,
                           const ActionMap &localValues,
                           const std::string &owner,
                           bool isMC) {
  std::string selector = getResolvedValue(action, "sample", configManager,
                                          localValues, owner);
  if (selector.empty()) {
    selector = getResolvedValue(action, "when", configManager, localValues,
                                owner, "all");
  }
  selector = toLower(selector);
  if (selector.empty() || selector == "all" || selector == "always" ||
      selector == "any") {
    return true;
  }
  if (selector == "mc") {
    return isMC;
  }
  if (selector == "data") {
    return !isMC;
  }
  throw std::runtime_error(owner + ": unsupported sample selector '" + selector + "'");
}

void defineRelativeUncertaintyScaleFactors(IDataFrameProvider &dataManager,
                                           ISystematicManager &systematicManager,
                                           const std::string &inputColumn,
                                           const std::string &upColumn,
                                           const std::string &downColumn) {
  dataManager.Define(
      upColumn,
      [](const ROOT::VecOps::RVec<Float_t> &uncertainties)
          -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> factors(uncertainties.size(), 1.0f);
        for (std::size_t index = 0; index < uncertainties.size(); ++index) {
          factors[index] += uncertainties[index];
        }
        return factors;
      },
      {inputColumn}, systematicManager);
  dataManager.Define(
      downColumn,
      [](const ROOT::VecOps::RVec<Float_t> &uncertainties)
          -> ROOT::VecOps::RVec<Float_t> {
        ROOT::VecOps::RVec<Float_t> factors(uncertainties.size(), 1.0f);
        for (std::size_t index = 0; index < uncertainties.size(); ++index) {
          factors[index] -= uncertainties[index];
        }
        return factors;
      },
      {inputColumn}, systematicManager);
}

bool tryApplyCommonWorkflowAction(const ActionMap &action,
                                  const ActionMap &localValues,
                                  IConfigurationProvider &configManager,
                                  IDataFrameProvider &dataManager,
                                  ISystematicManager &systematicManager,
                                  CorrectionManager *correctionManager,
                                  const std::string &owner) {
  const std::string type = toLower(
      requireResolvedValue(action, "type", configManager, localValues, owner));

  if (type == "registercorrection") {
    if (!correctionManager) {
      throw std::runtime_error(owner + ": registerCorrection action requires CorrectionManager");
    }
    correctionManager->registerCorrection(
        requireResolvedValue(action, "name", configManager, localValues, owner),
        requireResolvedValue(action, "file", configManager, localValues, owner),
        requireResolvedValue(action, "correctionName", configManager, localValues, owner),
        getResolvedList(action, "inputVariables", configManager, localValues, owner));
    return true;
  }

  if (type == "applycorrectionvec") {
    if (!correctionManager) {
      throw std::runtime_error(owner + ": applyCorrectionVec action requires CorrectionManager");
    }
    const std::string correctionName = requireResolvedAliasedValue(
        action, {"correction", "correctionName"}, configManager, localValues, owner);
    std::string outputColumn = getResolvedAliasedValue(
        action, {"outputColumn", "outputBranch"}, configManager, localValues, owner);
    correctionManager->applyCorrectionVec(
        correctionName,
        getResolvedList(action, "stringArgs", configManager, localValues, owner),
        getResolvedList(action, "inputColumns", configManager, localValues, owner),
        outputColumn);
    return true;
  }

  if (type == "definerelativeuncertaintyscalefactors") {
    defineRelativeUncertaintyScaleFactors(
        dataManager, systematicManager,
        requireResolvedValue(action, "inputColumn", configManager, localValues, owner),
        requireResolvedValue(action, "upColumn", configManager, localValues, owner),
        requireResolvedValue(action, "downColumn", configManager, localValues, owner));
    return true;
  }

  return false;
}

template <typename ManagerT>
void applyObjectWorkflowAction(ManagerT &manager,
                               const ActionMap &action,
                               const ActionMap &localValues,
                               IConfigurationProvider &configManager,
                               const std::string &owner) {
  const std::string type = toLower(
      requireResolvedValue(action, "type", configManager, localValues, owner));

  if (type == "setobjectcolumns") {
    manager.setObjectColumns(
        requireResolvedValue(action, "ptColumn", configManager, localValues, owner),
        requireResolvedValue(action, "etaColumn", configManager, localValues, owner),
        requireResolvedValue(action, "phiColumn", configManager, localValues, owner),
        getResolvedValue(action, "massColumn", configManager, localValues, owner));
    return;
  }
  if (type == "setmetcolumns") {
    manager.setMETColumns(
        requireResolvedValue(action, "metPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "metPhiColumn", configManager, localValues, owner));
    return;
  }
  if (type == "defineproduciblegaussian" || type == "definereproduciblegaussian") {
    manager.defineReproducibleGaussian(
        requireResolvedValue(action, "outputColumn", configManager, localValues, owner),
        requireResolvedValue(action, "sizeColumn", configManager, localValues, owner),
        requireResolvedValue(action, "runColumn", configManager, localValues, owner),
        requireResolvedValue(action, "lumiColumn", configManager, localValues, owner),
        requireResolvedValue(action, "eventColumn", configManager, localValues, owner),
        getResolvedValue(action, "salt", configManager, localValues, owner));
    return;
  }
  if (type == "applycorrection") {
    manager.applyCorrection(
        requireResolvedValue(action, "inputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "sfColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager, localValues, owner),
        getResolvedBool(action, "applyToMass", configManager, localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager, localValues, owner));
    return;
  }
  if (type == "applycorrectionlib") {
    throw std::runtime_error(owner + ": applyCorrectionlib requires manager-specific handling");
  }
  if (type == "applyresolutionsmearing") {
    manager.applyResolutionSmearing(
        requireResolvedValue(action, "inputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "sigmaColumn", configManager, localValues, owner),
        requireResolvedValue(action, "randomColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager, localValues, owner));
    return;
  }
  if (type == "addvariation") {
    manager.addVariation(
        requireResolvedValue(action, "systematicName", configManager, localValues, owner),
        requireResolvedValue(action, "upPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "downPtColumn", configManager, localValues, owner),
        getResolvedValue(action, "upMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "downMassColumn", configManager, localValues, owner));
    return;
  }
  if (type == "propagatemet") {
    manager.propagateMET(
        requireResolvedValue(action, "baseMETPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "baseMETPhiColumn", configManager, localValues, owner),
        requireResolvedValue(action, "nominalPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "variedPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputMETPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputMETPhiColumn", configManager, localValues, owner),
        getResolvedFloat(action, "ptThreshold", configManager, localValues, owner, 0.0f));
    return;
  }
  if (type == "registersystematicsources") {
    manager.registerSystematicSources(
        requireResolvedValue(action, "setName", configManager, localValues, owner),
        getResolvedList(action, "sources", configManager, localValues, owner));
    return;
  }

  throw std::runtime_error(owner + ": unsupported workflow action '" + type + "'");
}

void applyJetWorkflowAction(JetEnergyScaleManager &manager,
                            CorrectionManager *correctionManager,
                            IConfigurationProvider &configManager,
                            const ActionMap &action,
                            const ActionMap &localValues,
                            const std::string &owner) {
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager, localValues, owner));

  if (actionType == "setjetcolumns") {
    manager.setJetColumns(
        requireResolvedValue(action, "ptColumn", configManager, localValues, owner),
        requireResolvedValue(action, "etaColumn", configManager, localValues, owner),
        requireResolvedValue(action, "phiColumn", configManager, localValues, owner),
        getResolvedValue(action, "massColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "setmetcolumns") {
    manager.setMETColumns(
        requireResolvedValue(action, "metPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "metPhiColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "removeexistingcorrections") {
    manager.removeExistingCorrections(
        requireResolvedValue(action, "rawFactorColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "setrawptcolumn") {
    manager.setRawPtColumn(
        requireResolvedValue(action, "rawPtColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "setjersmearingcolumns") {
    manager.setJERSmearingColumns(
        requireResolvedValue(action, "genJetPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "rhoColumn", configManager, localValues, owner),
        requireResolvedValue(action, "eventColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "applycorrection") {
    manager.applyCorrection(
        requireResolvedValue(action, "inputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "sfColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager, localValues, owner),
        getResolvedBool(action, "applyToMass", configManager, localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "applycorrectionlib") {
    if (!correctionManager) {
      throw std::runtime_error(owner + ": applyCorrectionlib action requires CorrectionManager");
    }
    const std::string defaultInputPtColumn =
        getValue(localValues, "rawPtColumn", getValue(localValues, "ptColumn"));
    manager.applyCorrectionlib(
        *correctionManager,
        requireResolvedAliasedValue(action, {"correction", "correctionName"}, configManager,
                                    localValues, owner),
        getResolvedList(action, "stringArgs", configManager, localValues, owner),
        getResolvedValue(action, "inputPtColumn", configManager, localValues, owner,
                         defaultInputPtColumn),
        getResolvedValue(action, "outputPtColumn", configManager, localValues, owner,
                         getValue(localValues, "correctedPtColumn")),
        getResolvedBool(action, "applyToMass", configManager, localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager, localValues, owner),
        getResolvedList(action, "inputColumns", configManager, localValues, owner));
    return;
  }
  if (actionType == "applyjersmearing") {
    if (!correctionManager) {
      throw std::runtime_error(owner + ": applyJERSmearing action requires CorrectionManager");
    }
    manager.applyJERSmearing(
        *correctionManager,
        requireResolvedValue(action, "ptResolutionCorrection", configManager, localValues, owner),
        requireResolvedValue(action, "scaleFactorCorrection", configManager, localValues, owner),
        requireResolvedValue(action, "inputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "systematic", configManager, localValues, owner),
        getResolvedBool(action, "applyToMass", configManager, localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager, localValues, owner),
        getResolvedList(action, "ptResolutionInputs", configManager, localValues, owner),
        getResolvedList(action, "scaleFactorInputs", configManager, localValues, owner));
    return;
  }
  if (actionType == "addvariation") {
    manager.addVariation(
        requireResolvedValue(action, "systematicName", configManager, localValues, owner),
        requireResolvedValue(action, "upPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "downPtColumn", configManager, localValues, owner),
        getResolvedValue(action, "upMassColumn", configManager, localValues, owner),
        getResolvedValue(action, "downMassColumn", configManager, localValues, owner));
    return;
  }
  if (actionType == "propagatemet") {
    const std::string nominalPtColumn = requireResolvedAliasedValue(
        action, {"nominalJetPtColumn", "nominalPtColumn"}, configManager,
        localValues, owner);
    const std::string variedPtColumn = requireResolvedAliasedValue(
        action, {"variedJetPtColumn", "variedPtColumn"}, configManager,
        localValues, owner);
    manager.propagateMET(
        requireResolvedValue(action, "baseMETPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "baseMETPhiColumn", configManager, localValues, owner),
        nominalPtColumn,
        variedPtColumn,
        requireResolvedValue(action, "outputMETPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputMETPhiColumn", configManager, localValues, owner),
        getResolvedFloat(action, "ptThreshold", configManager, localValues, owner, 0.0f));
    return;
  }
  if (actionType == "registersystematicsources") {
    manager.registerSystematicSources(
        requireResolvedValue(action, "setName", configManager, localValues, owner),
        getResolvedList(action, "sources", configManager, localValues, owner));
    return;
  }
  if (actionType == "applysystematicset") {
    if (!correctionManager) {
      throw std::runtime_error(owner + ": applySystematicSet action requires CorrectionManager");
    }
    manager.applySystematicSet(
        *correctionManager,
        requireResolvedValue(action, "correctionName", configManager, localValues, owner),
        requireResolvedValue(action, "setName", configManager, localValues, owner),
        requireResolvedValue(action, "inputPtColumn", configManager, localValues, owner),
        requireResolvedValue(action, "outputPtPrefix", configManager, localValues, owner),
        getResolvedBool(action, "applyToMass", configManager, localValues, owner, false),
        getResolvedList(action, "inputColumns", configManager, localValues, owner),
        getResolvedValue(action, "inputMassColumn", configManager, localValues, owner));
    return;
  }

  throw std::runtime_error(owner + ": unsupported workflow action '" + actionType + "'");
}

template <typename VariationEntry>
std::vector<std::string> collectVariationNames(const std::vector<VariationEntry> &variations) {
  std::vector<std::string> names;
  names.reserve(variations.size());
  for (const auto &variation : variations) {
    names.push_back(variation.name);
  }
  return names;
}

} // namespace

// Compact workflow helper functions in global namespace
// Implementation of compact workflow helpers in global namespace
template <typename ManagerT>
void applyRelativePtUncertaintySystematic(
    ManagerT &manager,
    CorrectionManager &correctionManager,
    IDataFrameProvider &dataManager,
    ISystematicManager &systematicManager,
    IConfigurationProvider &configManager,
    const ActionMap &action,
    const ActionMap &localValues,
    const std::string &owner) {
  const std::string correctionName = requireResolvedAliasedValue(
    action, {"correction", "correctionName"}, configManager, localValues, owner);
  const std::string systematicName = requireResolvedValue(
    action, "systematicName", configManager, localValues, owner);
  const std::string inputPtColumn = getResolvedValue(
    action, "inputPtColumn", configManager, localValues, owner,
    localValues.at("ptColumn"));
  const std::string uncertaintyColumn = getResolvedAliasedValue(
    action, {"uncertaintyColumn", "outputUncertaintyColumn"}, configManager,
    localValues, owner, systematicName + "_uncertainty");
  const std::string scaleFactorPrefix = getResolvedValue(
    action, "scaleFactorPrefix", configManager, localValues, owner,
    systematicName + "_sf");
  const std::string outputPtPrefix = getResolvedValue(
    action, "outputPtPrefix", configManager, localValues, owner,
    inputPtColumn + "_" + systematicName);
  const bool applyToMass = getResolvedBool(
    action, "applyToMass", configManager, localValues, owner, false);
  const std::string inputMassColumn = getResolvedValue(
    action, "inputMassColumn", configManager, localValues, owner);
  const std::string outputMassPrefix = getResolvedValue(
    action, "outputMassPrefix", configManager, localValues, owner);

  correctionManager.applyCorrectionVec(
    correctionName,
    getResolvedList(action, "stringArgs", configManager, localValues, owner),
    getResolvedList(action, "inputColumns", configManager, localValues, owner),
    uncertaintyColumn);

  defineRelativeUncertaintyScaleFactors(
    dataManager, systematicManager, uncertaintyColumn,
    appendDirectionalSuffix(scaleFactorPrefix, "up"),
    appendDirectionalSuffix(scaleFactorPrefix, "down"));

  const std::string upMassColumn = outputMassPrefix.empty()
                     ? ""
                     : appendDirectionalSuffix(outputMassPrefix, "up");
  const std::string downMassColumn = outputMassPrefix.empty()
                     ? ""
                     : appendDirectionalSuffix(outputMassPrefix, "down");

  manager.applyCorrection(
    inputPtColumn,
    appendDirectionalSuffix(scaleFactorPrefix, "up"),
    appendDirectionalSuffix(outputPtPrefix, "up"),
    applyToMass,
    inputMassColumn,
    upMassColumn);
  manager.applyCorrection(
    inputPtColumn,
    appendDirectionalSuffix(scaleFactorPrefix, "down"),
    appendDirectionalSuffix(outputPtPrefix, "down"),
    applyToMass,
    inputMassColumn,
    downMassColumn);
  manager.addVariation(
    systematicName,
    appendDirectionalSuffix(outputPtPrefix, "up"),
    appendDirectionalSuffix(outputPtPrefix, "down"),
    upMassColumn,
    downMassColumn);
}

template <typename ManagerT>
void applyResolutionSmearingSystematic(
    ManagerT &manager,
    CorrectionManager &correctionManager,
    IConfigurationProvider &configManager,
    const ActionMap &action,
    const ActionMap &localValues,
    const std::string &owner) {
  const std::string correctionName = requireResolvedAliasedValue(
    action, {"correction", "correctionName"}, configManager, localValues, owner);
  const std::string systematicName = requireResolvedValue(
    action, "systematicName", configManager, localValues, owner);
  const std::string inputPtColumn = getResolvedValue(
    action, "inputPtColumn", configManager, localValues, owner,
    localValues.at("ptColumn"));
  const std::string nominalOutputPtColumn = getResolvedAliasedOrDefault(
    action, {"outputPtColumn", "nominalOutputPtColumn"}, configManager, localValues,
    owner, localValues.at("correctedPtColumn"));
  const std::string outputPtPrefix = getResolvedValue(
    action, "outputPtPrefix", configManager, localValues, owner,
    inputPtColumn + "_" + systematicName);
  const std::string sigmaColumnPrefix = getResolvedValue(
    action, "sigmaColumnPrefix", configManager, localValues, owner,
    systematicName + "_sigma");
  const std::string randomColumn = getResolvedValue(
    action, "randomColumn", configManager, localValues, owner,
    sigmaColumnPrefix + "_random");
  const std::string randomSizeColumn = getResolvedValue(
    action, "sizeColumn", configManager, localValues, owner, inputPtColumn);
  const std::string runColumn = getResolvedValue(
    action, "runColumn", configManager, localValues, owner,
    getValue(localValues, "runColumn"));
  const std::string lumiColumn = getResolvedValue(
    action, "lumiColumn", configManager, localValues, owner,
    getValue(localValues, "lumiColumn"));
  const std::string eventColumn = getResolvedValue(
    action, "eventColumn", configManager, localValues, owner,
    getValue(localValues, "eventColumn"));
  const std::string salt = getResolvedValue(
    action, "salt", configManager, localValues, owner, systematicName);

  if (!getResolvedBool(action, "skipRandomDefinition", configManager, localValues,
             owner, false)) {
  manager.defineReproducibleGaussian(randomColumn, randomSizeColumn, runColumn,
                     lumiColumn, eventColumn, salt);
  }

  auto nominalStringArgs =
      getResolvedList(action, "nominalStringArgs", configManager, localValues, owner);
  auto upStringArgs =
      getResolvedList(action, "upStringArgs", configManager, localValues, owner);
  auto downStringArgs =
      getResolvedList(action, "downStringArgs", configManager, localValues, owner);
  if (nominalStringArgs.empty()) {
    nominalStringArgs = {"smear"};
  }
  if (upStringArgs.empty()) {
    upStringArgs = {"smear_up"};
  }
  if (downStringArgs.empty()) {
    downStringArgs = {"smear_down"};
  }

  correctionManager.applyCorrectionVec(
    correctionName,
    nominalStringArgs,
    getResolvedList(action, "inputColumns", configManager, localValues, owner),
    appendDirectionalSuffix(sigmaColumnPrefix, "nominal"));
  correctionManager.applyCorrectionVec(
    correctionName,
    upStringArgs,
    getResolvedList(action, "inputColumns", configManager, localValues, owner),
    appendDirectionalSuffix(sigmaColumnPrefix, "up"));
  correctionManager.applyCorrectionVec(
    correctionName,
    downStringArgs,
    getResolvedList(action, "inputColumns", configManager, localValues, owner),
    appendDirectionalSuffix(sigmaColumnPrefix, "down"));

  manager.applyResolutionSmearing(inputPtColumn,
                  appendDirectionalSuffix(sigmaColumnPrefix, "nominal"),
                  randomColumn,
                  nominalOutputPtColumn);
  manager.applyResolutionSmearing(inputPtColumn,
                  appendDirectionalSuffix(sigmaColumnPrefix, "up"),
                  randomColumn,
                  appendDirectionalSuffix(outputPtPrefix, "up"));
  manager.applyResolutionSmearing(inputPtColumn,
                  appendDirectionalSuffix(sigmaColumnPrefix, "down"),
                  randomColumn,
                  appendDirectionalSuffix(outputPtPrefix, "down"));
  manager.addVariation(systematicName,
             appendDirectionalSuffix(outputPtPrefix, "up"),
             appendDirectionalSuffix(outputPtPrefix, "down"));
}

void applyScaleResolutionSystematics(
    MuonRochesterManager &manager,
    IConfigurationProvider &configManager,
    const ActionMap &action,
    const ActionMap &localValues,
    const std::string &owner) {
  const bool isMC = inferIsMC(configManager);
  const std::string jsonFile = requireResolvedValue(
    action, "jsonFile", configManager, localValues, owner);
  const std::string inputPtColumn = getResolvedValue(
    action, "inputPtColumn", configManager, localValues, owner,
    localValues.at("ptColumn"));
  const std::string nominalOutputPtColumn = getResolvedAliasedOrDefault(
    action, {"outputPtColumn", "nominalOutputPtColumn"}, configManager, localValues,
    owner, localValues.at("correctedPtColumn"));

  manager.applyScaleAndResolution(jsonFile, !isMC, inputPtColumn, nominalOutputPtColumn,
                  "nom", "nom");
  if (!isMC) {
  return;
  }

  const std::string scaleSystematicName = getResolvedValue(
    action, "scaleSystematicName", configManager, localValues, owner, "muon_scale");
  const std::string scaleOutputPtPrefix = getResolvedValue(
    action, "scaleOutputPtPrefix", configManager, localValues, owner,
    inputPtColumn + "_scale");
  const std::string smearSystematicName = getResolvedValue(
    action, "resolutionSystematicName", configManager, localValues, owner,
    "muon_smear");
  const std::string smearOutputPtPrefix = getResolvedValue(
    action, "resolutionOutputPtPrefix", configManager, localValues, owner,
    inputPtColumn + "_smear");

  manager.applyScaleAndResolution(jsonFile, false, inputPtColumn,
                  appendDirectionalSuffix(scaleOutputPtPrefix, "up"),
                  "up", "nom");
  manager.applyScaleAndResolution(jsonFile, false, inputPtColumn,
                  appendDirectionalSuffix(scaleOutputPtPrefix, "down"),
                  "down", "nom");
  manager.applyScaleAndResolution(jsonFile, false, inputPtColumn,
                  appendDirectionalSuffix(smearOutputPtPrefix, "up"),
                  "nom", "up");
  manager.applyScaleAndResolution(jsonFile, false, inputPtColumn,
                  appendDirectionalSuffix(smearOutputPtPrefix, "down"),
                  "nom", "down");
  manager.addVariation(scaleSystematicName,
             appendDirectionalSuffix(scaleOutputPtPrefix, "up"),
             appendDirectionalSuffix(scaleOutputPtPrefix, "down"));
  manager.addVariation(smearSystematicName,
             appendDirectionalSuffix(smearOutputPtPrefix, "up"),
             appendDirectionalSuffix(smearOutputPtPrefix, "down"));
}

void applyCorrectionlibVariation(
    JetEnergyScaleManager &manager,
    CorrectionManager &correctionManager,
    IConfigurationProvider &configManager,
    const ActionMap &action,
    const ActionMap &localValues,
    const std::string &owner) {
  const std::string correctionName = requireResolvedAliasedValue(
    action, {"correction", "correctionName"}, configManager, localValues, owner);
  const std::string systematicName = requireResolvedValue(
    action, "systematicName", configManager, localValues, owner);
  const std::string inputPtColumn = requireResolvedValue(
    action, "inputPtColumn", configManager, localValues, owner);
  const bool applyToMass = getResolvedBool(
    action, "applyToMass", configManager, localValues, owner, true);
  const std::string inputMassColumn = getResolvedValue(
    action, "inputMassColumn", configManager, localValues, owner);
  const std::string outputPtPrefix = requireResolvedValue(
    action, "outputPtPrefix", configManager, localValues, owner);
  const std::string outputMassPrefix = getResolvedValue(
    action, "outputMassPrefix", configManager, localValues, owner);
  const auto inputColumns = getResolvedList(action, "inputColumns", configManager,
                      localValues, owner);

  const std::string upPtColumn = appendDirectionalSuffix(outputPtPrefix, "up");
  const std::string downPtColumn = appendDirectionalSuffix(outputPtPrefix, "down");
  const std::string upMassColumn = outputMassPrefix.empty()
                     ? ""
                     : appendDirectionalSuffix(outputMassPrefix, "up");
  const std::string downMassColumn = outputMassPrefix.empty()
                     ? ""
                     : appendDirectionalSuffix(outputMassPrefix, "down");

  manager.applyCorrectionlib(
    correctionManager,
    correctionName,
    getResolvedList(action, "upStringArgs", configManager, localValues, owner),
    inputPtColumn,
    upPtColumn,
    applyToMass,
    inputMassColumn,
    upMassColumn,
    inputColumns);
  manager.applyCorrectionlib(
    correctionManager,
    correctionName,
    getResolvedList(action, "downStringArgs", configManager, localValues, owner),
    inputPtColumn,
    downPtColumn,
    applyToMass,
    inputMassColumn,
    downMassColumn,
    inputColumns);
  manager.addVariation(systematicName, upPtColumn, downPtColumn,
             upMassColumn, downMassColumn);

  const std::string baseMETPtColumn = getResolvedValue(
    action, "baseMETPtColumn", configManager, localValues, owner);
  const std::string baseMETPhiColumn = getResolvedValue(
    action, "baseMETPhiColumn", configManager, localValues, owner);
  const std::string nominalJetPtColumn = getResolvedAliasedOrDefault(
    action, {"nominalJetPtColumn", "nominalPtColumn"}, configManager,
    localValues, owner);
  const std::string outputMETPtPrefix = getResolvedValue(
    action, "outputMETPtPrefix", configManager, localValues, owner);
  const std::string outputMETPhiPrefix = getResolvedValue(
    action, "outputMETPhiPrefix", configManager, localValues, owner);
  const float ptThreshold = getResolvedFloat(
    action, "ptThreshold", configManager, localValues, owner, 15.0f);
  if (hasAnyValue({baseMETPtColumn, baseMETPhiColumn, nominalJetPtColumn,
           outputMETPtPrefix, outputMETPhiPrefix})) {
  manager.propagateMET(baseMETPtColumn, baseMETPhiColumn, nominalJetPtColumn,
             upPtColumn,
             appendDirectionalSuffix(outputMETPtPrefix, "up"),
             appendDirectionalSuffix(outputMETPhiPrefix, "up"),
             ptThreshold);
  manager.propagateMET(baseMETPtColumn, baseMETPhiColumn, nominalJetPtColumn,
             downPtColumn,
             appendDirectionalSuffix(outputMETPtPrefix, "down"),
             appendDirectionalSuffix(outputMETPhiPrefix, "down"),
             ptThreshold);
  }
}

void applyIndexedCorrectionlibVariations(
    JetEnergyScaleManager &manager,
    CorrectionManager &correctionManager,
    IConfigurationProvider &configManager,
    const ActionMap &action,
    const ActionMap &localValues,
    const std::string &owner) {
  const std::string slotPrefix = requireResolvedValue(
    action, "slotPrefix", configManager, localValues, owner);
  const int firstIndex = getResolvedInt(action, "firstIndex", configManager,
                    localValues, owner, 1);
  const int lastIndex = getResolvedInt(action, "lastIndex", configManager,
                     localValues, owner,
                     getResolvedInt(action, "slotCount", configManager,
                            localValues, owner, 0));
  if (lastIndex < firstIndex) {
  throw std::runtime_error(owner + ": invalid indexed systematic range");
  }

  const std::string registrationNamePrefix = getResolvedValue(
    action, "registrationNamePrefix", configManager, localValues, owner, "");
  const std::string systematicNamePrefix = getResolvedValue(
    action, "systematicNamePrefix", configManager, localValues, owner, "");
  const std::string file = requireResolvedValue(
    action, "file", configManager, localValues, owner);
  const std::string correctionFieldSuffix = getResolvedValue(
    action, "correctionFieldSuffix", configManager, localValues, owner,
    "CorrectionName");
  const std::string labelFieldSuffix = getResolvedValue(
    action, "labelFieldSuffix", configManager, localValues, owner, "Label");
  const std::string enabledFieldSuffix = getResolvedValue(
    action, "enabledFieldSuffix", configManager, localValues, owner, "Enabled");
  const std::string inputPtColumn = requireResolvedValue(
    action, "inputPtColumn", configManager, localValues, owner);
  const std::string inputMassColumn = getResolvedValue(
    action, "inputMassColumn", configManager, localValues, owner);
  const bool applyToMass = getResolvedBool(
    action, "applyToMass", configManager, localValues, owner, true);
  const std::string outputPtPrefix = requireResolvedValue(
    action, "outputPtPrefix", configManager, localValues, owner);
  const std::string outputMassPrefix = getResolvedValue(
    action, "outputMassPrefix", configManager, localValues, owner);
  const auto inputVariables = getResolvedList(
    action, "inputVariables", configManager, localValues, owner);

  for (int index = firstIndex; index <= lastIndex; ++index) {
  const std::string enabledKey = composeIndexedKey(slotPrefix, index, enabledFieldSuffix);
  if (!getConfigBool(configManager, enabledKey, false)) {
    continue;
  }

  const std::string label = getConfigValue(
    configManager, composeIndexedKey(slotPrefix, index, labelFieldSuffix));
  const std::string correctionName = getConfigValue(
    configManager, composeIndexedKey(slotPrefix, index, correctionFieldSuffix));
  if (label.empty() || correctionName.empty()) {
    throw std::runtime_error(owner + ": indexed systematic slot '" +
                 std::to_string(index) +
                 "' is missing label or correction name");
  }

  const std::string registeredName = registrationNamePrefix + label;
  correctionManager.registerCorrection(registeredName, file, correctionName,
                     inputVariables);

  ActionMap expandedAction = action;
  expandedAction["correction"] = registeredName;
  expandedAction["systematicName"] = systematicNamePrefix + label;
  expandedAction["outputPtPrefix"] = outputPtPrefix + "_" + label;
  if (!outputMassPrefix.empty()) {
    expandedAction["outputMassPrefix"] = outputMassPrefix + "_" + label;
  }
  if (action.count("outputMETPtPrefix") != 0) {
    expandedAction["outputMETPtPrefix"] =
      getResolvedValue(action, "outputMETPtPrefix", configManager, localValues, owner) +
      "_" + label;
  }
  if (action.count("outputMETPhiPrefix") != 0) {
    expandedAction["outputMETPhiPrefix"] =
      getResolvedValue(action, "outputMETPhiPrefix", configManager, localValues, owner) +
      "_" + label;
  }
  applyCorrectionlibVariation(manager, correctionManager, configManager,
                expandedAction, localValues, owner);
  }
}

CorrectedCollectionManagerBase::CorrectedCollectionManagerBase(
  std::string configKey, CorrectionManager *correctionManager)
  : configKey_m(std::move(configKey)), correctionManager_m(correctionManager) {}

void CorrectedCollectionManagerBase::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
}

CorrectedCollectionSpec
CorrectedCollectionManagerBase::parseSpec(const std::string &configFile) const {
  const auto config = configManager_m->parsePairBasedConfig(configFile);

  const auto enabledIt = config.find("enabled");
  if (enabledIt != config.end() && !enabledIt->second.empty() &&
      !isTruthy(enabledIt->second)) {
    return {};
  }

  CorrectedCollectionSpec spec;
  spec.configFile = configFile;
  spec.inputCollection = config.count("inputCollection") ? config.at("inputCollection") : "";
  spec.workflowConfig = config.count("workflowConfig") ? config.at("workflowConfig") : "";
  spec.ptColumn = config.count("ptColumn") ? config.at("ptColumn") : "";
  spec.etaColumn = config.count("etaColumn") ? config.at("etaColumn") : "";
  spec.phiColumn = config.count("phiColumn") ? config.at("phiColumn") : "";
  spec.massColumn = config.count("massColumn") ? config.at("massColumn") : "";
  spec.metPtColumn = getValue(config, "metPtColumn");
  spec.metPhiColumn = getValue(config, "metPhiColumn");
  spec.rawFactorColumn = getValue(config, "rawFactorColumn");
  spec.rawPtColumn = getValue(config, "rawPtColumn");
  spec.jerGenJetPtColumn = getValue(config, "jerGenJetPtColumn");
  spec.jerRhoColumn = getValue(config, "jerRhoColumn");
  spec.jerEventColumn = getValue(config, "jerEventColumn");
  spec.runColumn = getValue(config, "runColumn");
  spec.lumiColumn = getValue(config, "lumiColumn");
  spec.eventColumn = getValue(config, "eventColumn");
  spec.chargeColumn = getValue(config, "chargeColumn");
  spec.genPtColumn = getValue(config, "genPtColumn");
  spec.nLayersColumn = getValue(config, "nLayersColumn");
  spec.u1Column = getValue(config, "u1Column");
  spec.u2Column = getValue(config, "u2Column");
  spec.correctedPtColumn = requireValue(config, "correctedPtColumn", type(), configFile);
  spec.correctedMassColumn = config.count("correctedMassColumn") ? config.at("correctedMassColumn") : "";
  spec.outputCollection = requireValue(config, "outputCollection", type(), configFile);
  spec.variationMapColumn = config.count("variationMapColumn") ? config.at("variationMapColumn") : "";

  if (spec.inputCollection.empty()) {
    spec.ptColumn = requireValue(config, "ptColumn", type(), configFile);
    spec.etaColumn = requireValue(config, "etaColumn", type(), configFile);
    spec.phiColumn = requireValue(config, "phiColumn", type(), configFile);
    spec.massColumn = requireValue(config, "massColumn", type(), configFile);
    spec.inputCollection = spec.outputCollection + "_input";
    spec.autoBuildInputCollection = true;
  }

  return spec;
}

void CorrectedCollectionManagerBase::defineAutoInputCollection(
    const CorrectedCollectionSpec &spec) {
  dataManager_m->Define(
      spec.inputCollection,
      [](const ROOT::VecOps::RVec<Float_t> &pt,
         const ROOT::VecOps::RVec<Float_t> &eta,
         const ROOT::VecOps::RVec<Float_t> &phi,
         const ROOT::VecOps::RVec<Float_t> &mass) -> PhysicsObjectCollection {
        ROOT::VecOps::RVec<bool> mask(pt.size(), true);
        return PhysicsObjectCollection(pt, eta, phi, mass, mask);
      },
      {spec.ptColumn, spec.etaColumn, spec.phiColumn, spec.massColumn},
      *systematicManager_m);
}

void CorrectedCollectionManagerBase::applyWorkflowConfig(
    const CorrectedCollectionSpec &spec) {
  if (spec.workflowConfig.empty()) {
    return;
  }

  const auto actions = configManager_m->parseMultiKeyConfig(spec.workflowConfig, {"type"});

  ActionMap localValues = {
      {"configFile", spec.configFile},
      {"workflowConfig", spec.workflowConfig},
      {"inputCollection", spec.inputCollection},
      {"ptColumn", spec.ptColumn},
      {"etaColumn", spec.etaColumn},
      {"phiColumn", spec.phiColumn},
      {"massColumn", spec.massColumn},
        {"metPtColumn", spec.metPtColumn},
        {"metPhiColumn", spec.metPhiColumn},
        {"rawFactorColumn", spec.rawFactorColumn},
        {"rawPtColumn", spec.rawPtColumn},
        {"jerGenJetPtColumn", spec.jerGenJetPtColumn},
        {"jerRhoColumn", spec.jerRhoColumn},
        {"jerEventColumn", spec.jerEventColumn},
        {"runColumn", spec.runColumn},
        {"lumiColumn", spec.lumiColumn},
        {"eventColumn", spec.eventColumn},
        {"chargeColumn", spec.chargeColumn},
        {"genPtColumn", spec.genPtColumn},
        {"nLayersColumn", spec.nLayersColumn},
        {"u1Column", spec.u1Column},
        {"u2Column", spec.u2Column},
      {"correctedPtColumn", spec.correctedPtColumn},
      {"correctedMassColumn", spec.correctedMassColumn},
      {"outputCollection", spec.outputCollection},
      {"variationMapColumn", spec.variationMapColumn},
  };

  const bool isMC = inferIsMC(*configManager_m);
  localValues["sampleClass"] = isMC ? "mc" : "data";

  for (const auto &action : actions) {
    const std::string owner = type() + " workflow '" + spec.workflowConfig + "'";
    if (!getResolvedBool(action, "enabled", *configManager_m, localValues, owner, true)) {
      continue;
    }
    if (!actionAppliesToSample(action, *configManager_m, localValues, owner, isMC)) {
      continue;
    }

    if (tryApplyCommonWorkflowAction(action, localValues, *configManager_m,
                                     *dataManager_m, *systematicManager_m,
                                     correctionManager_m, owner)) {
      continue;
    }
    applyWorkflowAction(action, localValues);
  }
}

void CorrectedCollectionManagerBase::setupFromConfigFile() {
  if (!configManager_m || !dataManager_m || !systematicManager_m) {
    throw std::runtime_error(type() +
                             ": manager context was not initialized before setupFromConfigFile()");
  }

  const std::string configFile = configManager_m->get(configKey_m);
  if (configFile.empty()) {
    return;
  }

  spec_m = parseSpec(configFile);
  if (spec_m.outputCollection.empty()) {
    return;
  }

  if (spec_m.autoBuildInputCollection) {
    defineAutoInputCollection(spec_m);
  }

  applyImplicitSetup(spec_m);
  applyWorkflowConfig(spec_m);
  bindCollectionSpec(spec_m);
  materializeWrappedOutputs();
  configured_m = true;
}

void CorrectedCollectionManagerBase::execute() {
  if (!configured_m || systematicsRegistered_m || !systematicManager_m) {
    return;
  }
  systematicsRegistered_m = true;
}

void CorrectedCollectionManagerBase::reportMetadata() {
  if (!configured_m || !logger_m) {
    return;
  }

  logger_m->log(ILogger::Level::Info,
                type() + ": configured corrected " + objectLabel() +
                    " collection '" + spec_m.outputCollection +
                    "' from input collection '" + spec_m.inputCollection + "'");
}

std::unordered_map<std::string, std::string>
CorrectedCollectionManagerBase::collectProvenanceEntries() const {
  if (!configured_m) {
    return {};
  }

  std::unordered_map<std::string, std::string> entries;
  entries["config_file"] = spec_m.configFile;
  entries["input_collection"] = spec_m.inputCollection;
  entries["output_collection"] = spec_m.outputCollection;
  if (!spec_m.variationMapColumn.empty()) {
    entries["variation_map_column"] = spec_m.variationMapColumn;
  }
  if (spec_m.autoBuildInputCollection) {
    entries["pt_column"] = spec_m.ptColumn;
    entries["eta_column"] = spec_m.etaColumn;
    entries["phi_column"] = spec_m.phiColumn;
    entries["mass_column"] = spec_m.massColumn;
  }
  entries["corrected_pt_column"] = spec_m.correctedPtColumn;
  if (!spec_m.correctedMassColumn.empty()) {
    entries["corrected_mass_column"] = spec_m.correctedMassColumn;
  }
  if (!spec_m.workflowConfig.empty()) {
    entries["workflow_config"] = spec_m.workflowConfig;
  }
  return entries;
}

std::vector<std::string> CorrectedCollectionManagerBase::getRequiredColumns() const {
  if (!configured_m) {
    return {};
  }

  std::vector<std::string> columns = {spec_m.inputCollection, spec_m.correctedPtColumn};
  if (spec_m.autoBuildInputCollection) {
    columns.push_back(spec_m.ptColumn);
    columns.push_back(spec_m.etaColumn);
    columns.push_back(spec_m.phiColumn);
    columns.push_back(spec_m.massColumn);
  }
  if (!spec_m.correctedMassColumn.empty()) {
    columns.push_back(spec_m.correctedMassColumn);
  }
  return columns;
}

std::vector<std::string> CorrectedCollectionManagerBase::getProducedColumns() const {
  if (!configured_m) {
    return {};
  }

  std::vector<std::string> columns;
  if (spec_m.autoBuildInputCollection) {
    columns.push_back(spec_m.inputCollection);
  }
  columns.push_back(spec_m.outputCollection);
  if (!spec_m.variationMapColumn.empty()) {
    columns.push_back(spec_m.variationMapColumn);
  }
  for (const auto &variationName : getVariationNames()) {
    columns.push_back(spec_m.outputCollection + "_" + variationName + "Up");
    columns.push_back(spec_m.outputCollection + "_" + variationName + "Down");
  }
  return columns;
}

CorrectedJetCollectionManager::CorrectedJetCollectionManager(
    JetEnergyScaleManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedJetCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedJetCollectionManager>
CorrectedJetCollectionManager::create(Analyzer &an, const std::string &role,
                                      const std::string &jetManagerRole,
                                      const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<JetEnergyScaleManager>(jetManagerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedJetCollectionManager::create: manager role '" +
                             jetManagerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedJetCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedJetCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setJetColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                          spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
  if (hasValue(spec.rawFactorColumn)) {
    manager_m.removeExistingCorrections(spec.rawFactorColumn);
  }
  if (hasValue(spec.rawPtColumn)) {
    manager_m.setRawPtColumn(spec.rawPtColumn);
  }
  if (hasAnyValue({spec.jerGenJetPtColumn, spec.jerRhoColumn, spec.jerEventColumn})) {
    manager_m.setJERSmearingColumns(spec.jerGenJetPtColumn, spec.jerRhoColumn,
                                    spec.jerEventColumn);
  }
}

void CorrectedJetCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputJetCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedJetCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedJetCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));
  if (actionType == "applycorrectionlibvariation") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlibVariation action requires CorrectionManager");
    }
    applyCorrectionlibVariation(manager_m, *correctionManager(), configManager(),
                                action, localValues, owner);
    return;
  }
  if (actionType == "applyindexedcorrectionlibvariations") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyIndexedCorrectionlibVariations action requires CorrectionManager");
    }
    applyIndexedCorrectionlibVariations(manager_m, *correctionManager(),
                                        configManager(), action, localValues,
                                        owner);
    return;
  }
  applyJetWorkflowAction(manager_m, correctionManager(), configManager(), action,
                         localValues, owner);
}

CorrectedFatJetCollectionManager::CorrectedFatJetCollectionManager(
    JetEnergyScaleManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedFatJetCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedFatJetCollectionManager>
CorrectedFatJetCollectionManager::create(Analyzer &an, const std::string &role,
                                         const std::string &jetManagerRole,
                                         const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<JetEnergyScaleManager>(jetManagerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedFatJetCollectionManager::create: manager role '" +
                             jetManagerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedFatJetCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedFatJetCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setJetColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                          spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
  if (hasValue(spec.rawFactorColumn)) {
    manager_m.removeExistingCorrections(spec.rawFactorColumn);
  }
  if (hasValue(spec.rawPtColumn)) {
    manager_m.setRawPtColumn(spec.rawPtColumn);
  }
  if (hasAnyValue({spec.jerGenJetPtColumn, spec.jerRhoColumn, spec.jerEventColumn})) {
    manager_m.setJERSmearingColumns(spec.jerGenJetPtColumn, spec.jerRhoColumn,
                                    spec.jerEventColumn);
  }
}

void CorrectedFatJetCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputJetCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedFatJetCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedFatJetCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));
  if (actionType == "applycorrectionlibvariation") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlibVariation action requires CorrectionManager");
    }
    applyCorrectionlibVariation(manager_m, *correctionManager(), configManager(),
                                action, localValues, owner);
    return;
  }
  if (actionType == "applyindexedcorrectionlibvariations") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyIndexedCorrectionlibVariations action requires CorrectionManager");
    }
    applyIndexedCorrectionlibVariations(manager_m, *correctionManager(),
                                        configManager(), action, localValues,
                                        owner);
    return;
  }
  applyJetWorkflowAction(manager_m, correctionManager(), configManager(), action,
                         localValues, owner);
}

CorrectedElectronCollectionManager::CorrectedElectronCollectionManager(
    ElectronEnergyScaleManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedElectronCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedElectronCollectionManager>
CorrectedElectronCollectionManager::create(Analyzer &an, const std::string &role,
                                           const std::string &managerRole,
                                           const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<ElectronEnergyScaleManager>(managerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedElectronCollectionManager::create: manager role '" +
                             managerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedElectronCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedElectronCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setObjectColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                             spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
}

void CorrectedElectronCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedElectronCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedElectronCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));
  if (actionType == "applyrelativeptuncertaintysystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRelativePtUncertaintySystematic action requires CorrectionManager");
    }
    applyRelativePtUncertaintySystematic(manager_m, *correctionManager(), dataManager(),
                                         systematicManager(), configManager(), action,
                                         localValues, owner);
    return;
  }
  if (actionType == "applyresolutionsmearingsystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyResolutionSmearingSystematic action requires CorrectionManager");
    }
    applyResolutionSmearingSystematic(manager_m, *correctionManager(),
                                      configManager(), action, localValues, owner);
    return;
  }
  if (actionType == "applycorrectionlib") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlib action requires CorrectionManager");
    }
    manager_m.applyCorrectionlib(
        *correctionManager(),
        requireResolvedAliasedValue(action, {"correction", "correctionName"},
                                    configManager(), localValues, owner),
        getResolvedList(action, "stringArgs", configManager(), localValues, owner),
      getResolvedValue(action, "inputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "ptColumn")),
      getResolvedValue(action, "outputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "correctedPtColumn")),
        getResolvedBool(action, "applyToMass", configManager(), localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager(), localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager(), localValues, owner),
        getResolvedList(action, "inputColumns", configManager(), localValues, owner));
    return;
  }
  applyObjectWorkflowAction(manager_m, action, localValues, configManager(), owner);
}

CorrectedMuonCollectionManager::CorrectedMuonCollectionManager(
    MuonRochesterManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedMuonCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedMuonCollectionManager>
CorrectedMuonCollectionManager::create(Analyzer &an, const std::string &role,
                                       const std::string &managerRole,
                                       const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<MuonRochesterManager>(managerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedMuonCollectionManager::create: manager role '" +
                             managerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedMuonCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedMuonCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setObjectColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                             spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
  if (hasValue(spec.chargeColumn)) {
    manager_m.setRochesterInputColumns(spec.chargeColumn, spec.genPtColumn,
                                       spec.nLayersColumn, spec.u1Column,
                                       spec.u2Column);
  }
  if (hasAnyValue({spec.lumiColumn, spec.eventColumn})) {
    manager_m.setScaleResolutionEventColumns(spec.lumiColumn, spec.eventColumn);
  }
}

void CorrectedMuonCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedMuonCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedMuonCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));

  if (actionType == "applyrelativeptuncertaintysystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRelativePtUncertaintySystematic action requires CorrectionManager");
    }
    applyRelativePtUncertaintySystematic(manager_m, *correctionManager(), dataManager(),
                                         systematicManager(), configManager(), action,
                                         localValues, owner);
    return;
  }
  if (actionType == "applyresolutionsmearingsystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyResolutionSmearingSystematic action requires CorrectionManager");
    }
    applyResolutionSmearingSystematic(manager_m, *correctionManager(),
                                      configManager(), action, localValues, owner);
    return;
  }
  if (actionType == "applyscaleresolutionsystematics") {
    applyScaleResolutionSystematics(manager_m, configManager(), action,
                                    localValues, owner);
    return;
  }

  if (actionType == "applycorrectionlib") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlib action requires CorrectionManager");
    }
    manager_m.applyCorrectionlib(
        *correctionManager(),
        requireResolvedAliasedValue(action, {"correction", "correctionName"},
                                    configManager(), localValues, owner),
        getResolvedList(action, "stringArgs", configManager(), localValues, owner),
      getResolvedValue(action, "inputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "ptColumn")),
      getResolvedValue(action, "outputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "correctedPtColumn")),
        getResolvedBool(action, "applyToMass", configManager(), localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager(), localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager(), localValues, owner),
        getResolvedList(action, "inputColumns", configManager(), localValues, owner));
    return;
  }
  if (actionType == "setrochesterinputcolumns") {
    manager_m.setRochesterInputColumns(
        requireResolvedValue(action, "chargeColumn", configManager(), localValues, owner),
        getResolvedValue(action, "genPtColumn", configManager(), localValues, owner),
        getResolvedValue(action, "nLayersColumn", configManager(), localValues, owner),
        getResolvedValue(action, "u1Column", configManager(), localValues, owner),
        getResolvedValue(action, "u2Column", configManager(), localValues, owner));
    return;
  }
  if (actionType == "setscaleresolutioneventcolumns") {
    manager_m.setScaleResolutionEventColumns(
        requireResolvedValue(action, "lumiColumn", configManager(), localValues, owner),
        requireResolvedValue(action, "eventColumn", configManager(), localValues, owner));
    return;
  }
  if (actionType == "applyscaleandresolution") {
    manager_m.applyScaleAndResolution(
        requireResolvedValue(action, "jsonFile", configManager(), localValues, owner),
        getResolvedBool(action, "isData", configManager(), localValues, owner, false),
        requireResolvedValue(action, "inputPtColumn", configManager(), localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager(), localValues, owner),
        getResolvedValue(action, "scaleVariation", configManager(), localValues, owner, "nom"),
        getResolvedValue(action, "resolutionVariation", configManager(), localValues, owner,
                         "nom"));
    return;
  }
  if (actionType == "applyrochestercorrection") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRochesterCorrection action requires CorrectionManager");
    }
    manager_m.applyRochesterCorrection(
        *correctionManager(),
        requireResolvedValue(action, "correctionName", configManager(), localValues, owner),
        requireResolvedValue(action, "inputPtColumn", configManager(), localValues, owner),
        requireResolvedValue(action, "outputPtColumn", configManager(), localValues, owner));
    return;
  }
  if (actionType == "applyrochestersystematicset") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRochesterSystematicSet action requires CorrectionManager");
    }
    manager_m.applyRochesterSystematicSet(
        *correctionManager(),
        requireResolvedValue(action, "correctionName", configManager(), localValues, owner),
        requireResolvedValue(action, "setName", configManager(), localValues, owner),
        requireResolvedValue(action, "inputPtColumn", configManager(), localValues, owner),
        requireResolvedValue(action, "outputPtPrefix", configManager(), localValues, owner));
    return;
  }
  applyObjectWorkflowAction(manager_m, action, localValues, configManager(), owner);
}

CorrectedTauCollectionManager::CorrectedTauCollectionManager(
    TauEnergyScaleManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedTauCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedTauCollectionManager>
CorrectedTauCollectionManager::create(Analyzer &an, const std::string &role,
                                      const std::string &managerRole,
                                      const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<TauEnergyScaleManager>(managerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedTauCollectionManager::create: manager role '" +
                             managerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedTauCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedTauCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setObjectColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                             spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
}

void CorrectedTauCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedTauCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedTauCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));
  if (actionType == "applyrelativeptuncertaintysystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRelativePtUncertaintySystematic action requires CorrectionManager");
    }
    applyRelativePtUncertaintySystematic(manager_m, *correctionManager(), dataManager(),
                                         systematicManager(), configManager(), action,
                                         localValues, owner);
    return;
  }
  if (actionType == "applyresolutionsmearingsystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyResolutionSmearingSystematic action requires CorrectionManager");
    }
    applyResolutionSmearingSystematic(manager_m, *correctionManager(),
                                      configManager(), action, localValues, owner);
    return;
  }
  if (actionType == "applycorrectionlib") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlib action requires CorrectionManager");
    }
    manager_m.applyCorrectionlib(
        *correctionManager(),
        requireResolvedAliasedValue(action, {"correction", "correctionName"},
                                    configManager(), localValues, owner),
        getResolvedList(action, "stringArgs", configManager(), localValues, owner),
      getResolvedValue(action, "inputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "ptColumn")),
      getResolvedValue(action, "outputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "correctedPtColumn")),
        getResolvedBool(action, "applyToMass", configManager(), localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager(), localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager(), localValues, owner),
        getResolvedList(action, "inputColumns", configManager(), localValues, owner));
    return;
  }
  applyObjectWorkflowAction(manager_m, action, localValues, configManager(), owner);
}

CorrectedPhotonCollectionManager::CorrectedPhotonCollectionManager(
    PhotonEnergyScaleManager &manager, CorrectionManager *correctionManager)
    : CorrectedCollectionManagerBase("correctedPhotonCollectionConfig", correctionManager),
      manager_m(manager) {}

std::shared_ptr<CorrectedPhotonCollectionManager>
CorrectedPhotonCollectionManager::create(Analyzer &an, const std::string &role,
                                         const std::string &managerRole,
                                         const std::string &correctionManagerRole) {
  auto manager = an.getPlugin<PhotonEnergyScaleManager>(managerRole);
  if (!manager) {
    throw std::runtime_error("CorrectedPhotonCollectionManager::create: manager role '" +
                             managerRole + "' is not registered");
  }
  auto correctionManager = an.getPlugin<CorrectionManager>(correctionManagerRole);
  auto plugin = std::make_shared<CorrectedPhotonCollectionManager>(
      *manager, correctionManager ? correctionManager.get() : nullptr);
  an.addPlugin(role, plugin);
  return plugin;
}

void CorrectedPhotonCollectionManager::applyImplicitSetup(
    const CorrectedCollectionSpec &spec) {
  manager_m.setObjectColumns(spec.ptColumn, spec.etaColumn, spec.phiColumn,
                             spec.massColumn);
  if (hasAnyValue({spec.metPtColumn, spec.metPhiColumn})) {
    manager_m.setMETColumns(spec.metPtColumn, spec.metPhiColumn);
  }
}

void CorrectedPhotonCollectionManager::bindCollectionSpec(
    const CorrectedCollectionSpec &spec) {
  manager_m.setInputCollection(spec.inputCollection);
  manager_m.defineCollectionOutput(spec.correctedPtColumn, spec.outputCollection,
                                   spec.correctedMassColumn);
  manager_m.defineVariationCollections(spec.outputCollection, spec.outputCollection,
                                       spec.variationMapColumn);
}

std::vector<std::string> CorrectedPhotonCollectionManager::getVariationNames() const {
  return collectVariationNames(manager_m.getVariations());
}

void CorrectedPhotonCollectionManager::applyWorkflowAction(
    const ActionMap &action,
    const ActionMap &localValues) {
  const std::string owner = type();
  const std::string actionType = toLower(
      requireResolvedValue(action, "type", configManager(), localValues, owner));
  if (actionType == "applyrelativeptuncertaintysystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyRelativePtUncertaintySystematic action requires CorrectionManager");
    }
    applyRelativePtUncertaintySystematic(manager_m, *correctionManager(), dataManager(),
                                         systematicManager(), configManager(), action,
                                         localValues, owner);
    return;
  }
  if (actionType == "applyresolutionsmearingsystematic") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyResolutionSmearingSystematic action requires CorrectionManager");
    }
    applyResolutionSmearingSystematic(manager_m, *correctionManager(),
                                      configManager(), action, localValues, owner);
    return;
  }
  if (actionType == "applycorrectionlib") {
    if (!correctionManager()) {
      throw std::runtime_error(owner + ": applyCorrectionlib action requires CorrectionManager");
    }
    manager_m.applyCorrectionlib(
        *correctionManager(),
        requireResolvedAliasedValue(action, {"correction", "correctionName"},
                                    configManager(), localValues, owner),
        getResolvedList(action, "stringArgs", configManager(), localValues, owner),
      getResolvedValue(action, "inputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "ptColumn")),
      getResolvedValue(action, "outputPtColumn", configManager(), localValues, owner,
               getValue(localValues, "correctedPtColumn")),
        getResolvedBool(action, "applyToMass", configManager(), localValues, owner, false),
        getResolvedValue(action, "inputMassColumn", configManager(), localValues, owner),
        getResolvedValue(action, "outputMassColumn", configManager(), localValues, owner),
        getResolvedList(action, "inputColumns", configManager(), localValues, owner));
    return;
  }
  applyObjectWorkflowAction(manager_m, action, localValues, configManager(), owner);
}