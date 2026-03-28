#include <CorrectionManager.h>
#include <analyzer.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <algorithm>
#include <cctype>
#include <iostream>
#include <stdexcept>

namespace {

Float_t evaluateScalarCorrection(
  const correction::Correction::Ref &correction,
    const std::vector<std::string> &stringArgs,
    ROOT::VecOps::RVec<double> &inputVector) {
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
}

Float_t evaluateScalarCorrection(
    const correction::CompoundCorrection::Ref &correction,
    const std::vector<std::string> &stringArgs,
    ROOT::VecOps::RVec<double> &inputVector) {
  std::vector<std::variant<int, double, std::string>> values;
  auto stringArgIt = stringArgs.begin();
  auto doubleArgIt = inputVector.begin();
  for (const auto &varType : correction->inputs()) {
    if (varType.typeStr() == "string") {
      values.push_back(*stringArgIt);
      ++stringArgIt;
    } else {
      values.push_back(*doubleArgIt);
      ++doubleArgIt;
    }
  }
  return correction->evaluate(values);
}

ROOT::VecOps::RVec<Float_t> evaluateVectorCorrection(
    const correction::Correction::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> &inputMatrix) {
  ROOT::VecOps::RVec<Float_t> result(inputMatrix.size());
  for (size_t i = 0; i < inputMatrix.size(); ++i) {
    std::vector<std::variant<int, double, std::string>> values;
    auto stringArgIt = stringArgs.begin();
    auto doubleArgIt = inputMatrix[i].begin();
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
    result[i] = correction->evaluate(values);
  }
  return result;
}

ROOT::VecOps::RVec<Float_t> evaluateVectorCorrection(
    const correction::CompoundCorrection::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> &inputMatrix) {
  ROOT::VecOps::RVec<Float_t> result(inputMatrix.size());
  for (size_t i = 0; i < inputMatrix.size(); ++i) {
    std::vector<std::variant<int, double, std::string>> values;
    auto stringArgIt = stringArgs.begin();
    auto doubleArgIt = inputMatrix[i].begin();
    for (const auto &varType : correction->inputs()) {
      if (varType.typeStr() == "string") {
        values.push_back(*stringArgIt);
        ++stringArgIt;
      } else {
        values.push_back(*doubleArgIt);
        ++doubleArgIt;
      }
    }
    result[i] = correction->evaluate(values);
  }
  return result;
}

template <typename CorrectionSetT>
auto lookupCorrectionOrCompound(const CorrectionSetT &correctionSet,
                                const std::string &name)
    -> std::pair<correction::Correction::Ref, correction::CompoundCorrection::Ref> {
  try {
    return {correctionSet->at(name), nullptr};
  } catch (const std::out_of_range &) {
  }

  const auto compoundIt = correctionSet->compound().find(name);
  if (compoundIt != correctionSet->compound().end()) {
    return {nullptr, compoundIt->second};
  }

  throw std::runtime_error("Correction '" + name + "' was not found as either a regular or compound correction");
}

} // namespace

/**
 * @brief Construct a new CorrectionManager object
 * @param configProvider Reference to the configuration provider
 */
CorrectionManager::CorrectionManager(IConfigurationProvider const& configProvider) {
  std::cout  << "Constructing CorrectionManager with config provider" << std::endl;
  registerCorrectionlib(configProvider);
  initialized_m = true;
}

/**
 * @brief Validate that a string is safe to use as part of an RDF branch name.
 *
 * Rejects empty strings and any character other than alphanumerics and
 * underscores, which are the only characters guaranteed to produce valid
 * ROOT TTree / RDataFrame column names.
 */
static void validateBranchComponent(const std::string &s, const std::string &context) {
  if (s.empty()) {
    throw std::invalid_argument(
        context + ": string argument must not be empty");
  }
  for (char c : s) {
    if (!std::isalnum(static_cast<unsigned char>(c)) && c != '_') {
      throw std::invalid_argument(
          context + ": string argument '" + s +
          "' contains invalid character '" + c +
          "' (only alphanumerics and underscores are allowed in branch names)");
    }
  }
}

/**
 * @brief Build an output branch name from a correction name and string arguments.
 *
 * When @p stringArguments is non-empty each argument is appended to
 * @p correctionName separated by underscores, allowing the same correction
 * to be applied multiple times with different variations (e.g. "nominal",
 * "syst_up", "syst_down") and stored in distinct dataframe columns.
 *
 * @param correctionName Base correction name.
 * @param stringArguments String arguments passed to the correction.
 * @return The decorated branch name.
 */
static std::string makeBranchName(const std::string &correctionName,
                                  const std::vector<std::string> &stringArguments) {
  for (const auto &arg : stringArguments) {
    validateBranchComponent(arg, "makeBranchName");
  }
  std::string name = correctionName;
  for (const auto &arg : stringArguments) {
    name += "_" + arg;
  }
  return name;
}

/**
 * @brief Register a correction directly from C++ code (without a config file).
 */
void CorrectionManager::registerCorrection(
    const std::string &name,
    const std::string &file,
    const std::string &correctionlibName,
    const std::vector<std::string> &inputVariables) {
  if (objects_m.count(name) || compoundObjects_m.count(name)) {
    throw std::runtime_error(
        "CorrectionManager::registerCorrection: a correction named '" + name +
        "' is already registered. Use a unique name for each correction.");
  }
  try {
    auto correctionSet = correction::CorrectionSet::from_file(file);
    const auto [corr, compoundCorr] =
        lookupCorrectionOrCompound(correctionSet, correctionlibName);
    if (corr) {
      objects_m.emplace(name, corr);
    } else {
      compoundObjects_m.emplace(name, compoundCorr);
    }
  } catch (const std::exception &e) {
    throw std::runtime_error(
        "CorrectionManager::registerCorrection: failed to load correction '" +
        correctionlibName + "' from file '" + file + "' for registration as '" +
        name + "': " + e.what());
  }
  std::cout << "CorrectionManager: registering correction '" << name
            << "' from file '" << file << "'" << std::endl;
  features_m.emplace(name, inputVariables);
}

/**
 * @brief Apply a scalar correction to the dataframe.
 *
 * @param correctionName  Name of the registered correction.
 * @param stringArguments String arguments consumed by the correction; each is
 *        also appended to the output column name (joined with underscores).
 * @param inputColumns    Optional override for the RDF input column names.
 *        When non-empty, these are used instead of the configured columns.
 * @param outputBranch    Optional explicit output column name.
 *        When non-empty, used as-is instead of the auto-derived name.
 */
void CorrectionManager::applyCorrection(const std::string &correctionName,
                                        const std::vector<std::string> &stringArguments,
                                        const std::vector<std::string> &inputColumns,
                                        const std::string &outputBranch) {
  std::cout << "Applying correction " << correctionName << std::endl;

  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  }

  const std::string branchName = outputBranch.empty()
                                     ? makeBranchName(correctionName, stringArguments)
                                     : outputBranch;
  const std::string inputColName = "input_" + branchName;

  const std::vector<std::string> &resolvedInputs =
      inputColumns.empty() ? getCorrectionFeatures(correctionName) : inputColumns;

  std::cout << "Defining input features for correction " << correctionName << std::endl;
  dataManager_m->DefineVector(inputColName, resolvedInputs, "double", *systematicManager_m);
  if (const auto corrIt = this->objects_m.find(correctionName);
      corrIt != this->objects_m.end()) {
    auto correction = corrIt->second;
    auto stringArgs = stringArguments;
    auto correctionLambda =
        [correction, stringArgs](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
      return evaluateScalarCorrection(correction, stringArgs, inputVector);
    };
    dataManager_m->Define(branchName, correctionLambda, {inputColName}, *systematicManager_m);
    return;
  }

  if (const auto compoundIt = compoundObjects_m.find(correctionName);
      compoundIt != compoundObjects_m.end()) {
    auto correction = compoundIt->second;
    auto stringArgs = stringArguments;
    auto correctionLambda =
        [correction, stringArgs](ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
      return evaluateScalarCorrection(correction, stringArgs, inputVector);
    };
    dataManager_m->Define(branchName, correctionLambda, {inputColName}, *systematicManager_m);
    return;
  }

  throw std::runtime_error("CorrectionManager::applyCorrection: unknown correction '" + correctionName + "'");
}

/**
 * @brief Get a correction object by key
 * @param key Correction key
 * @return Correction reference
 */
correction::Correction::Ref
CorrectionManager::getCorrection(const std::string &key) const {
  return getObject(key);
}

correction::CompoundCorrection::Ref
CorrectionManager::getCompoundCorrection(const std::string &key) const {
  const auto it = compoundObjects_m.find(key);
  if (it != compoundObjects_m.end()) {
    return it->second;
  }
  throw std::runtime_error("Compound correction not found: " + key);
}

/**
 * @brief Get the features for a correction by key
 * @param key Correction key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
CorrectionManager::getCorrectionFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Apply a correction over a vector of objects and store per-object
 *        results as an RVec<Float_t> column in the dataframe.
 *
 * @param correctionName  Name of the registered correction.
 * @param stringArguments Constant string arguments consumed by the correction.
 * @param inputColumns    Optional override for the RDF input column names
 *        (RVec columns).  When non-empty, used instead of configured columns.
 * @param outputBranch    Optional explicit output column name.
 *        When non-empty, used as-is instead of the auto-derived name.
 */
void CorrectionManager::applyCorrectionVec(
    const std::string &correctionName,
    const std::vector<std::string> &stringArguments,
    const std::vector<std::string> &inputColumns,
    const std::string &outputBranch) {
  std::cout << "Applying vector correction " << correctionName << std::endl;
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error(
        "CorrectionManager: DataManager or SystematicManager not set");
  }

  const std::string branchName = outputBranch.empty()
                                     ? makeBranchName(correctionName, stringArguments)
                                     : outputBranch;

  const std::vector<std::string> &resolvedInputs =
      inputColumns.empty() ? getCorrectionFeatures(correctionName) : inputColumns;

  // Validate that all required input columns exist in the dataframe.
  {
    const auto existingColumns = dataManager_m->getDataFrame().GetColumnNames();
    std::vector<std::string> missing;
    for (const auto &f : resolvedInputs) {
      if (std::find(existingColumns.begin(), existingColumns.end(), f) ==
          existingColumns.end()) {
        missing.push_back(f);
      }
    }
    if (!missing.empty()) {
      std::string msg = "applyCorrectionVec: missing columns: ";
      for (const auto &m : missing) {
        msg += m + " ";
      }
      throw std::runtime_error(msg);
    }
  }

  // Build an intermediate column of type RVec<RVec<double>> that packs the
  // per-object feature values.  Element [i][j] holds the value of input
  // feature j for the i-th object in the collection.
  if (resolvedInputs.empty()) {
    throw std::runtime_error(
        "applyCorrectionVec: correction '" + correctionName +
        "' has no registered input variables");
  }
  // The intermediate packed column name includes the branch suffix so that the
  // same correction can be applied with different string arguments without
  // column name collisions.
  const std::string inputVecName = "input_vec_" + branchName;
  {
    auto dfNode = dataManager_m->getDataFrame();
    const auto existingColumns = dfNode.GetColumnNames();
    if (std::find(existingColumns.begin(), existingColumns.end(),
                  inputVecName) == existingColumns.end()) {
      std::vector<bool> isRVec;
      isRVec.reserve(resolvedInputs.size());
      std::string sizeColumn;
      for (const auto &f : resolvedInputs) {
        const std::string colType = dfNode.GetColumnType(f);
        const bool columnIsRVec = colType.find("RVec") != std::string::npos;
        isRVec.push_back(columnIsRVec);
        if (sizeColumn.empty() && columnIsRVec) {
          sizeColumn = f;
        }
      }

      if (sizeColumn.empty()) {
        throw std::runtime_error(
            "applyCorrectionVec: at least one input column must be an RVec for correction '" +
            correctionName + "'");
      }

      std::string expr =
          "ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>> _out(" +
          sizeColumn + ".size());\n";
      for (size_t index = 0; index < resolvedInputs.size(); ++index) {
        const auto &f = resolvedInputs[index];
        const bool columnIsRVec = isRVec[index];
        if (columnIsRVec) {
          expr += "for (size_t _i = 0; _i < _out.size(); ++_i)"
                  " _out[_i].push_back(static_cast<double>(" +
                  f + "[_i]));\n";
        } else {
          expr += "for (size_t _i = 0; _i < _out.size(); ++_i)"
                  " _out[_i].push_back(static_cast<double>(" +
                  f + "));\n";
        }
      }
      expr += "return _out;";
      dfNode = dfNode.Define(inputVecName, expr);
      dataManager_m->setDataFrame(dfNode);
    }
  }

  // Lambda that applies the correction to every object in the collection.
  if (const auto corrIt = this->objects_m.find(correctionName);
      corrIt != this->objects_m.end()) {
    auto correction = corrIt->second;
    auto stringArgs = stringArguments;
    auto correctionLambda =
        [correction, stringArgs](const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>>
                                     &inputMatrix) -> ROOT::VecOps::RVec<Float_t> {
      return evaluateVectorCorrection(correction, stringArgs, inputMatrix);
    };
    dataManager_m->Define(branchName, correctionLambda, {inputVecName},
                          *systematicManager_m);
    return;
  }

  if (const auto compoundIt = compoundObjects_m.find(correctionName);
      compoundIt != compoundObjects_m.end()) {
    auto correction = compoundIt->second;
    auto stringArgs = stringArguments;
    auto correctionLambda =
        [correction, stringArgs](const ROOT::VecOps::RVec<ROOT::VecOps::RVec<double>>
                                     &inputMatrix) -> ROOT::VecOps::RVec<Float_t> {
      return evaluateVectorCorrection(correction, stringArgs, inputMatrix);
    };
    dataManager_m->Define(branchName, correctionLambda, {inputVecName},
                          *systematicManager_m);
    return;
  }

  throw std::runtime_error("CorrectionManager::applyCorrectionVec: unknown correction '" + correctionName + "'");
}

/**
 * @brief Register corrections from correctionlib using the configuration
 * @param configProvider Reference to the configuration provider
 */
void CorrectionManager::registerCorrectionlib(
    const IConfigurationProvider &configProvider) {
  // configProvider.get() returns an empty string when the key is absent.
  // Prefer "correctionConfig"; fall back to the legacy "correctionlibConfig".
  auto correctionConfigFile = configProvider.get("correctionConfig");
  if (correctionConfigFile.empty()) {
    correctionConfigFile = configProvider.get("correctionlibConfig");
  }
  std::cout << "CorrectionManager: Registering corrections from config file: " << correctionConfigFile << std::endl;

  const auto correctionConfig = configProvider.parseMultiKeyConfig(
      correctionConfigFile,
      {"file", "correctionName", "name", "inputVariables"});
  
  std::cout << "CorrectionManager: Found " << correctionConfig.size() << " corrections in config file." << std::endl;

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // load correction object from json
    auto correctionF =
        correction::CorrectionSet::from_file(entryKeys.at("file"));
    const auto [correction, compoundCorrection] =
        lookupCorrectionOrCompound(correctionF, entryKeys.at("correctionName"));

    // Add the correction and feature list to their maps
    std::cout << "Adding correction " << entryKeys.at("name") << "!"
              << std::endl;
    if (correction) {
      objects_m.emplace(entryKeys.at("name"), correction);
    } else {
      compoundObjects_m.emplace(entryKeys.at("name"), compoundCorrection);
    }
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
  }
} 

void CorrectionManager::setupFromConfigFile() {
  if (initialized_m) {
    return;
  }

  if (!configManager_m) {
    throw std::runtime_error("CorrectionManager: ConfigManager not set");
  }

  auto correctionConfigFile = configManager_m->get("correctionConfig");
  if (correctionConfigFile.empty()) {
    correctionConfigFile = "correctionlibConfig";
  }

  const auto correctionConfig = configManager_m->parseMultiKeyConfig(
    correctionConfigFile,
    {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configManager_m->splitString(entryKeys.at("inputVariables"), ",");

    // load correction object from json
    auto correctionF =
      correction::CorrectionSet::from_file(entryKeys.at("file"));
    const auto [correction, compoundCorrection] =
      lookupCorrectionOrCompound(correctionF, entryKeys.at("correctionName"));

    // Add the correction and feature list to their maps
    std::cout << "Adding correction " << entryKeys.at("name") << "!"
              << std::endl;
    if (correction) {
      objects_m.emplace(entryKeys.at("name"), correction);
    } else {
      compoundObjects_m.emplace(entryKeys.at("name"), compoundCorrection);
    }
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
  }

  initialized_m = true;
}
void CorrectionManager::initialize() {
  std::cout << "CorrectionManager: initialized with " << objects_m.size()
            << " correction(s)." << std::endl;
}

void CorrectionManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "CorrectionManager loaded corrections: ";
  bool first = true;
  for (const auto& kv : objects_m) {
    if (!first) msg += ", ";
    msg += kv.first;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}

std::shared_ptr<CorrectionManager> CorrectionManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<CorrectionManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}
