#include <CorrectionManager.h>
#include <RDFColumnPacker.h>
#include <analyzer.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <algorithm>
#include <cctype>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <variant>

namespace {

using CorrectionValue = std::variant<int, double, std::string>;

enum class CorrectionInputKind {
  String,
  Int,
  Double,
};

std::vector<CorrectionInputKind>
buildCorrectionInputKinds(const correction::Correction::Ref &correction) {
  std::vector<CorrectionInputKind> inputKinds;
  inputKinds.reserve(correction->inputs().size());
  for (const auto &varType : correction->inputs()) {
    if (varType.typeStr() == "string") {
      inputKinds.push_back(CorrectionInputKind::String);
    } else if (varType.typeStr() == "int") {
      inputKinds.push_back(CorrectionInputKind::Int);
    } else {
      inputKinds.push_back(CorrectionInputKind::Double);
    }
  }
  return inputKinds;
}

std::vector<CorrectionInputKind>
buildCorrectionInputKinds(const correction::CompoundCorrection::Ref &correction) {
  std::vector<CorrectionInputKind> inputKinds;
  inputKinds.reserve(correction->inputs().size());
  for (const auto &varType : correction->inputs()) {
    if (varType.typeStr() == "string") {
      inputKinds.push_back(CorrectionInputKind::String);
    } else if (varType.typeStr() == "int") {
      inputKinds.push_back(CorrectionInputKind::Int);
    } else {
      inputKinds.push_back(CorrectionInputKind::Double);
    }
  }
  return inputKinds;
}

void validateCorrectionInputs(const std::string &name,
                              const std::vector<CorrectionInputKind> &kinds,
                              size_t numericCount, size_t stringCount) {
  const size_t expectedStrings = std::count(kinds.begin(), kinds.end(),
                                            CorrectionInputKind::String);
  const size_t expectedNumeric = kinds.size() - expectedStrings;
  if (numericCount != expectedNumeric || stringCount != expectedStrings) {
    throw std::runtime_error(
        "CorrectionManager: correction '" + name + "' expects " +
        std::to_string(expectedNumeric) + " numeric and " +
        std::to_string(expectedStrings) + " string inputs; received " +
        std::to_string(numericCount) + " numeric and " +
        std::to_string(stringCount) + " string inputs");
  }
}

template <typename DoubleIterator>
void populateCorrectionValues(std::vector<CorrectionValue> &values,
                              const std::vector<CorrectionInputKind> &inputKinds,
                              const std::vector<std::string> &stringArgs,
                              DoubleIterator doubleArgIt) {
  values.clear();
  auto stringArgIt = stringArgs.begin();
  for (const auto inputKind : inputKinds) {
    switch (inputKind) {
    case CorrectionInputKind::String:
      values.emplace_back(*stringArgIt);
      ++stringArgIt;
      break;
    case CorrectionInputKind::Int:
      values.emplace_back(static_cast<int>(*doubleArgIt));
      ++doubleArgIt;
      break;
    case CorrectionInputKind::Double:
      values.emplace_back(*doubleArgIt);
      ++doubleArgIt;
      break;
    }
  }
}

/// Evaluate a correction with safe fallback for out-of-range inputs.
///
/// Attempts `correction->evaluate(values)` and returns the result. Out-of-range
/// bin lookups return the fallback value. Depending on the Correctionlib
/// version, these are reported as either std::out_of_range or a runtime_error
/// whose message begins with "Index below/above bounds in". No range exception
/// propagates to the caller. Every std::out_of_range fallback is reported to
/// stderr, including missing category keys.
///
/// For any other exception type the evaluation is still re-thrown as a
/// `std::runtime_error` with a descriptive message, since those indicate
/// genuine problems (corrupt files, schema mismatches, etc.).
template <typename CorrectionRef>
Float_t safeEvaluate(const CorrectionRef &correction,
                     std::vector<CorrectionValue> &values,
                     Float_t fallback = 1.0f) {
  try {
    return correction->evaluate(values);
  } catch (const std::out_of_range &e) {
    // Preserve the neutral fallback, but report every failure: correctionlib
    // also uses this exception for missing string/category keys, which can
    // indicate a configuration error rather than an expected numeric range.
    std::ostringstream warning;
    warning << "Warning: CorrectionManager: std::out_of_range evaluating "
            << "correction '" << correction->name() << "': " << e.what()
            << "; using fallback " << fallback << " for inputs [";
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (i != 0) warning << ", ";
      std::visit([&warning](const auto &value) { warning << value; }, values[i]);
    }
    warning << "]\n";
    std::cerr << warning.str();
    return fallback;
  } catch (const std::runtime_error &e) {
    // The bundled Correctionlib throws runtime_error (rather than
    // out_of_range) for Binning/MultiBinning nodes configured with
    // flow: "error". Keep only those known range failures neutral; other
    // runtime errors still indicate a broken payload or schema mismatch.
    const std::string_view message(e.what());
    if (message.rfind("Index below bounds in ", 0) == 0 ||
        message.rfind("Index above bounds in ", 0) == 0) {
      return fallback;
    }
    throw std::runtime_error(
        std::string("Error evaluating correction '") + correction->name() +
        "': " + e.what() +
        ". This indicates a genuine problem (corrupt file, schema "
        "mismatch, or missing correction) and should be investigated.");
  } catch (const std::exception &e) {
    throw std::runtime_error(
        std::string("Error evaluating correction '") + correction->name() +
        "': " + e.what() +
        ". This indicates a genuine problem (corrupt file, schema "
        "mismatch, or missing correction) and should be investigated.");
  }
}

Float_t evaluateScalarCorrection(
    const correction::Correction::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const std::vector<CorrectionInputKind> &inputKinds,
    const ROOT::VecOps::RVec<double> &inputVector) {
  std::vector<CorrectionValue> values;
  values.reserve(inputKinds.size());
  populateCorrectionValues(values, inputKinds, stringArgs, inputVector.begin());
  return safeEvaluate(correction, values);
}

Float_t evaluateScalarCorrection(
    const correction::CompoundCorrection::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const std::vector<CorrectionInputKind> &inputKinds,
    const ROOT::VecOps::RVec<double> &inputVector) {
  std::vector<CorrectionValue> values;
  values.reserve(inputKinds.size());
  populateCorrectionValues(values, inputKinds, stringArgs, inputVector.begin());
  return safeEvaluate(correction, values);
}

ROOT::VecOps::RVec<Float_t> evaluateVectorCorrection(
    const correction::Correction::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const std::vector<CorrectionInputKind> &inputKinds,
    const ROOT::VecOps::RVec<double> &flatInputVector,
    size_t featureCount) {
  if (featureCount == 0) {
    throw std::runtime_error("evaluateVectorCorrection: featureCount must be positive");
  }
  if (flatInputVector.size() % featureCount != 0) {
    throw std::runtime_error(
        "evaluateVectorCorrection: flattened input size is not divisible by featureCount");
  }

  const size_t objectCount = flatInputVector.size() / featureCount;
  ROOT::VecOps::RVec<Float_t> result(objectCount);
  std::vector<CorrectionValue> values;
  values.reserve(inputKinds.size());
  for (size_t i = 0; i < objectCount; ++i) {
    auto doubleArgIt = flatInputVector.begin() + static_cast<std::ptrdiff_t>(i * featureCount);
    populateCorrectionValues(values, inputKinds, stringArgs, doubleArgIt);
    result[i] = safeEvaluate(correction, values);
  }
  return result;
}

ROOT::VecOps::RVec<Float_t> evaluateVectorCorrection(
    const correction::CompoundCorrection::Ref &correction,
    const std::vector<std::string> &stringArgs,
    const std::vector<CorrectionInputKind> &inputKinds,
    const ROOT::VecOps::RVec<double> &flatInputVector,
    size_t featureCount) {
  if (featureCount == 0) {
    throw std::runtime_error("evaluateVectorCorrection: featureCount must be positive");
  }
  if (flatInputVector.size() % featureCount != 0) {
    throw std::runtime_error(
        "evaluateVectorCorrection: flattened input size is not divisible by featureCount");
  }

  const size_t objectCount = flatInputVector.size() / featureCount;
  ROOT::VecOps::RVec<Float_t> result(objectCount);
  std::vector<CorrectionValue> values;
  values.reserve(inputKinds.size());
  for (size_t i = 0; i < objectCount; ++i) {
    auto doubleArgIt = flatInputVector.begin() + static_cast<std::ptrdiff_t>(i * featureCount);
    populateCorrectionValues(values, inputKinds, stringArgs, doubleArgIt);
    result[i] = safeEvaluate(correction, values);
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

std::shared_ptr<const correction::CorrectionSet>
CorrectionManager::loadCorrectionSet(const std::string &file) {
  std::error_code error;
  const auto canonicalPath = std::filesystem::weakly_canonical(file, error);
  const std::string cacheKey = error
                                   ? std::filesystem::path(file)
                                         .lexically_normal()
                                         .string()
                                   : canonicalPath.string();

  const auto cached = correctionSets_m.find(cacheKey);
  if (cached != correctionSets_m.end()) {
    return cached->second;
  }

  auto loaded = correction::CorrectionSet::from_file(file);
  auto correctionSet =
      std::shared_ptr<const correction::CorrectionSet>(std::move(loaded));
  return correctionSets_m.emplace(cacheKey, std::move(correctionSet))
      .first->second;
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
    auto correctionSet = loadCorrectionSet(file);
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

void CorrectionManager::applyCorrections(
    const std::string &name,
    const std::vector<std::vector<std::string>> &arguments,
    const std::vector<std::string> &inputs,
    const std::vector<std::string> &outputs,
    const std::vector<bool> &propagateSystematics) {
  applyCorrectionBatch(false, name, arguments, inputs, outputs, propagateSystematics);
}

void CorrectionManager::applyCorrectionsVec(
    const std::string &name,
    const std::vector<std::vector<std::string>> &arguments,
    const std::vector<std::string> &inputs,
    const std::vector<std::string> &outputs,
    const std::vector<bool> &propagateSystematics) {
  applyCorrectionBatch(true, name, arguments, inputs, outputs, propagateSystematics);
}

void CorrectionManager::applyCorrectionBatch(
    bool vector, const std::string &name,
    const std::vector<std::vector<std::string>> &arguments,
    const std::vector<std::string> &inputs,
    const std::vector<std::string> &outputs,
    const std::vector<bool> &propagateSystematics) {
  if (!propagateSystematics.empty() && propagateSystematics.size() != arguments.size()) {
    throw std::invalid_argument("CorrectionManager: batch propagation/argument count mismatch");
  }
  if (!outputs.empty() && outputs.size() != arguments.size()) {
    throw std::invalid_argument("CorrectionManager: batch output/argument count mismatch");
  }
  std::vector<std::string> branches;
  branches.reserve(arguments.size());
  for (std::size_t i = 0; i < arguments.size(); ++i) {
    branches.push_back(outputs.empty() || outputs[i].empty()
                           ? makeBranchName(name, arguments[i]) : outputs[i]);
  }
  if (branches.empty()) return;
  if (!dataManager_m || !systematicManager_m)
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  const auto &resolved = inputs.empty() ? getCorrectionFeatures(name) : inputs;
  std::vector<CorrectionInputKind> kinds;
  if (objects_m.count(name)) kinds = buildCorrectionInputKinds(objects_m.at(name));
  else if (compoundObjects_m.count(name)) kinds = buildCorrectionInputKinds(compoundObjects_m.at(name));
  else throw std::runtime_error("CorrectionManager: unknown correction '" + name + "'");
  for (const auto &args : arguments)
    validateCorrectionInputs(name, kinds, resolved.size(), args.size());
  auto df = dataManager_m->getDataFrame();
  if (std::all_of(branches.begin(), branches.end(),
                  [&df](const std::string &branch) { return df.HasColumn(branch); })) return;
  // Never cache readers between calls: Redefine can change an input without
  // changing its name. A partial repeat also needs fresh inputs for new outputs.
  std::string packed;
  do {
    packed = "__rdf_correction_input_" + std::to_string(batchInputIndex_m++);
  } while (df.HasColumn(packed));
  for (std::size_t i = 0; i < arguments.size(); ++i) {
    const bool expand = propagateSystematics.empty() || propagateSystematics[i];
    if (vector) applyCorrectionVecImpl(name, arguments[i], inputs, branches[i], packed, expand);
    else applyCorrectionImpl(name, arguments[i], inputs, branches[i], packed, expand);
  }
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
  applyCorrectionImpl(correctionName, stringArguments, inputColumns, outputBranch, "");
}

void CorrectionManager::applyCorrectionImpl(
    const std::string &correctionName,
    const std::vector<std::string> &stringArguments,
    const std::vector<std::string> &inputColumns,
    const std::string &outputBranch, const std::string &packedInput,
    bool propagateSystematics) {
  std::cout << "Applying correction " << correctionName << std::endl;

  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  }

  const std::string branchName = outputBranch.empty()
                                     ? makeBranchName(correctionName, stringArguments)
                                     : outputBranch;
  const std::string inputColName = packedInput.empty() ? "input_" + branchName : packedInput;

  const std::vector<std::string> &resolvedInputs =
      inputColumns.empty() ? getCorrectionFeatures(correctionName) : inputColumns;

  if (const auto it = objects_m.find(correctionName); it != objects_m.end()) {
    validateCorrectionInputs(correctionName, buildCorrectionInputKinds(it->second),
                             resolvedInputs.size(), stringArguments.size());
  } else if (const auto it = compoundObjects_m.find(correctionName);
             it != compoundObjects_m.end()) {
    validateCorrectionInputs(correctionName, buildCorrectionInputKinds(it->second),
                             resolvedInputs.size(), stringArguments.size());
  } else {
    throw std::runtime_error("CorrectionManager: unknown correction '" + correctionName + "'");
  }

  std::cout << "Defining input features for correction " << correctionName << std::endl;
  if (propagateSystematics) {
    // A later output in the batch may be the first to request input variations.
    dataManager_m->DefineVector(inputColName, resolvedInputs, "double", *systematicManager_m);
  } else {
    dataManager_m->DefineVectorNominal(inputColName, resolvedInputs, "double", *systematicManager_m);
  }
  if (const auto corrIt = this->objects_m.find(correctionName);
      corrIt != this->objects_m.end()) {
    auto correction = corrIt->second;
    auto stringArgs = stringArguments;
    auto inputKinds = buildCorrectionInputKinds(correction);
    auto correctionLambda =
        [correction, stringArgs, inputKinds](const ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
      return evaluateScalarCorrection(correction, stringArgs, inputKinds, inputVector);
    };
    if (propagateSystematics)
      dataManager_m->Define(branchName, correctionLambda, {inputColName}, *systematicManager_m);
    else dataManager_m->DefineNominal(branchName, correctionLambda, {inputColName});
    return;
  }

  if (const auto compoundIt = compoundObjects_m.find(correctionName);
      compoundIt != compoundObjects_m.end()) {
    auto correction = compoundIt->second;
    auto stringArgs = stringArguments;
    auto inputKinds = buildCorrectionInputKinds(correction);
    auto correctionLambda =
        [correction, stringArgs, inputKinds](const ROOT::VecOps::RVec<double> &inputVector) -> Float_t {
      return evaluateScalarCorrection(correction, stringArgs, inputKinds, inputVector);
    };
    if (propagateSystematics)
      dataManager_m->Define(branchName, correctionLambda, {inputColName}, *systematicManager_m);
    else dataManager_m->DefineNominal(branchName, correctionLambda, {inputColName});
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
  applyCorrectionVecImpl(correctionName, stringArguments, inputColumns, outputBranch, "");
}

void CorrectionManager::applyCorrectionVecImpl(
    const std::string &correctionName,
    const std::vector<std::string> &stringArguments,
    const std::vector<std::string> &inputColumns,
    const std::string &outputBranch, const std::string &packedInput,
    bool propagateSystematics) {
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

  if (const auto it = objects_m.find(correctionName); it != objects_m.end()) {
    validateCorrectionInputs(correctionName, buildCorrectionInputKinds(it->second),
                             resolvedInputs.size(), stringArguments.size());
  } else if (const auto it = compoundObjects_m.find(correctionName);
             it != compoundObjects_m.end()) {
    validateCorrectionInputs(correctionName, buildCorrectionInputKinds(it->second),
                             resolvedInputs.size(), stringArguments.size());
  } else {
    throw std::runtime_error("CorrectionManager: unknown correction '" + correctionName + "'");
  }

  // Validate that all required input columns exist in the dataframe.
  {
    auto df = dataManager_m->getDataFrame();
    std::vector<std::string> missing;
    for (const auto &f : resolvedInputs) {
      if (!df.HasColumn(f)) {
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

  // Build an intermediate flattened RVec<double> that packs the per-object
  // feature values in row-major order. The stride is the number of input
  // features, so element block [i * stride : (i + 1) * stride) stores the
  // values for the i-th object.
  if (resolvedInputs.empty()) {
    throw std::runtime_error(
        "applyCorrectionVec: correction '" + correctionName +
        "' has no registered input variables");
  }
  // Single calls retain the branch-derived name; batches supply one shared
  // numeric pack for their different string arguments.
  const std::string inputVecName = packedInput.empty() ? "input_vec_" + branchName : packedInput;
  {
    auto dfNode = dataManager_m->getDataFrame();
    if (!dfNode.HasColumn(inputVecName)) {
      bool hasVectorInput = false;
      for (const auto &f : resolvedInputs) {
        const std::string columnType = dfNode.GetColumnType(f);
        const bool columnIsVector = rdfcolumnpacker::isRVecColumnType(columnType);
        if (columnIsVector) {
          hasVectorInput = true;
        }
      }

      if (!hasVectorInput) {
        throw std::runtime_error(
            "applyCorrectionVec: at least one input column must be an RVec for correction '" +
            correctionName + "'");
      }

      dfNode = rdfcolumnpacker::defineFlattenedNumericInputs(
          dfNode, inputVecName, resolvedInputs, "CorrectionManager");

      dataManager_m->setDataFrame(dfNode);
    }
  }

  // Lambda that applies the correction to every object in the collection.
  if (const auto corrIt = this->objects_m.find(correctionName);
      corrIt != this->objects_m.end()) {
    auto correction = corrIt->second;
    auto stringArgs = stringArguments;
    auto inputKinds = buildCorrectionInputKinds(correction);
    const size_t featureCount = resolvedInputs.size();
    auto correctionLambda =
      [correction, stringArgs, inputKinds, featureCount](const ROOT::VecOps::RVec<double>
                                                   &flatInputVector) -> ROOT::VecOps::RVec<Float_t> {
      return evaluateVectorCorrection(correction, stringArgs, inputKinds, flatInputVector,
                                      featureCount);
    };
    if (propagateSystematics)
      dataManager_m->Define(branchName, correctionLambda, {inputVecName}, *systematicManager_m);
    else dataManager_m->DefineNominal(branchName, correctionLambda, {inputVecName});
    return;
  }

  if (const auto compoundIt = compoundObjects_m.find(correctionName);
      compoundIt != compoundObjects_m.end()) {
    auto correction = compoundIt->second;
    auto stringArgs = stringArguments;
    auto inputKinds = buildCorrectionInputKinds(correction);
    const size_t featureCount = resolvedInputs.size();
    auto correctionLambda =
      [correction, stringArgs, inputKinds, featureCount](const ROOT::VecOps::RVec<double>
                                                   &flatInputVector) -> ROOT::VecOps::RVec<Float_t> {
      return evaluateVectorCorrection(correction, stringArgs, inputKinds, flatInputVector,
                                      featureCount);
    };
    if (propagateSystematics)
      dataManager_m->Define(branchName, correctionLambda, {inputVecName}, *systematicManager_m);
    else dataManager_m->DefineNominal(branchName, correctionLambda, {inputVecName});
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
  if (correctionConfigFile.empty()) {
    std::cout << "CorrectionManager: no correction config supplied; "
                 "programmatic registration is enabled."
              << std::endl;
    return;
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
    auto correctionF = loadCorrectionSet(entryKeys.at("file"));
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
    correctionConfigFile = configManager_m->get("correctionlibConfig");
  }
  if (correctionConfigFile.empty()) {
    initialized_m = true;
    return;
  }

  const auto correctionConfig = configManager_m->parseMultiKeyConfig(
    correctionConfigFile,
    {"file", "correctionName", "name", "inputVariables"});

  for (const auto &entryKeys : correctionConfig) {
    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configManager_m->splitString(entryKeys.at("inputVariables"), ",");

    // load correction object from json
    auto correctionF = loadCorrectionSet(entryKeys.at("file"));
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
