#include <set>
#include <CorrectionManager.h>
#include <ArrayValidation.h>
#include <RDFColumnPacker.h>
#include <analyzer.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <algorithm>
#include <cctype>
#include <cmath>
#include <iomanip>
#include <limits>
#include <mutex>
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

template <typename Evaluate>
void defineSlotCorrection(IDataFrameProvider &provider, ISystematicManager &systematics,
                          const std::string &name, Evaluate evaluate,
                          const std::vector<std::string> &columns, bool propagate) {
  auto df = provider.getDataFrame();
  if (df.HasColumn(name)) return;
  // ROOT supplies the slot directly to the callable, including in varied Defines.
  const auto book = [&](ROOT::RDF::RNode node, const std::string &output,
                        const std::vector<std::string> &inputs) {
    return node.DefineSlot(output, evaluate, inputs);
  };
  provider.setDataFrame(propagate
      ? rdf::detail::defineWithVariations(df, name, columns, systematics, book)
      : book(df, name, columns));
}

bool scalarDoubleOutput(const IConfigurationProvider &config) {
  const auto &settings = config.getConfigMap();
  const auto it = settings.find("correctionScalarOutputType");
  const auto type = it == settings.end() ? "float" : it->second;
  if (type != "float" && type != "double")
    throw std::invalid_argument("CorrectionManager: correctionScalarOutputType must be float or double");
  return type == "double";
}

ROOT::RDF::RNode defineCorrectionInputPacks(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &inputs, ISystematicManager &systematics,
    bool propagate) {
  const auto pack = [&](ROOT::RDF::RNode node, const std::string &output,
                        const std::vector<std::string> &columns) {
    if (node.HasColumn(output)) return node;
    return rdfcolumnpacker::defineFlattenedNumericInputs(node, output, columns, "CorrectionManager");
  };
  df = pack(df, name, inputs);
  if (!propagate) return df;
  const auto sources = systematics.getSystematics();
  for (const auto &source : sources) {
    std::vector<std::string> up, down;
    for (const auto &input : inputs) {
      up.push_back(systematics.getVariationColumnName(input, source + "Up"));
      down.push_back(systematics.getVariationColumnName(input, source + "Down"));
    }
    if (up == inputs && down == inputs) continue;
    const auto upName = up == inputs ? name : name + "_" + source + "Up";
    const auto downName = down == inputs ? name : name + "_" + source + "Down";
    if (up != inputs) df = pack(df, upName, up);
    if (down != inputs) df = pack(df, downName, down);
    systematics.registerVariationColumns(name, source, upName, downName);
  }
  for (const auto &label : systematics.getExplicitVariationLabels()) {
    std::vector<std::string> columns;
    for (const auto &input : inputs)
      columns.push_back(systematics.getVariationColumnName(input, label));
    if (columns == inputs) continue;
    const auto mapped = systematics.getVariationColumnName(name, label);
    const auto output = mapped == name ? name + "_" + label : mapped;
    df = pack(df, output, columns);
    systematics.registerVariationColumn(name, label, output);
  }
  return df;
}

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
      values.emplace_back(rdf::checkedIntegerInput(*doubleArgIt, "CorrectionManager"));
      ++doubleArgIt;
      break;
    case CorrectionInputKind::Double:
      if (!std::isfinite(*doubleArgIt))
        throw std::runtime_error("CorrectionManager: non-finite numeric correction input");
      values.emplace_back(*doubleArgIt);
      ++doubleArgIt;
      break;
    }
  }
}

struct CorrectionScratch {
  std::vector<CorrectionValue> values;
  correction::CompoundCorrection::Workspace compound;
};

using CorrectionSlots = std::vector<CorrectionScratch>;

double evaluateWithScratch(const correction::Correction::Ref &correction, CorrectionScratch &scratch) {
  return correction->evaluate(scratch.values);
}

double evaluateWithScratch(const correction::CompoundCorrection::Ref &correction, CorrectionScratch &scratch) {
  return correction->evaluate_with_workspace(scratch.values, scratch.compound);
}

/// Evaluate a correction with safe fallback for out-of-range inputs.
///
/// Attempts `correction->evaluate(values)` and returns the result. Out-of-range
/// bin lookups return the fallback value. Depending on the Correctionlib
/// version, these are reported as either std::out_of_range or a runtime_error
/// whose message begins with "Index below/above bounds in". No range exception
/// propagates to the caller. Every std::out_of_range fallback is reported to
/// stderr, including runtime_error range failures and missing category keys.
///
/// For any other exception type the evaluation is still re-thrown as a
/// `std::runtime_error` with a descriptive message, since those indicate
/// genuine problems (corrupt files, schema mismatches, etc.).
template <typename Output = Float_t, typename CorrectionRef>
Output safeEvaluate(const CorrectionRef &correction,
                     CorrectionScratch &scratch,
                     Output fallback = Output(1)) {
  const auto &values = scratch.values;
  const auto reportFallback = [&](const char *type, const std::exception &error) {
    std::ostringstream warning;
    warning << std::setprecision(17)
            << "Warning: CorrectionManager: " << type << " evaluating correction '"
            << correction->name() << "': " << error.what()
            << "; using fallback " << fallback << " for inputs [";
    for (std::size_t i = 0; i < values.size(); ++i) {
      if (i) warning << ", ";
      std::visit([&warning](const auto &value) { warning << value; }, values[i]);
    }
    warning << "] (";
    for (std::size_t i = 0; i < correction->inputs().size(); ++i) {
      if (i) warning << ", ";
      warning << correction->inputs()[i].name();
    }
    warning << ")\n";
    // Emit complete records even when RDF evaluates corrections on many slots.
    static std::mutex warningMutex;
    const std::lock_guard<std::mutex> lock(warningMutex);
    std::cerr << warning.str();
  };
  try {
    const auto result = evaluateWithScratch(correction, scratch);
    if (!std::isfinite(result) || std::abs(result) > std::numeric_limits<Output>::max())
      throw std::runtime_error("non-finite or unrepresentable correction result");
    return result;
  } catch (const std::out_of_range &e) {
    // Preserve the neutral fallback, but report every failure: correctionlib
    // also uses this exception for missing string/category keys, which can
    // indicate a configuration error rather than an expected numeric range.
    reportFallback("std::out_of_range", e);
    return fallback;
  } catch (const std::runtime_error &e) {
    // The bundled Correctionlib throws runtime_error (rather than
    // out_of_range) for Binning/MultiBinning nodes configured with
    // flow: "error". Keep only those known range failures neutral; other
    // runtime errors still indicate a broken payload or schema mismatch.
    const std::string_view message(e.what());
    if (message.rfind("Index below bounds in ", 0) == 0 ||
        message.rfind("Index above bounds in ", 0) == 0) {
      reportFallback("range error", e);
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

template <typename Output, typename CorrectionRef>
Output evaluateScalarCorrection(const CorrectionRef &correction,
                                const std::vector<std::string> &stringArgs,
                                const std::vector<CorrectionInputKind> &inputKinds,
                                const ROOT::VecOps::RVec<double> &inputs,
                                CorrectionScratch &scratch) {
  populateCorrectionValues(scratch.values, inputKinds, stringArgs, inputs.begin());
  return safeEvaluate<Output>(correction, scratch);
}

template <typename CorrectionRef>
ROOT::VecOps::RVec<Float_t> evaluateVectorCorrection(
    const CorrectionRef &correction,
    const std::vector<std::string> &stringArgs,
    const std::vector<CorrectionInputKind> &inputKinds,
    const ROOT::VecOps::RVec<double> &flatInputVector,
    size_t featureCount, CorrectionScratch &scratch) {
  if (featureCount == 0) {
    throw std::runtime_error("evaluateVectorCorrection: featureCount must be positive");
  }
  if (flatInputVector.size() % featureCount != 0) {
    throw std::runtime_error(
        "evaluateVectorCorrection: flattened input size is not divisible by featureCount");
  }

  const size_t objectCount = flatInputVector.size() / featureCount;
  ROOT::VecOps::RVec<Float_t> result(objectCount);
  auto &values = scratch.values;
  values.reserve(inputKinds.size());
  for (size_t i = 0; i < objectCount; ++i) {
    auto doubleArgIt = flatInputVector.begin() + static_cast<std::ptrdiff_t>(i * featureCount);
    populateCorrectionValues(values, inputKinds, stringArgs, doubleArgIt);
    result[i] = safeEvaluate(correction, scratch);
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

// Shares the same nominal aligned input pack used by applyCorrectionVec.
// A failed preparation leaves the dataframe untouched for the scalar path.
bool CorrectionManager::tryApplyBinnedGroup(const std::vector<std::string>& names,
                                            const std::vector<std::string>& inputs) {
  if (!dataManager_m || !systematicManager_m)
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  if (names.size() < 2 || inputs.size() != 2) return false;
  if (std::set<std::string>(names.begin(), names.end()).size() != names.size()) return false;
  auto df = dataManager_m->getDataFrame();
  std::vector<correction::Correction::Ref> sources;
  for (const auto& name : names) {
    const auto it = objects_m.find(name);
    if (it == objects_m.end() || df.HasColumn(name) || df.HasColumn("input_vec_" + name) ||
        !systematicManager_m->getSystematicsForVariable("input_vec_" + name).empty())
      return false;
    sources.push_back(it->second);
  }
  for (const auto& input : inputs) {
    if (!df.HasColumn(input) || !rdfcolumnpacker::isRVecColumnType(df.GetColumnType(input)))
      return false;
  }
  std::shared_ptr<const correction::BinnedFormulaGroup> group =
      correction::BinnedFormulaGroup::prepare(sources);
  if (!group) return false;
  std::cout << "CorrectionManager: grouping " << names.size()
            << " binned corrections starting with " << names.front() << '\n';
  std::string table;
  do {
    table = "__rdf_binned_corrections_" + std::to_string(batchInputIndex_m++);
  } while (df.HasColumn(table));
  const std::string pack = "input_vec_" + names.front();
  df = defineCorrectionInputPacks(df, pack, inputs, *systematicManager_m, false);
  for (size_t k = 1; k < names.size(); ++k)
    df = df.Alias("input_vec_" + names[k], pack);
  df = df.Define(table, [group](const ROOT::VecOps::RVec<double>& flat) {
    const size_t count = flat.size() / 2;
    ROOT::VecOps::RVec<double> results(count * group->size());
    for (size_t jet = 0; jet < count; ++jet) {
      const double x = flat[2 * jet], y = flat[2 * jet + 1];
      if (!std::isfinite(x) || !std::isfinite(y))
        throw std::runtime_error("CorrectionManager: non-finite numeric correction input");
      group->evaluate(x, y, results.data() + jet * group->size(), group->size());
    }
    return results;
  }, {pack});
  dataManager_m->setDataFrame(df);
  for (size_t k = 0; k < names.size(); ++k) {
    const auto extract = [k, stride = names.size(), name = sources[k]->name()](
        const ROOT::VecOps::RVec<double>& rows) {
      ROOT::VecOps::RVec<float> values(rows.size() / stride);
      for (size_t jet = 0; jet < values.size(); ++jet) {
        const double v = rows[jet * stride + k];
        if (!std::isfinite(v) || std::abs(v) > std::numeric_limits<float>::max())
          throw std::runtime_error("Error evaluating correction '" + name +
                                   "': non-finite or unrepresentable correction result");
        values[jet] = static_cast<float>(v);
      }
      return values;
    };
    dataManager_m->Define(names[k], extract, {table}, *systematicManager_m);
  }
  return true;
}

void CorrectionManager::applySelectedCorrectionsVec(
    const std::vector<VectorCorrectionCase> &cases,
    const std::string &selectorColumn,
    const std::vector<std::string> &inputColumns,
    const std::string &outputBranch, bool propagateSystematics) {
  applySelectedCorrectionsVecBatch({cases}, selectorColumn, inputColumns,
                                   {outputBranch}, {propagateSystematics});
}

void CorrectionManager::applySelectedCorrectionsVecBatch(
    const std::vector<std::vector<VectorCorrectionCase>> &caseSets,
    const std::string &selectorColumn,
    const std::vector<std::string> &inputColumns,
    const std::vector<std::string> &outputs,
    const std::vector<bool> &propagate) {
  if (!dataManager_m || !systematicManager_m)
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  if (caseSets.size() != outputs.size() ||
      (!propagate.empty() && propagate.size() != outputs.size()))
    throw std::invalid_argument("CorrectionManager: selected batch output/argument count mismatch");
  if (caseSets.empty()) return;
  if (inputColumns.empty())
    throw std::invalid_argument("CorrectionManager: selected correction needs numeric inputs");
  struct PreparedCase {
    correction::Correction::Ref correction;
    correction::CompoundCorrection::Ref compound;
    std::vector<CorrectionInputKind> kinds;
    std::vector<std::string> strings;
  };
  std::vector<std::vector<PreparedCase>> preparedBatch;
  for (std::size_t i = 0; i < caseSets.size(); ++i) {
    if (caseSets[i].empty() || outputs[i].empty())
      throw std::invalid_argument("CorrectionManager: selected correction needs cases and an output");
    std::vector<PreparedCase> prepared;
    for (const auto &item : caseSets[i]) {
      PreparedCase entry;
      if (objects_m.count(item.correctionName)) {
        entry.correction = objects_m.at(item.correctionName);
        entry.kinds = buildCorrectionInputKinds(entry.correction);
      } else if (compoundObjects_m.count(item.correctionName)) {
        entry.compound = compoundObjects_m.at(item.correctionName);
        entry.kinds = buildCorrectionInputKinds(entry.compound);
      } else {
        throw std::runtime_error("CorrectionManager: unknown correction '" + item.correctionName + "'");
      }
      entry.strings = item.stringArguments;
      validateCorrectionInputs(item.correctionName, entry.kinds, inputColumns.size(), entry.strings.size());
      prepared.push_back(std::move(entry));
    }
    preparedBatch.push_back(std::move(prepared));
  }
  auto df = dataManager_m->getDataFrame();
  if (std::all_of(outputs.begin(), outputs.end(), [&df](const auto &name) { return df.HasColumn(name); })) return;
  const bool perObjectInputs = std::any_of(inputColumns.begin(), inputColumns.end(),
      [&df](const std::string &column) {
        const auto type = df.GetColumnType(column);
        return rdfcolumnpacker::isRVecColumnType(type) || type.find("vector<") != std::string::npos;
      });
  std::string packed;
  do {
    packed = "__rdf_selected_correction_input_" + std::to_string(batchInputIndex_m++);
  } while (df.HasColumn(packed));
  const bool expandInputs = propagate.empty() || std::any_of(propagate.begin(), propagate.end(), [](bool value) { return value; });
  df = defineCorrectionInputPacks(df, packed, inputColumns, *systematicManager_m, expandInputs);
  dataManager_m->setDataFrame(df);
  for (std::size_t output = 0; output < outputs.size(); ++output) {
    const auto outputBranch = outputs[output];
    auto slots = std::make_shared<CorrectionSlots>(df.GetNSlots());
    std::size_t argumentCount = 0;
    for (const auto &entry : preparedBatch[output])
      argumentCount = std::max(argumentCount, entry.kinds.size());
    const auto evaluate =
        [prepared = std::move(preparedBatch[output]), stride = inputColumns.size(),
         outputBranch, perObjectInputs, argumentCount, slots](
            unsigned slot, const ROOT::VecOps::RVec<double> &flat,
            const ROOT::VecOps::RVec<int> &selectors) {
          if (flat.size() % stride ||
              (perObjectInputs ? flat.size() / stride != selectors.size() : flat.size() != stride))
            throw std::runtime_error(outputBranch + ": selector and numeric object arrays do not align");
          ROOT::VecOps::RVec<float> result(selectors.size());
          if (selectors.empty()) return result;
          auto &scratch = (*slots)[slot];
          auto &values = scratch.values;
          values.reserve(argumentCount);
          for (std::size_t i = 0; i < selectors.size(); ++i) {
            const auto selected = rdf::checkedIndex(outputBranch, selectors[i], prepared.size());
            const auto &entry = prepared[selected];
            populateCorrectionValues(values, entry.kinds, entry.strings,
                                     flat.begin() + (perObjectInputs ? i * stride : 0));
            result[i] = entry.correction ? safeEvaluate(entry.correction, scratch)
                                         : safeEvaluate(entry.compound, scratch);
          }
          return result;
        };
    defineSlotCorrection(*dataManager_m, *systematicManager_m, outputBranch, evaluate,
                         {packed, selectorColumn}, propagate.empty() || propagate[output]);
  }
}

void CorrectionManager::applyConditionalCorrection(
    const std::string &name, const std::vector<std::string> &arguments,
    const std::vector<std::string> &inputs, const std::string &condition,
    const std::string &output, bool propagateSystematics) {
  applyConditionalCorrections(name, {arguments}, inputs, condition, {output}, {propagateSystematics});
}

void CorrectionManager::applyConditionalCorrections(
    const std::string &name, const std::vector<std::vector<std::string>> &arguments,
    const std::vector<std::string> &inputs, const std::string &condition,
    const std::vector<std::string> &outputs, const std::vector<bool> &propagate) {
  applyConditionalCorrections(name, arguments, inputs, condition, outputs,
                             propagate, {});
}

void CorrectionManager::applyConditionalCorrections(
    const std::string &name, const std::vector<std::vector<std::string>> &arguments,
    const std::vector<std::string> &inputs, const std::string &condition,
    const std::vector<std::string> &outputs, const std::vector<bool> &propagate,
    std::function<void(ROOT::VecOps::RVec<double> &)> prepareInputs) {
  if (!dataManager_m || !systematicManager_m)
    throw std::runtime_error("CorrectionManager: DataManager or SystematicManager not set");
  if (arguments.size() != outputs.size() ||
      (!propagate.empty() && propagate.size() != outputs.size()))
    throw std::invalid_argument("CorrectionManager: conditional batch output/argument count mismatch");
  if (arguments.empty()) return;
  const auto &resolved = inputs.empty() ? getCorrectionFeatures(name) : inputs;
  auto df = dataManager_m->getDataFrame();
  if (std::all_of(outputs.begin(), outputs.end(), [&df](const auto &output) { return df.HasColumn(output); })) return;
  for (const auto &input : resolved) {
    const auto type = df.GetColumnType(input);
    if (rdfcolumnpacker::isRVecColumnType(type) || type.find("vector<") != std::string::npos)
      throw std::invalid_argument("CorrectionManager: conditional scalar input '" + input + "' is an array");
  }
  const auto book = [&](const auto &correction) {
    const auto kinds = buildCorrectionInputKinds(correction);
    for (std::size_t i = 0; i < arguments.size(); ++i) {
      if (outputs[i].empty()) throw std::invalid_argument("CorrectionManager: conditional correction needs an output");
      validateCorrectionInputs(name, kinds, resolved.size(), arguments[i].size());
    }
    std::string packed;
    do {
      packed = "__rdf_conditional_correction_input_" + std::to_string(batchInputIndex_m++);
    } while (df.HasColumn(packed));
    const bool expandInputs = propagate.empty() || std::any_of(propagate.begin(), propagate.end(), [](bool value) { return value; });
    if (expandInputs)
      dataManager_m->DefineVector(packed, resolved, "double", *systematicManager_m);
    else
      dataManager_m->DefineVectorNominal(packed, resolved, "double", *systematicManager_m);
    for (std::size_t i = 0; i < arguments.size(); ++i) {
      const auto bookOutput = [&](auto outputTag) {
        using Output = decltype(outputTag);
        const auto slots = std::make_shared<CorrectionSlots>(df.GetNSlots());
        const auto evaluate = [correction, kinds, strings = arguments[i], prepareInputs, slots](
            unsigned slot, const ROOT::VecOps::RVec<double> &values, bool active) -> Output {
          if (!active) return Output(1);
          if (!prepareInputs)
            return evaluateScalarCorrection<Output>(correction, strings, kinds, values, (*slots)[slot]);
          auto prepared = values;
          prepareInputs(prepared);
          if (prepared.size() != values.size())
            throw std::runtime_error("CorrectionManager: conditional input preparation changed input count");
          return evaluateScalarCorrection<Output>(correction, strings, kinds, prepared, (*slots)[slot]);
        };
        defineSlotCorrection(*dataManager_m, *systematicManager_m, outputs[i], evaluate,
                             {packed, condition}, propagate.empty() || propagate[i]);
      };
      if (scalarDoubleOutput(*configManager_m)) bookOutput(double{});
      else bookOutput(Float_t{});
    }
  };
  if (objects_m.count(name)) book(objects_m.at(name));
  else if (compoundObjects_m.count(name)) book(compoundObjects_m.at(name));
  else throw std::runtime_error("CorrectionManager: unknown correction '" + name + "'");
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
  const auto labels = systematicManager_m->getVariationLabels();
  const auto conflictsWithPack = [&](const std::string &candidate) {
    const auto prefix = candidate + "_";
    const auto aliasesPack = [&](const std::string &column) {
      return column == candidate || column.rfind(prefix, 0) == 0;
    };
    if (df.HasColumn(candidate) ||
        !systematicManager_m->getSystematicsForVariable(candidate).empty()) return true;
    for (const auto &label : labels)
      if (df.HasColumn(prefix + label)) return true;
    for (const auto &branch : branches) {
      if (aliasesPack(branch)) return true;
      for (const auto &label : labels)
        if (aliasesPack(systematicManager_m->getVariationColumnName(branch, label))) return true;
    }
    return false;
  };
  std::string packed;
  do {
    packed = "__rdf_correction_input_" + std::to_string(batchInputIndex_m++);
  } while (conflictsWithPack(packed));
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
  const bool doubleOutput = scalarDoubleOutput(*configManager_m);

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
  const auto book = [&](const auto &correction, auto outputTag) {
    using Output = decltype(outputTag);
    const auto stringArgs = stringArguments;
    const auto inputKinds = buildCorrectionInputKinds(correction);
    const auto slots = std::make_shared<CorrectionSlots>(dataManager_m->getDataFrame().GetNSlots());
    const auto evaluate = [correction, stringArgs, inputKinds, slots](
        unsigned slot, const ROOT::VecOps::RVec<double> &inputs) -> Output {
      return evaluateScalarCorrection<Output>(correction, stringArgs, inputKinds, inputs, (*slots)[slot]);
    };
    defineSlotCorrection(*dataManager_m, *systematicManager_m, branchName, evaluate,
                         {inputColName}, propagateSystematics);
  };
  const auto dispatch = [&](const auto &correction) {
    if (doubleOutput) book(correction, double{});
    else book(correction, Float_t{});
  };
  if (const auto it = objects_m.find(correctionName); it != objects_m.end()) {dispatch(it->second); return;}
  if (const auto it = compoundObjects_m.find(correctionName); it != compoundObjects_m.end()) {dispatch(it->second); return;}

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

    }
    // Legacy single-vector callers include energy-workflow steps whose explicitly
    // resolved inputs are booked before their future variation columns exist.
    // Explicit batch calls own expansion; a later output can be the first to
    // request it after an earlier nominal-only output reused this same pack.
    dfNode = defineCorrectionInputPacks(dfNode, inputVecName, resolvedInputs,
                                       *systematicManager_m,
                                       !packedInput.empty() && propagateSystematics);
    dataManager_m->setDataFrame(dfNode);
  }

  const auto book = [&](const auto &correction) {
    auto slots = std::make_shared<CorrectionSlots>(dataManager_m->getDataFrame().GetNSlots());
    auto evaluate = [correction, strings = stringArguments, kinds = buildCorrectionInputKinds(correction),
                     count = resolvedInputs.size(), slots](unsigned slot, const ROOT::VecOps::RVec<double> &inputs) {
      return evaluateVectorCorrection(correction, strings, kinds, inputs, count, (*slots)[slot]);
    };
    defineSlotCorrection(*dataManager_m, *systematicManager_m, branchName, evaluate,
                         {inputVecName}, propagateSystematics);
  };
  if (const auto it = objects_m.find(correctionName); it != objects_m.end())
    return book(it->second);
  if (const auto it = compoundObjects_m.find(correctionName); it != compoundObjects_m.end())
    return book(it->second);

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
