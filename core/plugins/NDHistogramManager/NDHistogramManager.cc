#include <api/IConfigurationProvider.h>
#include <analyzer.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <api/IPluggableManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <RegionManager.h>
#include <CounterService.h>
#include <TFile.h>
#include <TH1F.h>
#include <THnSparse.h>
#include <cctype>
#include <cmath>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <functional>
#include <util.h>
#include <plots.h>
#include <string>
#include <vector>
#include <sstream>

/**
 * @brief Construct a new NDHistogramManager object
 */
NDHistogramManager::NDHistogramManager(IConfigurationProvider const& configProvider) {}

void NDHistogramManager::bindToRegionManager(RegionManager *rm) {
  regionManager_m = rm;
}

struct ColumnCache {
  ROOT::RDF::RNode& df;
  std::vector<std::string> names;
  std::unordered_map<std::string, std::string> types;

  explicit ColumnCache(ROOT::RDF::RNode& node)
      : df(node), names(node.GetColumnNames()) {}

  bool Has(const std::string& name) const {
    return std::find(names.begin(), names.end(), name) != names.end();
  }

  void Refresh() {
    names = df.GetColumnNames();
    types.clear();
  }

  std::string GetType(const std::string& name) {
    auto it = types.find(name);
    if (it != types.end()) {
      return it->second;
    }
    auto type = df.GetColumnType(name);
    types.emplace(name, type);
    return type;
  }
};

namespace {

enum class NDHistogramTypeToken {
  Bool,
  Float,
  Double,
  Int,
  UInt,
  Short,
  UShort,
  Char,
  UChar,
  Long64,
  ULong64,
  Unsupported,
};

std::string removeNDHistogramTypeWhitespace(std::string value) {
  value.erase(std::remove_if(value.begin(), value.end(),
                             [](unsigned char c) { return std::isspace(c) != 0; }),
              value.end());
  return value;
}

bool isNDHistogramRVecType(const std::string &columnType) {
  return columnType.find("RVec<") != std::string::npos;
}

std::string extractNDHistogramRVecElementType(const std::string &columnType) {
  const auto rvecPos = columnType.find("RVec<");
  if (rvecPos == std::string::npos) {
    return columnType;
  }

  const auto start = columnType.find('<', rvecPos);
  const auto end = columnType.rfind('>');
  if (start == std::string::npos || end == std::string::npos || end <= start + 1) {
    return columnType;
  }
  return columnType.substr(start + 1, end - start - 1);
}

NDHistogramTypeToken tokenizeNDHistogramType(const std::string &typeName) {
  const auto normalized = removeNDHistogramTypeWhitespace(typeName);

  if (normalized == "Bool_t" || normalized == "bool") {
    return NDHistogramTypeToken::Bool;
  }
  if (normalized == "Float_t" || normalized == "float") {
    return NDHistogramTypeToken::Float;
  }
  if (normalized == "Double_t" || normalized == "double") {
    return NDHistogramTypeToken::Double;
  }
  if (normalized == "Int_t" || normalized == "int") {
    return NDHistogramTypeToken::Int;
  }
  if (normalized == "UInt_t" || normalized == "unsignedint") {
    return NDHistogramTypeToken::UInt;
  }
  if (normalized == "Short_t" || normalized == "short") {
    return NDHistogramTypeToken::Short;
  }
  if (normalized == "UShort_t" || normalized == "unsignedshort") {
    return NDHistogramTypeToken::UShort;
  }
  if (normalized == "Char_t" || normalized == "char") {
    return NDHistogramTypeToken::Char;
  }
  if (normalized == "UChar_t" || normalized == "unsignedchar") {
    return NDHistogramTypeToken::UChar;
  }
  if (normalized == "Long64_t" || normalized == "longlong") {
    return NDHistogramTypeToken::Long64;
  }
  if (normalized == "ULong64_t" || normalized == "unsignedlonglong") {
    return NDHistogramTypeToken::ULong64;
  }

  return NDHistogramTypeToken::Unsupported;
}

std::string buildScalarBroadcastExpression(const std::string &referenceColumn,
                                           const std::string &scalarColumn) {
  return "ROOT::VecOps::RVec<Float_t>(" + referenceColumn +
         ".size(), static_cast<Float_t>(" + scalarColumn + "))";
}

std::string buildRegionMembershipBoolExpression(
    const std::vector<std::string> &filterChain) {
  std::string expr = filterChain[0];
  for (std::size_t index = 1; index < filterChain.size(); ++index) {
    expr += " && " + filterChain[index];
  }
  return "static_cast<bool>(" + expr + ")";
}

template <typename ReferenceT, typename ScalarT>
ROOT::VecOps::RVec<Float_t> broadcastScalarToReference(
    const ROOT::VecOps::RVec<ReferenceT> &reference, ScalarT value) {
  return ROOT::VecOps::RVec<Float_t>(reference.size(),
                                     static_cast<Float_t>(value));
}

template <typename ReferenceT, typename ScalarT>
ROOT::RDF::RNode defineBroadcastFloatVector(ROOT::RDF::RNode df,
                                            const std::string &name,
                                            const std::string &referenceColumn,
                                            const std::string &scalarColumn) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<ReferenceT> &reference,
         ScalarT value) -> ROOT::VecOps::RVec<Float_t> {
        return broadcastScalarToReference(reference, value);
      },
      {referenceColumn, scalarColumn});
}

template <typename ReferenceT>
ROOT::RDF::RNode dispatchBroadcastFloatVectorByScalar(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &referenceColumn, const std::string &scalarColumn,
    NDHistogramTypeToken scalarType) {
  switch (scalarType) {
  case NDHistogramTypeToken::Bool:
    return defineBroadcastFloatVector<ReferenceT, Bool_t>(df, name, referenceColumn,
                                                          scalarColumn);
  case NDHistogramTypeToken::Float:
    return defineBroadcastFloatVector<ReferenceT, Float_t>(df, name, referenceColumn,
                                                           scalarColumn);
  case NDHistogramTypeToken::Double:
    return defineBroadcastFloatVector<ReferenceT, Double_t>(df, name, referenceColumn,
                                                            scalarColumn);
  case NDHistogramTypeToken::Int:
    return defineBroadcastFloatVector<ReferenceT, Int_t>(df, name, referenceColumn,
                                                         scalarColumn);
  case NDHistogramTypeToken::UInt:
    return defineBroadcastFloatVector<ReferenceT, UInt_t>(df, name, referenceColumn,
                                                          scalarColumn);
  case NDHistogramTypeToken::Short:
    return defineBroadcastFloatVector<ReferenceT, Short_t>(df, name, referenceColumn,
                                                           scalarColumn);
  case NDHistogramTypeToken::UShort:
    return defineBroadcastFloatVector<ReferenceT, UShort_t>(df, name, referenceColumn,
                                                            scalarColumn);
  case NDHistogramTypeToken::Char:
    return defineBroadcastFloatVector<ReferenceT, Char_t>(df, name, referenceColumn,
                                                          scalarColumn);
  case NDHistogramTypeToken::UChar:
    return defineBroadcastFloatVector<ReferenceT, UChar_t>(df, name, referenceColumn,
                                                           scalarColumn);
  case NDHistogramTypeToken::Long64:
    return defineBroadcastFloatVector<ReferenceT, Long64_t>(df, name, referenceColumn,
                                                            scalarColumn);
  case NDHistogramTypeToken::ULong64:
    return defineBroadcastFloatVector<ReferenceT, ULong64_t>(df, name,
                                                             referenceColumn,
                                                             scalarColumn);
  case NDHistogramTypeToken::Unsupported:
    throw std::runtime_error("NDHistogramManager: unsupported scalar type in compiled broadcast path");
  }

  throw std::runtime_error("NDHistogramManager: unreachable scalar broadcast dispatch");
}

ROOT::RDF::RNode dispatchBroadcastFloatVector(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &referenceColumn, NDHistogramTypeToken referenceType,
    const std::string &scalarColumn, NDHistogramTypeToken scalarType) {
  switch (referenceType) {
  case NDHistogramTypeToken::Bool:
    return dispatchBroadcastFloatVectorByScalar<Bool_t>(df, name, referenceColumn,
                                                        scalarColumn, scalarType);
  case NDHistogramTypeToken::Float:
    return dispatchBroadcastFloatVectorByScalar<Float_t>(df, name, referenceColumn,
                                                         scalarColumn, scalarType);
  case NDHistogramTypeToken::Double:
    return dispatchBroadcastFloatVectorByScalar<Double_t>(df, name, referenceColumn,
                                                          scalarColumn, scalarType);
  case NDHistogramTypeToken::Int:
    return dispatchBroadcastFloatVectorByScalar<Int_t>(df, name, referenceColumn,
                                                       scalarColumn, scalarType);
  case NDHistogramTypeToken::UInt:
    return dispatchBroadcastFloatVectorByScalar<UInt_t>(df, name, referenceColumn,
                                                        scalarColumn, scalarType);
  case NDHistogramTypeToken::Short:
    return dispatchBroadcastFloatVectorByScalar<Short_t>(df, name, referenceColumn,
                                                         scalarColumn, scalarType);
  case NDHistogramTypeToken::UShort:
    return dispatchBroadcastFloatVectorByScalar<UShort_t>(df, name, referenceColumn,
                                                          scalarColumn, scalarType);
  case NDHistogramTypeToken::Char:
    return dispatchBroadcastFloatVectorByScalar<Char_t>(df, name, referenceColumn,
                                                        scalarColumn, scalarType);
  case NDHistogramTypeToken::UChar:
    return dispatchBroadcastFloatVectorByScalar<UChar_t>(df, name, referenceColumn,
                                                         scalarColumn, scalarType);
  case NDHistogramTypeToken::Long64:
    return dispatchBroadcastFloatVectorByScalar<Long64_t>(df, name, referenceColumn,
                                                          scalarColumn, scalarType);
  case NDHistogramTypeToken::ULong64:
    return dispatchBroadcastFloatVectorByScalar<ULong64_t>(df, name,
                                                           referenceColumn,
                                                           scalarColumn,
                                                           scalarType);
  case NDHistogramTypeToken::Unsupported:
    throw std::runtime_error("NDHistogramManager: unsupported reference vector type in compiled broadcast path");
  }

  throw std::runtime_error("NDHistogramManager: unreachable reference broadcast dispatch");
}

template <typename InputT>
ROOT::RDF::RNode defineRegionChainInit(ROOT::RDF::RNode df,
                                       const std::string &name,
                                       const std::string &column) {
  return df.Define(
      name,
      [](InputT value) { return static_cast<bool>(value); },
      {column});
}

template <typename InputT>
ROOT::RDF::RNode defineRegionChainAppend(ROOT::RDF::RNode df,
                                         const std::string &name,
                                         const std::string &currentColumn,
                                         const std::string &nextColumn) {
  return df.Define(
      name,
      [](bool current, InputT value) {
        return current && static_cast<bool>(value);
      },
      {currentColumn, nextColumn});
}

ROOT::RDF::RNode dispatchRegionChainInit(ROOT::RDF::RNode df,
                                         const std::string &name,
                                         const std::string &column,
                                         NDHistogramTypeToken inputType) {
  switch (inputType) {
  case NDHistogramTypeToken::Bool:
    return defineRegionChainInit<Bool_t>(df, name, column);
  case NDHistogramTypeToken::Float:
    return defineRegionChainInit<Float_t>(df, name, column);
  case NDHistogramTypeToken::Double:
    return defineRegionChainInit<Double_t>(df, name, column);
  case NDHistogramTypeToken::Int:
    return defineRegionChainInit<Int_t>(df, name, column);
  case NDHistogramTypeToken::UInt:
    return defineRegionChainInit<UInt_t>(df, name, column);
  case NDHistogramTypeToken::Short:
    return defineRegionChainInit<Short_t>(df, name, column);
  case NDHistogramTypeToken::UShort:
    return defineRegionChainInit<UShort_t>(df, name, column);
  case NDHistogramTypeToken::Char:
    return defineRegionChainInit<Char_t>(df, name, column);
  case NDHistogramTypeToken::UChar:
    return defineRegionChainInit<UChar_t>(df, name, column);
  case NDHistogramTypeToken::Long64:
    return defineRegionChainInit<Long64_t>(df, name, column);
  case NDHistogramTypeToken::ULong64:
    return defineRegionChainInit<ULong64_t>(df, name, column);
  case NDHistogramTypeToken::Unsupported:
    throw std::runtime_error("NDHistogramManager: unsupported region filter type in compiled path");
  }

  throw std::runtime_error("NDHistogramManager: unreachable region chain init dispatch");
}

ROOT::RDF::RNode dispatchRegionChainAppend(ROOT::RDF::RNode df,
                                           const std::string &name,
                                           const std::string &currentColumn,
                                           const std::string &nextColumn,
                                           NDHistogramTypeToken inputType) {
  switch (inputType) {
  case NDHistogramTypeToken::Bool:
    return defineRegionChainAppend<Bool_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Float:
    return defineRegionChainAppend<Float_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Double:
    return defineRegionChainAppend<Double_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Int:
    return defineRegionChainAppend<Int_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::UInt:
    return defineRegionChainAppend<UInt_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Short:
    return defineRegionChainAppend<Short_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::UShort:
    return defineRegionChainAppend<UShort_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Char:
    return defineRegionChainAppend<Char_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::UChar:
    return defineRegionChainAppend<UChar_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Long64:
    return defineRegionChainAppend<Long64_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::ULong64:
    return defineRegionChainAppend<ULong64_t>(df, name, currentColumn, nextColumn);
  case NDHistogramTypeToken::Unsupported:
    throw std::runtime_error("NDHistogramManager: unsupported region filter type in compiled path");
  }

  throw std::runtime_error("NDHistogramManager: unreachable region chain append dispatch");
}

ROOT::RDF::RNode defineCompiledRegionMembershipChain(
    ROOT::RDF::RNode df, const std::string &outputColumn,
    const std::vector<std::string> &filterChain,
    const std::vector<NDHistogramTypeToken> &filterTypes) {
  if (filterChain.empty()) {
    throw std::runtime_error("NDHistogramManager: empty region filter chain");
  }

  std::string currentName =
      (filterChain.size() == 1) ? outputColumn : outputColumn + "__chain_0";
  df = dispatchRegionChainInit(df, currentName, filterChain[0], filterTypes[0]);
  for (std::size_t index = 1; index < filterChain.size(); ++index) {
    const std::string nextName =
        (index + 1 == filterChain.size())
            ? outputColumn
            : outputColumn + "__chain_" + std::to_string(index);
    df = dispatchRegionChainAppend(df, nextName, currentName, filterChain[index],
                                   filterTypes[index]);
    currentName = nextName;
  }
  return df;
}

} // namespace

// Helper function to handle per-axis logic for varVector and DefineVector
static void HandleAxisVarVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    ColumnCache& cache,
    const std::string& variable,
    bool hasSystematic,
    bool hasMultiFill,
    const std::vector<std::string>& allSystematics,
    std::vector<std::string>& varVector,
    bool isWeight=false,
    std::vector<std::string> baseVariables={},
    const std::string& uniqueTag=std::string()) {

    const std::string refVector = baseVariables.empty() ? std::string() : baseVariables.front();
    const std::string tagSuffix = uniqueTag.empty() ? std::string() : "_" + uniqueTag;
    auto ensureExpandedVector = [&](const std::string& column) -> std::string {
      if (refVector.empty()) {
        return column;
      }
      const std::string colType = cache.GetType(column);
      if (colType.find("RVec") != std::string::npos) {
        return column;
      }
      const std::string expandedName = column + "_FillRVec_" + refVector + tagSuffix;
      if (!cache.Has(expandedName)) {
        const std::string refType = cache.GetType(refVector);
        const auto refToken =
            tokenizeNDHistogramType(extractNDHistogramRVecElementType(refType));
        const auto colToken = tokenizeNDHistogramType(colType);
        const bool useFallback = refToken == NDHistogramTypeToken::Unsupported ||
                                 colToken == NDHistogramTypeToken::Unsupported;
        if (useFallback) {
          df = df.Define(expandedName,
                         buildScalarBroadcastExpression(refVector, column));
        } else {
          df = dispatchBroadcastFloatVector(df, expandedName, refVector, refToken,
                                            column, colToken);
        }
        dataManager_m->setDataFrame(df);
        cache.Refresh();
      }
      return expandedName;
    };

    if (hasSystematic) {
        std::vector<std::string> systematicVariations;
        for (const auto& syst : allSystematics) {
        std::string columnName = systematicManager_m->getVariationColumnName(variable, syst);
        // A registered family can lack one or more columns in a particular
        // input. When filters force systematic packing for otherwise nominal
        // histogram axes, repeat the available nominal value for that slot.
        if (!cache.Has(columnName)) {
          columnName = variable;
        }
        if (!isWeight && !refVector.empty()) {
          columnName = ensureExpandedVector(columnName);
        }
        systematicVariations.push_back(columnName);
        }
        const std::string systVectorName = variable + "_systVector" + tagSuffix;
        if (!cache.Has(systVectorName)) {
            dataManager_m->DefineVectorNominal(systVectorName, systematicVariations,
                                               "Float_t", *systematicManager_m);
          df = dataManager_m->getDataFrame();
          cache.Refresh();
        }
        varVector.push_back(systVectorName);
    } else {
        if (hasMultiFill) {
            const std::string colType = cache.GetType(variable);
            if (colType.find("RVec") == std::string::npos && !refVector.empty()) {
              // Scalar variable in multi-fill context: expand to match refVector.
              varVector.push_back(ensureExpandedVector(variable));
            } else {
              // Already an RVec, or no refVector to expand against: use as-is.
              varVector.push_back(variable);
            }
        } else {
        if (!refVector.empty()) {
          varVector.push_back(ensureExpandedVector(variable));
        } else {
          const std::string rvecName = variable + "_RVec" + tagSuffix;
          if (!cache.Has(rvecName)) {
            dataManager_m->DefineVectorNominal(rvecName, {variable}, "Float_t",
                                               *systematicManager_m);
            df = dataManager_m->getDataFrame();
            cache.Refresh();
          }
          varVector.push_back(rvecName);
        }
        }
    }
}

static std::string buildSystematicVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    const std::vector<std::string>& systList,
    ColumnCache& cache) {
  const std::string baseName = "SystematicCounter";
  std::vector<std::string> systBranches;
  systBranches.reserve(systList.size());

  bool definedAny = false;
  for (size_t i = 0; i < systList.size(); ++i) {
    const auto &syst = systList[i];
    const std::string branchName = (syst == "Nominal") ? baseName : baseName + "_" + syst;
    if (!cache.Has(branchName)) {
      const int index = static_cast<int>(i);
      dataManager_m->DefinePerSample(
          branchName,
          [index](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
            return index;
          });
      definedAny = true;
    }
  }
  if (definedAny) {
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }

  for (const auto& syst : systList) {
    if (syst == "Nominal") {
      systBranches.push_back(baseName);
    } else {
      systBranches.push_back(baseName + "_" + syst);
    }
  }

  // A nominal-only booking must not reuse a previously materialised full
  // systematic vector: its longer size would be inconsistent with the
  // one-bin systematic axis.  Keep a dedicated vector for the nominal case.
  const bool nominalOnly =
      systList.size() == 1 && systList.front() == "Nominal";
  const std::string systVectorName =
      nominalOnly ? baseName + "_nominal_systVector"
                  : baseName + "_systVector";
  if (!cache.Has(systVectorName)) {
    dataManager_m->DefineVectorNominal(systVectorName, systBranches, "Float_t",
                                       *systematicManager_m);
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }
  return systVectorName;
}

static std::string buildSelectionMaskVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    const std::vector<std::string>& systList,
    ColumnCache& cache) {
  const std::string defaultMaskName = "__rdf_filter_mask_default_true";
  if (!cache.Has(defaultMaskName)) {
    df = df.Define(defaultMaskName, []() -> Bool_t { return true; });
    dataManager_m->setDataFrame(df);
    cache.Refresh();
  }

  std::vector<std::string> maskColumns;
  maskColumns.reserve(systList.size());
  for (const auto& label : systList) {
    const auto registeredMask =
        systematicManager_m->getSelectionMaskColumn(label);
    maskColumns.push_back(!registeredMask.empty() && cache.Has(registeredMask)
                              ? registeredMask
                              : defaultMaskName);
  }

  std::string signature;
  for (const auto& label : systList) {
    signature += std::to_string(label.size()) + ":" + label;
  }
  const std::string vectorName =
      "__rdf_filter_mask_systVector_" +
      std::to_string(std::hash<std::string>{}(signature));
  if (!cache.Has(vectorName)) {
    dataManager_m->DefineVectorNominal(vectorName, maskColumns, "Bool_t",
                                       *systematicManager_m);
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }
  return vectorName;
}

static bool HasSystematicColumns(
    ColumnCache& cache,
    ISystematicManager* systematicManager_m,
    const std::string& variable,
    const std::vector<std::string>& systList) {
  for (const auto& syst : systList) {
    if (syst == "Nominal") {
      continue;
    }
    if (!systematicManager_m->isVariableAffectedBySystematic(variable, syst)) {
      continue;
    }
    const std::string columnName = systematicManager_m->getVariationColumnName(variable, syst);
    if (cache.Has(columnName)) {
      return true;
    }
  }
  return false;
}

/**
 * @brief Automatically detect and register systematic variations from the
 *        current dataframe columns before the first histogram booking.
 *
 * Scans all available dataframe columns for `baseVar_systUp`/`baseVar_systDown`
 * pairs and registers them with the SystematicManager.  Incomplete pairs are
 * reported as warnings via the logger.
 *
 * This is a no-op if the canonical SystematicCounter branch has already been
 * materialised (i.e. makeSystList() was already called), because the cached
 * syst list cannot be extended after the fact.
 */
void NDHistogramManager::ensureSystematicsAutoRegistered() {
  if (!systematicManager_m || !dataManager_m) {
    return;
  }
  // If makeSystList() has already been called the syst list is cached; adding
  // new systematics would be silently ignored.  Skip to avoid confusion.
  if (systematicManager_m->isBranchNameMaterialized(
          ISystematicManager::CANONICAL_SYST_BRANCH_NAME)) {
    return;
  }

  auto df = dataManager_m->getDataFrame();
  const auto rawColNames = df.GetColumnNames();
  const std::vector<std::string> columnNames(rawColNames.begin(), rawColNames.end());

  const SystematicValidationResult result =
      systematicManager_m->autoRegisterSystematics(columnNames);

  if (!logger_m) {
    return;
  }

  if (!result.registered.empty()) {
    std::stringstream msg;
    msg << "NDHistogramManager: Auto-detected "
        << result.registered.size() << " systematic variation(s): ";
    for (const auto &p : result.registered) {
      msg << p.second << "(" << p.first << ") ";
    }
    logger_m->log(ILogger::Level::Info, msg.str());
  }
  for (const auto &col : result.missingDown) {
    logger_m->log(ILogger::Level::Warn,
                  "NDHistogramManager: Systematic column '" + col +
                  "' has no corresponding Down variation; this variation will be excluded.");
  }
  for (const auto &col : result.missingUp) {
    logger_m->log(ILogger::Level::Warn,
                  "NDHistogramManager: Systematic column '" + col +
                  "' has no corresponding Up variation; this variation will be excluded.");
  }
}

void NDHistogramManager::BookSingleHistogram(
    histInfo &info, // histogram info (name, bins, lowerBound, upperBound)
    selectionInfo &&sampleCategoryInfo, // selection info (variable, bins, lowerBound, upperBound)
    selectionInfo &&controlRegionInfo, // selection info (variable, bins, lowerBound, upperBound)
    selectionInfo &&channelInfo, // selection info (variable, bins, lowerBound, upperBound)
    std::string suffix){ // suffix to append to histogram names

  if (dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  std::vector<std::string> systList{"Nominal"};
  if (histogramSystematics_m) {
    ensureSystematicsAutoRegistered();
    systList =
        systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  }
  BookSingleHistogramWithSystList(info,
                                  std::move(sampleCategoryInfo),
                                  std::move(controlRegionInfo),
                                  std::move(channelInfo),
                                  std::move(suffix),
                                  systList);
}

void NDHistogramManager::BookSingleHistogramWithSystList(
    histInfo &info,
    selectionInfo &&sampleCategoryInfo,
    selectionInfo &&controlRegionInfo,
    selectionInfo &&channelInfo,
    std::string suffix,
    const std::vector<std::string> &systList,
    const std::string &baseRefVector) {

  // get the dataframe
  if(dataManager_m == nullptr) {
    throw std::runtime_error("NDHistogramManager::BookSingleHistogram: DataManager not set");
  }

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();
  ColumnCache cache(df);

  // set the basic information
  histFillInfo fillInfo = histFillInfo();

  fillInfo.name = info.name() + "_" + suffix;
  fillInfo.title = info.name() + " " + suffix;
  fillInfo.nSlots = df.GetNSlots();
  // Systematic axis will be inserted between sampleCategory and the histogram variable.
  fillInfo.nbins = {channelInfo.bins(), controlRegionInfo.bins(), sampleCategoryInfo.bins(), 1, info.bins()};
  fillInfo.xmin = {channelInfo.lowerBound(), controlRegionInfo.lowerBound(), sampleCategoryInfo.lowerBound(), 0.0, info.lowerBound()};
  fillInfo.xmax = {channelInfo.upperBound(), controlRegionInfo.upperBound(), sampleCategoryInfo.upperBound(), 1.0, info.upperBound()};

  // define zero vector if needed
  if(!cache.Has("zero__")) {
    dataManager_m->Define("zero__", []() -> Float_t { return 0.0f; }, {}, *systematicManager_m);
    df = dataManager_m->getDataFrame();
    cache.Refresh();
  }


  // determine if the variables are scalars or vectors
  const std::string channelType = cache.GetType(channelInfo.variable());
  const std::string controlRegionType = cache.GetType(controlRegionInfo.variable());
  const std::string sampleCategoryType = cache.GetType(sampleCategoryInfo.variable());

  if(channelType.find("RVec") != std::string::npos) {
    fillInfo.channel_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  if(controlRegionType.find("RVec") != std::string::npos) {
    fillInfo.controlRegion_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  if(sampleCategoryType.find("RVec") != std::string::npos) {
    fillInfo.sampleCategory_hasMultiFill = true; 
    fillInfo.hasMultiFill = true;
  }

  const std::string weightType = cache.GetType(info.weight());
  if(weightType.find("RVec") != std::string::npos) {
    fillInfo.weight_hasMultiFill = true;
    fillInfo.hasMultiFill = true;
  }

  // determine if the variables have systematic variations
  if(systematicManager_m->getSystematicsForVariable(channelInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, channelInfo.variable(), systList)) {
    fillInfo.channel_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(controlRegionInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, controlRegionInfo.variable(), systList)) {
    fillInfo.controlRegion_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(sampleCategoryInfo.variable()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, sampleCategoryInfo.variable(), systList)) {
    fillInfo.sampleCategory_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  if(systematicManager_m->getSystematicsForVariable(info.weight()).size() > 0 &&
     HasSystematicColumns(cache, systematicManager_m, info.weight(), systList)) {
    fillInfo.weight_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  // Selection migrations require every histogram axis to be represented once
  // per systematic, even when that axis itself is unaffected. Its nominal
  // value is then repeated into the varied bins, while the selection mask
  // decides whether each bin receives this event.
  const bool hasSelectionMasks = systematicManager_m->hasSelectionMasks();
  if (hasSelectionMasks) {
    fillInfo.channel_hasSystematic = true;
    fillInfo.controlRegion_hasSystematic = true;
    fillInfo.sampleCategory_hasSystematic = true;
    fillInfo.weight_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  // For now, we don't support multi-fill for systematic variations
  fillInfo.systematic_hasMultiFill = false;
  
  fillInfo.nbins[3] = static_cast<Int_t>(systList.size());
  fillInfo.xmax[3] = static_cast<Double_t>(systList.size());

  std::vector<std::string> varVector;
  std::string uniqueTag = fillInfo.name;
  std::replace(uniqueTag.begin(), uniqueTag.end(), ' ', '_');

  // We're going to fill for each systematic variation so we can reuse a lot of the same input vectors between the different histograms
  // But we will set the weights to 0 for the non-systematic variations
  std::vector<std::string> usedSystematics(systList.begin(), systList.end());

  // Build columns in the order expected by THnMulti::Exec and BHnMulti::Exec:
  // base values, weights, systematic indices, selection mask, sample category,
  // control region, channel, and per-systematic fill counts.
  //
  // When baseRefVector is non-empty (region-aware booking), scalar base
  // variables and their systematic variations are expanded to match the size
  // of the reference (the region membership RVec) so that multi-fill works
  // correctly without an extra event-loop pass.
  std::vector<std::string> baseValsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, info.variable(),
                      fillInfo.hasSystematic, fillInfo.hasMultiFill, usedSystematics,
                      baseValsVec, false,
                      baseRefVector.empty() ? std::vector<std::string>{}
                                            : std::vector<std::string>{baseRefVector},
                      uniqueTag);
  std::vector<std::string> baseWeightsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, info.weight(), fillInfo.weight_hasSystematic, fillInfo.weight_hasMultiFill, usedSystematics, baseWeightsVec, true, {baseValsVec.front()}, uniqueTag);

  const std::string systVectorName = buildSystematicVector(df, dataManager_m, systematicManager_m, systList, cache);
  const std::string selectionMaskVectorName =
      buildSelectionMaskVector(df, dataManager_m, systematicManager_m, systList,
                              cache);

  std::vector<std::string> sampleCategoryVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, sampleCategoryInfo.variable(), fillInfo.sampleCategory_hasSystematic, fillInfo.sampleCategory_hasMultiFill, usedSystematics, sampleCategoryVec, false, {baseValsVec.front()}, uniqueTag);
  std::vector<std::string> controlRegionVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, controlRegionInfo.variable(), fillInfo.controlRegion_hasSystematic, fillInfo.controlRegion_hasMultiFill, usedSystematics, controlRegionVec, false, {baseValsVec.front()}, uniqueTag);
  std::vector<std::string> channelVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, channelInfo.variable(), fillInfo.channel_hasSystematic, fillInfo.channel_hasMultiFill, usedSystematics, channelVec, false, {baseValsVec.front()}, uniqueTag);

  const std::string nFillsName = "SystematicCounter_nFills_" + baseValsVec.front() + "_" + uniqueTag;
  if (!cache.Has(nFillsName)) {
    dataManager_m->Define(
        nFillsName,
        [](const ROOT::VecOps::RVec<Float_t>& baseVals,
           const ROOT::VecOps::RVec<Float_t>& systVals) -> ROOT::VecOps::RVec<Int_t> {
          ROOT::VecOps::RVec<Int_t> out(systVals.size());
          const auto systSize = static_cast<Int_t>(systVals.size());
          Int_t fills = static_cast<Int_t>(baseVals.size());
          if (systSize > 0 && fills % systSize == 0) {
            fills = fills / systSize;
          }
          for (size_t i = 0; i < out.size(); ++i) {
            out[i] = fills;
          }
          return out;
        },
        {baseValsVec.front(), systVectorName},
        *systematicManager_m);
      df = dataManager_m->getDataFrame();
      cache.Refresh();
  }

  varVector = {
    baseValsVec.front(),
    baseWeightsVec.front(),
    systVectorName,
    selectionMaskVectorName,
    sampleCategoryVec.front(),
    controlRegionVec.front(),
    channelVec.front(),
    nFillsName
  };

  if (std::getenv("RDF_NDHIST_DEBUG") != nullptr) {
    std::cout << "[NDHistogramManager] Book " << fillInfo.name << " columns: ";
    for (const auto& name : varVector) {
      std::cout << name << " ";
    }
    std::cout << std::endl;
  }

  df = dataManager_m->getDataFrame();
  if (histogramBackend_m == "boost") {
    BHnMulti tempModel(fillInfo);
    histos_m.push_back(df.Book<
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Bool_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Int_t>>(
      std::move(tempModel), varVector));
  } else {
    THnMulti tempModel(fillInfo);
    histos_m.push_back(df.Book<
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Bool_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Float_t>,
      ROOT::VecOps::RVec<Int_t>>(
      std::move(tempModel), varVector));
  }
  histNodes_m.push_back(df);
}

void NDHistogramManager::bookND(std::vector<histInfo> &infos,
                                std::vector<selectionInfo> &selection,
                                const std::string &suffix,
                                std::vector<std::vector<std::string>> &allRegionNames) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("NDHistogramManager: DataManager or SystematicManager not set");
  }

  if (allRegionNames.empty()) {
    throw std::invalid_argument("NDHistogramManager: region names must not be empty");
  }

  if (infos.empty()) {
    return;
  }

  // Ensure we have channel/control/sample category selections (use defaults if missing)
  selectionInfo channelInfo = selection.size() > 0 ? selection[0] : selectionInfo();
  selectionInfo controlInfo = selection.size() > 1 ? selection[1] : selectionInfo();
  selectionInfo sampleInfo = selection.size() > 2 ? selection[2] : selectionInfo();

  // Normalize region names to match axes: channel, control, sample, systematic
  std::vector<std::vector<std::string>> normalizedRegionNames;
  normalizedRegionNames.reserve(4);
  normalizedRegionNames.emplace_back(allRegionNames.size() > 0 ? allRegionNames[0] : channelInfo.regions());
  normalizedRegionNames.emplace_back(allRegionNames.size() > 1 ? allRegionNames[1] : controlInfo.regions());
  normalizedRegionNames.emplace_back(allRegionNames.size() > 2 ? allRegionNames[2] : sampleInfo.regions());

  // Auto-detect systematic variations from dataframe columns before building
  // the syst list for the first time.
  // Append systematic axis info if not already appended.
  std::vector<std::string> systList{"Nominal"};
  if (histogramSystematics_m) {
    ensureSystematicsAutoRegistered();
    systList =
        systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  }
  if (allRegionNames.size() > 3) {
    for (size_t i = 3; i < allRegionNames.size(); ++i) {
      normalizedRegionNames.emplace_back(allRegionNames[i]);
    }
  }
  if (normalizedRegionNames.empty() || normalizedRegionNames.back() != systList) {
    normalizedRegionNames.emplace_back(systList);
  }
  allRegionNames = normalizedRegionNames;

  // Track booked infos and region names for the no-args saveHists() overload.
  trackedHistInfos_m.push_back(infos);
  trackedRegionNames_m = normalizedRegionNames;

  histos_m.reserve(histos_m.size() + infos.size());

  for (auto &info : infos) {
    BookSingleHistogramWithSystList(info,
                                    selectionInfo(sampleInfo),
                                    selectionInfo(controlInfo),
                                    selectionInfo(channelInfo),
                                    suffix,
                                    systList);
  }
}

void NDHistogramManager::saveHists() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  if (trackedHistInfos_m.empty()) {
    return;
  }
  SaveHists(trackedHistInfos_m, trackedRegionNames_m, *configManager_m);
}

void NDHistogramManager::saveHists(std::vector<std::vector<histInfo>> &fullHistList,
                                   std::vector<std::vector<std::string>> &allRegionNames) {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  SaveHists(fullHistList, allRegionNames, *configManager_m);
}

void NDHistogramManager::SaveHists(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames,
    const IConfigurationProvider &configProvider) {
  std::string fileName;
  if (metaSink_m) {
    fileName = metaSink_m->resolveOutputFile(configProvider, OutputChannel::Meta);
  }
  if (fileName.empty()) {
    fileName = configProvider.get("metaFile");
    if (fileName.empty()) {
      fileName = configProvider.get("saveFile");
    }
  }

  std::vector<std::string> allNames;
  std::vector<std::string> allVariables;
  std::vector<std::string> allLabels;
  std::vector<int> allBins;
  std::vector<float> allLowerBounds;
  std::vector<float> allUpperBounds;
  for (auto const &histList : fullHistList) {
    for (auto const &info : histList) {
      //std::cout << "Storing histogram: " << info.name() << std::endl;
      allNames.push_back(info.name());
      allVariables.push_back(info.variable());
      allLabels.push_back(info.label());
      allBins.push_back(info.bins());
      allLowerBounds.push_back(info.lowerBound());
      allUpperBounds.push_back(info.upperBound());
    }
  }

  // Open the meta file for update so we don't clobber histograms (e.g. counters)
  // written by Analyzer/CounterService earlier.
  TFile saveFile(fileName.c_str(), "UPDATE");

  std::vector<Int_t> commonAxisSize = {};
  for (const auto &regionNameList : allRegionNames) {
    commonAxisSize.push_back(regionNameList.size());
  }

  // Find sample categories represented by this input across all booked
  // histograms.  A category can be empty for an individual observable (for
  // example, because its value is outside that histogram's range), so use the
  // union across histograms before creating the complete output schema.
  std::unordered_set<std::size_t> populatedSampleCategories;
  if (allRegionNames.size() >= 3) {
    for (auto &histogramResult : histos_m) {
      auto hist = histogramResult.GetPtr();
      if (hist->GetNdimensions() <= 2) {
        continue;
      }

      std::vector<Int_t> indices(hist->GetNdimensions());
      for (Int_t i = 0; i < hist->GetNbins(); ++i) {
        const Float_t content = hist->GetBinContent(i, indices.data());
        const Float_t error = hist->GetBinError2(i);
        if (content == 0 && error == 0) {
          continue;
        }

        const Int_t categoryBin = indices[2];
        if (categoryBin > 0 &&
            categoryBin <= static_cast<Int_t>(allRegionNames[2].size())) {
          populatedSampleCategories.insert(
              static_cast<std::size_t>(categoryBin - 1));
        }
      }
    }
  }

  // Pre-build directory names matching the extraction code's dirName format.
  // dirName = channel/control/category  (from indices[0], indices[1], indices[2]).
  std::unordered_set<std::string> dirSet;
  if (allRegionNames.size() >= 3) {
    for (size_t ci = 0; ci < allRegionNames[0].size(); ++ci) {
      for (size_t si = 0; si < allRegionNames[1].size(); ++si) {
        for (size_t mi = 0; mi < allRegionNames[2].size(); ++mi) {
          if (populatedSampleCategories.count(mi) == 0) {
            continue;
          }
          dirSet.insert(allRegionNames[0][ci] + "/" +
                        allRegionNames[1][si] + "/" +
                        allRegionNames[2][mi]);
        }
      }
    }
  }

  // Create all directories once, before extracting any histogram data.
  // ROOT's mkdir handles nested paths: the first '/' is replaced with '_' to
  // produce a flat top-level name, while remaining slashes create subdirs.
  for (const auto &dirName : dirSet) {
    std::string newDir(dirName);
    {
      const size_t firstSlash = newDir.find('/');
      if (firstSlash != std::string::npos) {
        newDir[firstSlash] = '_';
      }
    }
    saveFile.cd();
    if (!newDir.empty()) {
      saveFile.mkdir(newDir.c_str());
    }
  }

  std::cout << "Processing " << histos_m.size() << " histograms for saving..." << std::endl;
  int histIndex = 0;
  for (auto &histo_m : histos_m) {
    // Local map — only holds TH1Fs for the current config histogram.
    // Written and discarded before processing the next histogram.
    std::unordered_map<std::string, TH1F> histMap;

    auto hist = histo_m.GetPtr();
    const Int_t currentHistogramSize = hist->GetNbins();
    const Int_t dim = hist->GetNdimensions();
    std::vector<Int_t> indices(dim);

    // Persist the booked schema even when this sample has no entries in the
    // observable range (e.g. a high-pT sample in a low-pT validation plot).
    // Limit systematic names to the axes actually booked for this histogram.
    if (allRegionNames.size() >= 4 && dim == 5) {
      for (const auto &dirName : dirSet) {
        const auto nSystematics = std::min(
            allRegionNames[3].size(),
            static_cast<std::size_t>(hist->GetAxis(3)->GetNbins()));
        for (std::size_t s = 0; s < nSystematics; ++s) {
          std::string name = allNames[histIndex];
          if (allRegionNames[3][s] != "Nominal") {
            name += "_" + allRegionNames[3][s];
          }
          auto &output = histMap[dirName + "/" + name];
          output.SetNameTitle(name.c_str(),
                             (allNames[histIndex] + ";" + allNames[histIndex] +
                              ";Counts").c_str());
          output.SetBins(allBins[histIndex], allLowerBounds[histIndex],
                         allUpperBounds[histIndex]);
          output.Sumw2();
          output.SetDirectory(nullptr);
        }
      }
    }
      
    std::string histName = allNames[histIndex];
    for (int i = 0; i < currentHistogramSize; i++) {
      Float_t content = hist->GetBinContent(i, indices.data());
      Float_t error = hist->GetBinError2(i);
      if (content == 0 && error == 0) {
        continue;
      }

      // Reset histName for each bin — it may be extended below with a
      // systematic suffix and must not accumulate suffixes across bins.
      histName = allNames[histIndex];

      std::string dirName = "";
      const Int_t regionAxes = std::min(static_cast<Int_t>(allRegionNames.size()), dim - 1);
      const Int_t size = regionAxes - 2;
      for (int j = 0; j < size; j++) {
        if (indices[j] > 0 && j < static_cast<Int_t>(allRegionNames.size()) &&
            indices[j] - 1 < static_cast<Int_t>(allRegionNames[j].size())) {
          dirName += allRegionNames[j][indices[j] - 1] + "/";
        }
      }

      if (regionAxes - 2 >= 0 &&
          regionAxes - 2 < static_cast<Int_t>(allRegionNames.size()) &&
          indices[regionAxes - 2] > 0 &&
          indices[regionAxes - 2] - 1 < static_cast<Int_t>(allRegionNames[regionAxes - 2].size())) {
        dirName += allRegionNames[regionAxes - 2][indices[regionAxes - 2] - 1];
      }

      
      
      if (regionAxes - 1 >= 0 &&
          regionAxes - 1 < static_cast<Int_t>(allRegionNames.size()) &&
          indices[regionAxes - 1] > 0 &&
          indices[regionAxes - 1] - 1 < static_cast<Int_t>(allRegionNames[regionAxes - 1].size())) {
        if (allRegionNames[regionAxes - 1][indices[regionAxes - 1] - 1] != "Nominal") {
          histName += "_" + allRegionNames[regionAxes - 1][indices[regionAxes - 1] - 1];
        }
      }

      const std::string key = dirName + "/" + histName;
      if (histMap.count(key) == 0) {
        histMap[key] =
            TH1F(histName.c_str(),
                 (allNames[histIndex] + ";" + allNames[histIndex] +
                  ";Counts").c_str(),
                 allBins[histIndex], allLowerBounds[histIndex],
                 allUpperBounds[histIndex]);
      }

      const Int_t valueAxisIndex = regionAxes;
      if (valueAxisIndex < dim) {
        histMap[key].SetBinContent(indices[valueAxisIndex], content);
        histMap[key].SetBinError(indices[valueAxisIndex], std::sqrt(error));
      }
    }

    // Write TH1Fs for this config histogram immediately, then discard the map.
    for (auto &pair : histMap) {
      const std::string &fullKey = pair.first;
      // Key format: "channel/control/category/histname" — dirName has two
      // slashes (channel/control/category).  Extract the directory part by
      // finding the LAST slash (before the histname).
      const size_t lastSlash = fullKey.rfind('/');
      std::string dirPart;
      if (lastSlash != std::string::npos) {
        dirPart = fullKey.substr(0, lastSlash);
      }
      std::string newDir(dirPart);
      {
        const size_t midSlash = newDir.find('/');
        if (midSlash != std::string::npos) {
          newDir[midSlash] = '_';
        }
      }
      saveFile.cd();
      if (!newDir.empty()) {
        saveFile.cd(newDir.c_str());
      }
      pair.second.Write();
      // Prevent the TH1F from remaining associated with the ROOT directory.
      // Without this, the TH1F destructor (when histMap is cleared) may
      // trigger a double-free with the TFile's directory cleanup.
      pair.second.SetDirectory(nullptr);
    }

    histIndex++;
  }
}



/**
 * @brief Get the vector of histogram result pointers
 * @return Reference to the vector of RResultPtr<THnSparseF>
 */
std::vector<ROOT::Detail::RDF::RResultPtr<THnSparseF>> &
NDHistogramManager::GetHistos() {
  return histos_m;
}

/**
 * @brief Clear all stored histograms
 */
void NDHistogramManager::Clear() {
  histos_m.clear();
  histNodes_m.clear();
}

/**
 * @brief Setup the manager from a configuration file
 */
void NDHistogramManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigManager not set");
  }

  // Histogram systematic variations are enabled by default for backwards
  // compatibility.  Analyses that only want nominal plots can explicitly set
  // histogramSystematics: false without changing their central event weights.
  const std::string histogramSystematics =
      configManager_m->get("histogramSystematics");
  histogramSystematics_m = true;
  if (!histogramSystematics.empty()) {
    std::string normalized = histogramSystematics;
    std::transform(normalized.begin(), normalized.end(), normalized.begin(),
                   [](unsigned char c) {
                     return static_cast<char>(std::tolower(c));
                   });
    if (normalized == "true" || normalized == "1" || normalized == "yes") {
      histogramSystematics_m = true;
    } else if (normalized == "false" || normalized == "0" ||
               normalized == "no") {
      histogramSystematics_m = false;
    } else {
      throw std::runtime_error(
          "NDHistogramManager: invalid histogramSystematics value '" +
          histogramSystematics + "'. Valid values are true or false.");
    }
  }

  // Read optional histogram backend selection (default: "root", option: "boost")
  const std::string backend = configManager_m->get("histogramBackend");
  if (!backend.empty()) {
    if (backend != "root" && backend != "boost") {
      throw std::runtime_error(
          "NDHistogramManager: invalid histogramBackend '" + backend +
          "'. Valid values are 'root' (default) or 'boost'.");
    }
    histogramBackend_m = backend;
  }

  // Parse histogram configuration if present
  std::string histogramConfigFile = configManager_m->get("histogramConfig");
  if (histogramConfigFile.empty()) {
    // No histogram config file specified - this is fine
    if (logger_m) {
      logger_m->log(ILogger::Level::Info, "NDHistogramManager: No histogramConfig specified, config-driven histograms disabled");
    }
    return;
  }

  try {
    // Parse the histogram configuration file
    // Required keys: name, variable, weight, bins, lowerBound, upperBound
    // Optional keys: label, suffix, channelVariable, channelBins, channelLowerBound, channelUpperBound, 
    //                channelRegions, controlRegionVariable, controlRegionBins, controlRegionLowerBound,
    //                controlRegionUpperBound, controlRegionRegions, sampleCategoryVariable, 
    //                sampleCategoryBins, sampleCategoryLowerBound, sampleCategoryUpperBound, sampleCategoryRegions
    auto histogramEntries = configManager_m->parseMultiKeyConfig(
        histogramConfigFile,
        {"name", "variable", "weight", "bins", "lowerBound", "upperBound"});

    configHistograms_m.reserve(histogramEntries.size());

    for (const auto &entry : histogramEntries) {
      HistogramConfig config;
      config.name = entry.at("name");
      config.variable = entry.at("variable");
      config.weight = entry.at("weight");
      config.bins = std::stoi(entry.at("bins"));
      config.lowerBound = std::stof(entry.at("lowerBound"));
      config.upperBound = std::stof(entry.at("upperBound"));

      // Optional fields
      auto labelIt = entry.find("label");
      config.label = (labelIt != entry.end()) ? labelIt->second : config.variable;

      auto suffixIt = entry.find("suffix");
      config.suffix = (suffixIt != entry.end()) ? suffixIt->second : "";

      // Channel selection info
      auto channelVarIt = entry.find("channelVariable");
      if (channelVarIt != entry.end()) {
        try {
          config.channelVariable = channelVarIt->second;
          config.channelBins = std::stoi(entry.at("channelBins"));
          config.channelLowerBound = std::stof(entry.at("channelLowerBound"));
          config.channelUpperBound = std::stof(entry.at("channelUpperBound"));
          
          auto channelRegionsIt = entry.find("channelRegions");
          if (channelRegionsIt != entry.end()) {
            config.channelRegions = configManager_m->splitString(channelRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing channel config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no channel selection
        config.channelVariable = "zero__";
        config.channelBins = 1;
        config.channelLowerBound = 0.0;
        config.channelUpperBound = 1.0;
        config.channelRegions = {"Default"};
      }

      // Control region selection info
      auto controlRegionVarIt = entry.find("controlRegionVariable");
      if (controlRegionVarIt != entry.end()) {
        try {
          config.controlRegionVariable = controlRegionVarIt->second;
          config.controlRegionBins = std::stoi(entry.at("controlRegionBins"));
          config.controlRegionLowerBound = std::stof(entry.at("controlRegionLowerBound"));
          config.controlRegionUpperBound = std::stof(entry.at("controlRegionUpperBound"));
          
          auto controlRegionRegionsIt = entry.find("controlRegionRegions");
          if (controlRegionRegionsIt != entry.end()) {
            config.controlRegionRegions = configManager_m->splitString(controlRegionRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing control region config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no control region selection
        config.controlRegionVariable = "zero__";
        config.controlRegionBins = 1;
        config.controlRegionLowerBound = 0.0;
        config.controlRegionUpperBound = 1.0;
        config.controlRegionRegions = {"Default"};
      }

      // Sample category selection info
      auto sampleCategoryVarIt = entry.find("sampleCategoryVariable");
      if (sampleCategoryVarIt != entry.end()) {
        try {
          config.sampleCategoryVariable = sampleCategoryVarIt->second;
          config.sampleCategoryBins = std::stoi(entry.at("sampleCategoryBins"));
          config.sampleCategoryLowerBound = std::stof(entry.at("sampleCategoryLowerBound"));
          config.sampleCategoryUpperBound = std::stof(entry.at("sampleCategoryUpperBound"));
          
          auto sampleCategoryRegionsIt = entry.find("sampleCategoryRegions");
          if (sampleCategoryRegionsIt != entry.end()) {
            config.sampleCategoryRegions = configManager_m->splitString(sampleCategoryRegionsIt->second, ",");
          }
        } catch (const std::exception &e) {
          throw std::runtime_error("NDHistogramManager: Error parsing sample category config for histogram '" + 
                                 config.name + "': " + e.what());
        }
      } else {
        // Use default "zero__" variable for no sample category selection
        config.sampleCategoryVariable = "zero__";
        config.sampleCategoryBins = 1;
        config.sampleCategoryLowerBound = 0.0;
        config.sampleCategoryUpperBound = 1.0;
        config.sampleCategoryRegions = {"Default"};
      }

      configHistograms_m.push_back(config);
    }

    if (logger_m) {
      std::stringstream msg;
      msg << "NDHistogramManager: Loaded " << configHistograms_m.size() 
          << " histogram configurations from " << histogramConfigFile;
      logger_m->log(ILogger::Level::Info, msg.str());
    }

  } catch (const std::exception &e) {
    if (logger_m) {
      std::stringstream msg;
      msg << "NDHistogramManager: Error parsing histogram config file: " << e.what();
      logger_m->log(ILogger::Level::Error, msg.str());
    }
    throw;
  }
}

/**
 * @brief Ensure the region membership RVec column exists on the dataframe.
 *
 * The column `__rm_region_membership__` holds, per event, the 1-indexed float
 * IDs of every declared region the event belongs to.  A value of 0.0 means
 * "not in this region" – it maps to the underflow bin (index < 1) and is
 * therefore silently ignored by the ROOT output code.
 *
 * Because DefineVector builds a compact scalar expression, no extra event-loop
 * pass is triggered; the column is computed lazily alongside everything else.
 */
std::string NDHistogramManager::ensureRegionMembershipColumn() {
  static const std::string kMembershipCol = "__rm_region_membership__";
  if (!regionManager_m) return kMembershipCol;

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();
  const auto existingCols = df.GetColumnNames();
  if (std::find(existingCols.begin(), existingCols.end(), kMembershipCol) !=
      existingCols.end()) {
    return kMembershipCol; // already defined
  }

  const auto &regionNames = regionManager_m->getRegionNames();
  if (regionNames.empty()) return kMembershipCol;

  // Step 1: For each region, define a boolean column whose value is true when
  //         the event satisfies the FULL ancestor filter chain.
  //         These are simple AND-expressions; RDF JIT-compiles them.
  std::vector<std::string> boolColNames;
  for (const auto &name : regionNames) {
    const std::string boolCol = "__rm_in_region_" + name + "__";
    boolColNames.push_back(boolCol);

    const auto existingNow = dataManager_m->getDataFrame().GetColumnNames();
    if (std::find(existingNow.begin(), existingNow.end(), boolCol) !=
        existingNow.end()) {
      continue; // already defined (e.g. called twice)
    }

    const auto chain = regionManager_m->getFilterChain(name);
    std::vector<NDHistogramTypeToken> filterTypes;
    filterTypes.reserve(chain.size());
    bool useFallback = false;
    for (const auto &filterColumn : chain) {
      const std::string filterType = dataManager_m->getDataFrame().GetColumnType(filterColumn);
      if (isNDHistogramRVecType(filterType)) {
        useFallback = true;
        break;
      }
      const auto token = tokenizeNDHistogramType(filterType);
      if (token == NDHistogramTypeToken::Unsupported) {
        useFallback = true;
        break;
      }
      filterTypes.push_back(token);
    }

    if (useFallback) {
      df = df.Define(boolCol, buildRegionMembershipBoolExpression(chain));
    } else {
      df = defineCompiledRegionMembershipChain(df, boolCol, chain, filterTypes);
    }
    dataManager_m->setDataFrame(df);
  }

  // Step 2: For each region, define a float-index column:
  //         __rm_ridx_<name>__ = bool ? float(i+1) : 0.0f
  //         (0.0 falls below lowerBound=0.5 → underflow → not counted)
  std::vector<std::string> idxColNames;
  for (std::size_t i = 0; i < regionNames.size(); ++i) {
    const std::string idxCol = "__rm_ridx_" + regionNames[i] + "__";
    idxColNames.push_back(idxCol);

    const auto existingNow = dataManager_m->getDataFrame().GetColumnNames();
    if (std::find(existingNow.begin(), existingNow.end(), idxCol) !=
        existingNow.end()) {
      continue;
    }

    const float regionIndex = static_cast<float>(i + 1);
    df = df.Define(
        idxCol,
        [regionIndex](bool inRegion) {
          return inRegion ? regionIndex : 0.0f;
        },
        {boolColNames[i]});
    dataManager_m->setDataFrame(df);
  }

  // Step 3: Use DefineVector to pack the per-region float indices into a
  //         single RVec<Float_t>.  DataManager::DefineVector builds a compact
  //         brace-initializer expression for scalar inputs – no JIT overhead.
  dataManager_m->DefineVectorNominal(kMembershipCol, idxColNames, "Float_t",
                                     *systematicManager_m);
  return kMembershipCol;
}

/**
 * @brief Book histograms defined in config file
 *
 * When a RegionManager is bound, each config histogram is booked once as a
 * single large THnSparse with an additional region axis.  All bookings are
 * lazy, so the event loop runs exactly once.
 */
void NDHistogramManager::bookConfigHistograms() {
  if (configHistograms_m.empty()) {
    return;
  }

  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("NDHistogramManager: DataManager or SystematicManager not set");
  }

  if (logger_m) {
    std::stringstream msg;
    msg << "NDHistogramManager: Booking " << configHistograms_m.size()
        << " histograms from config";
    if (regionManager_m && !regionManager_m->getRegionNames().empty()) {
      msg << " with region axis ("
          << regionManager_m->getRegionNames().size() << " region(s))";
    }
    logger_m->log(ILogger::Level::Info, msg.str());
  }

  // Build the region membership column once (no-op if already done or no RM).
  std::string membershipCol;
  std::vector<std::string> rmRegionNames;
  int nRegions = 0;
  if (regionManager_m && !regionManager_m->getRegionNames().empty()) {
    membershipCol = ensureRegionMembershipColumn();
    rmRegionNames = regionManager_m->getRegionNames();
    nRegions = static_cast<int>(rmRegionNames.size());
  }

  // Auto-detect systematic variations from dataframe columns before the syst
  // list is built for the first time.
  std::vector<std::string> systList{"Nominal"};
  if (histogramSystematics_m) {
    ensureSystematicsAutoRegistered();
    systList =
        systematicManager_m->makeSystList("SystematicCounter", *dataManager_m);
  }

  for (const auto &config : configHistograms_m) {
    histInfo info(config.name.c_str(), config.variable.c_str(),
                  config.label.c_str(), config.weight.c_str(),
                  config.bins, config.lowerBound, config.upperBound);

    if (nRegions > 0) {
      // Region-aware path: override the channel axis with the region
      // membership RVec.  Axis range [0.5, N+0.5] with N bins ensures that
      // index 0 (event not in this region) falls to underflow (ignored).
      selectionInfo regionChannelInfo(membershipCol, nRegions,
                                      0.5f,
                                      static_cast<float>(nRegions) + 0.5f,
                                      rmRegionNames);

      selectionInfo controlRegionInfo(config.controlRegionVariable,
                                      config.controlRegionBins,
                                      config.controlRegionLowerBound,
                                      config.controlRegionUpperBound,
                                      config.controlRegionRegions);

      selectionInfo sampleCategoryInfo(config.sampleCategoryVariable,
                                       config.sampleCategoryBins,
                                       config.sampleCategoryLowerBound,
                                       config.sampleCategoryUpperBound,
                                       config.sampleCategoryRegions);

      // Pass the membership column as baseRefVector so that scalar base
      // variables (and their systematic variations) are expanded to the same
      // size as the region RVec – required for correct multi-fill behaviour.
      BookSingleHistogramWithSystList(info,
                                      std::move(sampleCategoryInfo),
                                      std::move(controlRegionInfo),
                                      std::move(regionChannelInfo),
                                      config.suffix,
                                      systList,
                                      membershipCol);
    } else {
      // Standard (non-region) path: existing behaviour unchanged.
      selectionInfo channelInfo(config.channelVariable, config.channelBins,
                                config.channelLowerBound, config.channelUpperBound,
                                config.channelRegions);
      selectionInfo controlRegionInfo(config.controlRegionVariable,
                                      config.controlRegionBins,
                                      config.controlRegionLowerBound,
                                      config.controlRegionUpperBound,
                                      config.controlRegionRegions);
      selectionInfo sampleCategoryInfo(config.sampleCategoryVariable,
                                       config.sampleCategoryBins,
                                       config.sampleCategoryLowerBound,
                                       config.sampleCategoryUpperBound,
                                       config.sampleCategoryRegions);
      BookSingleHistogramWithSystList(info,
                                      std::move(sampleCategoryInfo),
                                      std::move(controlRegionInfo),
                                      std::move(channelInfo),
                                      config.suffix,
                                      systList);
    }
  }

  // Track booked config histogram infos so that the no-args saveHists()
  // overload (called by Analyzer::run()) can find and write them to the meta
  // file.  Without this, trackedHistInfos_m stays empty and saveHists()
  // returns early, leaving histograms out of the output.
  std::vector<histInfo> configBatch;
  configBatch.reserve(configHistograms_m.size());
  for (const auto& config : configHistograms_m) {
    configBatch.emplace_back(config.name.c_str(), config.variable.c_str(),
                              config.label.c_str(), config.weight.c_str(),
                              config.bins, config.lowerBound, config.upperBound);
  }
  if (!configBatch.empty()) {
    trackedHistInfos_m.push_back(std::move(configBatch));
    // Build region names that match the axes used when booking.
    // SaveHists() uses these to map histogram bins to output directory paths.
    // Axis order: [channel, controlRegion, sampleCategory, systematics].
    //
    // NOTE: When different config histograms use distinct channel/control/
    // sampleCategory region lists, SaveHists() can only honour one set of
    // region names for the entire batch.  Using the first config's regions is
    // correct for the common case where all histograms share the same region
    // structure.  If histograms with heterogeneous region lists are needed,
    // book them via separate bookND() calls with explicit allRegionNames.
    // The guard `configBatch.empty()` above ensures configHistograms_m is
    // non-empty before the `.front()` calls below.
    if (nRegions > 0) {
      // RegionManager path: channel axis is the region membership column.
      const auto& firstConfig = configHistograms_m.front();
      trackedRegionNames_m = {
          rmRegionNames,
          firstConfig.controlRegionRegions,
          firstConfig.sampleCategoryRegions,
          systList};
    } else {
      // Standard path: axes come from the per-histogram selectionInfo.
      // Use the first config's regions as the representative set.
      const auto& firstConfig = configHistograms_m.front();
      trackedRegionNames_m = {
          firstConfig.channelRegions,
          firstConfig.controlRegionRegions,
          firstConfig.sampleCategoryRegions,
          systList};
    }
  }

  if (logger_m) {
    logger_m->log(ILogger::Level::Info, "NDHistogramManager: Successfully booked all config histograms");
  }
}
void NDHistogramManager::initialize() {
  std::cout << "NDHistogramManager: initialized with "
            << configHistograms_m.size() << " config histogram(s)."
            << std::endl;
}

void NDHistogramManager::reportMetadata() {
  if (!logger_m) return;
  logger_m->log(ILogger::Level::Info,
                "NDHistogramManager: " +
                std::to_string(configHistograms_m.size()) +
                " config histogram(s) defined.");
}

std::shared_ptr<NDHistogramManager> NDHistogramManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<NDHistogramManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}
