#include <api/IConfigurationProvider.h>
#include <analyzer.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <api/IPluggableManager.h>
#include <DataManager.h>
#include <NDHistogramManager.h>
#include <RDFColumnPacker.h>
#include <RegionManager.h>
#include <CounterService.h>
#include <TFile.h>
#include <TList.h>
#include <TH1F.h>
#include <THnSparse.h>
#include <TObjString.h>
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
#include <iomanip>

/**
 * @brief Construct a new NDHistogramManager object
 */
NDHistogramManager::NDHistogramManager(IConfigurationProvider const& configProvider) {}

void NDHistogramManager::bindToRegionManager(RegionManager *rm) {
  regionManager_m = rm;
}

struct ColumnCache {
  ROOT::RDF::RNode& df;
  std::unordered_set<std::string> names;
  std::unordered_map<std::string, std::string> types;

  explicit ColumnCache(ROOT::RDF::RNode& node)
      : df(node) {
    Refresh();
  }

  bool Has(const std::string& name) const {
    return names.find(name) != names.end();
  }

  void Refresh() {
    const auto columnNames = df.GetColumnNames();
    names.clear();
    names.reserve(columnNames.size());
    names.insert(columnNames.begin(), columnNames.end());
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

std::string jsonEscape(const std::string &value) {
  std::ostringstream escaped;
  for (const unsigned char c : value) {
    switch (c) {
    case '"': escaped << "\\\""; break;
    case '\\': escaped << "\\\\"; break;
    case '\b': escaped << "\\b"; break;
    case '\f': escaped << "\\f"; break;
    case '\n': escaped << "\\n"; break;
    case '\r': escaped << "\\r"; break;
    case '\t': escaped << "\\t"; break;
    default:
      if (c < 0x20) {
        escaped << "\\u" << std::hex << std::setw(4) << std::setfill('0')
                << static_cast<int>(c) << std::dec;
      } else {
        escaped << static_cast<char>(c);
      }
    }
  }
  return escaped.str();
}

std::vector<Double_t> getAxisEdges(const TAxis &axis) {
  std::vector<Double_t> edges(static_cast<std::size_t>(axis.GetNbins()) + 1);
  for (Int_t bin = 1; bin <= axis.GetNbins() + 1; ++bin) {
    edges[static_cast<std::size_t>(bin - 1)] = axis.GetBinLowEdge(bin);
  }
  return edges;
}

void appendJsonStringArray(std::ostringstream &json,
                           const std::vector<std::string> &values) {
  json << '[';
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i != 0) json << ',';
    json << '"' << jsonEscape(values[i]) << '"';
  }
  json << ']';
}

void appendJsonDoubleArray(std::ostringstream &json,
                           const std::vector<Double_t> &values) {
  json << '[';
  for (std::size_t i = 0; i < values.size(); ++i) {
    if (i != 0) json << ',';
    json << std::setprecision(17) << values[i];
  }
  json << ']';
}

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

template <typename ScalarT>
ROOT::RDF::RNode defineSystematicBroadcastFloatVector(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &referenceColumn, const std::string &scalarColumn,
    std::size_t systematicSlots) {
  return df.Define(
      name,
      [systematicSlots](const ROOT::VecOps::RVec<Float_t> &reference,
                        ScalarT value) -> ROOT::VecOps::RVec<Float_t> {
        if (systematicSlots == 0 || reference.size() % systematicSlots != 0) {
          throw std::runtime_error(
              "NDHistogramManager: base fill vector is not divisible by its "
              "systematic slot count");
        }
        return ROOT::VecOps::RVec<Float_t>(
            reference.size() / systematicSlots, static_cast<Float_t>(value));
      },
      {referenceColumn, scalarColumn});
}

ROOT::RDF::RNode dispatchSystematicBroadcastFloatVector(
    ROOT::RDF::RNode df, const std::string &name,
    const std::string &referenceColumn, const std::string &scalarColumn,
    NDHistogramTypeToken scalarType, std::size_t systematicSlots) {
  switch (scalarType) {
  case NDHistogramTypeToken::Bool:
    return defineSystematicBroadcastFloatVector<Bool_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Float:
    return defineSystematicBroadcastFloatVector<Float_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Double:
    return defineSystematicBroadcastFloatVector<Double_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Int:
    return defineSystematicBroadcastFloatVector<Int_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::UInt:
    return defineSystematicBroadcastFloatVector<UInt_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Short:
    return defineSystematicBroadcastFloatVector<Short_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::UShort:
    return defineSystematicBroadcastFloatVector<UShort_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Char:
    return defineSystematicBroadcastFloatVector<Char_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::UChar:
    return defineSystematicBroadcastFloatVector<UChar_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Long64:
    return defineSystematicBroadcastFloatVector<Long64_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::ULong64:
    return defineSystematicBroadcastFloatVector<ULong64_t>(
        df, name, referenceColumn, scalarColumn, systematicSlots);
  case NDHistogramTypeToken::Unsupported:
    throw std::runtime_error(
        "NDHistogramManager: unsupported scalar type in systematic broadcast path");
  }

  throw std::runtime_error(
      "NDHistogramManager: unreachable systematic broadcast dispatch");
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


} // namespace

// Helper function to handle per-axis logic for varVector and DefineVector
static void HandleAxisVarVector(
    ROOT::RDF::RNode& df,
    IDataFrameProvider* dataManager_m,
    ISystematicManager* systematicManager_m,
    ColumnCache& cache,
    std::unordered_map<std::string, std::string>& packedColumns,
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
          const std::string columnType = cache.GetType(columnName);
          if (columnType.find("RVec") == std::string::npos) {
            const std::string expandedName =
                columnName + "_FillRVecPerSyst_" + refVector + "_" +
                std::to_string(allSystematics.size()) + tagSuffix;
            if (!cache.Has(expandedName)) {
              const auto scalarToken = tokenizeNDHistogramType(columnType);
              if (scalarToken == NDHistogramTypeToken::Unsupported) {
                df = df.Define(
                    expandedName,
                    "ROOT::VecOps::RVec<Float_t>(" + refVector + ".size() / " +
                        std::to_string(allSystematics.size()) +
                        ", static_cast<Float_t>(" + columnName + "))");
              } else {
                df = dispatchSystematicBroadcastFloatVector(
                    df, expandedName, refVector, columnName, scalarToken,
                    allSystematics.size());
              }
              dataManager_m->setDataFrame(df);
              cache.Refresh();
            }
            columnName = expandedName;
          }
        }
        systematicVariations.push_back(columnName);
        }
        std::string signature;
        for (const auto &column : systematicVariations)
          signature += std::to_string(column.size()) + ":" + column;
        const auto packed = packedColumns.find(signature);
        if (packed != packedColumns.end()) {
          varVector.push_back(packed->second);
        } else {
          const std::string systVectorName = variable + "_systVector" + tagSuffix;
          if (!cache.Has(systVectorName)) {
            dataManager_m->DefineVectorNominal(systVectorName, systematicVariations,
                                              "Float_t", *systematicManager_m);
            df = dataManager_m->getDataFrame();
            cache.Refresh();
          }
          packedColumns.emplace(std::move(signature), systVectorName);
          varVector.push_back(systVectorName);
        }
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

  // A nominal-only booking must not reuse a previously materialised full
  // systematic vector: its longer size would be inconsistent with the
  // one-bin systematic axis.  Keep a dedicated vector for the nominal case.
  const bool nominalOnly =
      systList.size() == 1 && systList.front() == "Nominal";
  const std::string systVectorName =
      nominalOnly ? baseName + "_nominal_systVector"
                  : baseName + "_systVector";
  if (!cache.Has(systVectorName)) {
    // These values are slot indices, independent of the event. Avoid a
    // chain of vector appends and column readers for a constant bundle.
    ROOT::VecOps::RVec<Float_t> indices(systList.size());
    for (std::size_t i = 0; i < indices.size(); ++i) indices[i] = i;
    df = df.Define(systVectorName, [indices]() { return indices; });
    dataManager_m->setDataFrame(df);
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
  // Equal label lists can refer to new cumulative masks after another filter.
  for (const auto &column : maskColumns)
    signature += std::to_string(column.size()) + ":" + column;
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

  axisPackCache_m.clear();
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
  fillInfo.forceSparse = info.forceSparse();

  fillInfo.name = info.name() + "_" + suffix;
  fillInfo.title = info.name() + " " + suffix;
  fillInfo.nSlots = df.GetNSlots();
  // Systematic axis will be inserted between sampleCategory and the histogram variable.
  fillInfo.nbins = {channelInfo.bins(), controlRegionInfo.bins(), sampleCategoryInfo.bins(), 1, info.bins()};
  fillInfo.xmin = {channelInfo.lowerBound(), controlRegionInfo.lowerBound(), sampleCategoryInfo.lowerBound(), 0.0, info.lowerBound()};
  fillInfo.xmax = {channelInfo.upperBound(), controlRegionInfo.upperBound(), sampleCategoryInfo.upperBound(), 1.0, info.upperBound()};
  fillInfo.binEdges.resize(fillInfo.dim);
  if (!info.binEdges().empty()) {
    if (histogramBackend_m == "boost") {
      throw std::runtime_error(
          "NDHistogramManager: variable-width bins require histogramBackend: root");
    }
    fillInfo.binEdges[4] = info.binEdges();
  }

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

  // The histogram variable is the fill axis too. Mark vector-valued input so
  // the action selects a multi-fill path when another axis introduces
  // systematics (or when this is a nominal-only multi-fill histogram).
  const std::string histogramVariableType = cache.GetType(info.variable());
  bool baseAxisHasMultiFill =
      histogramVariableType.find("RVec") != std::string::npos;
  if (fillInfo.hasSystematic) {
    for (const auto &systematic : systList) {
      const auto variationColumn =
          systematicManager_m->getVariationColumnName(info.variable(),
                                                       systematic);
      if (cache.Has(variationColumn) &&
          cache.GetType(variationColumn).find("RVec") != std::string::npos) {
        baseAxisHasMultiFill = true;
        break;
      }
    }
  }
  if (baseAxisHasMultiFill) {
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

  // Any systematic histogram fill requires every axis to be represented once
  // per systematic, even when that axis itself is unaffected. Unaffected
  // values repeat into the varied slots so multi-fill vectors stay aligned
  // with the systematic axis. Selection masks additionally decide whether a
  // particular slot receives the event.
  const bool hasSelectionMasks = systematicManager_m->hasSelectionMasks();
  if (fillInfo.hasSystematic || hasSelectionMasks) {
    fillInfo.channel_hasSystematic = true;
    fillInfo.controlRegion_hasSystematic = true;
    fillInfo.sampleCategory_hasSystematic = true;
    fillInfo.weight_hasSystematic = true;
    fillInfo.hasSystematic = true;
  }

  // When the observable has multiple fills, the systematic packing above
  // expands each category axis to one value per fill in each systematic slot.
  // Advance those axis vectors with the base value vector inside a slot.
  if (fillInfo.hasSystematic && baseAxisHasMultiFill) {
    fillInfo.channel_hasMultiFill = true;
    fillInfo.controlRegion_hasMultiFill = true;
    fillInfo.sampleCategory_hasMultiFill = true;
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
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, axisPackCache_m, info.variable(),
                      fillInfo.hasSystematic, fillInfo.hasMultiFill, usedSystematics,
                      baseValsVec, false,
                      baseRefVector.empty() ? std::vector<std::string>{}
                                            : std::vector<std::string>{baseRefVector},
                      uniqueTag);
  const std::vector<std::string> fillReference =
      baseAxisHasMultiFill || !baseRefVector.empty()
          ? std::vector<std::string>{baseValsVec.front()}
          : std::vector<std::string>{};
  std::vector<std::string> baseWeightsVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, axisPackCache_m, info.weight(), fillInfo.weight_hasSystematic, fillInfo.weight_hasMultiFill, usedSystematics, baseWeightsVec, true, fillReference, uniqueTag);

  const std::string systVectorName = buildSystematicVector(df, dataManager_m, systematicManager_m, systList, cache);
  const std::string selectionMaskVectorName =
      buildSelectionMaskVector(df, dataManager_m, systematicManager_m, systList,
                              cache);

  std::vector<std::string> sampleCategoryVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, axisPackCache_m, sampleCategoryInfo.variable(), fillInfo.sampleCategory_hasSystematic, fillInfo.sampleCategory_hasMultiFill, usedSystematics, sampleCategoryVec, false, fillReference, uniqueTag);
  std::vector<std::string> controlRegionVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, axisPackCache_m, controlRegionInfo.variable(), fillInfo.controlRegion_hasSystematic, fillInfo.controlRegion_hasMultiFill, usedSystematics, controlRegionVec, false, fillReference, uniqueTag);
  std::vector<std::string> channelVec;
  HandleAxisVarVector(df, dataManager_m, systematicManager_m, cache, axisPackCache_m, channelInfo.variable(), fillInfo.channel_hasSystematic, fillInfo.channel_hasMultiFill, usedSystematics, channelVec, false, fillReference, uniqueTag);

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
  histRegionNames_m.push_back({channelInfo.regions(), controlRegionInfo.regions(),
                               sampleCategoryInfo.regions(), systList});
  histNodes_m.push_back(df);
}

void NDHistogramManager::bookND(std::vector<histInfo> &infos,
                                std::vector<selectionInfo> &selection,
                                const std::string &suffix,
                                std::vector<std::vector<std::string>> &allRegionNames) {
  axisPackCache_m.clear();
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
    histRegionNames_m.back() = normalizedRegionNames;
  }
}

void NDHistogramManager::saveHists() {
  if (!configManager_m) {
    throw std::runtime_error("NDHistogramManager: ConfigurationManager not set");
  }
  if (trackedHistInfos_m.empty()) {
    return;
  }
  SaveHistsImpl(trackedHistInfos_m, trackedRegionNames_m, *configManager_m, true);
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
  SaveHistsImpl(fullHistList, allRegionNames, configProvider, false);
}

void NDHistogramManager::SaveHistsImpl(
    std::vector<std::vector<histInfo>> &fullHistList,
    std::vector<std::vector<std::string>> &allRegionNames,
    const IConfigurationProvider &configProvider,
    bool useBookedAxisLabels) {
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
  for (auto const &histList : fullHistList) {
    for (auto const &info : histList) {
      //std::cout << "Storing histogram: " << info.name() << std::endl;
      allNames.push_back(info.name());
      allVariables.push_back(info.variable());
      allLabels.push_back(info.label());
    }
  }

  // Open the meta file for update so we don't clobber histograms (e.g. counters)
  // written by Analyzer/CounterService earlier.
  TFile saveFile(fileName.c_str(), "UPDATE");
  if (saveFile.IsZombie() || !saveFile.IsOpen() || !saveFile.IsWritable()) {
    throw std::runtime_error("Cannot open histogram output for update: " + fileName);
  }
  // An interrupted first write can leave a header-only file. ROOT accepts
  // this in UPDATE mode without marking it zombie, but mkdir/TKey then
  // dereferences the missing first free segment (also in ROOT 6.36).
  // Refuse the update before triggering RDF, and avoid rewriting the damaged
  // file during stack unwinding so it remains available for inspection.
  if (!saveFile.GetListOfFree() || saveFile.GetListOfFree()->IsEmpty()) {
    saveFile.SetWritable(false);
    throw std::runtime_error(
        "Histogram output has no free-segment table (possibly an interrupted "
        "previous write): " + fileName +
        ". Preserve or move this file before rerunning with a fresh output path.");
  }

  constexpr const char *kHistogramSchemaKey = "__rdf_histogram_schema_v1";
  constexpr const char *kHistogramDirectory = "nd_histograms_v1";
  static const std::vector<std::string> axisRoles = {
      "channel", "control", "sample", "systematic", "observable"};

  TDirectory *ndDirectory = saveFile.GetDirectory(kHistogramDirectory);
  if (!ndDirectory) {
    ndDirectory = saveFile.mkdir(kHistogramDirectory);
  }
  if (!ndDirectory) {
    throw std::runtime_error("Cannot create N-dimensional histogram directory in " +
                             fileName);
  }

  std::ostringstream schema;
  schema << "{\"schema_version\":1,\"directory\":\""
         << kHistogramDirectory << "\",\"histograms\":[";

  std::cout << "Writing " << histos_m.size()
            << " N-dimensional histograms for merging..." << std::endl;
  for (std::size_t histIndex = 0; histIndex < histos_m.size(); ++histIndex) {
    if (histIndex >= allNames.size() || histIndex >= allVariables.size() ||
        histIndex >= allLabels.size()) {
      throw std::runtime_error(
          "NDHistogramManager: booked histogram list does not match save metadata");
    }

    auto hist = histos_m[histIndex].GetPtr();
    if (!hist) {
      throw std::runtime_error("NDHistogramManager: booked histogram result is null");
    }

    const Int_t dim = hist->GetNdimensions();
    if (dim < 2) {
      throw std::runtime_error(
          "NDHistogramManager: expected an observable axis and at least one category axis");
    }
    const std::string objectKey = hist->GetName();
    const std::string logicalName = allNames[histIndex];
    const auto &histogramRegionNames =
        useBookedAxisLabels && histIndex < histRegionNames_m.size() &&
                !histRegionNames_m[histIndex].empty()
            ? histRegionNames_m[histIndex]
            : allRegionNames;

    if (histIndex != 0) schema << ',';
    schema << "{\"key\":\"" << jsonEscape(objectKey)
           << "\",\"name\":\"" << jsonEscape(logicalName)
           << "\",\"variable\":\"" << jsonEscape(allVariables[histIndex])
           << "\",\"label\":\"" << jsonEscape(allLabels[histIndex])
           << "\",\"axes\":[";

    for (Int_t axisIndex = 0; axisIndex < dim; ++axisIndex) {
      if (axisIndex != 0) schema << ',';
      TAxis *axis = hist->GetAxis(axisIndex);
      const std::string role =
          axisIndex < static_cast<Int_t>(axisRoles.size())
              ? axisRoles[static_cast<std::size_t>(axisIndex)]
              : "axis_" + std::to_string(axisIndex);
      axis->SetTitle(role.c_str());

      std::vector<std::string> labels;
      if (axisIndex < dim - 1 &&
          axisIndex < static_cast<Int_t>(histogramRegionNames.size())) {
        labels = histogramRegionNames[static_cast<std::size_t>(axisIndex)];
      }
      const auto nLabels = std::min(
          labels.size(), static_cast<std::size_t>(axis->GetNbins()));
      for (std::size_t labelIndex = 0; labelIndex < nLabels; ++labelIndex) {
        axis->SetBinLabel(static_cast<Int_t>(labelIndex + 1),
                          labels[labelIndex].c_str());
      }
      labels.resize(nLabels);

      schema << "{\"role\":\"" << jsonEscape(role)
             << "\",\"bins\":" << axis->GetNbins()
             << ",\"labels\":";
      appendJsonStringArray(schema, labels);
      schema << ",\"edges\":";
      appendJsonDoubleArray(schema, getAxisEdges(*axis));
      schema << '}';
    }
    schema << "]}";

    ndDirectory->cd();
    hist->SetTitle(logicalName.c_str());
    hist->Write(objectKey.c_str(), TObject::kOverwrite);
  }

  schema << "]}";
  saveFile.cd();
  TObjString schemaObject(schema.str().c_str());
  schemaObject.Write(kHistogramSchemaKey, TObject::kOverwrite);
  saveFile.Flush();
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
  histRegionNames_m.clear();
  trackedHistInfos_m.clear();
  trackedRegionNames_m.clear();
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
    // Optional keys: label, suffix, binningRanges, channelVariable, channelBins, channelLowerBound, channelUpperBound,
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
      auto binningRangesIt = entry.find("binningRanges");
      if (binningRangesIt != entry.end()) {
        const auto ranges =
            configManager_m->splitString(binningRangesIt->second, ";");
        for (const auto &range : ranges) {
          const auto parts = configManager_m->splitString(range, ":");
          if (parts.size() != 3) {
            throw std::runtime_error(
                "NDHistogramManager: binningRanges for histogram '" +
                config.name +
                "' must contain semicolon-separated min:max:nbins ranges");
          }
          const Double_t lower = std::stod(parts[0]);
          const Double_t upper = std::stod(parts[1]);
          const Int_t nBins = std::stoi(parts[2]);
          if (!std::isfinite(lower) || !std::isfinite(upper) ||
              lower >= upper || nBins <= 0) {
            throw std::runtime_error(
                "NDHistogramManager: invalid binning range for histogram '" +
                config.name + "'");
          }
          if (config.binEdges.empty()) {
            config.binEdges.push_back(lower);
          } else if (std::abs(config.binEdges.back() - lower) > 1.0e-9) {
            throw std::runtime_error(
                "NDHistogramManager: binningRanges for histogram '" +
                config.name + "' must be contiguous and ordered");
          }
          for (Int_t bin = 1; bin <= nBins; ++bin) {
            config.binEdges.push_back(
                lower + (upper - lower) * static_cast<Double_t>(bin) / nBins);
          }
        }
        if (config.binEdges.size() !=
                static_cast<std::size_t>(config.bins + 1) ||
            std::abs(config.binEdges.front() - config.lowerBound) > 1.0e-6 ||
            std::abs(config.binEdges.back() - config.upperBound) > 1.0e-6) {
          throw std::runtime_error(
              "NDHistogramManager: binningRanges for histogram '" +
              config.name +
              "' must match bins, lowerBound, and upperBound");
        }
      }

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
    if (chain.empty())
      throw std::runtime_error("NDHistogramManager: empty region filter chain");
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
    }

    if (useFallback) {
      df = df.Define(boolCol, buildRegionMembershipBoolExpression(chain));
    } else {
      df = rdfcolumnpacker::defineScalarAll(df, boolCol, chain);
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
void NDHistogramManager::bookConfigHistograms(
    const std::vector<std::string> &excludedNames) {
  axisPackCache_m.clear();
  if (!excludedNames.empty()) {
    if (!histos_m.empty()) {
      throw std::runtime_error("NDHistogramManager: exclude config histograms before booking");
    }
    configHistograms_m.erase(
        std::remove_if(configHistograms_m.begin(), configHistograms_m.end(),
                       [&](const HistogramConfig &entry) {
                         return std::find(excludedNames.begin(), excludedNames.end(),
                                          entry.name) != excludedNames.end();
                       }),
        configHistograms_m.end());
  }
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
                  config.bins, config.lowerBound, config.upperBound,
                  config.binEdges);

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
                              config.bins, config.lowerBound, config.upperBound,
                              config.binEdges);
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
