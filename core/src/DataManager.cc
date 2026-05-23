#include <api/IConfigurationProvider.h>
#include <algorithm>
#include <ROOT/RVec.hxx>
#include <DataManager.h>
#include <TChain.h>
#include <cctype>
#include <iostream>
#include <util.h>
#include <filesystem>

#include <ROOT/RDFHelpers.hxx>
#include <functions.h>

namespace {

enum class DefineVectorTypeToken {
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

std::string removeWhitespace(std::string value) {
  value.erase(std::remove_if(value.begin(), value.end(),
                             [](unsigned char c) { return std::isspace(c) != 0; }),
              value.end());
  return value;
}

bool isRVecColumnType(const std::string &columnType) {
  return columnType.find("RVec<") != std::string::npos;
}

std::string extractRVecElementType(const std::string &columnType) {
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

DefineVectorTypeToken tokenizeDefineVectorType(const std::string &typeName) {
  const auto normalized = removeWhitespace(typeName);

  if (normalized == "Bool_t" || normalized == "bool") {
    return DefineVectorTypeToken::Bool;
  }
  if (normalized == "Float_t" || normalized == "float") {
    return DefineVectorTypeToken::Float;
  }
  if (normalized == "Double_t" || normalized == "double") {
    return DefineVectorTypeToken::Double;
  }
  if (normalized == "Int_t" || normalized == "int") {
    return DefineVectorTypeToken::Int;
  }
  if (normalized == "UInt_t" || normalized == "unsignedint") {
    return DefineVectorTypeToken::UInt;
  }
  if (normalized == "Short_t" || normalized == "short") {
    return DefineVectorTypeToken::Short;
  }
  if (normalized == "UShort_t" || normalized == "unsignedshort") {
    return DefineVectorTypeToken::UShort;
  }
  if (normalized == "Char_t" || normalized == "char") {
    return DefineVectorTypeToken::Char;
  }
  if (normalized == "UChar_t" || normalized == "unsignedchar") {
    return DefineVectorTypeToken::UChar;
  }
  if (normalized == "Long64_t" || normalized == "longlong") {
    return DefineVectorTypeToken::Long64;
  }
  if (normalized == "ULong64_t" || normalized == "unsignedlonglong") {
    return DefineVectorTypeToken::ULong64;
  }

  return DefineVectorTypeToken::Unsupported;
}

std::string buildScalarDefineVectorExpression(
    const std::vector<std::string> &columns, const std::string &type) {
  std::string expr = "ROOT::VecOps::RVec<" + type + ">{";
  for (size_t i = 0; i < columns.size(); ++i) {
    expr += "static_cast<" + type + ">(" + columns[i] + ")";
    if (i + 1 < columns.size()) {
      expr += ", ";
    }
  }
  expr += "}";
  return expr;
}

std::string buildRVecDefineVectorExpression(
    const std::vector<std::string> &columns, const std::string &type) {
  std::string expr = "size_t total = 0;\n";
  for (const auto &col : columns) {
    expr += "total += " + col + ".size();\n";
  }
  expr += "ROOT::VecOps::RVec<" + type + "> out;\n";
  expr += "out.reserve(total);\n";
  for (const auto &col : columns) {
    expr += "for (auto& x : " + col + ") out.push_back(static_cast<" + type + ">(x));\n";
  }
  expr += "return out;";
  return expr;
}

template <typename TargetT, typename InputT>
ROOT::RDF::RNode defineScalarVectorInit(ROOT::RDF::RNode df,
                                        const std::string &name,
                                        const std::string &column) {
  return df.Define(
      name,
      [](InputT value) -> ROOT::VecOps::RVec<TargetT> {
        return castValueToVector<TargetT>(value);
      },
      {column});
}

template <typename TargetT, typename InputT>
ROOT::RDF::RNode defineScalarVectorAppend(ROOT::RDF::RNode df,
                                          const std::string &name,
                                          const std::string &currentColumn,
                                          const std::string &nextColumn) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<TargetT> &existing,
         InputT value) -> ROOT::VecOps::RVec<TargetT> {
        return appendScalarCastToVector<TargetT>(existing, value);
      },
      {currentColumn, nextColumn});
}

template <typename TargetT, typename InputT>
ROOT::RDF::RNode defineRVecVectorInit(ROOT::RDF::RNode df,
                                      const std::string &name,
                                      const std::string &column) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<InputT> &values) -> ROOT::VecOps::RVec<TargetT> {
        return castRVec<TargetT>(values);
      },
      {column});
}

template <typename TargetT, typename InputT>
ROOT::RDF::RNode defineRVecVectorAppend(ROOT::RDF::RNode df,
                                        const std::string &name,
                                        const std::string &currentColumn,
                                        const std::string &nextColumn) {
  return df.Define(
      name,
      [](const ROOT::VecOps::RVec<TargetT> &existing,
         const ROOT::VecOps::RVec<InputT> &values) -> ROOT::VecOps::RVec<TargetT> {
        return concatenateCastRVec<TargetT>(existing, values);
      },
      {currentColumn, nextColumn});
}

template <typename TargetT>
ROOT::RDF::RNode defineEmptyVectorColumn(ROOT::RDF::RNode df,
                                         const std::string &name) {
  return df.Define(
      name,
      []() -> ROOT::VecOps::RVec<TargetT> { return defineEmptyVector<TargetT>(); },
      std::vector<std::string>{});
}

template <typename TargetT>
ROOT::RDF::RNode dispatchScalarInit(ROOT::RDF::RNode df,
                                    const std::string &name,
                                    const std::string &column,
                                    DefineVectorTypeToken inputType) {
  switch (inputType) {
  case DefineVectorTypeToken::Bool:
    return defineScalarVectorInit<TargetT, Bool_t>(df, name, column);
  case DefineVectorTypeToken::Float:
    return defineScalarVectorInit<TargetT, Float_t>(df, name, column);
  case DefineVectorTypeToken::Double:
    return defineScalarVectorInit<TargetT, Double_t>(df, name, column);
  case DefineVectorTypeToken::Int:
    return defineScalarVectorInit<TargetT, Int_t>(df, name, column);
  case DefineVectorTypeToken::UInt:
    return defineScalarVectorInit<TargetT, UInt_t>(df, name, column);
  case DefineVectorTypeToken::Short:
    return defineScalarVectorInit<TargetT, Short_t>(df, name, column);
  case DefineVectorTypeToken::UShort:
    return defineScalarVectorInit<TargetT, UShort_t>(df, name, column);
  case DefineVectorTypeToken::Char:
    return defineScalarVectorInit<TargetT, Char_t>(df, name, column);
  case DefineVectorTypeToken::UChar:
    return defineScalarVectorInit<TargetT, UChar_t>(df, name, column);
  case DefineVectorTypeToken::Long64:
    return defineScalarVectorInit<TargetT, Long64_t>(df, name, column);
  case DefineVectorTypeToken::ULong64:
    return defineScalarVectorInit<TargetT, ULong64_t>(df, name, column);
  case DefineVectorTypeToken::Unsupported:
    throw std::runtime_error("DefineVector: unsupported scalar input type in compiled path.");
  }

  throw std::runtime_error("DefineVector: unreachable scalar init dispatch.");
}

template <typename TargetT>
ROOT::RDF::RNode dispatchScalarAppend(ROOT::RDF::RNode df,
                                      const std::string &name,
                                      const std::string &currentColumn,
                                      const std::string &nextColumn,
                                      DefineVectorTypeToken inputType) {
  switch (inputType) {
  case DefineVectorTypeToken::Bool:
    return defineScalarVectorAppend<TargetT, Bool_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Float:
    return defineScalarVectorAppend<TargetT, Float_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Double:
    return defineScalarVectorAppend<TargetT, Double_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Int:
    return defineScalarVectorAppend<TargetT, Int_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UInt:
    return defineScalarVectorAppend<TargetT, UInt_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Short:
    return defineScalarVectorAppend<TargetT, Short_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UShort:
    return defineScalarVectorAppend<TargetT, UShort_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Char:
    return defineScalarVectorAppend<TargetT, Char_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UChar:
    return defineScalarVectorAppend<TargetT, UChar_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Long64:
    return defineScalarVectorAppend<TargetT, Long64_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::ULong64:
    return defineScalarVectorAppend<TargetT, ULong64_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Unsupported:
    throw std::runtime_error("DefineVector: unsupported scalar input type in compiled path.");
  }

  throw std::runtime_error("DefineVector: unreachable scalar append dispatch.");
}

template <typename TargetT>
ROOT::RDF::RNode dispatchRVecInit(ROOT::RDF::RNode df,
                                  const std::string &name,
                                  const std::string &column,
                                  DefineVectorTypeToken inputType) {
  switch (inputType) {
  case DefineVectorTypeToken::Bool:
    return defineRVecVectorInit<TargetT, Bool_t>(df, name, column);
  case DefineVectorTypeToken::Float:
    return defineRVecVectorInit<TargetT, Float_t>(df, name, column);
  case DefineVectorTypeToken::Double:
    return defineRVecVectorInit<TargetT, Double_t>(df, name, column);
  case DefineVectorTypeToken::Int:
    return defineRVecVectorInit<TargetT, Int_t>(df, name, column);
  case DefineVectorTypeToken::UInt:
    return defineRVecVectorInit<TargetT, UInt_t>(df, name, column);
  case DefineVectorTypeToken::Short:
    return defineRVecVectorInit<TargetT, Short_t>(df, name, column);
  case DefineVectorTypeToken::UShort:
    return defineRVecVectorInit<TargetT, UShort_t>(df, name, column);
  case DefineVectorTypeToken::Char:
    return defineRVecVectorInit<TargetT, Char_t>(df, name, column);
  case DefineVectorTypeToken::UChar:
    return defineRVecVectorInit<TargetT, UChar_t>(df, name, column);
  case DefineVectorTypeToken::Long64:
    return defineRVecVectorInit<TargetT, Long64_t>(df, name, column);
  case DefineVectorTypeToken::ULong64:
    return defineRVecVectorInit<TargetT, ULong64_t>(df, name, column);
  case DefineVectorTypeToken::Unsupported:
    throw std::runtime_error("DefineVector: unsupported RVec input type in compiled path.");
  }

  throw std::runtime_error("DefineVector: unreachable RVec init dispatch.");
}

template <typename TargetT>
ROOT::RDF::RNode dispatchRVecAppend(ROOT::RDF::RNode df,
                                    const std::string &name,
                                    const std::string &currentColumn,
                                    const std::string &nextColumn,
                                    DefineVectorTypeToken inputType) {
  switch (inputType) {
  case DefineVectorTypeToken::Bool:
    return defineRVecVectorAppend<TargetT, Bool_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Float:
    return defineRVecVectorAppend<TargetT, Float_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Double:
    return defineRVecVectorAppend<TargetT, Double_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Int:
    return defineRVecVectorAppend<TargetT, Int_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UInt:
    return defineRVecVectorAppend<TargetT, UInt_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Short:
    return defineRVecVectorAppend<TargetT, Short_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UShort:
    return defineRVecVectorAppend<TargetT, UShort_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Char:
    return defineRVecVectorAppend<TargetT, Char_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::UChar:
    return defineRVecVectorAppend<TargetT, UChar_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Long64:
    return defineRVecVectorAppend<TargetT, Long64_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::ULong64:
    return defineRVecVectorAppend<TargetT, ULong64_t>(df, name, currentColumn, nextColumn);
  case DefineVectorTypeToken::Unsupported:
    throw std::runtime_error("DefineVector: unsupported RVec input type in compiled path.");
  }

  throw std::runtime_error("DefineVector: unreachable RVec append dispatch.");
}

template <typename TargetT>
ROOT::RDF::RNode defineScalarVectorChain(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::vector<DefineVectorTypeToken> &inputTypes) {
  if (columns.empty()) {
    return defineEmptyVectorColumn<TargetT>(df, name);
  }

  std::string currentName = (columns.size() == 1) ? name : name + "__defineVector_scalar_0";
  df = dispatchScalarInit<TargetT>(df, currentName, columns[0], inputTypes[0]);
  for (size_t i = 1; i < columns.size(); ++i) {
    const std::string nextName = (i + 1 == columns.size())
                                     ? name
                                     : name + "__defineVector_scalar_" + std::to_string(i);
    df = dispatchScalarAppend<TargetT>(df, nextName, currentName, columns[i], inputTypes[i]);
    currentName = nextName;
  }
  return df;
}

template <typename TargetT>
ROOT::RDF::RNode defineRVecVectorChain(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::vector<DefineVectorTypeToken> &inputTypes) {
  if (columns.empty()) {
    return defineEmptyVectorColumn<TargetT>(df, name);
  }

  std::string currentName = (columns.size() == 1) ? name : name + "__defineVector_rvec_0";
  df = dispatchRVecInit<TargetT>(df, currentName, columns[0], inputTypes[0]);
  for (size_t i = 1; i < columns.size(); ++i) {
    const std::string nextName = (i + 1 == columns.size())
                                     ? name
                                     : name + "__defineVector_rvec_" + std::to_string(i);
    df = dispatchRVecAppend<TargetT>(df, nextName, currentName, columns[i], inputTypes[i]);
    currentName = nextName;
  }
  return df;
}

template <typename TargetT>
ROOT::RDF::RNode defineCompiledVectorColumn(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::vector<DefineVectorTypeToken> &inputTypes, bool allScalar) {
  if (allScalar) {
    return defineScalarVectorChain<TargetT>(df, name, columns, inputTypes);
  }
  return defineRVecVectorChain<TargetT>(df, name, columns, inputTypes);
}

bool hasUnsupportedInputTypes(const std::vector<DefineVectorTypeToken> &inputTypes) {
  return std::any_of(inputTypes.begin(), inputTypes.end(),
                     [](DefineVectorTypeToken token) {
                       return token == DefineVectorTypeToken::Unsupported;
                     });
}

} // namespace


/**
 * @brief Construct a new DataManager object
 * @param configProvider Reference to the configuration provider
 */
DataManager::DataManager(const IConfigurationProvider &configProvider)
    : chain_vec_m(makeTChain(configProvider)), df_m(ROOT::RDataFrame(1)) {

    // Attach friend trees (from friendConfig) BEFORE wrapping in RDataFrame
    // so that all friend branches are visible to the RDataFrame at creation.
    const std::string friendConfigFile = configProvider.get("friendConfig");
    if (!friendConfigFile.empty() && std::filesystem::exists(friendConfigFile)) {
      registerFriendTrees(configProvider);
    }

    // Fall back to a small in-memory dataframe (1 entry) if no input files were found
    // This allows unit tests to define variables and perform simple operations
    // that expect at least one row.
    if (!(chain_vec_m.empty()) && chain_vec_m[0]->GetEntries() > 0) {
      df_m = ROOT::RDataFrame(*chain_vec_m[0]);

      // Apply optional entry-range restriction.
      // Written by law tasks when partition='entry_range' is selected.
      // Both keys must be present; if only one is set the range is ignored.
      // Note: Range() disables implicit multi-threading (ImplicitMT) when
      // used with ROOT < 6.28.  In entry_range partition mode each condor
      // job is a separate process, so per-job parallelism is unaffected;
      // only the in-process thread count is restricted to 1 on older ROOT.
      const std::string firstEntryStr = configProvider.get("firstEntry");
      const std::string lastEntryStr  = configProvider.get("lastEntry");
      if (!firstEntryStr.empty() && !lastEntryStr.empty()) {
        const ULong64_t firstEntry = std::stoull(firstEntryStr);
        const ULong64_t lastEntry  = std::stoull(lastEntryStr);
        if (lastEntry > firstEntry) {
          df_m = df_m.Range(firstEntry, lastEntry);
          std::cout << "Entry range applied: [" << firstEntry << ", "
                    << lastEntry << ")" << std::endl;
        } else {
          std::cerr << "Warning: firstEntry (" << firstEntry
                    << ") >= lastEntry (" << lastEntry
                    << "); entry range ignored." << std::endl;
        }
      }
    } else {
      std::cout << "No input files found; using single-entry in-memory RDataFrame for testing." << std::endl;
    }

    // Display a progress bar depending on batch status and ROOT version
  #if defined(HAS_ROOT_PROGRESS_BAR)
    auto batch = configProvider.get("batch");
    if (batch != "True" && batch != "") {
      ROOT::RDF::Experimental::AddProgressBar(df_m);
    } else {
      //if (verbosityLevel_m >= 1) {
      std::cout << "Batch mode, no progress bar" << std::endl;
      //}
    }
  #else
    if (verbosityLevel_m >= 1) {
      std::cout << "ROOT version does not support progress bar, update to at "
                  "least 6.28 to get it."
                << std::endl;
    }
  #endif


      }

/**
 * @brief Construct a new DataManager object for testing with an in-memory RDataFrame
 * @param nEntries Number of entries for the in-memory RDataFrame
 */
DataManager::DataManager(size_t nEntries)
    : chain_vec_m(), df_m(ROOT::RDataFrame(nEntries)) {}

/**
 * @brief Get the current RDataFrame node
 * @return The current RNode
 */
ROOT::RDF::RNode DataManager::getDataFrame() { return df_m; }

/**
 * @brief Set the current RDataFrame node
 * @param node The RNode to set
 */
void DataManager::setDataFrame(const ROOT::RDF::RNode &node) { df_m = node; }

/**
 * @brief Get the main TChain pointer
 * @return Pointer to the main TChain
 */
TChain &DataManager::getChain() const { return *chain_vec_m[0]; }

/**
 * @brief Define a vector variable in the dataframe.
 *
 * If all input columns are scalars, this creates a vector from them. If all
 * input columns are RVecs, this concatenates them and casts every element to
 * the target type. Mixed scalar/RVec input is rejected. Common framework types
 * use compiled RDataFrame callables; unsupported type combinations fall back to
 * the legacy JIT expression path to preserve compatibility.
 * @param name Name of the variable
 * @param columns Input columns (scalars or vectors)
 * @param type Data type (default: Float_t)
 * @param systematicManager Reference to the systematic manager
 */
void DataManager::DefineVector(std::string name,
                               const std::vector<std::string> &columns,
                               std::string type,
                               ISystematicManager &systematicManager) {
  (void)systematicManager;
  //std::cout << "[DataManager] Defining vector column " << name << std::endl;

  const auto existingColumns = df_m.GetColumnNames();
  if (std::find(existingColumns.begin(), existingColumns.end(), name) != existingColumns.end()) {
    std::cout << "[DataManager] Vector column " << name << " already exists, skipping." << std::endl;
    return;
  }

  // Sanity-check that all requested columns exist in the dataframe.
  std::vector<std::string> missing;
  for (const auto &c : columns) {
    if (std::find(existingColumns.begin(), existingColumns.end(), c) == existingColumns.end()) {
      missing.push_back(c);
    }
  }
  if (!missing.empty()) {
    std::string msg = "DefineVector: missing columns: ";
    for (const auto &m : missing) {
      msg += m + " ";
    }
    throw std::runtime_error(msg);
  }

  // Determine the type of each column (scalar or RVec) and normalize the
  // element/scalar types for the compiled fast path.
  std::vector<bool> isRVec;
  std::vector<DefineVectorTypeToken> inputTypes;
  inputTypes.reserve(columns.size());
  for (const auto& col : columns) {
    const std::string colType = df_m.GetColumnType(col);
    const bool columnIsRVec = isRVecColumnType(colType);
    isRVec.push_back(columnIsRVec);
    inputTypes.push_back(
        tokenizeDefineVectorType(columnIsRVec ? extractRVecElementType(colType)
                                              : colType));
  }

  bool allRVec = std::all_of(isRVec.begin(), isRVec.end(), [](bool v){ return v; });
  bool allScalar = std::none_of(isRVec.begin(), isRVec.end(), [](bool v){ return v; });

  if (!(allRVec || allScalar)) {
    throw std::runtime_error("DefineVector: Mixed scalar and RVec types are not supported.");
  }

  const auto targetType = tokenizeDefineVectorType(type);
  const bool useFallback =
      targetType == DefineVectorTypeToken::Unsupported ||
      hasUnsupportedInputTypes(inputTypes);

  if (useFallback) {
    const std::string expr = allScalar
                                 ? buildScalarDefineVectorExpression(columns, type)
                                 : buildRVecDefineVectorExpression(columns, type);
    df_m = df_m.Define(name, expr);
    std::cout << "[DataManager] Vector column " << name
              << " defined via JIT fallback." << std::endl;
    std::cout << "Expression: \n" << expr << std::endl;
    return;
  }

  switch (targetType) {
  case DefineVectorTypeToken::Bool:
    df_m = defineCompiledVectorColumn<Bool_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Float:
    df_m = defineCompiledVectorColumn<Float_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Double:
    df_m = defineCompiledVectorColumn<Double_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Int:
    df_m = defineCompiledVectorColumn<Int_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::UInt:
    df_m = defineCompiledVectorColumn<UInt_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Short:
    df_m = defineCompiledVectorColumn<Short_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::UShort:
    df_m = defineCompiledVectorColumn<UShort_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Char:
    df_m = defineCompiledVectorColumn<Char_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::UChar:
    df_m = defineCompiledVectorColumn<UChar_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Long64:
    df_m = defineCompiledVectorColumn<Long64_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::ULong64:
    df_m = defineCompiledVectorColumn<ULong64_t>(df_m, name, columns, inputTypes, allScalar);
    break;
  case DefineVectorTypeToken::Unsupported:
    throw std::runtime_error("DefineVector: unsupported target type in compiled path.");
  }

  /*
  std::cout << "[DataManager] Vector column " << name
            << " defined via compiled RDataFrame transforms."
            << std::endl;
  */

}




/**
 * @brief Register constant variables from configuration
 * @param configProvider Reference to the configuration provider
 */
void DataManager::registerConstants(const IConfigurationProvider &configProvider, const std::string& floatConfigKey, const std::string& intConfigKey) {
  std::string floatFile = configProvider.get(floatConfigKey);
  if (!floatFile.empty()) {
    auto floatConfig = configProvider.parsePairBasedConfig(floatFile);
    for (auto &pair : floatConfig) {
      float val = std::stof(pair.second);
      defineConstant(pair.first, val);
    }
  }
  std::string intFile = configProvider.get(intConfigKey);
  if (!intFile.empty()) {
    auto intConfig = configProvider.parsePairBasedConfig(intFile);
    for (auto &pair : intConfig) {
      int val = std::stoi(pair.second);
      defineConstant(pair.first, val);
    }
  }
}

/**
 * @brief Register aliases from configuration
 * @param configProvider Reference to the configuration provider
 */
void DataManager::registerAliases(const IConfigurationProvider &configProvider, const std::string& aliasConfigKey) {
  auto aliasConfig = configProvider.parseMultiKeyConfig(
      configProvider.get(aliasConfigKey) , {"existingName", "newName"});
  const auto columnNames = df_m.GetColumnNames();
  for (const auto &entryKeys : aliasConfig) {
    const auto& existing = entryKeys.at("existingName");
    const auto& alias = entryKeys.at("newName");
    if (std::find(columnNames.begin(), columnNames.end(), existing) == columnNames.end()) {
      std::cout << "Alias skipped: missing column " << existing << std::endl;
      continue;
    }
    std::cout << "Aliasing " << existing << " to " << alias << std::endl;
    df_m = df_m.Alias(alias, existing);
  }
}

/**
 * @brief Register optional branches from configuration
 * @param configProvider Reference to the configuration provider
 */
void DataManager::registerOptionalBranches(
    const IConfigurationProvider &configProvider, const std::string& optionalBranchesConfigKey) {
  const auto aliasConfig = configProvider.parseMultiKeyConfig(
      configProvider.get(optionalBranchesConfigKey), {"name", "type", "default"});
  std::cout << "Optional branches config: " << optionalBranchesConfigKey << std::endl;

#if defined(HAS_DEFAULT_VALUE_FOR)
  // Precompute existing columns to avoid calling DefaultValueFor on missing
  // columns (which would throw). If a column does not exist, create it via
  // saveVar which safely defines a per-sample constant.
  const auto existingColumns = df_m.GetColumnNames();
  std::unordered_set<std::string> existingSet(existingColumns.begin(), existingColumns.end());
  for (const auto &entryKeys : aliasConfig) {
    const int varType = std::stoi(entryKeys.at("type"));
    const auto defaultValStr = entryKeys.at("default");
    const auto varName = entryKeys.at("name");
    const Bool_t defaultBool = defaultValStr == "1" ||
                               defaultValStr == "true" ||
                               defaultValStr == "True";
    const bool columnExists = existingSet.find(varName) != existingSet.end();
    if (columnExists) {
      switch (varType) {
      case 0:
        df_m = df_m.DefaultValueFor<UInt_t>(varName, std::stoul(defaultValStr));
        break;
      case 1:
        df_m = df_m.DefaultValueFor<Int_t>(varName, std::stoi(defaultValStr));
        break;
      case 2:
        df_m = df_m.DefaultValueFor<UShort_t>(varName, std::stoul(defaultValStr));
        break;
      case 3:
        df_m = df_m.DefaultValueFor<Short_t>(varName, std::stoi(defaultValStr));
        break;
      case 4:
        df_m = df_m.DefaultValueFor<UChar_t>(varName, UChar_t(std::stoul(defaultValStr)));
        break;
      case 5:
        df_m = df_m.DefaultValueFor<Char_t>(varName, Char_t(std::stoi(defaultValStr)));
        break;
      case 6:
        df_m = df_m.DefaultValueFor<Float_t>(varName, std::stof(defaultValStr));
        break;
      case 7:
        df_m = df_m.DefaultValueFor<Double_t>(varName, std::stod(defaultValStr));
        break;
      case 8:
        df_m = df_m.DefaultValueFor<Bool_t>(varName, defaultBool);
        break;
      case 10:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UInt_t>>(varName, {static_cast<UInt_t>(std::stoul(defaultValStr))});
        break;
      case 11:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Int_t>>(varName, {std::stoi(defaultValStr)});
        break;
      case 12:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UShort_t>>(varName, {static_cast<UShort_t>(std::stoul(defaultValStr))});
        break;
      case 13:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Short_t>>(varName, {static_cast<Short_t>(std::stoi(defaultValStr))});
        break;
      case 14:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UChar_t>>(varName, {UChar_t(std::stoul(defaultValStr))});
        break;
      case 15:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Char_t>>(varName, {Char_t(std::stoi(defaultValStr))});
        break;
      case 16:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Float_t>>(varName, {std::stof(defaultValStr)});
        break;
      case 17:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Double_t>>(varName, {std::stod(defaultValStr)});
        break;
      case 18:
        df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Bool_t>>(varName, {defaultBool});
        break;
      }
    } else {
      // Column missing — define it via saveVar
      switch (varType) {
      case 0:
        df_m = saveVar<UInt_t>(std::stoul(defaultValStr), varName, df_m);
        break;
      case 1:
        df_m = saveVar<Int_t>(std::stoi(defaultValStr), varName, df_m);
        break;
      case 2:
        df_m = saveVar<UShort_t>(static_cast<UShort_t>(std::stoul(defaultValStr)), varName, df_m);
        break;
      case 3:
        df_m = saveVar<Short_t>(static_cast<Short_t>(std::stoi(defaultValStr)), varName, df_m);
        break;
      case 4:
        df_m = saveVar<UChar_t>(UChar_t(std::stoul(defaultValStr)), varName, df_m);
        break;
      case 5:
        df_m = saveVar<Char_t>(Char_t(std::stoi(defaultValStr)), varName, df_m);
        break;
      case 6:
        df_m = saveVar<Float_t>(std::stof(defaultValStr), varName, df_m);
        break;
      case 7:
        df_m = saveVar<Double_t>(std::stod(defaultValStr), varName, df_m);
        break;
      case 8:
        df_m = saveVar<Bool_t>(defaultBool, varName, df_m);
        break;
      case 10:
        df_m = saveVar<ROOT::VecOps::RVec<UInt_t>>(ROOT::VecOps::RVec<UInt_t>{static_cast<UInt_t>(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 11:
        df_m = saveVar<ROOT::VecOps::RVec<Int_t>>(ROOT::VecOps::RVec<Int_t>{std::stoi(defaultValStr)}, varName, df_m);
        break;
      case 12:
        df_m = saveVar<ROOT::VecOps::RVec<UShort_t>>(ROOT::VecOps::RVec<UShort_t>{static_cast<UShort_t>(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 13:
        df_m = saveVar<ROOT::VecOps::RVec<Short_t>>(ROOT::VecOps::RVec<Short_t>{static_cast<Short_t>(std::stoi(defaultValStr))}, varName, df_m);
        break;
      case 14:
        df_m = saveVar<ROOT::VecOps::RVec<UChar_t>>(ROOT::VecOps::RVec<UChar_t>{UChar_t(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 15:
        df_m = saveVar<ROOT::VecOps::RVec<Char_t>>(ROOT::VecOps::RVec<Char_t>{Char_t(std::stoi(defaultValStr))}, varName, df_m);
        break;
      case 16:
        df_m = saveVar<ROOT::VecOps::RVec<Float_t>>(ROOT::VecOps::RVec<Float_t>{std::stof(defaultValStr)}, varName, df_m);
        break;
      case 17:
        df_m = saveVar<ROOT::VecOps::RVec<Double_t>>(ROOT::VecOps::RVec<Double_t>{std::stod(defaultValStr)}, varName, df_m);
        break;
      case 18:
        df_m = saveVar<ROOT::VecOps::RVec<Bool_t>>(ROOT::VecOps::RVec<Bool_t>{defaultBool}, varName, df_m);
        break;
      }
    }
  }
#else
  const auto columnNames = df_m.GetColumnNames();
  std::unordered_set<std::string> columnSet;
  for (auto &column : columnNames) {
    columnSet.insert(column);
  }
  for (const auto &entryKeys : aliasConfig) {
    std::cout << "Processing optional branch " << entryKeys.at("name") << std::endl;
    if (columnSet.find(entryKeys.at("name")) == columnSet.end()) {
      const int varType = std::stoi(entryKeys.at("type"));
      const auto defaultValStr = entryKeys.at("default");
      const auto varName = entryKeys.at("name");
      const Bool_t defaultBool = defaultValStr == "1" ||
                                 defaultValStr == "true" ||
                                 defaultValStr == "True";
      //std::cout << "Defining optional branch " << varName << " with default " << defaultValStr << " and type number " << varType << std::endl;
      switch (varType) {
      case 0:
        df_m = saveVar<UInt_t>(std::stoul(defaultValStr), varName, df_m);
        break;
      case 1:
        df_m = saveVar<Int_t>(std::stoi(defaultValStr), varName, df_m);
        break;
      case 2:
        df_m = saveVar<UShort_t>(std::stoul(defaultValStr), varName, df_m);
        break;
      case 3:
        df_m = saveVar<Short_t>(std::stoi(defaultValStr), varName, df_m);
        break;
      case 4:
        df_m = saveVar<UChar_t>(UChar_t(std::stoul(defaultValStr)), varName, df_m);
        break;
      case 5:
        df_m = saveVar<Char_t>(Char_t(std::stoi(defaultValStr)), varName, df_m);
        break;
      case 6:
        df_m = saveVar<Float_t>(std::stof(defaultValStr), varName, df_m);
        break;
      case 7:
        df_m = saveVar<Double_t>(std::stod(defaultValStr), varName, df_m);
        break;
      case 8:
        df_m = saveVar<Bool_t>(defaultBool, varName, df_m);
        break;
      case 10:
        df_m = saveVar<ROOT::VecOps::RVec<UInt_t>>(ROOT::VecOps::RVec<UInt_t>{static_cast<UInt_t>(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 11:
        df_m = saveVar<ROOT::VecOps::RVec<Int_t>>(ROOT::VecOps::RVec<Int_t>{std::stoi(defaultValStr)}, varName, df_m);
        break;
      case 12:
        df_m = saveVar<ROOT::VecOps::RVec<UShort_t>>(ROOT::VecOps::RVec<UShort_t>{static_cast<UShort_t>(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 13:
        df_m =  saveVar<ROOT::VecOps::RVec<Short_t>>(ROOT::VecOps::RVec<Short_t>{static_cast<Short_t>(std::stoi(defaultValStr))}, varName, df_m);
        break;
      case 14:
        df_m = saveVar<ROOT::VecOps::RVec<UChar_t>>(ROOT::VecOps::RVec<UChar_t>{UChar_t(std::stoul(defaultValStr))}, varName, df_m);
        break;
      case 15:
        df_m = saveVar<ROOT::VecOps::RVec<Char_t>>(ROOT::VecOps::RVec<Char_t>{Char_t(std::stoi(defaultValStr))}, varName, df_m);
        break;
      case 16:
        df_m = saveVar<ROOT::VecOps::RVec<Float_t>>(ROOT::VecOps::RVec<Float_t>{std::stof(defaultValStr)}, varName, df_m);
        break;
      case 17:
        df_m = saveVar<ROOT::VecOps::RVec<Double_t>>(ROOT::VecOps::RVec<Double_t>{std::stod(defaultValStr)}, varName, df_m);
        break;
      case 18:
        df_m = saveVar<ROOT::VecOps::RVec<Bool_t>>(ROOT::VecOps::RVec<Bool_t>{defaultBool}, varName, df_m);
        break;
      }
    }
  }
#endif
}

/**
 * @brief Finalize setup after all configuration is loaded
 * @param configProvider Reference to the configuration provider
 */
void DataManager::finalizeSetup(const IConfigurationProvider &configProvider,
                                const std::string& floatConfigKey,
                                const std::string& intConfigKey,
                                const std::string& aliasConfigKey,
                                const std::string& optionalBranchesConfigKey) {
  std::cout << "Finalizing setup" << std::endl;
  const auto floatConfig = configProvider.get(floatConfigKey);
  const auto intConfig = configProvider.get(intConfigKey);
  if ((!floatConfig.empty() && std::filesystem::exists(floatConfig)) ||
      (!intConfig.empty() && std::filesystem::exists(intConfig))) {
    std::cout << "Registering constants" << std::endl;
    registerConstants(configProvider, floatConfigKey, intConfigKey);
  }

  const auto aliasConfig = configProvider.get(aliasConfigKey);
  if (!aliasConfig.empty() && std::filesystem::exists(aliasConfig)) {
    std::cout << "Registering aliases" << std::endl;
    registerAliases(configProvider, aliasConfigKey);
  }

  const auto optionalBranchesConfig = configProvider.get(optionalBranchesConfigKey);
  if (!optionalBranchesConfig.empty() && std::filesystem::exists(optionalBranchesConfig)) {
    std::cout << "Registering optional branches" << std::endl;
    registerOptionalBranches(configProvider, optionalBranchesConfigKey);
  }
}

/**
 * @brief Attach a single friend tree specified by @p spec to the main TChain.
 */
void DataManager::attachFriendTree(const FriendTreeSpec &spec) {
  // Suppress non-fatal ROOT warnings (e.g. missing dictionaries for CMSSW
  // residual objects in NanoAOD files) during friend file discovery.
  ROOTWarningGuard warningGuard;

  auto friendChain = std::make_unique<TChain>(spec.treeName.c_str());

  if (!spec.files.empty()) {
    for (const auto &f : spec.files) {
      std::cout << "[DataManager] Adding friend file: " << f << std::endl;
      friendChain->Add(f.c_str());
    }
  } else if (!spec.directory.empty()) {
    scan(*friendChain, spec.directory, spec.globs, spec.antiglobs);
  } else {
    std::cerr << "[DataManager] Warning: friend tree '" << spec.alias
              << "' has neither fileList nor directory; skipping." << std::endl;
    return;
  }

  const Long64_t nFriendEntries = friendChain->GetEntries();
  if (nFriendEntries == 0) {
    std::cerr << "[DataManager] Warning: friend tree '" << spec.alias
              << "' has no entries (files may not exist or tree is empty)."
              << std::endl;
  }

  // Build an in-memory event index for non-sequential (identifier-based) matching.
  // Skip if the chain is empty to avoid segfaults when no files could be opened.
  if (!spec.indexBranches.empty() && nFriendEntries > 0) {
    const std::string &major = spec.indexBranches[0];
    const std::string minor =
        spec.indexBranches.size() > 1 ? spec.indexBranches[1] : "0";
    std::cout << "[DataManager] Building index on '" << major << "' / '"
              << minor << "' for friend '" << spec.alias << "'" << std::endl;
    friendChain->BuildIndex(major.c_str(), minor.c_str());
  }

  if (!chain_vec_m.empty() && chain_vec_m[0]) {
    chain_vec_m[0]->AddFriend(friendChain.get(), spec.alias.c_str());
    std::cout << "[DataManager] Attached friend tree '" << spec.alias
              << "' (tree='" << spec.treeName << "', entries=" << nFriendEntries
              << ")" << std::endl;
  } else {
    std::cerr << "[DataManager] Warning: no main chain available; cannot "
                 "attach friend tree '"
              << spec.alias << "'." << std::endl;
    return;
  }

  // Retain ownership so the main chain's raw pointer remains valid.
  friend_chains_m.push_back(std::move(friendChain));
}

/**
 * @brief Attach friend trees or sidecar files declared in a YAML config file.
 */
void DataManager::registerFriendTrees(
    const IConfigurationProvider &configProvider,
    const std::string &friendConfigKey) {
  const std::string friendConfigFile = configProvider.get(friendConfigKey);
  if (friendConfigFile.empty()) {
    return;
  }
  if (!std::filesystem::exists(friendConfigFile)) {
    std::cerr << "[DataManager] Warning: friendConfig file '" << friendConfigFile
              << "' not found; skipping friend tree registration." << std::endl;
    return;
  }

  std::cout << "[DataManager] Loading friend tree config from '"
            << friendConfigFile << "'" << std::endl;

  const auto specs = parseFriendTreeConfig(friendConfigFile);
  if (specs.empty()) {
    std::cout << "[DataManager] No friend trees found in config." << std::endl;
    return;
  }

  for (const auto &spec : specs) {
    attachFriendTree(spec);
  }

  // Rebuild the RDataFrame so that friend branches are visible.
  // This is a no-op when called from the constructor (the RDataFrame is
  // rebuilt immediately after this method returns), but is required when
  // registerFriendTrees is invoked programmatically after construction.
  if (!chain_vec_m.empty() && chain_vec_m[0] &&
      chain_vec_m[0]->GetEntries() > 0) {
    df_m = ROOT::RDataFrame(*chain_vec_m[0]);
    std::cout << "[DataManager] RDataFrame rebuilt after attaching "
              << specs.size() << " friend tree(s)." << std::endl;
  }
}

// Add virtual destructor for DataManager
DataManager::~DataManager() = default;

// Add out-of-line definition for IDataFrameProvider virtual destructor
IDataFrameProvider::~IDataFrameProvider() = default;

