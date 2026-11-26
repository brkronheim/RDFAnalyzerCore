#include "PreFilterRegistration.h"
#include "ConfigBoolean.h"
#include <unordered_set>
#include <api/IConfigurationProvider.h>
#include <algorithm>
#include <cmath>
#include <ROOT/RVec.hxx>
#include <DataManager.h>
#include <InputCache.h>
#include <TChain.h>
#include <RDFColumnPacker.h>
#include <cctype>
#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <util.h>
#include <filesystem>
#include <utility>
#include <limits>
#include <ROOT/RVersion.hxx>

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

bool isRemoteCondorJob() {
  return std::getenv("CONDOR_PROC") != nullptr ||
         std::getenv("CONDOR_CLUSTER") != nullptr ||
         std::getenv("_CONDOR_SCRATCH_DIR") != nullptr;
}

bool isEnabledFlag(std::string value) {
  value = removeWhitespace(std::move(value));
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return std::tolower(c); });
  return value == "true" || value == "1" || value == "yes" || value == "on";
}

float parseFloatConstant(const std::string& name, const std::string& value) {
  try {
    std::size_t consumed = 0;
    const float parsed = std::stof(value, &consumed);
    if (consumed != value.size()) {
      throw std::invalid_argument("trailing characters");
    }
    if (!std::isfinite(parsed)) {
      throw std::invalid_argument("non-finite value");
    }
    return parsed;
  } catch (const std::exception&) {
    throw std::runtime_error("DataManager: invalid float constant '" + name +
                             "=" + value +
                             "' (expected one complete finite float value).");
  }
}

int parseIntConstant(const std::string& name, const std::string& value) {
  try {
    std::size_t consumed = 0;
    const int parsed = std::stoi(value, &consumed);
    if (consumed != value.size()) {
      throw std::invalid_argument("trailing characters");
    }
    return parsed;
  } catch (const std::exception&) {
    throw std::runtime_error("DataManager: invalid integer constant '" + name +
                             "=" + value +
                             "' (expected one complete value in the C++ int range).");
  }
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
ROOT::RDF::RNode defineSmallVectorColumn(
    ROOT::RDF::RNode df, const std::string &name,
    const std::vector<std::string> &columns,
    const std::vector<DefineVectorTypeToken> &inputTypes, bool allScalar) {
  if (columns.empty()) return defineEmptyVectorColumn<TargetT>(df, name);
  return allScalar ? dispatchScalarInit<TargetT>(df, name, columns.front(), inputTypes.front())
                   : dispatchRVecInit<TargetT>(df, name, columns.front(), inputTypes.front());
}


} // namespace


/**
 * @brief Construct a new DataManager object
 * @param configProvider Reference to the configuration provider
 */
namespace {
std::string optionalSetting(const IConfigurationProvider& config, const std::string& key) {
  const auto& values = config.getConfigMap();
  return values.find(key) == values.end() ? std::string{} : config.get(key);
}
}

DataManager::DataManager(const IConfigurationProvider &configProvider)
    : DataManager(configProvider, {}) {}

DataManager::DataManager(const IConfigurationProvider &configProvider,
                         const std::vector<FriendTreeSpec> &friends)
    : chain_vec_m(makeTChain(configProvider)), df_m(ROOT::RDataFrame(0)) {

    // The config-backed constructor is for real input data. Do not silently
    // substitute an in-memory event when the configured files are missing,
    // unreadable, or do not contain the requested tree: that can make a bad
    // production config appear to have completed successfully.
    // Validate a real entry without GetEntries(): counting a lazy TChain
    // opens every remote file, closes those temporary readers, then opens
    // them all again for processing. LoadTree(0) validates the first usable
    // tree and leaves its file open for RDF; later files remain lazy.
    if (chain_vec_m.empty() || !chain_vec_m[0] ||
        chain_vec_m[0]->LoadTree(0) < 0) {
      std::string inputDescription = "no fileList or directory configured";
      const auto &configMap = configProvider.getConfigMap();
      const auto fileList = configMap.find("fileList");
      if (fileList != configMap.end() && !fileList->second.empty()) {
        inputDescription = "fileList='" + fileList->second + "'";
      } else {
        const auto directory = configMap.find("directory");
        if (directory != configMap.end() && !directory->second.empty()) {
          inputDescription = "directory='" + directory->second + "'";
        }
      }

      const auto treeNames = configProvider.getList("treeList", {"Events"});
      std::string treeDescription;
      for (const auto &treeName : treeNames) {
        if (!treeDescription.empty()) {
          treeDescription += ", ";
        }
        treeDescription += treeName;
      }

      throw std::runtime_error(
          "DataManager: no input entries found for tree(s) [" +
          treeDescription + "] from " + inputDescription +
          ". Check that the configured files are readable and contain the "
          "requested tree. For intentional in-memory data in tests, use "
          "DataManager(nEntries).");
    }

    // Attach friend trees (from friendConfig) BEFORE wrapping in RDataFrame
    // so that all friend branches are visible to the RDataFrame at creation.
    std::vector<FriendTreeSpec> sourceFriends;
    const auto &settings = configProvider.getConfigMap();
    const auto configured = settings.find("friendConfig");
    if (configured != settings.end() && !configured->second.empty()) {
      sourceFriends = parseFriendTreeConfig(configProvider.resolveConfigPath(configured->second));
    }
    sourceFriends.insert(sourceFriends.end(), friends.begin(), friends.end());
    std::unordered_set<std::string> friendAliases;
    for (const auto &spec : sourceFriends) {
      if (spec.alias.empty() || !friendAliases.insert(spec.alias).second) {
        throw std::invalid_argument("DataManager: friend aliases must be nonempty and unique: '" +
                                    spec.alias + "'");
      }
      attachSourceFriend(spec);
    }

    df_m = ROOT::RDataFrame(*chain_vec_m[0]);

    // Apply optional entry-range restriction.
    // Worker partitions specify an inclusive begin and exclusive end.
    // Both keys must be present. Legacy filter mode ignores a lone bound;
    // opt-in source mode rejects it. Entry-range workers use one RDF thread.
    const std::string firstEntryStr = optionalSetting(configProvider, "firstEntry");
    const std::string lastEntryStr  = optionalSetting(configProvider, "lastEntry");
    const auto modeSetting=configProvider.getConfigMap().find("entryRangeMode");
    const std::string rangeMode=modeSetting==configProvider.getConfigMap().end() ? "filter":modeSetting->second;
    if(rangeMode!="filter" && rangeMode!="source")
      throw std::invalid_argument("entryRangeMode must be filter or source");
    if(rangeMode=="source" && (firstEntryStr.empty() || lastEntryStr.empty()))
      throw std::invalid_argument("entryRangeMode=source requires firstEntry and lastEntry");
    if (!firstEntryStr.empty() && !lastEntryStr.empty()) {
      if(rangeMode=="source" &&
         (!std::all_of(firstEntryStr.begin(),firstEntryStr.end(),[](unsigned char c){return std::isdigit(c);}) ||
          !std::all_of(lastEntryStr.begin(),lastEntryStr.end(),[](unsigned char c){return std::isdigit(c);})))
        throw std::invalid_argument("source entry bounds must be nonnegative decimal integers");
      const ULong64_t firstEntry = std::stoull(firstEntryStr);
      const ULong64_t lastEntry  = std::stoull(lastEntryStr);
      if (lastEntry > firstEntry) {
        if(rangeMode=="source") {
#if (ROOT_VERSION_CODE >= ROOT_VERSION(6,40,0) && ROOT_VERSION_CODE < ROOT_VERSION(6,41,0)) || \
    ROOT_VERSION_CODE == ROOT_VERSION(6,41,99)
          if(ROOT::IsImplicitMTEnabled())
            throw std::invalid_argument("entryRangeMode=source currently requires threads=1");
          if(lastEntry>static_cast<ULong64_t>(std::numeric_limits<Long64_t>::max()))
            throw std::invalid_argument("source entry range exceeds signed64-bit ROOT limits");
          // This mutates the initial source before consumers capture nodes.
          // Unlike RRange it seeks instead of walking rejected prefix entries.
          // Keep this internal ROOT API opt-in and limited to the tested series.
          ROOT::Internal::RDF::ChangeBeginAndEndEntries(df_m,static_cast<Long64_t>(firstEntry),static_cast<Long64_t>(lastEntry));
#else
          throw std::runtime_error("entryRangeMode=source requires ROOT 6.40.x or 6.42.00-rc1; use filter on this ROOT version");
#endif
        } else {
          if(lastEntry>std::numeric_limits<unsigned int>::max())
            throw std::invalid_argument("ROOT Range uses 32-bit bounds; entryRangeMode=source supports larger ranges on ROOT 6.40.x and 6.42.00-rc1");
          df_m = df_m.Range(static_cast<unsigned int>(firstEntry),static_cast<unsigned int>(lastEntry));
        }
        std::cout << "Entry range applied: [" << firstEntry << ", "
                  << lastEntry << ") mode=" << rangeMode << std::endl;
      } else {
        if(rangeMode=="source")throw std::invalid_argument("source entry range requires lastEntry > firstEntry");
        std::cerr << "Warning: firstEntry (" << firstEntry
                  << ") >= lastEntry (" << lastEntry
                  << "); entry range ignored." << std::endl;
      }
    }

    // Display a progress bar depending on batch status and ROOT version
  #if defined(HAS_ROOT_PROGRESS_BAR)
    const auto batch = optionalSetting(configProvider, "batch");
    const bool remoteJob = isRemoteCondorJob();
    if (!remoteJob && !isEnabledFlag(batch) && !batch.empty()) {
      ROOT::RDF::Experimental::AddProgressBar(df_m);
    } else {
      //if (verbosityLevel_m >= 1) {
      std::cout << "Batch/remote mode, no progress bar" << std::endl;
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

void DataManager::markInputDependenciesIncomplete(const std::string &reason) {
  for (auto &chain : chain_vec_m)
    if (auto *input = dynamic_cast<InputCacheChain *>(chain.get()))
      input->markDependenciesIncomplete(reason);
}

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
  defineVectorImpl(std::move(name), columns, std::move(type), systematicManager,
                   true);
}

void DataManager::DefineVectorNominal(
    std::string name, const std::vector<std::string> &columns, std::string type,
    ISystematicManager &systematicManager) {
  defineVectorImpl(std::move(name), columns, std::move(type), systematicManager,
                   false);
}

void DataManager::defineVectorImpl(std::string name,
                                   const std::vector<std::string> &columns,
                                   std::string type,
                                   ISystematicManager &systematicManager,
                                   bool propagateSystematics) {
  // Define nominal and each paired variation through the same compiled/JIT
  // path.  Keeping this in one helper ensures systematic columns preserve all
  // existing type checks and scalar/RVec conversion behavior.
  auto defineVectorColumn = [&](const std::string &outputName,
                                const std::vector<std::string> &inputColumns) {
    if (df_m.HasColumn(outputName)) {
      return;
    }

    std::vector<std::string> missing;
    for (const auto &column : inputColumns) {
      if (!df_m.HasColumn(column)) {
        missing.push_back(column);
      }
    }
    if (!missing.empty()) {
      std::string msg = "DefineVector: missing columns for '" + outputName + "': ";
      for (const auto &column : missing) {
        msg += column + " ";
      }
      throw std::runtime_error(msg);
    }

    std::vector<bool> isRVec;
    std::vector<std::string> columnTypes;
    std::vector<DefineVectorTypeToken> inputTypes;
    inputTypes.reserve(inputColumns.size());
    for (const auto &column : inputColumns) {
      const std::string colType = df_m.GetColumnType(column);
      columnTypes.push_back(colType);
      const bool columnIsRVec = isRVecColumnType(colType);
      isRVec.push_back(columnIsRVec);
      inputTypes.push_back(
          tokenizeDefineVectorType(columnIsRVec ? extractRVecElementType(colType)
                                                : colType));
    }

    const bool allRVec =
        std::all_of(isRVec.begin(), isRVec.end(), [](bool value) { return value; });
    const bool allScalar =
        std::none_of(isRVec.begin(), isRVec.end(), [](bool value) { return value; });
    if (!(allRVec || allScalar)) {
      throw std::runtime_error(
          "DefineVector: Mixed scalar and RVec types are not supported.");
    }

    const auto targetType = tokenizeDefineVectorType(type);
    const bool useJit = inputColumns.size() > 1 ||
        targetType == DefineVectorTypeToken::Unsupported ||
        (!inputTypes.empty() && inputTypes.front() == DefineVectorTypeToken::Unsupported);
    if (useJit) {
      df_m = rdfcolumnpacker::definePackedVector(df_m, outputName, inputColumns,
          type, allScalar ? rdfcolumnpacker::PackingLayout::Scalars
                          : rdfcolumnpacker::PackingLayout::Concatenate);
      return;
    }

    switch (targetType) {
    case DefineVectorTypeToken::Bool:
      df_m = defineSmallVectorColumn<Bool_t>(df_m, outputName, inputColumns,
                                                inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Float:
      df_m = defineSmallVectorColumn<Float_t>(df_m, outputName, inputColumns,
                                                 inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Double:
      df_m = defineSmallVectorColumn<Double_t>(df_m, outputName, inputColumns,
                                                  inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Int:
      df_m = defineSmallVectorColumn<Int_t>(df_m, outputName, inputColumns,
                                               inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::UInt:
      df_m = defineSmallVectorColumn<UInt_t>(df_m, outputName, inputColumns,
                                                inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Short:
      df_m = defineSmallVectorColumn<Short_t>(df_m, outputName, inputColumns,
                                                 inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::UShort:
      df_m = defineSmallVectorColumn<UShort_t>(df_m, outputName, inputColumns,
                                                  inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Char:
      df_m = defineSmallVectorColumn<Char_t>(df_m, outputName, inputColumns,
                                                inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::UChar:
      df_m = defineSmallVectorColumn<UChar_t>(df_m, outputName, inputColumns,
                                                 inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Long64:
      df_m = defineSmallVectorColumn<Long64_t>(df_m, outputName, inputColumns,
                                                  inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::ULong64:
      df_m = defineSmallVectorColumn<ULong64_t>(df_m, outputName, inputColumns,
                                                   inputTypes, allScalar);
      break;
    case DefineVectorTypeToken::Unsupported:
      throw std::runtime_error(
          "DefineVector: unsupported target type in compiled path.");
    }
  };

  defineVectorColumn(name, columns);
  if (!propagateSystematics) {
    return;
  }

  const std::vector<std::string> systematics(systematicManager.getSystematics().begin(),
                                              systematicManager.getSystematics().end());
  for (const auto &systematic : systematics) {
    std::vector<std::string> upColumns;
    std::vector<std::string> downColumns;
    upColumns.reserve(columns.size());
    downColumns.reserve(columns.size());
    bool affected = false;
    for (const auto &column : columns) {
      const auto upColumn =
          systematicManager.getVariationColumnName(column, systematic + "Up");
      const auto downColumn =
          systematicManager.getVariationColumnName(column, systematic + "Down");
      affected = affected || upColumn != column || downColumn != column;
      upColumns.push_back(upColumn);
      downColumns.push_back(downColumn);
    }
    if (!affected) {
      continue;
    }

    const auto mappedUpName =
        systematicManager.getVariationColumnName(name, systematic + "Up");
    const auto mappedDownName =
        systematicManager.getVariationColumnName(name, systematic + "Down");
    const std::string upName = mappedUpName == name
                                   ? name + "_" + systematic + "Up"
                                   : mappedUpName;
    const std::string downName = mappedDownName == name
                                     ? name + "_" + systematic + "Down"
                                     : mappedDownName;

    defineVectorColumn(upName, upColumns);
    defineVectorColumn(downName, downColumns);
    systematicManager.registerVariationColumns(name, systematic, upName,
                                               downName);
  }

  // Explicit labels also need packed inputs. Without this, a model consuming
  // the pack silently falls back to nominal for an explicitly varied feature.
  for (const auto &label : systematicManager.getExplicitVariationLabels()) {
    std::vector<std::string> inputs;
    for (const auto &column : columns)
      inputs.push_back(systematicManager.getVariationColumnName(column, label));
    if (inputs == columns) continue;
    const auto mapped = systematicManager.getVariationColumnName(name, label);
    const auto output = mapped == name ? name + "_" + label : mapped;
    defineVectorColumn(output, inputs);
    systematicManager.registerVariationColumn(name, label, output);
  }

  /*
  std::cout << "[DataManager] Vector column " << name
            << " defined via compiled RDataFrame transforms."
            << std::endl;
  */

}




void DataManager::registerConstants(const IConfigurationProvider &configProvider, const std::string& floatConfigKey, const std::string& intConfigKey) {
  std::string floatFile = optionalSetting(configProvider, floatConfigKey);
  if (!floatFile.empty()) {
    floatFile = configProvider.resolveConfigPath(floatFile);
  }
  if (!floatFile.empty()) {
    auto floatConfig = configProvider.parsePairBasedConfig(floatFile);
    for (auto &pair : floatConfig) {
      float val = parseFloatConstant(pair.first, pair.second);
      defineConstant(pair.first, val);
    }
  }
  std::string intFile = optionalSetting(configProvider, intConfigKey);
  if (!intFile.empty()) {
    intFile = configProvider.resolveConfigPath(intFile);
  }
  if (!intFile.empty()) {
    auto intConfig = configProvider.parsePairBasedConfig(intFile);
    for (auto &pair : intConfig) {
      int val = parseIntConstant(pair.first, pair.second);
      defineConstant(pair.first, val);
    }
  }
}

void DataManager::registerAliases(const IConfigurationProvider &configProvider, const std::string& aliasConfigKey) {
  auto aliasConfig = configProvider.parseMultiKeyConfig(
      optionalSetting(configProvider, aliasConfigKey) , {"existingName", "newName"});
  if (aliasConfig.empty()) return;
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

void DataManager::registerOptionalBranches(
    const IConfigurationProvider &configProvider, const std::string& settingsKey) {
  const auto recipe = optionalSetting(configProvider, settingsKey);
  const auto entries = configProvider.parseMultiKeyConfig(recipe, {"name", "type", "default"});
  if (entries.empty()) return;
  const auto columns = df_m.GetColumnNames();
  const std::unordered_set<std::string> existing(columns.begin(), columns.end());
  for (const auto& entry : entries) {
    const auto& name = entry.at("name");
    const auto& token = entry.at("type");
    const auto& value = entry.at("default");
    const std::string context = settingsKey + " (" + recipe + "), branch '" + name + "': ";
    int type;
    try {
      std::size_t used = 0;
      type = std::stoi(token, &used);
      if (used != token.size() || type < 0 || type == 9 || type > 18)
        throw std::invalid_argument("type");
    } catch (const std::exception&) {
      throw std::invalid_argument(context + "unsupported type '" + token +
                                  "' (expected 0-8 for scalars or 10-18 for vectors)");
    }
    bool exists = existing.count(name);
    const auto install = [&](auto fallback) {
      if (exists) {
#ifdef HAS_DEFAULT_VALUE_FOR
        df_m = df_m.DefaultValueFor<decltype(fallback)>(name, fallback);
#endif
      } else {
        df_m = saveVar(std::move(fallback), name, df_m);
      }
    };
    // Each vector default is a one-element RVec, as in historical recipes.
    const auto book = [&](auto scalar) {
      if (type >= 10) install(ROOT::VecOps::RVec<decltype(scalar)>{scalar});
      else install(scalar);
    };
    const auto integer = [&](auto exemplar) {
      using T = decltype(exemplar);
      std::size_t used = 0;
      const auto v = std::stoll(value, &used);
      if (used != value.size() || v < std::numeric_limits<T>::lowest() ||
          v > static_cast<long long>(std::numeric_limits<T>::max()))
        throw std::invalid_argument("expected a representable integer");
      return static_cast<T>(v);
    };
    try {
      std::size_t used = 0;
      switch (type % 10) {
        case 0: book(integer(UInt_t{})); break;
        case 1: book(integer(Int_t{})); break;
        case 2: book(integer(UShort_t{})); break;
        case 3: book(integer(Short_t{})); break;
        case 4: book(integer(UChar_t{})); break;
        case 5: book(integer(Char_t{})); break;
        case 6: {
          const auto v = std::stof(value, &used);
          if (used != value.size()) throw std::invalid_argument("expected a float");
          book(v);
          break;
        }
        case 7: {
          const auto v = std::stod(value, &used);
          if (used != value.size()) throw std::invalid_argument("expected a double");
          book(v);
          break;
        }
        case 8:
          if (value.empty()) throw std::invalid_argument("expected a boolean");
          book(rdf_config_detail::parseBooleanConfig(name, value, false));
          break;
      }
    } catch (const std::exception& error) {
      throw std::runtime_error(context + "cannot apply default '" + value +
                               "' of type " + token + ": " + error.what());
    }
  }
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
  // Omission disables the recipe. Check every explicit path before any recipe
  // mutates the graph, including the two independently optional constant files.
  const auto supplied = [&](const std::string& key) {
    const auto value = optionalSetting(configProvider, key);
    if (value.empty()) return false;
    const auto path = configProvider.resolveConfigPath(value);
    if (!std::filesystem::is_regular_file(path))
      throw std::runtime_error("DataManager: configuration key '" + key +
                               "' refers to a missing recipe file: " + path);
    return true;
  };
  const bool floats = supplied(floatConfigKey);
  const bool ints = supplied(intConfigKey);
  const bool aliases = supplied(aliasConfigKey);
  const bool branches = supplied(optionalBranchesConfigKey);
  if (floats || ints) registerConstants(configProvider, floatConfigKey, intConfigKey);
  if (aliases) registerAliases(configProvider, aliasConfigKey);
  if (branches) registerOptionalBranches(configProvider, optionalBranchesConfigKey);
}

/**
 * @brief Attach a single friend tree specified by @p spec to the main TChain.
 */
void DataManager::attachSourceFriend(const FriendTreeSpec &spec) {
  if (spec.files.empty() && spec.directory.empty())
    throw std::invalid_argument("Friend '" + spec.alias + "' needs fileList or directory at source construction");
  if (spec.indexBranches.size() > 2 ||
      std::any_of(spec.indexBranches.begin(), spec.indexBranches.end(),
                  [](const std::string& name) { return name.empty(); }))
    throw std::invalid_argument("Friend '" + spec.alias + "' accepts at most two nonempty indexBranches");
  // Suppress non-fatal ROOT warnings (e.g. missing dictionaries for CMSSW
  // residual objects in NanoAOD files) during friend file discovery.
  ROOTWarningGuard warningGuard;

  std::unique_ptr<TChain> friendChain;
  const auto *primary = chain_vec_m.empty() ? nullptr :
      dynamic_cast<InputCacheChain *>(chain_vec_m.front().get());
  if (primary && primary->statisticsEnabled())
    friendChain = std::make_unique<InputCacheChain>(spec.treeName);
  else
    friendChain = std::make_unique<TChain>(spec.treeName.c_str());

  if (!spec.files.empty()) {
    for (const auto &f : spec.files) {
      std::cout << "[DataManager] Adding friend file: " << f << std::endl;
      friendChain->Add(f.c_str());
    }
  } else {
    scan(*friendChain, spec.directory, spec.globs, spec.antiglobs);
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
    if (friendChain->BuildIndex(major.c_str(), minor.c_str()) < 0 || !friendChain->GetTreeIndex())
      throw std::runtime_error("Cannot build friend index for '" + spec.alias + "' on " + major + ", " + minor);
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

void DataManager::registerFriendTrees(const IConfigurationProvider &, const std::string &) {
  throw std::logic_error("DataManager::registerFriendTrees: the input source already exists. "
                         "Set friendConfig before constructing the Analyzer/DataManager, or use "
                         "DataManager(config, friends).");
}

void DataManager::attachFriendTree(const FriendTreeSpec &) {
  throw std::logic_error("DataManager::attachFriendTree: the input source already exists. "
                         "Pass FriendTreeSpec values to DataManager(config, friends) at construction.");
}

// Add virtual destructor for DataManager
DataManager::~DataManager() = default;

// Add out-of-line definition for IDataFrameProvider virtual destructor
IDataFrameProvider::~IDataFrameProvider() = default;

void IDataFrameProvider::notifyBeforeFilter() {
  rdf::detail::PreFilterRegistration::notify(*this);
}
