#include <api/IConfigurationProvider.h>
#include <algorithm>
#include <ROOT/RVec.hxx>
#include <DataManager.h>
#include <TChain.h>
#include <util.h>
#include <filesystem>

#include <ROOT/RDFHelpers.hxx>
#include <functions.h>


/**
 * @brief Construct a new DataManager object
 * @param configProvider Reference to the configuration provider
 */
DataManager::DataManager(const IConfigurationProvider &configProvider)
    : chain_vec_m(makeTChain(configProvider)), df_m(ROOT::RDataFrame(1)) {

    // Fall back to a small in-memory dataframe (1 entry) if no input files were found
    // This allows unit tests to define variables and perform simple operations
    // that expect at least one row.
    if (!(chain_vec_m.empty()) && chain_vec_m[0]->GetEntries() > 0) {
      df_m = ROOT::RDataFrame(*chain_vec_m[0]);
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
TChain *DataManager::getChain() const { return chain_vec_m[0].get(); }

/**
 * @brief Define a vector variable in the dataframe. If all columns are scalars, creates a vector from them. If all columns are RVecs, concatenates and casts them to the target type. Mixed types are not supported and will throw an error at runtime. Uses a JIT lambda for type deduction and concatenation.
 * @param name Name of the variable
 * @param columns Input columns (scalars or vectors)
 * @param type Data type (default: Float_t)
 * @param systematicManager Reference to the systematic manager
 */
void DataManager::DefineVector(std::string name,
                               const std::vector<std::string> &columns,
                               std::string type,
                               ISystematicManager &systematicManager) {
  std::cout << "[DataManager] Defining vector column " << name << std::endl;

  // Sanity-check that all requested columns exist in the dataframe.
  const auto existingColumns = df_m.GetColumnNames();
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

  // Determine the type of each column (scalar or RVec)
  std::vector<bool> isRVec;
  for (const auto& col : columns) {
    std::string colType = df_m.GetColumnType(col);
    isRVec.push_back(colType.find("RVec") != std::string::npos);
  }

  bool allRVec = std::all_of(isRVec.begin(), isRVec.end(), [](bool v){ return v; });
  bool allScalar = std::none_of(isRVec.begin(), isRVec.end(), [](bool v){ return v; });

  if (!(allRVec || allScalar)) {
    throw std::runtime_error("DefineVector: Mixed scalar and RVec types are not supported.");
  }

  if (allScalar) {
    // Use the original string expression for scalars
    std::string expr = "ROOT::VecOps::RVec<" + type + ">{";
    for (size_t i = 0; i < columns.size(); ++i) {
      expr += "static_cast<" + type + ">(" + columns[i] + ")";
      if (i + 1 < columns.size()) {
        expr += ", ";
      }
    }
    expr += "}";
    df_m = df_m.Define(name, expr);
    std::cout << "[DataManager] Vector column " << name << " defined from scalars." << std::endl;
    return;
  } else {

    // All are RVecs: generate a JIT lambda string that concatenates and casts
    std::string expr = "size_t total = 0;\n";
    for (const auto& col : columns) {
      expr += "total += " + col + ".size();\n";
    }
    expr += "ROOT::VecOps::RVec<" + type + "> out;\n";
    expr += "out.reserve(total);\n";
    for (size_t i = 0; i < columns.size(); ++i) {
      expr += "for (auto& x : " + columns[i] + ") out.push_back(static_cast<" + type + ">(x));\n";
    }
    expr += "return out;";

    df_m = df_m.Define(name, expr);
    std::cout << "[DataManager] Vector column " << name << " defined by concatenating RVecs." << std::endl;
    std::cout << "Expression: \n" << expr << std::endl;
  }
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
    if (columnSet.find(entryKeys.at("name")) == columnSet.end()) {
      const int varType = std::stoi(entryKeys.at("type"));
      const auto defaultValStr = entryKeys.at("default");
      const auto varName = entryKeys.at("name");
      const Bool_t defaultBool = defaultValStr == "1" ||
                                 defaultValStr == "true" ||
                                 defaultValStr == "True";
      switch (varType) {
      case 0:
        SaveVar<UInt_t>(std::stoul(defaultValStr), varName);
        break;
      case 1:
        SaveVar<Int_t>(std::stoi(defaultValStr), varName);
        break;
      case 2:
        SaveVar<UShort_t>(std::stoul(defaultValStr), varName);
        break;
      case 3:
        SaveVar<Short_t>(std::stoi(defaultValStr), varName);
        break;
      case 4:
        SaveVar<UChar_t>(UChar_t(std::stoul(defaultValStr)), varName);
        break;
      case 5:
        SaveVar<Char_t>(Char_t(std::stoi(defaultValStr)), varName);
        break;
      case 6:
        SaveVar<Float_t>(std::stof(defaultValStr), varName);
        break;
      case 7:
        SaveVar<Double_t>(std::stod(defaultValStr), varName);
        break;
      case 8:
        SaveVar<Bool_t>(defaultBool, varName);
        break;
      case 10:
        SaveVar<ROOT::VecOps::RVec<UInt_t>>(
            {static_cast<UInt_t>(std::stoul(defaultValStr))}, varName);
        break;
      case 11:
        SaveVar<ROOT::VecOps::RVec<Int_t>>({std::stoi(defaultValStr)}, varName);
        break;
      case 12:
        SaveVar<ROOT::VecOps::RVec<UShort_t>>(
            {static_cast<UShort_t>(std::stoul(defaultValStr))}, varName);
        break;
      case 13:
        SaveVar<ROOT::VecOps::RVec<Short_t>>(
            {static_cast<Short_t>(std::stoi(defaultValStr))}, varName);
        break;
      case 14:
        SaveVar<ROOT::VecOps::RVec<UChar_t>>(
            {UChar_t(std::stoul(defaultValStr))}, varName);
        break;
      case 15:
        SaveVar<ROOT::VecOps::RVec<Char_t>>({Char_t(std::stoi(defaultValStr))},
                                            varName);
        break;
      case 16:
        SaveVar<ROOT::VecOps::RVec<Float_t>>({std::stof(defaultValStr)},
                                             varName);
        break;
      case 17:
        SaveVar<ROOT::VecOps::RVec<Double_t>>({std::stod(defaultValStr)},
                                              varName);
        break;
      case 18:
        SaveVar<ROOT::VecOps::RVec<Bool_t>>({defaultBool}, varName);
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

// Add virtual destructor for DataManager
DataManager::~DataManager() = default;

// Add out-of-line definition for IDataFrameProvider virtual destructor
IDataFrameProvider::~IDataFrameProvider() = default;

