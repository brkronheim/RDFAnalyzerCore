#include <api/IConfigurationProvider.h>
#include <algorithm>
#include <ROOT/RVec.hxx>
#include <DataManager.h>
#include <TChain.h>
#include <iostream>
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

    // Attach friend trees (from friendConfig) BEFORE wrapping in RDataFrame
    // so that all friend branches are visible to the RDataFrame at creation.
    const std::string friendConfigFile = configProvider.get("friendConfig");
    if (!friendConfigFile.empty() && std::filesystem::exists(friendConfigFile)) {
      registerFriendTrees(configProvider);
    }

    // Fall back to a small in-memory dataframe (1 entry) if no input files were found.
    // Use GetNtrees() rather than GetEntries() so that the file-based RDF is used
    // whenever files have been added to the chain.  GetEntries() can return 0 or -1
    // for files written by external tools (e.g. uproot) even when the file is valid
    // and readable, causing a spurious in-memory fallback.
    if (!(chain_vec_m.empty()) && chain_vec_m[0]->GetNtrees() > 0) {
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
    std::cout << "Processing optional branch " << entryKeys.at("name") << std::endl;
    if (columnSet.find(entryKeys.at("name")) == columnSet.end()) {
      const int varType = std::stoi(entryKeys.at("type"));
      const auto defaultValStr = entryKeys.at("default");
      const auto varName = entryKeys.at("name");
      const Bool_t defaultBool = defaultValStr == "1" ||
                                 defaultValStr == "true" ||
                                 defaultValStr == "True";
      std::cout << "Defining optional branch " << varName << " with default " << defaultValStr << " and type number " << varType << std::endl;
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
      chain_vec_m[0]->GetNtrees() > 0) {
    df_m = ROOT::RDataFrame(*chain_vec_m[0]);
    std::cout << "[DataManager] RDataFrame rebuilt after attaching "
              << specs.size() << " friend tree(s)." << std::endl;
  }
}

// Add virtual destructor for DataManager
DataManager::~DataManager() = default;

// Add out-of-line definition for IDataFrameProvider virtual destructor
IDataFrameProvider::~IDataFrameProvider() = default;

