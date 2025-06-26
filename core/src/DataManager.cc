#include <ConfigurationManager.h>
#include <DataManager.h>
#include <TChain.h>
#include <util.h>

/**
 * @brief Construct a new DataManager object
 * @param configManager Reference to the ConfigurationManager
 */
DataManager::DataManager(const ConfigurationManager &configManager)
    : chain_vec_m(makeTChain(configManager)),
      df_m(ROOT::RDataFrame(*chain_vec_m[0])) {}

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
 * @brief Define a vector variable in the dataframe
 * @param name Name of the variable
 * @param columns Input columns
 * @param type Data type (default: Float_t)
 */
void DataManager::DefineVector(std::string name,
                               const std::vector<std::string> &columns,
                               std::string type) {
  std::string arrayString = "ROOT::VecOps::RVec<" + type + ">({";
  // ... implementation ...
}

/**
 * @brief Make a list of systematic variations for a branch
 * @param branchName Name of the branch
 * @return Vector of systematic variation names
 */
std::vector<std::string>
DataManager::makeSystList(const std::string &branchName) {
  std::vector<std::string> systList = {"Nominal"};
  int var = 0;
  DefinePerSample_m(branchName,
                    [var](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
                      return var;
                    });
  for (const auto &syst : systematicManager_m.getSystematics()) {
    systList.push_back(syst + "Up");
    systList.push_back(syst + "Down");
    var++;
    DefinePerSample_m(
        branchName + "_" + syst + "Up",
        [var](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
          return var;
        });
    var++;
    DefinePerSample_m(
        branchName + "_" + syst + "Down",
        [var](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
          return var;
        });
  }
  return systList;
}

/**
 * @brief Register constant variables from configuration
 * @param configManager Reference to the ConfigurationManager
 */
void DataManager::registerConstants(const ConfigurationManager &configManager) {
  std::string floatFile = configManager.get("floatConfig");
  if (!floatFile.empty()) {
    auto floatConfig = configManager.parsePairBasedConfig(floatFile);
    for (auto &pair : floatConfig) {
      float val = std::stof(pair.second);
      DefinePerSample_m(
          pair.first,
          [val](unsigned int, const ROOT::RDF::RSampleInfo) -> float {
            return val;
          });
    }
  }
  std::string intFile = configManager.get("intConfig");
  if (!intFile.empty()) {
    auto intConfig = configManager.parsePairBasedConfig(intFile);
    for (auto &pair : intConfig) {
      int val = std::stoi(pair.second);
      DefinePerSample_m(
          pair.first, [val](unsigned int, const ROOT::RDF::RSampleInfo) -> int {
            return val;
          });
    }
  }
}

/**
 * @brief Register aliases from configuration
 * @param configManager Reference to the ConfigurationManager
 */
void DataManager::registerAliases(const ConfigurationManager &configManager) {
  auto aliasConfig = configManager.parseMultiKeyConfig(
      "aliasConfig", {"existingName", "newName"});
  const auto columnNames = df_m.GetColumnNames();
  for (const auto &entryKeys : aliasConfig) {
    df_m = df_m.Alias(entryKeys.at("newName"), entryKeys.at("existingName"));
  }
}

/**
 * @brief Register optional branches from configuration
 * @param configManager Reference to the ConfigurationManager
 */
void DataManager::registerOptionalBranches(
    const ConfigurationManager &configManager) {
  const auto aliasConfig = configManager.parseMultiKeyConfig(
      "optionalBranchesConfig", {"name", "type", "default"});
#if defined(HAS_DEFAULT_VALUE_FOR)
  for (const auto &entryKeys : aliasConfig) {
    const int varType = std::stoi(entryKeys.at("type"));
    const auto defaultValStr = entryKeys.at("default");
    const auto varName = entryKeys.at("name");
    const Bool_t defaultBool = defaultValStr == "1" ||
                               defaultValStr == "true" ||
                               defaultValStr == "True";
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
      df_m = df_m.DefaultValueFor<UChar_t>(varName,
                                           UChar_t(std::stoul(defaultValStr)));
      break;
    case 5:
      df_m = df_m.DefaultValueFor<Char_t>(varName,
                                          Char_t(std::stoi(defaultValStr)));
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
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UInt_t>>(
          varName, {static_cast<UInt_t>(std::stoul(defaultValStr))});
      break;
    case 11:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Int_t>>(
          varName, {std::stoi(defaultValStr)});
      break;
    case 12:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UShort_t>>(
          varName, {static_cast<UShort_t>(std::stoul(defaultValStr))});
      break;
    case 13:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Short_t>>(
          varName, {static_cast<Short_t>(std::stoi(defaultValStr))});
      break;
    case 14:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<UChar_t>>(
          varName, {UChar_t(std::stoul(defaultValStr))});
      break;
    case 15:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Char_t>>(
          varName, {Char_t(std::stoi(defaultValStr))});
      break;
    case 16:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Float_t>>(
          varName, {std::stof(defaultValStr)});
      break;
    case 17:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Double_t>>(
          varName, {std::stod(defaultValStr)});
      break;
    case 18:
      df_m = df_m.DefaultValueFor<ROOT::VecOps::RVec<Bool_t>>(varName,
                                                              {defaultBool});
      break;
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
 * @param configManager Reference to the ConfigurationManager
 */
void DataManager::finalizeSetup(const ConfigurationManager &configManager) {
  registerConstants(configManager);
  registerAliases(configManager);
  registerOptionalBranches(configManager);
}