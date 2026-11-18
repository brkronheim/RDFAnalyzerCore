#include <BDTManager.h>
#include <analyzer.h>
#include <api/IConfigurationProvider.h>
#include <api/IDataFrameProvider.h>
#include <api/ILogger.h>
#include <api/ISystematicManager.h>
#include <map>

/**
 * @brief Construct a new BDTManager object
 * @param configProvider Reference to the configuration provider
 */
BDTManager::BDTManager(IConfigurationProvider const& configProvider) {
  registerBDTs(configProvider);
}

/**
 * @brief Apply a BDT to the input features
 * @param bdtName Name of the BDT
 */
void BDTManager::applyBDT(const std::string &bdtName) {
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("BDTManager: DataManager or SystematicManager not set");
  }
  
  const auto &inputFeatures = getBDTFeatures(bdtName);
  const auto &runVar = getRunVar(bdtName);
  std::string layout = "scalar";
  if (configManager_m) {
    const auto &settings = configManager_m->getConfigMap();
    const auto found = settings.find("bdtInputLayout");
    if (found != settings.end()) layout = found->second;
  }
  if (layout != "scalar" && layout != "flatRows")
    throw std::runtime_error("bdtInputLayout must be scalar or flatRows");
  if (layout == "flatRows") {
    // Collection inference uses an existing row-major float feature matrix.
    // Width/order are the configured model features; rows are event-local.
    std::string input = "input_" + bdtName;
    if (configManager_m) {
      const auto &settings = configManager_m->getConfigMap();
      const auto shared = settings.find("bdtFlatRowsInputColumn");
      if (shared != settings.end()) {
        if (shared->second.empty()) throw std::runtime_error("bdtFlatRowsInputColumn must be nonempty");
        input = shared->second;
      }
    }
    if (inputFeatures.empty() || !dataManager_m->getDataFrame().HasColumn(input))
      throw std::runtime_error("BDT flatRows requires nonempty features and a predefined " + input);
    auto model = objects_m.at(bdtName);
    const auto width = inputFeatures.size();
    dataManager_m->Define(bdtName, [model, width](const ROOT::VecOps::RVec<float> &values, bool run) {
      if (values.size() % width)
        throw std::runtime_error("BDT flatRows input length is not divisible by model feature count");
      ROOT::VecOps::RVec<float> scores(values.size()/width, -1.f);
      if (run) for (size_t row = 0; row < scores.size(); ++row)
        scores[row] = 1. / (1. + std::exp(-(*model)(values.data()+row*width)));
      return scores;
    }, {input, runVar}, *systematicManager_m);
    return;
  }
  dataManager_m->DefineVector("input_" + bdtName, inputFeatures, "Float_t", *systematicManager_m);
  auto bdt = this->objects_m.at(bdtName);
  auto bdtLambda = [bdt](ROOT::VecOps::RVec<Float_t> &inputVector,
                         bool runVar) -> Float_t {
    if (runVar) {
      return (1. / (1. + std::exp(-((*bdt.get())(inputVector.data())))));
    } else {
      return (-1);
    }
  };
  dataManager_m->Define(bdtName, bdtLambda, {"input_" + bdtName, runVar}, *systematicManager_m);
}

/**
 * @brief Apply all BDTs to the dataframe provider
 */
void BDTManager::applyAllBDTs() {
  const auto names = getAllBDTNames();
  bool combineModels = false;
  if (configManager_m) {
    const auto &settings = configManager_m->getConfigMap();
    const auto layout = settings.find("bdtInputLayout");
    if (layout != settings.end() && layout->second != "scalar" && layout->second != "flatRows")
      throw std::runtime_error("bdtInputLayout must be scalar or flatRows");
    if (layout != settings.end() && layout->second == "flatRows" &&
        settings.count("bdtFlatRowsInputColumn") && !names.empty()) {
      for (const auto &name : names) if (getBDTFeatures(name) != getBDTFeatures(names.front()))
        throw std::invalid_argument("Shared flatRows models require identical ordered inputVariables");
    }
    const auto flag = settings.find("bdtCombineModels");
    if (flag != settings.end()) {
      if (flag->second != "true" && flag->second != "false")
        throw std::runtime_error("bdtCombineModels must be true or false");
      combineModels = flag->second == "true";
    }
  }
  if (combineModels && dataManager_m && systematicManager_m) {
    const auto &settings = configManager_m->getConfigMap();
    const auto layout = settings.find("bdtInputLayout");
    if (layout != settings.end() && layout->second == "flatRows") {
      // Keep collection rows independent from the scalar two-model grouping.
      for (const auto &name : names) applyBDT(name);
      return;
    }
    // Batch-local grouping only. Preserve individual helper columns and gates;
    // never replace caller-provided packed inputs or independently applied BDTs.
    std::map<std::vector<std::string>, std::vector<std::string>> groups;
    auto df = dataManager_m->getDataFrame();
    for (const auto &name : names) {
      const auto &gate = getRunVar(name);
      bool gateIsNominal = df.GetColumnType(gate) == "bool";
      for (const auto &label : systematicManager_m->getVariationLabels())
        gateIsNominal = gateIsNominal && systematicManager_m->getVariationColumnName(gate, label) == gate;
      if (df.HasColumn("input_"+name) || df.HasColumn(name) || !gateIsNominal) { applyBDT(name); continue; }
      groups[getBDTFeatures(name)].push_back(name);
    }
    for (const auto &[features, group] : groups) {
      if (group.size() != 2) {
        for (const auto &name : group) applyBDT(name);
        continue;
      }
      std::vector<std::shared_ptr<fastforest::FastForest>> models;
      std::vector<std::string> gates;
      for (const auto &name : group) {
        dataManager_m->DefineVector("input_"+name, features, "Float_t", *systematicManager_m);
        models.push_back(getBDT(name)); gates.push_back(getRunVar(name));
      }
      df = dataManager_m->getDataFrame();
      std::string prefix = "__rdf_bdt_combined";
      while (df.HasColumn(prefix) || df.HasColumn(prefix+"_gates")) prefix += "_";
      dataManager_m->Define(prefix, [models](const ROOT::VecOps::RVec<float> &input, bool a, bool b) {
        ROOT::VecOps::RVec<float> scores(2, -1.f);
        if (a) scores[0] = 1. / (1. + std::exp(-(*models[0])(input.data())));
        if (b) scores[1] = 1. / (1. + std::exp(-(*models[1])(input.data())));
        return scores;
      }, {"input_"+group.front(), gates[0], gates[1]}, *systematicManager_m);
      for (size_t i = 0; i < group.size(); ++i)
        dataManager_m->Define(group[i], [i](const ROOT::VecOps::RVec<float> &scores) { return scores.at(i); },
                             {prefix}, *systematicManager_m);
    }
    return;
  }
  for (const auto &bdtName : names) {
    applyBDT(bdtName);
  }
}

/**
 * @brief Get a BDT object by key
 * @param key BDT key
 * @return Shared pointer to the FastForest object
 */
std::shared_ptr<fastforest::FastForest>
BDTManager::getBDT(const std::string &key) const {
  return getObject(key);
}

/**
 * @brief Get the features for a BDT by key
 * @param key BDT key
 * @return Reference to the vector of feature names
 */
const std::vector<std::string> &
BDTManager::getBDTFeatures(const std::string &key) const {
  return getFeatures(key);
}

/**
 * @brief Get the run variable for a BDT
 * @param bdtName Name of the BDT
 * @return Reference to the run variable string
 */
const std::string &BDTManager::getRunVar(const std::string &bdtName) const {
  auto it = bdt_runVars_m.find(bdtName);
  if (it != bdt_runVars_m.end()) {
    return it->second;
  }
  throw std::runtime_error("RunVar not found for BDT: " + bdtName);
}

/**
 * @brief Get all BDT names
 * @return Vector of all BDT names
 */
std::vector<std::string> BDTManager::getAllBDTNames() const {
  std::vector<std::string> names;
  for (const auto &pair : bdt_runVars_m) {
    names.push_back(pair.first);
  }
  return names;
}

/**
 * @brief Register BDTs from configuration
 * @param configProvider Reference to the configuration provider
 */
void BDTManager::registerBDTs(const IConfigurationProvider &configProvider) {
  const auto bdtConfig = configProvider.parseMultiKeyConfig(
      configProvider.get("bdtConfig"),
      {"file", "name", "inputVariables", "runVar"});

  for (const auto &entryKeys : bdtConfig) {

    // Construction may already have loaded this immutable named resource.
    // Repeated lifecycle setup previously parsed the entire forest again,
    // then discarded it because emplace kept the existing object/features.
    if (objects_m.find(entryKeys.at("name")) != objects_m.end()) continue;

    // Split the variable list on commas, save to vector
    auto inputVariableVector =
        configProvider.splitString(entryKeys.at("inputVariables"), ",");

    // Add BDT
    std::vector<std::string> features = inputVariableVector;

    // Load the BDT
    auto bdt = fastforest::load_txt(entryKeys.at("file"), features);

    // Add the BDT and feature list to their maps
    objects_m.emplace(entryKeys.at("name"),
                      std::make_shared<fastforest::FastForest>(bdt));
    features_m.emplace(entryKeys.at("name"), inputVariableVector);
    bdt_runVars_m.emplace(entryKeys.at("name"), entryKeys.at("runVar"));
  }
} 

void BDTManager::setupFromConfigFile() {
  if (!configManager_m) {
    throw std::runtime_error("BDTManager: ConfigManager not set");
  }
  registerBDTs(*configManager_m);
}
void BDTManager::initialize() {
  std::cout << "BDTManager: initialized with " << bdt_runVars_m.size()
            << " BDT(s)." << std::endl;
}

void BDTManager::reportMetadata() {
  if (!logger_m) return;
  std::string msg = "BDTManager loaded BDTs: ";
  bool first = true;
  for (const auto& name : getAllBDTNames()) {
    if (!first) msg += ", ";
    msg += name;
    first = false;
  }
  logger_m->log(ILogger::Level::Info, msg);
}

std::shared_ptr<BDTManager> BDTManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<BDTManager>(an.getConfigurationProvider());
    an.addPlugin(role, plugin);
    return plugin;
}
