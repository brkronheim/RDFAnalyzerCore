#include <GenWeightManager.h>

#include <RtypesCore.h>
#include <analyzer.h>

#include <ROOT/RVec.hxx>

#include <algorithm>
#include <cctype>
#include <cmath>
#include <sstream>
#include <stdexcept>

namespace {

using FloatVec = ROOT::VecOps::RVec<Float_t>;

std::string toLowerCopy(std::string value) {
  std::transform(value.begin(), value.end(), value.begin(),
                 [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
  return value;
}

bool parseBool(const std::string &value, bool fallback) {
  if (value.empty()) {
    return fallback;
  }
  const std::string normalized = toLowerCopy(value);
  return normalized == "1" || normalized == "true" || normalized == "yes" ||
         normalized == "on";
}

float finiteOrOne(float value) {
  return std::isfinite(value) ? value : 1.0f;
}

float vectorValueOrOne(const FloatVec &weights, std::size_t index) {
  if (index >= weights.size()) {
    return 1.0f;
  }
  return finiteOrOne(weights[index]);
}

float lheScaleValue(const FloatVec &weights, int eightEntryIndex) {
  if (eightEntryIndex < 0) {
    return 1.0f;
  }
  std::size_t index = static_cast<std::size_t>(eightEntryIndex);
  if (weights.size() >= 9 && eightEntryIndex >= 4) {
    index = static_cast<std::size_t>(eightEntryIndex + 1);
  }
  return vectorValueOrOne(weights, index);
}

std::string joinVariationLabels(const std::vector<GenWeightVariationSpec> &specs) {
  std::ostringstream output;
  for (std::size_t index = 0; index < specs.size(); ++index) {
    if (index != 0) {
      output << ',';
    }
    output << specs[index].label;
  }
  return output.str();
}

} // namespace

std::shared_ptr<GenWeightManager> GenWeightManager::create(
    Analyzer &an, const std::string &role) {
  auto manager = std::make_shared<GenWeightManager>();
  an.addPlugin(role, manager);
  return manager;
}

void GenWeightManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  systematicManager_m = &ctx.systematics;
  logger_m = &ctx.logger;
}

void GenWeightManager::setupFromConfigFile() {
  enableLheScale_m = configBool("genWeightEnableLheScale", true);
  enablePs_m = configBool("genWeightEnablePs", true);
  enablePdf_m = configBool("genWeightEnablePdf", true);
  enablePdfAs_m = configBool("genWeightEnablePdfAs", true);
  lheScaleBranch_m = configString("genWeightLheScaleBranch", "LHEScaleWeight");
  psBranch_m = configString("genWeightPsBranch", "PSWeight");
  pdfBranch_m = configString("genWeightPdfBranch", "LHEPdfWeight");
  baseWeightColumn_m = configString("genWeightBaseWeightColumn", "genWeight");
  nominalColumn_m = configString("genWeightNominalColumn", "genWeight_nominal");
  pdfFirstLhaId_m = configInt("genWeightPdfFirstLhaId", 325300);
  pdfCount_m = configInt("genWeightPdfCount", 103);
  pdfAsUpOffset_m = configInt("genWeightPdfAsUpOffset", -1);
  pdfAsDownOffset_m = configInt("genWeightPdfAsDownOffset", -1);
  weightMode_m = toLowerCopy(configString("genWeightMode", "sign"));
  if (weightMode_m != "sign" && weightMode_m != "full") {
    throw std::invalid_argument(
        "GenWeightManager: genWeightMode must be 'sign' or 'full', got '" +
        weightMode_m + "'");
  }
}

void GenWeightManager::execute() {
  if (configManager_m && toLowerCopy(configManager_m->get("dtype")) == "data") {
    return;
  }
  defineVariationWeights();
}

void GenWeightManager::defineVariationWeights() {
  if (materialized_m) {
    return;
  }
  if (!dataManager_m || !systematicManager_m) {
    throw std::runtime_error("GenWeightManager: manager context is not set");
  }

  std::string baseWeightCol = baseWeightColumn_m;

  if (!hasColumn(baseWeightCol)) {
    throw std::runtime_error("GenWeightManager: base weight column '" +
                             baseWeightCol + "' is not available");
  }

  const std::string baseWeightType =
      dataManager_m->getDataFrame().GetColumnType(baseWeightCol);
  const bool useDoubleBase =
      baseWeightType == "double" || baseWeightType == "Double_t";

  if (weightMode_m == "sign") {
    const std::string signColumn = "__rdfanalyzer_genWeightSign";
    if (hasColumn(signColumn)) {
      throw std::runtime_error(
          "GenWeightManager: helper column '" + signColumn +
          "' already exists; rename the input column or configure a different "
          "generator-weight setup");
    }
    if (useDoubleBase) {
      dataManager_m->Define(
          signColumn,
          [](double weight) {
            return static_cast<float>((weight > 0.0) - (weight < 0.0));
          },
          {baseWeightCol}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          signColumn,
          [](float weight) {
            return static_cast<float>((weight > 0.0f) - (weight < 0.0f));
          },
          {baseWeightCol}, *systematicManager_m);
    }
    baseWeightCol = signColumn;
  }

  const std::string eventWeightType =
      dataManager_m->getDataFrame().GetColumnType(baseWeightCol);
  const bool useDoubleEventWeight =
      eventWeightType == "double" || eventWeightType == "Double_t";

  // Define the nominal column: copy of the base event weight.
  if (useDoubleEventWeight) {
    dataManager_m->Define(nominalColumn_m,
                          [](double base) {
                            return static_cast<float>(base);
                          },
                          {baseWeightCol}, *systematicManager_m);
  } else {
    dataManager_m->Define(nominalColumn_m,
                          [](float base) {
                            return static_cast<float>(base);
                          },
                          {baseWeightCol}, *systematicManager_m);
  }

  if (enableLheScale_m)
    defineLheScaleWeights(baseWeightCol, useDoubleEventWeight);
  if (enablePs_m) definePsWeights(baseWeightCol, useDoubleEventWeight);
  if (enablePdf_m) definePdfWeights(baseWeightCol, useDoubleEventWeight);
  if (enablePdfAs_m)
    definePdfAsWeights(baseWeightCol, useDoubleEventWeight);

  materialized_m = true;
}

std::unordered_map<std::string, std::string>
GenWeightManager::collectProvenanceEntries() const {
  return {{"nominal_column", nominalColumn_m},
      {"base_weight_column", baseWeightColumn_m},
          {"weight_mode", weightMode_m},
          {"lhe_scale_branch", lheScaleBranch_m},
          {"ps_branch", psBranch_m},
          {"pdf_branch", pdfBranch_m},
          {"pdf_first_lha_id", std::to_string(pdfFirstLhaId_m)},
          {"pdf_count", std::to_string(pdfCount_m)},
          {"variations", joinVariationLabels(variations_m)}};
}

bool GenWeightManager::hasColumn(const std::string &name) const {
  if (!dataManager_m) {
    return false;
  }
  auto df = dataManager_m->getDataFrame();
  const auto columns = df.GetColumnNames();
  return std::find(columns.begin(), columns.end(), name) != columns.end();
}

bool GenWeightManager::configBool(const std::string &key, bool fallback) const {
  if (!configManager_m) {
    return fallback;
  }
  return parseBool(configManager_m->get(key), fallback);
}

int GenWeightManager::configInt(const std::string &key, int fallback) const {
  if (!configManager_m) {
    return fallback;
  }
  const auto value = configManager_m->get(key);
  if (value.empty()) {
    return fallback;
  }
  return std::stoi(value);
}

std::string GenWeightManager::configString(const std::string &key,
                                           const std::string &fallback) const {
  if (!configManager_m) {
    return fallback;
  }
  const auto value = configManager_m->get(key);
  return value.empty() ? fallback : value;
}

void GenWeightManager::defineLheScaleWeights(const std::string &baseWeightCol,
                                            bool useDoubleBase) {
  if (!hasColumn(lheScaleBranch_m)) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Warn,
                    "GenWeightManager: branch '" + lheScaleBranch_m +
                        "' is not available; skipping LHE scale weights");
    }
    return;
  }

  struct ScalePoint {
    const char *label;
    int index;
  };
  const ScalePoint scalePoints[] = {
      {"lhe_scale_muf0p5_mur0p5", 0}, {"lhe_scale_muf1_mur0p5", 1},
      {"lhe_scale_muf2_mur0p5", 2},   {"lhe_scale_muf0p5_mur1", 3},
      {"lhe_scale_muf2_mur1", 4},     {"lhe_scale_muf0p5_mur2", 5},
      {"lhe_scale_muf1_mur2", 6},     {"lhe_scale_muf2_mur2", 7},
  };

  for (const auto &point : scalePoints) {
    const std::string label(point.label);
    const std::string column = nominalColumn_m + "_" + label;
    const int index = point.index;
    // Variation weight = base_event_weight * lhe_scale_ratio[index]
    if (useDoubleBase) {
      dataManager_m->Define(
          column,
          [index](double base, const FloatVec &weights) {
            const Float_t weight = lheScaleValue(weights, index);
            return static_cast<float>(base * weight);
          },
          {baseWeightCol, lheScaleBranch_m}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          column,
          [index](float base, const FloatVec &weights) {
            const Float_t weight = lheScaleValue(weights, index);
            return static_cast<float>(base * weight);
          },
          {baseWeightCol, lheScaleBranch_m}, *systematicManager_m);
    }
    registerVariation(label, column, "LHEScaleWeight", index);
  }
}

void GenWeightManager::definePsWeights(const std::string &baseWeightCol,
                                      bool useDoubleBase) {
  if (!hasColumn(psBranch_m)) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Warn,
                    "GenWeightManager: branch '" + psBranch_m +
                        "' is not available; skipping PS weights");
    }
    return;
  }

  struct PsPoint {
    const char *label;
    int index;
  };
  const PsPoint psPoints[] = {{"ps_isr2_fsr1", 0}, {"ps_isr1_fsr2", 1},
                              {"ps_isr0p5_fsr1", 2},
                              {"ps_isr1_fsr0p5", 3}};

  for (const auto &point : psPoints) {
    const std::string label(point.label);
    const std::string column = nominalColumn_m + "_" + label;
    const int index = point.index;
    // Variation weight = base_event_weight * ps_weight[index]
    if (useDoubleBase) {
      dataManager_m->Define(
          column,
          [index](double base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(index)));
          },
          {baseWeightCol, psBranch_m}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          column,
          [index](float base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(index)));
          },
          {baseWeightCol, psBranch_m}, *systematicManager_m);
    }
    registerVariation(label, column, "PSWeight", index);
  }
}

void GenWeightManager::definePdfWeights(const std::string &baseWeightCol,
                                       bool useDoubleBase) {
  if (pdfCount_m <= 0) {
    return;
  }
  if (!hasColumn(pdfBranch_m)) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Warn,
                    "GenWeightManager: branch '" + pdfBranch_m +
                        "' is not available; skipping PDF weights");
    }
    return;
  }

  for (int offset = 0; offset < pdfCount_m; ++offset) {
    const int lhaId = pdfFirstLhaId_m + offset;
    const std::string label = "pdf_" + std::to_string(lhaId);
    const std::string column = nominalColumn_m + "_" + label;
    // Variation weight = base_event_weight * pdf_weight[offset]
    if (useDoubleBase) {
      dataManager_m->Define(
          column,
          [offset](double base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(offset)));
          },
          {baseWeightCol, pdfBranch_m}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          column,
          [offset](float base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(offset)));
          },
          {baseWeightCol, pdfBranch_m}, *systematicManager_m);
    }
    registerVariation(label, column, "LHEPdfWeight", offset);
  }
}

void GenWeightManager::definePdfAsWeights(const std::string &baseWeightCol,
                                          bool useDoubleBase) {
  // αₛ (strong coupling) variations for the PDF set.
  // In the PDF4LHC15 convention, the αₛ variations are stored as the last
  // two entries in the LHEPdfWeight branch:
  //   offset = pdfCount_m - 2  →  αₛ up
  //   offset = pdfCount_m - 1  →  αₛ down
  // The user can override these offsets via config keys
  //   genWeightPdfAsUpOffset and genWeightPdfAsDownOffset.
  if (!hasColumn(pdfBranch_m)) return;

  const int asUpOff = (pdfAsUpOffset_m >= 0)
                          ? pdfAsUpOffset_m
                          : pdfCount_m - 2;
  const int asDownOff = (pdfAsDownOffset_m >= 0)
                            ? pdfAsDownOffset_m
                            : pdfCount_m - 1;

  const std::vector<std::pair<int, std::string>> asEntries = {
      {asUpOff, "pdf_as_up"},
      {asDownOff, "pdf_as_down"}};

  for (const auto &[offset, label] : asEntries) {
    if (offset < 0 || offset >= pdfCount_m) {
      if (logger_m) {
        logger_m->log(ILogger::Level::Warn,
                      "GenWeightManager: αₛ variation '" + label +
                          "' offset " + std::to_string(offset) +
                          " is out of range [0, " +
                          std::to_string(pdfCount_m) + "); skipping");
      }
      continue;
    }
    const std::string column = nominalColumn_m + "_" + label;
    if (useDoubleBase) {
      dataManager_m->Define(
          column,
          [offset](double base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(offset)));
          },
          {baseWeightCol, pdfBranch_m}, *systematicManager_m);
    } else {
      dataManager_m->Define(
          column,
          [offset](float base, const FloatVec &weights) {
            return static_cast<float>(
                base * vectorValueOrOne(weights, static_cast<std::size_t>(offset)));
          },
          {baseWeightCol, pdfBranch_m}, *systematicManager_m);
    }
    registerVariation(label, column, "LHEPdfWeight", offset);
  }
}

void GenWeightManager::registerVariation(const std::string &label,
                                         const std::string &column,
                                         const std::string &source,
                                         int index) {
  systematicManager_m->registerVariationColumn(nominalColumn_m, label, column);
  variations_m.push_back({label, column, source, index});
}
