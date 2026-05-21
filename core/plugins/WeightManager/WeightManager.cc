#include <WeightManager.h>
#include <analyzer.h>
#include <NullOutputSink.h>
#include <TFile.h>
#include <TH1D.h>
#include <TNamed.h>
#include <TObject.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <algorithm>
#include <cctype>
#include <limits>
#include <sstream>
#include <stdexcept>

namespace {

enum class WeightScalarTypeToken {
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

std::string removeWeightTypeWhitespace(std::string value) {
  value.erase(std::remove_if(value.begin(), value.end(),
                             [](unsigned char c) { return std::isspace(c) != 0; }),
              value.end());
  return value;
}

bool isUnsupportedWeightVectorType(const std::string &columnType) {
  return columnType.find("RVec<") != std::string::npos;
}

WeightScalarTypeToken tokenizeWeightScalarType(const std::string &typeName) {
  const auto normalized = removeWeightTypeWhitespace(typeName);

  if (normalized == "Bool_t" || normalized == "bool") {
    return WeightScalarTypeToken::Bool;
  }
  if (normalized == "Float_t" || normalized == "float") {
    return WeightScalarTypeToken::Float;
  }
  if (normalized == "Double_t" || normalized == "double") {
    return WeightScalarTypeToken::Double;
  }
  if (normalized == "Int_t" || normalized == "int") {
    return WeightScalarTypeToken::Int;
  }
  if (normalized == "UInt_t" || normalized == "unsignedint") {
    return WeightScalarTypeToken::UInt;
  }
  if (normalized == "Short_t" || normalized == "short") {
    return WeightScalarTypeToken::Short;
  }
  if (normalized == "UShort_t" || normalized == "unsignedshort") {
    return WeightScalarTypeToken::UShort;
  }
  if (normalized == "Char_t" || normalized == "char") {
    return WeightScalarTypeToken::Char;
  }
  if (normalized == "UChar_t" || normalized == "unsignedchar") {
    return WeightScalarTypeToken::UChar;
  }
  if (normalized == "Long64_t" || normalized == "longlong") {
    return WeightScalarTypeToken::Long64;
  }
  if (normalized == "ULong64_t" || normalized == "unsignedlonglong") {
    return WeightScalarTypeToken::ULong64;
  }

  return WeightScalarTypeToken::Unsupported;
}

std::string buildLegacyWeightInitExpression(const std::string &column) {
  return "static_cast<double>(" + column + ")";
}

std::string buildLegacyWeightMultiplyExpression(const std::string &accumulator,
                                                const std::string &column) {
  return accumulator + " * static_cast<double>(" + column + ")";
}

template <typename InputT>
ROOT::RDF::RNode defineWeightAccumulatorInit(ROOT::RDF::RNode df,
                                             const std::string &name,
                                             const std::string &column) {
  return df.Define(
      name,
      [](InputT value) { return static_cast<double>(value); },
      {column});
}

template <typename InputT>
ROOT::RDF::RNode defineWeightAccumulatorMultiply(ROOT::RDF::RNode df,
                                                 const std::string &name,
                                                 const std::string &accumulator,
                                                 const std::string &column) {
  return df.Define(
      name,
      [](double acc, InputT value) { return acc * static_cast<double>(value); },
      {accumulator, column});
}

ROOT::RDF::RNode dispatchWeightAccumulatorInit(ROOT::RDF::RNode df,
                                               const std::string &name,
                                               const std::string &column,
                                               WeightScalarTypeToken inputType) {
  switch (inputType) {
  case WeightScalarTypeToken::Bool:
    return defineWeightAccumulatorInit<Bool_t>(df, name, column);
  case WeightScalarTypeToken::Float:
    return defineWeightAccumulatorInit<Float_t>(df, name, column);
  case WeightScalarTypeToken::Double:
    return defineWeightAccumulatorInit<Double_t>(df, name, column);
  case WeightScalarTypeToken::Int:
    return defineWeightAccumulatorInit<Int_t>(df, name, column);
  case WeightScalarTypeToken::UInt:
    return defineWeightAccumulatorInit<UInt_t>(df, name, column);
  case WeightScalarTypeToken::Short:
    return defineWeightAccumulatorInit<Short_t>(df, name, column);
  case WeightScalarTypeToken::UShort:
    return defineWeightAccumulatorInit<UShort_t>(df, name, column);
  case WeightScalarTypeToken::Char:
    return defineWeightAccumulatorInit<Char_t>(df, name, column);
  case WeightScalarTypeToken::UChar:
    return defineWeightAccumulatorInit<UChar_t>(df, name, column);
  case WeightScalarTypeToken::Long64:
    return defineWeightAccumulatorInit<Long64_t>(df, name, column);
  case WeightScalarTypeToken::ULong64:
    return defineWeightAccumulatorInit<ULong64_t>(df, name, column);
  case WeightScalarTypeToken::Unsupported:
    throw std::runtime_error("WeightManager::defineWeightColumn: unsupported scalar input type in compiled path");
  }

  throw std::runtime_error("WeightManager::defineWeightColumn: unreachable init dispatch");
}

ROOT::RDF::RNode dispatchWeightAccumulatorMultiply(ROOT::RDF::RNode df,
                                                   const std::string &name,
                                                   const std::string &accumulator,
                                                   const std::string &column,
                                                   WeightScalarTypeToken inputType) {
  switch (inputType) {
  case WeightScalarTypeToken::Bool:
    return defineWeightAccumulatorMultiply<Bool_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Float:
    return defineWeightAccumulatorMultiply<Float_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Double:
    return defineWeightAccumulatorMultiply<Double_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Int:
    return defineWeightAccumulatorMultiply<Int_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::UInt:
    return defineWeightAccumulatorMultiply<UInt_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Short:
    return defineWeightAccumulatorMultiply<Short_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::UShort:
    return defineWeightAccumulatorMultiply<UShort_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Char:
    return defineWeightAccumulatorMultiply<Char_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::UChar:
    return defineWeightAccumulatorMultiply<UChar_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Long64:
    return defineWeightAccumulatorMultiply<Long64_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::ULong64:
    return defineWeightAccumulatorMultiply<ULong64_t>(df, name, accumulator, column);
  case WeightScalarTypeToken::Unsupported:
    throw std::runtime_error("WeightManager::defineWeightColumn: unsupported scalar input type in compiled path");
  }

  throw std::runtime_error("WeightManager::defineWeightColumn: unreachable multiply dispatch");
}

bool shouldUseLegacyWeightDefines(const std::vector<std::string> &columnTypes,
                                  const std::vector<WeightScalarTypeToken> &typeTokens) {
  return std::any_of(columnTypes.begin(), columnTypes.end(),
                     [](const std::string &columnType) {
                       return isUnsupportedWeightVectorType(columnType);
                     }) ||
         std::any_of(typeTokens.begin(), typeTokens.end(),
                     [](WeightScalarTypeToken token) {
                       return token == WeightScalarTypeToken::Unsupported;
                     });
}

} // namespace

// ---------------------------------------------------------------------------
// setContext
// ---------------------------------------------------------------------------

void WeightManager::setContext(ManagerContext &ctx) {
  configManager_m = &ctx.config;
  dataManager_m = &ctx.data;
  logger_m = &ctx.logger;
  metaSink_m = &ctx.metaSink;
}

// ---------------------------------------------------------------------------
// Component registration
// ---------------------------------------------------------------------------

void WeightManager::addScaleFactor(const std::string &name,
                                    const std::string &column) {
  if (name.empty())
    throw std::invalid_argument("WeightManager::addScaleFactor: name must not be empty");
  if (column.empty())
    throw std::invalid_argument("WeightManager::addScaleFactor: column must not be empty");
  scaleFactors_m.push_back({name, column});
}

void WeightManager::addNormalization(const std::string &name, double value) {
  if (name.empty())
    throw std::invalid_argument("WeightManager::addNormalization: name must not be empty");
  normalizations_m.push_back({name, value});
}

void WeightManager::addWeightVariation(const std::string &name,
                                        const std::string &upColumn,
                                        const std::string &downColumn) {
  addWeightVariation(name, name, upColumn, downColumn);
}

void WeightManager::addWeightVariation(const std::string &name,
                                        const std::string &componentName,
                                        const std::string &upColumn,
                                        const std::string &downColumn) {
  if (name.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: name must not be empty");
  if (componentName.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: componentName must not be empty");
  if (upColumn.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: upColumn must not be empty");
  if (downColumn.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: downColumn must not be empty");
  variations_m.push_back({name, componentName, upColumn, downColumn});
}

// ---------------------------------------------------------------------------
// Weight column scheduling
// ---------------------------------------------------------------------------

void WeightManager::defineNominalWeight(const std::string &outputColumn) {
  if (outputColumn.empty())
    throw std::invalid_argument("WeightManager::defineNominalWeight: outputColumn must not be empty");
  nominalOutputColumn_m = outputColumn;
  if (dataManager_m) {
    materializeScheduledWeights(false);
  }
}

void WeightManager::defineVariedWeight(const std::string &variationName,
                                        const std::string &direction,
                                        const std::string &outputColumn) {
  if (variationName.empty())
    throw std::invalid_argument("WeightManager::defineVariedWeight: variationName must not be empty");
  if (direction != "up" && direction != "down")
    throw std::invalid_argument("WeightManager::defineVariedWeight: direction must be \"up\" or \"down\"");
  if (outputColumn.empty())
    throw std::invalid_argument("WeightManager::defineVariedWeight: outputColumn must not be empty");
  variedColumnSpecs_m.push_back({variationName, direction, outputColumn});
  if (dataManager_m) {
    materializeScheduledWeights(false);
  }
}

// ---------------------------------------------------------------------------
// Accessors
// ---------------------------------------------------------------------------

const std::string &WeightManager::getNominalWeightColumn() const {
  return nominalOutputColumn_m;
}

std::string WeightManager::getWeightColumn(const std::string &variationName,
                                             const std::string &direction) const {
  for (const auto &[key, col] : variedColumns_m) {
    if (key.variationName == variationName && key.direction == direction)
      return col;
  }
  return {};
}

double WeightManager::getTotalNormalization() const {
  return computeNormProduct();
}

const std::vector<WeightAuditEntry> &WeightManager::getAuditEntries() const {
  return auditEntries_m;
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

double WeightManager::computeNormProduct() const {
  double product = 1.0;
  for (const auto &[name, value] : normalizations_m) {
    product *= value;
  }
  return product;
}

void WeightManager::defineWeightColumn(const std::string &outputColumn,
                                        const std::vector<std::string> &sfColumns) {
  if (!dataManager_m)
    throw std::runtime_error("WeightManager::defineWeightColumn: context not set");

  const double normProduct = computeNormProduct();

  // Build a lambda that multiplies all SF columns together, then scales by norm.
  // We use RDataFrame's variadic Define mechanism with a captured copy of the
  // norm product.  The lambda signature must match the number of input columns,
  // which varies at runtime.  We therefore handle the common cases explicitly
  // and fall back to a generic loop approach via a helper lambda defined below.
  //
  // Strategy: define a helper column "_tmp_wm_product_<outputColumn>" that
  // holds the product of all SF columns (as double), then multiply by normProduct.
  // If there are no SF columns the weight is just the normProduct scalar.

  ROOT::RDF::RNode df = dataManager_m->getDataFrame();

  if (sfColumns.empty()) {
    // No per-event SFs: weight is constant for every event.
    // Use rdfentry_ (always present) so the lambda signature is unambiguous.
    const double w = normProduct;
    auto newDf = df.Define(outputColumn,
        [w](ULong64_t) { return w; },
        {"rdfentry_"});
    dataManager_m->setDataFrame(newDf);
    return;
  }

  std::vector<std::string> columnTypes;
  columnTypes.reserve(sfColumns.size());
  std::vector<WeightScalarTypeToken> typeTokens;
  typeTokens.reserve(sfColumns.size());
  for (const auto &sfColumn : sfColumns) {
    const std::string columnType = df.GetColumnType(sfColumn);
    columnTypes.push_back(columnType);
    typeTokens.push_back(tokenizeWeightScalarType(columnType));
  }

  const bool useLegacyDefines = shouldUseLegacyWeightDefines(columnTypes, typeTokens);

  // Accumulate the product of scale factors step by step since the number of
  // inputs is only known at runtime. Common scalar types use compiled lambdas;
  // unsupported types keep the legacy string-Define fallback.

  // Define the first accumulator column
  std::string prevCol = outputColumn + "_wm_acc_0_";
  if (useLegacyDefines) {
    const std::string sfCol0 = sfColumns[0];
    df = df.Define(prevCol, buildLegacyWeightInitExpression(sfCol0));
  } else {
    df = dispatchWeightAccumulatorInit(df, prevCol, sfColumns[0], typeTokens[0]);
  }

  // Multiply in subsequent SF columns
  for (std::size_t i = 1; i < sfColumns.size(); ++i) {
    std::string nextCol = outputColumn + "_wm_acc_" + std::to_string(i) + "_";
    const std::string sfColI = sfColumns[i];
    const std::string pCol = prevCol;
    if (useLegacyDefines) {
      df = df.Define(nextCol, buildLegacyWeightMultiplyExpression(pCol, sfColI));
    } else {
      df = dispatchWeightAccumulatorMultiply(df, nextCol, pCol, sfColI,
                                             typeTokens[i]);
    }
    prevCol = nextCol;
  }

  // Final column: multiply accumulated product by normProduct
  const std::string lastAcc = prevCol;
  const double np = normProduct;
  auto finalDf = df.Define(outputColumn,
      [np](double acc) { return acc * np; },
      {lastAcc});

  dataManager_m->setDataFrame(finalDf);
}

void WeightManager::materializeScheduledWeights(bool shouldBookAudit) {
  if (!dataManager_m)
    throw std::runtime_error("WeightManager::materializeScheduledWeights: context not set");

  if (!nominalOutputColumn_m.empty() && !nominalMaterialized_m) {
    std::vector<std::string> sfCols;
    sfCols.reserve(scaleFactors_m.size());
    for (const auto &[name, col] : scaleFactors_m) {
      sfCols.push_back(col);
    }

    defineWeightColumn(nominalOutputColumn_m, sfCols);
    nominalMaterialized_m = true;
  }

  for (const auto &spec : variedColumnSpecs_m) {
    const auto alreadyDefined = std::any_of(
        variedColumns_m.begin(), variedColumns_m.end(),
        [&](const auto &entry) {
          return entry.first.variationName == spec.variationName &&
                 entry.first.direction == spec.direction;
        });
    if (alreadyDefined) {
      continue;
    }

    const WeightVariation *var = nullptr;
    for (const auto &v : variations_m) {
      if (v.name == spec.variationName) {
        var = &v;
        break;
      }
    }
    if (!var) {
      throw std::runtime_error(
          "WeightManager::materializeScheduledWeights: variation \"" +
          spec.variationName +
          "\" was not registered via addWeightVariation()");
    }

    const std::string variedSfCol =
        (spec.direction == "up") ? var->upColumn : var->downColumn;

    std::vector<std::string> sfCols;
    bool replaced = false;
    for (const auto &[sfName, sfCol] : scaleFactors_m) {
      if (sfName == var->componentName) {
        sfCols.push_back(variedSfCol);
        replaced = true;
      } else {
        sfCols.push_back(sfCol);
      }
    }
    if (!replaced) {
      sfCols.push_back(variedSfCol);
    }

    defineWeightColumn(spec.outputColumn, sfCols);
    variedColumns_m.push_back({{spec.variationName, spec.direction}, spec.outputColumn});
  }

  if (shouldBookAudit && !auditsBooked_m) {
    if (!nominalOutputColumn_m.empty()) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      bookAudit(nominalOutputColumn_m, nominalOutputColumn_m, df);
    }

    for (const auto &[key, col] : variedColumns_m) {
      ROOT::RDF::RNode df = dataManager_m->getDataFrame();
      const std::string label =
          col + " (" + key.variationName + " " + key.direction + ")";
      bookAudit(label, col, df);
    }
    auditsBooked_m = true;
  }
}

void WeightManager::bookAudit(const std::string &label,
                               const std::string &column,
                               ROOT::RDF::RNode &df) {
  const auto identity = [] {
    AuditAccumulator accumulator;
    accumulator.minWeight = std::numeric_limits<double>::max();
    accumulator.maxWeight = std::numeric_limits<double>::lowest();
    return accumulator;
  }();

  AuditPending ap;
  ap.name = label;
  ap.column = column;
  ap.aggregateResult = df.Aggregate(
      [](AuditAccumulator &accumulator, double weight) {
        accumulator.sumWeights += weight;
        accumulator.minWeight = std::min(accumulator.minWeight, weight);
        accumulator.maxWeight = std::max(accumulator.maxWeight, weight);
        accumulator.negativeCount += static_cast<ULong64_t>(weight < 0.0);
        // Exact equality to 0.0 is intentional: in HEP analyses a weight of exactly
        // zero is typically used to veto out-of-range events. Near-zero but
        // non-zero weights are not considered zero here.
        accumulator.zeroCount += static_cast<ULong64_t>(weight == 0.0);
        accumulator.entryCount += 1U;
      },
      [](AuditAccumulator left, AuditAccumulator right) {
        if (left.entryCount == 0U) {
          return right;
        }
        if (right.entryCount == 0U) {
          return left;
        }
        left.sumWeights += right.sumWeights;
        left.minWeight = std::min(left.minWeight, right.minWeight);
        left.maxWeight = std::max(left.maxWeight, right.maxWeight);
        left.negativeCount += right.negativeCount;
        left.zeroCount += right.zeroCount;
        left.entryCount += right.entryCount;
        return left;
      },
      column, identity);
  auditPending_m.push_back(std::move(ap));
}

// ---------------------------------------------------------------------------
// execute() — define columns and book audit actions
// ---------------------------------------------------------------------------

void WeightManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error("WeightManager::execute: context not set");

  materializeScheduledWeights(true);
}

// ---------------------------------------------------------------------------
// finalize() — retrieve audit results and write to meta file
// ---------------------------------------------------------------------------

void WeightManager::finalize() {
  auditEntries_m.clear();
  auditEntries_m.reserve(auditPending_m.size());

  for (auto &ap : auditPending_m) {
    const auto &aggregate = ap.aggregateResult.GetValue();
    WeightAuditEntry entry;
    entry.name          = ap.name;
    entry.column        = ap.column;
    entry.sumWeights    = aggregate.sumWeights;
    entry.meanWeight    = aggregate.entryCount == 0U
                              ? 0.0
                              : aggregate.sumWeights / static_cast<double>(aggregate.entryCount);
    entry.minWeight     = aggregate.entryCount == 0U ? 0.0 : aggregate.minWeight;
    entry.maxWeight     = aggregate.entryCount == 0U ? 0.0 : aggregate.maxWeight;
    entry.negativeCount = static_cast<long long>(aggregate.negativeCount);
    entry.zeroCount     = static_cast<long long>(aggregate.zeroCount);
    auditEntries_m.push_back(std::move(entry));
  }

  // Write audit results to the meta ROOT file.
  if (dynamic_cast<NullOutputSink *>(metaSink_m) != nullptr) return;

  const std::string fileName =
      metaSink_m->resolveOutputFile(*configManager_m, OutputChannel::Meta);
  if (fileName.empty()) return;

  TFile outFile(fileName.c_str(), "UPDATE");
  if (outFile.IsZombie()) {
    if (logger_m) {
      logger_m->log(ILogger::Level::Error,
                    "WeightManager: failed to open meta output file: " + fileName);
    }
    return;
  }

  // Write per-component normalization factors as TNamed objects.
  for (const auto &[name, value] : normalizations_m) {
    std::string key   = "weight_norm_" + name;
    std::string valStr = std::to_string(value);
    TNamed obj(key.c_str(), valStr.c_str());
    obj.Write(key.c_str(), TObject::kOverwrite);
  }

  // Write total normalization.
  {
    std::string valStr = std::to_string(computeNormProduct());
    TNamed obj("weight_norm_total", valStr.c_str());
    obj.Write("weight_norm_total", TObject::kOverwrite);
  }

  // Write audit statistics as TH1D histograms (one per audited column).
  for (const auto &entry : auditEntries_m) {
    // Summary histogram: bins = sumWeights, mean, min, max.
    const std::string histName = "weight_audit_" + entry.column;
    TH1D hist(histName.c_str(),
              (std::string("Weight audit: ") + entry.name +
               ";Statistic;Value").c_str(),
              4, -0.5, 3.5);
    hist.GetXaxis()->SetBinLabel(1, "sum");
    hist.GetXaxis()->SetBinLabel(2, "mean");
    hist.GetXaxis()->SetBinLabel(3, "min");
    hist.GetXaxis()->SetBinLabel(4, "max");
    hist.SetBinContent(1, entry.sumWeights);
    hist.SetBinContent(2, entry.meanWeight);
    hist.SetBinContent(3, entry.minWeight);
    hist.SetBinContent(4, entry.maxWeight);
    hist.SetDirectory(&outFile);
    hist.Write(histName.c_str(), TObject::kOverwrite);

    // Negative/zero weight count as TNamed.
    {
      const std::string key = histName + "_negCount";
      TNamed obj(key.c_str(), std::to_string(entry.negativeCount).c_str());
      obj.Write(key.c_str(), TObject::kOverwrite);
    }
    {
      const std::string key = histName + "_zeroCount";
      TNamed obj(key.c_str(), std::to_string(entry.zeroCount).c_str());
      obj.Write(key.c_str(), TObject::kOverwrite);
    }
  }

  outFile.Close();
}

// ---------------------------------------------------------------------------
// reportMetadata()
// ---------------------------------------------------------------------------

void WeightManager::reportMetadata() {
  if (!logger_m) return;

  // ---- Normalization summary ----
  if (!normalizations_m.empty()) {
    std::ostringstream ss;
    ss << "WeightManager: normalization factors\n";
    for (const auto &[name, value] : normalizations_m) {
      ss << "  " << name << " = " << value << "\n";
    }
    ss << "  total = " << computeNormProduct() << "\n";
    logger_m->log(ILogger::Level::Info, ss.str());
  }

  // ---- Scale factor summary ----
  if (!scaleFactors_m.empty()) {
    std::ostringstream ss;
    ss << "WeightManager: registered scale factors\n";
    for (const auto &[name, col] : scaleFactors_m) {
      ss << "  " << name << " <- column \"" << col << "\"\n";
    }
    logger_m->log(ILogger::Level::Info, ss.str());
  }

  // ---- Variation summary ----
  if (!variations_m.empty()) {
    std::ostringstream ss;
    ss << "WeightManager: weight variations\n";
    for (const auto &v : variations_m) {
      ss << "  " << v.name
         << " (up: " << v.upColumn
         << ", down: " << v.downColumn << ")\n";
    }
    logger_m->log(ILogger::Level::Info, ss.str());
  }

  // ---- Audit summary ----
  if (!auditEntries_m.empty()) {
    std::ostringstream ss;
    ss << "WeightManager: weight audit\n";
    for (const auto &entry : auditEntries_m) {
      ss << "  [" << entry.name << "]\n"
         << "    sum=" << entry.sumWeights
         << "  mean=" << entry.meanWeight
         << "  min=" << entry.minWeight
         << "  max=" << entry.maxWeight
         << "  neg=" << entry.negativeCount
         << "  zero=" << entry.zeroCount << "\n";
    }
    logger_m->log(ILogger::Level::Info, ss.str());
  }
}

// ---------------------------------------------------------------------------
// collectProvenanceEntries()
// ---------------------------------------------------------------------------

std::unordered_map<std::string, std::string>
WeightManager::collectProvenanceEntries() const {
  std::unordered_map<std::string, std::string> entries;

  // Registered scale factors: "name:column,name:column,..."
  if (!scaleFactors_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < scaleFactors_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << scaleFactors_m[i].first << ':' << scaleFactors_m[i].second;
    }
    entries["scale_factors"] = ss.str();
  }

  // Registered normalization factors: "name:value,name:value,..."
  if (!normalizations_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < normalizations_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << normalizations_m[i].first << ':' << normalizations_m[i].second;
    }
    entries["normalizations"] = ss.str();
  }

  // Registered weight variations: "name(up:col,dn:col),..."
  if (!variations_m.empty()) {
    std::ostringstream ss;
    for (std::size_t i = 0; i < variations_m.size(); ++i) {
      if (i > 0) ss << ',';
      ss << variations_m[i].name
         << "(up:" << variations_m[i].upColumn
         << ",dn:" << variations_m[i].downColumn << ')';
    }
    entries["weight_variations"] = ss.str();
  }

  // Nominal weight column (if scheduled)
  if (!nominalOutputColumn_m.empty()) {
    entries["nominal_weight_column"] = nominalOutputColumn_m;
  }

  return entries;
}

std::shared_ptr<WeightManager> WeightManager::create(
    Analyzer& an, const std::string& role) {
    auto plugin = std::make_shared<WeightManager>();
    an.addPlugin(role, plugin);
    return plugin;
}
