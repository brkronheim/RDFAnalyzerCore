#include <WeightManager.h>
#include <NullOutputSink.h>
#include <TFile.h>
#include <TH1D.h>
#include <TNamed.h>
#include <TObject.h>
#include <api/ILogger.h>
#include <api/IOutputSink.h>
#include <cmath>
#include <numeric>
#include <sstream>
#include <stdexcept>

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
  if (name.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: name must not be empty");
  if (upColumn.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: upColumn must not be empty");
  if (downColumn.empty())
    throw std::invalid_argument("WeightManager::addWeightVariation: downColumn must not be empty");
  variations_m.push_back({name, upColumn, downColumn});
}

// ---------------------------------------------------------------------------
// Weight column scheduling
// ---------------------------------------------------------------------------

void WeightManager::defineNominalWeight(const std::string &outputColumn) {
  if (outputColumn.empty())
    throw std::invalid_argument("WeightManager::defineNominalWeight: outputColumn must not be empty");
  nominalOutputColumn_m = outputColumn;
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

  // Generic approach: accumulate the product of scale factors step by step
  // using a chain of Define() calls, since the number of SF columns is only
  // known at runtime and variadic lambdas cannot be formed statically.

  // Define the first accumulator column
  std::string prevCol = outputColumn + "_wm_acc_0_";
  {
    const std::string sfCol0 = sfColumns[0];
    auto newDf = df.Define(prevCol,
        [](double sf) { return sf; },
        {sfCol0});
    df = newDf;
  }

  // Multiply in subsequent SF columns
  for (std::size_t i = 1; i < sfColumns.size(); ++i) {
    std::string nextCol = outputColumn + "_wm_acc_" + std::to_string(i) + "_";
    const std::string sfColI = sfColumns[i];
    const std::string pCol = prevCol;
    auto newDf = df.Define(nextCol,
        [](double acc, double sf) { return acc * sf; },
        {pCol, sfColI});
    df = newDf;
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

void WeightManager::bookAudit(const std::string &label,
                               const std::string &column,
                               ROOT::RDF::RNode &df) {
  AuditPending ap;
  ap.name = label;
  ap.column = column;
  ap.sumResult  = df.Sum<double>(column);
  ap.meanResult = df.Mean<double>(column);
  ap.minResult  = df.Min<double>(column);
  ap.maxResult  = df.Max<double>(column);
  ap.negCount   = df.Filter([](double w) { return w < 0.0; }, {column}).Count();
  // Exact equality to 0.0 is intentional: in HEP analyses a weight of exactly
  // zero is typically used to veto out-of-range events.  Near-zero but
  // non-zero weights are not considered zero here.
  ap.zeroCount  = df.Filter([](double w) { return w == 0.0; }, {column}).Count();
  auditPending_m.push_back(std::move(ap));
}

// ---------------------------------------------------------------------------
// execute() — define columns and book audit actions
// ---------------------------------------------------------------------------

void WeightManager::execute() {
  if (!dataManager_m)
    throw std::runtime_error("WeightManager::execute: context not set");

  // ---------- Define nominal weight column ----------
  if (!nominalOutputColumn_m.empty()) {
    std::vector<std::string> sfCols;
    sfCols.reserve(scaleFactors_m.size());
    for (const auto &[name, col] : scaleFactors_m)
      sfCols.push_back(col);

    defineWeightColumn(nominalOutputColumn_m, sfCols);
  }

  // ---------- Define varied weight columns ----------
  for (const auto &spec : variedColumnSpecs_m) {
    // Find the variation
    const WeightVariation *var = nullptr;
    for (const auto &v : variations_m) {
      if (v.name == spec.variationName) {
        var = &v;
        break;
      }
    }
    if (!var) {
      throw std::runtime_error(
          "WeightManager::execute: variation \"" + spec.variationName +
          "\" was not registered via addWeightVariation()");
    }

    const std::string variedSfCol =
        (spec.direction == "up") ? var->upColumn : var->downColumn;

    // Build SF column list: replace the matched SF for this variation (if it
    // exists in scaleFactors_m), or append the varied column otherwise.
    std::vector<std::string> sfCols;
    bool replaced = false;
    for (const auto &[sfName, sfCol] : scaleFactors_m) {
      // Match by column name: the nominal SF column that shares a name with
      // var->upColumn / var->downColumn's "base" may differ.
      // Convention: we match the scale factor whose name matches the variation name.
      if (sfName == spec.variationName) {
        sfCols.push_back(variedSfCol);
        replaced = true;
      } else {
        sfCols.push_back(sfCol);
      }
    }
    if (!replaced) {
      // No matching SF was found; append the varied column as an extra factor.
      sfCols.push_back(variedSfCol);
    }

    defineWeightColumn(spec.outputColumn, sfCols);

    // Record the mapping for getWeightColumn().
    variedColumns_m.push_back({{spec.variationName, spec.direction}, spec.outputColumn});
  }

  // ---------- Book audit actions ----------
  // Audit the nominal column (if defined).
  if (!nominalOutputColumn_m.empty()) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    bookAudit(nominalOutputColumn_m, nominalOutputColumn_m, df);
  }

  // Audit each varied column.
  for (const auto &[key, col] : variedColumns_m) {
    ROOT::RDF::RNode df = dataManager_m->getDataFrame();
    const std::string label = col + " (" + key.variationName + " " + key.direction + ")";
    bookAudit(label, col, df);
  }
}

// ---------------------------------------------------------------------------
// finalize() — retrieve audit results and write to meta file
// ---------------------------------------------------------------------------

void WeightManager::finalize() {
  auditEntries_m.clear();
  auditEntries_m.reserve(auditPending_m.size());

  for (auto &ap : auditPending_m) {
    WeightAuditEntry entry;
    entry.name          = ap.name;
    entry.column        = ap.column;
    entry.sumWeights    = ap.sumResult.GetValue();
    entry.meanWeight    = ap.meanResult.GetValue();
    entry.minWeight     = ap.minResult.GetValue();
    entry.maxWeight     = ap.maxResult.GetValue();
    entry.negativeCount = static_cast<long long>(ap.negCount.GetValue());
    entry.zeroCount     = static_cast<long long>(ap.zeroCount.GetValue());
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
