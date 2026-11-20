#include <SystematicBundle.h>
#include <RDFColumnPacker.h>
#include <api/IDataFrameProvider.h>
#include <api/ISystematicManager.h>
#include <ROOT/RVec.hxx>
#include <algorithm>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

namespace {

SystematicBundleMode resolveModeField(SystematicBundleMode invocation,
                                      SystematicBundleMode item,
                                      SystematicBundleMode plugin,
                                      SystematicBundleMode framework) {
  if (invocation != SystematicBundleMode::Inherit) return invocation;
  if (item != SystematicBundleMode::Inherit) return item;
  if (plugin != SystematicBundleMode::Inherit) return plugin;
  return framework;
}

std::string resolveStringField(const std::string &invocation,
                               const std::string &item,
                               const std::string &plugin,
                               const std::string &framework) {
  if (!invocation.empty()) return invocation;
  if (!item.empty()) return item;
  if (!plugin.empty()) return plugin;
  return framework;
}

std::string generateBundleTag() {
  static std::size_t counter = 0;
  return "bundle_" + std::to_string(++counter);
}

} // namespace

ResolvedSystematicBundleOptions
resolveBundleOptions(const SystematicBundleOptions &frameworkDefaults,
                     const SystematicBundleOptions &pluginDefaults,
                     const SystematicBundleOptions &itemOverrides,
                     const SystematicBundleOptions &invocationOverrides) {
  ResolvedSystematicBundleOptions resolved;
  resolved.mode = resolveModeField(invocationOverrides.mode, itemOverrides.mode,
                                   pluginDefaults.mode, frameworkDefaults.mode);
  resolved.branchName = resolveStringField(invocationOverrides.branchName, itemOverrides.branchName,
                                           pluginDefaults.branchName, frameworkDefaults.branchName);
  resolved.bundleTag = resolveStringField(invocationOverrides.bundleTag, itemOverrides.bundleTag,
                                          pluginDefaults.bundleTag, frameworkDefaults.bundleTag);
  if (resolved.bundleTag.empty()) resolved.bundleTag = generateBundleTag();
  resolved.registerOutputs = frameworkDefaults.registerOutputs;
  if (pluginDefaults.registerOutputs != frameworkDefaults.registerOutputs) resolved.registerOutputs = pluginDefaults.registerOutputs;
  if (itemOverrides.registerOutputs != pluginDefaults.registerOutputs) resolved.registerOutputs = itemOverrides.registerOutputs;
  if (invocationOverrides.registerOutputs != itemOverrides.registerOutputs) resolved.registerOutputs = invocationOverrides.registerOutputs;
  resolved.fanOutOutputs = frameworkDefaults.fanOutOutputs;
  if (pluginDefaults.fanOutOutputs != frameworkDefaults.fanOutOutputs) resolved.fanOutOutputs = pluginDefaults.fanOutOutputs;
  if (itemOverrides.fanOutOutputs != pluginDefaults.fanOutOutputs) resolved.fanOutOutputs = itemOverrides.fanOutOutputs;
  if (invocationOverrides.fanOutOutputs != itemOverrides.fanOutOutputs) resolved.fanOutOutputs = invocationOverrides.fanOutOutputs;
  resolved.selectionMaskColumn = resolveStringField(invocationOverrides.selectionMaskColumn, itemOverrides.selectionMaskColumn,
                                                     pluginDefaults.selectionMaskColumn, frameworkDefaults.selectionMaskColumn);
  return resolved;
}

std::vector<std::string>
resolveVariationLabels(ISystematicManager &sysMgr, IDataFrameProvider &dataMgr, const std::string &branchName) {
  return sysMgr.makeSystList(branchName, dataMgr);
}

bool hasUsableSystematicColumns(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                                 const std::string &variable, const std::vector<std::string> &variationLabels) {
  auto df = dataMgr.getDataFrame();
  const auto existingColumns = df.GetColumnNames();
  std::unordered_set<std::string> colSet(existingColumns.begin(), existingColumns.end());
  for (const auto &label : variationLabels) {
    if (label == "Nominal") continue;
    if (!sysMgr.isVariableAffectedBySystematic(variable, label)) continue;
    if (colSet.count(sysMgr.getVariationColumnName(variable, label))) return true;
  }
  return false;
}

std::vector<std::string>
resolveVariationColumns(ISystematicManager &sysMgr, const std::string &baseVariable,
                         const std::vector<std::string> &variationLabels) {
  std::vector<std::string> columns;
  columns.reserve(variationLabels.size());
  for (const auto &label : variationLabels)
    columns.push_back(sysMgr.getVariationColumnName(baseVariable, label));
  return columns;
}

SystematicBundleSpec
defineScalarBundle(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                   const std::string &baseVariable, const std::vector<std::string> &variationLabels,
                   const std::string &bundleColumnName, bool hasSystematics) {
  SystematicBundleSpec spec;
  spec.variationLabels = variationLabels;
  spec.bundleColumnName = bundleColumnName;
  spec.hasSystematics = hasSystematics;
  spec.layout = SystematicBundleLayout::VariationMajor;
  spec.resolvedColumnNames = resolveVariationColumns(sysMgr, baseVariable, variationLabels);
  dataMgr.DefineVectorNominal(bundleColumnName, spec.resolvedColumnNames,
                              "Float_t", sysMgr);
  return spec;
}

std::vector<SystematicBundleSpec>
defineScalarBundles(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                    const std::vector<std::string> &baseVariables, const std::vector<std::string> &variationLabels,
                    const std::string &bundleTag) {
  std::vector<SystematicBundleSpec> specs;
  specs.reserve(baseVariables.size());
  for (std::size_t i = 0; i < baseVariables.size(); ++i) {
    const bool hasSyst = hasUsableSystematicColumns(dataMgr, sysMgr, baseVariables[i], variationLabels);
    specs.push_back(defineScalarBundle(dataMgr, sysMgr, baseVariables[i], variationLabels,
                                       makeBundleColumnName(bundleTag, "scalar_" + std::to_string(i)), hasSyst));
  }
  return specs;
}

SystematicBundleSpec
definePackedInputBundle(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                         const std::vector<std::string> &inputFeatures, const std::vector<std::string> &variationLabels,
                         const std::string &bundleColumnName, bool hasSystematics) {
  SystematicBundleSpec spec;
  spec.variationLabels = variationLabels;
  spec.bundleColumnName = bundleColumnName;
  spec.hasSystematics = hasSystematics;
  spec.layout = SystematicBundleLayout::VariationMajor;
  const std::size_t nFeatures = inputFeatures.size();
  const std::size_t nVariations = variationLabels.size();
  if (nFeatures == 0 || nVariations == 0) {
    dataMgr.DefineVectorNominal(bundleColumnName, {}, "Float_t", sysMgr);
    return spec;
  }
  const auto existingCols = [&]() {
    auto f = dataMgr.getDataFrame();
    const auto cols = f.GetColumnNames();
    return std::unordered_set<std::string>(cols.begin(), cols.end());
  }();
  for (std::size_t v = 0; v < nVariations; ++v) {
    for (const auto &feat : inputFeatures) {
      const std::string resolved = sysMgr.getVariationColumnName(feat, variationLabels[v]);
      spec.resolvedColumnNames.push_back(
          existingCols.count(resolved) ? resolved : sysMgr.getVariationColumnName(feat, "Nominal"));
    }
  }
  auto df = dataMgr.getDataFrame();
  bool anyVector = false;
  bool allVector = true;
  for (const auto &column : spec.resolvedColumnNames) {
    const bool vector = rdfcolumnpacker::isRVecColumnType(df.GetColumnType(column));
    anyVector |= vector;
    allVector &= vector;
  }
  if (nFeatures > 1 && anyVector)
    throw std::runtime_error("Multi-feature packing with vector-valued columns not supported.");

  if (!anyVector || allVector) {
    // Scalar features are already listed in variation-major order. A single
    // feature containing an RVec is concatenated in that same order, including
    // empty or unequal-length variation arrays (the existing public contract).
    df = rdfcolumnpacker::definePackedVector(
        df, bundleColumnName, spec.resolvedColumnNames, "Float_t",
        anyVector ? rdfcolumnpacker::PackingLayout::Concatenate
                  : rdfcolumnpacker::PackingLayout::Scalars);
  } else {
    // Retain the single-feature scalar/vector mixture accepted by the old
    // per-variation implementation, without a growing concatenation chain.
    std::vector<std::string> packs;
    for (std::size_t v = 0; v < nVariations; ++v) {
      const auto name = bundleColumnName + "_v" + std::to_string(v);
      const auto &column = spec.resolvedColumnNames[v];
      df = rdfcolumnpacker::definePackedVector(
          df, name, {column}, "Float_t",
          rdfcolumnpacker::isRVecColumnType(df.GetColumnType(column))
              ? rdfcolumnpacker::PackingLayout::Concatenate
              : rdfcolumnpacker::PackingLayout::Scalars);
      packs.push_back(name);
    }
    df = rdfcolumnpacker::definePackedVector(
        df, bundleColumnName, packs, "Float_t",
        rdfcolumnpacker::PackingLayout::Concatenate);
  }
  dataMgr.setDataFrame(df);
  return spec;
}

SystematicBundleSpec
defineSelectionMaskBundle(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                           const std::string &selectionColumn, const std::vector<std::string> &variationLabels,
                           const std::string &bundleColumnName, bool hasSystematics) {
  SystematicBundleSpec spec;
  spec.variationLabels = variationLabels;
  spec.bundleColumnName = bundleColumnName;
  spec.hasSystematics = hasSystematics;
  spec.layout = SystematicBundleLayout::VariationMajor;
  spec.selectionMaskColumnName = bundleColumnName;
  if (!hasSystematics) {
    const std::vector<std::string> broadcastCols(variationLabels.size(), selectionColumn);
    dataMgr.DefineVectorNominal(bundleColumnName, broadcastCols, "Bool_t",
                                sysMgr);
    spec.resolvedColumnNames = broadcastCols;
    return spec;
  }
  std::vector<std::string> resolvedCols;
  resolvedCols.reserve(variationLabels.size());
  auto df = dataMgr.getDataFrame();
  const auto existingColumns = df.GetColumnNames();
  std::unordered_set<std::string> colSet(existingColumns.begin(), existingColumns.end());
  for (const auto &label : variationLabels) {
    const std::string colName = sysMgr.getVariationColumnName(selectionColumn, label);
    if (colSet.count(colName)) {
      resolvedCols.push_back(colName);
      spec.resolvedColumnNames.push_back(colName);
    } else if (!sysMgr.isVariableAffectedBySystematic(selectionColumn, label)) {
      resolvedCols.push_back(selectionColumn);
      spec.resolvedColumnNames.push_back(selectionColumn);
    } else {
      throw std::runtime_error("defineSelectionMaskBundle: Selection column '" + selectionColumn +
                               "' has systematic variation '" + label + "' but column '" + colName + "' not found.");
    }
  }
  dataMgr.DefineVectorNominal(bundleColumnName, resolvedCols, "Bool_t", sysMgr);
  return spec;
}

SystematicBundleOutputSpec
fanOutScalarResultBundle(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                          const std::string &outputBaseName, const std::vector<std::string> &variationLabels,
                          const std::string &resultBundleColumnName, bool registerOutputs) {
  SystematicBundleOutputSpec spec;
  spec.variationLabels = variationLabels;
  spec.resultBundleColumnName = resultBundleColumnName;
  const std::size_t nVariations = variationLabels.size();
  auto df = dataMgr.getDataFrame();
  const auto existingColumns = df.GetColumnNames();
  std::unordered_set<std::string> colSet(existingColumns.begin(), existingColumns.end());
  for (std::size_t i = 0; i < nVariations; ++i) {
    const auto &label = variationLabels[i];
    std::string outColName = (label == "Nominal") ? outputBaseName : outputBaseName + "_" + label;
    spec.outputColumnNames.push_back(outColName);
    if (colSet.count(outColName)) continue;
    df = df.Define(outColName, [i](const ROOT::VecOps::RVec<float> &bundle) -> float {
      return (i < bundle.size()) ? bundle[i] : 0.0f;
    }, {resultBundleColumnName});
    colSet.insert(outColName);
  }
  dataMgr.setDataFrame(df);
  if (registerOutputs) {
    for (const auto &label : variationLabels) {
      if (label == "Nominal") continue;
      std::string systName = label;
      if (label.size() > 2 && label.substr(label.size()-2) == "Up") systName = label.substr(0, label.size()-2);
      else if (label.size() > 4 && label.substr(label.size()-4) == "Down") systName = label.substr(0, label.size()-4);
      sysMgr.registerSystematic(systName, {outputBaseName});
    }
  }
  return spec;
}

std::vector<SystematicBundleOutputSpec>
fanOutMultiOutputResultBundle(IDataFrameProvider &dataMgr, ISystematicManager &sysMgr,
                               const std::vector<std::string> &outputBaseNames, const std::vector<std::string> &variationLabels,
                               const std::string &resultBundleColumnName, bool registerOutputs) {
  const std::size_t nOutputs = outputBaseNames.size();
  const std::size_t nVariations = variationLabels.size();
  std::vector<SystematicBundleOutputSpec> specs;
  specs.reserve(nOutputs);
  auto df = dataMgr.getDataFrame();
  const auto existingColumns = df.GetColumnNames();
  std::unordered_set<std::string> colSet(existingColumns.begin(), existingColumns.end());
  for (std::size_t o = 0; o < nOutputs; ++o) {
    SystematicBundleOutputSpec spec;
    spec.variationLabels = variationLabels;
    spec.resultBundleColumnName = resultBundleColumnName;
    for (std::size_t v = 0; v < nVariations; ++v) {
      const auto &label = variationLabels[v];
      const std::size_t flatIndex = o * nVariations + v;
      std::string outColName = (label == "Nominal") ? outputBaseNames[o] : outputBaseNames[o] + "_" + label;
      spec.outputColumnNames.push_back(outColName);
      if (colSet.count(outColName)) continue;
      df = df.Define(outColName, [flatIndex](const ROOT::VecOps::RVec<float> &bundle) -> float {
        return (flatIndex < bundle.size()) ? bundle[flatIndex] : 0.0f;
      }, {resultBundleColumnName});
      colSet.insert(outColName);
    }
    specs.push_back(std::move(spec));
  }
  dataMgr.setDataFrame(df);
  if (registerOutputs) {
    for (const auto &label : variationLabels) {
      if (label == "Nominal") continue;
      std::string systName = label;
      if (label.size() > 2 && label.substr(label.size()-2) == "Up") systName = label.substr(0, label.size()-2);
      else if (label.size() > 4 && label.substr(label.size()-4) == "Down") systName = label.substr(0, label.size()-4);
      sysMgr.registerSystematic(systName, {outputBaseNames[0]});
    }
  }
  return specs;
}
