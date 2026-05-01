#include <SystematicBundle.h>
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
  dataMgr.DefineVector(bundleColumnName, spec.resolvedColumnNames, "Float_t", sysMgr);
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
    dataMgr.DefineVector(bundleColumnName, {}, "Float_t", sysMgr);
    return spec;
  }
  const auto existingCols = [&]() {
    auto f = dataMgr.getDataFrame();
    const auto cols = f.GetColumnNames();
    return std::unordered_set<std::string>(cols.begin(), cols.end());
  }();
  std::vector<std::vector<std::string>> varColumns(nVariations);
  for (std::size_t v = 0; v < nVariations; ++v) {
    auto &cols = varColumns[v];
    cols.reserve(nFeatures);
    for (const auto &feat : inputFeatures) {
      const std::string resolved = sysMgr.getVariationColumnName(feat, variationLabels[v]);
      cols.push_back(existingCols.count(resolved) ? resolved : sysMgr.getVariationColumnName(feat, "Nominal"));
      spec.resolvedColumnNames.push_back(cols.back());
    }
  }
  std::vector<std::string> varBundleNames;
  varBundleNames.reserve(nVariations);
  for (std::size_t v = 0; v < nVariations; ++v) {
    const auto &cols = varColumns[v];
    if (cols.empty()) continue;
    const std::string varName = bundleColumnName + "_v" + std::to_string(v);
    auto df = dataMgr.getDataFrame();
    if (nFeatures == 1) {
      std::string colType = df.GetColumnType(cols[0]);
      if (colType.find("RVec") != std::string::npos) {
        df = df.Define(varName, [](const ROOT::VecOps::RVec<float> &x) -> ROOT::VecOps::RVec<float> { return x; }, {cols[0]});
      } else {
        df = df.Define(varName, [](Float_t a) -> ROOT::VecOps::RVec<float> { return {a}; }, {cols[0]});
      }
    } else {
      bool anyRVec = false;
      for (const auto &c : cols) { if (df.GetColumnType(c).find("RVec") != std::string::npos) anyRVec = true; }
      if (anyRVec) throw std::runtime_error("Multi-feature packing with vector-valued columns not supported.");
      const std::string step1 = varName + "_s1";
      df = df.Define(step1, [](Float_t a) -> ROOT::VecOps::RVec<float> { return {a}; }, {cols[0]});
      std::string current = step1;
      for (std::size_t ci = 1; ci < nFeatures; ++ci) {
        const std::string stepName = (ci + 1 == nFeatures) ? varName : varName + "_s" + std::to_string(ci + 1);
        df = df.Define(stepName, [](const ROOT::VecOps::RVec<float> &existing, Float_t nextVal) -> ROOT::VecOps::RVec<float> {
          ROOT::VecOps::RVec<float> out(existing.size() + 1);
          std::copy(existing.begin(), existing.end(), out.begin());
          out.back() = nextVal;
          return out;
        }, {current, cols[ci]});
        current = stepName;
      }
    }
    dataMgr.setDataFrame(df);
    varBundleNames.push_back(varName);
  }
  auto df = dataMgr.getDataFrame();
  if (nVariations == 1) {
    df = df.Define(bundleColumnName, [](const ROOT::VecOps::RVec<float> &vb0) -> ROOT::VecOps::RVec<float> { return vb0; }, {varBundleNames[0]});
  } else if (nVariations == 2) {
    df = df.Define(bundleColumnName, [](const ROOT::VecOps::RVec<float> &vb0, const ROOT::VecOps::RVec<float> &vb1) -> ROOT::VecOps::RVec<float> {
      ROOT::VecOps::RVec<float> out(vb0.size() + vb1.size());
      std::copy(vb0.begin(), vb0.end(), out.begin());
      std::copy(vb1.begin(), vb1.end(), out.begin() + vb0.size());
      return out;
    }, {varBundleNames[0], varBundleNames[1]});
  } else if (nVariations == 3) {
    df = df.Define(bundleColumnName, [](const ROOT::VecOps::RVec<float> &vb0, const ROOT::VecOps::RVec<float> &vb1, const ROOT::VecOps::RVec<float> &vb2) -> ROOT::VecOps::RVec<float> {
      ROOT::VecOps::RVec<float> out(vb0.size() + vb1.size() + vb2.size());
      auto *p = std::copy(vb0.begin(), vb0.end(), out.begin());
      p = std::copy(vb1.begin(), vb1.end(), p);
      std::copy(vb2.begin(), vb2.end(), p);
      return out;
    }, {varBundleNames[0], varBundleNames[1], varBundleNames[2]});
  } else {
    std::string current = varBundleNames[0];
    for (std::size_t v = 1; v < nVariations; ++v) {
      const std::string nextName = (v + 1 == nVariations) ? bundleColumnName : bundleColumnName + "_tmp_" + std::to_string(v);
      df = df.Define(nextName, [](const ROOT::VecOps::RVec<float> &a, const ROOT::VecOps::RVec<float> &b) -> ROOT::VecOps::RVec<float> {
        ROOT::VecOps::RVec<float> out(a.size() + b.size());
        std::copy(a.begin(), a.end(), out.begin());
        std::copy(b.begin(), b.end(), out.begin() + a.size());
        return out;
      }, {current, varBundleNames[v]});
      current = nextName;
    }
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
    dataMgr.DefineVector(bundleColumnName, broadcastCols, "Bool_t", sysMgr);
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
  dataMgr.DefineVector(bundleColumnName, resolvedCols, "Bool_t", sysMgr);
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
