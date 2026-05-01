#ifndef SYSTEMATICBUNDLE_H_INCLUDED
#define SYSTEMATICBUNDLE_H_INCLUDED

#include <cstddef>
#include <string>
#include <vector>

class IDataFrameProvider;
class ISystematicManager;

enum class SystematicBundleLayout {
  VariationMajor,
};

enum class SystematicBundleMode {
  Inherit,
  Off,
  Auto,
  Required,
};

struct SystematicBundleOptions {
  SystematicBundleMode mode = SystematicBundleMode::Inherit;
  std::string branchName;
  std::string bundleTag;
  bool registerOutputs = true;
  bool fanOutOutputs = true;
  std::string selectionMaskColumn;
};

struct ResolvedSystematicBundleOptions {
  SystematicBundleMode mode = SystematicBundleMode::Off;
  std::string branchName;
  std::string bundleTag;
  bool registerOutputs = true;
  bool fanOutOutputs = true;
  std::string selectionMaskColumn;
};

struct SystematicBundleSpec {
  std::vector<std::string> variationLabels;
  std::vector<std::string> resolvedColumnNames;
  std::string bundleColumnName;
  bool hasSystematics = false;
  SystematicBundleLayout layout = SystematicBundleLayout::VariationMajor;
  std::string selectionMaskColumnName;
};

struct SystematicBundleOutputSpec {
  std::vector<std::string> outputColumnNames;
  std::vector<std::string> variationLabels;
  std::string resultBundleColumnName;
};

ResolvedSystematicBundleOptions resolveBundleOptions(
    const SystematicBundleOptions &frameworkDefaults,
    const SystematicBundleOptions &pluginDefaults,
    const SystematicBundleOptions &itemOverrides,
    const SystematicBundleOptions &invocationOverrides);

std::vector<std::string> resolveVariationLabels(
    ISystematicManager &sysMgr,
    IDataFrameProvider &dataMgr,
    const std::string &branchName);

bool hasUsableSystematicColumns(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::string &variable,
    const std::vector<std::string> &variationLabels);

std::vector<std::string> resolveVariationColumns(
    ISystematicManager &sysMgr,
    const std::string &baseVariable,
    const std::vector<std::string> &variationLabels);

SystematicBundleSpec defineScalarBundle(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::string &baseVariable,
    const std::vector<std::string> &variationLabels,
    const std::string &bundleColumnName,
    bool hasSystematics);

std::vector<SystematicBundleSpec> defineScalarBundles(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::vector<std::string> &baseVariables,
    const std::vector<std::string> &variationLabels,
    const std::string &bundleTag);

SystematicBundleSpec definePackedInputBundle(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::vector<std::string> &inputFeatures,
    const std::vector<std::string> &variationLabels,
    const std::string &bundleColumnName,
    bool hasSystematics);

SystematicBundleSpec defineSelectionMaskBundle(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::string &selectionColumn,
    const std::vector<std::string> &variationLabels,
    const std::string &bundleColumnName,
    bool hasSystematics);

SystematicBundleOutputSpec fanOutScalarResultBundle(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::string &outputBaseName,
    const std::vector<std::string> &variationLabels,
    const std::string &resultBundleColumnName,
    bool registerOutputs);

std::vector<SystematicBundleOutputSpec> fanOutMultiOutputResultBundle(
    IDataFrameProvider &dataMgr,
    ISystematicManager &sysMgr,
    const std::vector<std::string> &outputBaseNames,
    const std::vector<std::string> &variationLabels,
    const std::string &resultBundleColumnName,
    bool registerOutputs);

constexpr const char *SYST_BUNDLE_PREFIX = "__syst_bundle__";

inline std::string makeBundleColumnName(const std::string &tag,
                                        const std::string &suffix) {
  return std::string(SYST_BUNDLE_PREFIX) + tag + "_" + suffix;
}

#endif
