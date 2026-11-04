---
name: rdfanalyzer-stxs
description: Trace or extend StxsManager classification columns, acceptance/migration uncertainties, auxiliary histograms, and STXS derivation or weight integration.
---

# STXS classification and derivation

Start at [StxsManager.h](../../../core/plugins/StxsManager/StxsManager.h), [implementation](../../../core/plugins/StxsManager/StxsManager.cc), [StxsColumns](../../../core/plugins/StxsManager/StxsColumns.cc), and the underlying [stxs::Manager](../../../core/plugins/StxsManager/stxs/Manager.h).

Resolve `stxsEnabled`, bin config, auxiliary directory, coarse/fine HTXS branches, optional analysis branches, decay channel, signal process/rules, acceptance/migration specifications, and histogram binning from the actual run. Classification IDs, analysis bin numbering, and display labels have distinct mappings; preserve them across derivation and card consumption.

Follow lifecycle hooks separately: setup reads configuration, initialization defines classification/weight columns, execution books standard histograms, finalization consumes/writes them. Check whether required input columns exist when the plugin initializes, especially when adding it before analysis-defined channel columns. Factories and direct construction can have different access to the Analyzer.

Verify any `WeightManager` integration in the implementation and caller. The optional dependency comment or a helper declaration alone does not establish that STXS variations reach the final event product. Likewise, an acceptance/migration spec does not prove matching input histograms or payloads were supplied.

Use [derivation_correctionlib.py](../../../core/python/derivation_correctionlib.py)
for generic payload export and [analysis_preflight.py](../../../core/python/analysis_preflight.py)
for shared report/YAML checks. Analysis-specific readiness gates belong to their
analysis repository; find them through the [analysis project map](../rdfanalyzer-analysis-projects/SKILL.md).

The plugin belongs to core and must build without CMS. `stxsDecayChannelBranch`
defaults to `analysis_channel`; analyses using another input must configure it
explicitly. Migration histograms require an analysis-supplied `Config::migrationBins`
map. `loadMigrationBinConfig` reads five columns: fine category, pt bin, jet/pt
bin, scale bin and acceptance-fallback flag (0 or 1). Set
`stxsMigrationBinConfig` for the plugin's text-file path. Unmapped categories get
unity migration weights; no category range or special-category convention is
inferred. YAML spec-loading hooks remain incomplete: inspect the implementation
before claiming they configured weights.

Use [testStxsManager](../../../core/tests/cpp/testStxsManager.cc) for arbitrary
category IDs, explicit fallback behavior, mapping validation and CMS-free linking.
Use [histograms](../rdfanalyzer-histograms/SKILL.md) and
[statistics](../rdfanalyzer-statistics/SKILL.md) for downstream contracts.

Automatic histogram bin count/range are derived from the loaded analysis bins,
including the default/non-signal bin. Explicit histogram settings override them.
