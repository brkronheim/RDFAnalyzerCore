---
name: rdfanalyzer-config-data
description: Trace or change RDFAnalyzerCore text/YAML configuration, input dataframes, numeric constants, aliases, optional branches, friend trees, and dataset manifests.
---

# Configuration and input columns

## Resolve the configuration actually consumed

Start with the analysis constructor and generated job config. Read [IConfigurationProvider](../../../core/interface/api/IConfigurationProvider.h), [ConfigurationManager](../../../core/src/ConfigurationManager.cc), [TextConfigAdapter](../../../core/src/TextConfigAdapter.cc), and [YamlConfigAdapter](../../../core/src/YamlConfigAdapter.cc). The filename extension and adapter select parsing; do not assume a YAML mapping is interchangeable with a plugin's sequence of records. `get()` can return an empty string for an absent key; validate required values where they are consumed.

Trace `resolveConfigPath` at each load site. Main-config-relative resolution and process-working-directory resolution matter for staging. Current [DataManager.cc](../../../core/src/DataManager.cc) resolves constant-file paths with `resolveConfigPath`; older prose in [CONFIG_REFERENCE](../../../docs/CONFIG_REFERENCE.md) describes a direct existence check. Follow the executable implementation and report the discrepancy if it affects a run. Likewise inspect the input tree fallback in code rather than assuming documentation defaults.

For optional C++17 setup helpers, use [CommonSettings.h](../../../core/interface/CommonSettings.h)
and [ValidationReport.h](../../../core/interface/ValidationReport.h), documented in
[native settings/validation](../../../docs/NATIVE_SETTINGS_AND_VALIDATION.md).
They emit existing native string-map options and `rdf-validation-v1` diagnostics
without changing providers, defaults or event graphs. Unset fields preserve
advanced options; explicitly typed collisions fail. Paths remain native-owned;
configuration validation does not open files or certify schemas/payloads.
Explicit [NativePreflight.h](../../../core/interface/NativePreflight.h) checks
only caller-declared local assets or supplied column metadata. It does not
recursively discover configs, normalize ROOT type spellings, open ROOT inputs,
compile kernels or certify every file in a chain. Asset and schema reports
advertise only the checked phase. [CommonSettingsSchema.h](../../../core/interface/CommonSettingsSchema.h)
generates help/descriptions from the same native keys/ranges used by settings
lowering; it neither queries runtime defaults nor intercepts executable arguments.

For explicitly requested Python input preflight, [rdfschema.py](../../../core/python/rdfschema.py)
compares supplied metadata or opens each supplied file separately through its
optional ROOT reader. [Input schema preflight](../../../docs/INPUT_SCHEMA_PREFLIGHT.md)
documents exact type spellings, coverage and custom readers. Import/configuration
checks stay stdlib-only; ROOT is loaded only for acquisition. The reader books no
actions and verifies zero RDF event loops. These are original input columns, not
aliases, optional defaults, friends or derived analysis columns. Do not certify a
mixed chain from its first file or treat schema success as later-read/physics proof.

## Construct and extend the dataframe

- Read [IDataFrameProvider](../../../core/interface/api/IDataFrameProvider.h) and [DataManager](../../../core/interface/DataManager.h). Track chain construction, `finalizeSetup`, constants, aliases, optional branches, and friend attachment before analysis `Define`s.
- Keep analysis numeric cut values and working points in commented `floatConfig`/`intConfig` files and consume their named columns. Ensure local and staged configs reference the same files. Do not move physics-specific thresholds into framework code.
- For missing-branch behavior, distinguish a compatibility alias, configured optional branch, and a physically meaningful fallback. Verify mixed-file behavior and ROOT-version support before asserting a default is supplied.
- Friend trees require the configured tree/path, alias, and matching policy. Read `registerFriendTrees`/`attachFriendTree` and callers; verify entry correspondence for the actual sample rather than assuming every friend is keyed by event ID.
- [dataset_manifest.py](../../../core/python/dataset_manifest.py) owns dataset metadata, friend configuration, and generator-weight denominator selection. A dataset manifest describes inputs; an [output manifest](../rdfanalyzer-outputs/SKILL.md) describes produced artifacts.

Use [systematics](../rdfanalyzer-systematics/SKILL.md) before replacing framework `Define` calls with raw RDF operations; use [input I/O](../rdfanalyzer-input-io/SKILL.md) for remote opening/cache behavior and [production](../rdfanalyzer-production/SKILL.md) for per-job overrides.

## Verify

Configuration tests live in [core/tests/cpp](../../../core/tests/cpp) (`testConfigurationManager*`, `testDataManager.cc`, `testManagerFactory.cc`). Python validation lives in [test_validate_config.py](../../../core/tests/python/test_validate_config.py), [test_dataset_manifest.py](../../../core/tests/python/test_dataset_manifest.py), and [test_analysis_config_validation.py](../../../core/tests/python/test_analysis_config_validation.py). Check validators in [validate_config.py](../../../core/python/validate_config.py) against runtime parsing; validation success does not instantiate a plugin or prove a generated config runs.

[convert_samples_to_manifest.py](../../../core/python/convert_samples_to_manifest.py)
imports block-structured samples JSON. Callers supply process/group classifiers;
core defaults preserve lowercase sample labels and use data/background groups.
Luminosities are supplied explicitly. Analysis-specific process inference belongs
to that analysis, not the format converter.
