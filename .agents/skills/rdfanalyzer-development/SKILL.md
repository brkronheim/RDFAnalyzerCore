---
name: rdfanalyzer-development
description: Develop RDFAnalyzerCore itself, including its public API, Analyzer lifecycle, managers, CMS plugins, configuration behavior, and tests. Use for reusable framework changes rather than analysis-specific physics logic.
---

# Develop RDFAnalyzerCore

Use this skill when changing reusable behavior in the RDFAnalyzerCore framework: `Analyzer`, data/configuration management, output handling, systematic support, plugins, CMS integration, or framework utilities. For a task focused on understanding or editing a physics analysis, use `rdfanalyzer-analysis` and keep the change in that analysis project unless the framework is missing a general capability.

## Select a focused workflow

Read only the skill relevant to the changed contract, then inspect its source and closest tests. The [component inventory](references/components.yaml) maps every plugin directory and major framework/workflow surface to its skill; it is an index, not a list of modules to load by default.

| Contract or subsystem | Skill |
|---|---|
| Analyzer ownership, role ordering, managers/services, new plugins | [Plugin lifecycle](../rdfanalyzer-plugin-lifecycle/SKILL.md) |
| Text/YAML config, DataManager, aliases/constants/friends | [Config and data](../rdfanalyzer-config-data/SKILL.md) |
| Varied columns, bundles, selection masks, acceptance | [Systematics](../rdfanalyzer-systematics/SKILL.md) |
| CMS registry/compatibility, correction stack, MET/cleaning | [CMS](../rdfanalyzer-cms/SKILL.md) |
| Correctionlib, energy/smearing managers, object collections | [Object corrections](../rdfanalyzer-object-corrections/SKILL.md) |
| Event weights, generator variations, normalization | [Weights](../rdfanalyzer-weights/SKILL.md) |
| Trigger, RegionManager and CutflowManager | [Selection and regions](../rdfanalyzer-selection-regions/SKILL.md) |
| Tagger recipes, fixed-WP/shape weights, efficiencies | [Tagging](../rdfanalyzer-tagging/SKILL.md) |
| NDHistogramManager, packing/projection, histogram I/O | [Histograms](../rdfanalyzer-histograms/SKILL.md) |
| Plot requests, persistent sessions, rendering and CLI | [Plotting](../rdfanalyzer-plotting/SKILL.md) |
| ONNX/FastForest/SOFIE | [Inference](../rdfanalyzer-inference/SKILL.md) |
| Kinematic fitter and CUDA path | [Kinematic fits](../rdfanalyzer-kinematic-fit/SKILL.md) |
| STXS columns and acceptance/migration derivation | [STXS](../rdfanalyzer-stxs/SKILL.md) |
| Sinks, counters, reports, provenance and versioned merging | [Outputs](../rdfanalyzer-outputs/SKILL.md) |
| LAW/Condor/Dask/discovery, staging and monitors | [Production](../rdfanalyzer-production/SKILL.md) |
| ROOT chains, InputCache, XrdAdaptor/read performance | [Input I/O](../rdfanalyzer-input-io/SKILL.md) |
| Cards, nuisance groups and fit tooling | [Statistics](../rdfanalyzer-statistics/SKILL.md) |
| Python authoring, pybind11 and native runtime | [Python](../rdfanalyzer-python/SKILL.md) |
| CMake, CI/containers, dependencies, tests and benchmarking | [Build and tests](../rdfanalyzer-build/SKILL.md) |
| Analysis-owned entry points and helpers | [Analysis project map](../rdfanalyzer-analysis-projects/SKILL.md) |

For logging, exceptions and local failure tracing, read [diagnostics](references/diagnostics.md). For API docs, search indexes and repository publishing, read [documentation maintenance](references/documentation.md).

## Framework map

- `core/interface/` contains public C++ interfaces and headers. Treat these as the API contract.
- `core/src/` implements the central `Analyzer`, configuration, data, output, and systematic behavior.
- `core/plugins/` contains optional managers; CMS-specific corrections and weights are under `extensions/cms/plugins/cms/`.
- `core/python/` contains Python bindings and workflow/submission code; `core/python/law/` owns LAW task graphs.
- `core/tests/` contains C++ and Python tests. `core/extern/` is vendored dependency code; avoid editing it unless the requested change is specifically there.
- `analyses/` holds analysis-specific code and is not the place for reusable framework behavior.

Read `docs/ARCHITECTURE.md` for manager ownership and lifecycle, `docs/API_REFERENCE.md` for public APIs, and `docs/PLUGIN_DEVELOPMENT.md` when adding or changing a manager. For analysis authorship patterns, use `docs/ANALYSIS_GUIDE.md`.

`Analyzer` stores plugins by role, validates dependencies, orders plugin roles, calls `setContext()` and `setupFromConfigFile()`, then initializes them. At run time, `execute()` commonly adds columns or books lazy actions; after the RDataFrame actions, `finalize()` consumes results, then plugins report metadata and contribute provenance. `getPlugin<T>(role)` is for required plugins and throws for a missing role or type mismatch; `findPlugin<T>(role)` is the optional lookup. Dependency strings are plugin roles. Use `core/interface/api/IPluggableManager.h` and `core/src/analyzer.cc` to verify exact behavior.

## Make a framework change

Inspect `git status --short` first and preserve the existing worktree. Find the public contract, implementation, callers, and closest tests before editing. Keep behavior behind a stable interface where practical; plugin configuration and plugin execution are distinct, so follow both registration and invocation paths. Check whether the feature is generic or CMS-specific before choosing `core/src/` versus `extensions/cms/plugins/cms/`.

Prefer a focused change with a meaningful test for the observable behavior. Update API or usage docs when a public contract or configuration behavior changes. Avoid tests that only restate the implementation. Keep edits to vendored code and unrelated generated outputs out of ordinary framework work.

For numeric behavior that belongs in configuration, prefer the native
`DataManager` `floatConfig`/`intConfig` registration path. Framework and
analysis code should consume the resulting named columns instead of copying
cut thresholds or region/category codes into C++. Configuration files should
be commented, staged with the job when referenced, and documented as part of
the public configuration contract.

## Build and verify

Use the existing configured build directory when one is present; otherwise configure with the repository's documented CMake options. Build the affected target first, then run the closest CTest or pytest coverage. The common full checks are:

```bash
cmake --build build -j$(nproc)
ctest --test-dir build --output-on-failure
```

Python-focused tests should use the repository virtualenv when available, for example:

```bash
.venv/bin/python -m pytest core/tests/python
```

ROOT, CMS environments, optional dependencies, and external services can limit which checks work locally. Report the exact checks run and distinguish environment failures from code failures. Do not broaden testing after relevant checks pass unless a change, failure, or unresolved risk warrants it.

## Maintain the skill map

When a task adds a plugin, LAW task/runtime module, or changes a documented contract, update its focused skill/reference and the relevant entry in [components.yaml](references/components.yaml). Keep source ownership and decision-changing constraints in one place; route to existing skills instead of copying their instructions. Use paths relative to each Markdown file and keep optional analysis guidance separate from framework guarantees. Avoid embedding dated production state, hard-coded saved fit results, or live credentials.

Run the repository-local checker after changing the skill tree:

```bash
python .agents/skills/rdfanalyzer-development/scripts/validate_skill_tree.py
```

It validates frontmatter/UI metadata, local Markdown links, reference/skill reachability from the two main skills, plugin-directory coverage, explicit LAW runtime-file routes, and concrete LAW task classes against their focused references. Task discovery uses Python AST and local inheritance without importing or running LAW. Optional analysis targets are checked when their checkout exists. Coverage checks establish navigation and class/module consistency; they do not prove that every repository file has procedural guidance, payloads are available, or runtime behavior is correct.

CMS support is an optional separate source checkout. See [CMS extension integration](../../../docs/CMS_EXTENSION.md) for ownership, AUTO/ON/OFF builds, public targets, Python providers, and worker packaging. Combine integration remains in core.

Optional source repositories use the [generic extension registry](../../../docs/EXTENSIONS.md). Enable flags belong to extension configure hooks; core discovers `rdf-extension.json` and aggregates registered targets, tests, factories, and provenance. Use `RDF_REQUIRED_EXTENSIONS` for missing-checkout errors and `RDF_BUILD_EXTENSIONS=OFF` to disable all extensions. Analysis requirements belong in each analysis's `rdf-dependencies.cmake`.
