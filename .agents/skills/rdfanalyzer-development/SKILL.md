---
name: rdfanalyzer-development
description: Develop RDFAnalyzerCore itself, including its public API, Analyzer lifecycle, managers, CMS plugins, configuration behavior, and tests. Use for reusable framework changes rather than analysis-specific physics logic.
---

# Develop RDFAnalyzerCore

Use this skill when changing reusable behavior in the RDFAnalyzerCore framework: `Analyzer`, data/configuration management, output handling, systematic support, plugins, CMS integration, or framework utilities. For a task focused on understanding or editing a physics analysis, use `rdfanalyzer-analysis` and keep the change in that analysis project unless the framework is missing a general capability.

## Framework map

- `core/interface/` contains public C++ interfaces and headers. Treat these as the API contract.
- `core/src/` implements the central `Analyzer`, configuration, data, output, and systematic behavior.
- `core/plugins/` contains optional managers; CMS-specific corrections and weights are under `core/plugins/cms/`.
- `core/python/` contains Python bindings and workflow/submission code; `core/python/law/` owns LAW task graphs.
- `core/tests/` contains C++ and Python tests. `core/extern/` is vendored dependency code; avoid editing it unless the requested change is specifically there.
- `analyses/` holds analysis-specific code and is not the place for reusable framework behavior.

Read `docs/ARCHITECTURE.md` for manager ownership and lifecycle, `docs/API_REFERENCE.md` for public APIs, and `docs/PLUGIN_DEVELOPMENT.md` when adding or changing a manager. For analysis authorship patterns, use `docs/ANALYSIS_GUIDE.md`.

`Analyzer` stores plugins by role, validates dependencies, orders plugin roles, calls `setContext()` and `setupFromConfigFile()`, then initializes them. At run time, `execute()` commonly adds columns or books lazy actions; after the RDataFrame actions, `finalize()` consumes results, then plugins report metadata and contribute provenance. `getPlugin<T>(role)` is for required plugins and throws for a missing role or type mismatch; `findPlugin<T>(role)` is the optional lookup. Dependency strings are plugin roles. Use `core/interface/api/IPluggableManager.h` and `core/src/analyzer.cc` to verify exact behavior.

## Make a framework change

Inspect `git status --short` first and preserve the existing worktree. Find the public contract, implementation, callers, and closest tests before editing. Keep behavior behind a stable interface where practical; plugin configuration and plugin execution are distinct, so follow both registration and invocation paths. Check whether the feature is generic or CMS-specific before choosing `core/src/` versus `core/plugins/cms/`.

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
