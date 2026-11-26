---
name: rdfanalyzer-plugin-lifecycle
description: Develop, register, or debug RDFAnalyzerCore managers and services, including plugin roles, dependency ordering, context lifetime, and lazy execution hooks.
---

# Plugin lifecycle and services

Use for manager wiring and reusable extension points. For a particular manager's physics or output behavior, select its domain skill from the [component inventory](../rdfanalyzer-development/references/components.yaml).

## Follow ownership and timing

- Read [IPluggableManager](../../../core/interface/api/IPluggableManager.h), [ManagerContext](../../../core/interface/api/ManagerContext.h), and `wirePluginManagers`, `reorderPlugins`, `addPlugin`, and `run` in [analyzer.cc](../../../core/src/analyzer.cc). Inspect both constructor-time bulk registration and incremental registration when diagnosing initialization.
- Dependencies are **role strings**, not class names. Required `getPlugin<T>(role)` throws; optional `findPlugin<T>(role)` returns an empty pointer for a missing or incompatible role. Do not depend on incidental alphabetical ordering of independent plugins.
- The Analyzer owns the context and registered managers. Context references inside a manager are non-owning. Extend the existing interfaces rather than creating another ownership path to config/data/systematics/sinks.
- Wiring calls `setContext`, `setupFromConfigFile`, then `initialize` in dependency order. `run()` invokes `execute`, captures the resulting graph, writes configured outputs, finalizes services and plugins, reports metadata, and collects provenance. Lazy actions may materialize when an output consumes them; `execute()` alone is not evidence that events ran. A second `run()` on the same Analyzer throws.
- Some public scheduling APIs materialize columns immediately once context exists; others defer them. Inspect the specific manager's implementation and callers before moving calls between analysis construction and `execute()`.

## Choose the extension

[NamedObjectManager](../../../core/plugins/NamedObjectManager/NamedObjectManager.h) stores keyed resources plus ordered features; use it for models/corrections when that shape fits. Direct graph/output plugins inherit `IPluggableManager`. [IAnalysisService](../../../core/interface/api/IAnalysisService.h) supplies service hooks, including `onPreFilter`; inspect [CounterService](../../../core/src/CounterService.cc) for counters captured before the first analysis filter.

Implement dependency and required/produced-column advertisements when useful to the actual workflow. Register compiled plugins through [plugins/CMakeLists.txt](../../../core/plugins/CMakeLists.txt); check the OBJECT-library archive and link lists, not just `add_subdirectory`. NamedObjectManager is header-only. Follow [PLUGIN_DEVELOPMENT](../../../docs/PLUGIN_DEVELOPMENT.md) for the public extension contract.

## Verify

Closest tests: [plugin lifecycle](../../../core/tests/cpp/testPluginLifecycle.cc), [wiring](../../../core/tests/cpp/testAnalyzer_PluginWiring.cc), [context persistence](../../../core/tests/cpp/testAnalyzer_ManagerContextPersistence.cc), and [NamedObjectManager](../../../core/tests/cpp/testNamedObjectManager.cc). Exercise the relevant observable through Analyzer construction and `run()`, including missing roles or cycles if wiring changed. Use [build](../rdfanalyzer-build/SKILL.md) to identify actual CTest names.
