---
name: rdfanalyzer-production
description: Trace, prepare, or diagnose RDFAnalyzerCore LAW task families, scheduler/environment setup, HTCondor/Dask, discovery, staging, stitching, retries, and merge workflows.
---

# Production workflows and diagnosis

Choose the relevant detail:

- [LAW environment and packaging](references/law-environment.md): task registration, scheduler setup, imports, backend/runtime staging.
- [LAW task families](references/law-tasks.md): concrete task routes, parameters, dependency and completion contracts.
- [Two-pass stitching](references/stitching.md): derivation, application and validation.

- [Task graph and input staging](references/task-graph.md): wrappers, dataset manifests, discovery, skim/test/dispatch, Dask, and merges.
- [Worker and recovery diagnosis](references/diagnosis.md): cluster/proc correlation, held jobs, XRootD versus stage-out failures, runtime copies, and retry state.

## Establish the workflow before acting

Start from the analysis wrapper and follow delegated scripts/tasks and CLI defaults into [analysis_tasks.py](../../../core/python/law/analysis_tasks.py), [submission_backend.py](../../../core/python/submission_backend.py), and the generated job bundle. The separate [production_manager.py](../../../core/python/production_manager.py) / [production_monitor.py](../../../core/python/production_monitor.py) workflow has its own state and commands; identify which system owns the run.

A request to diagnose permits inspection, not an unrelated cancellation, resubmission, monitor restart, or output cleanup. When submission/recovery is requested, complete the authorized staging and bounded verification, then submit only within that scope. Preserve run names, state, and existing outputs when resuming. Do not treat a local rebuild as deployment to prepared workers.

Use [input I/O](../rdfanalyzer-input-io/SKILL.md) for adaptor/cache bugs, [config/data](../rdfanalyzer-config-data/SKILL.md) for generated configuration and normalization, and [outputs](../rdfanalyzer-outputs/SKILL.md) for merge compatibility. The [analysis project map](../rdfanalyzer-analysis-projects/SKILL.md) points to checkout-specific wrappers.

Tests live in [core/tests/law](../../../core/tests/law) and [core/tests/python](../../../core/tests/python): choose the affected task/executor/submission/monitor test. Prefer mocked/local workflows for code verification; report live scheduler or storage checks separately from unit-test results.

CMS Rucio, federation and environment providers are documented by the
[extension production skill](../../../extensions/cms/.agents/skills/rdfanalyzer-cms-production/SKILL.md).
Generic task graphs and transport engines remain framework contracts.
