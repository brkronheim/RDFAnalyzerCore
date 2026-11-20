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

For long CPU-bound tails, use [job sizing](../../../docs/JOB_SIZING.md) and
[job_sizing.py](../../../core/python/job_sizing.py). `partition=events` packs
whole files up to per-dataset event budgets and slices oversized files. Cached
discovery groups retain byte boundaries but cannot override explicit file/event
policies. Entry metadata is tree-scoped and copied into each run; profile and
seed digests are sealed. Calibrate rates for slower workers, including datasets
whose fast-only completions would otherwise bias the profile. Retries bisect
bounded ranges before file lists; single-file jobs can use cached counts.
The default `partition=auto` uses file grouping until matching CPU-rate evidence
exists. Verified HTCondor completion updates a locked origin/workload cache from
returned stdout/events; future preparations pin selected snapshots locally.
Explicit profiles win, `none` disables lookup, and unmeasured datasets retain
file grouping. Cache failures cannot retry verified jobs; `complete()` must not
learn or consult mutable shared history.
For first-run calibration, `--sizing-pilots` runs one-file Condor jobs for
unprofiled datasets before preparation. `--sizing-pilot-parallel` bounds
simultaneous pilots per workflow. Prefix/full paired timings fit fixed overhead
and per-event cost; pilot outputs are diagnostics and never production inputs.
Default sizing targets are 1800 seconds for pilot priors and 3600 seconds for
production measurements, with an independent 7200-second Condor request.
Override with `--sizing-pilot-target-seconds`, `--sizing-target-seconds` and
`--max-runtime`; frozen plans and explicit profile budgets retain precedence.
See [pilot controls](../../../docs/JOB_SIZING.md#optional-pilots-before-bulk-submission).

Generated Condor workers preserve inherited site CPU affinity without selecting
or reassigning CPUs, even when the allowed mask is broader than `request_cpus`.
Setup/transfer helpers, ROOT/I/O threads and children inherit that placement.
Common numerical thread-pool limits remain tied to `request_cpus`; these do not
enforce an aggregate CPU quota without site affinity/cgroup enforcement.
Startup logs include `policy=inherited`. Newly generated scripts are required;
running/prepared workers are not rewritten by rebuilding or editing code.

The shared submission backend embeds validated XRootD historical rankings from
the I/O provider, or `RDF_XRD_SITE_RANKING_FILE`, into new worker scripts.
Workers select their own origin from site tags/hostname; do not freeze submit
host geography for a heterogeneous pool. Refine snapshots with
[xrd_site_rankings.py](../../../core/python/xrd_site_rankings.py), retaining other
origin profiles with `--base`. See the [I/O workflow](../rdfanalyzer-input-io/SKILL.md).

After every output is verified, `MonitorSkimJobs` (and its stitching subclass)
updates the user ranking cache through `xrd_site_rankings.update_after_run`.
New submission scripts prefer this cache over the provider seed; explicit file
pins still win. Cache updates are locked, idempotent and bounded to recent
completed-run history. They do not run from `complete()` or after permanent
job failures. Update errors warn and produce a run receipt without retrying
verified physics jobs. See [automatic learning controls](../../../docs/xrd-adaptor.md#historical-site-preferences-and-open-attempt-logging).

Tests live in [core/tests/law](../../../core/tests/law) and [core/tests/python](../../../core/tests/python): choose the affected task/executor/submission/monitor test. Prefer mocked/local workflows for code verification; report live scheduler or storage checks separately from unit-test results.

CMS Rucio, federation and environment providers are documented by the
[extension production skill](../../../extensions/cms/.agents/skills/rdfanalyzer-cms-production/SKILL.md).
Generic task graphs and transport engines remain framework contracts.

For an authorized runtime upgrade with active monitors, use
[update_queued_xrd_runtime.py](../../../core/python/update_queued_xrd_runtime.py).
It creates an immutable library-only archive and conditionally edits only idle,
never-started jobs. It preserves original artifacts and running jobs, without
hold/release operations that monitors would treat as failures. Preview before
applying and inspect the receipts; future retries from original artifacts retain
the old runtime by default. `--refresh-retries --apply` additionally backs up
and atomically replaces the original archive and loose library, verifying every
unrelated archive member is unchanged. This library-only refresh can run with
active monitors; it leaves their state and worker scripts intact. Older immutable
patches and tool-generated Condor transfer expressions can also be upgraded.
This is distinct from the broader stopped-monitor worker-script refresh workflow.
