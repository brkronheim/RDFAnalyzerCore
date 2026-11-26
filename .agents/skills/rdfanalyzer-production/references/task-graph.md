# Production task graph and staging

The [Snakemake production workflow](../../../../examples/snakemake/README.md)
accepts a sealed era collection or explicit per-era specs.
[campaign.py](../../../../core/python/rdfproduction/campaign.py)
validates the selected era inputs and output ownership before preflight.
The standalone path needs neither LAW nor Luigi.
The [Python source map](../../../../core/python/README.md) links to the
individual production stages. The [jobs package](../../../../core/python/rdfproduction/jobs/)
separates preparation, staging, submission, monitoring and output validation.
[discovery.py](../../../../core/python/rdfproduction/discovery.py) materializes
input inventories and [executors.py](../../../../core/python/rdfproduction/executors.py)
runs local jobs and dispatches branches to Dask.
[cli.py](../../../../core/python/rdfproduction/cli.py) provides
`inspect`, `preflight`, `prepare`, `smoke`, `submit`, `execute`, `status`, `merge`
and `verify`. Native build receipt checks are in
[build.py](../../../../core/python/rdfproduction/build.py), and
Python launchers are prepared in
[python_runtime.py](../../../../core/python/rdfproduction/python_runtime.py).
Use `plan` for offline inspection, `preflight` for runtime validation and
`prepare` for discovery and staging. Use `smoke` for local representative
jobs when the spec has no Condor sizing pilots. Calibration-enabled specs run pilots
with `submit` or `execute`. Use `verify` to check existing output and merge
receipts. See the [batch-stage contract](../../../../docs/BATCH_SUBMISSION.md#standalone-production-stages)
for initial submission recovery and the supported resume scope.

Use `backend: local` to run every prepared partition through the local skim
executor and publish the same verified index used by merge. The
[stitching composition](stitching.md) chains counter production, derivation,
application production, and validation using the verified indexes.
Use [`backend: dask`](../../../../examples/snakemake/README.md#dask-production)
for the same output contract over a shared filesystem. The local cluster
uses the rule's available cores. A configured Dask scheduler manages
its own workers.

The [downstream Snakemake stages](../../../../examples/snakemake/README.md#downstream-analysis)
bind histogram refills to the verified skim files, publish job manifests,
and hand those manifests to the shared merge, plot, datacard and fit services.
Existing LAW task entry points call the same services.
The `fit_diagnostics` rule checks the fit receipt and renders the saved
backend diagnostics through
[diagnostic_plots.py](../../../../core/python/statistical_inference/diagnostic_plots.py).
Start the standalone workflow at a [completed product](../../../../examples/snakemake/README.md#starting-from-completed-products), including verified skim indexes, native histogram inputs, a datacard, or a fit receipt.

For completed-output partial merges, `partial_merge.py --sparse-only` skips
legacy TH1 expansion while retaining the shared metadata merge, physical-axis
validation and companion artifacts. The generic compatibility default still
extracts TH1s; the vjet `mergeResults.sh` wrapper chooses sparse-only by default
and accepts `--extract-histograms` for compatibility. An explicit
`--sparse-only` cannot be combined with `--nominal-only`.

For sealed analysis inputs use [run bundles](../../../../docs/RUN_BUNDLES.md):
[run_bundle.py](../../../../core/python/run_bundle.py) owns publication,
asset closure, verification and execution identities;
[run_collection.py](../../../../core/python/run_collection.py) owns isolated
child names and bounded concurrent launches. Collections are not job bundles.
Sealed staging uses declared assets, not a live ancestor `aux/` directory.
Changed execution identities require a new run name; never bypass the check
by deleting identity records from an active run.

[partial_merge.py](../../../../core/python/partial_merge.py) adapts completed
monitor entries to the shared metadata merger and records snapshot membership.
[analysis_inputs.py](../../../../core/python/analysis_inputs.py) resolves one
dataset representation and normalization from the consumed input counters.

Read [LAW_TASKS](../../../../docs/LAW_TASKS.md), [BATCH_SUBMISSION](../../../../docs/BATCH_SUBMISSION.md), and the actual analysis wrapper. Resolve executable, submit config, manifest/file-source mode, era collection, run name, scheduler/backend, inputs, output destination, requested CPUs/memory, and environment before constructing a command.

File-source preparation treats the dataset manifest as authoritative. Extra
cached samples are ignored; missing, empty or duplicate requested discovery
payloads fail preparation. Verify discovery-cache names as well as LAW run
names for concurrent era workflows; inherited wrapper environment overrides
can otherwise point all children at one writable cache.

For environment/module registration use [LAW setup](law-environment.md); for every concrete task family use [task routing](law-tasks.md). Read [stitching](stitching.md), [merges](../../rdfanalyzer-outputs/references/law-merges.md), [plots](../../rdfanalyzer-plotting/references/law-plots.md), or [fits](../../rdfanalyzer-statistics/references/law-fits.md) for that consumer's completion and artifact contract.

## Relevant implementation

| Work | Entry points |
|---|---|
| Input discovery | [xrdfs_tasks.py](../../../../core/python/law/xrdfs_tasks.py), [rucio_tasks.py](../../../../extensions/cms/python/rdfanalyzer_cms/rucio_tasks.py), [opendata_tasks.py](../../../../core/python/law/opendata_tasks.py) |
| Dataset metadata and file-list conversion | [dataset_manifest.py](../../../../core/python/dataset_manifest.py), [build_manifest_from_filelist_dir.py](../../../../core/python/law/build_manifest_from_filelist_dir.py) |
| Prepare, test, dispatch, monitor, fill | [analysis_tasks.py](../../../../core/python/law/analysis_tasks.py): `PrepareSkimJobs`, `RunSkimTestJob`, `BuildSkimSubmission`, `SubmitSkimJobs`, `MonitorSkimJobs`, `SkimTask`, `HistFillTask` |
| Optional missing-rate pilots | `RunSkimSizingPilots`: discovery, bounded one-file Condor batches, paired prefix/full timing fit, then bulk preparation. |
| Shell/Condor submission generation | [submission_backend.py](../../../../core/python/submission_backend.py), [htcondor_bootstrap.sh](../../../../core/python/law/htcondor_bootstrap.sh) |
| Dask/executor behavior | [workflow_executors.py](../../../../core/python/law/workflow_executors.py), [branch_map_policy.py](../../../../core/python/law/branch_map_policy.py), [partitioning.py](../../../../core/python/rdfproduction/partitioning.py) |
| Retry classification and metrics | [retry.py](../../../../core/python/rdfproduction/retry.py), [performance.py](../../../../core/python/rdfproduction/performance.py) |
| Merge/full DAG | [merge_tasks.py](../../../../core/python/law/merge_tasks.py), [dag_tasks.py](../../../../core/python/law/dag_tasks.py) |
| Other production state machine | [production_manager.py](../../../../core/python/production_manager.py), [production_monitor.py](../../../../core/python/production_monitor.py) |

Inspect `requires`, `output`, `complete`, branch maps, and delegated commands to determine what a LAW invocation will do. A top-level flag can affect several stages. A collection of era configs is not necessarily one LAW bundle. A test marker can change whether test/dispatch stages repeat; preserve valid markers and verify their meaning before changing them.

Inspect `_write_job_config`, config/payload copying helpers, shared-input archive creation, runtime materialization, and output-destination rewriting. Worker cwd and generated config determine path resolution. Normalization mode, sample `dtype`/legacy `type`, staged payloads, executable, shared libraries, and archives must agree. Source-template edits do not update an already-prepared bundle.

Legacy prepared job directories expose `aux/` through a local link; remote
workers receive its contents through the shared archive. Sealed bundles use
their declared dependency closure. `MonitorSkimJobs.complete()` checks fresh
storage metadata under the selected validation policy. In `integrity` mode,
matching persistent receipts skip ROOT opens; changed artifacts receive the
full tree/metadata/counter checks. Missing files still invalidate completion.
The storage-signature contract and its remote timestamp precision limits are
documented in [monitor validation](law-tasks.md). `exists` mode checks paths
without opening ROOT files and does not certify integrity receipts.

For merges, inspect output-manifest validation and embedded histogram schema before `hadd` or equivalent aggregation. Choose resources within allocation and confirm where final outputs are written. Use [outputs](../../rdfanalyzer-outputs/SKILL.md) for schema contracts and [diagnosis](diagnosis.md) for recovery rather than rebuilding the workflow from defaults.
