# LAW task families and completion contracts

Use this map to select the relevant workflow source and detail reference. Every named task below is a concrete class in the current checkout; [components.yaml](../../rdfanalyzer-development/references/components.yaml) records class-to-reference routes checked without importing LAW.

| Task family | Classes | Source / details |
|---|---|---|
| Remote discovery | `GetXRDFSFileList`, `GetRucioFileList`, `GetOpenDataFileList` | [xrdfs_tasks](../../../../core/python/law/xrdfs_tasks.py), [rucio_tasks](../../../../extensions/cms/python/rdfanalyzer_cms/rucio_tasks.py), [opendata_tasks](../../../../core/python/law/opendata_tasks.py) |
| Preparation and bounded test | `PrepareSkimJobs`, `RunSkimTestJob`, `RunSkimSizingPilots` | [analysis_tasks](../../../../core/python/law/analysis_tasks.py); [staging](task-graph.md) |
| Delegated Condor dispatch | `BuildSkimSubmission`, `SubmitSkimJobs`, `MonitorSkimJobs` | analysis_tasks; [diagnosis](diagnosis.md) |
| Event execution and refill | `SkimTask`, `HistFillTask` | analysis_tasks; [environment/backend](law-environment.md) |
| Stitching | `StitchingDerivationTask`, `StitchingApplySubmitJobs`, `StitchingApplyMonitorJobs`, `StitchingApplyTask`, `StitchingValidationTask` | [two-pass stitching](stitching.md) |
| Skim-tree collection | `CollectHistogramsTask` | [merges and collection](../../rdfanalyzer-outputs/references/law-merges.md); inspect its actual `skim.root` glob |
| Manifest-aware merge | `MergeSkims`, `MergeHistograms`, `MergeCutflows`, `MergeMetadata`, `MergeAll` | [merges](../../rdfanalyzer-outputs/references/law-merges.md) |
| Plotting | `MakePlot`, `MakePlots`, `ManifestPlotTask` | [LAW plots](../../rdfanalyzer-plotting/references/law-plots.md) |
| Datacards and fits | `CreateDatacard`, `RunCombine`, `ManifestDatacardTask`, `ManifestFitTask`, `AnalyticWorkspaceFitTask` | [LAW fits](../../rdfanalyzer-statistics/references/law-fits.md) |
| Terminal orchestration | `FullAnalysisDAG` | [dag_tasks](../../../../core/python/law/dag_tasks.py); inspect `requires()` and its selected terminal tasks |

## Inspect execution and completion separately

Skim credentials require at least 48 hours (172800 seconds) of remaining
proxy validity. `PrepareSkimJobs` validates the copied `shared_inputs/x509`;
`BuildSkimSubmission`, `SubmitSkimJobs`, and `MonitorSkimJobs` retries validate
the worker copy inside `shared_inputs.tar.gz` (or the loose file for legacy
layouts). This also protects cached preparations. Missing configured proxies
and failed validity checks stop the task before submission. Proxy-free runs
remain supported. Renewing a source proxy does not refresh staged credentials;
rerun preparation to copy it and rebuild the archive.
Changed source-proxy contents invalidate `PrepareSkimJobs.complete()`.

Executable freshness uses SHA-256 equality with the current `--exe` binary.
`PrepareSkimJobs.complete()` also checks the archived executable;
`BuildSkimSubmission`, `SubmitSkimJobs`, and monitor retries reject missing,
unreadable, or mismatched worker executables before dispatch, preferring the
copy inside `shared_inputs.tar.gz` over loose staging. Timestamp or size
agreement alone is insufficient. Restage through preparation, respecting
sealed execution identities and choosing a new run name when those change.
The reference is the built binary, so compile source changes first.

For delegated skim monitoring, `SkimMixin.output_validation` selects `exists`
(batched path checks without ROOT opens) or `integrity` (the framework default,
also checking ROOT contents and MC counters). It propagates from `SkimTask` to
`MonitorSkimJobs` without changing the run identity. Both monitor polling and
completion use the policy. Monitoring does not aggregate sample counters or
write `normalization.json`; downstream consumers read the counters from their
input ROOT files when needed. Analysis wrappers can choose a different default.

Polling and completion share a persistent `output_validation.json` receipt
cache. Directory-level `xrdfs ls -l` batches (up to four directories in flight)
check existence, size and mtime, preserving each URL's server. Unchanged output
pairs reuse their integrity receipts; changed/missing outputs or changed
configuration, manifest, execution identity, tree or job attempt invalidate
them. Local signatures additionally include inode, device and nanosecond
mtime/ctime. Old `done` states without receipts receive one full validation;
existence-only or unknown-signature checks never certify ROOT integrity.
Receipts checkpoint every 25 results or ten seconds, and on normal exception
unwinding, using a locked atomic merge separate from monitor state. Cache-write
failure warns without failing validated outputs. Remote signatures are storage
metadata, not content hashes: same-size changes preserving the listed mtime
need explicit cache removal and revalidation.

On successful completion, `MonitorSkimJobs.run()` automatically refines the
origin-scoped XRootD user cache before writing its completion marker. It keeps
the bundled seed unchanged and writes `xrd_site_rankings_update.json` in the run
directory. This is ancillary: update failures warn without invalidating verified
physics outputs. `complete()` never backfills learned rankings/sizing or changes
job state; it may refresh output-validation receipts. See [ranking aggregation](../../../../core/python/xrd_site_rankings.py)
and its `update_after_run` entry point. Newly generated submission scripts
reuse the learned cache unless an explicit ranking file is pinned.

`MonitorSkimJobs` checks running jobs for runtime overruns on every poll and
removes them above 150% of their Condor `MaxRuntime` (falling back to the task's
`max_runtime` when the ad has no positive limit). It measures the current
execution using `ServerTime - JobCurrentStartDate`, with local-clock and missing
timestamp fallbacks; do not rely solely on cumulative `RemoteWallClockTime`.
Only successful removal enters the existing retry/splitting path, which respects
`max_retries`. See `_condor_job_elapsed_runtime` and `_condor_job_exceeded_runtime`
in `analysis_tasks.py` and the runtime-monitor tests in
[test_analysis_tasks.py](../../../../core/tests/law/test_analysis_tasks.py).

File/event policy is implemented by `partition_utils.py` and
[job_sizing.py](../../../../core/python/job_sizing.py). The latter owns offline
CPU-rate learning, process/dataset budget rules and tree-entry cache snapshots.
`partition=auto` is the default: first-run grouping and learned event budgets
for matching datasets later. Verified monitor completion learns from stdout and
Condor Run Remote Usage; the shared cache is origin/workload scoped, locked and
idempotent. Preparation snapshots its choice once; no shared-cache reads or
learning belong in `complete()`. Test cold starts, later reuse, explicit pins,
concurrent updates, cache failures and immunity of prepared identities to updates.
`--sizing-pilots` inserts `RunSkimSizingPilots` between discovery and preparation.
It submits one-file diagnostic Condor jobs only for datasets without sizing
evidence, with `--sizing-pilot-parallel` bounding batch size. Child runs disable
pilot recursion and retries. Each worker uses
[job_sizing_worker.py](../../../../core/python/job_sizing_worker.py) to pair a
short prefix with the full file. Fit CPU/wall slopes separately from fixed
setup, then freeze the combined profile before bulk preparation. Keep diagnostic
outputs outside production merge paths. Paired job CPU must never enter ordinary
single-execution rate learning; provisional priors are replaced by supported
production rates. Test batch gating, timing cancellation, worker execution and
failed-pilot behavior without submitting jobs.
Size fresh and cached pilot priors with `--sizing-pilot-target-seconds` (1800),
and production rates with `--sizing-target-seconds` (3600). `--max-runtime`
(7200) independently controls the Condor request. Test mixed evidence, explicit
profile precedence, custom targets and frozen selection reuse.
See [job-sizing controls](../../../../docs/JOB_SIZING.md). Preserve existing
execution identities when controls are unused; explicit profile/seed contents
are hashed. Test both prep and fallback branch maps, stitching forwarding,
cached-group precedence, and half-open retry coverage when changing this path.

For a requested command, resolve task parameters, their `significant` settings, `requires`/`workflow_requires`, branch maps, output targets, overridden `complete`, and `run`. Read [branch_map_policy.py](../../../../core/python/law/branch_map_policy.py) and [partitioning.py](../../../../core/python/rdfproduction/partitioning.py) for the actual partition strategy. A parameter marked nonsignificant can change behavior without changing task identity. Confirm the existing artifact/config before reusing a run name.

For discovery, distinguish upstream run name and file-list artifacts from a skim run. Inspect redirector precedence and dataset selection in the chosen discovery module; do not run fresh remote discovery to diagnose an existing local bundle unless needed by the request.

`FullAnalysisDAG` chooses downstream terminal tasks and relies on their dependency chains. Inspect `_skim_stage_enabled`, `_histfill_stage_enabled`, `_merge_stage_enabled`, resolved manifest and effective merge input directory; `requires()` is not an unconditional sequence of every stage. Existing-manifest/skip modes can bypass production. Some optional imports are caught; compare actual requirements with intended stages. Check forwarding such as `strict_coverage` through the specific terminal-task branch instead of assuming a top-level parameter reaches every consumer.

A done marker or existing directory can satisfy a target without proving every physical artifact or fit exists. Inspect the task's completion contract and provenance. Manifest plotting can produce stub files; analytic workspace generation can succeed without running Combine. See the consumer references for those behaviors.

Verification: [DAG tests](../../../../core/tests/law/test_dag_tasks.py), [branch maps](../../../../core/tests/law/test_branch_map_policy.py), [partitioning](../../../../core/tests/law/test_partition_utils.py), discovery tests in [core/tests/law](../../../../core/tests/law), and the selected consumer tests. No scheduler submission is necessary to validate source navigation or class names.
