# LAW task families and completion contracts

Use this map to select the relevant workflow source and detail reference. Every named task below is a concrete class in the current checkout; [components.yaml](../../rdfanalyzer-development/references/components.yaml) records class-to-reference routes checked without importing LAW.

| Task family | Classes | Source / details |
|---|---|---|
| Remote discovery | `GetXRDFSFileList`, `GetRucioFileList`, `GetOpenDataFileList` | [xrdfs_tasks](../../../../core/python/law/xrdfs_tasks.py), [rucio_tasks](../../../../extensions/cms/python/rdfanalyzer_cms/rucio_tasks.py), [opendata_tasks](../../../../core/python/law/opendata_tasks.py) |
| Preparation and bounded test | `PrepareSkimJobs`, `RunSkimTestJob` | [analysis_tasks](../../../../core/python/law/analysis_tasks.py); [staging](task-graph.md) |
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
completion use the policy. Final normalization still reads MC counters once
after all jobs finish. Analysis wrappers can choose a different default.

`MonitorSkimJobs` checks running jobs for runtime overruns on every poll and
removes them above 150% of their Condor `MaxRuntime` (falling back to the task's
`max_runtime` when the ad has no positive limit). It measures the current
execution using `ServerTime - JobCurrentStartDate`, with local-clock and missing
timestamp fallbacks; do not rely solely on cumulative `RemoteWallClockTime`.
Only successful removal enters the existing retry/splitting path, which respects
`max_retries`. See `_condor_job_elapsed_runtime` and `_condor_job_exceeded_runtime`
in `analysis_tasks.py` and the runtime-monitor tests in
[test_analysis_tasks.py](../../../../core/tests/law/test_analysis_tasks.py).

For a requested command, resolve task parameters, their `significant` settings, `requires`/`workflow_requires`, branch maps, output targets, overridden `complete`, and `run`. Read [branch_map_policy.py](../../../../core/python/law/branch_map_policy.py) and [partition_utils.py](../../../../core/python/law/partition_utils.py) for the actual partition strategy. A parameter marked nonsignificant can change behavior without changing task identity. Confirm the existing artifact/config before reusing a run name.

For discovery, distinguish upstream run name and file-list artifacts from a skim run. Inspect redirector precedence and dataset selection in the chosen discovery module; do not run fresh remote discovery to diagnose an existing local bundle unless needed by the request.

`FullAnalysisDAG` chooses downstream terminal tasks and relies on their dependency chains. Inspect `_skim_stage_enabled`, `_histfill_stage_enabled`, `_merge_stage_enabled`, resolved manifest and effective merge input directory; `requires()` is not an unconditional sequence of every stage. Existing-manifest/skip modes can bypass production. Some optional imports are caught; compare actual requirements with intended stages. Check forwarding such as `strict_coverage` through the specific terminal-task branch instead of assuming a top-level parameter reaches every consumer.

A done marker or existing directory can satisfy a target without proving every physical artifact or fit exists. Inspect the task's completion contract and provenance. Manifest plotting can produce stub files; analytic workspace generation can succeed without running Combine. See the consumer references for those behaviors.

Verification: [DAG tests](../../../../core/tests/law/test_dag_tasks.py), [branch maps](../../../../core/tests/law/test_branch_map_policy.py), [partitioning](../../../../core/tests/law/test_partition_utils.py), discovery tests in [core/tests/law](../../../../core/tests/law), and the selected consumer tests. No scheduler submission is necessary to validate source navigation or class names.
