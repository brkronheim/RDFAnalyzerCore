# Event budgets and workload profiles

The default `--partition auto` learns job sizes without manual profile files.
On the first run it uses ordinary file grouping and the configured file/byte
limits. After `MonitorSkimJobs` verifies every output, it learns CPU/event rates
and file entry counts from returned stdout and HTCondor event logs. A future
submission with a matching workload uses learned event budgets for measured
datasets; unmeasured datasets keep ordinary file grouping. Small runs can
accumulate evidence across completed submissions. No scheduler-history query is
required, and permanently failed runs do not update the shared cache.

Use `--partition events` to force grouping whole files by input-event count, then split
any oversized file into half-open entry ranges. This bounds computation more
reliably than a uniform file count when samples have different event densities.
CPU cost per event also varies by process, jet/pT bin, era, uncertainty setup and
worker. A workload profile converts a target runtime into a dataset event budget.
An explicit `--partition-profile` takes precedence over automatic lookup.

```bash
law run analysis_tasks.SkimTask ... \
  --partition events --entries-per-job 500000 \
  --partition-profile partition_profile.json \
  --input-metadata-cache input_metadata.json \
  --entry-range-mode source
```

`events` keeps the file-count limit and packs within existing discovery groups,
so Rucio's byte-size boundaries remain in effect. It does not merge across those
boundaries. `file` and `entry_range` explicitly override cached groups. A tighter
`--files-per-job` also applies inside cached groups in `file_group` mode.
Existing prepared workers are not changed by these settings: use a fresh run
name and regenerate job configs.

`entry_range` slices every file independently. `events` first packs smaller
whole files and only slices files exceeding the event budget. This usually
avoids repeated opens and prefix handling for files that fit within a job.
Empty files remain represented; absent or failed metadata queries are errors,
not zero-entry files. Input URLs are sorted before packing for deterministic
plans, and duplicate/inconsistent discovery groups are rejected.

Entry-range execution requires one RDF thread. `--entry-range-mode source`
uses direct source seeking on the validated ROOT 6.40 series, avoiding logical
`Range` traversal of rejected prefix entries. `filter` remains the compatibility
default. Configuration rejects source mode on unsupported ROOT versions.
Automatic mode retains file grouping when the submit config explicitly requests
multiple RDF threads. Explicit event/range modes still require one thread.

## Automatic learning and reuse

The cache lives in `$XDG_CACHE_HOME/rdfanalyzer/job_sizing` (normally
`~/.cache/rdfanalyzer/job_sizing`). Set `RDF_JOB_SIZING_CACHE` to another directory
or `none` to disable shared-cache learning and lookup. Set
`RDF_JOB_SIZING_AUTO_UPDATE=0` to disable learning while retaining lookup, or
`--partition-profile none` to disable lookup for a submission. An explicit
`--partition file_group` or `file` retains file-based planning.

Profiles are scoped to the submission origin and workload contents. The default
origin is the submit host's fully qualified hostname; set
`RDF_JOB_SIZING_ORIGIN` to a stable cluster label when several login nodes share
the same worker pool. Distinct clusters should use distinct labels. Workload
matching includes the executable, local runtime libraries, ROOT/container setup,
analysis settings, stitching mode and declared selection/calibration payloads.
Changed code, cuts or systematics start with fresh evidence. Input file lists,
dataset membership, output locations and run names do not enter this workload
key. Rates are still selected by exact dataset name. Undeclared external runtime
changes cannot be fingerprinted by this mechanism.

Automatic budgets and the HTCondor runtime request are independent:

| Option | Default | Purpose |
|---|---:|---|
| `--sizing-target-seconds` | `3600` | One hour of estimated work using learned production rates |
| `--sizing-pilot-target-seconds` | `1800` | Thirty minutes of estimated work using provisional pilot rates |
| `--max-runtime` | `7200` | Two-hour HTCondor runtime request for either source |

The pilot target also applies when reusing cached pilot estimates, including
profiles mixing pilot and production evidence. Changing `--max-runtime` alone
does not change event budgets. Pilot targets use the existing safety-adjusted
CPU/wall rates and fixed setup reserve. Explicit profile budgets and runtime
targets retain precedence. These choices provide headroom but cannot guarantee
a wall-time limit. Automatic sizing currently learns from the delegated
HTCondor monitor; local/Dask completion paths do not
emit these Condor observations.

Each preparation saves `job_sizing_selection.json`, a selected profile at
`partition_profile.selected.json` when available, and an optional
`input_metadata.seed.json`. These are local snapshots. Their digests enter
preparation freshness and sealed execution identities; updates to the shared
cache cannot alter existing partitions or invalidate prepared jobs. Sealed runs
reject a changed workload or explicit sizing input. Legacy unsealed preparations
retain their existing refresh behavior. Use a fresh run name to adopt a
new profile, executable or sizing policy. `complete()` only inspects saved
state and does not learn or consult mutable shared history.

After verified completion, `job_sizing_update.json` records the learning result,
including skipped/error states. Cache updates are locked, atomic and idempotent.
They retain raw cost observations from up to 32 completed runs over 30 days and
recompute rates without treating previous percentiles as new observations.
Old profiles expire after 30 days. The final successful execution's **Run Remote
Usage** supplies CPU time; evicted/failed attempts and cumulative **Total Remote
Usage** cannot inflate its rate. Existing CPU/sample-quality gates below still
apply. A cache-write failure warns without retrying verified physics jobs.

## Optional pilots before bulk submission

Enable a calibration phase when starting a run with missing speed estimates:

```bash
law run analysis_tasks.SkimTask ... \
  --partition auto --sizing-pilots true \
  --sizing-pilot-parallel 8 \
  --sizing-pilot-prefix-entries 10000 \
  --sizing-pilot-safety-factor 1.5 \
  --sizing-pilot-target-seconds 1800 \
  --sizing-target-seconds 3600 --max-runtime 7200
```

`RunSkimSizingPilots` depends on discovery. It chooses the first sorted file for
each manifest dataset without a matching profile/budget. Already-profiled
datasets are skipped. It submits and verifies batches of at most
`--sizing-pilot-parallel` Condor jobs before preparing any bulk partitions.
The default is four pilot jobs at once; each runs its two measurements
sequentially. This limit is per LAW run: independently launched era workflows
each have their own limit. Batches wait for their slowest job before advancing.
Pilot jobs use the normal CPU/memory/environment and `--max-runtime` settings.

Each job processes a short prefix and then the **entire same file** in separate
processes, with the full analysis and uncertainty setup. The worker records raw
input counts, process CPU and wall time separately for both. For each timing
dimension it fits:

```
rate  = (T_full - T_prefix) / (N_full - N_prefix)
setup = max(0, T_prefix - rate * N_prefix)
```

This factors repeatable framework initialization, graph construction and initial
file-opening overhead out of the per-event rate. Worker environment setup and
stage-in/out wall time are recorded separately and added to the fixed reserve.
The event budget uses the slower CPU/wall slope and subtracts fixed setup from
the pilot target runtime (default 1800 seconds). Both terms receive the
configurable safety factor (default 1.5). A pilot is an approximation, not a
full model of worker differences or
per-file transition costs. Remote latency, cache warming, JIT and output costs
can vary between measurements. Existing file/byte caps and runtime headroom
remain in effect.

If the file is shorter than the prefix, or a timing slope is nonpositive, that
dimension explicitly falls back to its full-run average rather than claiming
an overhead subtraction. Empty files retain default sizing with a receipt.
An unsuccessful pilot or missing measurement stops bulk preparation. Pilots do
not split or retry failed files automatically, preserving the concurrency limit.

Results and the frozen input plan live under
`skimRun_<name>/sizing_pilots/`. Diagnostic child runs use names ending in
`__sizing_pilot_<batch>` and remote output goes to a **sibling**
`<output-directory>_sizing_pilots/` tree. Pilot outputs are excluded from the
production dataset inventory and must not be merged into production. Bulk jobs
still process the complete original inventory; pilots are additional work.
Paired CPU totals are excluded from ordinary job-rate learning. Provisional
pilot priors can be reused through the shared cache, and supported production
measurements replace them later. Cache publication failures warn; the current
run can still use its local pilot profile.

The feature is opt-in and currently requires a discovery file source
(`rucio`, `xrdfs`, or `opendata`), `partition=auto/events/entry_range`, one RDF
thread, and a remote output destination. The vjet wrapper accepts the same
options, using a flag-style `--sizing-pilots`; corresponding environment controls
are `RDF_VJET_SIZING_PILOTS=true`, `RDF_VJET_SIZING_PILOT_PARALLEL`,
`RDF_VJET_SIZING_PILOT_PREFIX_ENTRIES`, and `RDF_VJET_SIZING_PILOT_SAFETY_FACTOR`.
The independent time controls are also accepted by the wrapper and can be set
with `RDF_VJET_SIZING_TARGET_SECONDS`, `RDF_VJET_SIZING_PILOT_TARGET_SECONDS`,
and `RDF_VJET_MAX_RUNTIME`. Without pilots, production measurements still use
the one-hour sizing target and two-hour runtime request by default.

## Fixed budgets and process rules

Profiles accept JSON or YAML with `schema_version: 1`:

```yaml
schema_version: 1
target_runtime_seconds: 2700
rules:
  - process: TTbar
    entries_per_job: 1000000
  - dataset: "DYto2Mu*2J*"
    era: "2024"
    cpu_seconds_per_event: 0.004
# An exact dataset override takes precedence over all matching rules.
datasets:
  TTToHadrons_2024:
    cpu_seconds_per_event: 0.00025
```

These values illustrate the format; calibrate them for the actual workload.
Rules match `dataset`, `process`, `year` and `era` with case-sensitive globs.
The last matching rule wins, then the exact dataset mapping wins. Each sizing
layer chooses either `entries_per_job` or `cpu_seconds_per_event`. A new sizing
method replaces the inherited one. CPU-based budgets use
`floor(target_runtime_seconds / cpu_seconds_per_event)`, at least one event.
Unmatched datasets use `--entries-per-job`, unless the profile supplies
`defaults`. Unknown sizing fields, invalid rates and invalid budgets fail.

The selected profile and metadata seed are preparation inputs. Their contents
are hashed for execution identity/config freshness. Discovery's mutable query
cache is owned by the new run at `skimRun_<name>/input_metadata.json`; the seed
is copied, not modified. Do not share that writable per-run cache between
preparations. An explicit profile should represent the same analysis configuration
and worker population; changed systematics or selections require recalibration.

## Learn a profile from completed jobs

Export history with CPU times, identifiers and verified exit status:

```bash
condor_history -constraint 'RDFRun == "my_run"' -json \
  -attributes ClusterId,ProcId,JobStatus,ExitCode,ExitBySignal,RemoteUserCpu,RemoteSysCpu,LastRemoteHost \
  > history.json
python core/python/job_sizing.py 'skimRun_my_run*' \
  --history history.json --target-seconds 2700 \
  --profile partition_profile.json --metadata-cache input_metadata.json
```

The tool reads worker stdout; it does not submit, cancel or edit jobs. Size the
history query to cover the run (`condor_history` limits depend on local setup).
Only successful exits with a completed-worker marker, at least 100,000 raw
input events, and under 10% recorded prefetch wait provide CPU-cost samples.
Failed attempts can still supply valid tree-entry metadata but never rate
samples. Wall time is not substituted for CPU time.

Rates use an upper quantile (default p90), at least three samples per
sample/host pair. Matched datasets estimate relative host costs; fast-only
completed samples are projected to the slowest calibrated host rather than
assuming that all workers match the fast host. Disconnected host populations
retain observed rates without an invented relative speed. The output records
host factors and sample counts. Sparse/censored datasets still need a conservative
fallback budget; a profile is an estimate, not a runtime guarantee.

The entry cache is scoped by tree name. Public CMS LFNs share counts across
redirectors; other remote URLs retain their authority. Local files require
size/mtime fingerprints. Changed local files invalidate cached counts. Missing
counts are read through uproot once (30-second open timeout, bounded query
concurrency). Historical log imports omit local files because their past
content fingerprint cannot be recovered.

## Retry coverage and validation

Retries split an existing `[firstEntry,lastEntry)` interval before splitting
file lists. Both children retain the same chain order and output naming gains
part suffixes. A single-file whole-input job can become ranges using its
`plannedInputEntries` or cached tree count. One-entry intervals cannot shrink
further; ordinary retry limits still apply. Legacy single-file jobs without
counts keep their existing retry behavior rather than introducing an unbounded
remote metadata lookup into the monitor.

Before adopting a new partition policy, compare the sum of planned entries with
the full inventory and check counters, normalization and histogram contents of
split/merged output. Splitting changes accumulation order; merged float-backed
histograms may differ at storage rounding precision. Cache/profile snapshots
and execution plans should be retained with the run.

The vjet `scripts/submit_condor.sh` wrapper exposes these options and the
corresponding `RDF_VJET_PARTITION`, `RDF_VJET_ENTRIES_PER_JOB`,
`RDF_VJET_PARTITION_PROFILE`, `RDF_VJET_INPUT_METADATA_CACHE` and
`RDF_VJET_ENTRY_RANGE_MODE` environment variables. The default is `auto`:
first-run file grouping followed by matching learned event budgets. Explicit
event/range modes also consult learned history when no profile is supplied;
without a match they use `--entries-per-job` as the fallback.
