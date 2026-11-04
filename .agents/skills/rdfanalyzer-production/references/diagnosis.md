# Worker and recovery diagnosis

## Inspect before changing state

1. Identify owning wrapper/task system and run directory. Read its actual state JSON, generated configs, submission description, and event/worker logs.
2. Correlate dataset/job ID, retry part, Condor cluster/proc, submission time, event log, stdout/stderr, and expected output paths. Ask the scheduler for current state when available; if it fails, label saved state/log observations as snapshots.
3. Find the earliest concrete failure for that attempt. A generic held status such as `OnExitHold` does not identify the failing operation.
4. Separate executable startup/linkage, input open/read, analysis processing, local output writing, and stage-out. Match the exact error to the layer before proposing a retry or code fix.

For absent EOS output, inspect the staged proxy and exact `xrdcp` stderr as well as worker exit. Certificate warnings alone do not prove authentication failure. An explicit destination `No space left on device`/ENOSPC error requires storage capacity/quota remediation; more splitting/retries will repeat the upload failure. Local scratch and remote destination capacity are different checks.

For XRootD reads, inspect original/selected URL and current [input backend](../../rdfanalyzer-input-io/SKILL.md). Distinguish transient replica failures from deterministic bad input/schema or runtime-library problems. Inspect retry classification/splitting in [analysis_tasks.py](../../../../core/python/law/analysis_tasks.py) and [failure_handler.py](../../../../core/python/law/failure_handler.py) rather than assuming all failures merit more attempts.

## Complete an authorized recovery

Preserve the original run/state and identify the exact files workers will receive: loose `shared_inputs`, `shared_inputs.tar.gz`, per-job configs/scripts, executable, adaptor/shared libraries, and proxy. Compare contents/hashes as appropriate. Verify the generated config with a bounded local/test job when available. Report separately which source/build and which staged artifacts changed.

[refresh_xrootd_runtime.py](../../../../core/python/refresh_xrootd_runtime.py) refreshes prepared adaptor libraries and worker input helpers, replacing legacy probes with global routing. It defaults to preview and has `--apply`; inspect its current preconditions and supported layout. Its documented apply workflow requires stopped LAW monitors. It preserves the saved analysis executable, does not submit/remove jobs, and is not a generic installer for arbitrary analysis fixes. A diagnosis request alone does not authorize stopping monitors or applying changes; an already-authorized recovery need not be reconfirmed for routine reversible preparation.

Resume only within the requested action and retry scope. Stop repeated retries when evidence shows the same deterministic blocker remains. Do not delete successful outputs or discard retry history to make a workflow appear fresh.

Tests: [failure handler](../../../../core/tests/law/test_failure_handler.py), [analysis tasks](../../../../core/tests/law/test_analysis_tasks.py), [runtime refresh](../../../../core/tests/python/test_refresh_xrootd_runtime.py), [submission backend](../../../../core/tests/python/test_submission_backend.py), and [production monitor](../../../../core/tests/python/test_production_monitor.py).
