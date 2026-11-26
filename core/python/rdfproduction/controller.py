"""Own production runtimes, job admission, execution, and recorded status."""

from __future__ import annotations

from contextlib import contextmanager
import fcntl
from importlib import import_module
import json
import os
from pathlib import Path
import tempfile

from run_bundle import content_digest, file_digest, find_bundle, record_execution, write_json
from . import REPOSITORY_ROOT, SOURCE_LAYOUT
from .specs import _json_object, _positive_integer, inspect_spec, normalize_spec_paths, validate_run_name


_WORKSPACE = REPOSITORY_ROOT if SOURCE_LAYOUT else Path.cwd()
_CLAIM = "rdf_production.json"
_RUNTIME_RECEIPT = "validated_runtime.json"


def run_directory(name, *, workspace=None):
    """Return the production run's path without opening or creating it."""
    return Path(_WORKSPACE if workspace is None else workspace) / f"skimRun_{validate_run_name(name)}"


@contextmanager
def _claim_run(workspace, plan):
    run_dir = run_directory(plan["task_parameters"]["name"], workspace=workspace)
    run_dir.parent.mkdir(parents=True, exist_ok=True)
    # Keep the lock beside the run so checking an old campaign leaves its
    # directory intact. An OS lock is released when the controller exits.
    with run_dir.with_name(f".{run_dir.name}.rdf-production.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"an RDF production controller already owns {run_dir}") from exc
        if run_dir.exists() or run_dir.is_symlink():
            if run_dir.is_symlink() or not (run_dir / _CLAIM).is_file():
                raise RuntimeError(f"run directory already belongs to another workflow: {run_dir}")
            record_execution(run_dir / _CLAIM, plan)
        else:
            # Publish the new run and its claim together, before any task can
            # stage inputs or contact the scheduler.
            with tempfile.TemporaryDirectory(prefix=f".{run_dir.name}.", dir=workspace) as temp:
                candidate = Path(temp) / "run"
                record_execution(candidate / _CLAIM, plan)
                candidate.rename(run_dir)
        yield run_dir


@contextmanager
def _credential_environment(proxy):
    previous = os.environ.get("X509_USER_PROXY")
    if proxy:
        if not Path(proxy).is_file() or not os.access(proxy, os.R_OK):
            raise ValueError(f"credential reference is not readable: {proxy}")
        os.environ["X509_USER_PROXY"] = proxy
    try:
        yield
    finally:
        if proxy:
            if previous is None:
                os.environ.pop("X509_USER_PROXY", None)
            else:
                os.environ["X509_USER_PROXY"] = previous


def _native_build(plan, executable, run_receipt, validated):
    from .build import build_verified, verify_build_receipt

    build = plan["build"]
    build_directory = Path(build["build_directory"])
    cache = build_directory / ".rdf-production"
    cache.mkdir(parents=True, exist_ok=True)
    key = content_digest({"build": build, "executable": executable})
    directory = cache / key
    directory.mkdir(exist_ok=True)
    with (cache / "build.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if run_receipt.is_file():
            previous = _json_object(run_receipt)
            receipt = Path(previous["build_receipt"]).resolve()
            if (not receipt.is_relative_to(directory) or not receipt.is_file()
                    or file_digest(receipt) != previous.get("build_receipt_sha256")):
                raise RuntimeError(f"restore the original validated build receipt for {run_receipt.parent}")
            verify_build_receipt(receipt, build, executable)
        elif key in validated:
            receipt = validated[key]
            verify_build_receipt(receipt, build, executable)
        else:
            built = build_verified(build, executable, directory)
            receipt = Path(built["log_directory"]) / "receipt.json"
            record_execution(receipt, built)
        record_execution(run_receipt, {
            "kind": "cmake", "spec_id": plan["spec_id"], "executable": executable,
            "executable_sha256": file_digest(executable), "build_receipt": str(receipt),
            "build_receipt_sha256": file_digest(receipt),
        })
        validated[key] = receipt


def _runtime_parameters(plan, run_dir, validated=None):
    parameters = dict(plan["task_parameters"])
    receipt = run_dir / _RUNTIME_RECEIPT
    if "build" in plan:
        _native_build(plan, parameters["exe"], receipt, {} if validated is None else validated)
    elif "python" in plan:
        from .python_runtime import materialize_python
        if receipt.is_file():
            previous = _json_object(receipt)
            executable = Path(previous["executable"])
            if not executable.is_file() or not os.access(executable, os.X_OK):
                raise RuntimeError(f"restore the original Python launcher: {executable}")
        else:
            executable = materialize_python(plan["python"], run_dir / "runtime")
        parameters["exe"] = str(executable)
        record_execution(receipt, {
            "kind": "python", "spec_id": plan["spec_id"], "executable": str(executable),
            "executable_sha256": file_digest(executable),
        })
    return parameters


def _check_build_recipes(plans):
    directories, executables = {}, {}
    for plan in plans:
        executable = plan["task_parameters"].get("exe")
        if executable is None:
            continue
        build = plan.get("build")
        if executables.setdefault(executable, build) != build:
            raise ValueError(f"campaign uses different build recipes for {executable}")
        if build is not None:
            directory = build["build_directory"]
            configuration = (build["source_directory"], build["configure_args"], build["configuration"])
            if directories.setdefault(directory, configuration) != configuration:
                raise ValueError(f"campaign uses incompatible configurations in {directory}; choose separate build directories")


def _require_dask():
    try:
        module = import_module("distributed")
        if not callable(module.Client) or not callable(module.as_completed):
            raise ImportError("Dask distributed client unavailable")
    except (ImportError, AttributeError) as exc:
        raise RuntimeError("Dask production requires dask[distributed] in the controller environment") from exc


def preflight(*paths):
    """Validate runtimes together without discovery or scheduler access."""
    plans = [inspect_spec(path) for path in paths]
    if any(plan.get("backend") == "dask" for plan in plans):
        _require_dask()
    _check_build_recipes(plans)
    builds = {}
    results = []
    for plan in plans:
        with _claim_run(plan.get("state_root", _WORKSPACE), plan) as run_dir:
            parameters = _runtime_parameters(plan, run_dir, builds)
            receipt = run_dir / _RUNTIME_RECEIPT
            results.append({"spec_id": plan["spec_id"], "executable": parameters["exe"],
                            "run_dir": str(run_dir), "runtime_receipt": str(receipt) if receipt.is_file() else None})
    return results


def _prepare(run):
    from .discovery import discover_inputs

    if run.file_source:
        discover_inputs(run.workspace, run.name, run.file_source, run.dataset_manifest,
                        submit_config=run.submit_config, file_source_name=run.file_source_name,
                        files_per_job=run.files_per_job, rucio_group_size_gb=run.rucio_group_size_gb,
                        xrdfs_server=run.xrdfs_server, xrdfs_pattern=run.xrdfs_pattern,
                        xrdfs_recursive=run.xrdfs_recursive, workers=run.prepare_workers)
    if run.sizing_pilots and not run.pilots_complete():
        plan = run._prepare_plan()
        for batch in plan["batches"]:
            run.publish_message(f"Calibrating {len(batch['datasets'])} dataset(s) in {batch['name']}")
            child = run.pilot_run(batch)
            _condor(child)
            if not child.monitoring_complete():
                raise RuntimeError(f"Sizing pilot batch failed: {child.name}; bulk preparation stopped")
        run.collect_pilot_results(plan)
    if run.preparation_complete():
        return False
    run.prepare()
    return True


def _build_submission(run):
    if run.submitted_path.exists() or run.submission_manifest_path.with_name("submission_journal.json").exists():
        if not run.submission_manifest_path.is_file():
            raise RuntimeError(f"restore the original submission manifest before resuming: {run._run_dir}")
        return
    if _prepare(run) or not run.submission_manifest_path.is_file():
        run.build_submission()


def _smoke(run):
    if not run.smoke_complete():
        run.smoke()


def _condor(run, *, submit_only=False, allow_partial=False):
    if not run.submitted_path.is_file():
        _build_submission(run)
        if run.make_test_job:
            _smoke(run)
        run.submit()
    if submit_only:
        return
    complete = run.monitoring_complete(allow_partial=True) if allow_partial else run.monitoring_complete()
    if not complete:
        run.monitor()


def _execute_local(run, backend, workers, available_cores):
    from concurrent.futures import ThreadPoolExecutor
    from .executors import dispatch_dask

    branches = {index: run.for_branch(index, entry)
                for index, entry in enumerate(run._prepared_branch_entries())}
    if not branches:
        raise RuntimeError(f"No prepared partitions in {run._run_dir}")
    if backend == "dask":
        threads = min(run.dask_threads_per_worker, available_cores or run.dask_threads_per_worker)
        capacity = available_cores // threads if available_cores else run.dask_workers
        dispatch_dask(
            {index: branch.branch_data for index, branch in branches.items()},
            complete=lambda index: branches[index].branch_complete(),
            work=run.get_dask_work,
            output_path=lambda index: branches[index].branch_output_path(),
            scheduler=run.dask_scheduler,
            workers=min(run.dask_workers, capacity),
            threads_per_worker=threads,
        )
    else:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            pending = [pool.submit(branch.run_branch) for branch in branches.values()
                       if not branch.branch_complete()]
            for future in pending:
                future.result()
    return branches


def run_stage(path, *, output_index=None, submit_only=False, smoke_only=False, local_workers=None,
              retry_limit=None, allow_partial=False):
    """Prepare, run representative inputs, submit, or execute to a verified index."""
    if sum((output_index is not None, submit_only, smoke_only)) > 1:
        raise ValueError("choose one production stage")
    plan = inspect_spec(path)
    parameters = plan["task_parameters"]
    backend = plan.get("backend", "htcondor")
    shared_filesystem = backend != "htcondor"
    if (retry_limit is not None or allow_partial) and (shared_filesystem or output_index is None):
        raise ValueError("retry and partial completion overrides require HTCondor execute")
    if retry_limit is not None and (type(retry_limit) is not int or retry_limit < parameters.get("max_retries", 50)):
        raise ValueError("retry limit must be at least the sealed retry limit")
    if shared_filesystem and submit_only:
        raise ValueError(f"{backend} production uses execute; submit is for HTCondor")
    if backend == "dask" and output_index is not None:
        _require_dask()
    workers = plan["workers"]
    if local_workers is not None and shared_filesystem:
        local_workers = _positive_integer(local_workers, "local_workers")
        if backend == "local":
            workers = min(workers, local_workers)
    if output_index is None and not submit_only and parameters["sizing_pilots"]:
        raise ValueError("sizing_pilots submits Condor jobs. Use submit or execute with this spec")
    if smoke_only and not parameters.get("make_test_job", True):
        raise ValueError("smoke requires make_test_job to be enabled in the spec")
    from rdfproduction.jobs.run import ProductionRun
    if output_index is not None:
        from .output_index import write_local_output_index, write_verified_output_index

        destination = Path(output_index).resolve()
        protected = {Path(path).resolve()}
        if "exe" in parameters:
            protected.add(Path(parameters["exe"]))
        if (destination in protected
                or find_bundle(destination) is not None):
            raise ValueError("the output index must have its own path outside the sealed bundle")
    with _credential_environment(parameters.get("x509")), _claim_run(plan.get("state_root", _WORKSPACE), plan) as run_dir:
        parameters = _runtime_parameters(plan, run_dir)
        if retry_limit is not None:
            parameters["max_retries"] = retry_limit
        run = ProductionRun(**parameters, workspace=str(run_dir.parent))
        if submit_only:
            _condor(run, submit_only=True)
            return run.submitted_path
        if smoke_only:
            _prepare(run)
            _smoke(run)
            return run.smoke_path
        if output_index is None and not shared_filesystem:
            _build_submission(run)
            return run.submission_manifest_path
        if shared_filesystem:
            _prepare(run)
            if output_index is None:
                return run.preparation_path
            if run.make_test_job:
                _smoke(run)
            branches = _execute_local(run, backend, workers, local_workers)
            write_local_output_index(run_dir, destination, backend=backend,
                                     validate=lambda index: branches[index].branch_complete())
        else:
            if allow_partial:
                _condor(run, allow_partial=True)
                write_verified_output_index(run_dir, destination, allow_partial=True,
                                            validate=lambda: run.monitoring_complete(allow_partial=True))
            else:
                _condor(run)
                write_verified_output_index(run_dir, destination, validate=run.monitoring_complete)
        write_json(run_dir / "published_output_index.json",
                   {"path": str(destination), "sha256": file_digest(destination)})
        return destination


def status(run_dir):
    """Summarize recorded state without contacting the scheduler or storage."""
    run_dir = Path(run_dir).resolve()
    claim = _json_object(run_dir / _CLAIM)
    records = (
        "prep_submission.json", "submission_manifest.json", "submission_journal.json", "submitted.json", "monitor_state.json",
        "sizing_pilots/result.json", "job_sizing_selection.json", "job_sizing_update.json",
        "xrd_site_rankings_update.json", _RUNTIME_RECEIPT, "published_output_index.json",
        "test_job/test_passed.txt", "all_outputs_verified.txt",
    )
    files = {name: (run_dir / name).stat().st_mtime for name in records if (run_dir / name).is_file()}
    counts = {}
    if "monitor_state.json" in files:
        for dataset in _json_object(run_dir / "monitor_state.json")["datasets"].values():
            for job in dataset["jobs"].values():
                state = job["status"]
                counts[state] = counts.get(state, 0) + 1
    batches = {}
    if "submission_journal.json" in files:
        batches = _json_object(run_dir / "submission_journal.json")["batches"]
    runtime = _json_object(run_dir / _RUNTIME_RECEIPT) if _RUNTIME_RECEIPT in files else None
    output_index = (_json_object(run_dir / "published_output_index.json")
                    if "published_output_index.json" in files else None)
    return {"run_dir": str(run_dir), "spec_id": claim["spec_id"],
            "backend": claim.get("backend", "htcondor"),
            "record_mtime_epoch": files, "recorded_partition_statuses": counts,
            "recorded_runtime": runtime, "recorded_output_index": output_index,
            "records": {name: str(run_dir / name) for name in files},
            "recorded_submissions": {
                name: {key: item[key] for key in ("id", "state", "cluster_id") if key in item}
                for name, item in batches.items()
            }}


def _output_signatures(paths):
    from rdfproduction.jobs.output_validation import _output_file_info_batch
    return _output_file_info_batch(paths)


def main(argv=None):
    """Preserve the original rdf_production.main Python entry point."""
    from .cli import main as command_main

    return command_main(argv)
