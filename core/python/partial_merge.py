"""Stage and atomically merge completed LAW outputs into provenance-tracked snapshots."""
from __future__ import annotations
import argparse
import fcntl
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from run_bundle import file_digest, write_json, derived_artifact, verify_artifact


def _acquire_merge_lock(path, wait=False):
    lock_file = Path(path).open("a+")
    try:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            if not wait:
                raise RuntimeError(f"Another merge process is already running (lock: {path})")
            print(f"Waiting for active merge to finish (lock: {path})", flush=True)
            fcntl.flock(lock_file, fcntl.LOCK_EX)
        lock_file.seek(0)
        lock_file.truncate()
        json.dump({"pid": os.getpid(), "command": sys.argv}, lock_file)
        lock_file.flush()
        return lock_file
    except BaseException:
        lock_file.close()
        raise


def merge_completed_outputs(state_path, merge_dir, *, hadd_jobs=4, xrdcp_jobs=4,
                            nominal_only=False, hadd_max_open_files=10000,
                            data_merge_threads=1, sparse_only=False,
                            semantic_config_compatibility=None, metadata_merger=None, wait_for_lock=False):
    if sparse_only and nominal_only:
        raise ValueError("--sparse-only and --nominal-only cannot be combined: nominal-only requires schema postprocessing")
    compatibility_policy = None
    if semantic_config_compatibility is not None:
        from metadata_io import load_semantic_config_compatibility
        semantic_config_compatibility = Path(semantic_config_compatibility).resolve()
        compatibility_policy = load_semantic_config_compatibility(semantic_config_compatibility)
    state_path, merge_dir = Path(state_path), Path(merge_dir)
    repo_root = Path(__file__).resolve().parents[2]
    extractor = repo_root / "core/python/extract_nd_histograms.py"
    cache_dir, logs_dir = merge_dir / ".input_cache", merge_dir / "logs"
    for directory in (merge_dir, cache_dir, logs_dir):
        directory.mkdir(parents=True, exist_ok=True)
    for label, value in (("hadd_jobs", hadd_jobs), ("xrdcp_jobs", xrdcp_jobs),
                         ("data_merge_threads", data_merge_threads)):
        if value < 1:
            raise ValueError(f"{label} must be positive")
    lock_file = _acquire_merge_lock(merge_dir / ".mergeResults.lock", wait_for_lock)
    try:
        state = json.loads(state_path.read_text())
        def safe_name(value):
            return re.sub(r"[^A-Za-z0-9_.-]+", "_", str(value)).strip("._") or "dataset"


        def source_url(path):
            if path.startswith("root://"):
                return path
            if path.startswith("/eos/"):
                return "root://eosuser.cern.ch/" + path
            return path


        def stage_input(dataset_name, job_id, original_path):
            source = source_url(original_path)
            dataset_cache = cache_dir / safe_name(dataset_name)
            dataset_cache.mkdir(parents=True, exist_ok=True)
            basename = Path(original_path.split("?", 1)[0]).name or "meta.root"
            source_id = hashlib.sha256(source.encode()).hexdigest()[:16]
            target = dataset_cache / f"job_{safe_name(job_id)}__{source_id}__{basename}"

            # Completed monitor entries are immutable outputs. Keeping successful
            # local copies means subsequent runs transfer only newly completed jobs.
            if target.is_file() and target.stat().st_size > 0:
                return target, False

            fd, temporary_name = tempfile.mkstemp(
                prefix=f".{target.name}.", suffix=".part", dir=dataset_cache
            )
            os.close(fd)
            temporary = Path(temporary_name)
            try:
                if source.startswith("root://"):
                    result = subprocess.run(
                        ["xrdcp", "-f", source, str(temporary)],
                        capture_output=True,
                        text=True,
                    )
                    if result.returncode:
                        detail = result.stderr.strip() or result.stdout.strip()
                        raise RuntimeError(
                            f"xrdcp exited with status {result.returncode}: {detail}"
                        )
                else:
                    shutil.copy2(source, temporary)
                if not temporary.is_file() or temporary.stat().st_size == 0:
                    raise RuntimeError(f"Copy produced an empty file: {source}")
                os.replace(temporary, target)
            except Exception:
                temporary.unlink(missing_ok=True)
                raise
            return target, True


        # Collect only outputs that the monitor has verified as complete. Copy new
        # files concurrently, then start hadd only after all staging has finished.
        inputs_by_dataset = {}
        failed_datasets = set()
        copy_counts = {"copied": 0, "reused": 0}
        copy_failures = []
        copy_tasks = []

        for dataset_name, dataset in sorted(state.get("datasets", {}).items()):
            for job_id, job in sorted(dataset.get("jobs", {}).items()):
                if job.get("status") != "done":
                    continue
                remote_path = job.get("meta")
                if not remote_path:
                    copy_failures.append(f"{dataset_name} job {job_id}: missing meta path")
                    failed_datasets.add(dataset_name)
                    continue
                copy_tasks.append((dataset_name, job_id, remote_path))

        with ThreadPoolExecutor(max_workers=xrdcp_jobs) as executor:
            futures = {
                executor.submit(stage_input, dataset_name, job_id, remote_path): (
                    dataset_name,
                    job_id,
                    remote_path,
                )
                for dataset_name, job_id, remote_path in copy_tasks
            }
            for future in as_completed(futures):
                dataset_name, job_id, remote_path = futures[future]
                try:
                    local_path, copied = future.result()
                    inputs_by_dataset.setdefault(dataset_name, []).append(str(local_path))
                    copy_counts["copied" if copied else "reused"] += 1
                except Exception as exc:
                    copy_failures.append(
                        f"{dataset_name} job {job_id} ({remote_path}): {exc}"
                    )
                    failed_datasets.add(dataset_name)

        for dataset_name in failed_datasets:
            inputs_by_dataset.pop(dataset_name, None)
            print(f"Skipping {dataset_name}: at least one completed output failed to copy.")
        for dataset_inputs in inputs_by_dataset.values():
            dataset_inputs.sort()

        print(
            f"Staging complete: {copy_counts['copied']} new file(s) copied, "
            f"{copy_counts['reused']} cached file(s) reused; "
            f"{len(inputs_by_dataset)} dataset merge(s) ready "
            f"(up to {xrdcp_jobs} xrdcp job(s) at once)."
        )
        if nominal_only:
            print("Nominal-only mode: non-nominal slices will be hidden from the schema and extracted histograms.")

        # The shared merger validates definitions, retains provenance and publishes the
        # ROOT file with its immutable sidecar before writing the completion marker.
        metadata_merger = Path(metadata_merger).resolve() if metadata_merger is not None else repo_root / "core/python/metadata_io.py"
        if not metadata_merger.is_file():raise ValueError("Missing metadata merger entry point")


        def merge_dataset(dataset_name, inputs):
            from extract_nd_histograms import read_embedded_histogram_schema
            from metadata_io import metadata_artifact_paths
            output = merge_dir / f"{safe_name(dataset_name)}_partial.root"
            log_path = logs_dir / f"{safe_name(dataset_name)}.hadd.log"
            configuration = {"dataset": dataset_name, "nominal_only": nominal_only,
                "sparse_only": sparse_only,
                "semantic_config_compatibility": compatibility_policy,
                "metadata_merger_sha256": file_digest(metadata_merger),
                "jobs": {job: entry["meta"] for job, entry in
                         state["datasets"][dataset_name]["jobs"].items()
                         if entry.get("status") == "done"}}
            parents = {"monitor_state": file_digest(state_path)}
            snapshot_path = output.with_suffix(".snapshot.json")
            if snapshot_path.is_file():
                try:
                    previous = verify_artifact(snapshot_path)
                    expected_files = set(map(str, [*metadata_artifact_paths(output), *inputs]))
                    if (previous.get("kind") == "partial-merge" and
                            previous.get("configuration") == configuration and
                            previous.get("parents") == parents and
                            set(previous["files"]) == expected_files):
                        print(f"Reused verified merge {dataset_name}", flush=True)
                        return dataset_name, len(inputs), output, log_path
                except (OSError, ValueError, KeyError):
                    pass  # Rebuild normally; old outputs survive failed publication.
            command = [sys.executable, str(metadata_merger), str(output), *inputs,
                       "--max-open-files", str(hadd_max_open_files)]
            if semantic_config_compatibility is not None:
                command.extend(["--semantic-config-compatibility", str(semantic_config_compatibility)])
            if not sparse_only and read_embedded_histogram_schema(inputs[0]) is not None:
                command.append("--extract-histograms")
            if dataset_name.startswith("DATA_"):
                command.extend(["--jobs", str(data_merge_threads)])
            if nominal_only:
                command.append("--nominal-only")
            with log_path.open("w") as log_file:
                result = subprocess.run(command, check=False, stdout=log_file, stderr=subprocess.STDOUT)
            if result.returncode:
                tail = "\n".join(log_path.read_text(errors="replace").splitlines()[-25:])
                raise RuntimeError(f"metadata merge failed ({result.returncode}):\n{tail}")
            snapshot = derived_artifact(kind="partial-merge",
                parents=parents, configuration=configuration,
                files=[*metadata_artifact_paths(output), *inputs])
            write_json(output.with_suffix(".snapshot.json"), snapshot)
            return dataset_name, len(inputs), output, log_path


        merge_failures = []
        with ThreadPoolExecutor(max_workers=hadd_jobs) as executor:
            futures = {
                executor.submit(merge_dataset, dataset_name, inputs): dataset_name
                for dataset_name, inputs in inputs_by_dataset.items()
            }
            for future in as_completed(futures):
                dataset_name = futures[future]
                try:
                    name, count, output, log_path = future.result()
                    print(f"Merged {name}: {count} input(s) -> {output} (log: {log_path})")
                except Exception as exc:
                    merge_failures.append(f"{dataset_name}: {exc}")
                    log_path = logs_dir / f"{safe_name(dataset_name)}.hadd.log"
                    print(
                        f"Merge failed for {dataset_name}: {exc} (log: {log_path})",
                        file=sys.stderr,
                    )

        if copy_failures or merge_failures:
            for failure in copy_failures:
                print(f"Copy failed: {failure}", file=sys.stderr)
            raise SystemExit(
                f"Partial merge finished with {len(copy_failures)} copy failure(s) and "
                f"{len(merge_failures)} merge failure(s)."
            )
        if not inputs_by_dataset:
            raise SystemExit("No successfully staged completed job outputs were found.")

        print(f"Finished partial merges with at most {hadd_jobs} hadd job(s) at once.")
    finally:
        lock_file.close()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--hadd-jobs", type=int, default=4)
    parser.add_argument("--wait-for-lock", action="store_true", help="wait for an existing merger, then verify/reuse its outputs")
    parser.add_argument("--xrdcp-jobs", type=int, default=4)
    parser.add_argument("--data-merge-threads", type=int, default=1)
    parser.add_argument("--nominal-only", action="store_true")
    parser.add_argument("--sparse-only", action="store_true",
                        help="retain merged sparse objects without creating legacy TH1 copies")
    parser.add_argument("--semantic-config-compatibility", type=Path,
                        help="JSON policy allowing only reviewed sample/semantic-hash combinations")
    parser.add_argument("--metadata-merger",type=Path,help="Analysis-owned validated metadata merge entry point")
    args = parser.parse_args(argv)
    merge_completed_outputs(args.state, args.output, hadd_jobs=args.hadd_jobs,
        xrdcp_jobs=args.xrdcp_jobs, nominal_only=args.nominal_only,
        data_merge_threads=args.data_merge_threads, sparse_only=args.sparse_only,
        semantic_config_compatibility=args.semantic_config_compatibility, metadata_merger=args.metadata_merger, wait_for_lock=args.wait_for_lock)


if __name__ == "__main__":
    main()
