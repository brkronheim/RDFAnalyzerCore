"""Publish and load verified completion for a sealed skim production."""
from __future__ import annotations

from native_config_io import job_config_path
from collections import defaultdict
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from job_sizing import EntryCache, file_key
from run_bundle import content_digest, record_execution
from native_config_io import parse_config_bytes


SCHEMA = "rdf-verified-outputs-v1"
REMOTE_PREFIXES = ("root://", "/eos/", "/store/")
_SPLIT_SUFFIX = re.compile(r"_part[01]$")


def _read_config(path):
    data = path.read_bytes()
    return parse_config_bytes(data, path), hashlib.sha256(data).hexdigest()


def _partition(config):
    files = [file_key(value.strip()) for value in
             config.get("__orig_fileList", config.get("fileList", "")).split(",")
             if value.strip()]
    first = int(config.get("firstEntry") or 0)
    last = int(config.get("lastEntry") or 0)
    return {"files": files, "tree": config.get("treeList") or "Events",
            "range": [first, last] if first or last else None}


def _output_path(config, role):
    key = {"save": "saveFile", "meta": "metaFile"}[role]
    return config.get("__orig_" + key, config.get(key, ""))


def _output_lineage(path, originals):
    """The worker appends one suffix per split, even when the job slot changes."""
    stem, extension = os.path.splitext(path)
    suffixes = []
    while True:
        if stem + extension in originals:
            return stem + extension, tuple(reversed(suffixes))
        match = _SPLIT_SUFFIX.search(stem)
        if not match:
            raise ValueError(f"Output is outside the prepared partition plan: {path}")
        suffixes.append(match.group())
        stem = stem[:match.start()]


def _require_split_tree(leaf_paths, label):
    branches = {path[:size] for path in leaf_paths for size in range(len(path))}
    if branches & leaf_paths:
        raise ValueError(f"An accepted terminal job is also a split parent for {label}")
    nodes = leaf_paths | branches
    if any(parent + (part,) not in nodes for parent in branches for part in ("_part0", "_part1")):
        raise ValueError(f"Split lineage is missing a sibling partition for {label}")


def _validate_partition(partition):
    files = partition["files"]
    if not files or any(not isinstance(file, str) or not file for file in files):
        raise ValueError("A partition must identify its input files")
    if len(set(files)) != len(files) or not partition["tree"]:
        raise ValueError("Duplicated files or missing tree in a partition")
    bounds = partition["range"]
    if bounds is not None and (
        len(files) != 1 or len(bounds) != 2
        or any(type(value) is not int for value in bounds)
        or not 0 <= bounds[0] < bounds[1]
    ):
        raise ValueError("A bounded partition needs one file and an increasing entry interval")


def _require_coverage(original, leaves, label):
    expected = original["partition"]
    _validate_partition(expected)
    positions = {file: index for index, file in enumerate(expected["files"])}
    seen = defaultdict(list)
    for leaf in leaves:
        _validate_partition(leaf)
        if leaf["tree"] != expected["tree"] or set(leaf["files"]) - positions.keys():
            raise ValueError(f"Unexpected input file or tree in terminal partition for {label}")
        order = [positions[file] for file in leaf["files"]]
        if order != sorted(order):
            raise ValueError(f"Changed input order in terminal partition for {label}")
        for file in leaf["files"]:
            seen[file].append(leaf["range"])
    if seen.keys() != positions.keys():
        raise ValueError(f"Incomplete input coverage for {label}")
    for file, parts in seen.items():
        interval = expected["range"]
        if interval is None and parts == [None]:
            continue
        if None in parts:
            raise ValueError(f"Overlapping or unbounded terminal partition for {label}")
        if interval is None:
            entries = original["file_entries"].get(file)
            if type(entries) is not int or entries <= 0:
                raise ValueError(f"Original entry count unavailable for split input {label}")
            interval = (0, entries)
        cursor, end = interval
        for first, last in sorted(parts):
            if first != cursor:
                raise ValueError(f"Gap or overlap in terminal entry ranges for {label}")
            cursor = last
        if cursor != end:
            raise ValueError(f"Incomplete terminal entry range for {label}")


def _check_execution(record):
    unsigned = {key: value for key, value in record.items() if key != "execution_id"}
    if record.get("schema") != "rdf-execution-v1" or record.get("execution_id") != content_digest(unsigned):
        raise ValueError("Invalid sealed execution identity")


def _require_subpath(path, parent):
    path = Path(path).resolve()
    if not path.is_relative_to(parent) or path == parent:
        raise ValueError(f"Run artifact is outside its expected directory: {path}")
    return path


def _originals(run_dir, plan, prepared, submitted):
    if not prepared or len(prepared) != len(plan["partitions"]):
        raise ValueError("Prepared jobs differ from the sealed execution plan")
    if not isinstance(submitted["datasets"], list) or not submitted["datasets"]:
        raise ValueError("No submitted datasets")
    by_dataset = {}
    seen = set()
    entry_cache = EntryCache(run_dir / "input_metadata.json")
    for record in submitted["datasets"]:
        dataset = record["dataset_name"]
        jobs = record["jobs"]
        if (dataset in by_dataset or not jobs or len(jobs) != record["job_count"]
                or not str(record["cluster_id"]).isdigit()):
            raise ValueError(f"Invalid submitted dataset {dataset}")
        submission = _require_subpath(record["submission_dir"], run_dir / "condor_submissions")
        originals = {}
        for local_index, job in enumerate(jobs):
            index = job["global_index"]
            if (type(index) is not int or not 0 <= index < len(prepared) or index in seen
                    or job["job_index"] != local_index):
                raise ValueError(f"Invalid or repeated submitted partition for {dataset}")
            original = prepared[index]
            source_dir = _require_subpath(original["job_dir"], run_dir / "jobs")
            source_config = job_config_path(source_dir)
            if (Path(original["config_path"]).resolve() != source_config or original["dataset_name"] != dataset
                    or Path(job["source_job_dir"]).resolve() != source_dir):
                raise ValueError(f"Submitted partition {dataset}/{index} differs from preparation")
            config, source_digest = _read_config(source_config)
            if plan["partitions"][index] != {"dataset": dataset, "config_sha256": source_digest}:
                raise ValueError(f"Prepared configuration changed for {dataset}/{index}")
            staged_dir = submission / f"job_{local_index}"
            if Path(job["job_dir"]).resolve() != staged_dir:
                raise ValueError(f"Unexpected submitted directory for {dataset}/{index}")
            staged, _ = _read_config(job_config_path(staged_dir))
            partition = _partition(config)
            if _partition(staged) != partition:
                raise ValueError(f"Staged partition differs from sealed inputs for {dataset}/{index}")
            require_skim = config.get("enableSkim", "").lower() in {"1", "true", "yes", "on"}
            roles = ("save", "meta") if require_skim else ("meta",)
            paths = {role: _output_path(staged, role) for role in roles}
            meta = paths["meta"]
            if meta in originals or not all(paths.values()):
                raise ValueError(f"Missing or repeated expected output path for {dataset}/{index}")
            _validate_partition(partition)
            counts = {}
            if partition["range"] is None:
                count = int(config.get("plannedInputEntries") or 0)
                if len(partition["files"]) == 1 and count > 0:
                    counts[partition["files"][0]] = count
                else:
                    # A bounded retry can descend from a whole-file group.
                    # Its source size must be in the tree-scoped planning cache.
                    for file in partition["files"]:
                        try:
                            count = entry_cache.get(file, partition["tree"])
                        except OSError:
                            count = None
                        if count is not None:
                            counts[file] = count
            originals[meta] = {"global_index": index, "partition": partition, "file_entries": counts,
                               "outputs": paths}
            seen.add(index)
        by_dataset[dataset] = (submission, originals)
    if len(seen) != len(prepared):
        raise ValueError("Some original partitions were never submitted")
    return by_dataset


def _collect_terminal_outputs(state, originals, receipts, execution_sha, manifest_sha, *, allow_partial=False):
    if state["datasets"].keys() != originals.keys():
        raise ValueError("Monitor dataset membership differs from the submitted plan")
    datasets = {}
    for dataset, (submission, expected) in originals.items():
        state_dataset = state["datasets"][dataset]
        if Path(state_dataset["submission_dir"]).resolve() != submission:
            raise ValueError(f"Monitor references a different submission for {dataset}")
        terminal, failures = {}, {}
        for slot, job in state_dataset["jobs"].items():
            failed = allow_partial and job["status"] == "perm_fail"
            if not slot.isdigit() or (job["status"] != "done" and not failed):
                raise ValueError(f"Partition {dataset}/{slot} is incomplete")
            terminal_dir = _require_subpath(job["job_dir"], submission)
            config, config_sha = _read_config(job_config_path(terminal_dir))
            if job["tree_name"] != (config.get("saveTree") or "Events"):
                raise ValueError(f"Staged output tree changed for {dataset}/{slot}")
            meta = job["meta"]
            root_meta, lineage = _output_lineage(meta, expected)
            original = expected[root_meta]
            suffix = "".join(lineage)
            paths = {role: job[role] for role in original["outputs"]}
            for role, path in paths.items():
                stem, ext = os.path.splitext(original["outputs"][role])
                if path != _output_path(config, role) or path != stem + suffix + ext:
                    raise ValueError(f"Unexpected {role} output for {dataset}/{slot}")
            entry = {"original": root_meta, "partition": _partition(config), "outputs": paths,
                     "attempt": [job["cluster"], job["proc"], job.get("retries", 0)],
                     "config_sha256": config_sha}
            if failed:
                failures[slot] = entry
                continue
            receipt = receipts.get(json.dumps([dataset, slot]), {})
            contract = receipt.get("contract", {})
            evidence = dict(version=1, dataset=dataset, config_sha256=config_sha,
                            manifest_sha256=manifest_sha, execution_sha256=execution_sha,
                            cluster=job["cluster"], proc=job["proc"], retries=job.get("retries", 0),
                            tree_name=job["tree_name"], roles=list(paths))
            signatures = receipt.get("files", {})
            if (any(contract.get(key) != value for key, value in evidence.items())
                    or signatures.keys() != set(paths.values())
                    or any(not signature for signature in signatures.values())):
                raise ValueError(f"Missing current integrity receipt for {dataset}/{slot}")
            terminal[slot] = {**entry, "signatures": signatures}
        datasets[dataset] = {"originals": expected, "jobs": terminal}
        if failures:
            datasets[dataset]["failures"] = failures
    return datasets


def _check_index(record):
    if record.get("schema") != SCHEMA:
        raise ValueError("Unsupported verified output index schema")
    unsigned = {key: value for key, value in record.items() if key != "index_id"}
    if record.get("index_id") != content_digest(unsigned):
        raise ValueError("Invalid verified output index digest")
    datasets = record["datasets"]
    partition_count = record["partition_count"]
    if record.get("validation") != "integrity" or type(partition_count) is not int or partition_count <= 0:
        raise ValueError("Invalid verified output index completion contract")
    if not datasets:
        raise ValueError("Verified output index has no datasets")
    seen = set()
    indices = set()
    for dataset, configuration in datasets.items():
        originals, jobs = configuration["originals"], configuration["jobs"]
        failures = configuration.get("failures", {})
        if not originals or not jobs:
            raise ValueError(f"Verified output index has no successful partitions for {dataset}")
        if jobs.keys() & failures.keys():
            raise ValueError(f"Repeated successful and failed partition for {dataset}")
        leaves = defaultdict(list)
        lineages = defaultdict(set)
        for slot, job in {**jobs, **failures}.items():
            outputs = job["outputs"]
            signatures = job.get("signatures", {})
            paths = set(outputs.values())
            if (not slot.isdigit() or job["original"] not in originals
                    or set(outputs) not in ({"meta"}, {"meta", "save"})
                    or len(paths) != len(outputs) or paths & seen
                    or signatures.keys() != (paths if slot in jobs else set())):
                raise ValueError(f"Invalid or repeated output for {dataset}/{slot}")
            original = originals[job["original"]]
            original_path, lineage = _output_lineage(outputs["meta"], originals)
            if original_path != job["original"]:
                raise ValueError(f"Output lineage changed for {dataset}/{slot}")
            suffix = "".join(lineage)
            if outputs.keys() != original["outputs"].keys():
                raise ValueError(f"Output roles changed for {dataset}/{slot}")
            for role, path in original["outputs"].items():
                stem, ext = os.path.splitext(path)
                if outputs[role] != stem + suffix + ext:
                    raise ValueError(f"Output lineage changed for {dataset}/{slot}")
            for path, signature in signatures.items():
                if (not isinstance(path, str) or not path.startswith(("root://", "/"))
                        or type(signature.get("size")) is not int or signature["size"] <= 0):
                    raise ValueError(f"Output lacks a trustworthy signature: {path}")
            seen.update(paths)
            leaves[job["original"]].append(job["partition"])
            lineages[job["original"]].add(lineage)
        for path, original in originals.items():
            index = original["global_index"]
            if type(index) is not int or index in indices or not 0 <= index < partition_count:
                raise ValueError(f"Invalid or repeated original partition for {dataset}/{path}")
            indices.add(index)
            _require_coverage(original, leaves[path], f"{dataset}/{path}")
            _require_split_tree(lineages[path], f"{dataset}/{path}")
    if len(indices) != partition_count:
        raise ValueError("Verified output index omits original partitions")


def _execution_plan(run_dir):
    identity_bytes = (run_dir / "execution_identity.json").read_bytes()
    identity = json.loads(identity_bytes)
    plan = json.loads((run_dir / "execution_plan.json").read_bytes())
    _check_execution(identity)
    _check_execution(plan)
    keys = ("bundle_id", "inputs", "options", "runtime")
    if any(identity[key] != plan[key] for key in keys):
        raise ValueError("Execution plan does not belong to this run identity")
    return identity_bytes, identity, plan


def _publish_index(destination, identity, plan, datasets, **evidence):
    unsigned = {"schema": SCHEMA, "execution_id": identity["execution_id"],
                "plan_id": plan["execution_id"], "bundle_id": plan["bundle_id"],
                "partition_count": len(plan["partitions"]),
                "validation": "integrity", "datasets": datasets, **evidence}
    record = dict(unsigned, index_id=content_digest(unsigned))
    _check_index(record)
    record_execution(destination, record)
    return record


def write_verified_output_index(run_dir, destination, *, validate, allow_partial=False):
    """Publish exact verified coverage from Condor integrity receipts under the run lock."""
    run_dir = Path(run_dir).resolve()
    state_path = run_dir / "monitor_state.json"
    state_bytes = state_path.read_bytes()
    identity_bytes, identity, plan = _execution_plan(run_dir)
    if not validate():
        raise ValueError("Run outputs are incomplete or failed integrity validation")
    receipts = json.loads((run_dir / "output_validation.json").read_bytes())
    if receipts.get("schema_version") != 1:
        raise ValueError("Unsupported output integrity receipt schema")
    originals = _originals(run_dir, plan,
                           json.loads((run_dir / "prep_submission.json").read_bytes()),
                           json.loads((run_dir / "submitted.json").read_bytes()))
    datasets = _collect_terminal_outputs(json.loads(state_bytes), originals, receipts["jobs"],
                                         hashlib.sha256(identity_bytes).hexdigest(),
                                         identity["inputs"]["manifest_sha256"], allow_partial=allow_partial)
    if state_path.read_bytes() != state_bytes:
        raise ValueError("Monitor state changed during validation")
    return _publish_index(destination, identity, plan, datasets,
                          monitor_state_sha256=hashlib.sha256(state_bytes).hexdigest())


def write_local_output_index(run_dir, destination, *, validate, backend="local"):
    """Publish sealed partitions on the shared filesystem after ROOT checks pass."""
    run_dir = Path(run_dir).resolve()
    _, identity, plan = _execution_plan(run_dir)
    manifest = run_dir / "prep_submission.json"
    prepared_bytes = manifest.read_bytes()
    prepared = json.loads(prepared_bytes)
    if not isinstance(prepared, list) or not prepared or len(prepared) != len(plan["partitions"]):
        raise ValueError("Prepared local jobs differ from the sealed execution plan")
    datasets = {}
    for index, job in enumerate(prepared):
        dataset = job["dataset_name"]
        job_dir = _require_subpath(job["job_dir"], run_dir / "jobs")
        config_path = job_config_path(job_dir)
        out_dir = _require_subpath(job["out_dir"], run_dir / "outputs")
        config, digest = _read_config(config_path)
        if (Path(job["config_path"]).resolve() != config_path
                or plan["partitions"][index] != {"dataset": dataset, "config_sha256": digest}):
            raise ValueError(f"Prepared local configuration changed for {dataset}/{index}")
        paths = {"meta": str(out_dir / "meta.root")}
        if config.get("enableSkim", "").lower() in {"1", "true", "yes", "on"}:
            paths["save"] = str(out_dir / "skim.root")
        if any(_output_path(config, role) != path for role, path in paths.items()):
            raise ValueError(f"Unexpected local output path for {dataset}/{index}")
        for path in paths.values():
            _require_subpath(path, out_dir)
        signatures = {path: local_output_signature(path) for path in paths.values()}
        if (not all(signatures.values()) or not validate(index)
                or any(local_output_signature(path) != signature for path, signature in signatures.items())):
            raise ValueError(f"Local outputs are incomplete or changed during validation for {dataset}/{index}")
        partition = _partition(config)
        meta = paths["meta"]
        group = datasets.setdefault(dataset, {"originals": {}, "jobs": {}})
        if meta in group["originals"]:
            raise ValueError(f"Repeated local output for {dataset}/{index}")
        group["originals"][meta] = {"global_index": index, "partition": partition,
                                    "file_entries": {}, "outputs": paths}
        group["jobs"][str(index)] = {"original": meta, "partition": partition, "outputs": paths,
                                     "signatures": signatures, "config_sha256": digest}
    if manifest.read_bytes() != prepared_bytes:
        raise ValueError("Local preparation changed during validation")
    return _publish_index(destination, identity, plan, datasets, backend=backend,
                          prepared_sha256=hashlib.sha256(prepared_bytes).hexdigest())


def load_verified_output_index(path):
    try:
        record = json.loads(Path(path).read_bytes())
        _check_index(record)
    except (KeyError, AttributeError, TypeError) as error:
        raise ValueError(f"Malformed verified output index {path}: {error}") from error
    return record


def local_output_signature(path):
    try:
        info = Path(path).stat()
    except OSError:
        return None
    if not stat.S_ISREG(info.st_mode):
        return None
    return dict(size=info.st_size, mtime_ns=info.st_mtime_ns, ctime_ns=info.st_ctime_ns,
                inode=info.st_ino, device=info.st_dev)


def verify_output_signatures(record, check_signatures=None):
    """Check the published fingerprints before consumption or cache reuse."""
    expected = {path: signature for dataset in record["datasets"].values()
                for job in dataset["jobs"].values() for path, signature in job["signatures"].items()}
    if check_signatures is None:
        if any(path.startswith(REMOTE_PREFIXES) for path in expected):
            raise ValueError("A storage signature checker is required for XRootD outputs")
        actual = {path: local_output_signature(path) for path in expected}
    else:
        actual = check_signatures(list(expected))
    for path, fingerprint in expected.items():
        if actual.get(path) != fingerprint:
            raise ValueError(f"Verified output changed or is unavailable: {path}")
