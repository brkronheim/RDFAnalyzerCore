"""Load and validate sealed production specifications without executing jobs."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re

from run_bundle import content_digest, file_digest, find_bundle, verify_bundle


_FILES = {"exe", "submit_config", "dataset_manifest"}
_PATH_OPTIONS = {"root_setup", "python_env", "partition_profile", "input_metadata_cache", "x509",
                 "stitch_weights"}
_INT_OPTIONS = {"files_per_job", "entries_per_job", "prepare_workers", "request_memory",
                "max_runtime", "sizing_target_seconds", "poll_interval",
                "sizing_pilot_target_seconds", "sizing_pilot_parallel", "sizing_pilot_prefix_entries",
                "dask_workers", "dask_threads_per_worker"}
_BOOL_OPTIONS = {"make_test_job", "stage_in", "sizing_pilots", "xrdfs_recursive"}
_CHOICES = {
    "file_source": {"", "rucio", "xrdfs", "opendata", "manifest"},
    "partition": {"auto", "file_group", "file", "entry_range", "events"},
    "entry_range_mode": {"filter", "source"},
    "stitch_mode": {"none", "correctionlib"},
}
_TEXT_OPTIONS = {"container_setup", "want_os", "xrdfs_server", "xrdfs_pattern"}


def _json_object(path):
    data = json.loads(Path(path).read_text())
    if not isinstance(data, dict):
        raise ValueError(f"expected a JSON object in {path}")
    return data


def load_spec_document(path):
    """Load an authored YAML or existing JSON spec, preserving scalar types."""
    path = Path(path)
    if path.suffix.lower() in (".yaml", ".yml"):
        from native_config_io import load_yaml_document
        document = load_yaml_document(path, typed=True)
        if not isinstance(document, dict):
            raise ValueError(f"expected a production specification mapping in {path}")
        return document
    return _json_object(path)


def _positive_integer(value, name):
    if type(value) is not int or value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _text(value, name):
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{name} must be a non-empty string")
    return value


def validate_run_name(name):
    """Require one safe name for a run or discovery inventory."""
    name = _text(name, "name")
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", name):
        raise ValueError("name must start with a letter or digit and contain only letters, digits, _, . or -")
    return name


def _local_path(value, name, base, *, check=True):
    path = (base / _text(value, name)).resolve()
    if check and not path.is_file():
        raise ValueError(f"{name} does not name an existing file: {path}")
    return str(path)


def normalize_spec_paths(document, base):
    """Make a spec portable between the campaign directory and generated specs."""
    base = Path(base).resolve()
    if not isinstance(document, dict):
        raise ValueError("production spec must be an object")
    result = dict(document)
    for key in (_FILES | {"state_root"}) & result.keys():
        result[key] = _local_path(result[key], key, base, check=False)
    sections = (
        ("options", _PATH_OPTIONS, ()),
        ("build", {"source_directory", "build_directory"}, ("inputs",)),
        ("python", {"entrypoint"}, ("modules",)),
    )
    for section, paths, mappings in sections:
        if section not in result:
            continue
        value = result[section]
        if not isinstance(value, dict):
            raise ValueError(f"{section} must be an object")
        value = dict(value)
        for key in paths & value.keys():
            if key != "partition_profile" or value[key] != "none":
                value[key] = _local_path(value[key], key, base, check=False)
        for key in mappings:
            if key in value:
                if not isinstance(value[key], dict):
                    raise ValueError(f"{section}.{key} must be an object")
                value[key] = {label: _local_path(path, f"{section}.{key}.{label}", base, check=False)
                              for label, path in value[key].items()}
        result[section] = value
    return result


def _options(raw, base):
    if not isinstance(raw, dict):
        raise ValueError("options must be a JSON object")
    options = {"sizing_pilots": False, "output_validation": "integrity"}
    for key, value in raw.items():
        if key in _PATH_OPTIONS:
            if key == "partition_profile" and value == "none":
                options[key] = value
            else:
                options[key] = _local_path(value, key, base, check=key != "x509")
        elif key in _INT_OPTIONS:
            options[key] = _positive_integer(value, key)
        elif key in _BOOL_OPTIONS and type(value) is bool:
            options[key] = value
        elif key in _CHOICES and isinstance(value, str) and value in _CHOICES[key]:
            options[key] = value
        elif key == "output_directory":
            output = _text(value, key)
            if not output.startswith(("/eos/", "root://")):
                raise ValueError("output_directory must be an EOS path or root:// URL")
            options[key] = output
        elif key == "file_source_name":
            options[key] = _text(value, key)
            validate_run_name(options[key])
        elif key in _TEXT_OPTIONS:
            options[key] = _text(value, key)
        elif key == "dask_scheduler" and isinstance(value, str) and "\x00" not in value:
            options[key] = value.strip()
        elif key == "rucio_group_size_gb" and type(value) in (int, float) and 0 < value < float("inf"):
            options[key] = value
        elif key == "sizing_pilot_safety_factor" and type(value) in (int, float) and 1 <= value < float("inf"):
            options[key] = value
        elif key == "max_retries" and type(value) is int and value >= 0:
            options[key] = value
        else:
            raise ValueError(f"unsupported option or invalid value: {key}")
    if options["sizing_pilots"] and (
        not options.get("file_source") or options.get("partition", "auto") not in {"auto", "events", "entry_range"}
    ):
        raise ValueError("sizing_pilots requires a file_source and partition auto, events, or entry_range")
    if "file_source_name" in options and options.get("file_source") not in {"rucio", "xrdfs", "opendata"}:
        raise ValueError("file_source_name requires a Rucio, XRootD, or Open Data source")
    if options.get("file_source") != "xrdfs" and options.keys() & {"xrdfs_server", "xrdfs_pattern", "xrdfs_recursive"}:
        raise ValueError("xrdfs options require file_source: xrdfs")
    return options


def inspect_spec(path):
    """Resolve a batch specification and verify its local inputs."""
    path = Path(path).resolve()
    data = normalize_spec_paths(load_spec_document(path), path.parent)
    required = {"submit_config", "dataset_manifest", "name", "version"}
    missing = required - data.keys()
    unexpected = data.keys() - required - {"options", "workers", "exe", "build", "python", "backend", "state_root"}
    if missing or unexpected or type(data.get("version")) is not int or data["version"] != 1:
        raise ValueError(f"expected version 1 batch spec (missing: {sorted(missing)}, unknown: {sorted(unexpected)})")
    name = data["name"]
    validate_run_name(name)
    if ("exe" in data) == ("python" in data) or ("python" in data and "build" in data):
        raise ValueError("specify either exe with an optional build, or a Python entrypoint")
    paths = {key: _local_path(data[key], key, path.parent)
             for key in ("submit_config", "dataset_manifest")}
    bundle = find_bundle(paths["submit_config"])
    if bundle is None:
        raise ValueError("submit_config and dataset_manifest must belong to a sealed run bundle")
    sealed = verify_bundle(bundle)
    for key in ("submit_config", "dataset_manifest"):
        member = Path(paths[key])
        if not member.is_relative_to(bundle) or str(member.relative_to(bundle)) not in sealed["files"]:
            raise ValueError(f"{key} is outside the declared bundle contents: {member}")

    options = _options(data.get("options", {}), path.parent)
    backend = data.get("backend", "htcondor")
    if backend not in ("htcondor", "local", "dask"):
        raise ValueError("backend must be htcondor, local, or dask")
    if backend != "htcondor" and (options["sizing_pilots"] or "output_directory" in options):
        raise ValueError(f"{backend} production uses the run's outputs directory and cannot use Condor sizing pilots")
    if backend != "dask" and options.keys() & {"dask_scheduler", "dask_workers", "dask_threads_per_worker"}:
        raise ValueError("dask options require backend: dask")
    hashes = {}
    runtime = {}
    if "exe" in data:
        paths["exe"] = _local_path(data["exe"], "exe", path.parent, check="build" not in data)
        if "build" in data:
            from .build import inspect_build
            runtime["build"] = inspect_build(data["build"], base=path.parent, executable=paths["exe"])
        else:
            if not os.access(paths["exe"], os.X_OK):
                raise ValueError(f"executable is not runnable: {paths['exe']}")
            hashes["exe"] = file_digest(paths["exe"])
    else:
        from .python_runtime import inspect_python
        runtime["python"] = inspect_python(data["python"], path.parent)
    for key in sorted((_PATH_OPTIONS - {"x509"}) & options.keys()):
        if options[key] != "none":
            hashes[key] = file_digest(options[key])
    record = {
        "schema": "rdf-production-spec-v1",
        "bundle_id": sealed["bundle_id"],
        "task_parameters": {"name": name, **paths, **options},
        "workers": _positive_integer(data.get("workers", 1), "workers"),
        "file_hashes": hashes,
        **runtime,
    }
    if "state_root" in data:
        state_root = Path(data["state_root"])
        if state_root.exists() and not state_root.is_dir():
            raise ValueError(f"state_root must name a directory: {state_root}")
        record["state_root"] = str(state_root)
    if backend != "htcondor":
        record["backend"] = backend
    return {**record, "spec_id": content_digest(record)}
