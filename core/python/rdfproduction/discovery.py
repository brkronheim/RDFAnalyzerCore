"""Materialize input file inventories for production partition planning."""

from concurrent.futures import ThreadPoolExecutor
import fnmatch
import json
import logging
from pathlib import Path
import subprocess

from dataset_manifest import DatasetManifest
from optional_extensions import load_extension_module
from run_bundle import write_json
from submission_backend import ensure_xrootd_redirector, read_config


_SOURCE_PREFIXES = {
    "rucio": "rucioFileList", "xrdfs": "xrdfsFileList", "opendata": "openDataFileList",
    "manifest": "manifestFileList",
}


def _require_path_component(value, label):
    if (not isinstance(value, str) or value in {"", ".", ".."} or Path(value).name != value
            or "\x00" in value):
        raise ValueError(f"invalid {label}: {value!r}")


def file_list_directory(workspace, name, file_source):
    """Return the canonical inventory directory for one source and run."""
    _require_path_component(name, "discovery run name")
    try:
        prefix = _SOURCE_PREFIXES[file_source]
    except KeyError:
        raise ValueError(f"unsupported file source: {file_source!r}") from None
    return Path(workspace) / f"{prefix}_{name}"


def _should_ignore_xrdfs_entry(path):
    return Path(path).name.startswith(".sys.")


def xrdfs_list_files(server, remote_path, pattern="*.root", recursive=True, timeout=60,
                     max_workers=8, *, strict=False):
    """List matching XRootD files with a parallel breadth-first traversal."""

    def list_directory(path):
        try:
            result = subprocess.run(["xrdfs", server, "ls", "-l", path], capture_output=True,
                                    text=True, timeout=timeout, check=True)
        except (OSError, subprocess.SubprocessError) as exc:
            detail = (getattr(exc, "stderr", None) or str(exc)).strip()
            message = f"xrdfs ls failed for {server} {path}: {detail}"
            if strict:
                raise RuntimeError(message) from exc
            logging.warning("%s", message)
            return [], []

        files, children = [], []
        for line in result.stdout.splitlines():
            parts = line.split()
            if not parts or not parts[-1].startswith("/") or _should_ignore_xrdfs_entry(parts[-1]):
                continue
            entry = parts[-1]
            if parts[0].startswith("d"):
                children.append(entry)
            elif fnmatch.fnmatch(Path(entry).name, pattern):
                files.append(entry)
        return files, children

    files, visited = set(), {remote_path}
    queue = [remote_path]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        while queue:
            next_queue = []
            for found, children in executor.map(list_directory, queue):
                files.update(found)
                if recursive:
                    for child in children:
                        if child not in visited:
                            visited.add(child)
                            next_queue.append(child)
            queue = next_queue
    return [f"{server.rstrip('/')}//{path.lstrip('/')}" for path in sorted(files)]


def xrdfs_files(entry, *, server="", pattern="*.root", recursive=True, strict=False):
    if entry.files:
        return [ensure_xrootd_redirector(uri.strip()) for uri in entry.files if uri.strip()]
    path = entry.extra.get("xrdfs_path")
    if not path:
        return []
    server = entry.extra.get("xrdfs_server") or server
    if not server:
        raise ValueError(f"{entry.name}: xrdfs_path requires an xrdfs_server")
    return xrdfs_list_files(server, path, pattern=pattern, recursive=recursive, strict=strict)


def _rucio_inventory(entry, manifest, files_per_job, group_size):
    if entry.files:
        return {"sample": entry.name, "files": list(dict.fromkeys(
            ensure_xrootd_redirector(uri.strip()) for uri in entry.files if uri.strip()
        ))}

    paths = list(dict.fromkeys(path.strip() for path in (entry.das or "").split(",") if path.strip()))
    if not paths:
        return {"sample": entry.name, "files": []}
    provider = load_extension_module("cms", "rucio_discovery", required=True)
    client = provider.get_rucio_client()

    def find(path):
        return provider.query_rucio(path, group_size, manifest.whitelist, manifest.blacklist,
                                    entry.site or "", client, max_files_per_group=files_per_job,
                                    raise_on_error=True)

    if len(paths) == 1:
        results = [find(paths[0])]
    else:
        with ThreadPoolExecutor(max_workers=min(len(paths), 8)) as executor:
            results = list(executor.map(find, paths))

    files, groups, redirectors = [], [], {}
    seen = set()
    for result in results:
        for _, group in sorted(result["groups"].items()):
            urls = []
            for uri in group.split(","):
                uri = ensure_xrootd_redirector(uri.strip())
                if uri and uri not in seen:
                    urls.append(uri)
                    seen.add(uri)
            if urls:
                files.extend(urls)
                groups.append(",".join(urls))
        redirectors.update(result["site_redirectors"])
    return {"sample": entry.name, "files": files, "groups": groups, "site_redirectors": redirectors}


def _opendata_inventories(entries, requested_names, submit_config):
    from opendata_discovery import parse_opendata_config, process_metadata

    catalogue = {}
    if submit_config and any(not entry.files for entry in entries if entry.name in requested_names):
        configured = read_config(str(submit_config)).get("sampleConfig")
        if configured:
            config_dir = Path(submit_config).resolve().parent
            path = Path(configured)
            candidates = (config_dir / path, path, config_dir.parent / path)
            catalogue, _, _ = parse_opendata_config(str(next(
                (candidate for candidate in candidates if candidate.is_file()), candidates[0]
            )))

    records = {}
    files_by_sample = {entry.name: [] for entry in entries if entry.name in requested_names}
    for entry in entries:
        source = catalogue.get(entry.name, entry)
        files = entry.files or source.files
        if files:
            if entry.name in requested_names:
                files_by_sample[entry.name].extend(ensure_xrootd_redirector(uri.strip())
                                                   for uri in files if uri.strip())
            continue
        recids = list(dict.fromkeys(value.strip() for value in (source.das or entry.das or "").split(",")
                                   if value.strip()))
        key = entry.extra.get("opendata_key", source.extra.get("opendata_key"))
        if key is not None and (not isinstance(key, str) or not key.strip()):
            raise ValueError(f"{entry.name}: opendata_key must be a nonempty string")
        for recid in recids:
            records.setdefault(recid, {})[entry.name] = key

    for recid, samples in records.items():
        if requested_names.isdisjoint(samples):
            continue
        keys = {
            key or sample: sample for sample, key in samples.items()
            if key is not None or len(samples) > 1
        }
        if keys:
            if len(keys) != len(samples):
                raise ValueError(f"duplicate Open Data sample key in record {recid}")
            found = process_metadata(recid, keys)
        else:
            sample, = samples
            found = {sample: [uri for files in process_metadata(recid).values() for uri in files]}
        for sample in requested_names & found.keys():
            files_by_sample[sample].extend(found[sample])
    for sample, files in files_by_sample.items():
        yield {"sample": sample, "files": list(dict.fromkeys(files))}


def discover_inputs(workspace, name, file_source, dataset_manifest, *, submit_config="",
                    file_source_name="", files_per_job=50, rucio_group_size_gb=10.0,
                    xrdfs_server="", xrdfs_pattern="*.root", xrdfs_recursive=True, workers=4):
    """Publish per-dataset inventories, or use a named inventory from an earlier run.

    The controller owns the ordinary run's inventory directory. Named external
    inventories are read-only, including when they contain extra datasets.
    """
    directory = file_list_directory(workspace, file_source_name or name, file_source)
    manifest = DatasetManifest.load(dataset_manifest)
    entries = {}
    for entry in manifest.datasets:
        _require_path_component(entry.name, "dataset name")
        if entry.name in entries:
            raise ValueError(f"duplicate discovery dataset: {entry.name}")
        entries[entry.name] = entry

    saved = {}
    for path in directory.glob("*.json"):
        if path.name.endswith(".perf.json"):
            continue
        payload = json.loads(path.read_text())
        if not isinstance(payload, dict) or not isinstance(payload.get("sample", path.stem), str):
            raise ValueError(f"invalid discovery inventory in {path}")
        sample = payload.get("sample", path.stem)
        if sample not in entries:
            continue
        if sample in saved:
            raise ValueError(f"duplicate discovery inventory for {sample} in {directory}")
        if not payload.get("files") and not payload.get("groups"):
            raise ValueError(f"empty discovery inventory for {sample} in {directory}")
        if file_source == "manifest" and (payload.get("files") != list(entries[sample].files or [])
                                          or payload.get("groups")):
            raise ValueError(f"explicit inventory for {sample} differs from its dataset manifest: {path}")
        saved[sample] = path

    missing = [entry for sample, entry in entries.items() if sample not in saved]
    if not missing:
        directory.mkdir(parents=True, exist_ok=True)
        return directory
    if file_source_name and file_source_name != name:
        raise ValueError(f"named discovery {directory} is missing dataset(s): "
                         + ", ".join(entry.name for entry in missing))

    def resolve(entry):
        if file_source == "manifest":
            return {"sample": entry.name, "files": list(entry.files or [])}
        if file_source == "rucio":
            return _rucio_inventory(entry, manifest, files_per_job, rucio_group_size_gb)
        return {"sample": entry.name, "files": xrdfs_files(
            entry, server=xrdfs_server, pattern=xrdfs_pattern, recursive=xrdfs_recursive, strict=True
        )}

    if file_source == "opendata":
        inventories = list(_opendata_inventories(entries.values(), {entry.name for entry in missing}, submit_config))
    else:
        with ThreadPoolExecutor(max_workers=min(workers, len(missing))) as executor:
            inventories = list(executor.map(resolve, missing))
    unresolved = [inventory["sample"] for inventory in inventories if not inventory["files"]]
    if unresolved:
        raise ValueError(f"{file_source} discovery returned no files for: " + ", ".join(unresolved))
    for inventory in inventories:
        write_json(directory / f"{inventory['sample']}.json", inventory)
        print(f"{file_source} discovery: {inventory['sample']} → {len(inventory['files'])} file(s)")
    return directory
