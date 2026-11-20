"""Content-addressed analysis bundles and execution/derived-artifact identities.

No scheduler, ROOT or experiment dependency is required. Analysis compilers own
their configuration grammar; this module owns publication and integrity.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import shutil
import tempfile

MANIFEST = "bundle_manifest.json"
SCHEMA = "rdf-run-bundle-v1"


class BundleError(ValueError):
    pass


def file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def content_digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def write_json(path, value):
    """Replace a JSON record atomically, never exposing a partial write."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


@contextmanager
def atomic_directory(destination):
    """Publish a new directory after successful construction; refuse reuse.

    A sibling lock also prevents concurrent cooperating publishers from
    replacing one another. Runtime state belongs outside this directory.
    """
    destination = Path(destination).resolve()
    destination.parent.mkdir(parents=True, exist_ok=True)
    lock = destination.with_name(f".{destination.name}.publish.lock")
    try:
        lock.mkdir()
    except FileExistsError as exc:
        raise BundleError(f"bundle publication already in progress: {destination}") from exc
    staging = None
    try:
        if destination.exists():
            raise BundleError(f"refusing to overwrite existing bundle: {destination}")
        staging = Path(tempfile.mkdtemp(prefix=f".{destination.name}.", dir=destination.parent))
        yield staging
        if destination.exists():
            raise BundleError(f"refusing to overwrite existing bundle: {destination}")
        staging.rename(destination)
    finally:
        if staging is not None and staging.exists():
            shutil.rmtree(staging)
        lock.rmdir()


def snapshot_assets(root, sources):
    """Copy declared files under unique basenames; return source -> relative path.

    Analyses supply the dependency graph, including nested payloads. Identical
    content and names share one copy. No directory search or basename guessing
    is used here.
    """
    root = Path(root)
    result = {}
    for source in sorted({str(Path(p).resolve()) for p in sources if p}):
        path = Path(source)
        if not path.is_file():
            raise BundleError(f"missing declared bundle asset: {source}")
        digest = file_digest(path)
        relative = f"assets/{digest}_{path.name}"
        target = root / relative
        target.parent.mkdir(exist_ok=True)
        if not target.exists():
            shutil.copy2(path, target)
        if file_digest(target) != digest:
            raise BundleError(f"asset changed while snapshotting: {source}")
        result[source] = relative
    return result


def rewrite_references(value, replacements):
    """Rewrite exact references in a parsed config, retaining all other values."""
    if isinstance(value, str):
        return replacements.get(value, value)
    if isinstance(value, list):
        return [rewrite_references(item, replacements) for item in value]
    if isinstance(value, dict):
        return {key: rewrite_references(item, replacements) for key, item in value.items()}
    return value


def compile_config_bundle(template, destination, *, asset_keys, overrides=None,
                          metadata=None, reuse=False):
    """Compile a YAML submit template and its declared file dependencies.

    ``asset_keys`` describes the analysis's top-level file-valued keys. Nested
    YAML ``file`` fields and text ``file=`` tokens declare further dependencies.
    Data input lists and output locations are not guessed from their suffixes.
    """
    import re
    import shlex
    import yaml

    template = Path(template).resolve()
    config = yaml.safe_load(template.read_text())
    if not isinstance(config, dict):
        raise BundleError(f"submit template must be a mapping: {template}")
    config.update(overrides or {})
    sources, dependencies = set(), {}
    # Text model recipes can already use the worker's cfg/<name> spelling.
    # Resolve it only through an explicitly declared top-level asset.
    worker_aliases = {}
    for key in asset_keys:
        if config.get(key):
            candidate = Path(config[key]).expanduser()
            candidate = candidate if candidate.is_absolute() else template.parent / candidate
            if candidate.is_file():
                worker_aliases.setdefault("cfg/" + candidate.name, set()).add(candidate.resolve())

    def resolve(value, parent):
        path = Path(value).expanduser()
        path = (parent / path).resolve() if not path.is_absolute() else path.resolve()
        if not path.is_file() and value in worker_aliases:
            matches = worker_aliases[value]
            if len(matches) != 1:
                raise BundleError(f"ambiguous declared worker asset: {value}")
            path = next(iter(matches))
        if not path.is_file() and not Path(value).is_absolute():
            bundle = find_bundle(parent)
            if bundle and value in json.loads((bundle / MANIFEST).read_text())["files"]:
                path = bundle / value
        if not path.is_file():
            raise BundleError(f"missing declared asset {value!r} relative to {parent}")
        visit(path)
        return str(path)

    def nested(node, parent):
        if isinstance(node, dict):
            return {key: resolve(value, parent) if key == "file" and isinstance(value, str)
                    else nested(value, parent) for key, value in node.items()}
        if isinstance(node, list):
            return [nested(item, parent) for item in node]
        return node

    def visit(path):
        if path in sources:
            return
        sources.add(path)
        if path.suffix in {".yaml", ".yml"}:
            dependencies[path] = nested(yaml.safe_load(path.read_text()), path.parent)
        elif path.suffix == ".txt":
            # ONNX/fit text grammars use whitespace-separated key=value tokens.
            text = path.read_text()
            replacements = {}
            for line in text.splitlines():
                for token in shlex.split(line, comments=True):
                    if token.startswith("file="):
                        value = token[5:]
                        replacements[value] = resolve(value, path.parent)
            dependencies[path] = (text, replacements)

    for key in asset_keys:
        if config.get(key):
            config[key] = resolve(config[key], template.parent)
    request_id = content_digest({"config": config,
        "sources": {str(path): file_digest(path) for path in sorted(sources)}})
    if Path(destination).exists() and reuse:
        record = verify_bundle(destination)
        if record["metadata"].get("request_id") != request_id:
            raise BundleError(f"bundle inputs changed; choose a new bundle directory: {destination}")
        return record
    with atomic_directory(destination) as root:
        assets = snapshot_assets(root, sources)
        for source, value in dependencies.items():
            target = root / assets[str(source)]
            if isinstance(value, tuple):
                text, refs = value
                for old, absolute in refs.items():
                    text = re.sub(r"(?<=file=)" + re.escape(old) + r"(?=\s|$)",
                                  assets[absolute], text)
                target.write_text(text)
            else:
                target.write_text(yaml.safe_dump(rewrite_references(value, assets), sort_keys=False))
        config = rewrite_references(config, assets)
        if config.get("sampleConfig") in assets.values():
            shutil.copy2(root / config["sampleConfig"], root / "dataset_manifest.yaml")
            config["sampleConfig"] = "dataset_manifest.yaml"
        (root / "submit_config.yaml").write_text(yaml.safe_dump(config, sort_keys=False))
        seal_bundle(root, metadata={**(metadata or {}), "request_id": request_id}, assets=assets,
                    entrypoints={"submit_config": "submit_config.yaml",
                                 **({"datasets": config["sampleConfig"]}
                                    if config.get("sampleConfig") == "dataset_manifest.yaml" else {})})
    return verify_bundle(destination)


def seal_bundle(root, *, metadata=None, assets=None, entrypoints=None):
    root = Path(root)
    if (root / MANIFEST).exists():
        raise BundleError(f"bundle is already sealed: {root}")
    files = {}
    for path in sorted(root.rglob("*")):
        if path.is_symlink():
            raise BundleError(f"bundle files must be materialized, not symlinks: {path}")
        if path.is_file():
            files[path.relative_to(root).as_posix()] = file_digest(path)
    for path in (entrypoints or {}).values():
        if path not in files:
            raise BundleError(f"entrypoint is not a bundle file: {path}")
    for path in (assets or {}).values():
        if path not in files:
            raise BundleError(f"asset is not a bundle file: {path}")
    record = dict(schema=SCHEMA, metadata=metadata or {}, files=files,
                  assets=assets or {}, entrypoints=entrypoints or {})
    record["bundle_id"] = content_digest(record)
    write_json(root / MANIFEST, record)
    return record


def verify_bundle(root):
    root = Path(root)
    try:
        record = json.loads((root / MANIFEST).read_text())
    except (OSError, ValueError) as exc:
        raise BundleError(f"cannot read bundle manifest in {root}: {exc}") from exc
    if record.get("schema") != SCHEMA:
        raise BundleError(f"unsupported bundle schema in {root}")
    identity = dict(record)
    bundle_id = identity.pop("bundle_id", None)
    if content_digest(identity) != bundle_id:
        raise BundleError(f"bundle manifest identity mismatch: {root}")
    for relative, expected in record.get("files", {}).items():
        path = root / relative
        if Path(relative).is_absolute() or ".." in Path(relative).parts or path.is_symlink():
            raise BundleError(f"invalid bundle member: {relative}")
        if not path.is_file() or file_digest(path) != expected:
            raise BundleError(f"bundle member missing or changed: {path}")
    return record


def find_bundle(path):
    """Find a sealed bundle enclosing a config; legacy configs return None."""
    path = Path(path).resolve()
    for parent in (path if path.is_dir() else path.parent, *path.parents):
        if (parent / MANIFEST).is_file():
            return parent
    return None


def verify_config_bundle(path):
    root = find_bundle(path)
    return verify_bundle(root) if root else None


def seal_collection(root, children, *, metadata=None):
    """Describe independently sealed children; a collection is not a job bundle."""
    from run_collection import isolated_name
    root = Path(root)
    entries = {}
    for name, value in children.items():
        isolated_name("collection", name)
        path = Path(value)
        child = path if path.is_absolute() else root/path
        entries[name] = dict(bundle=os.path.relpath(child, root),
                             bundle_id=verify_bundle(child)["bundle_id"])
    if not entries:
        raise BundleError("a run collection must have at least one child")
    record = dict(schema="rdf-run-collection-v1", children=entries, metadata=metadata or {})
    record["collection_id"] = content_digest(record)
    target = root/"collection_manifest.json"
    if target.exists():
        raise BundleError(f"collection is already sealed: {root}")
    write_json(target, record)
    return record


def verify_collection(root):
    root = Path(root)
    record = json.loads((root/"collection_manifest.json").read_text())
    identity = dict(record)
    collection_id = identity.pop("collection_id", None)
    if record.get("schema") != "rdf-run-collection-v1" or content_digest(identity) != collection_id:
        raise BundleError("invalid collection manifest identity")
    for name, entry in record["children"].items():
        if verify_bundle(root/entry["bundle"])["bundle_id"] != entry["bundle_id"]:
            raise BundleError(f"collection member changed: {name}")
    return record


def execution_identity(bundle_id, *, inputs, partitions, runtime, options):
    """Freeze discovered input order, partition plan and runtime content hashes."""
    record = dict(schema="rdf-execution-v1", bundle_id=bundle_id, inputs=inputs,
                  partitions=partitions, runtime=runtime, options=options)
    return {**record, "execution_id": content_digest(record)}


def record_execution(path, record):
    """Allow exact resumes; never silently repurpose an existing execution."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(record, stream, sort_keys=True, allow_nan=False)
            stream.write("\n")
        try:
            # Hard-link publication is atomic and cannot replace a competing
            # execution identity, unlike os.replace used for mutable state.
            os.link(name, path)
        except FileExistsError:
            if json.loads(path.read_text()) != record:
                raise BundleError(f"execution identity changed; choose a new run name: {path}")
    finally:
        Path(name).unlink(missing_ok=True)


def derived_artifact(*, kind, parents, configuration, files):
    record = dict(schema="rdf-derived-artifact-v1", kind=kind, parents=parents,
                  configuration=configuration,
                  files={str(p): file_digest(p) for p in sorted(map(str, files))})
    return {**record, "artifact_id": content_digest(record)}


def verify_artifact(path):
    record = json.loads(Path(path).read_text())
    identity = dict(record)
    artifact_id = identity.pop("artifact_id", None)
    if record.get("schema") != "rdf-derived-artifact-v1" or content_digest(identity) != artifact_id:
        raise BundleError(f"invalid derived artifact identity: {path}")
    for source, expected in record["files"].items():
        if not Path(source).is_file() or file_digest(source) != expected:
            raise BundleError(f"derived artifact input missing or changed: {source}")
    return record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("bundle", type=Path)
    args = parser.parse_args(argv)
    try:
        print(verify_bundle(args.bundle)["bundle_id"])
    except BundleError as exc:
        parser.exit(2, f"bundle error: {exc}\n")


if __name__ == "__main__":
    main()
