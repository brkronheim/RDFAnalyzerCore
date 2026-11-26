"""Package Python analyses for the framework's executable worker interface."""

from __future__ import annotations

import hashlib
from io import BytesIO
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import tempfile
import zipfile

from run_bundle import content_digest, file_digest


_MODULE_PATH = re.compile(r"(?:[A-Za-z_]\w*/)*[A-Za-z_]\w*\.py", re.ASCII)
_INTERPRETER = re.compile(r"(?:/[A-Za-z0-9_.+-]+)*/?[A-Za-z0-9_.+-]+")
# Match the Python authoring and controller files configured next to the native
# binding by InstallPythonAuthoring.cmake and InstallPythonController.cmake.
_RUNTIME_MODULES = (
    "_rdfjson", "_rdftext", "native_config_io", "optional_extensions", "rdfanalysis", "rdfcolumns",
    "rdfdeclarations", "rdfmodules", "rdfresults", "rdfschema", "rdfsettings",
    "rdftnp", "rdfkinematics", "rdfvalidation", "rdfworkflow", "run_bundle", "run_collection",
)


def _source(value, base):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Python analysis sources must be file paths")
    path = (Path(base) / value).resolve()
    if not path.is_file() or path.suffix != ".py":
        raise ValueError(f"Python analysis source must be an existing .py file: {path}")
    return {"path": str(path), "sha256": file_digest(path)}


def inspect_python(spec, base):
    """Resolve and fingerprint a Python entrypoint and its declared source modules.

    Module keys are import paths within the launcher, and source paths are
    relative to the spec. Native modules and site dependencies come from the
    worker environment.
    """
    if not isinstance(spec, dict) or "entrypoint" not in spec:
        raise ValueError("python must be an object with an entrypoint")
    unknown = spec.keys() - {"entrypoint", "modules", "interpreter"}
    if unknown:
        raise ValueError(f"unknown Python analysis options: {sorted(unknown)}")
    interpreter = spec.get("interpreter", "python3")
    if not isinstance(interpreter, str) or not _INTERPRETER.fullmatch(interpreter):
        raise ValueError("Python interpreter must be a command name or absolute path without spaces")
    modules = spec.get("modules", {})
    if not isinstance(modules, dict):
        raise ValueError("python.modules must map import paths to source files")

    sources = {"__main__.py": _source(spec["entrypoint"], base)}
    for name, value in modules.items():
        if not isinstance(name, str) or name == "__main__.py" or not _MODULE_PATH.fullmatch(name):
            raise ValueError(f"invalid Python module destination: {name!r}")
        sources[name] = _source(value, base)
    record = {"schema": "rdf-production-python-v1", "interpreter": interpreter, "sources": sources}
    return {**record, "launcher_id": content_digest(record)}


def _archive_bytes(record):
    interpreter = record["interpreter"]
    command = interpreter if interpreter.startswith("/") else f"/usr/bin/env {interpreter}"
    stream = BytesIO(f"#!{command}\n".encode("ascii"))
    stream.seek(0, os.SEEK_END)
    with zipfile.ZipFile(stream, "w") as archive:
        directories = {str(parent) + "/" for name in record["sources"]
                       for parent in PurePosixPath(name).parents if parent != PurePosixPath(".")}
        for name in sorted(directories):
            archive.writestr(zipfile.ZipInfo(name), b"")
        for name, source in sorted(record["sources"].items()):
            content = Path(source["path"]).read_bytes()
            if hashlib.sha256(content).hexdigest() != source["sha256"]:
                raise ValueError(f"Python source changed after inspection: {source['path']}")
            try:
                compile(content, source["path"], "exec", dont_inherit=True)
            except SyntaxError as error:
                location = f"{source['path']}:{error.lineno}" if error.lineno else source["path"]
                raise ValueError(f"invalid Python source {location}: {error.msg}") from error
            archive.writestr(zipfile.ZipInfo(name), content)
    return stream.getvalue()


def materialize_python(record, directory):
    """Validate sources and atomically publish a deterministic Python launcher."""
    content = _archive_bytes(record)
    directory = Path(directory).resolve()
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"analysis_{record['launcher_id']}.pyz"
    fd, name = tempfile.mkstemp(prefix=".python-launcher.", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            os.fchmod(handle.fileno(), 0o755)
        try:
            os.link(name, target)
        except FileExistsError:
            if (target.is_symlink() or not target.is_file() or not os.access(target, os.X_OK)
                    or file_digest(target) != hashlib.sha256(content).hexdigest()):
                raise ValueError(f"existing Python launcher does not match its declared sources: {target}")
    finally:
        Path(name).unlink()
    return target


def stage_python_bindings(module_dir, destination, repo_root):
    """Stage the binding selected by this interpreter and its local runtime."""
    from importlib.machinery import EXTENSION_SUFFIXES

    module_dir = Path(module_dir).resolve()
    destination = Path(destination)
    repo_root = Path(repo_root).resolve()
    module = next((module_dir / f"rdfanalyzer{suffix}" for suffix in EXTENSION_SUFFIXES
                   if (module_dir / f"rdfanalyzer{suffix}").is_file()), None)
    if module is None:
        return None

    from .transport import collect_local_shared_libs

    destination.mkdir(parents=True, exist_ok=True)
    shutil.copy2(module, destination)
    for name in _RUNTIME_MODULES:
        source = module_dir / f"{name}.py"
        if source.is_file():
            shutil.copy2(source, destination)

    # A configured build or installation can live outside the checkout. Its
    # python directory has the corresponding native libraries in its parent.
    runtime_root = repo_root if module_dir.is_relative_to(repo_root) else module_dir.parent
    for relative, source in collect_local_shared_libs(str(module), str(runtime_root)).items():
        shutil.copy2(source, destination / relative)

    for helper, name in (("rdftnp", "TagAndProbe.h"), ("rdfkinematics", "AnalysisMath.h"),
                         ("rdfkinematics", "ArrayValidation.h")):
        if not (destination / f"{helper}.py").is_file():
            continue
        candidates = (module_dir.parent / "include/rdfanalyzer" / name,
                      module_dir / name, repo_root / "core/interface" / name)
        header = next((path for path in candidates if path.is_file()), None)
        if header is None:
            raise FileNotFoundError(f"Cannot stage {helper}.py: missing required native header {name}")
        shutil.copy2(header, destination)
    return module
