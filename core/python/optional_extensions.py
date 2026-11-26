"""Discover extension-owned manifests, Python services, and environment hooks."""
from __future__ import annotations

import importlib
import importlib.util
import json
import os
from pathlib import Path
import sys


def _mode(variable: str, default="AUTO") -> str:
    mode = os.environ.get(variable, default).upper()
    if mode not in {"AUTO", "ON", "OFF"}:
        raise ValueError(f"{variable} must be AUTO, ON, or OFF")
    return mode


def extension_checkouts():
    """Return manifests by ID; explicit checkouts override automatic discovery."""
    if _mode("RDF_BUILD_EXTENSIONS", "ON") == "OFF":
        return {}
    root = Path(__file__).resolve().parents[2]
    explicit = [Path(p).resolve() for p in os.environ.get("RDF_EXTENSION_PATHS", "").split(os.pathsep) if p]
    automatic = [p.parent for p in sorted((root / "extensions").glob("*/rdf-extension.json"))]
    result = {}
    explicit_ids = set()
    for source in explicit + automatic:
        source = source.resolve()
        manifest = json.loads((source / "rdf-extension.json").read_text())
        name = manifest["name"]
        if name in result:
            if source == result[name][0]:
                continue
            if source in explicit or name not in explicit_ids:
                raise ImportError(f"Duplicate extension {name}")
            continue
        if manifest["core_api_version"] != 1:
            raise ImportError(f"Incompatible {name} extension API: {manifest['core_api_version']} (expected 1)")
        result[name] = (source, manifest)
        if source in explicit:
            explicit_ids.add(name)
    return result


def extension_source(name: str) -> Path | None:
    if _mode("RDF_BUILD_EXTENSIONS", "ON") == "OFF":
        return None
    # Compatibility override interpreted by name, without declaring any
    # experiment-specific switches in the framework.
    override = os.environ.get(f"RDF_{name.upper()}_SOURCE_DIR")
    record = extension_checkouts().get(name)
    if override:
        source = Path(override).resolve()
        if (source / "rdf-extension.json").is_file():
            manifest = json.loads((source / "rdf-extension.json").read_text())
            if manifest["name"] != name or manifest["core_api_version"] != 1:
                raise ImportError(f"Invalid or incompatible extension {name} at {source}")
            record = (source, manifest)
        else:
            record = None
    variable = record[1].get("enable_variable", f"RDF_ENABLE_{name.upper()}") if record else f"RDF_ENABLE_{name.upper()}"
    mode = _mode(variable)
    if mode == "OFF":
        return None
    if record:
        return record[0]
    if mode == "ON":
        raise ImportError(f"Missing {name} extension; add its checkout to RDF_EXTENSION_PATHS")
    return None


def load_extension_module(name: str, module: str, *, required: bool = False):
    """Load an optional package. Errors inside providers are never swallowed."""
    record = extension_checkouts().get(name)
    variable = record[1].get("enable_variable", f"RDF_ENABLE_{name.upper()}") if record else f"RDF_ENABLE_{name.upper()}"
    if _mode("RDF_BUILD_EXTENSIONS", "ON") == "OFF" or _mode(variable) == "OFF":
        if required:
            raise ImportError(f"{name} extension disabled")
        return None
    source = extension_source(name)
    package = f"rdfanalyzer_{name}"
    if source:
        manifest = json.loads((source / "rdf-extension.json").read_text())
        package = manifest.get("python_package", package)
        python_dir = str(source / "python")
        if python_dir not in sys.path:
            sys.path.insert(0, python_dir)
    if importlib.util.find_spec(package) is not None:
        return importlib.import_module(f"{package}.{module}")
    if required:
        raise ImportError(f"{name} extension unavailable. Download its checkout or set RDF_EXTENSION_PATHS.")
    return None


def load_extension_services(service: str):
    providers = []
    for name, (_, manifest) in extension_checkouts().items():
        module = manifest.get("python_services", {}).get(service)
        if module and extension_source(name) is not None:
            providers.append(load_extension_module(name, module, required=True))
    return providers


def load_extension_service(service: str):
    providers = load_extension_services(service)
    if len(providers) > 1:
        raise ImportError(f"Multiple enabled extensions provide the exclusive service {service}")
    return providers[0] if providers else None


def environment_scripts():
    for name, (source, manifest) in extension_checkouts().items():
        script = manifest.get("environment")
        if script and extension_source(name) is not None:
            yield source / script


def render_environment_setup(setup_text: str) -> str:
    """Snapshot marked extension hooks before relocating a setup script.

    LAW embeds setup text in local temporary scripts and Condor run scripts.
    Discover extension presets on the submit host so those scripts need no
    repository checkout or extension-discovery Python helper on the worker.
    Unmarked user setup scripts retain their original behavior.
    """
    begin = "# RDF_EXTENSION_ENV_BEGIN"
    end = "# RDF_EXTENSION_ENV_END"
    if begin not in setup_text and end not in setup_text:
        return setup_text
    if setup_text.count(begin) != 1 or setup_text.count(end) != 1:
        raise ValueError("invalid extension environment snapshot markers")
    prefix, rest = setup_text.split(begin, 1)
    if end not in rest:
        raise ValueError("extension environment snapshot markers are out of order")
    _, suffix = rest.split(end, 1)
    hooks = []
    for index, script in enumerate(environment_scripts()):
        # A function preserves a sourced hook's early-return semantics without
        # needing a worker-side file. Exported variables remain in this shell.
        function = f"rdf_snapshot_extension_environment_{index}"
        hooks.append(
            f"{function}() {{\n:\n{script.read_text().rstrip()}\n}}\n"
            f"{function} || return 1\nunset -f {function}"
        )
    return prefix + "# Snapshotted extension environment hooks.\n" + "\n".join(hooks) + suffix
