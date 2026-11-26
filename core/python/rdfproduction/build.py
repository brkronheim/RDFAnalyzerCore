"""Configure, build and validate a native executable for production."""

from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shutil
import tempfile

from rdfworkflow import CMakeBuildSpec, CppAnalysisSpec
from run_bundle import content_digest, file_digest


_FIELDS = {
    "build_directory", "target", "source_directory", "configure_args", "parallel",
    "configuration", "timeout_s", "ctest_regex", "test_targets", "inputs",
}
_SCHEMA = "rdf-production-build-v1"
_TARGET = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.:+-]*")


def _nonempty(value, field):
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError(f"build.{field}: expected a non-empty string")
    return value


def _argv(values, field):
    if not isinstance(values, list):
        raise ValueError(f"build.{field}: expected a list of strings")
    return [_nonempty(value, field) for value in values]


def _configured_source(directory):
    cache = directory / "CMakeCache.txt"
    if cache.is_file():
        for line in cache.read_text(errors="replace").splitlines():
            if line.startswith("CMAKE_HOME_DIRECTORY:INTERNAL="):
                return Path(line.split("=", 1)[1]).resolve()
    return None


def inspect_build(raw, *, base, executable):
    """Normalize the recipe and hash declared inputs without invoking CMake."""
    if not isinstance(raw, dict) or set(raw) - _FIELDS:
        raise ValueError(f"build: expected an object with fields {', '.join(sorted(_FIELDS))}")
    base = Path(base)
    directory = (base / _nonempty(raw.get("build_directory"), "build_directory")).resolve()
    if directory.exists() and not directory.is_dir():
        raise ValueError(f"build.build_directory is not a directory: {directory}")
    source_config = raw.get("source_directory")
    configured_source = _configured_source(directory)
    source = ((base / _nonempty(source_config, "source_directory")).resolve()
              if source_config is not None else configured_source)
    if source is None or not (source / "CMakeLists.txt").is_file():
        raise ValueError("build requires source_directory or an existing configured build_directory with CMakeLists.txt")
    if configured_source is not None and configured_source != source:
        raise ValueError(f"build directory is already configured for {configured_source}, expected {source}")
    if directory == source:
        raise ValueError("build.build_directory must be separate from source_directory")

    configure_args = _argv(raw.get("configure_args", []), "configure_args")
    if any(not re.match(r"^-[DU][^-].*", value) for value in configure_args):
        raise ValueError("build.configure_args accepts CMake cache options such as -DCMAKE_BUILD_TYPE=Release")
    configuration = raw.get("configuration")
    if configuration is not None and _nonempty(configuration, "configuration").startswith("-"):
        raise ValueError("build.configuration must be a CMake configuration name")
    test_targets = _argv(raw.get("test_targets", []), "test_targets")
    if any(not _TARGET.fullmatch(target) for target in test_targets):
        raise ValueError("build.test_targets must contain CMake target names")
    ctest_regex = raw.get("ctest_regex")
    if ctest_regex is not None:
        _nonempty(ctest_regex, "ctest_regex")
    elif test_targets:
        raise ValueError("build.test_targets requires build.ctest_regex")

    supplied_inputs = raw.get("inputs", {})
    if not isinstance(supplied_inputs, dict):
        raise ValueError("build.inputs must map names to spec-relative file paths")
    if "cmake_lists" in supplied_inputs:
        raise ValueError("build.inputs.cmake_lists is automatically selected from source_directory")
    inputs = {"cmake_lists": str(source / "CMakeLists.txt")}
    inputs.update({str(name): str((base / _nonempty(path, "inputs")).resolve())
                   for name, path in supplied_inputs.items()})
    executable = str((base / executable).resolve())
    builder = CMakeBuildSpec(
        str(directory), raw.get("target"), parallel=raw.get("parallel", 2),
        configuration=configuration, timeout_s=raw.get("timeout_s", 3600),
        inputs=inputs, products={"executable": executable},
    )
    for path in inputs.values():
        if not Path(path).is_file():
            raise ValueError(f"declared build input is missing: {path}")
    return {
        "build_directory": builder.build_directory,
        "source_directory": str(source),
        "target": builder.target,
        "configuration": builder.configuration,
        "configure_args": configure_args,
        "parallel": builder.parallel,
        "timeout_s": builder.timeout_s,
        "ctest_regex": ctest_regex,
        "test_targets": test_targets,
        "inputs": inputs,
        "input_hashes": {name: file_digest(path) for name, path in inputs.items()},
    }


def _check_inputs(plan):
    for name, path in plan["inputs"].items():
        if not Path(path).is_file() or file_digest(path) != plan["input_hashes"][name]:
            raise ValueError(f"declared build input changed since inspection: {path}")


def _run(tool, args, logs, plan, *, check=True):
    command = shutil.which(tool)
    if command is None:
        raise ValueError(f"build validation tool is unavailable: {tool}")
    return CppAnalysisSpec(
        executable=command, arguments=tuple(args), working_directory=plan["build_directory"],
        environment={"LC_ALL": "C"}, timeout_s=plan["timeout_s"],
    ).run(logs, check=check)


def _read_json(path):
    return json.loads(Path(path).read_text())


def _check_cmake_target(plan, directory, executable):
    """Bind the executable to its target and capture CMake-listed sources."""
    reply = directory / ".cmake/api/v1/reply"
    indices = list(reply.glob("index-*.json"))
    if not indices:
        raise RuntimeError(f"CMake did not publish a target catalog in {reply}")
    index = _read_json(max(indices))
    model_name = index["reply"]["client-rdf-production"]["codemodel-v2"]["jsonFile"]
    model = _read_json(reply / model_name)
    configs = model["configurations"]
    if plan["configuration"] is not None:
        configs = [item for item in configs if item["name"].lower() == plan["configuration"].lower()]
    if len(configs) != 1:
        raise ValueError("build.configuration must identify one configuration in the CMake target catalog")
    records = configs[0]["targets"]
    target_ids = [item["id"] for item in records if item["name"] == plan["target"]]
    if len(target_ids) != 1:
        raise ValueError(f"target is not uniquely defined in the CMake target catalog: {plan['target']}")
    targets = {item["id"]: item["jsonFile"] for item in records}
    target = _read_json(reply / targets[target_ids[0]])
    products = [(directory / item["path"]).resolve() for item in target.get("artifacts", [])]
    if target["type"] != "EXECUTABLE" or executable not in products:
        names = ", ".join(map(str, products)) or "(none)"
        raise ValueError(f"exe {executable} is not the executable artifact of {plan['target']}: {names}")
    pending = target_ids + [item["id"] for item in records if item["name"] in plan["test_targets"]]
    source = Path(model["paths"]["source"])
    visited, sources = set(), set()
    while pending:
        target_id = pending.pop()
        target_file = targets.get(target_id)
        if target_id in visited or target_file is None:
            continue
        visited.add(target_id)
        target = _read_json(reply / target_file)
        sources.update(str((source / item["path"]).resolve()) for item in target.get("sources", [])
                       if not item.get("isGenerated") and (source / item["path"]).is_file())
        pending.extend(item["id"] for item in target.get("dependencies", []))
    return {path: file_digest(path) for path in sorted(sources)}


def _check_target_sources(sources):
    for path, digest in sources.items():
        if not Path(path).is_file() or file_digest(path) != digest:
            raise ValueError(f"CMake-listed target source changed since build: {path}")


def _check_root_linkage(libraries):
    root = os.environ.get("ROOTSYS")
    if not root:
        return
    prefix = Path(root).resolve()
    for soname, path in libraries.items():
        if not Path(path).resolve().is_relative_to(prefix):
            raise ValueError(f"native executable links {soname} at {path}, outside the active ROOTSYS {root}")


def _validate_elf(executable, logs, plan):
    if not executable.is_file() or not os.access(executable, os.X_OK):
        raise ValueError(f"native build did not produce a runnable executable: {executable}")
    _run("readelf", ("-W", "-h", "-l", str(executable)), logs / "elf", plan)
    headers = (logs / "elf/stdout.log").read_text(errors="replace")
    entry = re.search(r"(?m)^\s*Entry point address:\s*(0x[\da-fA-F]+)", headers)
    if not entry or int(entry[1], 16) == 0:
        raise ValueError(f"native executable has no ELF entry point: {executable}")
    interpreter = re.search(r"\[Requesting program interpreter: ([^\]]+)\]", headers)
    linked = {"elf": str(logs / "elf/stdout.log"), "interpreter": interpreter[1] if interpreter else None}
    if interpreter or re.search(r"(?m)^\s+DYNAMIC\s", headers):
        result = _run("ldd", (str(executable),), logs / "link", plan, check=False)
        diagnostics = "\n".join((logs / "link" / name).read_text(errors="replace")
                                for name in ("stdout.log", "stderr.log"))
        if "not found" in diagnostics:
            missing = "\n".join(line.strip() for line in diagnostics.splitlines() if "not found" in line)
            raise ValueError(f"unresolved native dependencies for {executable}: {missing}. Logs: {logs / 'link'}")
        result.raise_for_status()
        linked["link"] = str(logs / "link/stdout.log")
        root_libraries = dict(re.findall(
            r"(?m)^\s*(lib(?:Core|RIO|ROOTDataFrame|Tree|Cling)\.so(?:\.[^\s]+)?)\s+=>\s+(\S+)",
            diagnostics,
        ))
        if any(name.startswith(("libRIO.so", "libROOTDataFrame.so", "libCling.so")) for name in root_libraries):
            _check_root_linkage(root_libraries)
            linked["root_libraries"] = root_libraries
    return linked


def build_verified(plan, executable, log_directory):
    """Build the target and return a receipt after host validation succeeds."""
    _check_inputs(plan)
    directory = Path(plan["build_directory"])
    executable = Path(executable).resolve()
    log_directory = Path(log_directory).resolve()
    log_directory.mkdir(parents=True, exist_ok=True)
    logs = Path(tempfile.mkdtemp(prefix="attempt-", dir=log_directory))

    query = directory / ".cmake/api/v1/query/client-rdf-production/codemodel-v2"
    query.parent.mkdir(parents=True, exist_ok=True)
    query.touch()
    _run("cmake", ("-S", plan["source_directory"], "-B", str(directory), *plan["configure_args"]),
         logs / "configure", plan)
    sources = _check_cmake_target(plan, directory, executable)
    builder = CMakeBuildSpec(
        str(directory), plan["target"], parallel=plan["parallel"], configuration=plan["configuration"],
        timeout_s=plan["timeout_s"], inputs=plan["inputs"], products={"executable": str(executable)},
    )
    builder.build(logs / "build")
    manifest_path = logs / "build/build_manifest.json"
    built = _read_json(manifest_path)["files"][str(executable)]

    for index, target in enumerate(plan["test_targets"]):
        CMakeBuildSpec(str(directory), target, parallel=plan["parallel"],
                       configuration=plan["configuration"], timeout_s=plan["timeout_s"]).build(
                           logs / f"test-target-{index}")
    ctest_log = None
    if plan["ctest_regex"] is not None:
        regex = plan["ctest_regex"]
        args = ["--test-dir", str(directory), "--tests-regex", regex, "--output-on-failure", "--no-tests=error"]
        if plan["configuration"] is not None:
            args += ["--build-config", plan["configuration"]]
        result = _run("ctest", args, logs / "ctest", plan, check=False)
        diagnostics = "\n".join((logs / "ctest" / name).read_text(errors="replace")
                                for name in ("stdout.log", "stderr.log"))
        if not result.success and "No tests were found" in diagnostics:
            raise ValueError(f"build.ctest_regex {regex!r} matched no tests in {directory}. Logs: {logs / 'ctest'}")
        result.raise_for_status()
        ctest_log = str(logs / "ctest")

    validation = _validate_elf(executable, logs, plan)
    _check_inputs(plan)
    _check_target_sources(sources)
    if file_digest(executable) != built:
        raise ValueError(f"built executable changed during validation: {executable}")
    record = {
        "schema": _SCHEMA,
        "plan_sha256": content_digest(plan),
        "executable": str(executable),
        "executable_sha256": built,
        "build_manifest": str(manifest_path),
        "target_source_hashes": sources,
        "validation": {**validation, "ctest_regex": plan["ctest_regex"], "ctest_log": ctest_log,
                       "scope": "CMake target sources, declared build inputs and submit-host executable linkage"},
        "log_directory": str(logs),
    }
    return {**record, "receipt_id": content_digest(record)}


def verify_build_receipt(receipt, plan, executable):
    """Check the receipt against the current recipe, inputs and executable."""
    record = receipt if isinstance(receipt, dict) else _read_json(receipt)
    if not isinstance(record, dict):
        raise ValueError("invalid native build receipt")
    body = {key: value for key, value in record.items() if key != "receipt_id"}
    if record.get("schema") != _SCHEMA or record.get("receipt_id") != content_digest(body):
        raise ValueError("invalid native build receipt")
    executable = Path(executable).resolve()
    if record.get("plan_sha256") != content_digest(plan) or record.get("executable") != str(executable):
        raise ValueError("native build receipt does not belong to the configured target and executable")
    _check_inputs(plan)
    _check_target_sources(record["target_source_hashes"])
    _check_root_linkage(record["validation"].get("root_libraries", {}))
    if not executable.is_file() or not os.access(executable, os.X_OK) or file_digest(executable) != record.get("executable_sha256"):
        raise ValueError(f"native build product is missing or has changed: {executable}")
    return record
