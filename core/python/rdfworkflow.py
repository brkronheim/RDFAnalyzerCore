"""Lightweight descriptions and Python steering of compiled analysis executables.

This module uses only the standard library and the existing run-bundle helpers.
It does not import ROOT, rdfanalyzer, NumPy or Numba. Analysis algorithms and
configuration grammars remain owned by the compiled analysis.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field, replace
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import time
from types import MappingProxyType
from typing import Mapping, Optional, Tuple

from run_bundle import content_digest, derived_artifact, file_digest, write_json


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    message: str
    location: str = ""

    def as_dict(self):
        return dict(code=self.code, message=self.message, location=self.location)


@dataclass(frozen=True)
class ValidationReport:
    level: str
    supported_levels: Tuple[str, ...]
    issues: Tuple[ValidationIssue, ...] = ()

    @property
    def passed(self):
        return not self.issues

    def as_dict(self):
        return dict(schema="rdf-validation-v1", level=self.level,
                    supported_levels=list(self.supported_levels), passed=self.passed,
                    issues=[issue.as_dict() for issue in self.issues])

    def raise_if_invalid(self):
        if not self.passed:
            raise ValueError("; ".join(f"{item.location}: {item.message}" for item in self.issues))
        return self


@dataclass(frozen=True)
class WorkflowDescription:
    backend: str
    capabilities: Tuple[str, ...]
    details: Mapping[str, object]

    def as_dict(self):
        # Return detached JSON data; no native graph or arbitrary callback owners.
        return json.loads(json.dumps(dict(schema="rdf-workflow-description-v1",
            backend=self.backend, capabilities=list(self.capabilities), details=dict(self.details))))

    def explain(self):
        return json.dumps(self.as_dict(), indent=2, sort_keys=True)


def _text(value, location):
    if not isinstance(value, (str, os.PathLike)) or not str(value).strip() or "\0" in str(value):
        raise ValueError(f"{location}: expected a nonempty path/string without NUL")
    return str(value)


def _seconds(value, location, *, zero=False):
    if type(value) not in (int, float) or not math.isfinite(value) or value < 0 or (not zero and value == 0):
        raise ValueError(f"{location}: expected a finite {'nonnegative' if zero else 'positive'} number")
    return float(value)


def _paths(values, base, location):
    if not isinstance(values, Mapping):
        raise ValueError(f"{location}: expected a mapping of names to paths")
    result = {}
    for name, value in values.items():
        if not isinstance(name, str) or not re.fullmatch(r"[A-Za-z_]\w*", name, re.ASCII):
            raise ValueError(f"{location}: names must be identifiers")
        path = Path(_text(value, f"{location}.{name}"))
        result[name] = str((path if path.is_absolute() else base / path).resolve())
    return MappingProxyType(result)


@dataclass(frozen=True)
class CommandResult:
    status: str
    returncode: Optional[int]
    command: Tuple[str, ...]
    wall_s: float
    log_directory: str
    outputs: Mapping[str, str] = field(default_factory=dict)
    issues: Tuple[ValidationIssue, ...] = ()
    provenance: Mapping[str, object] = field(default_factory=dict)
    operation: str = 'command'
    progress: Mapping[str, object] = field(default_factory=dict)

    @property
    def success(self):
        return self.status == "succeeded"

    def as_dict(self):
        return dict(schema="rdf-command-result-v1", status=self.status, success=self.success,
            returncode=self.returncode, command=list(self.command), wall_s=self.wall_s,
            log_directory=self.log_directory, outputs=dict(self.outputs),
            provenance=dict(self.provenance), operation=self.operation,
            progress=dict(self.progress),
            issues=[issue.as_dict() for issue in self.issues],
            completion_scope="process exit and declared files; analysis/framework output validation is separate")

    def raise_for_status(self):
        if not self.success:
            raise ExecutionError(self)
        return self

    def to_run_result(self):
        """Adapt process status without interpreting or reopening analysis outputs."""
        from rdfresults import RunResult
        backend = {'analysis': 'compiled-executable', 'build': 'cmake-build'}.get(self.operation, 'command')
        return RunResult(backend, self.status,
            'process exit and declared files; analysis/framework output validation is separate',
            timings={'worker_wall_s': self.wall_s}, outputs=self.outputs,
            provenance=self.provenance,
            details={'returncode': self.returncode, 'command': list(self.command), 'operation': self.operation,
                'log_directory': self.log_directory, 'progress': dict(self.progress)},
            issues=tuple(item.as_dict() for item in self.issues))


class ExecutionError(RuntimeError):
    def __init__(self, result):
        self.result = result
        detail = "; ".join(item.message for item in result.issues)
        super().__init__(f"Compiled workflow {result.status} (returncode={result.returncode}): "
                         f"{detail}. Logs: {result.log_directory}")


def _progress_interval(value):
    if value is None:
        return None
    value = _seconds(value, 'progress_interval_s')
    if not .5 <= value <= 3600:
        raise ValueError('progress_interval_s: expected 0.5 through 3600 seconds, or None to disable')
    return value


class _CommandProgress:
    """Fixed-size, best-effort snapshots; no child callbacks or log parsing."""
    def __init__(self, directory, interval, operation, started):
        self.path = directory/'progress.json'
        self.interval, self.operation, self.started = interval, operation, started
        self.last = started
        self.error = None
        self.sequence = 0

    def summary(self):
        return dict(enabled=True, path=str(self.path), interval_s=self.interval, write_error=self.error)

    def emit(self, phase, *, process=None, status='running', periodic=False, requested_status=None):
        now = time.perf_counter()
        if self.error is not None or periodic and now-self.last < self.interval:
            return
        self.sequence += 1
        record = dict(schema='rdf-command-progress-v1', phase=phase, status=status,
            operation=self.operation, elapsed_s=now-self.started, updated_unix_s=time.time(),
            pid=None if process is None else process.pid,
            returncode=None if process is None else process.returncode,
            requested_status=requested_status, sequence=self.sequence,
            observation_scope='controller observation only; no event count, percent, PID-liveness or physics validation')
        try:
            write_json(self.path, record)
        except OSError as error:
            self.error = str(error)[:1024]
        self.last = now


def read_command_status(log_directory):
    """Read last recorded state; never execute, signal, probe PIDs or open outputs."""
    directory = Path(log_directory).absolute()
    final = directory/'run.json'
    progress = directory/'progress.json'
    if final.is_file():
        record = json.loads(final.read_text())
        if not isinstance(record, dict):
            raise ValueError('Invalid command completion record: '+str(final))
        status = record.get('status')
        if (record.get('schema') != 'rdf-command-result-v1' or
                status not in ('succeeded', 'failed', 'cancelled', 'timed_out') or
                record.get('success') is not (status == 'succeeded')):
            raise ValueError('Invalid command completion record: '+str(final))
        return dict(schema='rdf-command-status-v1', source=str(final), phase='finished',
            status=status, terminal=True, operation=record.get('operation', 'command'),
            returncode=record.get('returncode'), elapsed_s=record.get('wall_s'),
            progress=record.get('progress', {}), issues=record.get('issues', []),
            observation_scope='recorded process/declared-file completion; outputs are not reread or physics validated')
    if progress.is_file():
        record = json.loads(progress.read_text())
        if (not isinstance(record, dict) or record.get('schema') != 'rdf-command-progress-v1' or
                record.get('phase') not in ('launching', 'running', 'stopping', 'finalizing', 'finished') or
                record.get('status') not in ('running', 'succeeded', 'failed', 'cancelled', 'timed_out')):
            raise ValueError('Invalid command progress record: '+str(progress))
        return dict(schema='rdf-command-status-v1', source=str(progress),
            phase=record['phase'], status='unconfirmed' if record['phase']=='finished' else record['status'],
            reported_status=record['status'], terminal=False, operation=record.get('operation'),
            pid=record.get('pid'), returncode=record.get('returncode'),
            requested_status=record.get('requested_status'), sequence=record.get('sequence'),
            elapsed_s=record.get('elapsed_s'), updated_unix_s=record.get('updated_unix_s'),
            observation_scope='last controller observation, possibly stale; no completion record or PID-liveness check')
    raise FileNotFoundError('No launch status in '+str(directory)+
        '; preparation may still be pending, progress may be disabled, or no command has started')


def _recorded_command_result(document):
    """Validate supplied terminal command values without consulting live state."""
    from rdfresults import RunResult
    scope = 'process exit and declared files; analysis/framework output validation is separate'
    if not isinstance(document, Mapping) or any(type(key) is not str for key in document):
        raise TypeError('command completion record must be a mapping with string keys')
    if type(document.get('schema')) is not str or document['schema'] != 'rdf-command-result-v1':
        raise ValueError('unsupported command completion schema')
    required = {'schema', 'status', 'success', 'returncode', 'command', 'wall_s',
                'log_directory', 'outputs', 'issues', 'completion_scope'}
    optional = {'operation', 'progress', 'provenance', 'artifacts'}
    missing, extra = required - document.keys(), document.keys() - required - optional
    if missing or extra:
        raise ValueError('command completion fields differ: missing ' + repr(sorted(missing)) +
                         '; unknown ' + repr(sorted(extra)))
    status = document['status']
    if type(status) is not str or status not in ('succeeded', 'failed', 'timed_out', 'cancelled'):
        raise ValueError('command completion status must be terminal')
    if type(document['success']) is not bool:
        raise TypeError('command completion success must be a boolean')
    if document['success'] is not (status == 'succeeded'):
        raise ValueError('command completion success disagrees with status')
    if type(document['completion_scope']) is not str or document['completion_scope'] != scope:
        raise ValueError('unsupported command completion scope')
    code = document['returncode']
    if code is not None and type(code) is not int:
        raise TypeError('command completion returncode must be an integer or None')
    if status == 'succeeded' and code != 0:
        raise ValueError('succeeded command completion requires returncode zero')
    command = document['command']
    if (not isinstance(command, (list, tuple)) or not command or
            any(type(item) is not str or '\0' in item for item in command) or not command[0]):
        raise TypeError('command completion command must contain executable/argument strings')
    directory = document['log_directory']
    if type(directory) is not str or not directory.strip() or '\0' in directory:
        raise ValueError('command completion log_directory must be a nonempty string without NUL')
    operation = document.get('operation', 'command')
    if type(operation) is not str or not operation.strip():
        raise ValueError('command completion operation must be a nonempty string')
    for name in ('outputs', 'progress', 'provenance', 'artifacts'):
        if not isinstance(document.get(name, {}), Mapping):
            raise TypeError('command completion ' + name + ' must be a mapping')
    issues = document['issues']
    if not isinstance(issues, (list, tuple)):
        raise TypeError('command completion issues must be a sequence of issue mappings')
    for issue in issues:
        if (not isinstance(issue, Mapping) or set(issue) != {'code', 'message', 'location'} or
                any(type(key) is not str or type(value) is not str for key, value in issue.items())):
            raise TypeError('command completion issues require string code, message and location fields')
    details = {'returncode': code, 'command': list(command), 'operation': operation,
               'log_directory': directory, 'progress': document.get('progress', {})}
    if 'artifacts' in document:
        details['recorded_artifacts'] = document['artifacts']
    backend = {'analysis': 'compiled-executable', 'build': 'cmake-build'}.get(operation, 'command')
    # RunResult validates finite JSON values, timings, path references and nested
    # keys, and freezes copies. Recorded artifacts never supply provenance.
    return RunResult(backend, status, scope,
        timings={'worker_wall_s': document['wall_s']}, outputs=document['outputs'],
        provenance=document.get('provenance', {}), details=details, issues=issues)


def read_command_result(log_directory):
    """Read only run.json into a detached process-scope RunResult snapshot.

    No progress fallback, output reads, process probes or execution occur. Old
    records may omit operation/progress/provenance; these remain command/{}/{}.
    """
    path = Path(_text(log_directory, 'log_directory')).absolute() / 'run.json'
    def unique_object(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError('duplicate command completion JSON key: ' + key)
            result[key] = value
        return result
    return _recorded_command_result(json.loads(path.read_text(), object_pairs_hook=unique_object))


def _stop(process, grace_s):
    """Terminate only this launch's process group, including surviving children."""
    def send(sig):
        try:
            if os.name == "posix":
                os.killpg(process.pid, sig)
            else:
                process.send_signal(sig)
        except ProcessLookupError:
            pass
    send(signal.SIGTERM)
    try:
        process.wait(timeout=grace_s)
    except subprocess.TimeoutExpired:
        pass
    # The leader may have exited while a descendant ignored SIGTERM.
    if os.name == "posix":
        send(signal.SIGKILL)
    elif process.poll() is None:
        process.kill()
    process.wait()


def _execute(command, directory, *, cwd, environment, timeout_s, grace_s,
             outputs=None, artifacts=None, cancel_event=None, finalize_artifacts=None, operation='command',
             progress_interval_s=None):
    progress_interval_s = _progress_interval(progress_interval_s)
    directory = Path(directory).absolute()
    directory.mkdir(parents=True, exist_ok=False)
    outputs = dict(outputs or {})
    started = time.perf_counter()
    observer = None if progress_interval_s is None else _CommandProgress(directory, progress_interval_s, operation, started)
    process = None
    status, issues, returncode = "failed", [], None
    interrupted = None
    with (directory / "stdout.log").open("wb") as stdout, (directory / "stderr.log").open("wb") as stderr:
        try:
            if observer is not None:
                observer.emit('launching')
            if cancel_event is not None and cancel_event.is_set():
                status = "cancelled"
            else:
                process = subprocess.Popen(list(command), cwd=cwd, env=environment,
                    stdout=stdout, stderr=stderr, start_new_session=(os.name == "posix"))
                if observer is not None:
                    observer.emit('running', process=process)
                deadline = started + timeout_s
                while True:
                    if cancel_event is not None and cancel_event.is_set():
                        status = "cancelled"
                        if observer is not None:
                            observer.emit('stopping', process=process, requested_status=status)
                        _stop(process, grace_s)
                        break
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        status = "timed_out"
                        if observer is not None:
                            observer.emit('stopping', process=process, requested_status=status)
                        _stop(process, grace_s)
                        break
                    try:
                        process.wait(timeout=min(remaining, 0.1))
                        status = "succeeded" if process.returncode == 0 else "failed"
                        break
                    except subprocess.TimeoutExpired:
                        if observer is not None:
                            observer.emit('running', process=process, periodic=True)
                        continue
                returncode = process.returncode
        except KeyboardInterrupt as error:
            interrupted, status = error, "cancelled"
            if observer is not None:
                observer.emit('stopping', process=process, requested_status=status)
            if process is not None:
                _stop(process, grace_s)
                returncode = process.returncode
        except OSError as error:
            issues.append(ValidationIssue("launch_failed", str(error), "executable"))
        except BaseException:
            if process is not None:
                _stop(process, grace_s)
            raise
    if observer is not None:
        observer.emit('finalizing', process=process)
    if status != "succeeded":
        issues.append(ValidationIssue(status, f"Child process {status}", "execution"))
    else:
        for name, path in outputs.items():
            if not Path(path).is_file():
                issues.append(ValidationIssue("missing_output", f"Declared output was not written: {path}", f"outputs.{name}"))
        if issues:
            status = "failed"
    if status == "succeeded" and finalize_artifacts is not None:
        try:
            artifacts = finalize_artifacts()
        except (OSError, ValueError) as error:
            status = "failed"
            issues.append(ValidationIssue("build_identity_failed", str(error), "build"))
    identities = {name: artifacts[name] for name in ('artifact_identity', 'build_identity', 'artifact_id')
                  if artifacts is not None and artifacts.get(name) is not None}
    if artifacts is not None and artifacts.get('build_manifest'):
        identities['build_identity'] = artifacts['build_manifest']['artifact_id']
    result = CommandResult(status, returncode, tuple(command), time.perf_counter() - started,
                           str(directory), MappingProxyType(outputs), tuple(issues),
                           MappingProxyType(identities), operation,
                           MappingProxyType({} if observer is None else observer.summary()))
    record = result.as_dict()
    record["artifacts"] = dict(artifacts or {})
    write_json(directory / "run.json", record)
    if observer is not None:
        observer.emit('finished', process=process, status=result.status)
        summary = observer.summary()
        if dict(result.progress) != summary:
            result = replace(result, progress=MappingProxyType(summary))
            record['progress'] = summary
            # The completion record is already committed. A reporting-only
            # diagnostic refresh must not invalidate that process result.
            try:
                write_json(directory/'run.json', record)
            except OSError:
                pass
    if interrupted is not None:
        raise interrupted
    return result


@dataclass(frozen=True)
class CMakeBuildSpec:
    """Explicit incremental build of a target in an already configured tree."""
    build_directory: str
    target: str
    parallel: int = 2
    configuration: Optional[str] = None
    timeout_s: float = 3600.0
    inputs: Mapping[str, str] = field(default_factory=dict)
    products: Mapping[str, str] = field(default_factory=dict)

    def __post_init__(self):
        object.__setattr__(self, "build_directory", str(Path(_text(self.build_directory, "build_directory")).absolute()))
        if not isinstance(self.target, str) or not re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.:+-]*", self.target):
            raise ValueError("target: expected a CMake target name, not command options")
        if type(self.parallel) is not int or self.parallel < 1:
            raise ValueError("parallel: expected a positive integer")
        if self.configuration is not None:
            object.__setattr__(self, "configuration", _text(self.configuration, "configuration"))
        object.__setattr__(self, "timeout_s", _seconds(self.timeout_s, "build.timeout_s"))
        for name in ("inputs", "products"):
            object.__setattr__(self, name, _paths(getattr(self, name), Path(self.build_directory), "build."+name))
        if bool(self.inputs) != bool(self.products):
            raise ValueError("build.inputs and build.products must both be supplied for a build manifest")
        if set(self.inputs.values()).intersection(self.products.values()):
            raise ValueError("build.products must not overwrite declared build inputs")

    def build(self, log_directory, *, check=True, cancel_event=None, progress_interval_s=None):
        progress_interval_s = _progress_interval(progress_interval_s)
        cache = Path(self.build_directory) / "CMakeCache.txt"
        if not cache.is_file():
            raise ValueError(f"build_directory: no CMakeCache.txt in {self.build_directory}; configure explicitly first")
        cmake = shutil.which("cmake")
        if cmake is None:
            raise ValueError("cmake: executable is unavailable in this environment")
        command = [cmake, "--build", self.build_directory, "--target", self.target, "--parallel", str(self.parallel)]
        if self.configuration is not None:
            command += ["--config", self.configuration]
        artifacts = {"cmake_cache_sha256": file_digest(cache)}
        finalize = None
        if self.inputs:
            identity_started = time.perf_counter()
            owned = {str((Path(log_directory).absolute()/name).resolve()) for name in
                     ("run.json", "progress.json", "stdout.log", "stderr.log", "build_manifest.json")}
            if owned.intersection(self.products.values()):
                raise ValueError("build.products must not overwrite controller-owned logs or build_manifest.json")
            inputs = _capture_files(self.inputs)
            tools = _capture_files({"cmake_cache": str(cache), "cmake": cmake})
            artifacts["declared_inputs"] = inputs
            artifacts["identity_preparation_s"] = time.perf_counter() - identity_started

            def finalize():
                finalization_started = time.perf_counter()
                for name, item in {**inputs, **{"tool."+key: value for key, value in tools.items()}}.items():
                    if not Path(item["path"]).is_file() or file_digest(item["path"]) != item["sha256"]:
                        raise ValueError(f"{name}: declared input/tool changed during build; configure/build again explicitly")
                for name, path in self.products.items():
                    if not Path(path).is_file():
                        raise ValueError(f"products.{name}: successful command did not produce declared file: {path}")
                manifest = derived_artifact(kind="compiled-analysis-build", parents={
                    "declared_inputs": content_digest(inputs)}, configuration={
                    "target": self.target, "configuration": self.configuration,
                    "build_directory": self.build_directory, "declared_inputs": inputs,
                    "tools": tools, "products": dict(self.products),
                    "scope": "declared files observed around an explicit build; transitive closure and loaded paths are not inferred"},
                    files=self.products.values())
                path = Path(log_directory).absolute() / "build_manifest.json"
                write_json(path, manifest)
                return {**artifacts, "build_manifest": {"path": str(path),
                    "artifact_id": manifest["artifact_id"], "sha256": file_digest(path)},
                    "identity_finalization_s": time.perf_counter() - finalization_started}

        result = _execute(command, log_directory, cwd=self.build_directory,
            environment=dict(os.environ), timeout_s=self.timeout_s, grace_s=2.0,
            artifacts=artifacts, cancel_event=cancel_event, finalize_artifacts=finalize, operation='build',
            progress_interval_s=progress_interval_s)
        return result.raise_for_status() if check else result


def _capture_files(paths):
    """Hash each resolved file once, retaining the requested invocation path."""
    hashes = {}
    resolved_paths = {}
    result = {}
    for name, path in paths.items():
        if not Path(path).is_file():
            raise ValueError(f"{name}: required file is missing or not a regular file: {path}")
        if path not in resolved_paths:
            resolved_paths[path] = str(Path(path).resolve())
        resolved = resolved_paths[path]
        if resolved not in hashes:
            hashes[resolved] = file_digest(path)
        result[name] = {"path": path, "sha256": hashes[resolved]}
    return result


def _read_build_manifest(path):
    data = Path(path).read_bytes()
    record = json.loads(data)
    if not isinstance(record, dict):
        raise ValueError("build manifest must be a JSON object")
    identity = dict(record)
    artifact_id = identity.pop("artifact_id", None)
    if record.get("schema") != "rdf-derived-artifact-v1" or content_digest(identity) != artifact_id:
        raise ValueError("invalid build manifest identity")
    if record.get("kind") != "compiled-analysis-build":
        raise ValueError("expected a compiled-analysis-build manifest")
    configuration = record.get("configuration", {})
    if not isinstance(configuration, dict):
        raise ValueError("invalid build manifest configuration")
    products = configuration.get("products", {})
    files = record.get("files", {})
    if not products or not isinstance(products, dict) or not isinstance(files, dict):
        raise ValueError("build manifest requires declared products")
    if any(not isinstance(p, str) or not Path(p).is_absolute() or str(Path(p).resolve()) != p
           for p in products.values()) or set(products.values()) != set(files):
        raise ValueError("build manifest products do not match recorded files")
    for name in ("declared_inputs", "tools"):
        items = configuration.get(name)
        if not isinstance(items, dict) or not items or any(
                not isinstance(item, dict) or not isinstance(item.get("path"), str) or not Path(item["path"]).is_absolute() or
                not re.fullmatch(r"[0-9a-f]{64}", str(item.get("sha256"))) for item in items.values()):
            raise ValueError("invalid build manifest "+name)
    if any(not re.fullmatch(r"[0-9a-f]{64}", str(value)) for value in files.values()):
        raise ValueError("invalid build product digest")
    return record, hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class LocalExecutionResources:
    """Opt-in affinity for one local child; no thread or scheduler policy."""
    cpus: Tuple[int, ...]

    def __post_init__(self):
        if (not isinstance(self.cpus, (tuple, list)) or not self.cpus or
                any(type(cpu) is not int or cpu < 0 for cpu in self.cpus) or
                len(set(self.cpus)) != len(self.cpus)):
            raise ValueError("resources.cpus: expected nonempty unique nonnegative integer CPU IDs")
        object.__setattr__(self, "cpus", tuple(self.cpus))


def _local_resource_tool(resources, environment, working_directory):
    """Resolve an opt-in local tool without changing controller affinity/state."""
    if not sys.platform.startswith("linux") or not hasattr(os, "sched_getaffinity"):
        raise ValueError("resources.cpus: local CPU affinity requires Linux sched_getaffinity")
    try:
        available = os.sched_getaffinity(0)
    except OSError as error:
        raise ValueError("resources.cpus: cannot observe available CPUs: " + str(error)) from error
    unavailable = set(resources.cpus) - available
    if unavailable:
        raise ValueError("resources.cpus: CPUs unavailable to this controller: " + repr(sorted(unavailable)))
    # Relative PATH components belong to the child cwd, never the controller cwd.
    search = environment.get("PATH", os.environ.get("PATH", os.defpath))
    search = os.pathsep.join(str(Path(part) if Path(part).is_absolute() else
        Path(working_directory) / (part or ".")) for part in search.split(os.pathsep))
    tool = shutil.which("taskset", path=search)
    if tool is None:
        raise ValueError("resources.cpus: required taskset tool is unavailable on PATH")
    path = str(Path(tool).resolve())
    if not Path(path).is_file() or not os.access(path, os.X_OK):
        raise ValueError("resources.cpus: taskset must be an executable regular file")
    return {"path": path, "sha256": file_digest(path)}


@dataclass(frozen=True)
class CppAnalysisSpec:
    """Describe a native executable without importing or constructing its graph.

    Argument references are complete tokens, e.g. ``{asset:config}`` and
    ``{output:summary}``. All other tokens are passed literally, without a shell.
    """
    executable: str
    arguments: Tuple[str, ...] = ()
    working_directory: str = "."
    assets: Mapping[str, str] = field(default_factory=dict)
    outputs: Mapping[str, str] = field(default_factory=dict)
    libraries: Mapping[str, str] = field(default_factory=dict)
    environment: Mapping[str, str] = field(default_factory=dict)
    timeout_s: float = 7200.0
    terminate_grace_s: float = 2.0
    build_spec: Optional[CMakeBuildSpec] = None
    build_manifest: Optional[str] = None
    resources: Optional[LocalExecutionResources] = None

    def __post_init__(self):
        base = Path(_text(self.working_directory, "working_directory")).absolute()
        object.__setattr__(self, "working_directory", str(base))
        # Executables are anchored at construction, independently of child cwd.
        object.__setattr__(self, "executable", str(Path(_text(self.executable, "executable")).absolute()))
        if not isinstance(self.arguments, (tuple, list)) or any(not isinstance(arg, str) or "\0" in arg for arg in self.arguments):
            raise ValueError("arguments: expected a sequence of literal argv strings")
        object.__setattr__(self, "arguments", tuple(self.arguments))
        for name in ("assets", "outputs", "libraries"):
            object.__setattr__(self, name, _paths(getattr(self, name), base, name))
        if not isinstance(self.environment, Mapping) or any(
                not isinstance(key, str) or not key or "=" in key or "\0" in key or
                not isinstance(value, str) or "\0" in value for key, value in self.environment.items()):
            raise ValueError("environment: expected valid string keys and values")
        object.__setattr__(self, "environment", MappingProxyType(dict(self.environment)))
        object.__setattr__(self, "timeout_s", _seconds(self.timeout_s, "timeout_s"))
        grace = _seconds(self.terminate_grace_s, "terminate_grace_s", zero=True)
        if grace > 60:
            raise ValueError("terminate_grace_s: maximum is 60 seconds")
        object.__setattr__(self, "terminate_grace_s", grace)
        if self.build_spec is not None and not isinstance(self.build_spec, CMakeBuildSpec):
            raise ValueError("build_spec: expected CMakeBuildSpec or None")
        if self.resources is not None and not isinstance(self.resources, LocalExecutionResources):
            raise ValueError("resources: expected LocalExecutionResources or None")
        if self.build_manifest is not None:
            path = Path(_text(self.build_manifest, "build_manifest"))
            object.__setattr__(self, "build_manifest", str((path if path.is_absolute() else base / path).resolve()))
        destinations = list(self.outputs.values())
        if len(set(destinations)) != len(destinations):
            raise ValueError("outputs: multiple names refer to the same destination")
        if set(destinations).intersection([str(Path(self.executable).resolve()), *self.assets.values(), *self.libraries.values(), self.build_manifest]):
            raise ValueError("outputs: must not overwrite executable, assets or libraries")
        self.command()  # Validate explicit references before accessing files.

    def command(self):
        result = [self.executable]
        for argument in self.arguments:
            match = re.fullmatch(r"\{(asset|output):([A-Za-z_]\w*)\}", argument, re.ASCII)
            if match:
                kind, name = match.groups()
                mapping = self.assets if kind == "asset" else self.outputs
                if name not in mapping:
                    raise ValueError(f"arguments: unknown {kind} reference '{name}'")
                argument = mapping[name]
            result.append(argument)
        return tuple(result)

    def validate(self, level="configuration"):
        supported = ("configuration", "assets") + (("build-inputs",) if self.build_manifest else ())
        if self.resources is not None:
            supported += ("local-resources",)
        if level not in supported:
            return ValidationReport(level, supported, (ValidationIssue("unsupported_level",
                f"Compiled adapter does not advertise validation level '{level}'", "level"),))
        issues = []
        if level == "local-resources":
            try:
                _local_resource_tool(self.resources, self.environment, self.working_directory)
            except (ValueError, OSError) as error:
                issues.append(ValidationIssue("local_resources_unavailable", str(error), "resources.cpus"))
            return ValidationReport(level, supported, tuple(issues))
        if level in ("assets", "build-inputs") and self.build_manifest:
            try:
                manifest, _ = _read_build_manifest(self.build_manifest)
                products = manifest["files"]
                for name, path in (("executable", self.executable),
                                   *(("libraries."+key, value) for key, value in self.libraries.items())):
                    if str(Path(path).resolve()) not in products:
                        raise ValueError(f"{name}: file is not a product of this recorded build")
                if level == "build-inputs":
                    for item in (*manifest["configuration"]["declared_inputs"].values(),
                                 *manifest["configuration"]["tools"].values()):
                        if not Path(item["path"]).is_file() or file_digest(item["path"]) != item["sha256"]:
                            issues.append(ValidationIssue("build_input_changed", "Recorded build input missing or changed: "+item["path"], "build_manifest"))
            except (OSError, ValueError, TypeError) as error:
                issues.append(ValidationIssue("invalid_build_manifest", str(error), "build_manifest"))
        if level == "assets":
            if not Path(self.working_directory).is_dir():
                issues.append(ValidationIssue("missing_directory", "Child working directory does not exist", "working_directory"))
            for name, path in {"executable": self.executable, **{"assets."+key: value for key, value in self.assets.items()},
                    **{"libraries."+key: value for key, value in self.libraries.items()}}.items():
                if not Path(path).is_file():
                    issues.append(ValidationIssue("missing_file", f"Required file does not exist: {path}", name))
            if Path(self.executable).is_file() and not os.access(self.executable, os.X_OK):
                issues.append(ValidationIssue("not_executable", "Executable lacks execute permission", "executable"))
            for name, path in self.outputs.items():
                if Path(path).exists():
                    issues.append(ValidationIssue("existing_output", f"Refusing to overwrite output: {path}", "outputs."+name))
                elif not Path(path).parent.is_dir():
                    issues.append(ValidationIssue("missing_directory", f"Output parent does not exist: {Path(path).parent}", "outputs."+name))
        return ValidationReport(level, supported, tuple(issues))

    def describe(self):
        capabilities = ("configuration", "assets", "run") + (("build",) if self.build_spec else ())
        if self.build_manifest:
            capabilities += ("build-inputs",)
        if self.resources is not None:
            capabilities += ("local-resources",)
        details = {
            "command": list(self.command()), "working_directory": self.working_directory,
            "assets": dict(self.assets), "outputs": dict(self.outputs), "libraries": dict(self.libraries),
            "environment_override_names": sorted(self.environment), "timeout_s": self.timeout_s,
            "build": None if self.build_spec is None else {**self.build_spec.__dict__,
                "inputs": dict(self.build_spec.inputs), "products": dict(self.build_spec.products)},
            "build_manifest": self.build_manifest,
            "build_validation": "optional declared build inputs/products; build-inputs explicitly checks current sources/tools; launch needs only recorded products",
            "graph": "opaque: analysis-owned C++ graph; no dependency inference",
            "library_validation": "declared file hashes only; loaded-path validation and staging are separate"}
        if self.resources is not None:
            details["local_resources"] = {"cpus": list(self.resources.cpus),
                "scope": "requested child CPU affinity only; host/tool checked by explicit local-resources validation, preparation and launch"}
        return WorkflowDescription("compiled-executable", capabilities, details)

    def prepare(self):
        started = time.perf_counter()
        self.validate("assets").raise_if_invalid()
        artifacts = {"executable": self.executable, **{"asset:"+key: value for key, value in self.assets.items()},
                     **{"library:"+key: value for key, value in self.libraries.items()}}
        manifest = None
        if self.build_manifest:
            manifest, digest = _read_build_manifest(self.build_manifest)
            artifacts["build_manifest"] = self.build_manifest
            artifacts.update({"build_product:"+name: path for name, path in manifest["configuration"]["products"].items()})
        hashes = _capture_files(artifacts) if manifest else {
            name: {"path": path, "sha256": file_digest(path)} for name, path in artifacts.items()}
        if manifest:
            if hashes["build_manifest"]["sha256"] != digest:
                raise ValueError("build manifest changed during preparation")
            for name, path in manifest["configuration"]["products"].items():
                if hashes["build_product:"+name]["sha256"] != manifest["files"][path]:
                    raise ValueError("build product changed since recorded build: "+path)
        resource_tool = None if self.resources is None else _local_resource_tool(self.resources, self.environment, self.working_directory)
        return PreparedCppAnalysis(self, hashes, time.perf_counter()-started,
                                   None if manifest is None else manifest["artifact_id"], resource_tool)

    def run(self, log_directory, *, check=True, cancel_event=None, progress_interval_s=None):
        """Prepare immediately before launch, hashing each declared file once.

        Use prepare().run() when retaining an artifact for later execution;
        that path additionally verifies it against the earlier snapshot.
        """
        progress_interval_s = _progress_interval(progress_interval_s)
        return self.prepare()._launch(log_directory, check=check,
            cancel_event=cancel_event, verification_s=0.0,
            verification_policy="prepare-immediately-before-launch", progress_interval_s=progress_interval_s)

    @classmethod
    def from_file(cls, path):
        path = Path(path).absolute()
        data = json.loads(path.read_text())
        if not isinstance(data, dict):
            raise ValueError("compiled spec: expected a JSON object")
        unknown = data.keys() - cls.__dataclass_fields__.keys()
        if unknown:
            raise ValueError("compiled spec: unknown fields: " + ", ".join(sorted(unknown)))
        for name in ("executable", "working_directory"):
            value = data.get(name, "." if name == "working_directory" else None)
            if value is not None:
                data[name] = str(path.parent / Path(_text(value, name)))
        if data.get("build_spec") is not None:
            build = dict(data["build_spec"])
            if "build_directory" in build:
                build["build_directory"] = str(path.parent / Path(build["build_directory"]))
            data["build_spec"] = CMakeBuildSpec(**build)
        if data.get("resources") is not None:
            resource = data["resources"]
            if not isinstance(resource, dict) or set(resource) != {"cpus"}:
                raise ValueError("resources: expected an object containing only cpus")
            data["resources"] = LocalExecutionResources(resource["cpus"])
        return cls(**data)


@dataclass(frozen=True)
class PreparedCppAnalysis:
    spec: CppAnalysisSpec
    artifacts: Mapping[str, object]
    preparation_s: float
    build_identity: Optional[str] = None
    resource_tool: Optional[Mapping[str, str]] = None

    def __post_init__(self):
        frozen = {name: MappingProxyType(dict(item)) for name, item in self.artifacts.items()}
        object.__setattr__(self, "artifacts", MappingProxyType(frozen))
        if self.resource_tool is not None:
            object.__setattr__(self, "resource_tool", MappingProxyType(dict(self.resource_tool)))

    def run(self, log_directory, *, check=True, cancel_event=None, progress_interval_s=None):
        progress_interval_s = _progress_interval(progress_interval_s)
        started = time.perf_counter()
        self.spec.validate("assets").raise_if_invalid()
        current = _capture_files({name: item["path"] for name, item in self.artifacts.items()}) if self.build_identity else None
        for name, artifact in self.artifacts.items():
            digest = current[name]["sha256"] if current is not None else file_digest(artifact["path"])
            if digest != artifact["sha256"]:
                raise ValueError(f"{name}: artifact changed since prepare(); rebuild/reprepare explicitly")
        verification_s = time.perf_counter()-started
        return self._launch(log_directory, check=check, cancel_event=cancel_event,
            verification_s=verification_s, verification_policy="recheck-prepared-artifact",
            progress_interval_s=progress_interval_s)

    def _launch(self, log_directory, *, check, cancel_event, verification_s, verification_policy,
                progress_interval_s=None):
        environment = dict(os.environ)
        environment.update(self.spec.environment)
        artifacts = {name: dict(item) for name, item in self.artifacts.items()}
        record = {"files": artifacts, "artifact_identity": content_digest(artifacts),
                  "preparation_s": self.preparation_s, "verification_s": verification_s,
                  "verification_policy": verification_policy,
                  "build_identity": self.build_identity,
                  "environment_override_names": sorted(self.spec.environment)}
        command = self.spec.command()
        if self.spec.resources is not None:
            tool = _local_resource_tool(self.spec.resources, self.spec.environment, self.spec.working_directory)
            if self.resource_tool is None or tool != dict(self.resource_tool):
                raise ValueError("resources: taskset tool changed since prepare(); reprepare explicitly")
            command = (tool["path"], "--cpu-list", "--", ",".join(map(str, self.spec.resources.cpus)), *command)
            record["local_resources"] = {"cpus": list(self.spec.resources.cpus), "tool": tool,
                "scope": "requested local child affinity; thread counts and NUMA memory policy are not inferred"}
        result = _execute(command, log_directory, cwd=self.spec.working_directory,
            environment=environment, timeout_s=self.spec.timeout_s, grace_s=self.spec.terminate_grace_s,
            outputs=self.spec.outputs, artifacts=record, cancel_event=cancel_event, operation='analysis',
            progress_interval_s=progress_interval_s)
        return result.raise_for_status() if check else result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    for name in ("explain", "validate", "build", "run"):
        command = commands.add_parser(name)
        command.add_argument("spec", help="compiled-analysis JSON specification")
        if name == "validate":
            command.add_argument("--level", default="configuration")
        if name in ("build", "run"):
            command.add_argument("--directory", required=True, help="fresh directory for logs and run.json")
            command.add_argument('--progress-interval', type=float, default=None,
                help='optional controller snapshot interval in seconds (0.5 to 3600; disabled by default)')
            command.add_argument('--result-format', choices=('command','run-result'), default='command',
                help='stdout envelope only; recorded command status and execution are unchanged')
    status_command = commands.add_parser('status', help='read last recorded command status without execution')
    status_command.add_argument('directory', help='existing log directory')
    result_command = commands.add_parser('result', help='recover a recorded terminal command result without execution')
    result_command.add_argument('directory', help='existing log directory containing run.json')
    args = parser.parse_args(argv)
    try:
        if args.command == 'status':
            print(json.dumps(read_command_status(args.directory), indent=2, sort_keys=True))
            return 0
        if args.command == 'result':
            print(json.dumps(read_command_result(args.directory).as_dict(), indent=2, sort_keys=True))
            return 0
        spec = CppAnalysisSpec.from_file(args.spec)
        if args.command == "explain":
            report, passed = spec.describe().as_dict(), True
        elif args.command == "validate":
            validation = spec.validate(args.level)
            report, passed = validation.as_dict(), validation.passed
        elif args.command == "build":
            if spec.build_spec is None:
                raise ValueError("build_spec: absent; supply an explicit CMake build or use a prebuilt executable")
            result = spec.build_spec.build(args.directory, check=False, progress_interval_s=args.progress_interval)
            report, passed = (result.to_run_result().as_dict() if args.result_format=='run-result' else result.as_dict()), result.success
        else:
            result = spec.run(args.directory, check=False, progress_interval_s=args.progress_interval)
            report, passed = (result.to_run_result().as_dict() if args.result_format=='run-result' else result.as_dict()), result.success
    except KeyboardInterrupt as error:
        print(json.dumps({"passed":False,"error":"cancelled"}))
        return 128 + getattr(error,"signal_number",signal.SIGINT)
    except (ValueError, TypeError, OSError) as error:
        report, passed = {"passed": False, "error": str(error)}, False
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if passed else 1


if __name__ == "__main__":
    # CLI-only handling; library calls never change the parent's signal handlers.
    def interrupt(signum, frame):
        error = KeyboardInterrupt()
        error.signal_number = signum
        raise error
    if os.name == "posix":
        signal.signal(signal.SIGTERM, interrupt)
    raise SystemExit(main())
