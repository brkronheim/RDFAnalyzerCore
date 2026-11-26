"""Shared standalone Combine runtime and validated MultiDimFit execution."""
from __future__ import annotations
import json
import math
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from .errors import FitError

def _find_combine(executable: str | None) -> str:
    if executable:
        found = shutil.which(executable)
        if found:
            return found
        candidate = Path(executable).expanduser().resolve()
        if candidate.is_file() and candidate.stat().st_mode & 0o111:
            return str(candidate)
        raise FitError(f"CMS Combine executable is not executable: {executable}")

    workspace = Path(__file__).resolve().parents[3]
    candidates = [
        workspace / "build/external/HiggsAnalysis/CombinedLimit/bin/combine",
        workspace / "build/external/HiggsAnalysis/CombinedLimit/exe/combine",
    ]
    for candidate in candidates:
        if candidate.is_file() and candidate.stat().st_mode & 0o111:
            return str(candidate)
    found = shutil.which("combine")
    if found:
        return found
    raise FitError(
        "CMS Combine was not found. Build it with "
        "cmake -S . -B build -DBUILD_COMBINE=ON && cmake --build build, "
        "or pass --combine-exe /path/to/combine."
    )


def _ensure_combine_stack_size() -> int | None:
    """Give large RooFit graphs enough stack in this process and its children.

ROOT workspace import can exhaust an 8 MiB stack and segfault without a useful
trace. Raise only this process's soft limit, respecting the existing hard limit;
the setting is inherited by converter, fit, impacts and GoF child processes.
"""
    try:
        import resource
    except ImportError:
        return None
    soft, hard = resource.getrlimit(resource.RLIMIT_STACK)
    desired = 512 * 1024 * 1024
    if soft == resource.RLIM_INFINITY or soft >= desired:
        return soft
    target = desired if hard == resource.RLIM_INFINITY else min(desired, hard)
    if target > soft:
        try:
            resource.setrlimit(resource.RLIMIT_STACK, (target, hard))
            return target
        except (OSError, ValueError):
            pass
    return soft


def _combine_runtime_environment(
    executable: str,
) -> tuple[dict[str, str], list[str], list[str], list[str]]:
    """Expose the selected standalone Combine install to its child processes."""
    _ensure_combine_stack_size()
    environment = os.environ.copy()
    # text2workspace is also launched internally by Combine. Keep its env
    # shebang on the invoking Python environment, which owns numpy/PyROOT.
    python_bin = str(Path(sys.executable).absolute().parent)
    environment["PATH"] = os.pathsep.join([python_bin, environment.get("PATH", "")])
    resolved_executable = Path(executable).expanduser().resolve()
    prefix = resolved_executable.parent.parent

    executable_dir = resolved_executable.parent
    runtime_bin_dirs: list[str] = []
    if (executable_dir / "text2workspace.py").is_file():
        combine_bin = str(executable_dir)
        inherited_path = environment.get("PATH")
        path_entries = inherited_path.split(os.pathsep) if inherited_path else []
        if combine_bin not in path_entries:
            environment["PATH"] = (
                f"{combine_bin}{os.pathsep}{inherited_path}"
                if inherited_path is not None else combine_bin
            )
            runtime_bin_dirs.append(combine_bin)

    runtime_python_dirs: list[str] = []
    combine_python = prefix / "python"
    if (combine_python / "HiggsAnalysis" / "CombinedLimit").is_dir():
        combine_python_path = str(combine_python)
        inherited_pythonpath = environment.get("PYTHONPATH")
        python_entries = (
            inherited_pythonpath.split(os.pathsep) if inherited_pythonpath else []
        )
        if combine_python_path not in python_entries:
            environment["PYTHONPATH"] = (
                f"{combine_python_path}{os.pathsep}{inherited_pythonpath}"
                if inherited_pythonpath is not None else combine_python_path
            )
            runtime_python_dirs.append(combine_python_path)

    library_dirs = [prefix / name for name in ("lib", "lib64")]
    library_dirs = [
        path for path in library_dirs
        if (path / "libHiggsAnalysisCombinedLimit.so").is_file()
    ]

    inherited_value = environment.get("LD_LIBRARY_PATH")
    inherited = inherited_value.split(os.pathsep) if inherited_value else []
    inherited_nonempty = {path for path in inherited if path}
    selected = [
        str(path) for path in library_dirs
        if str(path) not in inherited_nonempty
    ]
    if selected:
        environment["LD_LIBRARY_PATH"] = os.pathsep.join([*selected, *inherited])
    return environment, selected, runtime_bin_dirs, runtime_python_dirs


def _theory_nuisance_names(systematics: dict[str, Any]) -> list[str]:
    """Return LHE scale and ISR/FSR shape nuisances from a saved card map."""
    names: set[str] = set()
    for record in systematics.get("shape_nuisances") or []:
        if not isinstance(record, dict) or not record.get("name"):
            continue
        name = str(record["name"])
        group = str(record.get("theory_group", "")).lower()
        # The name checks keep fit-only runs compatible with cards prepared
        # before theory_group was recorded in the nuisance metadata.
        if group in {"isr", "fsr", "muf_mur"} or name.startswith((
            "PS_ISR_", "PS_FSR_", "QCDscale_",
        )):
            names.add(name)
    return sorted(names)


def physics_model_workspace_options(card_dir: Path, environment: dict[str, str]) -> list[str]:
    """Load the staged plugin when any fit/diagnostic rebuilds a polynomial workspace."""
    map_path = card_dir / "poi_map.json"
    model = json.loads(map_path.read_text()).get("physics_model") if map_path.is_file() else None
    if not model:
        return []
    # Resolve staged artifacts beside the card, so the directory can be moved.
    module = card_dir / Path(model["module"]).name
    config = card_dir / Path(model["config"]).name
    for path in (module, config):
        if not path.is_file():
            raise FitError(f"prepared polynomial model is missing: {path}")
    environment["PYTHONPATH"] = os.pathsep.join(
        [str(card_dir.resolve()), environment.get("PYTHONPATH", "")]
    )
    return ["-P", model["plugin"], "--PO", f"modelConfig={config.resolve()}"]


def run_combine(card_info: dict[str, Any], output_dir: Path,
                asimov: bool, combine_exe: str | None,
                freeze_theory_nuisances: bool = True, *,
                minimizer_max_calls: int | None = None,
                combine_verbosity: int | None = None,
                finder=None, runtime=None, workspace_options_factory=None) -> dict[str, Any]:
    polynomial_fit = card_info.get("fit_model") == "polynomial"
    if minimizer_max_calls is None and polynomial_fit:
        minimizer_max_calls = 200000
    if minimizer_max_calls is not None and minimizer_max_calls <= 0:
        raise FitError("--minimizer-max-calls must be positive")
    if combine_verbosity is None:
        # FitterAlgoBase silences both stdout and stderr below verbosity 3,
        # including Minuit's actual reason for a failed minimization.
        combine_verbosity = 3 if polynomial_fit else 0
    executable = (finder or _find_combine)(combine_exe)
    card_path = Path(card_info["datacard"])
    pois = card_info["active_pois"]
    fit_parameters = card_info.get("floating_parameters", pois)
    systematics = card_info.get("systematics") or {}
    tracked_nuisances = sorted({
        str(record["name"])
        for record in [
            *(systematics.get("background_normalizations") or []),
            *(systematics.get("shape_nuisances") or []),
        ]
        if isinstance(record, dict) and record.get("name")
    })
    tracked_nuisances = sorted(set(tracked_nuisances) | (set(fit_parameters) - set(pois)))
    tracked_parameters = [*pois, *tracked_nuisances]
    frozen_theory_nuisances = (
        _theory_nuisance_names(systematics) if freeze_theory_nuisances else []
    )
    frozen_parameters = [*(["r"] if "r" not in fit_parameters else []), *frozen_theory_nuisances]
    nominal_parameter_values = [
        "r=1", *(f"{name}=0" for name in frozen_theory_nuisances),
        *(f"{name}=1" for name in fit_parameters if name.startswith("ttbar_c_")),
    ]
    tag = card_info.get("fit_tag_prefix", "rdf_fit_") + re.sub(r"[^A-Za-z0-9_]+", "_", card_path.parent.name)
    if asimov:
        tag += "_asimov"
    # Saving pseudo-data adds the explicit toy seed to Combine's filename.
    seed_suffix = ".123456" if asimov else ""
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    root_path = output_dir / f"higgsCombine{tag}.MultiDimFit.mH125{seed_suffix}.root"
    robust_hesse_path = output_dir / f"robustHesse{tag}.root"

    def output_signature(path: Path) -> tuple[int, int, int] | None:
        stat = path.stat() if path.is_file() else None
        return (stat.st_mtime_ns, stat.st_ctime_ns, stat.st_size) if stat else None

    previous_outputs = {path: output_signature(path)
                        for path in (root_path, robust_hesse_path)}
    command = [
        executable, "-M", "MultiDimFit", str(card_path), "-m", "125",
        "--algo", "none",
        # Combine's --saveFitResult also requests per-POI MINOS scans, even
        # with robustHesse. Polynomial results use the full robustHesse output
        # below; avoid redundant scans of every coefficient.
        *([] if card_info.get("fit_model") == "polynomial" else ["--saveFitResult"]),
        "--saveWorkspace",
        "--cminDefaultMinimizerStrategy", "1" if polynomial_fit else "0",
        "--robustHesse", "1",
        *(["--X-rtd", f"MINIMIZER_MaxCalls={minimizer_max_calls}"]
          if minimizer_max_calls is not None else []),
        *(["--cminFallbackAlgo", "Minuit2,Migrad,2:0.1"] if polynomial_fit else []),
        "-v", str(combine_verbosity),
        "--setParameters", ",".join(nominal_parameter_values),
        *(["--freezeParameters", ",".join(frozen_parameters)] if frozen_parameters else []),
        "--redefineSignalPOIs", ",".join(pois),
        "--trackParameters", ",".join(tracked_parameters),
        "--trackErrors", ",".join(tracked_parameters),
        "-n", tag,
    ]
    if asimov:
        # Keep the fitted pseudo-data for prefit/postfit plots. data_obs in
        # shapes.root remains the observed input even for an Asimov fit.
        command.extend(["-t", "-1", "--expectSignal", "1", "--saveToys", "--seed", "123456"])
    log_path = output_dir / ("combine_asimov.log" if asimov else "combine.log")
    (environment, runtime_library_dirs, runtime_bin_dirs,
     runtime_python_dirs) = (runtime or _combine_runtime_environment)(executable)
    workspace_command = None
    workspace_log = None
    conversion_seconds = 0.0
    workspace_options = (workspace_options_factory or physics_model_workspace_options)(card_path.parent, environment)
    if workspace_options:
        converter = Path(executable).resolve().parent / "text2workspace.py"
        if not converter.is_file():
            raise FitError(f"Combine text2workspace.py is missing: {converter}")
        workspace_path = card_path.parent / "workspace_polynomial.root"
        workspace_command = [str(converter), str(card_path), "-m", "125", "-o",
                             str(workspace_path), *workspace_options]
        workspace_log = card_path.parent / "text2workspace_polynomial.log"
        conversion_start = time.perf_counter()
        with workspace_log.open("w") as log:
            conversion = subprocess.run(workspace_command, cwd=card_path.parent,
                                        stdout=log, stderr=subprocess.STDOUT,
                                        env=environment, check=False)
        conversion_seconds = time.perf_counter() - conversion_start
        if conversion.returncode or not workspace_path.is_file():
            tail = "\n".join(workspace_log.read_text(errors="replace").splitlines()[-35:])
            raise FitError(f"polynomial workspace conversion failed (exit code {conversion.returncode}); "
                           f"see {workspace_log}\n{tail}")
        command[3] = str(workspace_path)
    command_path = output_dir / ("combine_asimov_command.json" if asimov else "combine_command.json")
    command_path.write_text(json.dumps(command, indent=2) + "\n")
    try:
        fit_start = time.perf_counter()
        with log_path.open("w") as log_file:
            result = subprocess.run(
                command, cwd=output_dir, stdout=log_file,
                stderr=subprocess.STDOUT, check=False, env=environment,
            )
        fit_seconds = time.perf_counter() - fit_start
    except OSError as exc:
        raise FitError(f"could not execute CMS Combine: {exc}") from exc
    if result.returncode != 0:
        tail = "\n".join(log_path.read_text(errors="replace").splitlines()[-35:])
        raise FitError(
            f"CMS Combine failed with exit status {result.returncode}; log: {log_path}\n{tail}"
        )
    # Combine can catch a fit exception and still return exit status zero.
    # Never interpret a previous run's covariance as the result of this run.
    log_text = log_path.read_text(errors="replace")
    minimizer_statuses = [int(status) for status in re.findall(
        r"Minimization finished with status=(-?\d+)", log_text,
    )]
    if re.search(r"MultiDimFit failed|Caught exception|Initial minimization failed", log_text):
        # robustHesse can continue after a failed initial fit and bury the
        # useful Minuit diagnostics hundreds of lines above the log tail.
        failure_lines = [line for line in log_text.splitlines() if re.search(
            r"Minimization did NOT converge|Invalid minimum|Reached call limit|"
            r"Initial minimization failed|MultiDimFit failed|Caught exception|^Edm\s+=|^Nfcn\s+=", line,
        )]
        tail = "\n".join(log_text.splitlines()[-35:])
        raise FitError(f"CMS Combine reported a failed fit despite exit status zero; "
                       f"see {log_path}\n" + "\n".join(failure_lines[-8:]) + f"\n{tail}")
    if not root_path.is_file():
        raise FitError(f"Combine returned success but did not create {root_path}; see {log_path}")
    if not robust_hesse_path.is_file():
        raise FitError(
            f"Combine returned success but did not create {robust_hesse_path}; "
            f"robustHesse may have failed; see {log_path}"
        )
    for path, previous in previous_outputs.items():
        if previous is not None and output_signature(path) == previous:
            raise FitError(f"Combine did not update {path}; refusing a stale fit output; "
                           f"see {log_path}")
    values: dict[str, Any] = {}
    nuisance_values: dict[str, Any] = {}
    covariance = None
    fit_covariance = None
    try:
        import ROOT
        hesse_file = ROOT.TFile.Open(str(robust_hesse_path), "READ")
        robust_parameters = (
            hesse_file.Get("floatParsFinal")
            if hesse_file and not hesse_file.IsZombie() else None
        )
        if not robust_parameters:
            raise FitError(
                f"robustHesse output has no floatParsFinal parameter list: "
                f"{robust_hesse_path}"
            )
        for poi in fit_parameters:
            parameter = robust_parameters.find(poi)
            if not parameter:
                raise FitError(
                    f"robustHesse did not retain floating model parameter {poi}; see {log_path}"
                )
            value = float(parameter.getVal())
            error = float(parameter.getError())
            if not math.isfinite(value) or not math.isfinite(error) or error <= 0.0:
                raise FitError(
                    f"robustHesse returned an invalid value/error for floating model parameter "
                    f"{poi}: {value} +/- {error}; see {log_path}"
                )
            if poi in pois:
                values[poi] = {"value": value, "error": error}
        for nuisance in tracked_nuisances:
            parameter = robust_parameters.find(nuisance)
            if parameter:
                nuisance_values[nuisance] = {
                    "value": float(parameter.getVal()),
                    "error": float(parameter.getError()),
                }
            elif nuisance in frozen_theory_nuisances:
                nuisance_values[nuisance] = {"value": 0.0, "error": 0.0}
            else:
                nuisance_values[nuisance] = {"value": None, "error": None}
        covariance_hist = hesse_file.Get("h_covariance")
        if covariance_hist:
            bins = {
                str(covariance_hist.GetXaxis().GetBinLabel(i)): i
                for i in range(1, covariance_hist.GetNbinsX() + 1)
            }
            if all(name in bins for name in pois):
                covariance = [[float(covariance_hist.GetBinContent(bins[a], bins[b]))
                               for b in pois] for a in pois]
            if all(name in bins for name in fit_parameters):
                fit_covariance = [[float(covariance_hist.GetBinContent(bins[a], bins[b]))
                                   for b in fit_parameters] for a in fit_parameters]
        if workspace_options and fit_covariance is None:
            raise FitError(f"polynomial/normalization covariance missing from {robust_hesse_path}")
        hesse_file.Close()
    except ImportError:
        # The covariance file remains available for inspection when PyROOT
        # cannot expose its RooArgSet in this runtime.
        values = {}
        nuisance_values = {}
        if workspace_options:
            raise FitError("reading polynomial coefficients/covariance requires PyROOT")
    return {
        "status": "combine_complete",
        "executable": executable,
        "command": command,
        "command_file": str(command_path.resolve()),
        "workspace_command": workspace_command,
        "workspace_log": str(workspace_log) if workspace_log else None,
        "timing_seconds": {"workspace_conversion": conversion_seconds, "native_driver": fit_seconds},
        "runtime_library_dirs": runtime_library_dirs,
        "runtime_bin_dirs": runtime_bin_dirs,
        "runtime_python_dirs": runtime_python_dirs,
        "method": "MultiDimFit",
        "minimizer_status_history": minimizer_statuses,
        "minimizer_status": minimizer_statuses[-1] if minimizer_statuses else None,
        "minimizer_covariance_forced_positive_definite": (
            minimizer_statuses[-1] == 1 if minimizer_statuses else None
        ),
        "covariance_method": "robustHesse",
        "robust_hesse_dropped_parameters": sorted(set(re.findall(
            r"^>> Dropping (\S+) from the hessian$", log_path.read_text(errors="replace"), re.MULTILINE,
        ))),
        "mode": "asimov" if asimov else "observed",
        "frozen_theory_nuisances": frozen_theory_nuisances,
        "theory_nuisances_enabled": not freeze_theory_nuisances,
        "log": str(log_path.resolve()),
        "root_output": str(root_path.resolve()),
        "robust_hesse_output": str(robust_hesse_path.resolve()),
        "poi_results": values,
        "covariance_order": pois,
        "covariance": covariance,
        "fit_parameter_covariance_order": fit_parameters,
        "fit_parameter_covariance": fit_covariance,
        "nuisance_results": nuisance_values,
    }



def combine_failed(returncode, log_text):
    """Detect Combine failures that can leave a ROOT file and a zero exit status."""
    return bool(returncode or re.search(
        r"MultiDimFit failed|Caught exception|Initial minimization failed|Error when running the combination",
        log_text,
    ))


def run_standard_combine(card, output_dir, *, asimov=False, executable=None,
                         method="MultiDimFit", options=()):
    """Run a standard card with a free common r, retaining fresh native artifacts."""
    import tempfile
    output_dir = Path(output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    executable = _find_combine(executable)
    environment, libraries, bins, python_dirs = _combine_runtime_environment(executable)
    command = [executable, "-M", method, str(Path(card).resolve()), "-n", "rdf_fit"]
    if method == "MultiDimFit":
        command += ["--algo", "none", "--saveFitResult", "--saveWorkspace", "--robustHesse", "1"]
    if asimov:
        command += ["-t", "-1", "--expectSignal", "1"]
    command += list(options)
    log = output_dir / "combine.log"
    with tempfile.TemporaryDirectory(prefix=".fit-", dir=output_dir) as stage:
        with log.open("w") as stream:
            proc = subprocess.run(command, env=environment, cwd=stage, stdout=stream,
                                  stderr=subprocess.STDOUT, check=False)
        log_text = log.read_text(errors="replace")
        artifacts = list(Path(stage).glob("*.root"))
        if not artifacts or combine_failed(proc.returncode, log_text):
            raise FitError(f"Combine failed (exit {proc.returncode}); see {log}")
        targets = []
        for artifact in artifacts:
            target = output_dir / artifact.name
            os.replace(artifact, target)
            targets.append(str(target))
    summary = dict(backend="combine", status="combine_complete", method=method,
                   mode="asimov" if asimov else "observed", command=command,
                   log=str(log), root_outputs=targets, poi_results={}, nuisance_results={})
    hesse_path = output_dir / "robustHesserdf_fit.root"
    if method == "MultiDimFit" and hesse_path.is_file():
        try:
            import ROOT
            handle = ROOT.TFile.Open(str(hesse_path))
            parameters = handle.Get("floatParsFinal")
            hcov = handle.Get("h_covariance")
            names = [p.GetName() for p in parameters]
            values = {p.GetName(): dict(value=float(p.getVal()), error=float(p.getError())) for p in parameters}
            bins_by_name = {hcov.GetXaxis().GetBinLabel(i): i for i in range(1, hcov.GetNbinsX() + 1)}
            cov = [[float(hcov.GetBinContent(bins_by_name[a], bins_by_name[b])) for b in names] for a in names]
            summary.update(poi_results={"r": values["r"]}, nuisance_results={n: v for n, v in values.items() if n != "r"},
                           covariance_order=["r"], covariance=[[cov[names.index("r")][names.index("r")]]],
                           fit_parameter_covariance_order=names, fit_parameter_covariance=cov,
                           covariance_method="robustHesse", robust_hesse_output=str(hesse_path))
            handle.Close()
        except ImportError:
            summary["parameter_readout"] = "PyROOT unavailable; native artifacts retained"
    return summary
