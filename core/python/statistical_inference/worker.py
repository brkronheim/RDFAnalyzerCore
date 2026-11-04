"""Isolated Rabbit driver. Invoke through run_rabbit, not the analysis interpreter."""
from __future__ import annotations
import importlib.metadata
import json
import os
from pathlib import Path
import sys
import time

import numpy as np

from .errors import FitError
from .parameters import free_parameters, restore_parameters


def _runtime_libraries():
    """Expose pip-installed CUDA libraries before importing TensorFlow.

    Re-exec once: dlopen's library search path is initialized at process start.
    """
    if os.environ.get("RDF_RABBIT_CUDA_READY"):
        return
    import site
    roots = [Path(p) for p in site.getsitepackages()]
    libraries = [str(p) for root in roots for p in (root / "nvidia").glob("*/lib")]
    binaries = [str(p) for root in roots for p in (root / "nvidia").glob("*/bin")]
    os.environ["LD_LIBRARY_PATH"] = os.pathsep.join(libraries + [os.environ.get("LD_LIBRARY_PATH", "")])
    os.environ["PATH"] = os.pathsep.join(binaries + [os.environ.get("PATH", "")])
    os.environ["RDF_RABBIT_CUDA_READY"] = "1"
    os.execv(sys.executable, [sys.executable, "-m", "statistical_inference.worker", *sys.argv[1:]])


def main():
    _runtime_libraries()
    worker_start = time.perf_counter()
    import tensorflow as tf

    path = Path(sys.argv[1]).resolve()
    job = json.loads(path.read_text())
    target = path.parent
    gpus = tf.config.list_physical_devices("GPU")
    if job["device"] == "gpu" and len(gpus) < job["n_devices"]:
        raise FitError(f"requested {job['n_devices']} GPUs; TensorFlow sees {len(gpus)}: {gpus}")
    for gpu in gpus:
        tf.config.experimental.set_memory_growth(gpu, True)
    versions = {p: importlib.metadata.version(p) for p in ("rabbit-fit", "tensorflow", "tensorflow-probability", "numpy", "scipy")}
    direct_url = importlib.metadata.distribution("rabbit-fit").read_text("direct_url.json")
    revision = json.loads(direct_url).get("vcs_info", {}).get("commit_id") if direct_url else None
    if revision != job["expected_rabbit_revision"]:
        raise FitError(f"Rabbit revision {revision!r} differs from validated {job['expected_rabbit_revision']}; run scripts/setup_rabbit.sh")
    versions["rabbit_revision"] = revision
    model = json.loads(Path(job["model_cache"]).read_text())["model"]
    compatibility = job.get("likelihood", "native") == "combine"
    if compatibility:
        model["likelihood"] = "combine"
    model_path = target / "model.json"
    model_path.write_text(json.dumps(model, allow_nan=False) + "\n")
    from .tensor import write_tensor
    tensor_start = time.perf_counter()
    print("Writing native Rabbit tensor", flush=True)
    tensor_path = write_tensor(model, target / "input.hdf5", sparse=job["sparse"],
                               zero_shape_policy=job.get("zero_shape_policy", "error"),
                               likelihood=job.get("likelihood", "native"))
    tensor_seconds = time.perf_counter() - tensor_start
    print(f"Native Rabbit tensor ready in {tensor_seconds:.2f} s", flush=True)

    records = free_parameters(model)
    internal = {p["parameter"]: f"rdf_internal_{i}" for i, p in enumerate(records)}
    nuisance_names = {e["name"] for e in model["effects"]}
    unknown = set(job["freeze_parameters"]) - set(internal) - nuisance_names
    if unknown:
        raise FitError(f"cannot freeze absent parameters: {sorted(unknown)}")
    mc_stat = job["mc_stat"]
    mc_enabled = model["mc_stat"] if mc_stat == "card" else mc_stat != "none"
    arguments = [str(tensor_path), "-o", str(target), "--outname", "fitresults.hdf5",
                 "-t", "-1" if job["asimov"] else "0", "--unblind",
                 "--paramModel", "statistical_inference.param_models.StructuredModel", str(model_path),
                 "--allowNegativeParam", "--nDevices", str(job["n_devices"]),
                 "--binByBinStatMode", "full" if mc_stat == "full" else "lite",
                 "--binByBinStatType", "normal-additive" if compatibility else "gamma",
                 "--saveHists", "--saveHistsPerProcess",
                 "--verbose", "4", "--noColorLogger", "--minimizerGtol", str(job.get("minimizer_gtol") or 1e-7)]
    if job.get("histogram_errors", True):
        arguments.append("--computeHistErrors")
    else:
        # Histogram chi2 independently builds the same dense observable
        # Jacobian, even when histogram variances themselves are disabled.
        arguments.append("--noChi2")
    if not mc_enabled:
        arguments.append("--noBinByBinStat")
    if job["devices"]:
        arguments.extend(["--devices", *map(str, job["devices"])])
    if job["freeze_parameters"]:
        arguments.extend(["--freezeParameters", *[internal.get(n, n) for n in job["freeze_parameters"]]])
    if job["scan"]:
        arguments.extend(["--scan", *[internal.get(n, n) for n in job["scan"]]])
    if job["impacts"]:
        arguments.extend(["--doImpacts", "--globalImpacts"])
    if job["maxiter"] is not None:
        arguments.extend(["--minimizerMaxiter", str(job["maxiter"])])
    if job.get("minimizer_method"):
        arguments.extend(["--minimizerMethod", job["minimizer_method"]])
    if job.get("hvp_batch") is not None:
        arguments.extend(["--hvpBatch", str(job["hvp_batch"])])
    (target / "native_arguments.json").write_text(json.dumps(arguments, indent=2) + "\n")
    start = time.perf_counter()
    # Avoid upstream's writable-eager-buffer assumption. Construct tensors from
    # owned arrays through the public TensorFlow API in this pinned worker.
    from rabbit import h5pyutils_read, inputdata

    def read_owned_tensor(dataset):
        values = np.asarray(dataset[...])
        shape = dataset.attrs.get("original_shape", dataset.shape)
        return tf.reshape(tf.convert_to_tensor(values), shape)

    h5pyutils_read.maketensor = read_owned_tensor
    inputdata.maketensor = read_owned_tensor
    from bin import rabbit_fit as native_driver
    if compatibility:
        from .combine_compat import install
        install(model, covariance=job.get("covariance", "analytic"), output=target/"combine_covariance.json")
    # Preserve the fitted point even if a later covariance calculation fails.
    # This diagnostic is never a completion record or an uncertainty result.
    from rabbit.fitter import Fitter
    original_minimize = Fitter.minimize

    def record_minimized_point(self, *args, **kwargs):
        result = original_minimize(self, *args, **kwargs)
        raw_names = [n.decode() if isinstance(n, bytes) else str(n) for n in self.parms]
        names, values, _ = restore_parameters(raw_names, self.x.numpy(), np.eye(len(raw_names)), model)
        value, gradient = self.loss_val_grad()
        diagnostic = dict(status="minimized_point_only", model_identity=model["identity"],
                          parameters=dict(zip(names, values.tolist())),
                          nll=float(value), raw_gradient_max=float(np.max(np.abs(gradient.numpy()))),
                          minimizer_status=self.minimizer_status())
        (target / "minimized_point.json").write_text(json.dumps(diagnostic, indent=2, allow_nan=False) + "\n")
        return result

    Fitter.minimize = record_minimized_point
    # Rabbit's native -t -1 computes curvature at the generating point without
    # minimizing. The central fit interface promises an actual fit for either
    # dataset choice, including honest minimizer timing and status for Asimov.
    if job["asimov"]:
        native_fit = native_driver.fit

        def minimize_generated_data(args, fitter, workspace, dofit=True):
            return native_fit(args, fitter, workspace, dofit=not args.noFit)

        native_driver.fit = minimize_generated_data
    sys.argv = ["rabbit_fit", *arguments]
    native_driver.main()
    elapsed = time.perf_counter() - start
    from rabbit.io_tools import get_fitresult
    import h5py
    with h5py.File(target / "fitresults.hdf5", "r") as handle:
        result = get_fitresult(handle)
        h = result["parms"].get()
        raw_names = list(np.asarray(h.axes[0]).astype(str))
        raw_values = h.values()
        raw_covariance = result["cov"].get().values()
        names, values, covariance = restore_parameters(raw_names, raw_values, raw_covariance, model)
        edm = float(result["edmval"])
        status = result.get("minimizer_status")
        if status is None:
            raise FitError("Rabbit output has no minimizer status; an uncertainty-only evaluation is not a completed fit")
        if not np.isfinite(edm) or edm < -1e-10 or edm > 1e-3:
            raise FitError(f"Rabbit did not reach a valid minimum: EDM={edm}, status={status}")
        if not np.all(np.isfinite(values)) or not np.all(np.isfinite(covariance)) or not np.allclose(covariance, covariance.T):
            raise FitError("Rabbit returned non-finite parameters or invalid covariance")
        nll = float(result["nllvalreduced"])
        full_eigenvalues = np.linalg.eigvalsh(covariance)
        if not np.isfinite(nll) or full_eigenvalues[0] < -1e-8 * max(1., full_eigenvalues[-1]):
            raise FitError("Rabbit returned a non-finite likelihood or indefinite full covariance")
        model_names = [p["parameter"] for p in records if p["parameter"] not in job["freeze_parameters"]]
        selected = [names.index(n) for n in model_names]
        model_cov = covariance[np.ix_(selected, selected)]
        eigenvalues = np.linalg.eigvalsh(model_cov)
        if len(eigenvalues) and (eigenvalues[0] <= 0 or np.any(np.diag(model_cov) <= 0)):
            raise FitError(f"Rabbit model covariance is not positive definite: min eigenvalue={eigenvalues[0]}")
        fitted = {n: dict(value=float(v), error=float(np.sqrt(max(0., covariance[i, i]))))
                  for i, (n, v) in enumerate(zip(names, values))}
        pois = [p["parameter"] for p in records if p["is_poi"]]
        poi_indices = [names.index(n) for n in pois]
        near_bounds = []
        for p in records:
            v = fitted[p["parameter"]]["value"]
            if any(b is not None and abs(v - b) < 1e-5 * max(1., abs(b)) for b in (p.get("min"), p.get("max"))):
                near_bounds.append(p["parameter"])
        if compatibility:
            near_bounds.extend(n for n in nuisance_names if n in fitted and abs(fitted[n]["value"]) > 7.-1e-5)
        summary = dict(schema="rdf-fit-result-v1", backend="rabbit", status="rabbit_complete",
                       mode="asimov" if job["asimov"] else "observed", versions=versions,
                       asimov_minimized=bool(job["asimov"]),
                       model_identity=model["identity"], input_tensor=str(tensor_path),
                       hdf5_output=str(target / "fitresults.hdf5"), model=str(model_path),
                       wall_seconds=elapsed, device=job["device"], n_devices=job["n_devices"],
                       timing_seconds=dict(tensor_preparation=tensor_seconds, native_driver=elapsed,
                                           worker_before_readout=time.perf_counter() - worker_start),
                       visible_gpus=[g.name for g in gpus], selected_gpu_indices=job["devices"],
                       tensor_reader="owned NumPy array copied into TensorFlow",
                       sparse_input=job["sparse"],
                       logical_gpus=[d.name for d in tf.config.list_logical_devices("GPU")],
                       native_arguments=arguments, minimizer_status=status, edm=edm,
                       nll=nll, covariance_method=("Combine-style finite stencils; MC-stat Schur complement; physical-coordinate Jacobian"
                                                  if compatibility and job.get("covariance") == "combine" else
                                                  "Rabbit observed Hessian; physical-coordinate Jacobian"),
                       covariance_rank=int(np.linalg.matrix_rank(covariance)),
                       covariance_min_eigenvalue=float(full_eigenvalues[0]),
                       parameters_near_bounds=near_bounds,
                       likelihood=job.get("likelihood", "native"),
                       histogram_errors_computed=job.get("histogram_errors", True),
                       histogram_chi2_computed=job.get("histogram_errors", True),
                       mc_stat=dict(enabled=mc_enabled, mode="full" if mc_stat == "full" else "lite",
                                    distribution="normal-additive" if compatibility else "gamma"),
                       shape_interpolation=("Combine v11 normalized additive shape, separate AsymPow rates" if compatibility
                                            else "asymmetric log-normal; differs from Combine shape morphing"),
                       zero_shape_audit=json.loads(tensor_path.with_suffix(".metadata.json").read_text()),
                       poi_results={n: fitted[n] for n in pois},
                       nuisance_results={n: r for n, r in fitted.items() if n not in pois},
                       covariance_order=pois, covariance=covariance[np.ix_(poi_indices, poi_indices)].tolist(),
                       fit_parameter_covariance_order=model_names, fit_parameter_covariance=model_cov.tolist(),
                       full_covariance_order=names, full_covariance=covariance.tolist(),
                       frozen_parameters=job["freeze_parameters"])
        diagnostics = target/"combine_covariance.json"
        if compatibility and job.get("covariance") == "combine" and diagnostics.is_file():
            summary["covariance_diagnostics"] = str(diagnostics)
        histograms = {}
        for channel, entries in result["mappings"]["BaseMapping"]["channels"].items():
            converted = {}
            for key, native_key in (("prefit", "hist_prefit_inclusive"),
                                    ("postfit", "hist_postfit_inclusive"),
                                    ("data", "hist_nobs"), ("observed", "hist_data_obs")):
                native = entries[native_key].get()
                converted[key] = native.values().tolist()
                variance = native.variances()
                converted[key + "_variance"] = (np.zeros_like(native.values()) if variance is None else variance).tolist()
            histograms[channel] = converted
        hist_path = target / "histograms.json"
        hist_path.write_text(json.dumps(dict(model_identity=model["identity"], channels=histograms,
                                            parameter_uncertainties_computed=job.get("histogram_errors", True)),
                                       allow_nan=False) + "\n")
        summary["histograms"] = str(hist_path)
    (target / "fit_result.json").write_text(json.dumps(summary, indent=2, allow_nan=False) + "\n")
    # Publish completion only after validating the fresh native output.
    (target / "complete.json").write_text(json.dumps(dict(model_identity=model["identity"], status="rabbit_complete")) + "\n")


if __name__ == "__main__":
    main()
