"""Execute Rabbit in its optional, isolated CPU/CUDA Python environment."""
from __future__ import annotations
import json
import os
from pathlib import Path
import subprocess
import sys

from .errors import FitError
from .model import cached_model


RABBIT_REVISION = "a0e8323a1f6489abc5d25b6c1c90933ccee55deb"


def rabbit_python(requested=None):
    candidate = requested or os.environ.get("RDF_RABBIT_PYTHON")
    if not candidate:
        local = Path(__file__).resolve().parents[3] / ".venv-rabbit/bin/python"
        candidate = str(local) if local.is_file() else sys.executable
    path = Path(candidate).expanduser().absolute()
    if not path.is_file():
        raise FitError(f"Rabbit Python does not exist: {path}; run scripts/setup_rabbit.sh")
    return str(path)


def run_rabbit(card_info, output_dir, asimov=False, *, python=None, device="gpu",
               devices=None, n_devices=1, freeze_parameters=(), mc_stat="card",
               sparse=True, scan=(), impacts=False, maxiter=None, zero_shape_policy="error", hvp_batch=None,
               likelihood="native", covariance="auto", histogram_errors=True, template_reader="root",
               minimizer_method=None, minimizer_gtol=None):
    """Fit a strict binned model and return a backend-neutral parameter summary."""
    if device not in ("cpu", "gpu") or n_devices < 1:
        raise FitError("Rabbit requires device cpu/gpu and n-devices >= 1")
    devices = list(devices or [])
    if device == "cpu" and (n_devices != 1 or devices):
        raise FitError("CPU execution requires n-devices=1 and no GPU indices")
    if devices and (len(devices) != n_devices or len(set(devices)) != len(devices) or min(devices) < 0):
        raise FitError("GPU indices must be distinct, nonnegative, and match n-devices")
    if impacts and n_devices > 1:
        raise FitError("Rabbit impacts currently require one device")
    if mc_stat not in ("card", "none", "lite", "full"):
        raise FitError("Rabbit mc-stat must be card, none, lite, or full")
    if likelihood not in ("native", "combine"):
        raise FitError("Rabbit likelihood must be native or combine")
    if covariance not in ("auto", "analytic", "combine") or (covariance == "combine" and likelihood != "combine"):
        raise FitError("Combine covariance requires the compatible likelihood; choose auto/analytic/combine")
    covariance = ("combine" if likelihood == "combine" else "analytic") if covariance == "auto" else covariance
    if likelihood == "combine" and (mc_stat not in ("card", "none") or zero_shape_policy != "error"):
        raise FitError("Combine compatibility requires mc-stat card/none and no native zero-shape floor")
    if zero_shape_policy not in ("error", "native"):
        raise FitError("Rabbit zero-shape policy must be error or native")
    if hvp_batch is not None and hvp_batch < 1:
        raise FitError("Rabbit Hessian-vector batch must be positive")
    if maxiter is not None and maxiter <= 0:
        raise FitError("Rabbit maxiter must be positive")
    if minimizer_method not in (None, "trust-krylov", "trust-exact", "BFGS", "L-BFGS-B", "trust-ncg"):
        raise FitError("unsupported Rabbit minimizer method")
    if minimizer_gtol is not None and not (0 < minimizer_gtol < float("inf")):
        raise FitError("Rabbit gradient tolerance must be finite and positive")
    cards = card_info.get("datacards") or [card_info["datacard"]]
    if likelihood == "combine":
        mc_policies = []
        for card in cards:
            lines = [line.split("#", 1)[0].split() for line in Path(card).read_text().splitlines()]
            rates = next(tokens[1:] for tokens in lines if tokens[:1] == ["rate"])
            if any(float(value) != -1. for value in rates):
                raise FitError("Combine compatibility currently requires template-derived rates (-1)")
            mc_policies.append(any(tokens[1:2] == ["autoMCStats"] for tokens in lines))
        if len(set(mc_policies)) > 1:
            raise FitError("Combine compatibility requires the same autoMCStats policy across cards")
    dataset = "asimov" if asimov else "observed"
    target = Path(output_dir).resolve() / "rabbit" / (dataset+"_combine" if likelihood == "combine" else dataset)
    target.mkdir(parents=True, exist_ok=True)
    completion = target / "complete.json"
    completion.unlink(missing_ok=True)
    (target / "combine_covariance.json").unlink(missing_ok=True)
    cache_path = target.parent / "prepared_model.json"
    print(f"Preparing Rabbit model from {len(cards)} card(s); validating cache {cache_path}", flush=True)
    model = cached_model(cards, cache_path, poi_map=card_info.get("poi_map"), template_reader=template_reader)
    print(f"Rabbit model: {len(model['channels'])} channels, {len(model['parameters'])} model parameters, "
          f"{len(model['effects'])} nuisance effects; running on {n_devices} {device} device(s)", flush=True)
    job = dict(model_cache=str(cache_path), asimov=bool(asimov), device=device, devices=devices,
               n_devices=n_devices, freeze_parameters=list(freeze_parameters),
               mc_stat=mc_stat, sparse=sparse and device == "cpu" and likelihood == "native", scan=list(scan), impacts=impacts,
               likelihood=likelihood,
               covariance=covariance,
               zero_shape_policy=zero_shape_policy,
               hvp_batch=hvp_batch,
               histogram_errors=bool(histogram_errors),
               minimizer_method=minimizer_method,
               minimizer_gtol=minimizer_gtol,
               maxiter=maxiter, expected_rabbit_revision=RABBIT_REVISION)
    job_path = target / "job.json"
    job_path.write_text(json.dumps(job, allow_nan=False) + "\n")
    environment = os.environ.copy()
    core_python = str(Path(__file__).resolve().parents[1])
    environment["PYTHONPATH"] = os.pathsep.join(filter(None, [core_python, environment.get("PYTHONPATH")]))
    if device == "cpu":
        environment["CUDA_VISIBLE_DEVICES"] = "-1"
    environment.setdefault("TF_FORCE_GPU_ALLOW_GROWTH", "true")
    environment.setdefault("TF_CPP_MIN_LOG_LEVEL", "1")
    environment.setdefault("TF_NUM_INTRAOP_THREADS", "8")
    environment.setdefault("TF_NUM_INTEROP_THREADS", "2")
    command = [rabbit_python(python), "-m", "statistical_inference.worker", str(job_path)]
    (target / "command.json").write_text(json.dumps(command, indent=2) + "\n")
    log = target / "rabbit.log"
    with log.open("w") as stream:
        proc = subprocess.run(command, stdout=stream, stderr=subprocess.STDOUT,
                              env=environment, cwd=target, check=False)
    if proc.returncode or not completion.is_file():
        tail = "\n".join(log.read_text(errors="replace").splitlines()[-35:])
        raise FitError(f"Rabbit failed (exit {proc.returncode}); see {log}\n{tail}")
    result = json.loads((target / "fit_result.json").read_text())
    result.update(command=command, log=str(log), command_file=str(target / "command.json"))
    return result


def add_rabbit_options(parser):
    parser.add_argument("--rabbit-python", help="Rabbit interpreter (default: RDF_RABBIT_PYTHON or .venv-rabbit)")
    parser.add_argument("--rabbit-template-reader", choices=("root", "uproot"), default="root",
                        help="reader for uncached conversion (default: root); requires PyROOT in the preparing interpreter")
    parser.add_argument("--rabbit-device", choices=("cpu", "gpu"), default="gpu")
    parser.add_argument("--rabbit-devices", type=lambda s: [int(v) for v in s.split(",")],
                        help="comma-separated GPU indices, within CUDA_VISIBLE_DEVICES")
    parser.add_argument("--rabbit-n-devices", type=int, default=1,
                        help="number of GPUs for a bins-sharded fit (e.g. 4)")
    parser.add_argument("--rabbit-mc-stat", choices=("card", "none", "lite", "full"), default="card",
                        help="card enables Rabbit lite/gamma when autoMCStats is present; modes are not asserted equivalent to Combine")
    parser.add_argument("--rabbit-maxiter", type=int)
    parser.add_argument("--rabbit-minimizer-method", choices=("trust-krylov", "trust-exact", "BFGS", "L-BFGS-B", "trust-ncg"),
                        help="explicit Rabbit minimizer for controlled numerical comparisons; default is Rabbit's trust-krylov")
    parser.add_argument("--rabbit-minimizer-gtol", type=float,
                        help="explicit raw-coordinate gradient tolerance (default 1e-7); compare EDM and parameter stability")
    parser.add_argument("--rabbit-likelihood", choices=("native", "combine"), default="native",
                        help="combine reproduces Combine v11 template morphing and Gaussian autoMCStats")
    parser.add_argument("--rabbit-covariance", choices=("auto", "analytic", "combine"), default="auto",
                        help="auto uses finite Combine-style stencils for compatible mode, autodiff otherwise")
    parser.add_argument("--rabbit-zero-shape-policy", choices=("error", "native"), default="error",
                        help="native uses Rabbit's 1e-3 relative floor for zero-valued shifted templates; recorded in fit metadata")
    parser.add_argument("--rabbit-hvp-batch", type=int,
                        help="directions per native Hessian-vector batch; 1 uses sequential products and limits tracing/memory")
    parser.add_argument("--rabbit-no-histogram-errors", action="store_true",
                        help="omit optional histogram-error/chi2 Jacobians; retain the complete fit-parameter covariance")


def options_from_args(args):
    return dict(python=args.rabbit_python, device=args.rabbit_device, devices=args.rabbit_devices,
                n_devices=args.rabbit_n_devices, mc_stat=args.rabbit_mc_stat, maxiter=args.rabbit_maxiter,
                zero_shape_policy=args.rabbit_zero_shape_policy, hvp_batch=args.rabbit_hvp_batch,
                likelihood=getattr(args, "rabbit_likelihood", "native"),
                covariance=getattr(args, "rabbit_covariance", "auto"),
                histogram_errors=not getattr(args, "rabbit_no_histogram_errors", False),
                template_reader=getattr(args, "rabbit_template_reader", "root"),
                minimizer_method=getattr(args, "rabbit_minimizer_method", None),
                minimizer_gtol=getattr(args, "rabbit_minimizer_gtol", None))
