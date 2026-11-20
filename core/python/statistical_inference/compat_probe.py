"""Evaluate the compatible Rabbit likelihood for independent RooFit validation.

Run in the Rabbit environment, in a separate process from PyROOT. Inputs are
an existing compatible fit directory and a JSON list of physical parameter
points. This evaluates predictions and derivatives, without fitting.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import tensorflow as tf

from .combine_compat import _components, install
from .parameters import free_parameters


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("points", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--hessian", action="store_true")
    parser.add_argument("--covariance-stencils", type=Path,
                        help="validation only: evaluate covariance with explicit physical finite-difference nodes")
    args = parser.parse_args()
    model = json.loads((args.directory/"model.json").read_text())
    if model.get("likelihood") != "combine":
        raise ValueError("Probe requires a compatible fit's model.json")
    from rabbit import inputdata, h5pyutils_read
    def tensor(dataset):
        return tf.reshape(tf.convert_to_tensor(np.asarray(dataset[...])),
                          dataset.attrs.get("original_shape", dataset.shape))
    inputdata.maketensor = h5pyutils_read.maketensor = tensor
    from bin.rabbit_fit import make_parser
    from rabbit.fitter import Fitter
    from .param_models import StructuredModel
    install(model)
    filename = str(args.directory/"input.hdf5")
    options = make_parser().parse_args([filename, "--unblind", "--binByBinStatType", "normal-additive"]+
                                      ([] if model["mc_stat"] else ["--noBinByBinStat"]))
    data = inputdata.FitInputData(filename)
    fitter = Fitter(data, StructuredModel(data, str(args.directory/"model.json")), options)
    records = free_parameters(model)
    nuisance_names = [n.decode() if isinstance(n, bytes) else str(n) for n in data.systs]
    names = [p["parameter"] for p in records]+nuisance_names
    defaults = np.array([p["initial"] for p in records]+[0.]*len(nuisance_names))

    def raw(physical):
        values = []
        for i, record in enumerate(records):
            v = physical[i]
            lo, hi = record.get("min"), record.get("max")
            if lo is not None and hi is not None:
                v = tf.math.log((v-lo)/(hi-v))
            elif lo is not None:
                v = tf.math.log(v-lo)
            elif hi is not None:
                v = tf.math.log(hi-v)
            values.append(v)
        values.extend(tf.unstack(7.*tf.math.atanh(physical[len(records):]/7.)))
        return tf.stack(values)

    # Use the compatible model's exact nominal Asimov counts in both engines.
    nominal, _ = _components(fitter, raw(tf.constant(defaults))[:, None], True)
    observed = tf.reduce_sum(nominal, axis=1)[:, 0]
    fitter.set_nobs(observed)

    @tf.function
    def evaluate(physical):
        x = raw(physical)[:, None]
        norm, variance = _components(fitter, x, True)
        profiled, _, beta = fitter._compute_yields_with_beta_batched(x)
        with tf.GradientTape() as tape:
            tape.watch(physical)
            nll = fitter._compute_nll_batched(raw(physical)[:, None])[0]
        gradient = tape.gradient(nll, physical)
        return (tf.reduce_sum(norm, axis=1)[:, 0],
                tf.reduce_sum(variance, axis=1)[:, 0], profiled[:, 0], nll, gradient)

    if args.covariance_stencils:
        from .combine_covariance import finite_covariance
        stencil_record = json.loads(args.covariance_stencils.read_text())
        if stencil_record["model_identity"] != model["identity"]:
            raise ValueError("Covariance validation stencils belong to a different model")
        finite_covariance(fitter, model, args.output.with_suffix(".covariance.json"),
                          stencils=stencil_record["stencils"])
    output = dict(parameter_order=names, observations=observed.numpy().tolist(), points=[],
                  channel_ranges={name: dict(start=int(info["start"]), stop=int(info["stop"]))
                                  for name, info in data.channel_info.items()})
    for point in json.loads(args.points.read_text()):
        values = defaults.copy()
        for name, value in point.items():
            values[names.index(name)] = value
        if np.any(np.abs(values[len(records):]) >= 7.):
            raise ValueError("Constrained nuisances must be strictly inside [-7, 7]")
        physical = tf.constant(values)
        expected, variance, profiled, nll, gradient = evaluate(physical)
        if not model["mc_stat"]:
            variance = tf.zeros_like(variance)
        entry = dict(parameters=dict(zip(names, values.tolist())),
                     expected=expected.numpy().tolist(), variance=variance.numpy().tolist(),
                     profiled=profiled.numpy().tolist(), nll=float(nll), gradient=gradient.numpy().tolist())
        if args.hessian:
            with tf.GradientTape() as second:
                second.watch(physical)
                with tf.GradientTape() as first:
                    first.watch(physical)
                    value = fitter._compute_nll_batched(raw(physical)[:, None])[0]
                grad = first.gradient(value, physical)
            entry["hessian"] = second.jacobian(grad, physical).numpy().tolist()
        output["points"].append(entry)
    args.output.write_text(json.dumps(output, indent=2, allow_nan=False)+"\n")


if __name__ == "__main__":
    main()
