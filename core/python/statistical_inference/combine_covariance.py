"""Finite-stencil covariance for Combine-compatible clipped likelihoods.

Evaluate the unprofiled Gaussian MC-stat likelihood, then eliminate its
diagonal MC-stat block by a Schur complement. This is the same full-block
calculation as robustHesse without evaluating identically zero cross-bin
MC-stat derivatives. All predictions are evaluated by Rabbit/TensorFlow.
"""
import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import tensorflow as tf

from .combine_compat import _components
from .errors import FitError
from .parameters import free_parameters, restore_parameters


def finite_covariance(fitter, model, output, batch_size=16, *, stencils=None):
    from rabbit.fitter import Fitter
    from rabbit.blinding import BlindingView

    records = free_parameters(model)
    names = [n.decode() if isinstance(n, bytes) else str(n) for n in fitter.parms]
    names, point, jac_cov = restore_parameters(names, fitter.x.numpy(), np.eye(len(names)), model)
    jac = np.sqrt(np.diag(jac_cov))
    by_name = {p["parameter"]: p for p in records}
    for i, name in enumerate(names):
        record = by_name.get(name, {})
        if record.get("min") is None and record.get("max") is not None:
            jac[i] *= -1.
    if np.any(~np.isfinite(jac)) or np.any(jac == 0):
        raise FitError("Compatible covariance requires a nonsingular interior parameter coordinate")
    npar = len(point)
    active = np.flatnonzero(~fitter.frozen_params_mask.numpy())
    if not len(active):
        raise FitError("Compatible covariance requires at least one floating parameter")
    bounds = [(p.get("min"), p.get("max")) for p in records]+[(-7., 7.)]*(npar-len(records))
    observed = fitter.nobs.numpy()
    beta = fitter.bbstat.beta.numpy() if fitter.bbstat.enabled else np.zeros_like(observed)
    beta0 = fitter.bbstat.beta0.numpy() if fitter.bbstat.enabled else np.zeros_like(observed)
    centres = fitter.x0.numpy()[len(records):]
    contexts = []
    if getattr(fitter, "shards", None):
        for shard in fitter.shards:
            context = SimpleNamespace(**shard.__dict__)
            with tf.device(shard.device):
                context.frozen_params_mask = tf.constant(fitter.frozen_params_mask.numpy())
            context.blinding = BlindingView(False)
            for name in ("get_poi", "get_model_nui", "get_theta"):
                setattr(context, name, getattr(Fitter, name).__get__(context))
            contexts.append(context)
    else:
        contexts = [fitter]

    def encode(values):
        encoded = []
        for i, (lo, hi) in enumerate(bounds):
            value = values[i]
            if i >= len(records):
                value = 7.*tf.math.atanh(value/7.)
            elif lo is not None and hi is not None:
                value = tf.math.log((value-lo)/(hi-value))
            elif lo is not None:
                value = tf.math.log(value-lo)
            elif hi is not None:
                value = tf.math.log(hi-value)
            encoded.append(value)
        return tf.stack(encoded)

    @tf.function(input_signature=[tf.TensorSpec([npar, None], tf.float64)])
    def predictions(values):
        x = encode(values)
        means, variances = [], []
        for context in contexts:
            with tf.device(getattr(context, "device", None)):
                norm, var = _components(context, x, True)
                means.append(tf.reduce_sum(norm, axis=1))
                variances.append(tf.reduce_sum(var, axis=1))
        return tf.concat(means, axis=0), tf.concat(variances, axis=0)

    def predict(points):
        values, variances = predictions(tf.constant(np.asarray(points).T))
        return values.numpy().T, variances.numpy().T

    def poisson(mean):
        y = np.maximum(mean, 1e-9)
        return y-observed-observed*np.log(y/np.where(observed > 0, observed, 1.))

    def evaluate(points, components=False):
        mu, var = predict(points)
        sigma = np.sqrt(var) if fitter.bbstat.enabled else np.zeros_like(mu)
        bins = poisson(mu+sigma*beta)+.5*(beta-beta0)**2
        priors = .5*np.sum((np.asarray(points)[:, len(records):]-centres)**2, axis=1)
        value = bins.sum(axis=1)+priors
        return (value, mu, sigma) if components else value

    base, mu0, sigma0 = evaluate([point], True)
    base = float(base[0])
    mu0, sigma0 = mu0[0], sigma0[0]
    target = .1
    # Fixed NLL target makes the numerical convention reproducible, rather
    # than inheriting an optimizer's potentially regularized initial errors.
    offsets = np.zeros((npar, 2))
    for sign_index, sign in enumerate((-1., 1.)):
        lower = np.zeros(npar)
        upper = np.array([min(1., .9*abs((bound[0 if sign < 0 else 1] if
                                        bound[0 if sign < 0 else 1] is not None else
                                        point[i]+sign*100.)-point[i])) for i, bound in enumerate(bounds)])
        remaining = list(active)
        for _ in range(30):
            if not remaining:
                break
            unbracketed = []
            for begin in range(0, len(remaining), batch_size):
                indices = remaining[begin:begin+batch_size]
                points = np.tile(point, (len(indices), 1))
                points[np.arange(len(indices)), indices] += sign*upper[indices]
                loss = evaluate(points)-base
                for i, delta in zip(indices, loss):
                    if not np.isfinite(delta):
                        raise FitError(f"Non-finite covariance stencil for {names[i]}")
                    if delta < target:
                        lower[i] = upper[i]
                        limit = bounds[i][0 if sign < 0 else 1]
                        limit = abs(limit-point[i])*(1.-1e-7) if limit is not None else 1e6
                        upper[i] = min(2*upper[i], limit)
                        if upper[i] <= lower[i]:
                            raise FitError(f"No interior covariance stencil for {names[i]}; use profile intervals near bounds")
                        unbracketed.append(i)
            remaining = unbracketed
        if remaining:
            raise FitError(f"Cannot bracket covariance stencils for {[names[i] for i in remaining]}")
        # Bisection over all coordinates in parallel, strict target tolerance.
        for _ in range(24):
            trial = .5*(lower+upper)
            for begin in range(0, len(active), batch_size):
                indices = active[begin:begin+batch_size]
                points = np.tile(point, (len(indices), 1))
                points[np.arange(len(indices)), indices] += sign*trial[indices]
                loss = evaluate(points)-base
                for i, delta in zip(indices, loss):
                    if delta < target:
                        lower[i] = trial[i]
                    else:
                        upper[i] = trial[i]
            if np.max(upper[active]-lower[active]) < 1e-8:
                break
        offsets[:, sign_index] = sign*.5*(lower+upper)
    offsets = np.column_stack([offsets[:, 0], np.zeros(npar), offsets[:, 1]])

    def supplied(name, default):
        if stencils is None:
            return default
        if name not in stencils:
            raise FitError(f"Explicit covariance stencils lack {name}")
        nodes = np.asarray(stencils[name], dtype=float)
        if nodes.shape != (3,) or not np.all(np.isfinite(nodes)) or not (nodes[0] < 0 == nodes[1] < nodes[2]):
            raise FitError(f"Invalid interior covariance stencil for {name}")
        return nodes

    if stencils is not None:
        for i in active:
            offsets[i] = supplied(names[i], offsets[i])
            lo, hi = bounds[i]
            if (lo is not None and point[i]+offsets[i, 0] <= lo) or (hi is not None and point[i]+offsets[i, 2] >= hi):
                raise FitError(f"Explicit covariance stencil exceeds bounds for {names[i]}")

    def coefficients(nodes, order):
        return np.linalg.solve(np.array([nodes**i for i in range(3)]), np.eye(3)[order]*[1., 1., 2.][order])

    d1 = np.zeros((npar, 3))
    d2 = np.zeros_like(d1)
    for i in active:
        d1[i], d2[i] = coefficients(offsets[i], 1), coefficients(offsets[i], 2)
    singles = np.zeros((npar, 3))
    for slot in (0, 2):
        for begin in range(0, len(active), batch_size):
            ids = active[begin:begin+batch_size]
            points = np.tile(point, (len(ids), 1))
            points[np.arange(len(ids)), ids] += offsets[ids, slot]
            singles[ids, slot] = evaluate(points)-base
    hessian = np.zeros((npar, npar))
    for i in active:
        hessian[i, i] = d2[i]@singles[i]
    pairs = [(i, j) for ii, i in enumerate(active) for j in active[ii+1:]]
    for begin in range(0, len(pairs), max(1, batch_size//4)):
        chunk = pairs[begin:begin+max(1, batch_size//4)]
        points = []
        for i, j in chunk:
            for si, sj in ((0, 0), (0, 2), (2, 0), (2, 2)):
                p = point.copy(); p[i] += offsets[i, si]; p[j] += offsets[j, sj]; points.append(p)
        values = (evaluate(points)-base).reshape(-1, 4)
        for (i, j), value in zip(chunk, values):
            stencil = np.zeros((3, 3))
            stencil[:, 1] = singles[i]
            stencil[1, :] = singles[j]
            stencil[0, 0], stencil[0, 2], stencil[2, 0], stencil[2, 2] = value
            hessian[i, j] = hessian[j, i] = d1[i]@stencil@d1[j]
        if begin % 1000 == 0:
            print(f"Combine-compatible covariance: {begin}/{len(pairs)} primary pairs", flush=True)
    mc_offsets = None
    if fitter.bbstat.enabled:
        # Per-bin MC-stat directions are independent. Their finite stencils
        # and mixed blocks can be evaluated without rerunning the templates.
        def mc_loss(displacement, mu, sigma):
            return poisson(mu+sigma*(beta+displacement))+.5*(beta+displacement-beta0)**2
        base_bins = mc_loss(0., mu0, sigma0)
        mc_offsets = np.zeros((len(beta), 3))
        for slot, sign in ((0, -1.), (2, 1.)):
            lo, hi = np.zeros_like(beta), np.minimum(1., 7.-sign*beta-1e-7)
            if stencils is None and np.any((hi <= 0) | (mc_loss(sign*hi, mu0, sigma0)-base_bins < target)):
                raise FitError("No interior MC-stat covariance stencil; use profile intervals near Gaussian bounds")
            for _ in range(28):
                mid = (lo+hi)*.5
                delta = mc_loss(sign*mid, mu0, sigma0)-base_bins
                lo, hi = np.where(delta < target, mid, lo), np.where(delta >= target, mid, hi)
            mc_offsets[:, slot] = sign*(lo+hi)*.5
        if stencils is not None:
            for name, info in fitter.indata.channel_info.items():
                for j in range(int(info["stop"])-int(info["start"])):
                    index = int(info["start"])+j
                    mc_offsets[index] = supplied(f"prop_bin{name}_bin{j}", mc_offsets[index])
            if np.any(beta+mc_offsets[:, 0] <= -7.) or np.any(beta+mc_offsets[:, 2] >= 7.):
                raise FitError("Explicit MC-stat covariance stencil exceeds Gaussian bounds")
        mc_d1 = np.array([coefficients(row, 1) for row in mc_offsets])
        mc_d2 = np.array([coefficients(row, 2) for row in mc_offsets])
        mc_singles = np.stack([mc_loss(mc_offsets[:, k], mu0, sigma0)-base_bins for k in range(3)], axis=1)
        diagonal = np.sum(mc_d2*mc_singles, axis=1)
        cross = np.zeros((npar, len(beta)))
        for i in active:
            for slot in (0, 2):
                p = point.copy(); p[i] += offsets[i, slot]
                _, mu, sigma = evaluate([p], True)
                for k in (0, 2):
                    mixed = mc_loss(mc_offsets[:, k], mu[0], sigma[0])-mc_loss(0., mu[0], sigma[0])
                    cross[i] += d1[i, slot]*mc_d1[:, k]*(mixed-mc_singles[:, k])
        hessian -= (cross/diagonal)@cross.T
    selected = hessian[np.ix_(active, active)]
    eigen = np.linalg.eigvalsh(selected)
    if eigen[0] <= 0:
        raise FitError(f"Compatible finite-stencil Hessian is not positive definite: {eigen[0]}")
    cov = np.zeros_like(hessian)
    cov[np.ix_(active, active)] = np.linalg.inv(selected)
    Path(output).write_text(json.dumps(dict(method="Combine-style unprofiled finite stencils and MC-stat Schur complement",
                                          stencil_selection="explicit validation nodes" if stencils is not None else "fixed delta NLL target",
                                          target_delta_nll=target, parameter_order=names, offsets=offsets.tolist(),
                                          active_parameters=[names[i] for i in active],
                                          mc_stat_offsets=None if mc_offsets is None else mc_offsets.tolist(),
                                          mc_stat_beta=beta.tolist() if fitter.bbstat.enabled else None,
                                          covariance=cov.tolist(), minimum_hessian_eigenvalue=float(eigen[0])), indent=2)+"\n")
    return cov/np.outer(jac, jac)
