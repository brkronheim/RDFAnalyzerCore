"""Combine v11 template semantics for the pinned Rabbit worker.

This module is imported only in the isolated TensorFlow process. It replaces
the yield/profile kernels, leaving Rabbit's minimizer, differentiation, output
and device sharding intact. No RooFit objects enter the Rabbit calculation.
"""
from __future__ import annotations

import numpy as np
import tensorflow as tf

from .errors import FitError


def smooth_step(x):
    """Combine's C2 interpolation, saturated outside [-1, 1]."""
    z = tf.clip_by_value(x, -1., 1.)
    z2 = z*z
    return z*(z2*(3.*z2-10.)+15.)/8.


class CombineTemplates:
    """Dense bin/process kernels and separate process-rate modifiers.

    ShapeTools rebins TH1 templates into TH1F, renormalizes shifted TH1F
    clones for autoMCStats hist-mode 1, and rounds AsymPow constants to six
    decimal places. Reproduce those details, including the 1e-3 rate cutoff.
    """

    def __init__(self, model, indata):
        self.nbins = int(indata.nbinsfull)
        self.nproc = int(indata.nproc)
        self.nsyst = int(indata.nsyst)
        self.histfunc = bool(model["mc_stat"])
        procs = {p.decode() if isinstance(p, bytes) else str(p): i
                 for i, p in enumerate(indata.procs)}
        systs = {s.decode() if isinstance(s, bytes) else str(s): i
                 for i, s in enumerate(indata.systs)}
        shape = (self.nbins, self.nproc)
        nominal = np.zeros(shape)
        variance = np.zeros(shape)
        present = np.zeros(shape, dtype=bool)
        slots = np.zeros(shape, dtype=np.int32)
        totals = np.zeros(shape)
        lookup = {}
        slot_count = 0
        scaled = {(e["channel"], e["process"]) for e in model["scales"]}
        normalized = {(e["channel"], e["process"]) for e in model["effects"] if e["kind"] == "lnN"}
        for channel in model["channels"]:
            bounds = indata.channel_info[channel["name"]]
            rows = slice(int(bounds["start"]), int(bounds["stop"]))
            for process in channel["processes"]:
                values = np.asarray(process["values"])
                total = float(values.sum())
                if total == 0:
                    continue  # Combine excludes zero-rate processes entirely.
                column = procs[process["name"]]
                rounded = values.astype(np.float32).astype(np.float64)
                if not self.histfunc:
                    # ModelTools creates a constant unmodified rate with %g.
                    key = (channel["name"], process["name"])
                    rate_total = total if key in scaled or key in normalized else float(f"{total:g}")
                    rounded *= rate_total/rounded.sum()
                nominal[rows, column] = rounded
                variance[rows, column] = process["variances"]
                present[rows, column] = True
                slots[rows, column] = slot_count
                totals[rows, column] = float(rounded.sum())
                lookup[(channel["name"], process["name"])] = (rows, column, slot_count, values)
                slot_count += 1
        effects = [e for e in model["effects"] if (e["channel"], e["process"]) in lookup]
        qrange = {key: 1. for key in lookup}
        for e in effects:
            if e["kind"] == "shape":
                factor = float(e["factor"])
                if factor <= 0:
                    raise FitError("Combine compatibility requires positive shape coefficients")
                key = (e["channel"], e["process"])
                qrange[key] = min(qrange[key], factor)
        slope = np.zeros((*shape, self.nsyst))
        curved = {}
        rate_avg = np.zeros((slot_count, self.nsyst))
        rate_diff = np.zeros_like(rate_avg)
        for e in effects:
            key = (e["channel"], e["process"])
            rows, column, slot, original = lookup[key]
            index = systs[e["name"]]
            if e["kind"] == "lnN":
                factors = e["factors"]
                low, high = (1./factors[0], factors[0]) if len(factors) == 1 else factors
            else:
                factor = float(e["factor"])
                shifted = []
                for direction in ("down", "up"):
                    values = np.asarray(e[direction]["values"])
                    if values.sum() <= 0:
                        raise FitError(f"Combine rejects zero-integral shifted templates: {e['name']}")
                    rounded = values.astype(np.float32).astype(np.float64)
                    rounded *= nominal[rows, column].sum()/rounded.sum()
                    if self.histfunc:
                        rounded = rounded.astype(np.float32).astype(np.float64)
                    shifted.append(rounded)
                down, up = shifted
                centre = nominal[rows, column]
                slope[rows, column, index] += .5*factor*(up-down)
                ratio = factor/qrange[key]
                if ratio not in curved:
                    curved[ratio] = np.zeros_like(slope)
                curved[ratio][rows, column, index] += .5*factor*(up+down-2*centre)
                low = float(np.sum(e["down"]["values"])/original.sum())
                high = float(np.sum(e["up"]["values"])/original.sum())
                if abs(low-1) < 1e-3 and abs(high-1) < 1e-3:
                    continue
                low, high = (float(f"{value**factor:.6f}") for value in (low, high))
                if min(low, high) <= 0:
                    raise FitError(f"Combine AsymPow constant rounds to zero: {e['name']}")
            rate_avg[slot, index] += .5*(np.log(high)-np.log(low))
            rate_diff[slot, index] += .5*(np.log(high)+np.log(low))
        self.slot_count = slot_count
        # Retain host arrays: multi-device kernels copy only their bin slices.
        self.arrays = dict(nominal=nominal, variance=variance, present=present,
                           slots=slots, totals=totals, slope=slope,
                           curved=curved, rate_avg=rate_avg, rate_diff=rate_diff)
        self.views = {}

    @tf.autograph.experimental.do_not_convert
    def view(self, start, stop, device):
        key = (start, stop, device)
        if key not in self.views:
            a = self.arrays
            with tf.init_scope(), tf.device(device):
                result = {name: tf.constant(a[name][start:stop])
                          for name in ("nominal", "variance", "present", "slots", "totals", "slope")}
                result["curved"] = [(ratio, tf.constant(matrix[start:stop]))
                                    for ratio, matrix in a["curved"].items()]
                result["rate_avg"] = tf.constant(a["rate_avg"])
                result["rate_diff"] = tf.constant(a["rate_diff"])
            self.views[key] = result
        return self.views[key]


def _components(fitter, x, full):
    from rabbit.param_models.param_model import compute_batched

    templates = fitter.combine_templates
    start = int(getattr(fitter.indata, "start", 0))
    stop = int(getattr(fitter.indata, "stop", templates.nbins))
    device = getattr(fitter, "device", None)
    # A non-histfunc PDF normalizes each whole channel after clipping. Its
    # shape must therefore be computed before slicing channels across devices.
    a, b = (start, stop) if templates.histfunc else (0, templates.nbins)
    arrays = templates.view(a, b, device)
    theta = fitter.get_theta(x)
    nominal = arrays["nominal"]
    rows = b-a
    flat_slope = tf.reshape(arrays["slope"], [rows*templates.nproc, templates.nsyst])
    delta = tf.matmul(flat_slope, theta)
    for ratio, matrix in arrays["curved"]:
        delta += tf.matmul(tf.reshape(matrix, [rows*templates.nproc, templates.nsyst]),
                           theta*smooth_step(theta*ratio))
    shape = nominal[..., None] + tf.reshape(delta, [rows, templates.nproc, -1])
    floor = tf.ones_like(nominal)*1e-9 if templates.histfunc else arrays["totals"]*1e-9
    shape = tf.where(arrays["present"][..., None], tf.maximum(shape, floor[..., None]), 0.)
    if not templates.histfunc:
        # slots identify each channel/process. Reduce over bins separately.
        slot = tf.reshape(arrays["slots"], [-1])
        total = tf.math.unsorted_segment_sum(tf.reshape(shape, [rows*templates.nproc, -1]),
                                             slot, templates.slot_count)
        denominator = tf.gather(total, arrays["slots"])
        shape *= tf.math.divide_no_nan(arrays["totals"][..., None], denominator)
        shape = shape[start:stop]
    rate_log = tf.matmul(arrays["rate_avg"], theta)
    rate_log += tf.matmul(arrays["rate_diff"], theta*smooth_step(2.*theta))
    slot = arrays["slots"] if templates.histfunc else arrays["slots"][start:stop]
    rate = tf.exp(tf.gather(rate_log, slot))
    params = tf.concat([fitter.get_poi(x), fitter.get_model_nui(x)], axis=0)
    coefficient = compute_batched(fitter.param_model, params, full)*rate
    norm = shape*coefficient
    variance = arrays["variance"] if templates.histfunc else arrays["variance"][start:stop]
    per_process_variance = variance[..., None]*tf.square(coefficient)
    return norm, per_process_variance


def _no_stat(self, x, full=True, compute_norm=True):
    norm, _ = _components(self, x, full)
    return tf.reduce_sum(norm, axis=1), norm if compute_norm else None


def _with_stat(self, x, data=None, profile=True, compute_norm=False, full=True, beta0=None):
    norm, variance = _components(self, x, full)
    nexp = tf.reduce_sum(norm, axis=1)
    if not self.bbstat.enabled:
        return nexp, norm if compute_norm else None, None
    total_var = tf.reduce_sum(variance, axis=1)
    sigma = tf.where(total_var > 0, tf.sqrt(tf.where(total_var > 0, total_var, 1.)), 0.)
    nobs = self._observed_data()[0] if data is None else data[0]
    centre = self.bbstat.beta0[:, None] if beta0 is None else beta0
    if profile:
        mean = nexp+sigma*centre
        denominator = mean+total_var+tf.sqrt(tf.square(mean-total_var)+4.*total_var*nobs)
        beta = centre+tf.math.divide_no_nan(2.*sigma*(nobs-mean), denominator)
        # Match RooRealVar's supported range for Gaussian autoMCStats terms.
        beta = tf.clip_by_value(beta, -7., 7.)
    else:
        beta = self.bbstat.beta[:, None]+tf.zeros_like(nexp)
    beta += self.bbstat.ubeta[:, None]
    nexp = tf.maximum(nexp+sigma*beta, 1e-9)
    if compute_norm:
        # CMSHistFuncWrapper allocates a bin's fluctuation by variance share.
        norm += tf.math.divide_no_nan(variance, sigma[:, None, :])*beta[:, None, :]
    return nexp, norm if compute_norm else None, beta


def install(model, *, covariance="analytic", output=None):
    """Install version-pinned kernels inside this worker process only."""
    from rabbit.fitter import Fitter
    from rabbit.sharding import MultiDeviceFitter

    original_init = Fitter._init_logk_scaled
    original_shards = MultiDeviceFitter._build_shards
    original_theta = Fitter.get_theta
    original_minimize = Fitter.minimize

    def theta(self, x=None):
        return 7.*tf.math.tanh(original_theta(self, x)/7.)

    def initialize(self):
        original_init(self)
        if not hasattr(self, "combine_templates"):
            self.combine_templates = CombineTemplates(model, self.indata)

    def shards(self):
        original_shards(self)
        for shard in self.shards:
            shard.combine_templates = self.combine_templates
            # Place slices before tracing; never copy full tensors to a GPU.
            self.combine_templates.view(shard.indata.start, shard.indata.stop, shard.device)

    Fitter._init_logk_scaled = initialize
    Fitter.get_theta = theta
    Fitter._compute_yields_noBBB_batched = _no_stat
    Fitter._compute_yields_with_beta_batched = _with_stat
    MultiDeviceFitter._build_shards = shards
    if covariance == "combine":
        def minimize(self, *args, **kwargs):
            # A later minimization (scan/impact/refit) must evaluate its own
            # local derivatives rather than a previous postfit covariance.
            if hasattr(self, "_rdf_compat_native_hessian"):
                self.loss_val_grad_hess = self._rdf_compat_native_hessian
            result = original_minimize(self, *args, **kwargs)
            native_hessian = self.loss_val_grad_hess
            self._rdf_compat_native_hessian = native_hessian
            cached = {}
            def postfit_hessian(*args, **kwargs):
                profile = kwargs.get("profile", args[0] if args else True)
                if not profile or self.param_model.nparams != len([p for p in model["parameters"] if not p.get("constant")]):
                    return native_hessian(*args, **kwargs)
                if not cached:
                    from .combine_covariance import finite_covariance
                    cov = finite_covariance(self, model, output)
                    active = np.flatnonzero(~self.frozen_params_mask.numpy())
                    hess = np.eye(len(cov))
                    hess[np.ix_(active, active)] = np.linalg.inv(cov[np.ix_(active, active)])
                    cached["hessian"] = tf.constant(hess)
                value, grad = self.loss_val_grad()
                return value, grad, cached["hessian"]
            self.loss_val_grad_hess = postfit_hessian
            return result
        Fitter.minimize = minimize
