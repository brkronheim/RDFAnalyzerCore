"""Native Rabbit model for structured scales; loaded only in the Rabbit runtime."""
import json
import numpy as np
import tensorflow as tf
from rabbit.param_models.param_model import ParamModel
from .parameters import encode, free_parameters


class StructuredModel(ParamModel):
    """Apply explicit rate parameters and normalized polynomial source-bin scales.

    Finite bounds use a smooth bijection so one- and multi-device fits have
    identical models. Results are converted back with the full Jacobian.
    Boundary solutions need profile intervals; Hessian errors there are flagged.
    """

    def __init__(self, indata, config, **kwargs):
        super().__init__(indata)
        with open(config) as stream:
            self.model = json.load(stream)
        self.records = free_parameters(self.model)
        self.npoi = sum(p["is_poi"] for p in self.records)
        self.npou = len(self.records) - self.npoi
        self.params = np.array([f"rdf_internal_{i}".encode() for i in range(len(self.records))])
        self.xparamdefault = tf.constant([encode(p["initial"], p) for p in self.records], dtype=indata.dtype)
        self.allowNegativeParam = True
        self.is_linear = False
        self.procs = [p.decode() if isinstance(p, bytes) else str(p) for p in indata.procs]
        specifications = {}
        for spec in self.model["scales"]:
            specifications.setdefault((spec["channel"], spec["process"]), []).append(
                json.dumps({k: v for k, v in spec.items() if k not in ("channel", "process")}, sort_keys=True))
        presence = {p: [] for p in self.procs}
        for channel in self.model["channels"]:
            for process in channel["processes"]:
                presence[process["name"]].append(channel["name"])
        self.process_channel = {p: channels[0] for p, channels in presence.items() if channels}
        self.global_scales = all(len({tuple(specifications.get((ch, p), [])) for ch in channels}) <= 1
                                 for p, channels in presence.items())

    def compute(self, param, full=False):
        physical = {p["parameter"]: tf.constant(p["initial"], dtype=self.indata.dtype)
                    for p in self.model["parameters"] if p.get("constant")}
        for i, p in enumerate(self.records):
            lo, hi = p.get("min"), p.get("max")
            value = param[i]
            if lo is not None and hi is not None:
                value = lo + (hi - lo) * tf.math.sigmoid(value)
            elif lo is not None:
                value = lo + tf.exp(value)
            elif hi is not None:
                value = hi - tf.exp(value)
            physical[p["parameter"]] = value

        bases = {}
        averages = {}

        def linear(record):
            key = (tuple(record["parameters"]), tuple(record["weights"]))
            if key not in bases:
                bases[key] = tf.add_n([float(w) * physical[n] for n, w in zip(*key)])
            return bases[key]

        scales = {}
        for spec in self.model["scales"]:
            key = (spec["channel"], spec["process"])
            if spec["kind"] == "parameter":
                value = physical[spec["parameter"]]
            elif spec["kind"] == "linear":
                value = linear(spec["record"])
            else:
                rec = spec["record"]
                value = physical[rec["normalization"]] if rec.get("normalization_only") else linear(rec)
                if "normalization" in rec and not rec.get("normalization_only"):
                    poly = self.model["metadata"]["polynomial"]
                    group = rec.get("normalization_group", rec["truth_definition"]["gen_decay"])
                    normalization = poly["normalizations"][group]
                    if group not in averages:
                        averages[group] = tf.add_n([
                            (term["yield"] / normalization["reference_total"]) * linear(poly["processes"][term["process"]])
                            for term in normalization["reference_terms"]])
                    value = physical[rec["normalization"]] * value / averages[group]
            scales[key] = scales[key] * value if key in scales else value
        if self.global_scales:
            row = tf.stack([scales.get((self.process_channel.get(p), p), tf.constant(1., dtype=self.indata.dtype))
                            for p in self.procs])
            return row[None, :]
        blocks = []
        for channel, info in self.indata.channel_info.items():
            if info.get("masked") and not full:
                continue
            row = tf.stack([scales.get((channel, p), tf.constant(1., dtype=self.indata.dtype)) for p in self.procs])
            blocks.append(tf.broadcast_to(row[None, :], [info["stop"] - info["start"], len(self.procs)]))
        return tf.concat(blocks, axis=0)
