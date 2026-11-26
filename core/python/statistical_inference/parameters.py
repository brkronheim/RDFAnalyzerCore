"""Bounded model coordinates and covariance conversion, independent of TensorFlow."""
import math
import numpy as np


def encode(value, record):
    lo, hi = record.get("min"), record.get("max")
    if lo is not None and hi is not None:
        return math.log((value - lo) / (hi - value))
    if lo is not None:
        return math.log(value - lo)
    if hi is not None:
        return math.log(hi - value)
    return value


def decode(raw, record):
    lo, hi = record.get("min"), record.get("max")
    if lo is not None and hi is not None:
        z = math.exp(-abs(raw))
        fraction = 1 / (1 + z) if raw >= 0 else z / (1 + z)
        return lo + (hi - lo) * fraction, (hi - lo) * fraction * (1 - fraction)
    if lo is not None:
        return lo + math.exp(raw), math.exp(raw)
    if hi is not None:
        return hi - math.exp(raw), -math.exp(raw)
    return raw, 1.


def free_parameters(model):
    return [p for p in model["parameters"] if not p.get("constant")]


def restore_parameters(names, values, covariance, model):
    """Return physical names/values/covariance with the complete Jacobian."""
    records = {f"rdf_internal_{i}": p for i, p in enumerate(free_parameters(model))}
    values = np.array(values, dtype=float, copy=True)
    jacobian = np.ones(len(names))
    translated = list(names)
    for i, name in enumerate(names):
        if name in records:
            record = records[name]
            translated[i] = record["parameter"]
            values[i], jacobian[i] = decode(values[i], record)
        elif model.get("likelihood") == "combine":
            fraction = np.tanh(values[i]/7.)
            values[i], jacobian[i] = 7.*fraction, 1.-fraction*fraction
    covariance = np.asarray(covariance, dtype=float) * np.outer(jacobian, jacobian)
    return translated, values, covariance
