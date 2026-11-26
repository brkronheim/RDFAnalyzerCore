"""Validated covariance eigenmodes for calibration uncertainty propagation."""
import numpy as np


def covariance_modes(covariance, *, tolerance=1e-10):
    matrix = np.asarray(covariance, dtype=float)
    if matrix.ndim != 2 or matrix.shape[0] != matrix.shape[1] or not matrix.size:
        raise ValueError("covariance must be a nonempty square matrix")
    if not np.all(np.isfinite(matrix)) or not np.allclose(matrix, matrix.T, rtol=1e-8, atol=1e-10):
        raise ValueError("covariance is non-finite or not symmetric")
    values, vectors = np.linalg.eigh(.5*(matrix+matrix.T))
    if float(values.min()) < -tolerance*max(float(np.abs(values).max()), 1.):
        raise ValueError("covariance has a significantly negative eigenvalue")
    return [(float(value), vectors[:,index]) for index,value in enumerate(values) if value > 0]
