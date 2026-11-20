"""Read current backend-neutral and historical analysis fit summaries."""
from .errors import FitError


def read_fit_histograms(result, *, require_uncertainties=True):
    """Read validated Rabbit pre/postfit predictions without importing TensorFlow."""
    import json
    import numpy as np
    from pathlib import Path
    from .model import _file_digest
    fit = fit_section(result)
    if require_uncertainties and not fit.get("histogram_errors_computed", True):
        raise FitError("saved per-bin histogram errors were disabled; use values-only reading or the full parameter covariance for function bands")
    model = json.loads(Path(fit["model"]).read_text())
    if model["identity"] != fit["model_identity"] or any(
            _file_digest(source) != digest for source, digest in model["sources"].items()):
        raise FitError("saved fit inputs have changed; refit before plotting")
    path = Path(fit["histograms"])
    payload = json.loads(path.read_text())
    if payload["model_identity"] != fit["model_identity"]:
        raise FitError("fit histograms belong to a different prepared model")
    return path, {ch: {k: np.asarray(v, dtype=float) for k, v in entries.items()}
                  for ch, entries in payload["channels"].items()}


def fit_section(result, default=None):
    """Return the fitted parameter section without guessing a different backend."""
    backend = result.get("backend")
    if backend in ("rabbit", "combine") and isinstance(result.get(backend), dict):
        return result[backend]
    if isinstance(result.get("fit"), dict):
        return result["fit"]
    # Historical vjet results did not consistently record the top-level backend.
    if isinstance(result.get("combine"), dict):
        return result["combine"]
    if default is not None:
        return default
    raise FitError("result has no supported fitted parameter section")
