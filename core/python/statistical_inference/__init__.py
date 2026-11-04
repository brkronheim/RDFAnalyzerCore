"""Optional statistical backends; importing this package does not load TensorFlow."""
from .errors import FitError
from .results import fit_section

__all__ = ["FitError", "fit_section"]
