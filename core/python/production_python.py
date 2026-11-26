"""Compatibility entry point for :mod:`rdfproduction.python_runtime`."""

import sys

from rdfproduction import python_runtime as _implementation

sys.modules[__name__] = _implementation
