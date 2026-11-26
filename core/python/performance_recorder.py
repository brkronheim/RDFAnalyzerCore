"""Compatibility entry point for :mod:`rdfproduction.performance`."""

import sys

from rdfproduction import performance as _implementation

sys.modules[__name__] = _implementation
