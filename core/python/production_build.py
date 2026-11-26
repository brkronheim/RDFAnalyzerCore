"""Compatibility entry point for :mod:`rdfproduction.build`."""

import sys

from rdfproduction import build as _implementation

sys.modules[__name__] = _implementation
