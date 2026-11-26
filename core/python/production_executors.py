"""Compatibility entry point for :mod:`rdfproduction.executors`."""

import sys

from rdfproduction import executors as _implementation

sys.modules[__name__] = _implementation
