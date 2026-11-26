"""Compatibility entry point for :mod:`rdfproduction.artifacts`."""

import sys

from rdfproduction import artifacts as _implementation

sys.modules[__name__] = _implementation
