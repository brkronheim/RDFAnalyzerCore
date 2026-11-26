"""Compatibility entry point for :mod:`rdfproduction.retry`."""

import sys

from rdfproduction import retry as _implementation

sys.modules[__name__] = _implementation
