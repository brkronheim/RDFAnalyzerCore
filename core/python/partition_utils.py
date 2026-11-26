"""Compatibility entry point for :mod:`rdfproduction.partitioning`."""

import sys

from rdfproduction import partitioning as _implementation

sys.modules[__name__] = _implementation
