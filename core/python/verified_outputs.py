"""Compatibility entry point for :mod:`rdfproduction.output_index`."""

import sys

from rdfproduction import output_index as _implementation

sys.modules[__name__] = _implementation
