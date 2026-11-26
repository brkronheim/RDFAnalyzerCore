"""Compatibility entry point for :mod:`rdfproduction.manifest_plotting`."""

import sys

from rdfproduction import manifest_plotting as _implementation

sys.modules[__name__] = _implementation
