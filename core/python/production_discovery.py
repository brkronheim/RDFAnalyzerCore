"""Compatibility entry point for :mod:`rdfproduction.discovery`."""

import sys

from rdfproduction import discovery as _implementation

sys.modules[__name__] = _implementation
