"""Compatibility entry point for :mod:`rdfproduction.transport`."""

import sys

from rdfproduction import transport as _implementation

sys.modules[__name__] = _implementation
