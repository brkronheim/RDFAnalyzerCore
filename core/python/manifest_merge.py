"""Compatibility entry point for :mod:`rdfproduction.manifest_merge`."""

import sys

from rdfproduction import manifest_merge as _implementation

sys.modules[__name__] = _implementation
