"""Compatibility entry point for :mod:`rdfproduction.condor_submission`."""

import sys

from rdfproduction import condor_submission as _implementation

sys.modules[__name__] = _implementation
