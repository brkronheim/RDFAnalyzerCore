"""Compatibility entry point for :mod:`rdfproduction.campaign`."""

import sys

from rdfproduction import campaign as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
