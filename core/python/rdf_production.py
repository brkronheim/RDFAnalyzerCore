"""Compatibility entry point for :mod:`rdfproduction.cli`."""

import sys

from rdfproduction import controller as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
