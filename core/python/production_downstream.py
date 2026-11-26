"""Compatibility entry point for :mod:`rdfproduction.downstream`."""

import sys

from rdfproduction import downstream as _implementation

if __name__ == "__main__":
    raise SystemExit(_implementation.main())

sys.modules[__name__] = _implementation
