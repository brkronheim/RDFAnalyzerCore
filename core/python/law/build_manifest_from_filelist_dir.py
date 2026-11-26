#!/usr/bin/env python3
"""Legacy entry point for python -m rdfproduction.filelist_manifest."""

from rdfproduction.filelist_manifest import _collect_filelists, main, parse_args

if __name__ == "__main__":
    raise SystemExit(main())
