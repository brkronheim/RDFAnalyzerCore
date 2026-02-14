#!/usr/bin/env python3
"""Compatibility wrapper for relocated Python bindings test."""

import subprocess
import sys
from pathlib import Path


def main() -> int:
    test_path = Path(__file__).resolve().parents[1] / "test" / "test_python_bindings.py"
    return subprocess.call([sys.executable, str(test_path)])


if __name__ == "__main__":
    sys.exit(main())
