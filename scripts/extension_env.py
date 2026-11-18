#!/usr/bin/env python3
"""Print enabled extension environment-script paths for env.sh."""
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core/python"))
from optional_extensions import environment_scripts

for script in environment_scripts():
    print(script)
