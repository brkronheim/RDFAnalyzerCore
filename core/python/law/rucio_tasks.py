"""Compatibility task name for the optional CMS discovery provider."""
import sys
from pathlib import Path
_core_python = str(Path(__file__).resolve().parents[1])
if _core_python not in sys.path:
    sys.path.insert(0, _core_python)
from optional_extensions import load_extension_module
sys.modules[__name__] = load_extension_module("cms", "rucio_tasks", required=True)
