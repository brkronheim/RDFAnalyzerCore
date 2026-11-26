"""Production planning, execution, and artifact services for RDFAnalyzerCore."""

from pathlib import Path

PYTHON_ROOT = Path(__file__).resolve().parents[1]
SOURCE_LAYOUT = (PYTHON_ROOT / "controller/CMakeLists.txt").is_file()
REPOSITORY_ROOT = PYTHON_ROOT.parents[1] if SOURCE_LAYOUT else PYTHON_ROOT.parent
