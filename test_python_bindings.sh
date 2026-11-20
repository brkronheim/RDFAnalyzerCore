#!/usr/bin/env bash
# The integration test selects the module path and reports missing prerequisites.
python3 "$(dirname -- "${BASH_SOURCE[0]}")/core/tests/cpp/test_python_bindings.py"
