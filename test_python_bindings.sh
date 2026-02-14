#!/bin/bash
# Script to test Python bindings build
# Run this after building the project with CMake

set -e  # Exit on error

echo "=========================================="
echo "Python Bindings Build Test"
echo "=========================================="
echo ""

# Check if ROOT is available
if ! command -v root &> /dev/null; then
    echo "ERROR: ROOT not found in PATH"
    echo "Please source env.sh or thisroot.sh first"
    exit 1
fi

echo "✓ ROOT found: $(root-config --version)"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "ERROR: Python3 not found"
    exit 1
fi

echo "✓ Python found: $(python3 --version)"
echo ""

# Check for required Python packages
echo "Checking Python packages..."
python3 -c "import pybind11; print('  ✓ pybind11:', pybind11.__version__)" || {
    echo "  ✗ pybind11 not found"
    echo "    Install with: pip install pybind11"
    exit 1
}

python3 -c "import numpy; print('  ✓ numpy:', numpy.__version__)" || {
    echo "  ✗ numpy not found"
    echo "    Install with: pip install numpy"
    exit 1
}

python3 -c "import numba; print('  ✓ numba:', numba.__version__)" || {
    echo "  ✗ numba not found"
    echo "    Install with: pip install numba"
    exit 1
}

echo ""

# Check if the module was built
MODULE_PATH="build/python/rdfanalyzer*.so"
if ls $MODULE_PATH 1> /dev/null 2>&1; then
    MODULE=$(ls $MODULE_PATH | head -1)
    echo "✓ Python module found: $MODULE"
else
    echo "✗ Python module not found in build/python/"
    echo "  Expected: build/python/rdfanalyzer*.so"
    echo ""
    echo "  The module may not have been built. Common reasons:"
    echo "  1. pybind11 was not available during CMake configuration"
    echo "  2. Python development headers not found"
    echo "  3. Build failed - check build logs"
    echo ""
    echo "  To rebuild with Python bindings:"
    echo "  1. Ensure pybind11 is installed: pip install pybind11"
    echo "  2. Clean and rebuild: rm -rf build && cmake -S . -B build && cmake --build build"
    exit 1
fi

echo ""

# Try to import the module
echo "Testing module import..."
python3 -c "import sys; sys.path.insert(0, 'build/python'); import rdfanalyzer; print('✓ Successfully imported rdfanalyzer module')" || {
    echo "✗ Failed to import module"
    echo "  This may indicate:"
    echo "  - Missing ROOT libraries in LD_LIBRARY_PATH"
    echo "  - ABI incompatibility"
    echo "  - Missing dependencies"
    exit 1
}

echo ""

# Run the test suite
echo "Running test suite..."
python3 core/test/test_python_bindings.py

echo ""
echo "=========================================="
echo "Build test complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo "1. Try the examples in examples/ directory"
echo "2. Read the documentation in docs/PYTHON_BINDINGS.md"
echo "3. Check examples/README.md for usage instructions"
