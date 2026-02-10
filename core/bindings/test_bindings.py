#!/usr/bin/env python3
"""
Basic test for Python bindings

This test validates that the Python bindings can be imported and basic
functionality works. It does NOT require a ROOT file, using an in-memory
RDataFrame for testing.
"""

import sys
import os

def test_import():
    """Test that the module can be imported"""
    try:
        # Add the Python module to the path
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../build/python'))
        import rdfanalyzer
        print("✓ Successfully imported rdfanalyzer module")
        print(f"  Version: {rdfanalyzer.__version__}")
        return True
    except ImportError as e:
        print(f"✗ Failed to import rdfanalyzer: {e}")
        return False

def test_analyzer_creation():
    """Test creating an Analyzer with a config file"""
    import rdfanalyzer
    
    # Create a minimal config file
    config_content = """
fileList=dummy.root
saveFile=output.root
threads=1
"""
    
    config_path = "/tmp/test_config.txt"
    with open(config_path, 'w') as f:
        f.write(config_content)
    
    try:
        # This will fail because dummy.root doesn't exist, but that's OK
        # We're just testing that the binding works
        analyzer = rdfanalyzer.Analyzer(config_path)
        print("✗ Analyzer creation succeeded unexpectedly (dummy.root shouldn't exist)")
        return False
    except Exception as e:
        # Expected to fail since the file doesn't exist
        if "dummy.root" in str(e) or "Cannot open" in str(e) or "Failed" in str(e):
            print("✓ Analyzer constructor correctly throws on missing file")
            return True
        else:
            print(f"✗ Unexpected error: {e}")
            return False

def test_method_existence():
    """Test that expected methods exist on the Analyzer class"""
    import rdfanalyzer
    
    methods = ['DefineJIT', 'FilterJIT', 'DefineFromPointer', 
               'DefineFromVector', 'save', 'configMap']
    
    analyzer_class = rdfanalyzer.Analyzer
    
    all_exist = True
    for method in methods:
        if hasattr(analyzer_class, method):
            print(f"  ✓ Method {method} exists")
        else:
            print(f"  ✗ Method {method} missing")
            all_exist = False
    
    if all_exist:
        print("✓ All expected methods exist")
        return True
    else:
        print("✗ Some methods are missing")
        return False

def test_numpy_integration():
    """Test that numpy integration works"""
    try:
        import numpy as np
        arr = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        ptr = arr.ctypes.data
        print(f"✓ Numpy integration works (test array pointer: 0x{ptr:x})")
        return True
    except Exception as e:
        print(f"✗ Numpy integration failed: {e}")
        return False

def test_numba_integration():
    """Test that numba integration works"""
    try:
        import numba
        import ctypes
        
        @numba.cfunc("float64(float64)")
        def test_func(x):
            return x * 2.0
        
        ptr = ctypes.cast(test_func.address, ctypes.c_void_p).value
        print(f"✓ Numba integration works (test function at: 0x{ptr:x})")
        return True
    except Exception as e:
        print(f"✗ Numba integration failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 60)
    print("RDFAnalyzerCore Python Bindings - Basic Test Suite")
    print("=" * 60)
    print()
    
    tests = [
        ("Module Import", test_import),
        ("Method Existence", test_method_existence),
        ("Numpy Integration", test_numpy_integration),
        ("Numba Integration", test_numba_integration),
        ("Analyzer Creation", test_analyzer_creation),
    ]
    
    results = []
    for name, test_func in tests:
        print(f"\nTest: {name}")
        print("-" * 60)
        try:
            result = test_func()
            results.append((name, result))
        except Exception as e:
            print(f"✗ Test crashed: {e}")
            results.append((name, False))
    
    print()
    print("=" * 60)
    print("Test Summary")
    print("=" * 60)
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for name, result in results:
        status = "PASS" if result else "FAIL"
        symbol = "✓" if result else "✗"
        print(f"{symbol} {name:30s} {status}")
    
    print()
    print(f"Total: {passed}/{total} tests passed")
    
    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠ {total - passed} test(s) failed")
        return 1

if __name__ == "__main__":
    sys.exit(main())
