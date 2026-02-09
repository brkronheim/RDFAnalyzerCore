#!/usr/bin/env python3
"""
Test script to verify YAML config parsing functionality
"""

import sys
import os
import tempfile

# Add the python directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python'))

from submission_backend import (
    read_config, 
    write_config, 
    get_config_extension,
    read_config_text,
    read_config_yaml
)

def test_text_format():
    """Test reading text format config files"""
    print("Testing text format...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
        f.write("# This is a comment\n")
        f.write("key1=value1\n")
        f.write("key2=value2 # inline comment\n")
        f.write("key3=value with spaces\n")
    
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1', f"Expected 'value1', got {result['key1']}"
        assert result['key2'] == 'value2', f"Expected 'value2', got {result['key2']}"
        assert result['key3'] == 'value with spaces', f"Expected 'value with spaces', got {result['key3']}"
        print("✓ Text format reading works")
    finally:
        os.unlink(fname)

def test_yaml_format():
    """Test reading YAML format config files"""
    print("Testing YAML format...")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
        f.write("key1: value1\n")
        f.write("key2: value2\n")
        f.write("key3: value with spaces\n")
    
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1', f"Expected 'value1', got {result['key1']}"
        assert result['key2'] == 'value2', f"Expected 'value2', got {result['key2']}"
        assert result['key3'] == 'value with spaces', f"Expected 'value with spaces', got {result['key3']}"
        print("✓ YAML format reading works")
    finally:
        os.unlink(fname)

def test_write_text_format():
    """Test writing text format config files"""
    print("Testing text format writing...")
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
    
    try:
        write_config(test_data, fname)
        result = read_config(fname)
        assert result == test_data, f"Round trip failed: {result} != {test_data}"
        print("✓ Text format writing works")
    finally:
        os.unlink(fname)

def test_write_yaml_format():
    """Test writing YAML format config files"""
    print("Testing YAML format writing...")
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
    
    try:
        write_config(test_data, fname)
        result = read_config(fname)
        assert result == test_data, f"Round trip failed: {result} != {test_data}"
        print("✓ YAML format writing works")
    finally:
        os.unlink(fname)

def test_auto_detection():
    """Test auto-detection of format based on file extension"""
    print("Testing auto-detection...")
    test_data = {'key1': 'value1', 'key2': 'value2'}
    
    # Test .txt extension
    assert get_config_extension('config.txt') == '.txt'
    assert get_config_extension('/path/to/config.txt') == '.txt'
    
    # Test .yaml extension
    assert get_config_extension('config.yaml') == '.yaml'
    assert get_config_extension('/path/to/config.yml') == '.yaml'
    assert get_config_extension('config.yml') == '.yaml'
    
    print("✓ Auto-detection works")

def test_consistency():
    """Test that text and YAML formats produce consistent results"""
    print("Testing consistency between formats...")
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname_txt = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname_yaml = f.name
    
    try:
        write_config(test_data, fname_txt)
        write_config(test_data, fname_yaml)
        
        result_txt = read_config(fname_txt)
        result_yaml = read_config(fname_yaml)
        
        assert result_txt == result_yaml, f"Text and YAML results differ: {result_txt} != {result_yaml}"
        print("✓ Text and YAML formats are consistent")
    finally:
        os.unlink(fname_txt)
        os.unlink(fname_yaml)

def main():
    """Run all tests"""
    print("=" * 60)
    print("Testing YAML Config Parsing Implementation")
    print("=" * 60)
    
    try:
        test_text_format()
        test_yaml_format()
        test_write_text_format()
        test_write_yaml_format()
        test_auto_detection()
        test_consistency()
        
        print("=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)
        return 0
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == '__main__':
    sys.exit(main())
