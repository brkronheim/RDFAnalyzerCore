#!/usr/bin/env python3
"""
Validation script for the datacard generator.
Tests the basic structure and logic without requiring ROOT.
"""

import os
import sys
import yaml
import tempfile

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def validate_config_schema(config_file):
    """Validate that a YAML configuration has the required structure."""
    print(f"Validating configuration schema: {config_file}")
    
    with open(config_file, 'r') as f:
        config = yaml.safe_load(f)
    
    required_keys = ['output_dir', 'input_files', 'processes', 'control_regions']
    optional_keys = ['systematics', 'sample_combinations', 'correlations']
    
    errors = []
    warnings = []
    
    # Check required keys
    for key in required_keys:
        if key not in config:
            errors.append(f"Missing required key: {key}")
    
    # Validate input_files structure
    if 'input_files' in config:
        for sample_name, sample_info in config['input_files'].items():
            if 'path' not in sample_info:
                errors.append(f"Input file '{sample_name}' missing 'path' field")
    
    # Validate processes structure
    if 'processes' in config:
        for process_name, process_info in config['processes'].items():
            if 'samples' not in process_info:
                errors.append(f"Process '{process_name}' missing 'samples' field")
    
    # Validate control_regions structure
    if 'control_regions' in config:
        for region_name, region_info in config['control_regions'].items():
            required_region_keys = ['observable', 'processes']
            for key in required_region_keys:
                if key not in region_info:
                    errors.append(f"Control region '{region_name}' missing '{key}' field")
    
    # Validate systematics structure
    if 'systematics' in config:
        for syst_name, syst_info in config['systematics'].items():
            if 'type' not in syst_info:
                warnings.append(f"Systematic '{syst_name}' missing 'type' field")
            if 'applies_to' not in syst_info:
                warnings.append(f"Systematic '{syst_name}' missing 'applies_to' field")
    
    # Print results
    if errors:
        print("  ✗ ERRORS:")
        for error in errors:
            print(f"    - {error}")
    else:
        print("  ✓ No errors found")
    
    if warnings:
        print("  ⚠ WARNINGS:")
        for warning in warnings:
            print(f"    - {warning}")
    
    return len(errors) == 0


def validate_script_imports():
    """Validate that the script can be imported and has required functions."""
    print("\nValidating script imports and structure...")
    
    try:
        # Temporarily redirect stderr to suppress ROOT import error
        import io
        import contextlib
        
        stderr_backup = sys.stderr
        
        with contextlib.redirect_stderr(io.StringIO()):
            with contextlib.redirect_stdout(io.StringIO()):
                try:
                    from create_datacards import DatacardGenerator
                    print("  ✓ DatacardGenerator class imported successfully")
                    
                    # Check that required methods exist
                    required_methods = [
                        'read_histograms',
                        'combine_samples',
                        'generate_datacard',
                        'generate_all_datacards',
                        'run'
                    ]
                    
                    for method in required_methods:
                        if hasattr(DatacardGenerator, method):
                            print(f"  ✓ Method '{method}' exists")
                        else:
                            print(f"  ✗ Method '{method}' missing")
                            return False
                    
                    return True
                except SystemExit:
                    # ROOT not available, but we can still check the script structure
                    print("  ⚠ ROOT not available in this environment")
                    print("  ℹ Script requires ROOT for full functionality")
                    print("  ℹ Checking script syntax instead...")
                    
                    # Check if file exists and has valid syntax
                    script_path = os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        'create_datacards.py'
                    )
                    
                    if os.path.exists(script_path):
                        print(f"  ✓ Script file exists: {script_path}")
                        
                        # Check syntax by compiling
                        with open(script_path, 'r') as f:
                            code = f.read()
                        
                        try:
                            compile(code, script_path, 'exec')
                            print("  ✓ Script has valid Python syntax")
                            return True
                        except SyntaxError as e:
                            print(f"  ✗ Syntax error: {e}")
                            return False
                    else:
                        print(f"  ✗ Script file not found: {script_path}")
                        return False
        
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_yaml_config_generation():
    """Test that we can create a valid YAML configuration."""
    print("\nTesting YAML configuration generation...")
    
    config = {
        'output_dir': 'test_output',
        'input_files': {
            'data': {'path': 'data.root', 'type': 'data'},
            'signal': {'path': 'signal.root', 'type': 'signal'}
        },
        'processes': {
            'signal': {'samples': ['signal'], 'description': 'Test signal'}
        },
        'control_regions': {
            'SR': {
                'observable': 'mT',
                'processes': ['signal'],
                'signal_processes': ['signal'],
                'data_process': 'data'
            }
        },
        'systematics': {
            'lumi': {
                'type': 'rate',
                'distribution': 'lnN',
                'value': 1.025,
                'applies_to': {'signal': True}
            }
        }
    }
    
    # Try to write and read back
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        yaml.dump(config, f)
        temp_file = f.name
    
    try:
        with open(temp_file, 'r') as f:
            loaded_config = yaml.safe_load(f)
        
        if loaded_config == config:
            print("  ✓ YAML configuration can be written and read correctly")
            return True
        else:
            print("  ✗ YAML configuration changed after write/read")
            return False
    finally:
        os.unlink(temp_file)


def validate_example_config():
    """Validate the example configuration file."""
    print("\nValidating example configuration file...")
    
    example_config = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'example_datacard_config.yaml'
    )
    
    if not os.path.exists(example_config):
        print(f"  ✗ Example config not found: {example_config}")
        return False
    
    return validate_config_schema(example_config)


def check_documentation():
    """Check that documentation exists."""
    print("\nChecking documentation...")
    
    doc_file = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        '../../docs/DATACARD_GENERATOR.md'
    )
    
    if os.path.exists(doc_file):
        print(f"  ✓ Documentation found: {doc_file}")
        
        # Check file size
        size = os.path.getsize(doc_file)
        print(f"    Size: {size} bytes")
        
        if size > 1000:
            print("    ✓ Documentation appears to be substantial")
            return True
        else:
            print("    ⚠ Documentation seems short")
            return False
    else:
        print(f"  ✗ Documentation not found: {doc_file}")
        return False


def main():
    """Run all validation tests."""
    print("=" * 80)
    print("Datacard Generator Validation Suite")
    print("=" * 80)
    
    results = []
    
    # Test 1: Script imports
    results.append(("Script imports", validate_script_imports()))
    
    # Test 2: YAML configuration
    results.append(("YAML configuration", test_yaml_config_generation()))
    
    # Test 3: Example config validation
    results.append(("Example config", validate_example_config()))
    
    # Test 4: Documentation
    results.append(("Documentation", check_documentation()))
    
    # Summary
    print("\n" + "=" * 80)
    print("Validation Summary")
    print("=" * 80)
    
    all_passed = True
    for test_name, passed in results:
        status = "✓ PASS" if passed else "✗ FAIL"
        print(f"  {status}: {test_name}")
        if not passed:
            all_passed = False
    
    print("=" * 80)
    if all_passed:
        print("All validation tests passed!")
        print("\nNote: Full functionality testing requires ROOT and actual data files.")
        print("To test with real data:")
        print("  1. Source the ROOT environment (source env.sh)")
        print("  2. Create test ROOT files with histograms")
        print("  3. Run: python create_datacards.py example_datacard_config.yaml")
    else:
        print("Some validation tests failed. Please review the errors above.")
    print("=" * 80)
    
    return all_passed


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
