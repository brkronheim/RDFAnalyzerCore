#!/usr/bin/env python3
"""
Originally a PyROOT-based datacard generator test; now rewritten to use
uproot so that no ROOT Python bindings are required.

The logic mirrors test_uproot_datacard.py but remains self-contained so the
existing CTest rule can continue to execute this file.
"""

import os
import sys
import tempfile

# Add parent directory to path to import the library under test
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# dependencies
import yaml

try:
    import uproot
except ImportError:
    print("SKIP: uproot not available")
    sys.exit(77)

import numpy as np


def create_mock_root_file(filename, histograms):
    """Create a mock ROOT file with simple histograms using uproot.

    `histograms` should be a dict mapping hist_name -> (values_array, edges_array)
    """
    with uproot.recreate(filename) as f:
        for name, (values, edges) in histograms.items():
            f[name] = (values, edges)

def create_test_config(test_dir, input_dir):
    """Create a concise YAML configuration for the datacard generator."""
    config = {
        'output_dir': os.path.join(test_dir, 'output'),
        'input_files': {
            'data_obs': {'path': os.path.join(input_dir, 'data.root'), 'type': 'data'},
            'signal': {'path': os.path.join(input_dir, 'signal.root'), 'type': 'signal'},
            'ttbar': {'path': os.path.join(input_dir, 'ttbar.root'), 'type': 'background'},
        },
        'processes': {
            'signal': {'samples': ['signal'], 'description': 'Test signal'},
            'ttbar': {'samples': ['ttbar'], 'description': 'Top pair'},
        },
        'control_regions': {
            'signal_region': {
                'observable': 'mT',
                'processes': ['signal', 'ttbar'],
                'signal_processes': ['signal'],
                'data_process': 'data_obs',
                'description': 'Test signal region'
            }
        },
        'systematics': {
            'lumi': {
                'type': 'rate',
                'distribution': 'lnN',
                'value': 1.025,
                'applies_to': {'signal': True, 'ttbar': True},
                'regions': ['signal_region'],
                'description': 'Luminosity'
            },
            'JES': {
                'type': 'shape',
                'distribution': 'shape',
                'variation': 0.03,
                'applies_to': {'signal': True, 'ttbar': True},
                'regions': ['signal_region'],
                'description': 'Jet energy scale'
            }
        }
    }
    config_file = os.path.join(test_dir, 'test_config.yaml')
    with open(config_file, 'w') as f:
        yaml.dump(config, f, default_flow_style=False)
    print(f"Created test configuration: {config_file}")
    return config_file


def test_datacard_generator():
    """Main test function using uproot-based histogram files."""
    print("=" * 80)
    print("Testing Datacard Generator (pure uproot)")
    print("=" * 80)

    # Create temporary directory for test files
    test_dir = tempfile.mkdtemp(prefix='datacard_test_')
    input_dir = os.path.join(test_dir, 'input')
    os.makedirs(input_dir, exist_ok=True)

    try:
        print("\nCreating mock ROOT files...")
        # uniform binning for simplicity
        edges = np.linspace(0, 500, 51)

        create_mock_root_file(
            os.path.join(input_dir, 'data.root'),
            {'mT': (np.random.poisson(100, 50), edges)}
        )
        create_mock_root_file(
            os.path.join(input_dir, 'signal.root'),
            {
                'mT': (np.random.poisson(10, 50), edges),
                'mT_JESUp': (np.random.poisson(10, 50) * 1.03, edges),
                'mT_JESDown': (np.random.poisson(10, 50) * 0.97, edges),
            }
        )
        create_mock_root_file(
            os.path.join(input_dir, 'ttbar.root'),
            {
                'mT': (np.random.poisson(50, 50), edges),
                'mT_JESUp': (np.random.poisson(50, 50) * 1.03, edges),
                'mT_JESDown': (np.random.poisson(50, 50) * 0.97, edges),
            }
        )

        # Create test configuration
        print("\nCreating test configuration...")
        config_file = create_test_config(test_dir, input_dir)

        # Import and run the datacard generator
        print("\nRunning datacard generator...")
        from create_datacards import DatacardGenerator
        generator = DatacardGenerator(config_file)
        generator.run()

        # Verify outputs
        print("\nVerifying outputs...")
        output_dir = os.path.join(test_dir, 'output')
        expected_files = [
            'datacard_signal_region.txt',
            'shapes_signal_region.root'
        ]
        all_found = True
        for expected_file in expected_files:
            file_path = os.path.join(output_dir, expected_file)
            if os.path.exists(file_path):
                print(f"  ✓ Found: {expected_file}")
                size = os.path.getsize(file_path)
                print(f"    Size: {size} bytes")
                
                # For datacard, print first few lines
                if expected_file.endswith('.txt'):
                    print(f"    Content preview:")
                    with open(file_path, 'r') as f:
                        for i, line in enumerate(f):
                            if i < 10:
                                print(f"      {line.rstrip()}")
                
                # For ROOT file, check histograms with uproot
                if expected_file.endswith('.root'):
                    with uproot.open(file_path) as rf:
                        print(f"    Histograms: {rf.keys()}")
            else:
                print(f"  ✗ Missing: {expected_file}")
                all_found = False
        
        print("\n" + "=" * 80)
        if all_found:
            print("TEST PASSED: All expected files were created")
        else:
            print("TEST FAILED: Some expected files are missing")
        print("=" * 80)
        
        # Keep test directory for inspection
        print(f"\nTest files are in: {test_dir}")
        print("(Directory will not be automatically cleaned up)")
        
        return all_found
        
    except Exception as e:
        print(f"\nTEST FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_datacard_generator()
    sys.exit(0 if success else 1)
