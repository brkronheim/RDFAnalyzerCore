#!/usr/bin/env python3
"""
Simple test for the datacard generator using mock data.
Tests the uproot-based implementation.
"""

import os
import sys
import tempfile
import yaml
import numpy as np

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def create_mock_root_file_with_uproot(filename, histograms):
    """Create a mock ROOT file with histograms using uproot."""
    try:
        import uproot
    except ImportError:
        print("uproot not available, skipping test")
        return False
    
    hist_dict = {}
    for hist_name, (values, edges) in histograms.items():
        hist_dict[hist_name] = (values, edges)
    
    with uproot.recreate(filename) as f:
        for name, data in hist_dict.items():
            f[name] = data
    
    return True


def test_basic_functionality():
    """Test basic datacard generator functionality."""
    print("=" * 80)
    print("Testing Datacard Generator (uproot-based)")
    print("=" * 80)
    
    # Check if uproot is available
    try:
        import uproot
        print("✓ uproot is available")
    except ImportError:
        print("✗ uproot not available. Install with: pip install uproot")
        return False
    
    # Create temporary directory
    test_dir = tempfile.mkdtemp(prefix='datacard_test_uproot_')
    input_dir = os.path.join(test_dir, 'input')
    os.makedirs(input_dir, exist_ok=True)
    
    try:
        print("\nCreating mock ROOT files...")
        
        # Create simple histograms
        edges = np.linspace(0, 500, 51)  # 50 bins from 0 to 500
        
        # Data histogram
        data_values = np.random.poisson(100, 50)
        create_mock_root_file_with_uproot(
            os.path.join(input_dir, 'data.root'),
            {'mT': (data_values, edges)}
        )
        print("  Created data.root")
        
        # Signal histogram
        signal_values = np.random.poisson(10, 50)
        create_mock_root_file_with_uproot(
            os.path.join(input_dir, 'signal.root'),
            {
                'mT': (signal_values, edges),
                'mT_JESUp': (signal_values * 1.03, edges),
                'mT_JESDown': (signal_values * 0.97, edges)
            }
        )
        print("  Created signal.root with systematics")
        
        # Background histogram
        ttbar_values = np.random.poisson(50, 50)
        create_mock_root_file_with_uproot(
            os.path.join(input_dir, 'ttbar.root'),
            {
                'mT': (ttbar_values, edges),
                'mT_JESUp': (ttbar_values * 1.03, edges),
                'mT_JESDown': (ttbar_values * 0.97, edges)
            }
        )
        print("  Created ttbar.root with systematics")
        
        # Create test configuration
        print("\nCreating test configuration...")
        config = {
            'output_dir': os.path.join(test_dir, 'output'),
            'input_files': {
                'data_obs': {
                    'path': os.path.join(input_dir, 'data.root'),
                    'type': 'data'
                },
                'signal': {
                    'path': os.path.join(input_dir, 'signal.root'),
                    'type': 'signal'
                },
                'ttbar': {
                    'path': os.path.join(input_dir, 'ttbar.root'),
                    'type': 'background'
                }
            },
            'processes': {
                'signal': {
                    'samples': ['signal'],
                    'description': 'Test signal'
                },
                'ttbar': {
                    'samples': ['ttbar'],
                    'description': 'Top pair'
                }
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
                    'applies_to': {
                        'signal': True,
                        'ttbar': True
                    },
                    'regions': ['signal_region'],
                    'description': 'Luminosity'
                },
                'JES': {
                    'type': 'shape',
                    'distribution': 'shape',
                    'variation': 0.03,
                    'applies_to': {
                        'signal': True,
                        'ttbar': True
                    },
                    'regions': ['signal_region'],
                    'description': 'Jet energy scale'
                }
            }
        }
        
        config_file = os.path.join(test_dir, 'test_config.yaml')
        with open(config_file, 'w') as f:
            yaml.dump(config, f, default_flow_style=False)
        
        print(f"  Created config: {config_file}")
        
        # Run datacard generator
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
                
                # For datacard, check content
                if expected_file.endswith('.txt'):
                    with open(file_path, 'r') as f:
                        content = f.read()
                        if 'shapes *' in content and 'observation' in content:
                            print("    ✓ Datacard has expected structure")
                        else:
                            print("    ✗ Datacard missing expected content")
                            all_found = False
                
                # For ROOT file, check with uproot
                if expected_file.endswith('.root'):
                    with uproot.open(file_path) as f:
                        keys = f.keys()
                        print(f"    Contains {len(keys)} histograms:")
                        for key in keys:
                            print(f"      - {key}")
                        
                        # Check for expected histograms
                        expected_hists = ['signal', 'ttbar', 'signal_JESUp', 'signal_JESDown',
                                        'ttbar_JESUp', 'ttbar_JESDown']
                        for hist_name in expected_hists:
                            if hist_name in keys or f"{hist_name};1" in keys:
                                print(f"    ✓ Found expected histogram: {hist_name}")
                            else:
                                print(f"    ✗ Missing histogram: {hist_name}")
            else:
                print(f"  ✗ Missing: {expected_file}")
                all_found = False
        
        print("\n" + "=" * 80)
        if all_found:
            print("TEST PASSED: All checks passed")
            print(f"\nTest files in: {test_dir}")
        else:
            print("TEST FAILED: Some checks failed")
        print("=" * 80)
        
        return all_found
        
    except Exception as e:
        print(f"\nTEST FAILED with exception: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = test_basic_functionality()
    sys.exit(0 if success else 1)
