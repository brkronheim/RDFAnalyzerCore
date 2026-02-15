#!/usr/bin/env python3
"""
Test script for the datacard generator.
Creates mock ROOT files and tests the datacard generation functionality.
"""

import os
import sys
import tempfile
import shutil

# Add parent directory to path to import ROOT
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import ROOT
import yaml


def create_mock_histogram(name, nbins=50, xmin=0, xmax=500, mean=250, sigma=50, integral=1000):
    """Create a mock histogram with Gaussian distribution."""
    hist = ROOT.TH1F(name, name, nbins, xmin, xmax)
    
    # Fill with Gaussian distribution
    for i in range(int(integral)):
        hist.Fill(ROOT.gRandom.Gaus(mean, sigma))
    
    return hist


def create_mock_root_file(filename, histograms):
    """Create a mock ROOT file with given histograms."""
    root_file = ROOT.TFile.Open(filename, "RECREATE")
    
    for hist_name, hist_params in histograms.items():
        hist = create_mock_histogram(hist_name, **hist_params)
        hist.Write()
    
    root_file.Close()
    print(f"Created mock ROOT file: {filename}")


def create_test_config(test_dir, input_dir):
    """Create a test YAML configuration file."""
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
            },
            'wjets': {
                'path': os.path.join(input_dir, 'wjets.root'),
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
            },
            'wjets': {
                'samples': ['wjets'],
                'description': 'W+jets'
            }
        },
        'control_regions': {
            'signal_region': {
                'observable': 'mT',
                'processes': ['signal', 'ttbar', 'wjets'],
                'signal_processes': ['signal'],
                'data_process': 'data_obs',
                'rebin': 2,
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
                    'ttbar': True,
                    'wjets': True
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
                    'ttbar': True,
                    'wjets': True
                },
                'regions': ['signal_region'],
                'correlated': True,
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
    """Main test function."""
    print("=" * 80)
    print("Testing Datacard Generator")
    print("=" * 80)
    
    # Create temporary directory for test files
    test_dir = tempfile.mkdtemp(prefix='datacard_test_')
    input_dir = os.path.join(test_dir, 'input')
    os.makedirs(input_dir, exist_ok=True)
    
    try:
        # Create mock ROOT files
        print("\nCreating mock ROOT files...")
        
        # Data file
        create_mock_root_file(
            os.path.join(input_dir, 'data.root'),
            {
                'mT': {'mean': 250, 'sigma': 50, 'integral': 5000}
            }
        )
        
        # Signal file
        create_mock_root_file(
            os.path.join(input_dir, 'signal.root'),
            {
                'mT': {'mean': 300, 'sigma': 30, 'integral': 500}
            }
        )
        
        # ttbar file
        create_mock_root_file(
            os.path.join(input_dir, 'ttbar.root'),
            {
                'mT': {'mean': 200, 'sigma': 60, 'integral': 2000}
            }
        )
        
        # wjets file
        create_mock_root_file(
            os.path.join(input_dir, 'wjets.root'),
            {
                'mT': {'mean': 180, 'sigma': 70, 'integral': 2500}
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
                
                # Print file size
                size = os.path.getsize(file_path)
                print(f"    Size: {size} bytes")
                
                # For datacard, print first few lines
                if expected_file.endswith('.txt'):
                    print(f"    Content preview:")
                    with open(file_path, 'r') as f:
                        for i, line in enumerate(f):
                            if i < 10:
                                print(f"      {line.rstrip()}")
                
                # For ROOT file, check histograms
                if expected_file.endswith('.root'):
                    root_file = ROOT.TFile.Open(file_path, "READ")
                    print(f"    Histograms:")
                    for key in root_file.GetListOfKeys():
                        obj = key.ReadObj()
                        if obj.InheritsFrom("TH1"):
                            integral = obj.Integral()
                            print(f"      - {obj.GetName()}: {integral:.2f} events")
                    root_file.Close()
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
    # Suppress ROOT messages
    ROOT.gROOT.SetBatch(True)
    ROOT.gErrorIgnoreLevel = ROOT.kWarning
    
    success = test_datacard_generator()
    sys.exit(0 if success else 1)
