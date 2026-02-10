#!/usr/bin/env python3
"""
Example: Using numba-compiled functions with RDFAnalyzerCore Python bindings

This example demonstrates how to use numba to compile Python functions to
native code and pass them to the analyzer via function pointers.
"""

import sys
import os
import ctypes

# Add the Python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../build/python'))

import numpy as np
import numba
import rdfanalyzer

# Define numba-compiled functions
# Note: Use @numba.cfunc to create C-compatible function pointers

@numba.cfunc("float64(float64)")
def convert_to_gev(pt_mev):
    """Convert PT from MeV to GeV"""
    return pt_mev / 1000.0

@numba.cfunc("float64(float64, float64)")
def compute_delta_r(delta_eta, delta_phi):
    """Compute delta R from delta eta and delta phi"""
    return np.sqrt(delta_eta * delta_eta + delta_phi * delta_phi)

@numba.cfunc("float64(float64, float64, float64, float64)")
def compute_invariant_mass(pt1, eta1, pt2, eta2):
    """Simplified invariant mass calculation"""
    # This is a simplified example - real calculation would need phi and mass
    return np.sqrt((pt1 + pt2) * (pt1 + pt2) - (eta1 - eta2) * (eta1 - eta2))

def main():
    """
    Example analysis using numba-compiled functions
    
    This approach allows you to write Python functions and compile them
    with numba for high performance, then pass them to the analyzer.
    """
    
    # Check if config file is provided
    if len(sys.argv) != 2:
        print("Usage: python example_numba_functions.py <config_file>")
        print("\nExample config file (cfg.txt):")
        print("  fileList=data.root")
        print("  saveFile=output.root")
        print("  threads=-1")
        return 1
    
    config_file = sys.argv[1]
    
    # Create analyzer from configuration file
    print(f"Creating analyzer with config: {config_file}")
    analyzer = rdfanalyzer.Analyzer(config_file)
    
    # Get function pointers from numba-compiled functions
    print("\nGetting function pointers from numba...")
    convert_to_gev_ptr = ctypes.cast(convert_to_gev.address, ctypes.c_void_p).value
    compute_delta_r_ptr = ctypes.cast(compute_delta_r.address, ctypes.c_void_p).value
    compute_mass_ptr = ctypes.cast(compute_invariant_mass.address, ctypes.c_void_p).value
    
    print(f"  convert_to_gev at: 0x{convert_to_gev_ptr:x}")
    print(f"  compute_delta_r at: 0x{compute_delta_r_ptr:x}")
    print(f"  compute_invariant_mass at: 0x{compute_mass_ptr:x}")
    
    # Define variables using function pointers
    print("\nDefining variables using numba functions...")
    
    # Example 1: Single argument function
    analyzer.DefineFromPointer("pt_gev", 
                               convert_to_gev_ptr,
                               "double(double)",
                               ["pt"])
    
    # Example 2: Two argument function
    analyzer.DefineFromPointer("delta_r",
                               compute_delta_r_ptr,
                               "double(double, double)",
                               ["delta_eta", "delta_phi"])
    
    # Example 3: Four argument function
    analyzer.DefineFromPointer("inv_mass",
                               compute_mass_ptr,
                               "double(double, double, double, double)",
                               ["pt1", "eta1", "pt2", "eta2"])
    
    # Apply filters using the computed variables
    print("Applying filters...")
    analyzer.FilterJIT("pt_cut", "pt_gev > 20.0", ["pt_gev"])
    analyzer.FilterJIT("delta_r_cut", "delta_r < 0.4", ["delta_r"])
    
    # Save the results
    print("\nSaving results...")
    analyzer.save()
    
    print("Analysis complete!")
    print("\nNote: Numba functions provide near-native performance while allowing")
    print("      you to write analysis code in Python!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
