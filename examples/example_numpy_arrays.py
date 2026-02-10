#!/usr/bin/env python3
"""
Example: Using numpy arrays with RDFAnalyzerCore Python bindings

This example demonstrates how to pass numpy arrays to the analyzer
using the pointer + size interface.
"""

import sys
import os

# Add the Python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../build/python'))

import numpy as np
import rdfanalyzer

def main():
    """
    Example analysis using numpy arrays
    
    This approach allows you to define RDataFrame columns from numpy
    arrays by passing the data pointer and size.
    """
    
    # Check if config file is provided
    if len(sys.argv) != 2:
        print("Usage: python example_numpy_arrays.py <config_file>")
        print("\nExample config file (cfg.txt):")
        print("  fileList=data.root")
        print("  saveFile=output.root")
        print("  threads=-1")
        return 1
    
    config_file = sys.argv[1]
    
    # Create analyzer from configuration file
    print(f"Creating analyzer with config: {config_file}")
    analyzer = rdfanalyzer.Analyzer(config_file)
    
    # Create some example numpy arrays
    print("\nCreating numpy arrays...")
    
    # Example 1: Event weights (float32)
    event_weights = np.array([1.0, 1.2, 0.8, 1.1, 0.9], dtype=np.float32)
    print(f"  Event weights: {event_weights}")
    
    # Example 2: Scale factors (float64/double)
    scale_factors = np.array([1.05, 1.05, 1.05, 1.05, 1.05], dtype=np.float64)
    print(f"  Scale factors: {scale_factors}")
    
    # Example 3: Integer flags
    quality_flags = np.array([1, 1, 0, 1, 1], dtype=np.int32)
    print(f"  Quality flags: {quality_flags}")
    
    # Define variables from numpy arrays
    print("\nDefining variables from numpy arrays...")
    
    # Note: The arrays must remain in memory while the analysis runs!
    # Pass the pointer and size to the analyzer
    
    analyzer.DefineFromVector("event_weight",
                              event_weights.ctypes.data,
                              len(event_weights),
                              "float")
    
    analyzer.DefineFromVector("scale_factor",
                              scale_factors.ctypes.data,
                              len(scale_factors),
                              "double")
    
    analyzer.DefineFromVector("quality",
                              quality_flags.ctypes.data,
                              len(quality_flags),
                              "int")
    
    # Use the arrays in expressions
    print("Defining computed variables...")
    analyzer.DefineJIT("weighted_pt", "pt * event_weight", ["pt", "event_weight"])
    analyzer.DefineJIT("scaled_pt", "pt * scale_factor", ["pt", "scale_factor"])
    
    # Apply filters
    print("Applying filters...")
    analyzer.FilterJIT("quality_cut", "quality == 1", ["quality"])
    
    # Save the results
    print("\nSaving results...")
    analyzer.save()
    
    print("Analysis complete!")
    print("\nNote: Ensure numpy arrays remain in memory during analysis!")
    print("      The analyzer stores pointers, not copies of the data.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
