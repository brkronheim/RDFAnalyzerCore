#!/usr/bin/env python3
"""
Example: Basic usage of RDFAnalyzerCore Python bindings with string-based expressions

This example demonstrates how to use the Python bindings with ROOT JIT compilation
for variable definitions and filters.
"""

import sys
import os

# Add the Python module to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../build/python'))

import rdfanalyzer

def main():
    """
    Example analysis using string-based expressions (ROOT JIT)
    
    This approach allows you to write C++ expressions as strings,
    which ROOT will compile and execute efficiently.
    """
    
    # Check if config file is provided
    if len(sys.argv) != 2:
        print("Usage: python example_string_expressions.py <config_file>")
        print("\nExample config file (cfg.txt):")
        print("  fileList=data.root")
        print("  saveFile=output.root")
        print("  threads=-1")
        return 1
    
    config_file = sys.argv[1]
    
    # Create analyzer from configuration file
    print(f"Creating analyzer with config: {config_file}")
    analyzer = rdfanalyzer.Analyzer(config_file)
    
    # Define new variables using C++ expressions
    print("\nDefining variables...")
    
    # Example 1: Simple arithmetic
    analyzer.Define("pt_gev", "pt / 1000.0", ["pt"])
    
    # Example 2: Multiple input columns
    analyzer.Define("delta_r", 
                    "sqrt(delta_eta*delta_eta + delta_phi*delta_phi)", 
                    ["delta_eta", "delta_phi"])
    
    # Example 3: Vector operations (if working with ROOT::VecOps::RVec)
    analyzer.Define("high_pt_jets", "jet_pt > 25000.0", ["jet_pt"])
    analyzer.Define("n_high_pt_jets", "Sum(high_pt_jets)", ["high_pt_jets"])
    
    # Apply filters
    print("Applying filters...")
    
    # Example 1: Simple threshold
    analyzer.Filter("pt_cut", "pt_gev > 20.0", ["pt_gev"])
    
    # Example 2: Multiple conditions
    analyzer.Filter("quality", "quality_flag == 1 && pt_gev > 25.0", 
                    ["quality_flag", "pt_gev"])
    
    # Example 3: Jet multiplicity
    analyzer.Filter("jet_selection", "n_high_pt_jets >= 4", ["n_high_pt_jets"])
    
    # Save the results
    print("\nSaving results...")
    analyzer.save()
    
    print("Analysis complete!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
