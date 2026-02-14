#!/usr/bin/env python3
"""
Convert configuration files between text and YAML formats.

Usage:
    python convert_config.py input.txt output.yaml
    python convert_config.py input.yaml output.txt
"""

import sys
import os

# Add the python directory to the path
script_dir = os.path.dirname(os.path.abspath(__file__))
python_dir = os.path.join(script_dir, '..', 'python')
sys.path.insert(0, python_dir)

from submission_backend import read_config, write_config

def main():
    if len(sys.argv) != 3:
        print("Usage: convert_config.py <input_file> <output_file>")
        print()
        print("Examples:")
        print("  convert_config.py config.txt config.yaml")
        print("  convert_config.py config.yaml config.txt")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' not found")
        sys.exit(1)
    
    try:
        print(f"Reading config from {input_file}...")
        config = read_config(input_file)
        
        print(f"Writing config to {output_file}...")
        write_config(config, output_file)
        
        print(f"✓ Successfully converted {input_file} to {output_file}")
        print(f"  {len(config)} configuration keys converted")
        
    except Exception as e:
        print(f"✗ Error during conversion: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()
