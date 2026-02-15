#!/usr/bin/env python3
"""
CMS Combine Datacard Generator

This script generates datacards and ROOT files for CMS combine fits from analysis output files.
It is controlled by a YAML configuration file that specifies control regions, observables,
sample combinations, processes, rebinning, and systematics.

Usage:
    python create_datacards.py config.yaml

Author: RDFAnalyzerCore
"""

import argparse
import yaml
import ROOT
import os
import sys
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any


class DatacardGenerator:
    """Generates CMS combine datacards and ROOT files from analysis outputs."""
    
    def __init__(self, config_file: str):
        """
        Initialize the datacard generator.
        
        Args:
            config_file: Path to YAML configuration file
        """
        with open(config_file, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.output_dir = self.config.get('output_dir', 'datacards')
        self.input_files = self.config.get('input_files', {})
        self.control_regions = self.config.get('control_regions', {})
        self.processes = self.config.get('processes', {})
        self.systematics = self.config.get('systematics', {})
        self.sample_combinations = self.config.get('sample_combinations', {})
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Store histogram data
        self.histograms = {}
        
    def read_histograms(self) -> None:
        """Read histograms from input ROOT files."""
        print("Reading histograms from input files...")
        
        for sample_name, file_info in self.input_files.items():
            file_path = file_info.get('path')
            if not os.path.exists(file_path):
                print(f"Warning: File {file_path} not found for sample {sample_name}")
                continue
            
            print(f"  Processing {sample_name}: {file_path}")
            root_file = ROOT.TFile.Open(file_path, "READ")
            
            if not root_file or root_file.IsZombie():
                print(f"Error: Cannot open file {file_path}")
                continue
            
            # Store file reference
            if sample_name not in self.histograms:
                self.histograms[sample_name] = {}
            
            # Read histograms from file
            self._read_histograms_from_file(root_file, sample_name)
            
            root_file.Close()
    
    def _read_histograms_from_file(self, root_file: ROOT.TFile, sample_name: str) -> None:
        """
        Read all histograms from a ROOT file.
        
        Args:
            root_file: Opened ROOT file
            sample_name: Name of the sample
        """
        # Iterate through keys in the file
        for key in root_file.GetListOfKeys():
            obj = key.ReadObj()
            if obj.InheritsFrom("TH1") or obj.InheritsFrom("THnSparse"):
                hist_name = obj.GetName()
                # Clone the histogram to keep it in memory after file closes
                hist_clone = obj.Clone(f"{sample_name}_{hist_name}")
                hist_clone.SetDirectory(0)
                self.histograms[sample_name][hist_name] = hist_clone
    
    def combine_samples(self, region: str, process: str) -> Optional[ROOT.TH1]:
        """
        Combine multiple samples according to configuration.
        
        Args:
            region: Control region name
            process: Process name
            
        Returns:
            Combined histogram or None
        """
        # Get sample list for this process
        process_config = self.processes.get(process, {})
        samples = process_config.get('samples', [process])
        
        if not samples:
            return None
        
        # Get observable for this region
        region_config = self.control_regions.get(region, {})
        observable = region_config.get('observable')
        
        if not observable:
            print(f"Warning: No observable defined for region {region}")
            return None
        
        combined_hist = None
        
        for sample in samples:
            # Handle stitched samples
            if sample in self.sample_combinations:
                stitch_config = self.sample_combinations[sample]
                hist = self._combine_stitched_samples(stitch_config, observable, region)
            else:
                # Single sample
                hist = self._get_histogram(sample, observable, region)
            
            if hist is None:
                continue
            
            # Add to combined histogram
            if combined_hist is None:
                combined_hist = hist.Clone(f"{process}_{region}_{observable}")
                combined_hist.SetDirectory(0)
            else:
                combined_hist.Add(hist)
        
        # Apply rebinning if configured
        if combined_hist and 'rebin' in region_config:
            combined_hist = self._rebin_histogram(combined_hist, region_config['rebin'])
        
        return combined_hist
    
    def _combine_stitched_samples(self, stitch_config: Dict, observable: str, region: str) -> Optional[ROOT.TH1]:
        """
        Combine stitched samples (e.g., different HT bins).
        
        Args:
            stitch_config: Configuration for stitching
            observable: Observable name
            region: Control region name
            
        Returns:
            Combined histogram or None
        """
        samples = stitch_config.get('samples', [])
        method = stitch_config.get('method', 'sum')
        
        combined_hist = None
        
        for sample in samples:
            hist = self._get_histogram(sample, observable, region)
            if hist is None:
                continue
            
            if method == 'sum':
                if combined_hist is None:
                    combined_hist = hist.Clone()
                    combined_hist.SetDirectory(0)
                else:
                    combined_hist.Add(hist)
            elif method == 'weighted':
                # Implement weighted combination if needed
                weight = stitch_config.get('weights', {}).get(sample, 1.0)
                hist.Scale(weight)
                if combined_hist is None:
                    combined_hist = hist.Clone()
                    combined_hist.SetDirectory(0)
                else:
                    combined_hist.Add(hist)
        
        return combined_hist
    
    def _get_histogram(self, sample: str, observable: str, region: str) -> Optional[ROOT.TH1]:
        """
        Get histogram for a specific sample, observable, and region.
        
        Args:
            sample: Sample name
            observable: Observable name
            region: Control region name
            
        Returns:
            Histogram or None
        """
        if sample not in self.histograms:
            return None
        
        # Try different naming conventions
        possible_names = [
            f"{observable}_{region}",
            f"{region}_{observable}",
            observable,
            f"{observable}_{sample}",
        ]
        
        for hist_name in possible_names:
            if hist_name in self.histograms[sample]:
                hist = self.histograms[sample][hist_name]
                
                # Convert THnSparse to TH1 if needed
                if hist.InheritsFrom("THnSparse"):
                    hist = self._project_sparse_histogram(hist, observable, region)
                
                return hist
        
        return None
    
    def _project_sparse_histogram(self, sparse_hist: ROOT.THnSparse, observable: str, region: str) -> Optional[ROOT.TH1]:
        """
        Project THnSparse histogram to 1D.
        
        Args:
            sparse_hist: Sparse histogram
            observable: Observable name
            region: Control region name
            
        Returns:
            1D histogram or None
        """
        # Find the axis corresponding to the observable
        n_axes = sparse_hist.GetNdimensions()
        
        for i in range(n_axes):
            axis = sparse_hist.GetAxis(i)
            axis_name = axis.GetName()
            if observable in axis_name or axis_name in observable:
                # Project onto this axis
                projected = sparse_hist.Projection(i)
                projected.SetDirectory(0)
                return projected
        
        # If no matching axis found, project onto first axis
        if n_axes > 0:
            projected = sparse_hist.Projection(0)
            projected.SetDirectory(0)
            return projected
        
        return None
    
    def _rebin_histogram(self, hist: ROOT.TH1, rebin_config: Any) -> ROOT.TH1:
        """
        Rebin histogram according to configuration.
        
        Args:
            hist: Input histogram
            rebin_config: Rebinning configuration (integer or list of bin edges)
            
        Returns:
            Rebinned histogram
        """
        if isinstance(rebin_config, int):
            # Simple rebinning by factor
            rebinned = hist.Rebin(rebin_config)
            rebinned.SetDirectory(0)
            return rebinned
        elif isinstance(rebin_config, list):
            # Variable bin width rebinning
            import array
            bin_edges = array.array('d', rebin_config)
            rebinned = hist.Rebin(len(bin_edges) - 1, hist.GetName() + "_rebinned", bin_edges)
            rebinned.SetDirectory(0)
            return rebinned
        
        return hist
    
    def generate_datacard(self, region: str) -> None:
        """
        Generate a datacard for a specific control region.
        
        Args:
            region: Control region name
        """
        print(f"Generating datacard for region: {region}")
        
        region_config = self.control_regions[region]
        processes = region_config.get('processes', [])
        observable = region_config.get('observable')
        
        if not processes or not observable:
            print(f"Error: Missing processes or observable for region {region}")
            return
        
        # Prepare histograms for all processes
        process_hists = {}
        for process in processes:
            hist = self.combine_samples(region, process)
            if hist:
                process_hists[process] = hist
        
        if not process_hists:
            print(f"Warning: No histograms found for region {region}")
            return
        
        # Create ROOT file for combine
        root_filename = os.path.join(self.output_dir, f"shapes_{region}.root")
        root_file = ROOT.TFile.Open(root_filename, "RECREATE")
        
        # Write histograms to ROOT file
        for process, hist in process_hists.items():
            # Write nominal histogram
            hist.SetName(process)
            hist.Write()
            
            # Write systematic variations
            self._write_systematic_histograms(root_file, hist, process, region)
        
        root_file.Close()
        print(f"  Created ROOT file: {root_filename}")
        
        # Write datacard
        datacard_filename = os.path.join(self.output_dir, f"datacard_{region}.txt")
        self._write_datacard_file(datacard_filename, region, process_hists, root_filename)
        print(f"  Created datacard: {datacard_filename}")
    
    def _write_systematic_histograms(self, root_file: ROOT.TFile, nominal_hist: ROOT.TH1, 
                                    process: str, region: str) -> None:
        """
        Write systematic variation histograms.
        
        Args:
            root_file: Output ROOT file
            nominal_hist: Nominal histogram
            process: Process name
            region: Control region name
        """
        # Get systematics for this process and region
        systematics_list = self._get_systematics_for_process(process, region)
        
        for syst_name, syst_config in systematics_list.items():
            if not syst_config.get('applies_to', {}).get(process, True):
                continue
            
            syst_type = syst_config.get('type', 'shape')
            
            if syst_type == 'shape':
                # Create up and down variations
                # For now, create placeholder variations
                # In a real implementation, these would come from the input files
                
                up_hist = nominal_hist.Clone(f"{process}_{syst_name}Up")
                down_hist = nominal_hist.Clone(f"{process}_{syst_name}Down")
                
                # Apply systematic variation (placeholder implementation)
                variation = syst_config.get('variation', 0.1)
                up_hist.Scale(1.0 + variation)
                down_hist.Scale(1.0 - variation)
                
                up_hist.Write()
                down_hist.Write()
    
    def _get_systematics_for_process(self, process: str, region: str) -> Dict:
        """
        Get systematics that apply to a specific process and region.
        
        Args:
            process: Process name
            region: Control region name
            
        Returns:
            Dictionary of applicable systematics
        """
        applicable_systematics = {}
        
        for syst_name, syst_config in self.systematics.items():
            # Check if systematic applies to this region
            regions = syst_config.get('regions', [])
            if regions and region not in regions:
                continue
            
            # Check if systematic applies to this process
            applies_to = syst_config.get('applies_to', {})
            if applies_to and not applies_to.get(process, False):
                continue
            
            applicable_systematics[syst_name] = syst_config
        
        return applicable_systematics
    
    def _write_datacard_file(self, filename: str, region: str, 
                            process_hists: Dict[str, ROOT.TH1], root_filename: str) -> None:
        """
        Write the datacard text file.
        
        Args:
            filename: Output datacard filename
            region: Control region name
            process_hists: Dictionary of process histograms
            root_filename: Name of the ROOT file with shapes
        """
        region_config = self.control_regions[region]
        
        with open(filename, 'w') as f:
            # Header
            f.write(f"# Datacard for region: {region}\n")
            f.write(f"# Observable: {region_config.get('observable')}\n")
            f.write("#" + "="*80 + "\n")
            f.write(f"imax 1  number of channels\n")
            f.write(f"jmax {len(process_hists) - 1}  number of backgrounds\n")
            f.write(f"kmax *  number of nuisance parameters\n")
            f.write("#" + "-"*80 + "\n")
            
            # Shapes
            f.write(f"shapes * {region} {os.path.basename(root_filename)} $PROCESS $PROCESS_$SYSTEMATIC\n")
            f.write("#" + "-"*80 + "\n")
            
            # Observations
            # Get data process
            data_process = region_config.get('data_process', 'data_obs')
            if data_process in process_hists:
                n_obs = int(process_hists[data_process].Integral())
            else:
                # Sum all backgrounds as pseudo-data
                n_obs = int(sum(h.Integral() for h in process_hists.values()))
            
            f.write(f"bin          {region}\n")
            f.write(f"observation  {n_obs}\n")
            f.write("#" + "-"*80 + "\n")
            
            # Expected rates
            processes = list(process_hists.keys())
            n_proc = len(processes)
            
            # Determine signal and background process IDs
            signal_processes = region_config.get('signal_processes', [])
            
            # Process IDs: signal processes get 0, -1, -2, ...; backgrounds get 1, 2, 3, ...
            process_ids = []
            signal_id = 0
            background_id = 1
            for proc in processes:
                if proc in signal_processes:
                    process_ids.append(signal_id)
                    signal_id -= 1
                else:
                    process_ids.append(background_id)
                    background_id += 1
            
            # Write bin row
            f.write("bin          " + "  ".join([region] * n_proc) + "\n")
            
            # Write process name row
            f.write("process      " + "  ".join(processes) + "\n")
            
            # Write process ID row
            f.write("process      " + "  ".join(str(pid) for pid in process_ids) + "\n")
            
            # Write rate row
            rates = [f"{process_hists[proc].Integral():.4f}" for proc in processes]
            f.write("rate         " + "  ".join(rates) + "\n")
            f.write("#" + "-"*80 + "\n")
            
            # Systematics
            systematics_list = self._get_systematics_for_region(region)
            
            for syst_name, syst_config in systematics_list.items():
                syst_type = syst_config.get('type', 'shape')
                distribution = syst_config.get('distribution', 'lnN')
                
                # Build systematic line
                syst_values = []
                for proc in processes:
                    applies = syst_config.get('applies_to', {}).get(proc, True)
                    if applies:
                        if syst_type == 'shape':
                            syst_values.append("1")
                        else:
                            # Rate systematic
                            value = syst_config.get('value', 1.1)
                            syst_values.append(f"{value:.3f}")
                    else:
                        syst_values.append("-")
                
                f.write(f"{syst_name:20s} {distribution:8s} " + "  ".join(syst_values) + "\n")
            
            # Correlations
            if 'correlations' in region_config:
                f.write("#" + "-"*80 + "\n")
                f.write("# Correlations\n")
                for corr_name, corr_info in region_config['correlations'].items():
                    f.write(f"# {corr_name}: {corr_info}\n")
    
    def _get_systematics_for_region(self, region: str) -> Dict:
        """
        Get all systematics for a specific region.
        
        Args:
            region: Control region name
            
        Returns:
            Dictionary of systematics
        """
        applicable_systematics = {}
        
        for syst_name, syst_config in self.systematics.items():
            regions = syst_config.get('regions', [])
            if regions and region not in regions:
                continue
            
            applicable_systematics[syst_name] = syst_config
        
        return applicable_systematics
    
    def generate_all_datacards(self) -> None:
        """Generate datacards for all configured control regions."""
        print("\nGenerating datacards...")
        print("=" * 80)
        
        for region in self.control_regions.keys():
            self.generate_datacard(region)
            print()
    
    def run(self) -> None:
        """Run the complete datacard generation pipeline."""
        print("=" * 80)
        print("CMS Combine Datacard Generator")
        print("=" * 80)
        
        self.read_histograms()
        self.generate_all_datacards()
        
        print("=" * 80)
        print(f"Datacard generation complete. Output in: {self.output_dir}")
        print("=" * 80)


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Generate CMS combine datacards from analysis output files',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog='''
Example:
    python create_datacards.py config.yaml

See example_datacard_config.yaml for configuration file format.
        '''
    )
    parser.add_argument('config', help='YAML configuration file')
    parser.add_argument('-v', '--verbose', action='store_true', 
                       help='Verbose output')
    
    args = parser.parse_args()
    
    if not os.path.exists(args.config):
        print(f"Error: Configuration file '{args.config}' not found")
        sys.exit(1)
    
    try:
        generator = DatacardGenerator(args.config)
        generator.run()
    except Exception as e:
        print(f"Error: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
