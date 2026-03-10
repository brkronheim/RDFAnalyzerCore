#!/usr/bin/env python3
"""
CMS Combine Datacard Generator

This script generates datacards and ROOT files for CMS combine fits from analysis output files.
It is controlled by a YAML configuration file that specifies control regions, observables,
sample combinations, processes, rebinning, and systematics.

Usage:
    python create_datacards.py config.yaml

Dependencies:
    - uproot: For reading ROOT files without PyROOT
    - numpy: For histogram operations
    - PyYAML: For configuration parsing

Author: RDFAnalyzerCore
"""

import argparse
import yaml
import os
import sys
import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple, Optional, Any, Union

# Try to import required packages
try:
    import uproot
except ImportError:
    print("Error: uproot package not found.")
    print("Please install it with: pip install uproot")
    sys.exit(1)

# Optional nuisance group registry – imported lazily so that the module is
# still usable when nuisance_groups.py is not on the path.
try:
    from nuisance_groups import NuisanceGroupRegistry
    _NUISANCE_GROUPS_AVAILABLE = True
except ImportError:  # pragma: no cover
    NuisanceGroupRegistry = None  # type: ignore[assignment,misc]
    _NUISANCE_GROUPS_AVAILABLE = False


class Histogram1D:
    """Simple 1D histogram class for storing and manipulating histograms."""
    
    def __init__(self, values: np.ndarray, edges: np.ndarray, name: str = ""):
        """
        Initialize histogram.
        
        Args:
            values: Bin contents
            edges: Bin edges (len = len(values) + 1)
            name: Histogram name
        """
        self.values = np.array(values, dtype=np.float64)
        self.edges = np.array(edges, dtype=np.float64)
        self.name = name
        
        if len(self.edges) != len(self.values) + 1:
            raise ValueError(f"edges must have length values + 1: {len(self.edges)} vs {len(self.values) + 1}")
    
    def integral(self) -> float:
        """Get integral of histogram."""
        return np.sum(self.values)
    
    def add(self, other: 'Histogram1D') -> 'Histogram1D':
        """Add another histogram to this one (in-place)."""
        if not np.allclose(self.edges, other.edges):
            raise ValueError("Cannot add histograms with different binning")
        self.values += other.values
        return self
    
    def scale(self, factor: float) -> 'Histogram1D':
        """Scale histogram by a factor (in-place)."""
        self.values *= factor
        return self
    
    def clone(self, name: str = "") -> 'Histogram1D':
        """Create a copy of this histogram."""
        return Histogram1D(self.values.copy(), self.edges.copy(), name or self.name)
    
    def rebin(self, factor: int) -> 'Histogram1D':
        """
        Rebin histogram by an integer factor.
        
        Args:
            factor: Rebinning factor
            
        Returns:
            New rebinned histogram
        """
        if len(self.values) % factor != 0:
            raise ValueError(f"Number of bins ({len(self.values)}) must be divisible by factor ({factor})")
        
        new_values = self.values.reshape(-1, factor).sum(axis=1)
        new_edges = self.edges[::factor]
        # Add the last edge
        if len(new_edges) == len(new_values):
            new_edges = np.append(new_edges, self.edges[-1])
        
        return Histogram1D(new_values, new_edges, self.name)
    
    def rebin_variable(self, new_edges: np.ndarray) -> 'Histogram1D':
        """
        Rebin histogram with variable bin widths.
        
        Args:
            new_edges: New bin edges
            
        Returns:
            New rebinned histogram
        """
        new_values = np.zeros(len(new_edges) - 1)
        
        for i in range(len(new_edges) - 1):
            low, high = new_edges[i], new_edges[i + 1]
            
            # Find bins that overlap with this new bin
            for j in range(len(self.values)):
                bin_low, bin_high = self.edges[j], self.edges[j + 1]
                
                # Calculate overlap
                overlap_low = max(low, bin_low)
                overlap_high = min(high, bin_high)
                
                if overlap_high > overlap_low:
                    # Fraction of original bin that overlaps
                    fraction = (overlap_high - overlap_low) / (bin_high - bin_low)
                    new_values[i] += self.values[j] * fraction
        
        return Histogram1D(new_values, new_edges, self.name)


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

        # Build a NuisanceGroupRegistry from the config when available.
        # A top-level ``nuisance_groups`` key (list of group dicts) is
        # preferred; if absent the flat ``systematics`` dict is used as a
        # fallback so that every existing config benefits from group-aware
        # filtering without any migration effort.
        if _NUISANCE_GROUPS_AVAILABLE:
            if "nuisance_groups" in self.config:
                self.nuisance_registry: Optional[NuisanceGroupRegistry] = (
                    NuisanceGroupRegistry.from_config(self.config)
                )
            elif self.systematics:
                self.nuisance_registry = NuisanceGroupRegistry.from_config(
                    {"systematics": self.systematics}
                )
            else:
                self.nuisance_registry = NuisanceGroupRegistry()
        else:
            self.nuisance_registry = None
        
        # Create output directory
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Store histogram data
        self.histograms = {}
        
    def read_histograms(self) -> None:
        """Read histograms from input ROOT files using uproot."""
        print("Reading histograms from input files...")
        
        for sample_name, file_info in self.input_files.items():
            file_path = file_info.get('path')
            if not os.path.exists(file_path):
                print(f"Warning: File {file_path} not found for sample {sample_name}")
                continue
            
            print(f"  Processing {sample_name}: {file_path}")
            
            try:
                with uproot.open(file_path) as root_file:
                    # Store file reference
                    if sample_name not in self.histograms:
                        self.histograms[sample_name] = {}
                    
                    # Read histograms from file
                    self._read_histograms_from_file(root_file, sample_name)
            except Exception as e:
                print(f"Error reading file {file_path}: {e}")
                continue
    
    def _read_histograms_from_file(self, root_file, sample_name: str) -> None:
        """
        Read all 1D histograms from a ROOT file using uproot.
        
        Args:
            root_file: Opened uproot file
            sample_name: Name of the sample
        """
        # Iterate through keys in the file
        for key in root_file.keys(cycle=False):
            try:
                obj = root_file[key]
                
                # Check if it's a TH1 histogram (not TH2, TH3, etc.)
                if hasattr(obj, 'axes') and hasattr(obj, 'values'):
                    # Check if it's 1D (uproot v4+ API; avoids the removed
                    # uproot.models.TAxis attribute from uproot v3)
                    if len(obj.axes) == 1:
                        values = obj.values()
                        edges = obj.axis().edges()
                        
                        # Create Histogram1D object
                        hist_name = key.split(';')[0]  # Remove cycle number
                        hist = Histogram1D(values, edges, hist_name)
                        self.histograms[sample_name][hist_name] = hist
                        
            except Exception as e:
                print(f"  Warning: Could not read {key}: {e}")
                continue
    
    def combine_samples(self, region: str, process: str) -> Optional[Histogram1D]:
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
                combined_hist = hist.clone(f"{process}_{region}_{observable}")
            else:
                combined_hist.add(hist)
        
        # Apply rebinning if configured
        if combined_hist and 'rebin' in region_config:
            combined_hist = self._rebin_histogram(combined_hist, region_config['rebin'])
        
        return combined_hist
    
    def _combine_stitched_samples(self, stitch_config: Dict, observable: str, region: str) -> Optional[Histogram1D]:
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
                    combined_hist = hist.clone()
                else:
                    combined_hist.add(hist)
            elif method == 'weighted':
                # Implement weighted combination
                weight = stitch_config.get('weights', {}).get(sample, 1.0)
                hist_weighted = hist.clone()
                hist_weighted.scale(weight)
                if combined_hist is None:
                    combined_hist = hist_weighted
                else:
                    combined_hist.add(hist_weighted)
        
        return combined_hist
    
    def _get_histogram(self, sample: str, observable: str, region: str, systematic: str = "") -> Optional[Histogram1D]:
        """
        Get histogram for a specific sample, observable, region, and systematic.
        
        Args:
            sample: Sample name
            observable: Observable name
            region: Control region name
            systematic: Systematic variation name (empty for nominal)
            
        Returns:
            Histogram or None
        """
        if sample not in self.histograms:
            return None
        
        # Build histogram name with systematic suffix
        syst_suffix = ""
        if systematic:
            syst_suffix = f"_{systematic}"
        
        # Try different naming conventions
        possible_names = [
            f"{observable}_{region}{syst_suffix}",
            f"{region}_{observable}{syst_suffix}",
            f"{observable}{syst_suffix}",
            f"{observable}_{sample}{syst_suffix}",
        ]
        
        for hist_name in possible_names:
            if hist_name in self.histograms[sample]:
                return self.histograms[sample][hist_name]
        
        return None
    
    def _rebin_histogram(self, hist: Histogram1D, rebin_config: Union[int, List[float]]) -> Histogram1D:
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
            return hist.rebin(rebin_config)
        elif isinstance(rebin_config, list):
            # Variable bin width rebinning
            new_edges = np.array(rebin_config, dtype=np.float64)
            return hist.rebin_variable(new_edges)
        
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
        
        # Create ROOT file for combine using uproot
        root_filename = os.path.join(self.output_dir, f"shapes_{region}.root")
        self._write_root_file(root_filename, process_hists, region)
        print(f"  Created ROOT file: {root_filename}")
        
        # Write datacard
        datacard_filename = os.path.join(self.output_dir, f"datacard_{region}.txt")
        self._write_datacard_file(datacard_filename, region, process_hists, root_filename)
        print(f"  Created datacard: {datacard_filename}")
    
    def _write_root_file(self, filename: str, process_hists: Dict[str, Histogram1D], region: str) -> None:
        """
        Write histograms to ROOT file using uproot.
        
        Args:
            filename: Output ROOT filename
            process_hists: Dictionary of process histograms
            region: Control region name
        """
        histograms_to_write = {}
        
        for process, hist in process_hists.items():
            # Write nominal histogram
            hist_dict = self._histogram_to_uproot_dict(hist, process)
            histograms_to_write[process] = hist_dict
            
            # Write systematic variations
            syst_hists = self._get_systematic_histograms(process, hist, region)
            for syst_name, syst_hist in syst_hists.items():
                syst_dict = self._histogram_to_uproot_dict(syst_hist, f"{process}_{syst_name}")
                histograms_to_write[f"{process}_{syst_name}"] = syst_dict
        
        # Write to file
        with uproot.recreate(filename) as f:
            for name, hist_dict in histograms_to_write.items():
                f[name] = hist_dict
    
    def _histogram_to_uproot_dict(self, hist: Histogram1D, name: str) -> Tuple:
        """
        Convert Histogram1D to format suitable for uproot writing.
        
        Args:
            hist: Histogram to convert
            name: Name for the histogram
            
        Returns:
            Tuple suitable for uproot.recreate
        """
        # Return tuple: (values, edges)
        return (hist.values, hist.edges)
    
    def _get_systematic_histograms(self, process: str, nominal_hist: Histogram1D, region: str) -> Dict[str, Histogram1D]:
        """
        Get systematic variation histograms for a process.
        
        Args:
            process: Process name
            nominal_hist: Nominal histogram
            region: Control region name
            
        Returns:
            Dictionary of systematic histograms (key: "systnameUp"/"systnameDown")
        """
        syst_hists = {}
        systematics_list = self._get_systematics_for_process(process, region)
        
        for syst_name, syst_config in systematics_list.items():
            if not syst_config.get('applies_to', {}).get(process, True):
                continue
            
            syst_type = syst_config.get('type', 'shape')
            
            if syst_type == 'shape':
                # Try to read systematic variations from input files
                up_hist = self._get_systematic_from_files(process, region, syst_name, "Up")
                down_hist = self._get_systematic_from_files(process, region, syst_name, "Down")
                
                # If not found in files, create placeholder variations
                if up_hist is None or down_hist is None:
                    variation = syst_config.get('variation', 0.1)
                    up_hist = nominal_hist.clone(f"{process}_{syst_name}Up")
                    up_hist.scale(1.0 + variation)
                    down_hist = nominal_hist.clone(f"{process}_{syst_name}Down")
                    down_hist.scale(1.0 - variation)
                
                syst_hists[f"{syst_name}Up"] = up_hist
                syst_hists[f"{syst_name}Down"] = down_hist
        
        return syst_hists
    
    def _get_systematic_from_files(self, process: str, region: str, syst_name: str, direction: str) -> Optional[Histogram1D]:
        """
        Try to read systematic variation from input files.
        
        Args:
            process: Process name
            region: Control region name
            syst_name: Systematic name
            direction: "Up" or "Down"
            
        Returns:
            Histogram or None if not found
        """
        # Get samples for this process
        process_config = self.processes.get(process, {})
        samples = process_config.get('samples', [process])
        
        region_config = self.control_regions.get(region, {})
        observable = region_config.get('observable')
        
        if not observable:
            return None
        
        # Build systematic name
        systematic = f"{syst_name}{direction}"
        
        combined_hist = None
        
        for sample in samples:
            # Try to get histogram with systematic variation
            hist = self._get_histogram(sample, observable, region, systematic)
            
            if hist is None:
                continue
            
            if combined_hist is None:
                combined_hist = hist.clone()
            else:
                combined_hist.add(hist)
        
        # Apply rebinning if configured and histogram was found
        if combined_hist and 'rebin' in region_config:
            combined_hist = self._rebin_histogram(combined_hist, region_config['rebin'])
        
        return combined_hist
    
    def _get_systematics_for_process(self, process: str, region: str) -> Dict:
        """
        Get systematics that apply to a specific process and region.

        When a :class:`~nuisance_groups.NuisanceGroupRegistry` is available it
        is used as the authoritative source for applicability; the flat
        ``systematics`` dict is used as a fallback.
        
        Args:
            process: Process name
            region: Control region name
            
        Returns:
            Dictionary of applicable systematics
        """
        # Registry-aware path: query by process, region, and datacard usage.
        if self.nuisance_registry is not None:
            syst_map = self.nuisance_registry.get_systematics_for_process_and_region(
                process, region, output_usage="datacard"
            )
            applicable_systematics = {}
            for syst_name in syst_map:
                if syst_name in self.systematics:
                    applicable_systematics[syst_name] = self.systematics[syst_name]
                else:
                    # Synthesise a minimal config entry from the group metadata.
                    group = syst_map[syst_name]
                    applicable_systematics[syst_name] = {
                        "type": group.group_type,
                        "distribution": "shape" if group.group_type == "shape" else "lnN",
                    }
            return applicable_systematics

        # Fallback: original flat-dict logic.
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

    def validate_coverage(
        self, available_variations: Optional[Dict[str, List[str]]] = None
    ) -> List[str]:
        """Validate that all declared systematics have up and down shifts.

        Uses the :class:`~nuisance_groups.NuisanceGroupRegistry` when
        available, otherwise checks the flat ``systematics`` dictionary.

        Parameters
        ----------
        available_variations : dict[str, list[str]] or None
            Mapping from base systematic name to the list of histogram/column
            names found in the output.  When ``None`` an empty dict is used
            (all systematics will be reported as missing).

        Returns
        -------
        list[str]
            Human-readable issue descriptions.  An empty list means all
            declared systematics have complete coverage.
        """
        avail = available_variations or {}
        messages: List[str] = []

        if self.nuisance_registry is not None:
            from nuisance_groups import CoverageSeverity
            issues = self.nuisance_registry.validate_coverage(avail)
            for issue in issues:
                prefix = "ERROR" if issue.severity == CoverageSeverity.ERROR else "WARNING"
                messages.append(f"[{prefix}] {issue.group_name}/{issue.systematic_name}: {issue.message}")
            return messages

        # Fallback: check flat systematics dict.
        for syst_name in self.systematics:
            variations = avail.get(syst_name, [])
            up_names = {v.lower() for v in variations}
            has_up = (syst_name.lower() + "up") in up_names
            has_down = (syst_name.lower() + "down") in up_names
            if syst_name not in avail:
                messages.append(
                    f"[ERROR] {syst_name}: not present in available variations."
                )
            elif not has_up:
                messages.append(f"[ERROR] {syst_name}: missing Up variation.")
            elif not has_down:
                messages.append(f"[ERROR] {syst_name}: missing Down variation.")

        return messages
    
    def _write_datacard_file(self, filename: str, region: str, 
                            process_hists: Dict[str, Histogram1D], root_filename: str) -> None:
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
                n_obs = int(process_hists[data_process].integral())
            else:
                # Sum all backgrounds as pseudo-data
                n_obs = int(sum(h.integral() for h in process_hists.values()))
            
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
            rates = [f"{process_hists[proc].integral():.4f}" for proc in processes]
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
            
            # Correlations (as comments for documentation)
            if 'correlations' in region_config:
                f.write("#" + "-"*80 + "\n")
                f.write("# Correlations\n")
                for corr_name, corr_info in region_config['correlations'].items():
                    f.write(f"# {corr_name}: {corr_info}\n")
    
    def _get_systematics_for_region(self, region: str) -> Dict:
        """
        Get all systematics for a specific region.

        When a :class:`~nuisance_groups.NuisanceGroupRegistry` is available it
        is used as the authoritative source; the flat ``systematics`` dict is
        used as a fallback.
        
        Args:
            region: Control region name
            
        Returns:
            Dictionary of systematics
        """
        if self.nuisance_registry is not None:
            # Collect all systematics in groups that apply to this region
            # and are intended for datacard output.
            applicable_systematics = {}
            for group in self.nuisance_registry.get_groups_for_output("datacard"):
                if not group.applies_to_region(region):
                    continue
                for syst_name in group.systematics:
                    if syst_name not in applicable_systematics:
                        if syst_name in self.systematics:
                            applicable_systematics[syst_name] = self.systematics[syst_name]
                        else:
                            applicable_systematics[syst_name] = {
                                "type": group.group_type,
                                "distribution": "shape" if group.group_type == "shape" else "lnN",
                            }
            return applicable_systematics

        # Fallback: original flat-dict logic.
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

Dependencies:
    pip install uproot awkward numpy pyyaml
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
