"""Reusable preflight reports and YAML/asset checks for analysis-owned policies."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

class PreflightResult:
    """Collects validation results with pass/fail/warn/skip status."""

    def __init__(self, title: str = "Analysis Preflight") -> None:
        self.title = title
        self.checks: List[Dict[str, Any]] = []

    def ok(self, name: str, detail: str = "") -> None:
        self.checks.append({"name": name, "status": "PASS", "detail": detail})

    def fail(self, name: str, detail: str) -> None:
        self.checks.append({"name": name, "status": "FAIL", "detail": detail})

    def warn(self, name: str, detail: str) -> None:
        self.checks.append({"name": name, "status": "WARN", "detail": detail})

    def skip(self, name: str, detail: str = "") -> None:
        self.checks.append({"name": name, "status": "SKIP", "detail": detail})

    @property
    def passed(self) -> bool:
        return all(c["status"] != "FAIL" for c in self.checks)

    def summary(self) -> str:
        lines = []
        total = len(self.checks)
        n_pass = sum(1 for c in self.checks if c["status"] == "PASS")
        n_fail = sum(1 for c in self.checks if c["status"] == "FAIL")
        n_warn = sum(1 for c in self.checks if c["status"] == "WARN")
        n_skip = sum(1 for c in self.checks if c["status"] == "SKIP")
        lines.append(f"{self.title}: {n_pass}/{total} passed, "
                     f"{n_fail} failed, {n_warn} warnings, {n_skip} skipped")
        for c in self.checks:
            lines.append(f"  [{c['status']:4s}] {c['name']}")
            if c["detail"]:
                lines.append(f"         {c['detail']}")
        return "\n".join(lines)


def resolve_asset_path(base_dir: str, path: str) -> str:
    """Resolve *path* relative to *base_dir* if it is not already absolute."""
    if not path:
        return ""
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(base_dir, path))


def path_exists(path: str) -> bool:
    return bool(path) and os.path.exists(path)


class AnalysisPreflight:
    """Base for checks whose requirements are supplied by an analysis."""

    def __init__(self, analysis_dir: str, title: str = "Analysis Preflight") -> None:
        self.analysis_dir = Path(analysis_dir).resolve()
        self.cfg_dir = self.analysis_dir / "cfg"
        self.result = PreflightResult(title)

    def check_required_files(self, files, *, executable=()):
        """Check analysis-declared paths without applying experiment policy."""
        for label, value in files.items():
            path = Path(value).expanduser()
            if not path.is_absolute():
                path = self.analysis_dir / path
            if not path.is_file():
                self.result.fail(label, f"File not found: {path}")
            elif label in executable and not os.access(path, os.X_OK):
                self.result.fail(label, f"File is not executable: {path}")
            else:
                self.result.ok(label, str(path))

    def check_run_bundle(self, path):
        from run_bundle import verify_bundle, BundleError
        try:
            record = verify_bundle(path)
            self.result.ok("Run bundle", record["bundle_id"])
        except (BundleError, OSError) as exc:
            self.result.fail("Run bundle", str(exc))

    def check_yaml_syntax(self, path: str, label: str) -> None:
        """Validate that a YAML file parses correctly.

        If *path* is absolute, use it directly.  If it starts with ``cfg/``,
        resolve relative to *analysis_dir*.  Otherwise resolve relative to
        *cfg_dir*.
        """
        if os.path.isabs(path):
            full_path = path
        elif path.startswith("cfg/"):
            full_path = resolve_asset_path(str(self.analysis_dir), path)
        else:
            full_path = resolve_asset_path(str(self.cfg_dir), path)
        if not path_exists(full_path):
            self.result.fail(f"YAML syntax [{label}]", f"File not found: {full_path}")
            return
        try:
            with open(full_path) as f:
                data = yaml.safe_load(f)
            if data is None:
                self.result.warn(f"YAML syntax [{label}]", f"File is empty: {full_path}")
            else:
                self.result.ok(f"YAML syntax [{label}]", full_path)
        except yaml.YAMLError as e:
            self.result.fail(f"YAML syntax [{label}]", f"YAML parse error: {e}")


    def _load_yaml(self, rel_path: str, base_dir: Optional[str] = None) -> Tuple[Optional[dict], str]:
        """Load a YAML file, return (data, full_path).

        If *base_dir* is provided, resolve *rel_path* relative to it.
        Otherwise resolve relative to *cfg_dir* (stripping a leading 'cfg/' prefix
        to avoid double-pathing when the caller already includes it).
        """
        if base_dir is not None:
            full_path = resolve_asset_path(base_dir, rel_path)
        else:
            # If rel_path already starts with cfg/, resolve relative to analysis_dir
            if rel_path.startswith("cfg/"):
                full_path = resolve_asset_path(str(self.analysis_dir), rel_path)
            else:
                full_path = resolve_asset_path(str(self.cfg_dir), rel_path)
        if not path_exists(full_path):
            return None, full_path
        try:
            with open(full_path) as f:
                return yaml.safe_load(f), full_path
        except yaml.YAMLError:
            return None, full_path


    def check_histogram_config(self, hist_config_rel_path: str) -> None:
        """Validate histogram config using the shared validator."""
        # Resolve relative to analysis_dir if path starts with cfg/
        if hist_config_rel_path.startswith("cfg/"):
            full_path = resolve_asset_path(str(self.analysis_dir), hist_config_rel_path)
        else:
            full_path = resolve_asset_path(str(self.cfg_dir), hist_config_rel_path)
        if not path_exists(full_path):
            self.result.fail("Histogram config", f"File not found: {full_path}")
            return

        # Re-use the existing validate_histogram_config_file when possible
        try:
            from validate_config import validate_histogram_config_file
            errors, warnings = validate_histogram_config_file(full_path)
            if errors:
                for err in errors:
                    self.result.fail("Histogram config", err)
            else:
                self.result.ok("Histogram config",
                               f"{len(warnings)} warnings" if warnings else "OK")
                for w in warnings:
                    self.result.warn("Histogram config", w)
        except ImportError:
            # Fallback: basic YAML validation using resolved full path
            self.check_yaml_syntax(full_path, "histogram config (fallback)")
        except Exception as e:
            self.result.fail("Histogram config", f"Validation error: {e}")


    def check_dataset_manifest(self, manifest_rel_path: str) -> None:
        """Validate a dataset manifest YAML."""
        data, full_path = self._load_yaml(manifest_rel_path)
        if data is None:
            self.result.fail("Dataset manifest", f"Cannot load: {manifest_rel_path}")
            return

        if not isinstance(data, dict):
            self.result.fail("Dataset manifest", "Top-level must be a YAML mapping")
            return

        # Check that datasets key exists
        if "datasets" not in data:
            self.result.fail("Dataset manifest",
                             "Missing 'datasets' key in manifest")
            return

        datasets = data["datasets"]
        if not isinstance(datasets, list):
            self.result.fail("Dataset manifest", "'datasets' must be a list")
            return

        if len(datasets) == 0:
            self.result.fail("Dataset manifest", "'datasets' list is empty")
            return

        # Check each dataset entry has basic required keys
        seen_names: set = set()
        for idx, ds in enumerate(datasets):
            if not isinstance(ds, dict):
                self.result.fail("Dataset manifest",
                                 f"Entry {idx} is not a YAML mapping")
                continue
            name = ds.get("name", "")
            if not name:
                self.result.fail("Dataset manifest",
                                 f"Entry {idx} missing required 'name'")
                continue
            if name in seen_names:
                self.result.fail("Dataset manifest",
                                 f"Duplicate dataset name: '{name}'")
            seen_names.add(name)

            # Check for DAS or files source
            if not ds.get("das") and not ds.get("files"):
                self.result.warn(f"Dataset manifest [{name}]",
                                 "No 'das' or 'files' source specified")

            # MC datasets should have cross-section
            if ds.get("dtype", "").lower() == "mc":
                if ds.get("xsec") is None:
                    self.result.warn(f"Dataset manifest [{name}]",
                                     "MC dataset missing 'xsec'")

        self.result.ok("Dataset manifest",
                       f"{len(datasets)} datasets, {len(seen_names)} unique names")


    def check_stxs_bin_config(self, bin_config_rel_path: str) -> None:
        """Validate the STXS bin mapping file (stxs_bins.txt)."""
        if bin_config_rel_path.startswith("cfg/"):
            full_path = resolve_asset_path(str(self.analysis_dir), bin_config_rel_path)
        else:
            full_path = resolve_asset_path(str(self.cfg_dir), bin_config_rel_path)
        if not path_exists(full_path):
            self.result.fail("STXS bin config", f"File not found: {full_path}")
            return

        bins: List[Dict] = []
        errors: List[str] = []
        with open(full_path) as f:
            for lineno, line in enumerate(f, start=1):
                line = line.split("#")[0].strip()
                if not line:
                    continue
                parts = line.split()
                if len(parts) < 3:
                    errors.append(f"Line {lineno}: expected '<bin> <label> <categories>', got {len(parts)} tokens")
                    continue
                try:
                    bin_num = int(parts[0])
                except ValueError:
                    errors.append(f"Line {lineno}: bin number '{parts[0]}' is not an integer")
                    continue
                label = parts[1]
                categories = [int(c) for c in parts[2].split(",") if c.strip().isdigit()]
                bins.append({"num": bin_num, "label": label, "categories": categories})

        if errors:
            for err in errors:
                self.result.fail("STXS bin config", err)
            return

        if not bins:
            self.result.fail("STXS bin config", "No valid bin entries found")
            return

        # Check for gap or duplicate bins
        bin_nums = sorted(set(b["num"] for b in bins))
        expected = list(range(1, len(bin_nums) + 1))
        if bin_nums != expected:
            actual_set = set(bin_nums)
            expected_set = set(expected)
            missing = expected_set - actual_set
            extra = actual_set - expected_set
            if missing:
                self.result.warn("STXS bin config",
                                 f"Non-contiguous bin numbers; missing: {sorted(missing)}")
            if extra:
                self.result.warn("STXS bin config",
                                 f"Unexpected bin numbers: {sorted(extra)}")

        self.result.ok("STXS bin config",
                       f"{len(bins)} bins defined (1–{max(bin_nums)})")


    def check_fit_metadata(self, metadata_rel_path: str) -> None:
        """Validate fit_metadata.yaml for sample selectors and process definitions."""
        data, full_path = self._load_yaml(metadata_rel_path, base_dir=str(self.analysis_dir))
        if data is None:
            self.result.fail("Fit metadata", f"Cannot load: {metadata_rel_path}")
            return

        if not isinstance(data, dict):
            self.result.fail("Fit metadata", "Top-level must be a YAML mapping")
            return

        # Check sample_metadata
        sample_meta = data.get("sample_metadata", {})
        if not sample_meta:
            self.result.warn("Fit metadata", "No 'sample_metadata' defined")
        elif not isinstance(sample_meta, dict):
            self.result.fail("Fit metadata", "'sample_metadata' must be a mapping")
        else:
            for sname, sinfo in sample_meta.items():
                if not isinstance(sinfo, dict):
                    self.result.fail(f"Fit metadata [{sname}]",
                                     "Sample metadata entry must be a mapping")
                    continue
                for req in ("process",):
                    if req not in sinfo:
                        self.result.fail(f"Fit metadata [{sname}]",
                                         f"Missing required key '{req}'")

        # Check sample_combinations
        combinations = data.get("sample_combinations", {})
        for cname, cinfo in combinations.items():
            if not isinstance(cinfo, dict):
                self.result.fail(f"Fit metadata [combinations.{cname}]",
                                 "Combination entry must be a mapping")
                continue
            if "method" not in cinfo:
                self.result.fail(f"Fit metadata [combinations.{cname}]",
                                 "Missing 'method' (e.g. 'sum')")
            if "select" not in cinfo:
                self.result.fail(f"Fit metadata [combinations.{cname}]",
                                 "Missing 'select' criteria")

        # Check processes
        processes = data.get("processes", {})
        if not processes:
            self.result.warn("Fit metadata", "No 'processes' defined")
        for pname, pinfo in processes.items():
            if not isinstance(pinfo, dict):
                self.result.fail(f"Fit metadata [processes.{pname}]",
                                 "Process entry must be a mapping")
                continue
            has_select = "select" in pinfo
            has_samples = "samples" in pinfo
            if not has_select and not has_samples:
                self.result.fail(f"Fit metadata [processes.{pname}]",
                                 "Must have 'select' or 'samples' key")
            # Check that samples referenced in combinations exist
            if has_samples:
                for ref in pinfo["samples"]:
                    if ref not in combinations and ref not in sample_meta:
                        self.result.warn(f"Fit metadata [processes.{pname}]",
                                         f"Referenced sample/combination '{ref}' "
                                         f"not found in sample_metadata or combinations")

        self.result.ok("Fit metadata", "Schema validation passed")


    def check_column_coverage(
        self,
        datacard_rel_path: str,
        manifest_path: str,
    ) -> None:
        """Cross-reference datacard systematics against the column inventory.

        When a merged ``OutputManifest`` with a ``column_inventory`` is
        available, this check loads it and reports which datacard-declared
        systematics are not found in the inventory.

        Parameters
        ----------
        datacard_rel_path : str
            Path to the datacard YAML config (``datacard.yaml``).
        manifest_path : str
            Path to a merged ``output_manifest.yaml`` that contains a
            ``column_inventory`` section.
        """
        # Load datacard config
        dc_data, _ = self._load_yaml(datacard_rel_path, base_dir=str(self.analysis_dir))
        if dc_data is None:
            self.result.skip("Column coverage", "Cannot load datacard config")
            return

        # Load manifest
        if not path_exists(manifest_path):
            self.result.skip("Column coverage",
                             f"Manifest not found: {manifest_path}")
            return

        try:
            from output_schema import OutputManifest
            manifest = OutputManifest.load_yaml(manifest_path)
        except Exception as e:
            self.result.skip("Column coverage",
                             f"Cannot load manifest: {e}")
            return

        if manifest.column_inventory is None or not manifest.column_inventory.entries:
            self.result.skip("Column coverage",
                             "No column_inventory in manifest")
            return

        # Build set of available systematic base names from the inventory
        available = {e.base_name.lower() for e in manifest.column_inventory.entries}

        # Check each datacard systematic
        systs = dc_data.get("systematics", {})
        if not systs:
            self.result.warn("Column coverage", "No systematics in datacard config")
            return

        missing = []
        for sname in systs:
            if sname.lower() not in available:
                missing.append(sname)

        if missing:
            self.result.warn("Column coverage",
                             f"Systematics in datacard config not found in "
                             f"column inventory: {missing}")
        else:
            self.result.ok("Column coverage",
                           f"All {len(systs)} datacard systematics match "
                           f"column inventory")


    def check_region_agreement(
        self,
        datacard_rel_path: str,
        hist_config_rel_path: str,
    ) -> None:
        """Verify that datacard region names match histogram channel/region names."""
        dc_data, _ = self._load_yaml(datacard_rel_path, base_dir=str(self.analysis_dir))
        hc_data, hc_path = self._load_yaml(hist_config_rel_path, base_dir=str(self.analysis_dir))

        if dc_data is None or hc_data is None:
            self.result.skip("Region agreement", "Cannot load one of the configs")
            return

        dc_regions = set(dc_data.get("control_regions", {}).keys())
        manifest_regions = set(
            dc_data.get("manifest_defaults", {}).get("region_names", [])
        )
        all_dc_regions = dc_regions | manifest_regions

        # Extract channel/region references from histogram config.
        # Compare channel/control-region axes; sample-category labels are
        # independent of datacard region names.
        hist_region_refs: set = set()
        if isinstance(hc_data, list):
            for entry in hc_data:
                if isinstance(entry, dict):
                    for key in ("channelRegions", "controlRegionRegions"):
                        val = entry.get(key, "")
                        if val:
                            for r in str(val).split(","):
                                hist_region_refs.add(r.strip())

        if all_dc_regions and hist_region_refs:
            missing_in_dc = hist_region_refs - all_dc_regions
            missing_in_hist = all_dc_regions - hist_region_refs
            if missing_in_dc:
                self.result.warn("Region agreement",
                                 f"Histogram channels/regions not in datacard: {missing_in_dc}")
            if missing_in_hist:
                self.result.warn("Region agreement",
                                 f"Datacard regions not in histogram channels: {missing_in_hist}")
            if not missing_in_dc and not missing_in_hist:
                self.result.ok("Region agreement", "Region names match")
        else:
            self.result.skip("Region agreement",
                             "Insufficient region metadata to compare")

