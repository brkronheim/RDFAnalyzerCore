"""
VHbb STXS preflight readiness gate.

Validates all configuration files, dataset manifests, STXS bin mappings,
fit metadata, datacard configs, and referenced assets before launching
expensive Law production jobs.

Usage
-----
    python -m vhbb_stxs_preflight \\
        --analysis-dir analyses/VHbbSTXS/VHqqRDF \\
        --pass-config cfg/pass3_hist_from_skim.yaml \\
        --dataset-manifest cfg/datasets_vhbb.yaml \\
        --year 2022_preEE

Or as a standalone check::

    from vhbb_stxs_preflight import VHbbStxsPreflight

    preflight = VHbbStxsPreflight("analyses/VHbbSTXS/VHqqRDF")
    report = preflight.run()
    print(report.summary())
"""

from __future__ import annotations

import os
import sys
import glob
import gzip
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure the repo's core/python is on the path for sibling imports
_repo_root = Path(__file__).resolve().parent.parent.parent
_core_python = str(_repo_root / "core" / "python")
if _core_python not in sys.path:
    sys.path.insert(0, _core_python)

try:
    import yaml
except ImportError:
    # Fallback: try adding the system python3.9 site-packages for LCG environments
    import site
    for candidate in [
        "/usr/lib64/python3.9/site-packages",
        "/usr/lib/python3.9/site-packages",
    ]:
        if os.path.isdir(candidate) and candidate not in sys.path:
            sys.path.insert(0, candidate)
    try:
        import yaml
    except ImportError:
        # Will raise at point of use - allow module to load for doc/import purposes
        yaml = None  # type: ignore[assignment]

# Lazy import of OutputManifest to avoid circular dependencies
try:
    from output_schema import OutputManifest
except ImportError:
    OutputManifest = None  # type: ignore[assignment,misc]

# ---------------------------------------------------------------------------
# PreflightResult — structured pass/fail/warn output
# ---------------------------------------------------------------------------


class PreflightResult:
    """Collects validation results with pass/fail/warn/skip status."""

    def __init__(self) -> None:
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
        lines.append(f"VHbb STXS Preflight: {n_pass}/{total} passed, "
                     f"{n_fail} failed, {n_warn} warnings, {n_skip} skipped")
        for c in self.checks:
            lines.append(f"  [{c['status']:4s}] {c['name']}")
            if c["detail"]:
                lines.append(f"         {c['detail']}")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Path resolution helpers
# ---------------------------------------------------------------------------

def _abs_path(base_dir: str, path: str) -> str:
    """Resolve *path* relative to *base_dir* if it is not already absolute."""
    if not path:
        return ""
    if os.path.isabs(path):
        return os.path.normpath(path)
    return os.path.normpath(os.path.join(base_dir, path))


def _exists(path: str) -> bool:
    return bool(path) and os.path.exists(path)


def _resolve_cvmfs(path: str) -> str:
    """Resolve a correctionsBase-relative CVMFS path if possible."""
    corrections_base = "/cvmfs/cms-griddata.cern.ch/cat/metadata"
    if not path:
        return path
    # If already absolute, check existence
    if os.path.exists(path):
        return path
    # Try prepending correctionsBase
    candidate = os.path.join(corrections_base, path)
    if os.path.exists(candidate):
        return candidate
    return path


# ---------------------------------------------------------------------------
# Core preflight class
# ---------------------------------------------------------------------------


class VHbbStxsPreflight:
    """Run preflight checks for the VHbb STXS analysis.

    Parameters
    ----------
    analysis_dir : str
        Path to the VHqqRDF analysis directory (containing cfg/, aux/, src/, etc.).
    """

    def __init__(self, analysis_dir: str) -> None:
        self.analysis_dir = Path(analysis_dir).resolve()
        self.cfg_dir = self.analysis_dir / "cfg"
        self.aux_dir = self.analysis_dir / "aux"
        self.src_dir = self.analysis_dir / "src"
        self.base_dir = str(self.analysis_dir.parent.parent.parent)  # repo root heuristic
        self.result = PreflightResult()

    # -----------------------------------------------------------------------
    # 1. YAML syntax & schema checks
    # -----------------------------------------------------------------------

    def check_yaml_syntax(self, path: str, label: str) -> None:
        """Validate that a YAML file parses correctly.

        If *path* is absolute, use it directly.  If it starts with ``cfg/``,
        resolve relative to *analysis_dir*.  Otherwise resolve relative to
        *cfg_dir*.
        """
        if os.path.isabs(path):
            full_path = path
        elif path.startswith("cfg/"):
            full_path = _abs_path(str(self.analysis_dir), path)
        else:
            full_path = _abs_path(str(self.cfg_dir), path)
        if not _exists(full_path):
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
            full_path = _abs_path(base_dir, rel_path)
        else:
            # If rel_path already starts with cfg/, resolve relative to analysis_dir
            if rel_path.startswith("cfg/"):
                full_path = _abs_path(str(self.analysis_dir), rel_path)
            else:
                full_path = _abs_path(str(self.cfg_dir), rel_path)
        if not _exists(full_path):
            return None, full_path
        try:
            with open(full_path) as f:
                return yaml.safe_load(f), full_path
        except yaml.YAMLError:
            return None, full_path

    # -----------------------------------------------------------------------
    # 2. Pass config validation
    # -----------------------------------------------------------------------

    def check_pass_config(self, config_rel_path: str, pass_label: str) -> None:
        """Validate a pass-level YAML config for required keys and asset refs."""
        data, full_path = self._load_yaml(config_rel_path, base_dir=str(self.analysis_dir))
        if data is None:
            self.result.fail(f"Pass config [{pass_label}]",
                             f"Cannot load: {config_rel_path}")
            return

        if not isinstance(data, dict):
            self.result.fail(f"Pass config [{pass_label}]",
                             "Top-level must be a YAML mapping")
            return

        # Required keys for any pass
        required_keys = ["treeList", "saveDirectory", "saveTree"]
        missing = [k for k in required_keys if k not in data]
        if missing:
            self.result.fail(f"Pass config [{pass_label}]",
                             f"Missing required keys: {missing}")
        else:
            self.result.ok(f"Pass config [{pass_label}]", f"All required keys present")

        # Check referenced config files exist (relative to cfg_dir)
        cfg_refs = [
            ("histogramConfig", "histogram config"),
            ("triggerConfig", "trigger config"),
            ("onnxConfig", "ONNX config"),
            ("kinematicFitConfig", "kinematic fit config"),
            ("correctedJetCollectionConfig", "corrected jet config"),
            ("correctedElectronCollectionConfig", "corrected electron config"),
            ("correctedMuonCollectionConfig", "corrected muon config"),
            ("correctionConfig", "correction config"),
            ("floatConfig", "float config"),
            ("intConfig", "int config"),
            ("optionalBranchesConfig", "optional branches config"),
            ("fitMetadataConfig", "fit metadata config"),
            ("manifestMetadataConfig", "manifest metadata config"),
        ]
        for key, label in cfg_refs:
            if key in data and data[key]:
                ref_path = data[key]
                if ref_path.startswith("cfg/"):
                    full = self.cfg_dir / ref_path[4:]
                else:
                    full = _abs_path(str(self.cfg_dir), ref_path)
                if not os.path.exists(full):
                    self.result.warn(f"Pass config [{pass_label}]",
                                     f"Referenced {label} not found: {ref_path}")

        # Check golden JSON config (optional — data-only)
        if "goldenJsonConfig" in data and data["goldenJsonConfig"]:
            gj_path = data["goldenJsonConfig"]
            if gj_path.startswith("cfg/"):
                full = self.cfg_dir / gj_path[4:]
            else:
                full = _abs_path(str(self.cfg_dir), gj_path)
            if not os.path.exists(full):
                self.result.warn(f"Pass config [{pass_label}]",
                                 f"Golden JSON config file not found: {gj_path}")
            else:
                # Try loading it to validate the golden JSON paths within.
                try:
                    with open(full) as f:
                        gj_list = yaml.safe_load(f)
                    if isinstance(gj_list, list) and gj_list:
                        for gj_entry in gj_list:
                            if not os.path.exists(str(gj_entry)):
                                self.result.warn(
                                    f"Pass config [{pass_label}]",
                                    f"Golden JSON file referenced in {gj_path} "
                                    f"not found: {gj_entry}")
                except Exception:
                    pass  # parse errors handled separately
        else:
            self.result.warn(f"Pass config [{pass_label}]",
                             "goldenJsonConfig not set — data samples will not "
                             "be filtered by certified luminosity sections.")

        # Check JetVetoMap config (optional stub — not yet implemented)
        if "jetVetoMapConfig" in data and data["jetVetoMapConfig"]:
            jv_path = data["jetVetoMapConfig"]
            if not os.path.exists(jv_path):
                self.result.warn(f"Pass config [{pass_label}]",
                                 f"JetVetoMap config file not found: {jv_path}")
            self.result.warn(f"Pass config [{pass_label}]",
                             "JetVetoMap filtering is a stub; no filtering applied. "
                             "Requires CMS JetMET validation of Run 3 payloads.")
        else:
            self.result.warn(f"Pass config [{pass_label}]",
                             "jetVetoMapConfig not set — JetVetoMap filtering "
                             "is disabled (expected until CMS JetMET validation).")

        # Check STXS config references (paths are relative to analysis_dir)
        stxs_keys = [
            ("stxsBinConfig", "STXS bin config"),
            ("stxsAuxDirectory", "STXS aux directory"),
        ]
        for key, label in stxs_keys:
            if key in data and data[key]:
                ref_path = data[key]
                full = _abs_path(str(self.analysis_dir), ref_path)
                if not os.path.exists(full):
                    self.result.warn(f"Pass config [{pass_label}]",
                                     f"Referenced {label} not found: {full}")

        # Check DNN normalization file
        if "dnnNormalizationFile" in data and data["dnnNormalizationFile"]:
            dnn_path = _abs_path(str(self.analysis_dir), data["dnnNormalizationFile"])
            if not os.path.exists(dnn_path):
                self.result.warn(f"Pass config [{pass_label}]",
                                 f"DNN normalization file not found: {dnn_path}")

        # Check V+jets NLO assets (pass 3 specific)
        vjet_keys = [
            ("vjetNloTransformer", "NLO transformer ONNX"),
            ("vjetStitchingCorrectionFile", "Stitching correction file"),
            ("vjetNjetPtCorrectionFile", "NJetPt correction file"),
        ]
        for key, label in vjet_keys:
            if key in data and data[key]:
                vjet_path = _abs_path(str(self.analysis_dir), data[key])
                if not os.path.exists(vjet_path):
                    self.result.warn(f"Pass config [{pass_label}]",
                                     f"{label} not found: {vjet_path}")

    # -----------------------------------------------------------------------
    # 3. Dataset manifest validation
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # 4. STXS bin config validation
    # -----------------------------------------------------------------------

    def check_stxs_bin_config(self, bin_config_rel_path: str) -> None:
        """Validate the STXS bin mapping file (stxs_bins.txt)."""
        if bin_config_rel_path.startswith("cfg/"):
            full_path = _abs_path(str(self.analysis_dir), bin_config_rel_path)
        else:
            full_path = _abs_path(str(self.cfg_dir), bin_config_rel_path)
        if not _exists(full_path):
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

    # -----------------------------------------------------------------------
    # 5. Fit metadata validation
    # -----------------------------------------------------------------------

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

    # -----------------------------------------------------------------------
    # 6. Datacard config validation
    # -----------------------------------------------------------------------

    def check_datacard_config(self, datacard_rel_path: str) -> None:
        """Validate datacard_vhbb_stxs.yaml for region/observable/nuisance policy."""
        data, full_path = self._load_yaml(datacard_rel_path, base_dir=str(self.analysis_dir))
        if data is None:
            self.result.fail("Datacard config", f"Cannot load: {datacard_rel_path}")
            return

        if not isinstance(data, dict):
            self.result.fail("Datacard config", "Top-level must be a YAML mapping")
            return

        # Check control_regions
        crs = data.get("control_regions", {})
        if not crs:
            self.result.warn("Datacard config", "No 'control_regions' defined")
        elif not isinstance(crs, dict):
            self.result.fail("Datacard config", "'control_regions' must be a mapping")
        else:
            for rname, rinfo in crs.items():
                if not isinstance(rinfo, dict):
                    self.result.fail(f"Datacard config [region {rname}]",
                                     "Region entry must be a mapping")
                    continue
                if "observable" not in rinfo:
                    self.result.fail(f"Datacard config [region {rname}]",
                                     "Missing 'observable'")
                if "data_process" not in rinfo:
                    self.result.fail(f"Datacard config [region {rname}]",
                                     "Missing 'data_process'")

        # Check manifest_defaults
        defaults = data.get("manifest_defaults", {})
        if defaults:
            region_names = defaults.get("region_names", [])
            if region_names and crs:
                for rn in region_names:
                    if rn not in crs:
                        self.result.warn("Datacard config",
                                         f"manifest_defaults region '{rn}' "
                                         f"not found in control_regions")

        # Check systematics
        systs = data.get("systematics", {})
        if not systs:
            self.result.warn("Datacard config", "No 'systematics' defined")
        elif not isinstance(systs, dict):
            self.result.fail("Datacard config", "'systematics' must be a mapping")
        else:
            for sname, sinfo in systs.items():
                if not isinstance(sinfo, dict):
                    self.result.fail(f"Datacard config [syst {sname}]",
                                     "Systematic entry must be a mapping")
                    continue
                for req in ("type", "distribution"):
                    if req not in sinfo:
                        self.result.fail(f"Datacard config [syst {sname}]",
                                         f"Missing required key '{req}'")

        self.result.ok("Datacard config",
                       f"{len(crs)} regions, {len(systs)} systematics")

    # -----------------------------------------------------------------------
    # 7. Column coverage check (cross-reference datacard systematics against
    #    the SystematicColumnInventory from a merged manifest)
    # -----------------------------------------------------------------------

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
            Path to the datacard YAML config (``datacard_vhbb_stxs.yaml``).
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
        if not _exists(manifest_path):
            self.result.skip("Column coverage",
                             f"Manifest not found: {manifest_path}")
            return

        try:
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

    # -----------------------------------------------------------------------
    # 8. Correction payload existence check
    # -----------------------------------------------------------------------

    def check_correction_payloads(self, year: str, year_settings_rel_path: str) -> None:
        """Verify that correction payload files for a given year are accessible."""
        data, full_path = self._load_yaml(year_settings_rel_path, base_dir=str(self.analysis_dir))
        if data is None or "years" not in data:
            self.result.fail(f"Correction payloads [{year}]",
                             f"Cannot load year settings: {year_settings_rel_path}")
            return

        years = data["years"]
        if year not in years:
            self.result.fail(f"Correction payloads [{year}]",
                             f"Year '{year}' not found in year_settings.yaml")
            return

        yr = years[year]
        corrections_base = "/cvmfs/cms-griddata.cern.ch/cat/metadata"

        # Check CVMFS payload files
        payload_keys = [
            ("btvBTagFile", "BTV b-tag file"),
            ("btvCTagFile", "BTV c-tag file"),
            ("pileupFile", "Pileup file"),
            ("muonFile", "Muon file"),
            ("electronFile", "Electron file"),
            ("jmeFile", "JME file"),
        ]

        for key, label in payload_keys:
            rel = yr.get(key, "")
            if not rel:
                self.result.warn(f"Correction payloads [{year}]",
                                 f"No '{key}' defined")
                continue
            candidate = os.path.join(corrections_base, rel)
            if not os.path.exists(candidate):
                # Try gzipped version
                candidate_gz = candidate + ".gz"
                if not os.path.exists(candidate_gz):
                    self.result.warn(f"Correction payloads [{year}]",
                                     f"{label} not found: {candidate}")

        # Check golden JSON file (optional; absolute paths from year_settings)
        golden_json = yr.get("goldenJsonFile", "")
        if golden_json:
            if not os.path.exists(golden_json):
                self.result.warn(f"Correction payloads [{year}]",
                                 f"Golden JSON file not found: {golden_json}")

        # Check trigger SF root directory (relative to repo root)
        trigger_root = yr.get("triggerSfRoot", "")
        if trigger_root:
            # The triggerSfRoot is relative to the repo root, e.g. analyses/VHbbSTXS/VHqqRDF/aux/trigger_Sf
            repo_root = self.analysis_dir.parent.parent.parent  # VHqqRDF -> VHbbSTXS -> analyses -> repo
            trigger_path = _abs_path(str(repo_root), trigger_root)
            if not os.path.isdir(trigger_path):
                self.result.warn(f"Correction payloads [{year}]",
                                 f"Trigger SF directory not found: {trigger_path}")

        self.result.ok(f"Correction payloads [{year}]",
                       "Payload path check completed")

    # -----------------------------------------------------------------------
    # 8. Histogram config validation (delegates to validate_config)
    # -----------------------------------------------------------------------

    def check_histogram_config(self, hist_config_rel_path: str) -> None:
        """Validate histogram config using the shared validator."""
        # Resolve relative to analysis_dir if path starts with cfg/
        if hist_config_rel_path.startswith("cfg/"):
            full_path = _abs_path(str(self.analysis_dir), hist_config_rel_path)
        else:
            full_path = _abs_path(str(self.cfg_dir), hist_config_rel_path)
        if not _exists(full_path):
            self.result.fail("Histogram config", f"File not found: {full_path}")
            return

        # Re-use the existing validate_histogram_config_file when possible
        try:
            from core.python.validate_config import validate_histogram_config_file
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

    # -----------------------------------------------------------------------
    # 9. Auxiliary asset checks
    # -----------------------------------------------------------------------

    def check_aux_assets(self) -> None:
        """Verify existence of key auxiliary files."""
        aux_dir = self.aux_dir
        if not aux_dir.is_dir():
            self.result.warn("Aux assets", f"Aux directory not found: {aux_dir}")
            return

        # Check DNN normalization file
        dnn_norm = aux_dir / "dnn" / "normDF_DNN.csv"
        if dnn_norm.exists():
            self.result.ok("Aux assets [DNN norm]", str(dnn_norm))
        else:
            self.result.warn("Aux assets [DNN norm]",
                             f"Not found: {dnn_norm}")

        # Check trigger SF directory
        trigger_sf = aux_dir / "trigger_Sf"
        if trigger_sf.is_dir():
            sf_files = list(trigger_sf.glob("*.root"))
            self.result.ok("Aux assets [trigger SF]",
                           f"{trigger_sf} ({len(sf_files)} root files)")
        else:
            self.result.warn("Aux assets [trigger SF]",
                             f"Not found: {trigger_sf}")

        # Check STXS aux directory
        stxs_aux = aux_dir / "stxs"
        if stxs_aux.is_dir():
            stxs_files = list(stxs_aux.glob("*.root"))
            self.result.ok("Aux assets [STXS aux]",
                           f"{stxs_aux} ({len(stxs_files)} root files)")
        else:
            self.result.warn("Aux assets [STXS aux]",
                             f"Not found: {stxs_aux}")

        # Check ONNX models directory
        onnx_models = aux_dir / "onnx"
        if onnx_models.is_dir():
            onnx_files = list(onnx_models.glob("*.onnx"))
            self.result.ok("Aux assets [ONNX models]",
                           f"{onnx_models} ({len(onnx_files)} onnx files)")
        else:
            self.result.warn("Aux assets [ONNX models]",
                             f"Not found: {onnx_models}")

    # -----------------------------------------------------------------------
    # 10. Histogram/datacard region agreement
    # -----------------------------------------------------------------------

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
        # Only consider channelRegions (2L, 1L, 0L) and controlRegionRegions
        # (SR, CR_1B, etc.).  sampleCategoryRegions contain STXS bin labels
        # (wh_301, zh_401, etc.) which are not datacard region names.
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

    # -----------------------------------------------------------------------
    # Main run method
    # -----------------------------------------------------------------------

    def run(
        self,
        *,
        pass_configs: Optional[List[str]] = None,
        dataset_manifest: Optional[str] = None,
        year: Optional[str] = None,
        year_settings: str = "cfg/year_settings.yaml",
        stxs_bin_config: str = "cfg/stxs_bins.txt",
        datacard_config: str = "cfg/datacard_vhbb_stxs.yaml",
        fit_metadata: str = "cfg/fit_metadata.yaml",
        histogram_configs: Optional[List[str]] = None,
    ) -> PreflightResult:
        """Run all preflight checks.

        Parameters
        ----------
        pass_configs : list of str, optional
            Relative paths to pass config files to validate.
        dataset_manifest : str, optional
            Relative path to a dataset manifest YAML.
        year : str, optional
            Year token to validate correction payloads for (e.g. '2022_preEE').
        year_settings : str
            Relative path to year_settings.yaml.
        stxs_bin_config : str
            Relative path to stxs_bins.txt.
        datacard_config : str
            Relative path to datacard config YAML.
        fit_metadata : str
            Relative path to fit metadata YAML.
        histogram_configs : list of str, optional
            Relative paths to histogram config files.

        Returns
        -------
        PreflightResult
            Collected pass/fail/warn results.
        """
        # --- Pass configs ---
        if pass_configs:
            for pcfg in pass_configs:
                self.check_pass_config(pcfg, pcfg)
        else:
            # Check default pass configs
            defaults = [
                ("cfg/pass1_hist_derivations.yaml", "pass1_hist_derivations"),
                ("cfg/pass2_skim_rucio.yaml", "pass2_skim_rucio"),
                ("cfg/pass3_hist_from_skim.yaml", "pass3_hist_from_skim"),
            ]
            for rel, label in defaults:
                self.check_pass_config(rel, label)

        # --- Dataset manifest ---
        if dataset_manifest:
            self.check_dataset_manifest(dataset_manifest)

        # --- STXS bin config ---
        self.check_stxs_bin_config(stxs_bin_config)

        # --- Fit metadata ---
        self.check_fit_metadata(fit_metadata)

        # --- Datacard config ---
        self.check_datacard_config(datacard_config)

        # --- Histogram configs ---
        if histogram_configs:
            for hcfg in histogram_configs:
                self.check_histogram_config(hcfg)
        else:
            # Check defaults
            for hcfg in ["cfg/histograms_pass1_derivations.yaml",
                         "cfg/histograms_stxs.yaml"]:
                full = _abs_path(str(self.cfg_dir), hcfg)
                if os.path.exists(full):
                    self.check_histogram_config(hcfg)

        # --- Auxiliary assets ---
        self.check_aux_assets()

        # --- Year-specific correction payloads ---
        if year:
            self.check_correction_payloads(year, year_settings)

        # --- Region agreement ---
        self.check_region_agreement(datacard_config,
                                    "cfg/histograms_stxs.yaml")

        return self.result


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="VHbb STXS Preflight Readiness Gate",
    )
    parser.add_argument(
        "--analysis-dir",
        default=None,
        help="Path to VHqqRDF analysis directory (default: auto-detect from CWD)",
    )
    parser.add_argument(
        "--pass-config",
        action="append",
        dest="pass_configs",
        help="Pass config file to validate (can be repeated)",
    )
    parser.add_argument(
        "--dataset-manifest",
        help="Dataset manifest YAML to validate",
    )
    parser.add_argument(
        "--year",
        help="Year token for correction payload validation (e.g. 2022_preEE)",
    )
    parser.add_argument(
        "--year-settings",
        default="cfg/year_settings.yaml",
        help="Year settings file relative to cfg_dir",
    )
    parser.add_argument(
        "--stxs-bin-config",
        default="cfg/stxs_bins.txt",
        help="STXS bin config relative to cfg_dir",
    )
    parser.add_argument(
        "--datacard-config",
        default="cfg/datacard_vhbb_stxs.yaml",
        help="Datacard config relative to cfg_dir",
    )
    parser.add_argument(
        "--fit-metadata",
        default="cfg/fit_metadata.yaml",
        help="Fit metadata config relative to cfg_dir",
    )
    parser.add_argument(
        "--histogram-config",
        action="append",
        dest="histogram_configs",
        help="Histogram config to validate (can be repeated)",
    )

    args = parser.parse_args()

    # Auto-detect analysis dir from CWD if not specified
    analysis_dir = args.analysis_dir
    if analysis_dir is None:
        cwd = os.getcwd()
        # First check direct paths
        candidates = [
            cwd,
            os.path.join(cwd, "VHqqRDF"),
            os.path.join(cwd, "analyses", "VHbbSTXS", "VHqqRDF"),
        ]
        # Walk up from cwd looking for cfg/ directory
        here = cwd
        for _ in range(10):
            here = os.path.dirname(here)
            if here == os.path.dirname(here):  # reached filesystem root
                break
            candidates.append(here)
            candidates.append(os.path.join(here, "VHqqRDF"))
            candidates.append(os.path.join(here, "analyses", "VHbbSTXS", "VHqqRDF"))
        for candidate in candidates:
            if os.path.isdir(os.path.join(candidate, "cfg")):
                analysis_dir = candidate
                break
        if analysis_dir is None:
            analysis_dir = candidates[0]

    preflight = VHbbStxsPreflight(analysis_dir)
    result = preflight.run(
        pass_configs=args.pass_configs,
        dataset_manifest=args.dataset_manifest,
        year=args.year,
        year_settings=args.year_settings,
        stxs_bin_config=args.stxs_bin_config,
        datacard_config=args.datacard_config,
        fit_metadata=args.fit_metadata,
        histogram_configs=args.histogram_configs,
    )

    print(result.summary())

    if not result.passed:
        sys.exit(1)


if __name__ == "__main__":
    main()
