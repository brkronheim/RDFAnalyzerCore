#!/usr/bin/env python3
"""
analysis_wrapper.py — Generic Python base class for CMS NanoAOD analyses.

This module provides ``CMSAnalysisBase``, a reusable base class that uses the
``rdfanalyzer`` Python bindings (built from ``core/python/bindings/``) to
run an analysis entirely from Python — no compiled analysis binary required.

Key design goals
----------------
* **Config-driven collections**: object collections are defined in
  ``collections.yaml`` using plain YAML; no C++ code is needed.
* **Extendable analyzer**: the underlying ``rdfanalyzer.Analyzer`` is exposed
  via the :py:attr:`analyzer` property so any ``Define``, ``Filter``, or
  plugin call can be added on top of the default setup.
* **Subclass pattern**: override :py:meth:`build_analysis` to encapsulate
  reusable analysis-specific logic.
* **Standalone script**: can be run from the command line with ``--config``.

Prerequisites
-------------
Build the ``rdfanalyzer`` Python module first::

    source env.sh
    cmake -S . -B build && cmake --build build -j$(nproc)
    export PYTHONPATH=$PWD/build/python:$PYTHONPATH

Quick-start
-----------
**Option 1 — extend the analyzer directly (simplest)**::

    from analysis_wrapper import CMSAnalysisBase

    base = CMSAnalysisBase("cfg.yaml")
    an = base.analyzer          # rdfanalyzer.Analyzer, collections already built
    an.Define("MT", "sqrt(2*tightMuons_pt[0]*MET_pt*(1-cos(tightMuons_phi[0]-MET_phi)))")
    an.Filter("MT_cut", "MT > 40.f")
    base.run()

**Option 2 — subclass for reusable analyses**::

    from analysis_wrapper import CMSAnalysisBase

    class WmuNuAnalysis(CMSAnalysisBase):
        def build_analysis(self):
            an = self.analyzer
            an.Define("MT", "sqrt(2*tightMuons_pt[0]*MET_pt*(1-cos(tightMuons_phi[0]-MET_phi)))")
            an.Filter("MT_cut", "MT > 40.f")

    WmuNuAnalysis("cfg.yaml").run()

**Option 3 — command-line**::

    python analysis_wrapper.py --config cfg.yaml

Collections
-----------
Collections are defined in ``collections.yaml`` (key ``collectionsConfig`` in
``cfg.yaml``) or passed explicitly.  Each entry produces the following columns
on the RDataFrame:

* ``{name}_mask``   — ``RVec<bool>``  selection mask (with overlap removal)
* ``{name}_pt``     — ``RVec<float>`` pT of selected objects
* ``{name}_eta``    — ``RVec<float>`` η of selected objects
* ``{name}_phi``    — ``RVec<float>`` φ of selected objects
* ``{name}_mass``   — ``RVec<float>`` mass of selected objects
* ``{name}_n``      — ``int``         number of selected objects

Example ``collections.yaml`` entry::

    collections:
      - name: tightMuons
        pt: Muon_pt
        eta: Muon_eta
        phi: Muon_phi
        mass: Muon_mass
        cuts:
          - "Muon_tightId"
          - "Muon_pfRelIso04_all < 0.15f"
          - "Muon_pt > 26.0f"
          - "abs(Muon_eta) < 2.4f"
      - name: goodJets
        pt: Jet_pt
        eta: Jet_eta
        phi: Jet_phi
        mass: Jet_mass
        cuts:
          - "Jet_jetId >= 2"
          - "Jet_pt > 30.0f"
          - "abs(Jet_eta) < 4.7f"
        overlap_removal:
          - collection: tightMuons
            dr: 0.4
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# JIT helper: ΔR overlap-removal function declared once in Cling
# ---------------------------------------------------------------------------

# This inline function is declared in ROOT's Cling interpreter the first time
# a CMSAnalysisBase instance builds an overlap-removal collection.  It avoids
# repeated re-declarations and works with all ROOT JIT Define calls.
_DR_OVERLAP_HELPER = """
#ifndef _RDFANALYZER_DR_OVERLAP_MASK_DECLARED_
#define _RDFANALYZER_DR_OVERLAP_MASK_DECLARED_
#include <cmath>
inline ROOT::VecOps::RVec<bool> _rdf_drOverlapMask(
    const ROOT::VecOps::RVec<float>& srcEta,
    const ROOT::VecOps::RVec<float>& srcPhi,
    const ROOT::VecOps::RVec<bool>&  baseMask,
    const ROOT::VecOps::RVec<float>& refEta,
    const ROOT::VecOps::RVec<float>& refPhi,
    float drCut) {
    ROOT::VecOps::RVec<bool> keep = baseMask;
    for (std::size_t i = 0; i < srcEta.size(); i++) {
        if (!keep[i]) continue;
        for (std::size_t j = 0; j < refEta.size(); j++) {
            float deta = srcEta[i] - refEta[j];
            float dphi = srcPhi[i] - refPhi[j];
            while (dphi >  3.14159265f) dphi -= 6.28318530f;
            while (dphi < -3.14159265f) dphi += 6.28318530f;
            if (std::sqrt(deta*deta + dphi*dphi) < drCut) {
                keep[i] = false;
                break;
            }
        }
    }
    return keep;
}
#endif
"""

_dr_helper_declared = False  # module-level flag to declare only once


def _ensure_dr_helper() -> None:
    """Declare the ΔR overlap helper in ROOT's Cling interpreter (idempotent)."""
    global _dr_helper_declared  # noqa: PLW0603
    if _dr_helper_declared:
        return
    try:
        import ROOT  # noqa: PLC0415
        ROOT.gInterpreter.Declare(_DR_OVERLAP_HELPER)
        _dr_helper_declared = True
    except Exception as exc:  # noqa: BLE001
        print(f"[WARNING] Could not declare ΔR helper: {exc}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def validate_config(config_path: str) -> list[str]:
    """
    Perform lightweight validation of a cfg.yaml file.

    Returns a (possibly empty) list of warning/error strings.
    """
    issues: list[str] = []
    path = Path(config_path)
    if not path.exists():
        issues.append(f"Config file not found: {config_path}")
        return issues

    try:
        import yaml  # optional
        with open(path) as fh:
            cfg = yaml.safe_load(fh) or {}

        for key in ("fileList", "treeList", "saveFile", "type"):
            if key not in cfg:
                issues.append(f"Missing required config key: '{key}'")

        cfg_dir = path.parent
        for key in ("triggerConfig", "histogramConfig", "floatConfig",
                    "intConfig", "collectionsConfig"):
            if key in cfg:
                sub = Path(cfg[key])
                if not sub.is_absolute():
                    sub = cfg_dir / sub
                if not sub.exists():
                    issues.append(
                        f"Sub-config file not found: {cfg[key]!r} (key: {key})"
                    )
    except ImportError:
        issues.append(
            "PyYAML not installed — config validation skipped.  "
            "Install with: pip install pyyaml"
        )
    except Exception as exc:  # noqa: BLE001
        issues.append(f"Config parse error: {exc}")

    return issues


# ---------------------------------------------------------------------------
# CMSAnalysisBase
# ---------------------------------------------------------------------------

class CMSAnalysisBase:
    """
    Generic base class for CMS NanoAOD analyses using ``rdfanalyzer``.

    The class handles framework boilerplate (Analyzer construction, plugin
    registration, trigger application, config-driven collection building) and
    exposes the underlying ``rdfanalyzer.Analyzer`` for direct extension.

    Parameters
    ----------
    config_path:
        Path to the main ``cfg.yaml`` configuration file.
    collections_config:
        Explicit path to a ``collections.yaml`` file.  When *None*, the value
        of the ``collectionsConfig`` key in ``cfg.yaml`` is used (if present).

    Examples
    --------
    **Extend the analyzer directly**::

        base = CMSAnalysisBase("cfg.yaml")
        an = base.analyzer   # rdfanalyzer.Analyzer
        an.Define("MT", "sqrt(2*tightMuons_pt[0]*MET_pt*(1-cos(tightMuons_phi[0]-MET_phi)))")
        an.Filter("MT_cut", "MT > 40.f")
        base.run()

    **Subclass pattern**::

        class MyAnalysis(CMSAnalysisBase):
            def build_analysis(self):
                an = self.analyzer
                an.Define("MT", "...")
                an.Filter("MT_cut", "MT > 40.f")

        MyAnalysis("cfg.yaml").run()
    """

    def __init__(
        self,
        config_path: str,
        collections_config: Optional[str] = None,
    ) -> None:
        self._config_path = str(Path(config_path).resolve())
        self._collections_config = (
            str(Path(collections_config).resolve()) if collections_config else None
        )
        self._an = None  # rdfanalyzer.Analyzer, created lazily
        self._initialized = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def analyzer(self):
        """
        The underlying ``rdfanalyzer.Analyzer`` object.

        Access this property to add custom ``Define``, ``Filter``, or plugin
        operations before calling :py:meth:`run`.  The analyzer is initialized
        (with plugins and collections) on first access.

        Returns
        -------
        rdfanalyzer.Analyzer
        """
        if not self._initialized:
            self._initialize()
        return self._an

    def build_analysis(self) -> None:
        """
        Override this method to add analysis-specific logic.

        Called by :py:meth:`run` after the default setup (trigger application,
        collection building) has been completed.  The :py:attr:`analyzer`
        property is available inside this method.

        Example::

            class WmuNuAnalysis(CMSAnalysisBase):
                def build_analysis(self):
                    an = self.analyzer
                    an.Define("MT", "...")
                    an.Filter("MT_cut", "MT > 40.f")
        """

    def run(self) -> None:
        """
        Execute the full analysis.

        Calls :py:meth:`build_analysis` (user overridable), books
        config-driven histograms, and saves all outputs.
        """
        # Ensure the framework is initialized and build_analysis is called
        # before saving, so that any user-defined columns are available.
        _ = self.analyzer  # trigger initialization if not done yet
        self.build_analysis()
        self._an.save()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        """Create the Analyzer, add plugins, apply triggers, build collections."""
        try:
            import rdfanalyzer  # noqa: PLC0415
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "rdfanalyzer module not found.\n"
                "Build with:\n"
                "  cmake -S . -B build && cmake --build build -j$(nproc)\n"
                "  export PYTHONPATH=$PWD/build/python:$PYTHONPATH"
            ) from exc

        self._an = rdfanalyzer.Analyzer(self._config_path)
        self._add_default_plugins()
        self._apply_triggers()
        self._build_collections()
        self._initialized = True

    def _add_default_plugins(self) -> None:
        """Register the standard set of analysis plugins."""
        an = self._an
        an.AddPlugin("trigger", "TriggerManager")
        an.AddPlugin("hist",    "NDHistogramManager")

    def _apply_triggers(self) -> None:
        """Apply all trigger groups matching the sample type in cfg.yaml."""
        self._an.applyAllTriggers("trigger")

    def _build_collections(self) -> None:
        """Discover and load the collections config, then build all collections."""
        col_cfg = self._collections_config
        if col_cfg is None:
            col_cfg = self._find_collections_config()
        if col_cfg is None:
            return  # no collections config — user defines everything manually
        self._load_and_build(col_cfg)

    def _find_collections_config(self) -> Optional[str]:
        """Look up the ``collectionsConfig`` key in cfg.yaml."""
        try:
            import yaml  # noqa: PLC0415
            with open(self._config_path) as fh:
                cfg = yaml.safe_load(fh) or {}
            key = cfg.get("collectionsConfig")
            if not key:
                return None
            path = Path(key)
            if not path.is_absolute():
                path = Path(self._config_path).parent / path
            return str(path) if path.exists() else None
        except Exception:  # noqa: BLE001
            return None

    def _load_and_build(self, collections_yaml: str) -> None:
        """Parse ``collections_yaml`` and define all collection columns."""
        try:
            import yaml  # noqa: PLC0415
        except ImportError:
            print(
                "[WARNING] PyYAML not installed — object collections from config "
                "cannot be built automatically.  Install with: pip install pyyaml",
                file=sys.stderr,
            )
            return

        with open(collections_yaml) as fh:
            data = yaml.safe_load(fh) or {}

        for spec in data.get("collections", []):
            self._build_collection(spec)

    # ------------------------------------------------------------------
    # Collection building
    # ------------------------------------------------------------------

    def _build_collection(self, spec: dict) -> None:
        """
        Build a single collection from its specification dictionary.

        Each collection produces these RDataFrame columns:

        * ``{name}_mask``  — ``RVec<bool>``  selection mask
        * ``{name}_pt``    — ``RVec<float>`` pT of selected objects
        * ``{name}_eta``   — ``RVec<float>`` η
        * ``{name}_phi``   — ``RVec<float>`` φ
        * ``{name}_mass``  — ``RVec<float>`` mass
        * ``{name}_n``     — ``int``         multiplicity
        """
        name   = spec["name"]
        pt_col = spec.get("pt",   f"{name}_pt_raw")
        eta_col = spec.get("eta", f"{name}_eta_raw")
        phi_col = spec.get("phi", f"{name}_phi_raw")
        mass_col = spec.get("mass", f"{name}_mass_raw")
        cuts   = spec.get("cuts", [])
        overlaps = spec.get("overlap_removal", [])

        an = self._an

        # --- Build the base selection mask ---
        if cuts:
            # AND all cut expressions together
            mask_expr = " && ".join(f"({c})" for c in cuts)
        else:
            mask_expr = (
                f"ROOT::VecOps::RVec<bool>({pt_col}.size(), true)"
            )

        if not overlaps:
            # Simple mask — no overlap removal
            an.Define(f"{name}_mask", mask_expr, [])
        else:
            # Define base mask under a private name, then chain overlap removal
            an.Define(f"_{name}_base_mask", mask_expr, [])
            prev_mask_col = f"_{name}_base_mask"

            for idx, ovl in enumerate(overlaps):
                against = ovl["collection"]
                dr_cut  = float(ovl.get("dr", 0.4))
                is_last = (idx == len(overlaps) - 1)
                next_mask_col = f"{name}_mask" if is_last else f"_{name}_or{idx}_mask"

                _ensure_dr_helper()

                # Use the globally-declared _rdf_drOverlapMask helper
                ovl_expr = (
                    f"_rdf_drOverlapMask("
                    f"{eta_col}, {phi_col}, {prev_mask_col}, "
                    f"{against}_eta, {against}_phi, {dr_cut}f)"
                )
                an.Define(next_mask_col, ovl_expr, [])
                prev_mask_col = next_mask_col

        # --- Define the filtered per-object arrays ---
        an.Define(f"{name}_pt",   f"{pt_col}[{name}_mask]",   [])
        an.Define(f"{name}_eta",  f"{eta_col}[{name}_mask]",  [])
        an.Define(f"{name}_phi",  f"{phi_col}[{name}_mask]",  [])
        an.Define(f"{name}_mass", f"{mass_col}[{name}_mask]", [])
        # Multiplicity as a plain int
        an.Define(f"{name}_n",
                  f"(int)ROOT::VecOps::Sum({name}_mask)", [])


# ---------------------------------------------------------------------------
# Results inspector (requires PyROOT)
# ---------------------------------------------------------------------------

class AnalysisResults:
    """
    Thin read-only wrapper around the ROOT output file produced by an analysis.

    Provides helpers for listing histograms and printing cutflow tables.
    Requires PyROOT (``import ROOT``).

    Parameters
    ----------
    root_file:
        Path to the ROOT file written by the analysis (the value of
        ``saveFile`` in ``cfg.yaml``, or the ``*_meta.root`` variant).
    """

    def __init__(self, root_file: str) -> None:
        self.root_file = str(Path(root_file).resolve())
        self._file = None

    # ------------------------------------------------------------------
    # File access
    # ------------------------------------------------------------------

    def _open(self):
        if self._file is None:
            import ROOT  # noqa: PLC0415
            self._file = ROOT.TFile.Open(self.root_file)
            if not self._file or self._file.IsZombie():
                raise RuntimeError(f"Cannot open ROOT file: {self.root_file!r}")
        return self._file

    def close(self) -> None:
        """Explicitly close the ROOT file."""
        if self._file is not None:
            self._file.Close()
            self._file = None

    def __del__(self) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Histogram access
    # ------------------------------------------------------------------

    def list_histograms(self) -> list[str]:
        """Return names of all TH1 objects stored under ``histograms/``."""
        import ROOT  # noqa: PLC0415
        f = self._open()
        names: list[str] = []
        hdir = f.Get("histograms")
        if hdir:
            for key in hdir.GetListOfKeys():
                names.append(key.GetName())
        else:
            for key in f.GetListOfKeys():
                if isinstance(key.ReadObj(), ROOT.TH1):
                    names.append(key.GetName())
        return sorted(names)

    def get_histogram(self, name: str):
        """Return the TH1 named *name* (searches ``histograms/`` first)."""
        f = self._open()
        hdir = f.Get("histograms")
        if hdir:
            h = hdir.Get(name)
            if h:
                return h
        return f.Get(name)

    def print_histogram_summary(self, name: str) -> None:
        """Print a brief statistical summary of histogram *name*."""
        h = self.get_histogram(name)
        if h is None:
            print(f"  Histogram '{name}' not found in {self.root_file!r}")
            return
        print(f"  {name}: entries={h.GetEntries():.0f}  "
              f"mean={h.GetMean():.4f}  integral={h.Integral():.2f}")

    # ------------------------------------------------------------------
    # Cutflow
    # ------------------------------------------------------------------

    def print_cutflow(self) -> None:
        """Print the sequential cutflow table."""
        f = self._open()
        h = f.Get("cutflow")
        if h is None:
            print("No 'cutflow' histogram found.")
            return
        print("\n=== Sequential cutflow ===")
        print(f"  {'Cut':<30s}  {'Yield':>10s}")
        print("  " + "-" * 42)
        for i in range(1, h.GetNbinsX() + 1):
            print(f"  {h.GetXaxis().GetBinLabel(i):<30s}  "
                  f"{h.GetBinContent(i):>10.0f}")

    def print_nminus1(self) -> None:
        """Print the N-1 cutflow table."""
        f = self._open()
        h = f.Get("cutflow_nminus1")
        if h is None:
            print("No 'cutflow_nminus1' histogram found.")
            return
        print("\n=== N-1 cutflow ===")
        print(f"  {'Cut':<30s}  {'N-1 yield':>12s}")
        print("  " + "-" * 44)
        for i in range(1, h.GetNbinsX() + 1):
            print(f"  {h.GetXaxis().GetBinLabel(i):<30s}  "
                  f"{h.GetBinContent(i):>12.0f}")

    def summary(self) -> None:
        """Print cutflow tables and a summary of all histograms."""
        print(f"\n{'='*60}")
        print(f"  Results: {self.root_file}")
        print(f"{'='*60}")
        self.print_cutflow()
        self.print_nminus1()
        histograms = self.list_histograms()
        if histograms:
            print(f"\n=== Histograms ({len(histograms)}) ===")
            for name in histograms:
                self.print_histogram_summary(name)
        print()


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Generic Python base for CMS NanoAOD analyses (rdfanalyzer).",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--config", "-c", default="cfg.yaml",
                   help="Path to cfg.yaml (default: cfg.yaml).")
    p.add_argument("--collections", default=None,
                   help="Path to collections.yaml (overrides cfg.yaml lookup).")
    p.add_argument("--validate-only", action="store_true",
                   help="Validate configs and exit without running.")
    p.add_argument("--results", default=None,
                   help="ROOT output file to inspect after running.")
    return p


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    issues = validate_config(args.config)
    for issue in issues:
        level = "ERROR" if ("Missing" in issue or "not found" in issue) else "WARNING"
        print(f"[{level}] {issue}", file=sys.stderr)

    if args.validate_only:
        return 0 if not any("Missing" in i or "not found" in i for i in issues) else 1

    analysis = CMSAnalysisBase(
        config_path=args.config,
        collections_config=args.collections,
    )
    analysis.run()

    if args.results and Path(args.results).exists():
        try:
            AnalysisResults(args.results).summary()
        except Exception as exc:  # noqa: BLE001
            print(f"[WARNING] Could not load results: {exc}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    sys.exit(main())
