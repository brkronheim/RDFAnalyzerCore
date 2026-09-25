#!/usr/bin/env python3
"""
analysis_wrapper.py — Python base class for the CMS NanoAOD analysis template.

This module provides ``CMSAnalysisBase``, a thin Python wrapper around the C++
analysis setup implemented in ``analysis_setup.h``.  The C++ code is called
directly via the ``cms_analysis_template`` pybind11 module — there is no
Python re-implementation of any analysis logic.

Architecture
------------
::

    analysis_setup.h          ← single C++ implementation
         │
         ├── analysis.cc       ← standalone binary (main calls setupCMSAnalysis)
         │
         └── analysis_bindings.cc  ← pybind11 module (cms_analysis_template)
                  │
                  └── analysis_wrapper.py  ← CMSAnalysisBase (Python base class)

The Python base class:

1. Creates a ``rdfanalyzer.Analyzer`` from ``cfg.yaml``.
2. Calls ``cms_analysis_template.setup_analysis(an._get_analyzer_ptr())`` to
   invoke **the same C++ code** that the standalone binary uses.
3. Exposes the fully-configured ``rdfanalyzer.Analyzer`` object via
   :py:attr:`analyzer` so users can add more ``Define``/``Filter`` calls in
   Python.
4. Calls ``an.save()`` in :py:meth:`run` to trigger the event loop.

Prerequisites
-------------
Build the project first::

    source env.sh
    cmake -S . -B build && cmake --build build -j$(nproc)
    export PYTHONPATH=$PWD/build/python:$PYTHONPATH

Quick-start
-----------
**Option 1 — extend the analyzer after C++ setup (simplest)**::

    from analysis_wrapper import CMSAnalysisBase

    base = CMSAnalysisBase("cfg.yaml")
    an = base.analyzer   # C++ setup already applied; all collections and
                         # regions are defined by the compiled C++ code
    an.Define("myVar", "TransverseMass * 2.0f")
    an.Filter("extra_cut", "myVar > 80.f")
    base.run()

**Option 2 — subclass for reusable analyses**::

    from analysis_wrapper import CMSAnalysisBase

    class WmuNuAnalysis(CMSAnalysisBase):
        def build_analysis(self):
            an = self.analyzer
            an.Define("myVar", "TransverseMass * 2.0f")
            an.Filter("extra_cut", "myVar > 80.f")

    WmuNuAnalysis("cfg.yaml").run()

**Option 3 — command line**::

    python analysis_wrapper.py --config cfg.yaml

Columns available after C++ setup
----------------------------------
After the C++ setup all columns defined by ``analysis_setup.h`` are available.
Key columns (``PhysicsObjectCollection`` unless noted):

* ``looseMuons``, ``tightMuons``              — muon collections
* ``looseElectrons``, ``tightElectrons``      — electron collections
* ``preJets``, ``goodJets``, ``cleanJets``    — jet collections (after ΔR removal)
* ``TransverseMass``                          — Float_t, m_T(μ, MET)
* ``nCleanJets``                              — Int_t, jet multiplicity
* ``LeadMuPt``, ``LeadMuEta``, ``LeadMuPhi`` — Float_t, leading muon kinematics
* ``LeadJetPt``, ``LeadJetEta``              — Float_t, leading jet kinematics
* ``weight_nominal``                          — Float_t, nominal event weight
* ``pass_signal``, ``pass_wCR``, ``pass_topCR`` — bool, region flags
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional


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
        import yaml
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
    Python base class for the CMS analysis template.

    On construction this class:

    1. Creates a ``rdfanalyzer.Analyzer`` from *config_path*.
    2. Imports the ``cms_analysis_template`` pybind11 module (built from
       ``analysis_bindings.cc``) and calls
       ``setup_analysis(an._get_analyzer_ptr())`` to run the full C++ setup
       from ``analysis_setup.h``.

    The resulting :py:attr:`analyzer` object is fully configured and can be
    extended with additional ``Define``/``Filter`` calls in Python.

    Parameters
    ----------
    config_path :
        Path to the main ``cfg.yaml`` configuration file.
    build_dir :
        Path to the CMake build directory.  When *None* the wrapper searches
        standard build-tree locations (``build/``, ``build_release/``, etc.)
        relative to the repository root, or accepts ``RDFANALYZER_BUILD``
        as an environment variable.
    """

    def __init__(
        self,
        config_path: str,
        build_dir: Optional[str] = None,
    ) -> None:
        self._config_path = str(Path(config_path).resolve())
        self._build_dir = build_dir
        self._an = None
        self._initialized = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def analyzer(self):
        """
        The ``rdfanalyzer.Analyzer`` object after C++ setup.

        All collections, plugins, and derived columns from ``analysis_setup.h``
        are already registered on this analyzer.  Add further ``Define``,
        ``Filter``, or ``AddPlugin`` calls to extend the analysis.

        Returns
        -------
        rdfanalyzer.Analyzer
        """
        if not self._initialized:
            self._initialize()
        return self._an

    def build_analysis(self) -> None:
        """
        Override this method to add Python-level analysis logic.

        Called by :py:meth:`run` after the C++ setup has been applied.
        The :py:attr:`analyzer` property is available here.

        Example::

            class WmuNuAnalysis(CMSAnalysisBase):
                def build_analysis(self):
                    an = self.analyzer
                    an.Define("myVar", "TransverseMass * 2.0f")
                    an.Filter("extra_cut", "myVar > 80.f")
        """

    def run(self) -> None:
        """
        Execute the analysis: apply C++ setup, call build_analysis(), save.

        Sequence:
        1. C++ setup via ``cms_analysis_template.setup_analysis(...)``
           (on first :py:attr:`analyzer` access).
        2. :py:meth:`build_analysis` (override to add Python-level logic).
        3. ``rdfanalyzer.Analyzer.save()`` to trigger the event loop.
        """
        _ = self.analyzer  # ensure C++ setup is applied
        self.build_analysis()
        self._an.save()

    # ------------------------------------------------------------------
    # Initialization
    # ------------------------------------------------------------------

    def _initialize(self) -> None:
        """Create the Analyzer, then call the C++ setup."""
        rdfanalyzer = self._import_rdfanalyzer()
        setup_module = self._import_setup_module()

        self._an = rdfanalyzer.Analyzer(self._config_path)
        setup_module.setup_analysis(self._an._get_analyzer_ptr())
        self._initialized = True

    def _import_rdfanalyzer(self):
        """Import rdfanalyzer, adding the build directory to sys.path if needed."""
        build_python = self._find_build_python()
        if build_python and str(build_python) not in sys.path:
            sys.path.insert(0, str(build_python))
        try:
            import rdfanalyzer  # noqa: PLC0415
            return rdfanalyzer
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "rdfanalyzer module not found.\n"
                "Build with:\n"
                "  cmake -S . -B build && cmake --build build -j$(nproc)\n"
                "  export PYTHONPATH=$PWD/build/python:$PYTHONPATH"
            ) from exc

    def _import_setup_module(self):
        """Import cms_analysis_template, adding the build directory to sys.path."""
        build_python = self._find_build_python()
        if build_python and str(build_python) not in sys.path:
            sys.path.insert(0, str(build_python))
        try:
            import cms_analysis_template  # noqa: PLC0415
            return cms_analysis_template
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "cms_analysis_template module not found.\n"
                "Build with:\n"
                "  cmake --build build --target cms_analysis_template_py -j$(nproc)\n"
                "  export PYTHONPATH=$PWD/build/python:$PYTHONPATH"
            ) from exc

    def _find_build_python(self) -> Optional[Path]:
        """Return the ``build/python`` directory, or None if not found."""
        import os  # noqa: PLC0415
        # Explicit override from constructor
        if self._build_dir:
            p = Path(self._build_dir) / "python"
            return p if p.exists() else None
        # Environment variable override
        env_build = os.environ.get("RDFANALYZER_BUILD")
        if env_build:
            p = Path(env_build) / "python"
            return p if p.exists() else None
        # Walk up from this file to find the repository root
        here = Path(__file__).resolve().parent
        for candidate in [here] + list(here.parents):
            if (candidate / "CMakeLists.txt").exists():
                for build_name in ("build", "build_release", "build_debug"):
                    p = candidate / build_name / "python"
                    if p.exists():
                        return p
                break
        return None


# ---------------------------------------------------------------------------
# Results inspector (requires PyROOT)
# ---------------------------------------------------------------------------

class AnalysisResults:
    """
    Read-only wrapper around the ROOT output file produced by an analysis.

    Provides helpers for listing histograms and printing cutflow tables.
    Requires PyROOT.

    Parameters
    ----------
    root_file :
        Path to the ROOT file written by the analysis (the ``saveFile``
        value in ``cfg.yaml``).
    """

    def __init__(self, root_file: str) -> None:
        self.root_file = str(Path(root_file).resolve())
        self._file = None

    def _open(self):
        if self._file is None:
            import ROOT  # noqa: PLC0415
            self._file = ROOT.TFile.Open(self.root_file)
            if not self._file or self._file.IsZombie():
                raise RuntimeError(f"Cannot open ROOT file: {self.root_file!r}")
        return self._file

    def close(self) -> None:
        if self._file is not None:
            self._file.Close()
            self._file = None

    def __del__(self) -> None:
        self.close()

    def list_histograms(self) -> list[str]:
        """Return names of all TH1 objects in the file."""
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
        h = self.get_histogram(name)
        if h is None:
            print(f"  Histogram '{name}' not found in {self.root_file!r}")
            return
        print(f"  {name}: entries={h.GetEntries():.0f}  "
              f"mean={h.GetMean():.4f}  integral={h.Integral():.2f}")

    def print_cutflow(self) -> None:
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
        description="CMS analysis template — Python wrapper.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument("--config", "-c", default="cfg.yaml",
                   help="Path to cfg.yaml (default: cfg.yaml).")
    p.add_argument("--build-dir", default=None,
                   help="Path to CMake build directory (default: auto-detect).")
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
        build_dir=args.build_dir,
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
