#!/usr/bin/env python3
"""
analysis_wrapper.py — Python high-level wrapper for the CMS analysis template.

This script provides a convenient Python API around the C++ analysis binary.
It handles:
  - Config file validation before launching the analysis.
  - Building the analysis executable if needed.
  - Running the C++ binary with the correct config.
  - Loading and displaying output histograms and cutflow tables.

Usage (standalone):
    python analysis_wrapper.py --config cfg.yaml [options]

Usage as a library:
    from analysis_wrapper import CMSAnalysisRunner, AnalysisResults
    runner = CMSAnalysisRunner("cfg.yaml")
    runner.run()
    results = AnalysisResults("output/cms_template_output.root")
    results.print_cutflow()
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_repo_root() -> Path:
    """Walk up from this file to find the repository root (contains CMakeLists.txt)."""
    here = Path(__file__).resolve().parent
    for candidate in [here] + list(here.parents):
        if (candidate / "CMakeLists.txt").exists():
            return candidate
    return here  # fallback


def _find_binary(config_dir: Path) -> Optional[Path]:
    """Search for the compiled analysis binary relative to the build tree."""
    repo_root = _find_repo_root()
    # Typical CMake build directories
    for build_name in ("build", "build_release", "build_debug"):
        candidate = repo_root / build_name / "analyses" / "CMSAnalysisTemplate" / "cms_analysis_template"
        if candidate.exists():
            return candidate
    return None


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------

def validate_config(config_path: str) -> list[str]:
    """
    Perform lightweight validation of a cfg.yaml file.

    Returns a (possibly empty) list of warning/error strings.
    Does not import PyYAML; parses only the keys it cares about.
    """
    warnings = []
    path = Path(config_path)
    if not path.exists():
        warnings.append(f"Config file not found: {config_path}")
        return warnings

    try:
        import yaml  # optional dependency
        with open(path) as fh:
            cfg = yaml.safe_load(fh)

        required_keys = ["fileList", "treeList", "saveFile", "type"]
        for key in required_keys:
            if key not in cfg:
                warnings.append(f"Missing required config key: '{key}'")

        # Validate referenced sub-configs exist
        config_dir = path.parent
        for key in ("triggerConfig", "histogramConfig", "floatConfig", "intConfig"):
            if key in cfg:
                sub = Path(cfg[key])
                if not sub.is_absolute():
                    sub = config_dir / sub
                if not sub.exists():
                    warnings.append(f"Sub-config file not found: {cfg[key]!r} (key: {key})")

    except ImportError:
        warnings.append(
            "PyYAML not installed — skipping config validation.  "
            "Install with: pip install pyyaml"
        )
    except Exception as exc:  # noqa: BLE001
        warnings.append(f"Config parse error: {exc}")

    return warnings


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------

class CMSAnalysisRunner:
    """
    Wrapper around the ``cms_analysis_template`` C++ binary.

    Parameters
    ----------
    config_path:
        Path to the main cfg.yaml file.
    binary_path:
        Explicit path to the compiled binary.  If *None*, the wrapper
        searches standard build-tree locations automatically.
    verbose:
        Print subprocess output in real time when *True*.
    """

    def __init__(
        self,
        config_path: str,
        binary_path: Optional[str] = None,
        verbose: bool = True,
    ) -> None:
        self.config_path = str(Path(config_path).resolve())
        self.verbose = verbose

        if binary_path is not None:
            self.binary_path = str(Path(binary_path).resolve())
        else:
            found = _find_binary(Path(self.config_path).parent)
            self.binary_path = str(found) if found else "cms_analysis_template"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def validate(self) -> bool:
        """
        Validate the config file.  Prints warnings and returns *False* if
        any errors were found.
        """
        warnings = validate_config(self.config_path)
        if warnings:
            for w in warnings:
                print(f"[WARNING] {w}", file=sys.stderr)
            return False
        return True

    def build(self, jobs: int = 0, build_dir: Optional[str] = None) -> bool:
        """
        Build the analysis binary using CMake.

        Parameters
        ----------
        jobs:
            Number of parallel build jobs (0 = hardware concurrency).
        build_dir:
            Explicit build directory.  Defaults to ``<repo_root>/build``.
        """
        repo_root = _find_repo_root()
        bdir = Path(build_dir) if build_dir else repo_root / "build"
        bdir.mkdir(parents=True, exist_ok=True)

        # Configure
        cmake_cmd = ["cmake", "-S", str(repo_root), "-B", str(bdir)]
        self._run_subprocess(cmake_cmd, "CMake configure")

        # Build
        j_flag = str(jobs) if jobs > 0 else str(os.cpu_count() or 4)
        build_cmd = [
            "cmake", "--build", str(bdir),
            "--target", "cms_analysis_template",
            "-j", j_flag,
        ]
        self._run_subprocess(build_cmd, "CMake build")
        return True

    def run(self) -> int:
        """
        Execute the C++ analysis binary with the configured YAML file.

        Returns the process exit code (0 = success).
        """
        # Validate config first
        warnings = validate_config(self.config_path)
        if any("Missing required" in w for w in warnings):
            for w in warnings:
                print(f"[ERROR] {w}", file=sys.stderr)
            return 1

        if not Path(self.binary_path).exists():
            print(
                f"[ERROR] Binary not found: {self.binary_path!r}\n"
                "  Build the project first with: cmake --build build -j$(nproc)",
                file=sys.stderr,
            )
            return 1

        cmd = [self.binary_path, self.config_path]
        if self.verbose:
            print(f"[INFO] Running: {' '.join(cmd)}")

        result = subprocess.run(cmd, capture_output=not self.verbose, text=True)

        if result.returncode != 0 and not self.verbose:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)

        return result.returncode

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _run_subprocess(self, cmd: list[str], label: str) -> None:
        if self.verbose:
            print(f"[INFO] {label}: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=not self.verbose, text=True)
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr, file=sys.stderr)
            raise RuntimeError(f"{label} failed (exit {result.returncode})")


# ---------------------------------------------------------------------------
# Results loader
# ---------------------------------------------------------------------------

class AnalysisResults:
    """
    Thin wrapper around the ROOT output file produced by the analysis.

    Provides helpers for listing and displaying histograms and cutflow tables
    without requiring the user to interact with the ROOT C++ API directly.

    Requires PyROOT (``import ROOT``).

    Parameters
    ----------
    root_file:
        Path to the ROOT file written by the analysis (the ``saveFile``
        value in cfg.yaml, or the meta file which contains histograms).
    """

    def __init__(self, root_file: str) -> None:
        self.root_file = str(Path(root_file).resolve())
        self._file = None  # opened lazily

    # ------------------------------------------------------------------
    # File access
    # ------------------------------------------------------------------

    def _open(self):
        if self._file is None:
            import ROOT  # noqa: PLC0415 (deferred import)
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
        f = self._open()
        names: list[str] = []
        hdir = f.Get("histograms")
        if not hdir:
            # Fall back to flat structure
            for key in f.GetListOfKeys():
                obj = key.ReadObj()
                import ROOT
                if isinstance(obj, ROOT.TH1):
                    names.append(key.GetName())
        else:
            for key in hdir.GetListOfKeys():
                names.append(key.GetName())
        return sorted(names)

    def get_histogram(self, name: str):
        """
        Return the ROOT TH1 object with the given name.

        Searches ``histograms/<name>`` first, then the file root.
        """
        f = self._open()
        hdir = f.Get("histograms")
        if hdir:
            h = hdir.Get(name)
            if h:
                return h
        return f.Get(name)

    def print_histogram_summary(self, name: str) -> None:
        """Print a text summary of a named histogram."""
        h = self.get_histogram(name)
        if h is None:
            print(f"Histogram '{name}' not found in {self.root_file!r}")
            return
        print(f"  {name}: {h.GetTitle()}")
        print(f"    Entries : {h.GetEntries():.0f}")
        print(f"    Mean    : {h.GetMean():.4f}")
        print(f"    Std-dev : {h.GetStdDev():.4f}")
        print(f"    Integral: {h.Integral():.2f}")

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
            label = h.GetXaxis().GetBinLabel(i)
            val   = h.GetBinContent(i)
            print(f"  {label:<30s}  {val:>10.0f}")

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
            label = h.GetXaxis().GetBinLabel(i)
            val   = h.GetBinContent(i)
            print(f"  {label:<30s}  {val:>12.0f}")

    def summary(self) -> None:
        """Print a full summary: cutflow + all histogram statistics."""
        print(f"\n{'='*60}")
        print(f"  Analysis results: {self.root_file}")
        print(f"{'='*60}")
        self.print_cutflow()
        self.print_nminus1()
        histograms = self.list_histograms()
        if histograms:
            print(f"\n=== Histograms ({len(histograms)} total) ===")
            for name in histograms:
                self.print_histogram_summary(name)
        print()


# ---------------------------------------------------------------------------
# Command-line interface
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Python wrapper for the CMS analysis template.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    p.add_argument(
        "--config", "-c",
        default="cfg.yaml",
        help="Path to the main YAML config file (default: cfg.yaml).",
    )
    p.add_argument(
        "--binary", "-b",
        default=None,
        help="Explicit path to the compiled binary (auto-detected if omitted).",
    )
    p.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate the config file and exit without running the analysis.",
    )
    p.add_argument(
        "--build",
        action="store_true",
        help="Build the binary before running.",
    )
    p.add_argument(
        "--build-jobs",
        type=int,
        default=0,
        help="Number of parallel build jobs (default: hardware concurrency).",
    )
    p.add_argument(
        "--results",
        default=None,
        help="Path to output ROOT file for which to print a summary.",
    )
    p.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress real-time subprocess output.",
    )
    return p


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()

    runner = CMSAnalysisRunner(
        config_path=args.config,
        binary_path=args.binary,
        verbose=not args.quiet,
    )

    # Validate
    warnings = validate_config(args.config)
    for w in warnings:
        print(f"[{'ERROR' if 'Missing' in w or 'not found' in w else 'WARNING'}] {w}",
              file=sys.stderr)

    if args.validate_only:
        return 0 if not any("Missing" in w or "not found" in w for w in warnings) else 1

    # Optional build step
    if args.build:
        try:
            runner.build(jobs=args.build_jobs)
        except RuntimeError as exc:
            print(f"[ERROR] Build failed: {exc}", file=sys.stderr)
            return 1

    # Run analysis
    rc = runner.run()

    # Print results summary
    if args.results and Path(args.results).exists():
        try:
            AnalysisResults(args.results).summary()
        except Exception as exc:  # noqa: BLE001
            print(f"[WARNING] Could not load results: {exc}", file=sys.stderr)

    return rc


if __name__ == "__main__":
    sys.exit(main())
