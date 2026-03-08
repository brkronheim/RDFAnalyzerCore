"""
Law tasks for generating CMS Combine datacards and performing basic Combine fits.

These tasks integrate with the outputs of the base analysis run tasks
(PrepareNANOSample / PrepareOpenDataSample workflows).

Workflow
--------
  CreateDatacard  (Task)
      → reads analysis output ROOT files listed in a YAML config
      → runs core/python/create_datacards.py to generate datacards + shape ROOT files
      → output: <output_dir>/  (directory containing datacards and shape files)

  RunCombine  (Task)
      → requires CreateDatacard (or accepts an explicit datacard path)
      → runs `combine` for a chosen fit method (default: AsymptoticLimits)
      → output: combine_results/ directory with fit output ROOT files and logs

Usage
-----
  source law/env.sh

  # Generate datacards
  law run CreateDatacard \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --name myRun

  # Run Combine fits (requires CreateDatacard to have run first)
  law run RunCombine \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --name myRun \\
      --method AsymptoticLimits

  # Run Combine on an existing datacard directly (without CreateDatacard)
  law run RunCombine \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --name myRun \\
      --datacard-path datacards/datacard_signal_region.txt \\
      --method FitDiagnostics
"""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore

# ---------------------------------------------------------------------------
# Make sure core/python is importable regardless of how the script is invoked
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))

# Default search locations for the combine binary
_COMBINE_SEARCH_PATHS = [
    os.path.join(WORKSPACE, "build", "external", "HiggsAnalysis", "CombinedLimit", "exe", "combine"),
    "combine",  # system PATH
]


def _find_combine(combine_exe: str = "") -> str:
    """Return the path to the combine binary.

    Searches in:
    1. ``combine_exe`` parameter (if non-empty).
    2. The build directory: build/external/HiggsAnalysis/CombinedLimit/exe/combine.
    3. System PATH.

    Raises RuntimeError if not found.
    """
    if combine_exe:
        if os.path.isfile(combine_exe) and os.access(combine_exe, os.X_OK):
            return combine_exe
        raise RuntimeError(
            f"Specified combine executable not found or not executable: {combine_exe!r}"
        )

    for candidate in _COMBINE_SEARCH_PATHS:
        if os.path.isabs(candidate):
            if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
                return candidate
        else:
            # Relative → check via shutil.which (portable, no subprocess)
            found = shutil.which(candidate)
            if found:
                return found

    raise RuntimeError(
        "CMS Combine binary not found.  Either:\n"
        "  1. Build with Combine support: cmake -S . -B build -DBUILD_COMBINE=ON && cmake --build build\n"
        "  2. Source a CMSSW environment that provides `combine`\n"
        "  3. Pass --combine-exe /path/to/combine explicitly"
    )


# ===========================================================================
# Shared parameter mixin
# ===========================================================================

class CombineMixin:
    """Parameters shared by CreateDatacard and RunCombine."""

    datacard_config = luigi.Parameter(
        description=(
            "Path to the YAML datacard configuration file consumed by "
            "core/python/create_datacards.py.  Controls which ROOT files are "
            "read, which histograms to use, and which systematics to apply."
        ),
    )
    name = luigi.Parameter(
        description=(
            "Run name.  Datacard output is written to "
            "<workspace>/combineRun_<name>/datacards/ and combine results to "
            "<workspace>/combineRun_<name>/combine_results/."
        ),
    )

    # ---- derived helpers ---------------------------------------------------

    @property
    def _run_dir(self) -> str:
        """Root directory for all outputs of this named run."""
        return os.path.join(WORKSPACE, f"combineRun_{self.name}")

    @property
    def _datacard_dir(self) -> str:
        return os.path.join(self._run_dir, "datacards")

    @property
    def _results_dir(self) -> str:
        return os.path.join(self._run_dir, "combine_results")


# ===========================================================================
# Task 1 – CreateDatacard
# ===========================================================================

class CreateDatacard(CombineMixin, law.Task):
    """
    Generate CMS Combine datacards and shape ROOT files from analysis outputs.

    Reads histograms from the ROOT files specified in ``--datacard-config``,
    combines samples, applies systematics, and writes:

    - One ``datacard_<region>.txt`` per control region
    - One ``shapes_<region>.root`` per control region (containing process
      histograms and systematic variations)

    All outputs land in ``combineRun_<name>/datacards/``.

    The YAML config format is documented in docs/DATACARD_GENERATOR.md and
    an example is provided in core/python/example_datacard_config.yaml.
    """

    task_namespace = ""

    def output(self):
        return law.LocalDirectoryTarget(self._datacard_dir)

    def run(self):
        config_file = os.path.abspath(self.datacard_config)
        if not os.path.exists(config_file):
            raise RuntimeError(
                f"Datacard configuration file not found: {config_file!r}"
            )

        # Patch the output directory in the config so outputs go to our run dir.
        # We do this by importing the generator directly rather than spawning a
        # subprocess so that we can override output_dir at runtime.
        try:
            from create_datacards import DatacardGenerator  # noqa: E402
        except ImportError as exc:
            raise RuntimeError(
                "Cannot import create_datacards from core/python. "
                "Ensure core/python is on PYTHONPATH (source law/env.sh)."
            ) from exc

        self.publish_message(f"Reading datacard config: {config_file}")
        generator = DatacardGenerator(config_file)

        # Override the output directory so results land in our run dir
        generator.output_dir = self._datacard_dir
        Path(self._datacard_dir).mkdir(parents=True, exist_ok=True)

        self.publish_message(f"Generating datacards in: {self._datacard_dir}")

        with PerformanceRecorder("CreateDatacard") as rec:
            generator.run()

        rec.save(os.path.join(self._datacard_dir, "create_datacard.perf.json"))

        # Verify at least one datacard was produced
        datacards = list(Path(self._datacard_dir).glob("datacard_*.txt"))
        if not datacards:
            raise RuntimeError(
                f"Datacard generation completed but no datacard_*.txt files "
                f"were found in {self._datacard_dir!r}.  Check the YAML config."
            )
        self.publish_message(
            f"Created {len(datacards)} datacard(s):\n"
            + "\n".join(f"  {dc.name}" for dc in sorted(datacards))
        )


# ===========================================================================
# Task 2 – RunCombine
# ===========================================================================

class RunCombine(CombineMixin, law.Task):
    """
    Run CMS Combine fits on the datacards produced by CreateDatacard.

    By default this task requires CreateDatacard and uses its output directory
    to discover datacards automatically.  You can override this by passing
    ``--datacard-path`` pointing to a specific datacard file.

    Results (combine output ROOT files and logs) are written to
    ``combineRun_<name>/combine_results/``.

    Supported methods (``--method``):
      - AsymptoticLimits  (default) – CLs limits without toys
      - HybridNew                   – CLs limits with toys
      - FitDiagnostics              – S+B and B-only fits, pulls, impacts
      - Significance                – observed/expected significance
      - MultiDimFit                 – multi-dimensional likelihood scan
    """

    task_namespace = ""

    method = luigi.Parameter(
        default="AsymptoticLimits",
        description=(
            "Combine fit method.  Common choices: AsymptoticLimits, "
            "FitDiagnostics, Significance, MultiDimFit, HybridNew.  "
            "Default: AsymptoticLimits"
        ),
    )
    datacard_path = luigi.Parameter(
        default="",
        description=(
            "Path to a specific datacard file to run Combine on.  "
            "If empty, Combine is run on every datacard_*.txt found in the "
            "CreateDatacard output directory."
        ),
    )
    combine_exe = luigi.Parameter(
        default="",
        description=(
            "Path to the combine binary.  If empty, the task searches in "
            "build/external/HiggsAnalysis/CombinedLimit/exe/combine and then "
            "in the system PATH."
        ),
    )
    combine_options = luigi.Parameter(
        default="",
        description=(
            "Extra command-line options forwarded verbatim to combine, "
            "e.g. '--rMin 0 --rMax 5 --toys 1000'."
        ),
    )

    def requires(self):
        # Only require CreateDatacard when no explicit datacard path is given
        if not self.datacard_path:
            return CreateDatacard.req(self)
        return None

    def output(self):
        return law.LocalDirectoryTarget(self._results_dir)

    def _collect_datacards(self) -> list[str]:
        """Return the list of datacard files to run Combine on."""
        if self.datacard_path:
            path = os.path.abspath(self.datacard_path)
            if not os.path.exists(path):
                raise RuntimeError(f"Datacard file not found: {path!r}")
            return [path]

        # Use datacards produced by CreateDatacard
        cards = sorted(Path(self._datacard_dir).glob("datacard_*.txt"))
        if not cards:
            raise RuntimeError(
                f"No datacard_*.txt files found in {self._datacard_dir!r}.  "
                "Run CreateDatacard first or pass --datacard-path."
            )
        return [str(c) for c in cards]

    def run(self):
        combine_bin = _find_combine(self.combine_exe)
        self.publish_message(f"Using combine binary: {combine_bin}")

        datacards = self._collect_datacards()
        self.publish_message(
            f"Running Combine ({self.method}) on {len(datacards)} datacard(s)."
        )

        Path(self._results_dir).mkdir(parents=True, exist_ok=True)

        # combine writes output ROOT files to the current working directory;
        # we run it from _results_dir so everything lands there.
        extra_opts = shlex.split(self.combine_options) if self.combine_options else []

        all_ok = True
        with PerformanceRecorder(f"RunCombine[method={self.method}]") as rec:
            for datacard in datacards:
                tag = Path(datacard).stem.replace("datacard_", "")
                cmd = (
                    [combine_bin, "-M", self.method, datacard, "-n", f"_{tag}"]
                    + extra_opts
                )
                self.publish_message("Running: " + " ".join(cmd))

                log_path = os.path.join(self._results_dir, f"combine_{tag}.log")
                with open(log_path, "w") as log_fh:
                    result = subprocess.run(
                        cmd,
                        stdout=log_fh,
                        stderr=subprocess.STDOUT,
                        cwd=self._results_dir,
                    )

                if result.returncode != 0:
                    self.publish_message(
                        f"ERROR: combine failed for {datacard!r} (exit {result.returncode}).  "
                        f"See log: {log_path}"
                    )
                    all_ok = False
                else:
                    self.publish_message(
                        f"Combine finished for {tag}.  Log: {log_path}"
                    )

        rec.save(os.path.join(self._results_dir, "run_combine.perf.json"))

        # Write a summary file listing all outputs
        output_roots = sorted(Path(self._results_dir).glob("higgsCombine*.root"))
        summary_path = os.path.join(self._results_dir, "summary.txt")
        with open(summary_path, "w") as fh:
            fh.write(f"method={self.method}\n")
            fh.write(f"n_datacards={len(datacards)}\n")
            fh.write(f"ok={all_ok}\n")
            for r in output_roots:
                fh.write(f"output={r.name}\n")

        if not all_ok:
            raise RuntimeError(
                f"One or more Combine fits failed.  Check logs in {self._results_dir!r}."
            )

        self.publish_message(
            f"All Combine fits completed.  Results in: {self._results_dir}\n"
            f"Output ROOT files: {[r.name for r in output_roots]}"
        )
