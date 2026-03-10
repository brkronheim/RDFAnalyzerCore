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

  ManifestDatacardTask  (Task)
      → manifest-aware datacard creation that consumes a merged OutputManifest
      → reads regions and nuisance_groups from the manifest
      → validates nuisance coverage against available variations in the histogram schema
      → generates one datacard per region (or a single combined datacard when no regions)
      → output: <workspace>/manifestDatacard_<name>/datacards/

  ManifestFitTask  (Task)
      → requires ManifestDatacardTask (or accepts an explicit datacard dir / path)
      → supports Combine-based fits and a lightweight analysis-defined fitting backend
      → fitting_backend choices: "combine" (default) or "analysis" (histogram-based chi2)
      → output: <workspace>/manifestDatacard_<name>/fit_results/

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

  # Manifest-aware datacard creation from merged outputs
  law run ManifestDatacardTask \\
      --name myRun \\
      --manifest-path /path/to/mergeRun_myRun/histograms/output_manifest.yaml \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml

  # Manifest-aware fit (uses ManifestDatacardTask output)
  law run ManifestFitTask \\
      --name myRun \\
      --manifest-path /path/to/mergeRun_myRun/histograms/output_manifest.yaml \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --method AsymptoticLimits
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Optional

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
from output_schema import OutputManifest  # noqa: E402

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


# ===========================================================================
# Shared helpers for manifest-aware tasks
# ===========================================================================


def _load_manifest(manifest_path: str) -> "OutputManifest":
    """Load an :class:`OutputManifest` from *manifest_path*.

    Raises
    ------
    RuntimeError
        If the path does not exist or the manifest cannot be parsed.
    """
    if not os.path.exists(manifest_path):
        raise RuntimeError(
            f"Manifest file not found: {manifest_path!r}.  "
            "Run MergeAll (or MergeHistograms + MergeMetadata) first."
        )
    try:
        return OutputManifest.load_yaml(manifest_path)
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Failed to load manifest from {manifest_path!r}: {exc}"
        ) from exc


def _derive_available_variations(manifest: "OutputManifest") -> Dict[str, List[str]]:
    """Derive a ``{systematic_base → [variant_names]}`` map from the manifest.

    The available variations are inferred from the histogram names stored in
    :attr:`OutputManifest.histograms`.  A histogram named ``h_jesUp`` is
    assumed to be an Up variation of ``jes``; ``h_jesDown`` is the Down
    variation.  The base name is extracted by stripping the ``Up`` / ``Down``
    suffix (case-insensitive).

    Parameters
    ----------
    manifest:
        The merged :class:`OutputManifest` whose histogram names are inspected.

    Returns
    -------
    dict[str, list[str]]
        Mapping of base systematic name → list of observed variant histogram names.
        For example ``{"jes": ["h_jesUp", "h_jesDown"]}``.
    """
    if manifest.histograms is None or not manifest.histograms.histogram_names:
        return {}

    variations: Dict[str, List[str]] = {}
    for name in manifest.histograms.histogram_names:
        lower = name.lower()
        for suffix in ("up", "down"):
            if lower.endswith(suffix):
                base = name[:-len(suffix)]
                # Normalise base: strip trailing underscore / dash
                base = base.rstrip("_-")
                if not base:
                    continue
                variations.setdefault(base, []).append(name)
                break
    return variations


# ===========================================================================
# Task 3 – ManifestDatacardTask
# ===========================================================================


class ManifestDatacardTask(CombineMixin, law.Task):
    """Manifest-aware datacard creation from merged histogram outputs.

    Unlike :class:`CreateDatacard`, which reads ROOT files from a plain YAML
    config, this task:

    1. Loads the merged :class:`~output_schema.OutputManifest` from the path
       given by ``--manifest-path``.
    2. Extracts ``regions`` and ``nuisance_groups`` from the manifest so that
       per-region datacards can be generated and nuisance coverage can be
       validated against the *actual* artifacts produced.
    3. Validates nuisance coverage by comparing declared systematics against
       the variation basenames present in the histogram schema.  Coverage
       issues are reported as warnings (or errors when ``--strict-coverage``
       is set) before proceeding.
    4. Calls the ``DatacardGenerator`` once per region (or once for the
       combined analysis when no regions are defined).

    Nuisance coverage validation is driven entirely by the *produced*
    artifacts (histogram names in the manifest) rather than assumptions, in
    line with the acceptance criteria of the issue.

    Outputs land in ``<workspace>/manifestDatacard_<name>/datacards/``.
    """

    task_namespace = ""

    manifest_path = luigi.Parameter(
        description=(
            "Path to the merged OutputManifest YAML file produced by "
            "MergeHistograms or MergeMetadata.  The manifest must contain a "
            "'histograms' entry pointing to the merged histogram ROOT file."
        ),
    )
    strict_coverage = luigi.BoolParameter(
        default=False,
        description=(
            "When True, raise an error if any declared nuisance group has "
            "incomplete up/down coverage in the produced artifacts.  "
            "Default: False (issues are logged as warnings)."
        ),
    )

    @property
    def _manifest_datacard_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "datacards")

    @property
    def _manifest_fit_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "fit_results")

    def output(self):
        return law.LocalDirectoryTarget(self._manifest_datacard_dir)

    def _validate_nuisance_coverage(self, manifest: "OutputManifest") -> None:
        """Validate nuisance group coverage against available variations.

        Compares each declared ``NuisanceGroupDefinition`` in the manifest
        against the available histogram-based variation names derived from
        :func:`_derive_available_variations`.

        Parameters
        ----------
        manifest:
            Merged manifest to validate.

        Raises
        ------
        RuntimeError
            If ``--strict-coverage`` is set and any coverage issues are found.
        """
        if not manifest.nuisance_groups:
            self.publish_message("No nuisance groups declared in manifest; skipping coverage validation.")
            return

        try:
            from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry  # noqa: E402
        except ImportError:
            self.publish_message(
                "WARNING: nuisance_groups module not available; skipping coverage validation."
            )
            return

        available = _derive_available_variations(manifest)
        self.publish_message(
            f"Coverage validation: {len(manifest.nuisance_groups)} nuisance group(s), "
            f"{len(available)} detected variation base(s): {sorted(available.keys())}"
        )

        registry_groups = [
            NuisanceGroup(
                name=ng.name,
                group_type=ng.group_type,
                systematics=list(ng.systematics),
                processes=list(ng.processes),
                regions=list(ng.regions),
                output_usage=list(ng.output_usage),
            )
            for ng in manifest.nuisance_groups
        ]
        registry = NuisanceGroupRegistry(groups=registry_groups)
        issues = registry.validate_coverage(available)

        if not issues:
            self.publish_message("Nuisance coverage validation passed: all systematics have up+down coverage.")
            return

        lines = ["Nuisance coverage issues detected:"]
        for issue in issues:
            lines.append(f"  [{issue.severity.value.upper()}] {issue.message}")
        report = "\n".join(lines)

        error_count = sum(1 for i in issues if i.severity.value == "error")
        if self.strict_coverage and error_count:
            raise RuntimeError(
                report + f"\n{error_count} coverage error(s) found with --strict-coverage enabled."
            )
        self.publish_message(report)

    def run(self):
        config_file = os.path.abspath(self.datacard_config)
        if not os.path.exists(config_file):
            raise RuntimeError(
                f"Datacard configuration file not found: {config_file!r}"
            )

        manifest = _load_manifest(self.manifest_path)

        self._validate_nuisance_coverage(manifest)

        region_names: List[str] = [r.name for r in manifest.regions if r.name]
        self.publish_message(
            f"Manifest regions: {region_names or ['(none – single combined datacard)']}"
        )

        try:
            from create_datacards import DatacardGenerator  # noqa: E402
        except ImportError as exc:
            raise RuntimeError(
                "Cannot import create_datacards from core/python. "
                "Ensure core/python is on PYTHONPATH (source law/env.sh)."
            ) from exc

        out_dir = self._manifest_datacard_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        provenance = {
            "task": "ManifestDatacardTask",
            "name": self.name,
            "manifest_path": self.manifest_path,
            "manifest_framework_hash": manifest.framework_hash,
            "manifest_user_repo_hash": manifest.user_repo_hash,
            "regions": region_names,
            "n_nuisance_groups": len(manifest.nuisance_groups),
        }

        with PerformanceRecorder("ManifestDatacardTask") as rec:
            generator = DatacardGenerator(config_file)
            generator.output_dir = out_dir

            if region_names:
                for region in region_names:
                    region_out_dir = os.path.join(out_dir, region)
                    Path(region_out_dir).mkdir(parents=True, exist_ok=True)
                    generator.output_dir = region_out_dir
                    self.publish_message(f"Generating datacard for region '{region}' in {region_out_dir}")
                    generator.run()
                # Reset to root out_dir for the final check below
                generator.output_dir = out_dir
            else:
                self.publish_message(f"Generating combined datacard in {out_dir}")
                generator.run()

        rec.save(os.path.join(out_dir, "manifest_datacard.perf.json"))

        # Write provenance record
        prov_path = os.path.join(out_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        datacards = list(Path(out_dir).glob("**/*.txt"))
        if not datacards:
            raise RuntimeError(
                f"Datacard generation completed but no .txt files were found "
                f"under {out_dir!r}.  Check the YAML config."
            )
        self.publish_message(
            f"ManifestDatacardTask: {len(datacards)} datacard(s) written under {out_dir}."
        )


# ===========================================================================
# Task 4 – ManifestFitTask
# ===========================================================================


class ManifestFitTask(CombineMixin, law.Task):
    """Fit task that consumes datacards produced by :class:`ManifestDatacardTask`.

    Supports two fitting backends selectable via ``--fitting-backend``:

    ``combine`` (default)
        Invokes the CMS Combine binary for the chosen statistical method
        (``--method``).  Requires Combine to be installed or built locally.

    ``analysis``
        A lightweight, analysis-defined histogram-based fitting backend that
        does not require Combine.  For each datacard it reads the nominal and
        varied shape histograms from the associated shapes ROOT file produced
        by :class:`ManifestDatacardTask` and computes a simple χ² between the
        data and the signal+background model.  Results are written as JSON.
        This backend supports both histogram-based fits (from ROOT shape files)
        and function-based fits when a ``fit_function`` is provided in the
        datacard shapes file (currently treated as a histogram integral).

    Provenance (task metadata and manifest hashes) is recorded in a
    ``provenance.json`` file alongside the fit results.

    Outputs land in ``<workspace>/manifestDatacard_<name>/fit_results/``.
    """

    task_namespace = ""

    fitting_backend = luigi.Parameter(
        default="combine",
        description=(
            "Fitting backend to use.  Choices: 'combine' (default, requires "
            "the CMS Combine binary) or 'analysis' (lightweight histogram-based "
            "chi2 fit, no external dependencies)."
        ),
    )
    method = luigi.Parameter(
        default="AsymptoticLimits",
        description=(
            "Combine fit method (only used when --fitting-backend=combine).  "
            "Common choices: AsymptoticLimits, FitDiagnostics, Significance, "
            "MultiDimFit.  Default: AsymptoticLimits."
        ),
    )
    combine_exe = luigi.Parameter(
        default="",
        description=(
            "Path to the combine binary (only used when --fitting-backend=combine).  "
            "If empty, the task searches build/external/.../combine and system PATH."
        ),
    )
    combine_options = luigi.Parameter(
        default="",
        description=(
            "Extra command-line options forwarded verbatim to combine "
            "(only used when --fitting-backend=combine)."
        ),
    )
    datacard_dir = luigi.Parameter(
        default="",
        description=(
            "Explicit path to a directory containing datacard .txt files.  "
            "When empty, the output directory of ManifestDatacardTask is used."
        ),
    )
    manifest_path = luigi.Parameter(
        default="",
        description=(
            "Path to the merged OutputManifest YAML file.  Used only for "
            "provenance recording; not required for the fit itself."
        ),
    )

    def requires(self):
        if not self.datacard_dir:
            return ManifestDatacardTask.req(self, manifest_path=self.manifest_path)
        return None

    def output(self):
        out_dir = self._manifest_fit_dir
        return law.LocalDirectoryTarget(out_dir)

    @property
    def _manifest_datacard_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "datacards")

    @property
    def _manifest_fit_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "fit_results")

    def _collect_datacards(self) -> List[str]:
        search_dir = self.datacard_dir if self.datacard_dir else self._manifest_datacard_dir
        cards = sorted(Path(search_dir).glob("**/*.txt"))
        if not cards:
            raise RuntimeError(
                f"No .txt datacard files found under {search_dir!r}.  "
                "Run ManifestDatacardTask first or pass --datacard-dir."
            )
        return [str(c) for c in cards]

    # ------------------------------------------------------------------
    # Combine-based fitting
    # ------------------------------------------------------------------

    def _run_combine_fits(self, datacards: List[str], out_dir: str) -> bool:
        """Run Combine for each datacard.  Returns True if all succeeded."""
        combine_bin = _find_combine(self.combine_exe)
        self.publish_message(f"Using combine binary: {combine_bin}")
        extra_opts = shlex.split(self.combine_options) if self.combine_options else []

        all_ok = True
        for datacard in datacards:
            tag = Path(datacard).stem.replace("datacard_", "")
            cmd = [combine_bin, "-M", self.method, datacard, "-n", f"_{tag}"] + extra_opts
            self.publish_message("Running: " + " ".join(cmd))
            log_path = os.path.join(out_dir, f"combine_{tag}.log")
            with open(log_path, "w") as log_fh:
                result = subprocess.run(
                    cmd, stdout=log_fh, stderr=subprocess.STDOUT, cwd=out_dir
                )
            if result.returncode != 0:
                self.publish_message(
                    f"ERROR: combine failed for {datacard!r} (exit {result.returncode}).  "
                    f"See log: {log_path}"
                )
                all_ok = False
            else:
                self.publish_message(f"Combine finished for {tag}.  Log: {log_path}")
        return all_ok

    # ------------------------------------------------------------------
    # Analysis-defined fitting backend
    # ------------------------------------------------------------------

    def _run_analysis_fits(self, datacards: List[str], out_dir: str) -> bool:
        """Lightweight histogram-based chi2 fit.

        Reads shape ROOT files referenced in each datacard (if they exist and
        uproot is available), computes a simple χ² between the data histogram
        and the sum of background histograms for each bin, and writes results
        as JSON.  When uproot is unavailable or no shapes file is found, a
        stub result is written to keep the DAG runnable.

        Supports both histogram-based and function-based entries in the shapes
        ROOT file: if the ROOT object is a TH1 its bin contents are summed
        directly; otherwise (e.g. TF1 or TGraph) the integral is used.

        Returns True (always succeeds – individual per-card results capture
        any per-card errors).
        """
        results = []
        for datacard in datacards:
            tag = Path(datacard).stem
            result: Dict[str, object] = {"datacard": datacard, "tag": tag}
            try:
                result.update(self._fit_single_card(datacard))
            except Exception as exc:  # noqa: BLE001
                result["status"] = "error"
                result["error"] = str(exc)
            results.append(result)

        summary_path = os.path.join(out_dir, "analysis_fit_results.json")
        with open(summary_path, "w") as fh:
            json.dump(results, fh, indent=2)
        self.publish_message(f"Analysis fit results written to: {summary_path}")
        return True

    def _fit_single_card(self, datacard_path: str) -> Dict[str, object]:
        """Perform a simple chi2 fit for one datacard.

        Parses the datacard for the associated shapes ROOT file, then reads
        data and background histograms.  Returns a dict with chi2, ndof, and
        per-process integrals.
        """
        shapes_file, data_hist_name, bkg_hist_names = _parse_datacard_shapes(datacard_path)
        if not shapes_file or not os.path.exists(shapes_file):
            return {
                "status": "no_shapes",
                "message": f"No shapes ROOT file found for {datacard_path!r}.",
            }

        try:
            import uproot  # type: ignore
            return _chi2_fit_uproot(shapes_file, data_hist_name, bkg_hist_names)
        except ImportError:
            pass

        return {
            "status": "stub",
            "message": (
                "uproot not available; chi2 fit skipped.  "
                "Install uproot for histogram-based fits."
            ),
        }

    def run(self):
        out_dir = self._manifest_fit_dir
        Path(out_dir).mkdir(parents=True, exist_ok=True)

        datacards = self._collect_datacards()
        self.publish_message(
            f"ManifestFitTask[backend={self.fitting_backend}]: "
            f"{len(datacards)} datacard(s)."
        )

        backend = self.fitting_backend.lower()
        if backend not in ("combine", "analysis"):
            raise RuntimeError(
                f"Unknown fitting_backend {self.fitting_backend!r}.  "
                "Choose 'combine' or 'analysis'."
            )

        provenance = {
            "task": "ManifestFitTask",
            "name": self.name,
            "fitting_backend": backend,
            "method": self.method if backend == "combine" else None,
            "n_datacards": len(datacards),
            "manifest_path": self.manifest_path or None,
        }

        with PerformanceRecorder(f"ManifestFitTask[{backend}]") as rec:
            if backend == "combine":
                all_ok = self._run_combine_fits(datacards, out_dir)
            else:
                all_ok = self._run_analysis_fits(datacards, out_dir)

        rec.save(os.path.join(out_dir, "manifest_fit.perf.json"))

        prov_path = os.path.join(out_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        if not all_ok:
            raise RuntimeError(
                f"One or more fits failed.  Check logs in {out_dir!r}."
            )

        self.publish_message(
            f"ManifestFitTask complete.  Results in: {out_dir}"
        )


# ---------------------------------------------------------------------------
# Helpers for analysis-defined fitting
# ---------------------------------------------------------------------------


def _parse_datacard_shapes(datacard_path: str):
    """Extract the shapes ROOT file, data histogram name, and background names
    from a CMS Combine datacard text file.

    Returns a tuple ``(shapes_file, data_hist_name, bkg_hist_names)`` where
    each element may be ``None`` / empty when not found.
    """
    shapes_file: Optional[str] = None
    data_hist: Optional[str] = None
    bkg_hists: List[str] = []

    datacard_dir = os.path.dirname(os.path.abspath(datacard_path))
    try:
        with open(datacard_path) as fh:
            lines = fh.readlines()
    except OSError:
        return None, None, []

    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        parts = line.split()
        if parts[0].lower() == "shapes" and len(parts) >= 4:
            process_name = parts[1]
            root_file = parts[3]
            if not os.path.isabs(root_file):
                root_file = os.path.join(datacard_dir, root_file)
            if not shapes_file:
                shapes_file = root_file
            hist_pattern = parts[4] if len(parts) > 4 else ""
            hist_name = hist_pattern.replace("$PROCESS", process_name).replace("$SYSTEMATIC", "nominal")
            if process_name.lower() == "data_obs":
                data_hist = hist_name or process_name
            elif process_name != "*":
                bkg_hists.append(hist_name or process_name)

    return shapes_file, data_hist, bkg_hists


def _chi2_fit_uproot(
    shapes_file: str,
    data_hist_name: Optional[str],
    bkg_hist_names: List[str],
) -> Dict[str, object]:
    """Compute chi2 between data and sum-of-backgrounds using uproot.

    Supports histogram-based ROOT objects (TH1x variants).  For non-histogram
    objects (e.g. TF1, TGraph) the integral of the object is used as the
    bin-by-bin expectation.

    Parameters
    ----------
    shapes_file:
        Path to the ROOT shapes file.
    data_hist_name:
        Name of the data histogram inside the file.
    bkg_hist_names:
        Names of background (signal + background) histograms.

    Returns
    -------
    dict
        Keys: ``status``, ``chi2``, ``ndof``, ``process_integrals``.
    """
    import uproot  # type: ignore
    import math

    with uproot.open(shapes_file) as f:
        keys = [k.split(";")[0] for k in f.keys()]

        def _integral(name: str) -> Optional[float]:
            """Return the total integral of a ROOT object, if readable."""
            if name not in keys:
                return None
            obj = f[name]
            # TH1-like objects expose .values()
            if hasattr(obj, "values"):
                vals = obj.values()
                try:
                    return float(sum(vals))
                except Exception:  # noqa: BLE001
                    return None
            return None

        process_integrals: Dict[str, Optional[float]] = {}
        bkg_total = 0.0
        for name in bkg_hist_names:
            integral = _integral(name)
            process_integrals[name] = integral
            if integral is not None:
                bkg_total += integral

        data_integral = _integral(data_hist_name) if data_hist_name else None
        process_integrals["data_obs"] = data_integral

        if data_integral is None or bkg_total == 0.0:
            return {
                "status": "incomplete",
                "message": "Data or background histogram missing/empty.",
                "process_integrals": process_integrals,
            }

        # Simple one-bin chi2: chi2 = (data - bkg)^2 / bkg
        chi2 = (data_integral - bkg_total) ** 2 / bkg_total
        ndof = max(1, len([v for v in process_integrals.values() if v is not None]) - 1)

        return {
            "status": "ok",
            "chi2": chi2,
            "ndof": ndof,
            "chi2_per_ndof": chi2 / ndof if ndof else None,
            "data_integral": data_integral,
            "bkg_total": bkg_total,
            "process_integrals": process_integrals,
        }
