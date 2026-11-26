"""
Law tasks for generating CMS Combine datacards and performing basic Combine fits.

These tasks integrate with the outputs of the base analysis run tasks
(PrepareSkimJobs / SkimTask workflows).

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
      → fitting_backend choices: "combine" (default), "rabbit" (CPU/CUDA), or "analysis" (histogram-based chi2)
      → output: <workspace>/manifestDatacard_<name>/fit_results/

  AnalyticWorkspaceFitTask  (Task)
      → generic analytic PDF fit using user-configurable signal + background models
      → reads a YAML workspace config (--workspace-config) that specifies:
          observable range, signal PDF type and parameters, background PDF type and
          parameters, and the list of channels (histogram → channel name mapping)
      → supported signal PDFs: voigtian, gaussian, crystalball, double_gaussian,
          breit_wigner
      → supported background PDFs: exponential, polynomial, chebychev, bernstein
      → parameters marked shared:true use a common name across channels so that
          Combine ties them together in the simultaneous combined fit
      → builds per-channel RooWorkspaces, writes analytic-shape Combine datacards,
          runs combine -M <method> for each channel and for the
          combined simultaneous fit
      → output: <workspace>/analyticFit_<name>/

Usage
-----
    source core/python/law/env.sh

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

  # Generic analytic workspace fit (Z→μμ Voigtian+exp, per jet bin)
  law run AnalyticWorkspaceFitTask \\
      --name zpeak_run2016g \\
      --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \\
      --histogram-file output/dimuon_zpeak.root \\
      --method FitDiagnostics \\
      --combine-options "--saveShapes --saveNormalizations"
"""

from __future__ import annotations

import json
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
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from rdfproduction.performance import PerformanceRecorder  # noqa: E402
from output_schema import OutputManifest  # noqa: E402, F401

from statistical_inference.manifest_fits import (  # noqa: E402, F401
    _AnalyticWorkspaceOperations,
    _ManifestDatacardOperations,
    _ManifestFitOperations,
    _COMBINE_SEARCH_PATHS,
    _find_combine as _locate_combine,
    _find_histogram_in_file,
    _load_manifest,
    _derive_available_variations,
    _resolve_manifest_histogram_file,
    _collect_config_sample_names,
    _load_dataset_manifest_sample_names,
    _load_manifest_sample_names,
    _build_manifest_region_config,
    _prepare_manifest_datacard_config,
    _make_roo_var,
    _build_signal_pdf,
    _build_background_pdf,
    _build_analytic_workspace,
    _write_analytic_datacard,
    _write_analytic_combined_datacard,
    _parse_datacard_shapes,
    _chi2_fit_uproot,
)

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))

def _find_combine(combine_exe: str = "") -> str:
    return _locate_combine(combine_exe, search_paths=_COMBINE_SEARCH_PATHS)


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
                "Ensure core/python and core/python/law are on PYTHONPATH (source core/python/law/env.sh)."
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
            "build/external/HiggsAnalysis/CombinedLimit/bin/combine, then legacy exe/combine and then "
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


class ManifestDatacardTask(CombineMixin, _ManifestDatacardOperations, law.Task):
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
    merge_input_dir = luigi.Parameter(
        default="",
        description=(
            "Optional MergeAll input directory.  When set, this task declares "
            "a dependency on MergeAll so manifest-based datacards wait for the "
            "merge stage to complete."
        ),
    )
    dataset_manifest = luigi.Parameter(
        default="",
        description=(
            "Optional dataset manifest YAML. When provided, missing input-file "
            "sample aliases in the datacard config are synthesized from the "
            "dataset entries."
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
    output_dir = luigi.Parameter(default="", description="Override the manifest datacard output directory.")

    @property
    def _manifest_datacard_dir(self) -> str:
        return self.output_dir or os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "datacards")

    @property
    def _manifest_fit_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "fit_results")

    def requires(self):
        if not self.merge_input_dir:
            return None
        from merge_tasks import MergeAll  # noqa: E402

        return MergeAll.req(self, input_dir=self.merge_input_dir, output_dir="")

    def output(self):
        return law.LocalDirectoryTarget(self._manifest_datacard_dir)


class ManifestFitTask(CombineMixin, _ManifestFitOperations, law.Task):
    """Fit task that consumes datacards produced by :class:`ManifestDatacardTask`.

    Supports three fitting backends selectable via ``--fitting-backend``:

    ``combine`` (default)
        Invokes the CMS Combine binary for the chosen statistical method
        (``--method``).  Requires Combine to be installed or built locally.

    ``rabbit``
        Native binned profile-likelihood fits in a dedicated Python runtime,
        with explicit CPU/GPU selection and multi-GPU bin sharding. Consumes
        unique-channel cards jointly. ``method`` is Combine-specific.

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
            "the CMS Combine binary), 'rabbit' (native CPU/CUDA fits), or 'analysis' (lightweight histogram-based "
            "chi2 fit, no external dependencies)."
        ),
    )
    rabbit_python = luigi.Parameter(default="", description="Dedicated Rabbit CPU/CUDA Python interpreter.")
    rabbit_device = luigi.ChoiceParameter(default="gpu", choices=("cpu", "gpu"))
    rabbit_devices = luigi.Parameter(default="", description="Comma-separated GPU indices.")
    rabbit_n_devices = luigi.IntParameter(default=1)
    rabbit_mc_stat = luigi.ChoiceParameter(default="card", choices=("card", "none", "lite", "full"))
    rabbit_likelihood = luigi.ChoiceParameter(default="native", choices=("native", "combine"))
    rabbit_covariance = luigi.ChoiceParameter(default="auto", choices=("auto", "analytic", "combine"))
    rabbit_zero_shape_policy = luigi.ChoiceParameter(default="error", choices=("error", "native"))
    rabbit_hvp_batch = luigi.IntParameter(default=0, description="Hessian-vector directions per batch; zero uses Rabbit's default.")
    fit_dataset = luigi.ChoiceParameter(default="observed", choices=("asimov", "observed"))
    strict_coverage = luigi.BoolParameter(default=False)
    output_dir = luigi.Parameter(default="", description="Override the manifest fit output directory.")
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
    merge_input_dir = luigi.Parameter(
        default="",
        description=(
            "Optional MergeAll input directory forwarded to "
            "ManifestDatacardTask when datacards are produced as part of a "
            "single end-to-end DAG run."
        ),
    )

    def requires(self):
        if not self.datacard_dir:
            return ManifestDatacardTask.req(
                self,
                manifest_path=self.manifest_path,
                merge_input_dir=self.merge_input_dir,
                strict_coverage=self.strict_coverage,
                output_dir="",
            )
        return None

    def output(self):
        out_dir = self._manifest_fit_dir
        if self.fitting_backend.lower() == "rabbit":
            return law.LocalFileTarget(str(self._rabbit_completion_path))
        return law.LocalDirectoryTarget(out_dir)
    @property
    def _manifest_datacard_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "datacards")

    @property
    def _manifest_fit_dir(self) -> str:
        return self.output_dir or os.path.join(WORKSPACE, f"manifestDatacard_{self.name}", "fit_results")
    @property
    def _completion_id(self) -> str:
        return self.task_id

    def complete(self):
        if self.fitting_backend.lower() == "rabbit":
            return self._verify_rabbit_result()
        return super().complete()


class AnalyticWorkspaceFitTask(_AnalyticWorkspaceOperations, law.Task):
    """Generic analytic PDF workspace fit driven by a YAML config.

    Reads per-channel histograms from an analysis ROOT file (produced by an
    ``analysis.cc`` run), builds per-channel RooFit workspaces using fully
    configurable analytic signal + background PDFs, generates CMS Combine
    datacards with analytic shapes, and runs
    ``combine -M <method>`` for every channel and for a combined simultaneous
    fit across all channels.

    The entire fit model — observable range, PDF types, parameter names,
    initial values, constraints, and parameter-sharing strategy — is specified
    in a single YAML *workspace config* file (``--workspace-config``).  This
    makes the task reusable for any narrow-resonance peak: Z→μμ, J/ψ→μμ,
    Υ→μμ, H→γγ, etc., simply by swapping the config.

    Supported signal PDFs
    ----------------------
    ``voigtian``
        Breit-Wigner ⊗ Gaussian convolution (RooVoigtian).  Standard CMS
        model for the Z/Υ/J/ψ peaks where both the natural width and the
        detector resolution matter.  Parameters: ``mean``, ``width``, ``sigma``.

    ``gaussian``
        Pure Gaussian (RooGaussian).  Use when the natural width is small
        compared to detector resolution.  Parameters: ``mean``, ``sigma``.

    ``crystalball``
        Crystal Ball shape (RooCBShape): Gaussian core with a power-law tail
        on one side.  Common for tracker and ECAL energy measurements.
        Parameters: ``mean``, ``sigma``, ``alpha``, ``n``.

    ``double_gaussian``
        Sum of two Gaussians (RooAddPdf of two RooGaussians).  Useful when
        the resolution has two distinct components.  Parameters: ``mean``,
        ``sigma1``, ``sigma2``, ``frac`` (fraction of first Gaussian).

    ``breit_wigner``
        Pure Breit-Wigner (RooBreitWigner).  Use when detector resolution can
        be neglected.  Parameters: ``mean``, ``width``.

    Supported background PDFs
    --------------------------
    ``exponential``
        Single falling exponential (RooExponential).  Parameter: ``decay``.

    ``polynomial``
        Polynomial of configurable order (RooPolynomial).  Use
        ``order: N`` in the config.  Coefficient parameters: ``c1``, ``c2``, …

    ``chebychev``
        Chebyshev polynomial basis (RooChebychev), better conditioned than
        plain polynomial.  Coefficients: ``a0``, ``a1``, …

    ``bernstein``
        Bernstein polynomial basis (RooBernstein), strictly non-negative.
        Use ``order: N``; coefficients: ``b0``, ``b1``, …, ``bN``.

    Parameter sharing in the combined fit
    ----------------------------------------
    Parameters marked ``shared: true`` in the config receive a *name with no
    channel suffix* (e.g. ``mean`` instead of ``mean_0j``).  Combine
    automatically ties same-named parameters across channels in the
    simultaneous fit.  Use this for physics quantities expected to be
    universal (e.g. Z mass, detector resolution) and ``shared: false`` for
    background shape parameters that may vary per channel.

    Workspace config YAML format
    ----------------------------
    See ``analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml`` for a
    fully annotated example. The optional ``combine_rates`` mapping supplies
    reference signal and background yields for each channel. These cards use
    Combine's common signal-strength POI and a separate free background scale
    per channel. All configured channels are required when rates are supplied.

    Outputs
    -------
    All outputs land in ``<workspace>/analyticFit_<name>/``:

    - ``ws_<channel>.root`` — RooWorkspace per channel
    - ``datacard_<channel>.txt`` — Combine datacard per channel (analytic shapes)
    - ``datacard_combined.txt`` — simultaneous fit across all channels
    - ``combine_<tag>.log`` — Combine stdout/stderr per card
    - ``fitDiagnostics_<tag>.root`` — Combine output (if FitDiagnostics method)
    - ``provenance.json`` — task provenance record
    - ``analytic_fit.perf.json`` — wall-time / memory performance record

    Usage
    -----
    ::

        # Z→μμ Voigtian+exponential fit in bins of jet multiplicity
        law run AnalyticWorkspaceFitTask \\
            --name zpeak_run2016g \\
            --workspace-config analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml \\
            --histogram-file output/dimuon_zpeak.root \\
            --method FitDiagnostics \\
            --combine-options "--saveShapes --saveNormalizations"

        # J/ψ → μμ Gaussian+Chebyshev fit — just swap the workspace config
        law run AnalyticWorkspaceFitTask \\
            --name jpsi_fit \\
            --workspace-config analyses/MyAnalysis/jpsi_workspace.yaml \\
            --histogram-file output/jpsi.root \\
            --method FitDiagnostics
    """

    task_namespace = ""

    name = luigi.Parameter(
        description=(
            "Unique run name.  Outputs are written to "
            "<workspace>/analyticFit_<name>/."
        ),
    )
    workspace_config = luigi.Parameter(
        description=(
            "Path to the YAML workspace config file that specifies the "
            "observable, signal PDF, background PDF, parameters, and channels.  "
            "See analyses/CMS_Run2016_DoubleMuon/zpeak_workspace.yaml for a "
            "fully-annotated example."
        ),
    )
    histogram_file = luigi.Parameter(
        description=(
            "Path to the ROOT file produced by the analysis executable.  "
            "Histograms are expected under a 'histograms/' TDirectory; the "
            "key names must match the 'histogram' fields in the workspace config."
        ),
    )
    method = luigi.Parameter(
        default="FitDiagnostics",
        description=(
            "CMS Combine fit method.  Common choices: FitDiagnostics, "
            "AsymptoticLimits, MultiDimFit.  Default: FitDiagnostics."
        ),
    )
    combine_exe = luigi.Parameter(
        default="",
        description=(
            "Path to the combine binary.  Auto-detected from the build "
            "directory and system PATH when empty."
        ),
    )
    combine_options = luigi.Parameter(
        default="--saveShapes --saveNormalizations",
        description=(
            "Extra command-line options forwarded verbatim to every combine "
            "invocation.  Default: --saveShapes --saveNormalizations."
        ),
    )
    skip_fit = luigi.BoolParameter(
        default=False,
        description=(
            "When True, build workspaces and datacards but do not invoke "
            "combine.  Useful for inspecting the workspace before fitting or "
            "when Combine is not installed.  Default: False."
        ),
    )
    output_dir = luigi.Parameter(default="", description="Override the analytic workspace output directory.")

    @property
    def _out_dir(self) -> str:
        return self.output_dir or os.path.join(WORKSPACE, f"analyticFit_{self.name}")

    def output(self):
        return law.LocalDirectoryTarget(self._out_dir)
