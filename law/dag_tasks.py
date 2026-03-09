"""
Law task that wires the complete analysis DAG end-to-end.

Provides:

  FullAnalysisDAG  (Task)
      Orchestrates the full pipeline:
          SkimTask  →  HistFillTask  →  MergeAll
              → ManifestDatacardTask  (datacards + nuisance coverage validation)
              → ManifestPlotTask      (per-region plots)
              → ManifestFitTask       (Combine or analysis-defined fits)

      All downstream tasks receive the merged OutputManifest written by
      MergeHistograms / MergeMetadata so that they are schema- and
      provenance-aware.

Usage
-----
  source law/env.sh

  law run FullAnalysisDAG \\
      --name myRun \\
      --exe build/analyses/myAnalysis/myanalysis \\
      --submit-config analyses/myAnalysis/cfg/skim_config.txt \\
      --dataset-manifest analyses/myAnalysis/cfg/datasets.yaml \\
      --hist-config analyses/myAnalysis/cfg/hist_config.txt \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --plot-config analyses/myAnalysis/cfg/plots.json \\
      --fitting-backend analysis

  # Skip skim + histfill (start from pre-existing merged outputs):
  law run FullAnalysisDAG \\
      --name myRun \\
      --datacard-config analyses/myAnalysis/cfg/datacard_config.yaml \\
      --plot-config analyses/myAnalysis/cfg/plots.json \\
      --manifest-path /path/to/mergeRun_myRun/histograms/output_manifest.yaml \\
      --skip-skim \\
      --skip-histfill \\
      --skip-merge
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import List, Optional

import luigi  # type: ignore
import law  # type: ignore

_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from performance_recorder import PerformanceRecorder  # noqa: E402
from combine_tasks import ManifestDatacardTask, ManifestFitTask  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))


class FullAnalysisDAG(law.Task):
    """Orchestrate the complete analysis pipeline as a single law task.

    The DAG is structured as:

    .. code-block:: text

        SkimTask          (optional, --skip-skim)
            ↓
        HistFillTask      (optional, --skip-histfill)
            ↓
        MergeAll          (optional, --skip-merge)
            ↓
        ┌─────────────────────────────────────────┐
        │ ManifestDatacardTask  ManifestPlotTask   │
        └────────────────┬────────────────────────┘
                         ↓
                ManifestFitTask

    Each downstream task (datacards, plots, fits) consumes the
    ``OutputManifest`` produced by ``MergeHistograms`` / ``MergeMetadata``
    so that regions, nuisance groups, and histogram schema information flow
    automatically through the pipeline without manual re-specification.

    Parameters
    ----------
    name:
        Unique run identifier.  Controls output directory naming for all
        sub-tasks.
    manifest_path:
        Explicit path to an already-produced ``OutputManifest`` YAML file.
        When provided and ``--skip-merge`` is set, the skim / histfill /
        merge stages are bypassed and this manifest is consumed directly by
        the datacard, plot, and fit tasks.
    datacard_config:
        Path to the YAML datacard configuration (same format as
        :class:`~combine_tasks.CreateDatacard`).
    plot_config:
        Path to the JSON plot configuration (same format as
        :class:`~plot_tasks.MakePlots`).
    exe:
        Path to the analysis executable (used by SkimTask / HistFillTask).
    submit_config:
        Path to the submit_config.txt template (SkimTask / HistFillTask).
    dataset_manifest:
        Path to the dataset manifest YAML (SkimTask / HistFillTask).
    hist_config:
        Path to the histogram fill configuration (HistFillTask).
    merge_input_dir:
        Input directory for MergeAll (defaults to condorSub_<name>).
    fitting_backend:
        Fitting backend for ManifestFitTask: ``"combine"`` or ``"analysis"``.
    method:
        Combine method (used when fitting_backend=combine).
    combine_exe:
        Path to the Combine binary (used when fitting_backend=combine).
    strict_coverage:
        Raise an error on nuisance coverage issues.
    skip_skim:
        Skip the SkimTask stage.
    skip_histfill:
        Skip the HistFillTask stage.
    skip_merge:
        Skip the MergeAll stage (implies the merged manifest already exists).
    skip_plots:
        Skip the ManifestPlotTask stage.
    skip_fits:
        Skip the ManifestFitTask stage.
    """

    task_namespace = ""

    name = luigi.Parameter(
        description="Unique run name.  Controls all output directory paths.",
    )
    manifest_path = luigi.Parameter(
        default="",
        description=(
            "Explicit path to a merged OutputManifest YAML.  When set together "
            "with --skip-merge the merge stage is bypassed and this manifest is "
            "used directly by the datacard/plot/fit tasks."
        ),
    )
    datacard_config = luigi.Parameter(
        default="",
        description="Path to the YAML datacard configuration file.",
    )
    plot_config = luigi.Parameter(
        default="",
        description="Path to the JSON plot configuration file.",
    )
    exe = luigi.Parameter(
        default="",
        description="Path to the analysis executable (SkimTask / HistFillTask).",
    )
    submit_config = luigi.Parameter(
        default="",
        description="Path to submit_config.txt template (SkimTask / HistFillTask).",
    )
    dataset_manifest = luigi.Parameter(
        default="",
        description="Path to the dataset manifest YAML (SkimTask / HistFillTask).",
    )
    hist_config = luigi.Parameter(
        default="",
        description="Path to histogram fill configuration (HistFillTask).",
    )
    merge_input_dir = luigi.Parameter(
        default="",
        description="Input directory for MergeAll (defaults to condorSub_<name>).",
    )
    fitting_backend = luigi.Parameter(
        default="analysis",
        description="Fitting backend: 'combine' or 'analysis'.",
    )
    method = luigi.Parameter(
        default="AsymptoticLimits",
        description="Combine fit method (fitting_backend=combine only).",
    )
    combine_exe = luigi.Parameter(
        default="",
        description="Path to Combine binary (fitting_backend=combine only).",
    )
    strict_coverage = luigi.BoolParameter(
        default=False,
        description="Raise an error on nuisance coverage issues.",
    )
    skip_skim = luigi.BoolParameter(
        default=False,
        description="Skip the SkimTask stage.",
    )
    skip_histfill = luigi.BoolParameter(
        default=False,
        description="Skip the HistFillTask stage.",
    )
    skip_merge = luigi.BoolParameter(
        default=False,
        description="Skip the MergeAll stage.",
    )
    skip_plots = luigi.BoolParameter(
        default=False,
        description="Skip the ManifestPlotTask stage.",
    )
    skip_fits = luigi.BoolParameter(
        default=False,
        description="Skip the ManifestFitTask stage.",
    )

    # ------------------------------------------------------------------ helpers

    @property
    def _dag_dir(self) -> str:
        return os.path.join(WORKSPACE, f"dagRun_{self.name}")

    def _resolved_manifest_path(self) -> str:
        """Return the manifest path to use for downstream tasks.

        If ``--manifest-path`` is set explicitly, return that.  Otherwise
        return the path where ``MergeHistograms`` writes its merged manifest.
        """
        if self.manifest_path:
            return self.manifest_path
        return os.path.join(
            WORKSPACE, f"mergeRun_{self.name}", "histograms", "output_manifest.yaml"
        )

    # ------------------------------------------------------------------ DAG wiring

    def requires(self) -> list:
        reqs: list = []

        # ---- Skim stage ----
        if not self.skip_skim and self.exe and self.submit_config and self.dataset_manifest:
            try:
                from analysis_tasks import SkimTask  # noqa: E402
                reqs.append(
                    SkimTask(
                        exe=self.exe,
                        submit_config=self.submit_config,
                        dataset_manifest=self.dataset_manifest,
                        name=self.name,
                    )
                )
            except ImportError:
                pass

        # ---- HistFill stage ----
        if not self.skip_histfill and self.exe and self.submit_config and self.dataset_manifest:
            try:
                from analysis_tasks import HistFillTask  # noqa: E402
                reqs.append(
                    HistFillTask(
                        exe=self.exe,
                        submit_config=self.hist_config or self.submit_config,
                        dataset_manifest=self.dataset_manifest,
                        name=self.name,
                    )
                )
            except ImportError:
                pass

        # ---- Merge stage ----
        if not self.skip_merge:
            try:
                from merge_tasks import MergeAll  # noqa: E402
                reqs.append(
                    MergeAll(
                        name=self.name,
                        input_dir=self.merge_input_dir,
                    )
                )
            except ImportError:
                pass

        # ---- Datacard stage ----
        if self.datacard_config:
            reqs.append(
                ManifestDatacardTask(
                    name=self.name,
                    datacard_config=self.datacard_config,
                    manifest_path=self._resolved_manifest_path(),
                    strict_coverage=self.strict_coverage,
                )
            )

        # ---- Plot stage ----
        if not self.skip_plots and self.plot_config:
            try:
                from manifest_plot_tasks import ManifestPlotTask  # noqa: E402
                reqs.append(
                    ManifestPlotTask(
                        name=self.name,
                        manifest_path=self._resolved_manifest_path(),
                        plot_config=self.plot_config,
                    )
                )
            except ImportError:
                pass

        # ---- Fit stage ----
        if not self.skip_fits and self.datacard_config:
            reqs.append(
                ManifestFitTask(
                    name=self.name,
                    datacard_config=self.datacard_config,
                    manifest_path=self._resolved_manifest_path(),
                    fitting_backend=self.fitting_backend,
                    method=self.method,
                    combine_exe=self.combine_exe,
                )
            )

        return reqs

    def output(self):
        return law.LocalFileTarget(os.path.join(self._dag_dir, "dag.done"))

    def run(self):
        Path(self._dag_dir).mkdir(parents=True, exist_ok=True)

        summary = {
            "name": self.name,
            "dag_dir": self._dag_dir,
            "manifest_path": self._resolved_manifest_path(),
            "stages": {
                "skim": not self.skip_skim,
                "histfill": not self.skip_histfill,
                "merge": not self.skip_merge,
                "datacards": bool(self.datacard_config),
                "plots": not self.skip_plots and bool(self.plot_config),
                "fits": not self.skip_fits and bool(self.datacard_config),
            },
        }
        summary_path = os.path.join(self._dag_dir, "dag_summary.json")
        with open(summary_path, "w") as fh:
            json.dump(summary, fh, indent=2)

        self.publish_message(
            f"FullAnalysisDAG complete for run '{self.name}'.  "
            f"Summary: {summary_path}"
        )

        with open(self.output().path, "w") as fh:
            fh.write(json.dumps(summary, indent=2) + "\n")
