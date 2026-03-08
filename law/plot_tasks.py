# coding: utf-8

"""
Law tasks for creating analysis-style ROOT stack plots using PlottingUtility.

This module provides two tasks:

  MakePlot  (PlotTask)
      → Creates a single stack plot from a meta output ROOT file.
        The process list, histogram names, colours and other options are
        configured entirely through luigi parameters.

  MakePlots  (PlotTask, law.LocalWorkflow)
      → Batch variant: one branch per entry in a JSON plot-config file.
        Each entry may contain the same fields as the MakePlot parameters.
        Requires a single meta ROOT file shared by all branches.

Usage
-----
  source law/env.sh

  # Single plot
  law run MakePlot \\
      --version v1 \\
      --meta-file /path/to/meta.root \\
      --histogram-name pt \\
      --output-file /path/to/pt.pdf \\
      --x-axis-title "p_{T} [GeV]" \\
      --processes '[{"directory":"signal","histogramName":"pt","legendLabel":"Signal","color":2,"normalizationHistogram":"counter_weightSum_signal"}]'

  # Batch plots (JSON config)
  law run MakePlots \\
      --version v1 \\
      --meta-file /path/to/meta.root \\
      --plot-config /path/to/plots.json

Example JSON plot-config (plots.json)
--------------------------------------
  [
    {
      "outputFile": "pt.pdf",
      "title": "Transverse Momentum",
      "xAxisTitle": "p_{T} [GeV]",
      "yAxisTitle": "Events",
      "logY": false,
      "drawRatio": true,
      "processes": [
        {
          "directory": "signal",
          "histogramName": "pt",
          "legendLabel": "Signal MC",
          "color": 2,
          "normalizationHistogram": "counter_weightSum_signal"
        },
        {
          "directory": "data",
          "histogramName": "pt",
          "legendLabel": "Data",
          "color": 1,
          "isData": true
        }
      ]
    }
  ]
"""

from __future__ import annotations

import json
import os
import sys

import luigi  # type: ignore
import law  # type: ignore

from framework import Task, PlotTask, view_output_plots  # noqa: E402

# ---------------------------------------------------------------------------
# Make sure the compiled rdfanalyzer module is importable
# ---------------------------------------------------------------------------
_HERE = os.path.dirname(os.path.abspath(__file__))
_BUILD_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "build", "python"))
if _BUILD_PYTHON not in sys.path:
    sys.path.insert(0, _BUILD_PYTHON)
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from performance_recorder import PerformanceRecorder, perf_path_for  # noqa: E402


def _build_plot_request(
    meta_file: str,
    output_file: str,
    title: str,
    x_axis_title: str,
    y_axis_title: str,
    log_y: bool,
    draw_ratio: bool,
    processes_cfg: list,
    rdfanalyzer,
):
    """Build a :class:`rdfanalyzer.PlotRequest` from plain Python objects."""

    req = rdfanalyzer.PlotRequest()
    req.metaFile = meta_file
    req.outputFile = output_file
    req.title = title
    req.xAxisTitle = x_axis_title
    req.yAxisTitle = y_axis_title
    req.logY = log_y
    req.drawRatio = draw_ratio

    procs = []
    for cfg in processes_cfg:
        proc = rdfanalyzer.PlotProcessConfig()
        proc.directory = cfg.get("directory", "")
        proc.histogramName = cfg.get("histogramName", "")
        proc.legendLabel = cfg.get("legendLabel", "")
        proc.color = int(cfg.get("color", 1))
        proc.scale = float(cfg.get("scale", 1.0))
        proc.normalizationHistogram = cfg.get("normalizationHistogram", "")
        proc.isData = bool(cfg.get("isData", False))
        procs.append(proc)

    req.processes = procs
    return req


# ===========================================================================
# MakePlot
# ===========================================================================

class MakePlot(PlotTask):
    """
    Create a single analysis-style ROOT stack plot.

    The plot is built by the C++ :class:`rdfanalyzer.PlottingUtility` directly
    from the meta output ROOT file produced by the analysis framework.
    """

    meta_file = luigi.Parameter(
        description="Path to the analysis meta output ROOT file.",
    )
    histogram_name = luigi.Parameter(
        description="Name of the TH1D histogram inside each process directory.",
    )
    output_file = luigi.Parameter(
        description="Destination path for the saved plot (PDF, PNG, …).",
    )
    title = luigi.Parameter(
        default="",
        description="Canvas/histogram title; default: empty string.",
    )
    x_axis_title = luigi.Parameter(
        default="",
        description="Label for the x-axis.",
    )
    y_axis_title = luigi.Parameter(
        default="Counts",
        description="Label for the y-axis; default: 'Counts'.",
    )
    log_y = luigi.BoolParameter(
        default=False,
        description="Draw y-axis in log scale; default: False.",
    )
    draw_ratio = luigi.BoolParameter(
        default=True,
        description="Add a data/MC ratio panel; default: True.",
    )
    processes = luigi.Parameter(
        default="[]",
        description=(
            "JSON-encoded list of process config dicts.  Each dict may contain: "
            "directory, histogramName, legendLabel, color, scale, "
            "normalizationHistogram, isData.  "
            "The 'histogramName' field defaults to --histogram-name when absent."
        ),
    )

    def output(self):
        return law.LocalFileTarget(self.output_file)

    @law.decorator.log
    @law.decorator.safe_output
    @view_output_plots
    def run(self):
        import rdfanalyzer  # type: ignore

        processes_cfg: list = json.loads(self.processes)
        # Fill histogramName from the shared parameter when absent
        for proc in processes_cfg:
            if not proc.get("histogramName"):
                proc["histogramName"] = self.histogram_name

        req = _build_plot_request(
            meta_file=self.meta_file,
            output_file=self.output().path,
            title=self.title,
            x_axis_title=self.x_axis_title,
            y_axis_title=self.y_axis_title,
            log_y=self.log_y,
            draw_ratio=self.draw_ratio,
            processes_cfg=processes_cfg,
            rdfanalyzer=rdfanalyzer,
        )

        with PerformanceRecorder("MakePlot") as rec:
            pu = rdfanalyzer.PlottingUtility()
            result = pu.makeStackPlot(req)

        rec.save(perf_path_for(self.output().path))

        if not result.success:
            raise RuntimeError(f"PlottingUtility failed: {result.message}")

        self.publish_message(
            f"saved {self.output().path}  "
            f"(MC integral={result.mcIntegral:.3g}, "
            f"data integral={result.dataIntegral:.3g})"
        )


# ===========================================================================
# MakePlots  (batch workflow, one branch per plot in a JSON config)
# ===========================================================================

class MakePlots(PlotTask, law.LocalWorkflow):
    """
    Create multiple analysis-style ROOT stack plots in batch.

    Each entry in the JSON plot-config file becomes one workflow branch.
    The meta ROOT file is shared across all branches.

    Plot-config JSON format
    -----------------------
    A JSON array where each element may contain:

    * ``outputFile``  (str, required)
    * ``title``       (str, default "")
    * ``xAxisTitle``  (str, default "")
    * ``yAxisTitle``  (str, default "Counts")
    * ``logY``        (bool, default false)
    * ``drawRatio``   (bool, default true)
    * ``processes``   (list of process-config dicts)

    Each process-config dict may contain: ``directory``, ``histogramName``,
    ``legendLabel``, ``color``, ``scale``, ``normalizationHistogram``,
    ``isData``.
    """

    meta_file = luigi.Parameter(
        description="Path to the analysis meta output ROOT file.",
    )
    plot_config = luigi.Parameter(
        description="Path to a JSON file describing the list of plots to create.",
    )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _load_plot_configs(self) -> list[dict]:
        with open(self.plot_config) as fh:
            configs = json.load(fh)
        if not isinstance(configs, list):
            raise ValueError(
                f"plot_config must be a JSON array, got: {type(configs).__name__}"
            )
        return configs

    # ------------------------------------------------------------------
    # Workflow interface
    # ------------------------------------------------------------------

    def create_branch_map(self):
        return {i: cfg for i, cfg in enumerate(self._load_plot_configs())}

    def output(self):
        cfg = self.branch_data
        return law.LocalFileTarget(cfg["outputFile"])

    @law.decorator.log
    @law.decorator.safe_output
    @view_output_plots
    def run(self):
        import rdfanalyzer  # type: ignore

        cfg: dict = self.branch_data

        req = _build_plot_request(
            meta_file=self.meta_file,
            output_file=self.output().path,
            title=cfg.get("title", ""),
            x_axis_title=cfg.get("xAxisTitle", ""),
            y_axis_title=cfg.get("yAxisTitle", "Counts"),
            log_y=bool(cfg.get("logY", False)),
            draw_ratio=bool(cfg.get("drawRatio", True)),
            processes_cfg=cfg.get("processes", []),
            rdfanalyzer=rdfanalyzer,
        )

        with PerformanceRecorder(f"MakePlots[branch={self.branch}]") as rec:
            pu = rdfanalyzer.PlottingUtility()
            result = pu.makeStackPlot(req)

        rec.save(perf_path_for(self.output().path))

        if not result.success:
            raise RuntimeError(f"PlottingUtility failed: {result.message}")

        self.publish_message(
            f"branch {self.branch}: saved {self.output().path}  "
            f"(MC integral={result.mcIntegral:.3g}, "
            f"data integral={result.dataIntegral:.3g})"
        )
