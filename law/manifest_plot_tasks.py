"""
Manifest-aware plotting task for the RDFAnalyzerCore LAW workflow.

This module provides :class:`ManifestPlotTask`, a LAW task that creates
analysis-style stack plots from merged histogram outputs described by an
:class:`~output_schema.OutputManifest`.

Unlike :class:`~plot_tasks.MakePlots` (which reads from a raw meta ROOT
file), :class:`ManifestPlotTask`:

* Loads the merged :class:`~output_schema.OutputManifest` from
  ``--manifest-path``.
* Reads ``regions`` from the manifest and creates one set of plots per
  region using the histogram ROOT file referenced in the manifest's
  ``histograms`` schema entry.
* Applies the process-level configuration from ``--plot-config`` (same
  JSON format as :class:`~plot_tasks.MakePlots`) to each region
  independently.
* Records task provenance (manifest hashes, region list) in a
  ``provenance.json`` file alongside the plots.

When no regions are defined in the manifest a single combined set of plots
is created for each entry in the plot-config.

Outputs are written to ``<workspace>/manifestPlots_<name>/``.

Usage
-----
  source law/env.sh

  law run ManifestPlotTask \\
      --name myRun \\
      --manifest-path /path/to/mergeRun_myRun/histograms/output_manifest.yaml \\
      --plot-config analyses/myAnalysis/cfg/plots.json
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional

import luigi  # type: ignore
import law  # type: ignore

_HERE = os.path.dirname(os.path.abspath(__file__))
_BUILD_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "build", "python"))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "core", "python"))
for _p in (_BUILD_PYTHON, _HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from performance_recorder import PerformanceRecorder  # noqa: E402
from output_schema import OutputManifest  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, ".."))


def _build_plot_request_from_dict(
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
    """Build a :class:`rdfanalyzer.PlotRequest` from plain Python objects.

    This is a standalone copy of the helper in :mod:`plot_tasks` so that this
    module does not need to import :mod:`plot_tasks` (which requires the
    optional ``framework`` module).
    """
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


class ManifestPlotTask(law.Task):
    """Manifest-aware batch plotting from merged histogram outputs.

    Unlike :class:`~plot_tasks.MakePlots` which reads from a raw meta ROOT
    file, this task:

    1. Loads the merged :class:`~output_schema.OutputManifest` from
       ``--manifest-path``.
    2. Reads ``regions`` from the manifest and creates one plot per region
       using the histogram ROOT file referenced in the manifest's ``histograms``
       schema entry.
    3. Applies the process-level configuration from ``--plot-config`` (same
       JSON format as :class:`~plot_tasks.MakePlots`) to each region
       independently.
    4. Records task provenance (manifest hashes, region list) in a
       ``provenance.json`` file alongside the plots.

    When no regions are defined in the manifest a single combined plot is
    created for each entry in the plot-config.

    Outputs are written to ``<workspace>/manifestPlots_<name>/``.

    Plot-config JSON format
    -----------------------
    Same format as :class:`~plot_tasks.MakePlots`:

    .. code-block:: json

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
                "color": 2
              }
            ]
          }
        ]

    When ``regions`` are present in the manifest each config entry is
    replicated per region and the ``outputFile`` is automatically prefixed
    with the region name (e.g. ``pt.pdf`` → ``signal_region/pt.pdf``).
    """

    task_namespace = ""

    name = luigi.Parameter(
        description=(
            "Run name.  Plot outputs are written to "
            "<workspace>/manifestPlots_<name>/."
        ),
    )
    manifest_path = luigi.Parameter(
        description=(
            "Path to the merged OutputManifest YAML file produced by "
            "MergeHistograms or MergeMetadata.  The manifest's 'histograms' "
            "entry provides the merged histogram ROOT file used for all plots."
        ),
    )
    plot_config = luigi.Parameter(
        description=(
            "Path to a JSON file describing the list of plots to create.  "
            "Same format as MakePlots."
        ),
    )

    @property
    def _plots_dir(self) -> str:
        return os.path.join(WORKSPACE, f"manifestPlots_{self.name}")

    def output(self):
        return law.LocalDirectoryTarget(self._plots_dir)

    def _load_plot_configs(self) -> List[Dict]:
        with open(self.plot_config) as fh:
            configs = json.load(fh)
        if not isinstance(configs, list):
            raise ValueError(
                f"plot_config must be a JSON array, got: {type(configs).__name__}"
            )
        return configs

    def _load_manifest(self) -> OutputManifest:
        if not os.path.exists(self.manifest_path):
            raise RuntimeError(
                f"Manifest file not found: {self.manifest_path!r}.  "
                "Run MergeAll (or MergeHistograms) first."
            )
        try:
            return OutputManifest.load_yaml(self.manifest_path)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(
                f"Failed to load manifest from {self.manifest_path!r}: {exc}"
            ) from exc

    def run(self):
        manifest = self._load_manifest()
        plot_cfgs = self._load_plot_configs()

        meta_file = ""
        if manifest.histograms is not None:
            meta_file = manifest.histograms.output_file
        if not meta_file or not os.path.exists(meta_file):
            self.publish_message(
                f"WARNING: Histogram ROOT file not found at {meta_file!r}.  "
                "Plots will be attempted but PlottingUtility may fail."
            )

        region_names: List[str] = [r.name for r in manifest.regions if r.name]
        self.publish_message(
            f"ManifestPlotTask: {len(plot_cfgs)} plot config(s), "
            f"{len(region_names) or 1} region(s), "
            f"histogram file: {meta_file!r}"
        )

        Path(self._plots_dir).mkdir(parents=True, exist_ok=True)

        provenance = {
            "task": "ManifestPlotTask",
            "name": self.name,
            "manifest_path": self.manifest_path,
            "manifest_framework_hash": manifest.framework_hash,
            "manifest_user_repo_hash": manifest.user_repo_hash,
            "regions": region_names,
            "n_plot_configs": len(plot_cfgs),
        }

        plots_created: List[str] = []
        errors: List[str] = []

        with PerformanceRecorder("ManifestPlotTask") as rec:
            try:
                import rdfanalyzer  # type: ignore
                pu = rdfanalyzer.PlottingUtility()
                _rdfanalyzer_available = True
            except ImportError:
                pu = None
                _rdfanalyzer_available = False
                rdfanalyzer = None
                self.publish_message(
                    "WARNING: rdfanalyzer module not available.  "
                    "Plot stubs will be written instead of real plots."
                )

            contexts = region_names if region_names else [""]
            for cfg in plot_cfgs:
                for region in contexts:
                    out_file = cfg.get("outputFile", "plot.pdf")
                    if region:
                        out_file = os.path.join(region, out_file)
                    out_path = os.path.join(self._plots_dir, out_file)
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)

                    if not _rdfanalyzer_available or not os.path.exists(meta_file):
                        # Write a stub so the output target is satisfied
                        with open(out_path + ".stub.json", "w") as fh:
                            json.dump(
                                {
                                    "stub": True,
                                    "region": region,
                                    "config": cfg,
                                    "meta_file": meta_file,
                                },
                                fh,
                                indent=2,
                            )
                        plots_created.append(out_path + ".stub.json")
                        continue

                    try:
                        req = _build_plot_request_from_dict(
                            meta_file=meta_file,
                            output_file=out_path,
                            title=cfg.get("title", region or ""),
                            x_axis_title=cfg.get("xAxisTitle", ""),
                            y_axis_title=cfg.get("yAxisTitle", "Counts"),
                            log_y=bool(cfg.get("logY", False)),
                            draw_ratio=bool(cfg.get("drawRatio", True)),
                            processes_cfg=cfg.get("processes", []),
                            rdfanalyzer=rdfanalyzer,
                        )
                        result = pu.makeStackPlot(req)
                        if result.success:
                            plots_created.append(out_path)
                            self.publish_message(
                                f"  [{region or 'combined'}] {out_file} saved "
                                f"(MC={result.mcIntegral:.3g}, data={result.dataIntegral:.3g})"
                            )
                        else:
                            errors.append(f"PlottingUtility failed for {out_file!r}: {result.message}")
                    except Exception as exc:  # noqa: BLE001
                        errors.append(f"Error plotting {out_file!r}: {exc}")

        rec.save(os.path.join(self._plots_dir, "manifest_plot.perf.json"))

        provenance["plots_created"] = plots_created
        provenance["errors"] = errors
        prov_path = os.path.join(self._plots_dir, "provenance.json")
        with open(prov_path, "w") as fh:
            json.dump(provenance, fh, indent=2)

        if errors:
            msg = f"ManifestPlotTask completed with {len(errors)} error(s):\n" + "\n".join(
                f"  {e}" for e in errors
            )
            self.publish_message(msg)
        else:
            self.publish_message(
                f"ManifestPlotTask complete.  {len(plots_created)} plot(s) written to {self._plots_dir}."
            )
