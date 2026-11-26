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
    source core/python/law/env.sh

  law run ManifestPlotTask \\
      --name myRun \\
      --manifest-path /path/to/mergeRun_myRun/histograms/output_manifest.yaml \\
      --plot-config analyses/myAnalysis/cfg/plots.json
"""

from __future__ import annotations

import os
import sys

import luigi  # type: ignore
import law  # type: ignore

_HERE = os.path.dirname(os.path.abspath(__file__))
_BUILD_PYTHON = os.path.abspath(os.path.join(_HERE, "..", "build", "python"))
_CORE_PYTHON = os.path.abspath(os.path.join(_HERE, ".."))
for _p in (_BUILD_PYTHON, _HERE, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from rdfproduction.manifest_plotting import _ManifestPlotOperations, _build_plot_request_from_dict  # noqa: F401, E402
from output_schema import OutputManifest  # noqa: E402

WORKSPACE = os.path.abspath(os.path.join(_HERE, "..", "..", ".."))



class ManifestPlotTask(_ManifestPlotOperations, law.Task):
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
    merge_input_dir = luigi.Parameter(
        default="",
        description=(
            "Optional MergeAll input directory.  When set, this task declares "
            "a dependency on MergeAll so plotting waits for merged histograms."
        ),
    )
    plot_config = luigi.Parameter(
        description=(
            "Path to a JSON file describing the list of plots to create.  "
            "Same format as MakePlots."
        ),
    )

    output_dir = luigi.Parameter(default="", description="Override the manifest plot output directory.")

    @property
    def _plots_dir(self) -> str:
        return self.output_dir or os.path.join(WORKSPACE, f"manifestPlots_{self.name}")

    def requires(self):
        if not self.merge_input_dir:
            return None
        from merge_tasks import MergeAll  # noqa: E402

        return MergeAll.req(self, input_dir=self.merge_input_dir, output_dir="")

    def output(self):
        return law.LocalDirectoryTarget(self._plots_dir)
