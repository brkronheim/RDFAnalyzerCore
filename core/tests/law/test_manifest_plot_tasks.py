from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "core", "python", "law")
_CORE_PY = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

try:
    import luigi  # noqa: F401
    import law  # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

if _LAW_AVAILABLE:
    import manifest_plot_tasks

_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestManifestPlotTasks(unittest.TestCase):
    def test_build_plot_request_from_dict_converts_processes(self):
        class FakePlotProcessConfig:
            pass

        class FakePlotRequest:
            pass

        fake_rdf = type(
            "FakeRDFAnalyzer",
            (),
            {
                "PlotRequest": FakePlotRequest,
                "PlotProcessConfig": FakePlotProcessConfig,
            },
        )

        req = manifest_plot_tasks._build_plot_request_from_dict(
            meta_file="meta.root",
            output_file="plot.pdf",
            title="Test Plot",
            x_axis_title="p_{T}",
            y_axis_title="Events",
            log_y=True,
            draw_ratio=False,
            processes_cfg=[
                {
                    "directory": "signal",
                    "histogramName": "pt",
                    "legendLabel": "Signal",
                    "color": 2,
                    "scale": 1.5,
                    "normalizationHistogram": "counter_weightSum_signal",
                    "isData": False,
                },
                {
                    "directory": "data",
                    "histogramName": "data_pt",
                    "legendLabel": "Data",
                    "color": 1,
                    "isData": True,
                },
            ],
            rdfanalyzer=fake_rdf,
        )

        self.assertEqual(req.metaFile, "meta.root")
        self.assertEqual(req.outputFile, "plot.pdf")
        self.assertEqual(req.title, "Test Plot")
        self.assertEqual(len(req.processes), 2)
        self.assertEqual(req.processes[0].directory, "signal")
        self.assertEqual(req.processes[0].scale, 1.5)
        self.assertTrue(req.processes[1].isData)

    def test_load_plot_configs_requires_array_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
            json.dump({"outputFile": "plot.pdf"}, fh)
            config_path = fh.name

        try:
            task = manifest_plot_tasks.ManifestPlotTask(
                name="run1",
                manifest_path="/tmp/missing.yaml",
                plot_config=config_path,
            )
            with self.assertRaises(ValueError):
                task._load_plot_configs()
        finally:
            os.remove(config_path)

    def test_load_manifest_missing_file_raises(self):
        task = manifest_plot_tasks.ManifestPlotTask(
            name="run2",
            manifest_path="/tmp/definitely_missing_manifest.yaml",
            plot_config="/tmp/plot_config.json",
        )
        with self.assertRaises(RuntimeError) as ctx:
            task._load_manifest()
        self.assertIn("Manifest file not found", str(ctx.exception))

    def test_output_directory_uses_workspace_prefix(self):
        task = manifest_plot_tasks.ManifestPlotTask(
            name="run3",
            manifest_path="/tmp/missing.yaml",
            plot_config="/tmp/plot_config.json",
        )
        self.assertIn("manifestPlots_run3", task.output().path)
