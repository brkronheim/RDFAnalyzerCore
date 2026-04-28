import json
import os
import sys
import tempfile
import types
import unittest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "core", "python", "law")
if _LAW_DIR not in sys.path:
    sys.path.insert(0, _LAW_DIR)

try:
    import luigi  # noqa: F401
    import law  # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

if _LAW_AVAILABLE:
    import plot_tasks


_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestPlotTasks(unittest.TestCase):
    def test_build_plot_request_converts_process_config(self):
        class FakePlotProcessConfig:
            pass

        class FakePlotRequest:
            pass

        fake_rdf = types.SimpleNamespace(
            PlotRequest=FakePlotRequest,
            PlotProcessConfig=FakePlotProcessConfig,
        )

        processes = [
            {"directory": "signal", "histogramName": "pt", "legendLabel": "Signal", "color": 2, "normalizationHistogram": "counter_weightSum_signal", "isData": False},
            {"directory": "data", "histogramName": "data_pt", "legendLabel": "Data", "color": 1, "isData": True},
        ]
        req = plot_tasks._build_plot_request(
            meta_file="meta.root",
            output_file="plot.pdf",
            title="Test Plot",
            x_axis_title="p_{T}",
            y_axis_title="Events",
            log_y=True,
            draw_ratio=False,
            processes_cfg=processes,
            rdfanalyzer=fake_rdf,
        )

        self.assertIsInstance(req, FakePlotRequest)
        self.assertEqual(req.metaFile, "meta.root")
        self.assertEqual(len(req.processes), 2)
        self.assertEqual(req.processes[0].directory, "signal")
        self.assertEqual(req.processes[1].isData, True)

    def test_load_plot_configs_and_branch_map(self):
        configs = [
            {"outputFile": "plot1.pdf", "title": "Plot 1", "processes": []},
            {"outputFile": "plot2.pdf", "title": "Plot 2", "processes": []},
        ]
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
            json.dump(configs, fh)
            config_path = fh.name

        try:
            task = plot_tasks.MakePlots(meta_file="meta.root", plot_config=config_path, branch=0)
            loaded = task._load_plot_configs()
            self.assertEqual(loaded, configs)
            self.assertEqual(task.create_branch_map(), {0: configs[0], 1: configs[1]})
        finally:
            os.remove(config_path)

    def test_load_plot_configs_rejects_non_list_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as fh:
            json.dump({"outputFile": "plot.pdf"}, fh)
            config_path = fh.name

        try:
            task = plot_tasks.MakePlots(meta_file="meta.root", plot_config=config_path, branch=0)
            with self.assertRaises(ValueError):
                task._load_plot_configs()
        finally:
            os.remove(config_path)
