#!/usr/bin/env python3
"""
Tests for the dag_tasks law module.

These tests verify:
  - Module structure and imports
  - FullAnalysisDAG task construction and parameter handling
  - Output path computation
  - DAG wiring (requires() returns the right sub-tasks)
  - run() writes dag.done and dag_summary.json
  - Graceful handling of missing optional sub-task modules
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PY = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optional dependency guard
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


# ===========================================================================
# Helpers
# ===========================================================================

def _write_manifest(directory: str, **kwargs) -> str:
    import yaml
    manifest = {
        "manifest_version": 1,
        "skim": None,
        "histograms": None,
        "metadata": None,
        "cutflow": None,
        "law_artifacts": [],
        "framework_hash": "fw_test",
        "user_repo_hash": None,
        "config_mtime": None,
        "dataset_manifest_provenance": None,
    }
    manifest.update(kwargs)
    os.makedirs(directory, exist_ok=True)
    path = os.path.join(directory, "output_manifest.yaml")
    with open(path, "w") as fh:
        yaml.dump(manifest, fh)
    return path


# ===========================================================================
# Tests
# ===========================================================================


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestDagTasksImport(unittest.TestCase):
    """Smoke tests: dag_tasks imports without errors."""

    def test_importable(self):
        import dag_tasks  # noqa: F401

    def test_full_dag_defined(self):
        import dag_tasks
        self.assertTrue(hasattr(dag_tasks, "FullAnalysisDAG"))

    def test_task_namespace(self):
        import dag_tasks
        self.assertEqual(dag_tasks.FullAnalysisDAG.task_namespace, "")


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestFullAnalysisDAGConstruction(unittest.TestCase):
    """Test parameter defaults and property helpers."""

    def _make_task(self, **kwargs):
        import dag_tasks
        defaults = dict(name="testDAG")
        defaults.update(kwargs)
        return dag_tasks.FullAnalysisDAG(**defaults)

    def test_default_parameters(self):
        task = self._make_task()
        self.assertEqual(task.fitting_backend, "analysis")
        self.assertFalse(task.skip_skim)
        self.assertFalse(task.skip_histfill)
        self.assertFalse(task.skip_merge)
        self.assertFalse(task.skip_plots)
        self.assertFalse(task.skip_fits)

    def test_dag_dir_property(self):
        import dag_tasks
        task = self._make_task(name="myRun")
        self.assertIn("dagRun_myRun", task._dag_dir)

    def test_output_is_done_marker(self):
        task = self._make_task(name="myRun2")
        out = task.output()
        self.assertIn("dagRun_myRun2", out.path)
        self.assertTrue(out.path.endswith("dag.done"))

    def test_resolved_manifest_uses_explicit_when_set(self):
        task = self._make_task(manifest_path="/explicit/path/manifest.yaml")
        self.assertEqual(task._resolved_manifest_path(), "/explicit/path/manifest.yaml")

    def test_resolved_manifest_defaults_to_merge_dir(self):
        import dag_tasks
        task = self._make_task(name="defaultManifest", manifest_path="")
        path = task._resolved_manifest_path()
        self.assertIn("mergeRun_defaultManifest", path)
        self.assertTrue(path.endswith("output_manifest.yaml"))
        self.assertIn(dag_tasks._MERGED_HISTOGRAM_MANIFEST_RELPATH, path)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestFullAnalysisDAGRequires(unittest.TestCase):
    """Test that requires() returns the expected sub-tasks."""

    def _make_task(self, **kwargs):
        import dag_tasks
        defaults = dict(
            name="reqDAG",
            skip_skim=True,
            skip_histfill=True,
        )
        defaults.update(kwargs)
        return dag_tasks.FullAnalysisDAG(**defaults)

    def test_no_requirements_when_all_skipped(self):
        task = self._make_task(
            skip_skim=True,
            skip_histfill=True,
            skip_merge=True,
            skip_plots=True,
            skip_fits=True,
            datacard_config="",
            plot_config="",
        )
        reqs = task.requires()
        self.assertEqual(reqs, [])

    def test_manifest_datacard_task_included_when_config_set(self):
        import dag_tasks
        import combine_tasks
        task = self._make_task(
            skip_merge=True,
            skip_plots=True,
            skip_fits=True,
            datacard_config="some_config.yaml",
            manifest_path="/some/manifest.yaml",
        )
        reqs = task.requires()
        types = [type(r).__name__ for r in reqs]
        self.assertIn("ManifestDatacardTask", types)

    def test_manifest_fit_task_included_when_datacard_config_set(self):
        import dag_tasks
        task = self._make_task(
            skip_merge=True,
            skip_plots=True,
            skip_fits=False,
            datacard_config="some_config.yaml",
            manifest_path="/some/manifest.yaml",
        )
        reqs = task.requires()
        types = [type(r).__name__ for r in reqs]
        self.assertIn("ManifestFitTask", types)

    def test_fit_task_not_included_when_no_datacard_config(self):
        task = self._make_task(
            skip_merge=True,
            skip_plots=True,
            skip_fits=False,
            datacard_config="",
        )
        reqs = task.requires()
        types = [type(r).__name__ for r in reqs]
        self.assertNotIn("ManifestFitTask", types)

    def test_merge_all_included_when_not_skipped(self):
        # Patch the merge_tasks import inside dag_tasks.requires() so that
        # merge_tasks.py is not imported here (which would cause test isolation
        # issues with test_executor_parity's output_schema reload).
        mock_merge_all = MagicMock()
        mock_merge_all.__name__ = "MergeAll"
        type(mock_merge_all()).__name__ = "MergeAll"
        with patch.dict(sys.modules, {"merge_tasks": MagicMock(MergeAll=mock_merge_all)}):
            task = self._make_task(
                skip_merge=False,
                skip_plots=True,
                skip_fits=True,
                datacard_config="",
            )
            reqs = task.requires()
        types = [type(r).__name__ for r in reqs]
        self.assertIn("MergeAll", types)

    def test_merge_all_not_included_when_skipped(self):
        task = self._make_task(
            skip_merge=True,
            skip_plots=True,
            skip_fits=True,
            datacard_config="",
        )
        reqs = task.requires()
        types = [type(r).__name__ for r in reqs]
        self.assertNotIn("MergeAll", types)

    def test_skim_leaf_forwards_file_source_parameters(self):
        class SkimTask:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        mod = types.ModuleType("analysis_tasks")
        mod.SkimTask = SkimTask

        task = self._make_task(
            skip_skim=False,
            skip_histfill=True,
            skip_merge=True,
            skip_plots=True,
            skip_fits=True,
            exe="analysis.exe",
            submit_config="submit_config.txt",
            dataset_manifest="datasets.yaml",
            file_source="nano",
            file_source_name="nanoFiles",
        )
        with patch.dict(sys.modules, {"analysis_tasks": mod}):
            reqs = task.requires()

        self.assertEqual(len(reqs), 1)
        self.assertEqual(type(reqs[0]).__name__, "SkimTask")
        self.assertEqual(reqs[0].kwargs["file_source"], "nano")
        self.assertEqual(reqs[0].kwargs["file_source_name"], "nanoFiles")

    def test_histfill_leaf_uses_skim_outputs_when_skim_enabled(self):
        class HistFillTask:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        mod = types.ModuleType("analysis_tasks")
        mod.HistFillTask = HistFillTask

        task = self._make_task(
            skip_skim=False,
            skip_histfill=False,
            skip_merge=True,
            skip_plots=True,
            skip_fits=True,
            exe="analysis.exe",
            submit_config="submit_config.txt",
            hist_config="hist_config.txt",
            dataset_manifest="datasets.yaml",
        )
        with patch.dict(sys.modules, {"analysis_tasks": mod}):
            reqs = task.requires()

        self.assertEqual(len(reqs), 1)
        self.assertEqual(type(reqs[0]).__name__, "HistFillTask")
        self.assertEqual(reqs[0].kwargs["skim_name"], "reqDAG")
        self.assertEqual(reqs[0].kwargs["submit_config"], "hist_config.txt")

    def test_merge_uses_hist_output_directory_by_default(self):
        import dag_tasks

        class MergeAll:
            def __init__(self, **kwargs):
                self.kwargs = kwargs

        mod = types.ModuleType("merge_tasks")
        mod.MergeAll = MergeAll

        task = self._make_task(
            skip_skim=False,
            skip_histfill=False,
            skip_merge=False,
            skip_plots=True,
            skip_fits=True,
            datacard_config="",
            exe="analysis.exe",
            submit_config="submit_config.txt",
            dataset_manifest="datasets.yaml",
        )
        with patch.dict(sys.modules, {"merge_tasks": mod}):
            reqs = task.requires()

        self.assertEqual(len(reqs), 1)
        self.assertEqual(type(reqs[0]).__name__, "MergeAll")
        self.assertEqual(
            reqs[0].kwargs["input_dir"],
            os.path.join(dag_tasks.WORKSPACE, "histRun_reqDAG", "outputs"),
        )

    def test_fit_task_receives_merge_input_dir_for_full_pipeline(self):
        import dag_tasks

        task = self._make_task(
            skip_skim=False,
            skip_histfill=False,
            skip_merge=False,
            skip_plots=True,
            skip_fits=False,
            datacard_config="some_config.yaml",
            exe="analysis.exe",
            submit_config="submit_config.txt",
            dataset_manifest="datasets.yaml",
        )
        reqs = task.requires()
        self.assertEqual(len(reqs), 1)
        self.assertEqual(type(reqs[0]).__name__, "ManifestFitTask")
        self.assertEqual(
            reqs[0].merge_input_dir,
            os.path.join(dag_tasks.WORKSPACE, "histRun_reqDAG", "outputs"),
        )


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestFullAnalysisDAGRun(unittest.TestCase):
    """Test the run() method of FullAnalysisDAG."""

    def test_run_writes_dag_done_and_summary(self):
        """run() writes dag.done and dag_summary.json."""
        import dag_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            import dag_tasks as dt
            orig = dt.FullAnalysisDAG._dag_dir.fget
            dt.FullAnalysisDAG._dag_dir = property(lambda self: tmpdir)
            try:
                task = dt.FullAnalysisDAG(
                    name="run_test",
                    skip_skim=True,
                    skip_histfill=True,
                    skip_merge=True,
                    skip_plots=True,
                    skip_fits=True,
                )
                task.run()
                done_path = os.path.join(tmpdir, "dag.done")
                summary_path = os.path.join(tmpdir, "dag_summary.json")
                self.assertTrue(os.path.exists(done_path))
                self.assertTrue(os.path.exists(summary_path))
                with open(summary_path) as fh:
                    summary = json.load(fh)
                self.assertEqual(summary["name"], "run_test")
            finally:
                dt.FullAnalysisDAG._dag_dir = property(orig)

    def test_run_summary_stages_reflect_parameters(self):
        """dag_summary.json stages field accurately reflects which stages ran."""
        import dag_tasks as dt

        with tempfile.TemporaryDirectory() as tmpdir:
            orig = dt.FullAnalysisDAG._dag_dir.fget
            dt.FullAnalysisDAG._dag_dir = property(lambda self: tmpdir)
            try:
                task = dt.FullAnalysisDAG(
                    name="stages_test",
                    skip_skim=True,
                    skip_histfill=True,
                    skip_merge=True,
                    skip_plots=True,
                    skip_fits=True,
                    datacard_config="dc.yaml",
                )
                task.run()
                with open(os.path.join(tmpdir, "dag_summary.json")) as fh:
                    summary = json.load(fh)
                stages = summary["stages"]
                self.assertFalse(stages["skim"])
                self.assertFalse(stages["histfill"])
                self.assertFalse(stages["merge"])
                self.assertTrue(stages["datacards"])
                self.assertFalse(stages["plots"])
            finally:
                dt.FullAnalysisDAG._dag_dir = property(orig)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestManifestPlotTask(unittest.TestCase):
    """Tests for ManifestPlotTask in manifest_plot_tasks.py."""

    def _import(self):
        import manifest_plot_tasks
        return manifest_plot_tasks

    def test_manifest_plot_task_importable(self):
        mod = self._import()
        self.assertTrue(hasattr(mod, "ManifestPlotTask"))

    def test_task_namespace(self):
        mod = self._import()
        self.assertEqual(mod.ManifestPlotTask.task_namespace, "")

    def test_output_is_directory(self):
        mod = self._import()
        task = mod.ManifestPlotTask(
            name="ptest",
            manifest_path="dummy.yaml",
            plot_config="plots.json",
        )
        out = task.output()
        self.assertIsInstance(out, law.LocalDirectoryTarget)
        self.assertIn("manifestPlots_ptest", out.path)

    def test_load_manifest_raises_for_missing(self):
        mod = self._import()
        task = mod.ManifestPlotTask(
            name="missing",
            manifest_path="/nonexistent/manifest.yaml",
            plot_config="plots.json",
        )
        with self.assertRaises(RuntimeError) as ctx:
            task._load_manifest()
        self.assertIn("not found", str(ctx.exception).lower())

    def test_load_plot_configs_invalid_type_raises(self):
        mod = self._import()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"not": "a list"}, f)
            fname = f.name
        try:
            task = mod.ManifestPlotTask(
                name="badcfg",
                manifest_path="dummy.yaml",
                plot_config=fname,
            )
            with self.assertRaises(ValueError):
                task._load_plot_configs()
        finally:
            os.unlink(fname)

    def test_run_writes_provenance(self):
        """run() writes provenance.json."""
        mod = self._import()
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            mpath = _write_manifest(tmpdir)
            cfg_path = os.path.join(tmpdir, "plots.json")
            with open(cfg_path, "w") as fh:
                json.dump([{"outputFile": "pt.pdf", "processes": []}], fh)

            import manifest_plot_tasks as mpt
            orig = mpt.ManifestPlotTask._plots_dir.fget
            mpt.ManifestPlotTask._plots_dir = property(lambda self: tmpdir)
            try:
                task = mod.ManifestPlotTask(
                    name="prov_test",
                    manifest_path=mpath,
                    plot_config=cfg_path,
                )
                task.run()
                prov_path = os.path.join(tmpdir, "provenance.json")
                self.assertTrue(os.path.exists(prov_path))
                with open(prov_path) as fh:
                    prov = json.load(fh)
                self.assertEqual(prov["task"], "ManifestPlotTask")
                self.assertEqual(prov["manifest_framework_hash"], "fw_test")
            finally:
                mpt.ManifestPlotTask._plots_dir = property(orig)

    def test_run_per_region_stubs(self):
        """run() writes per-region stubs when rdfanalyzer is unavailable."""
        mod = self._import()
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create manifest with a region
            from output_schema import RegionDefinition
            region_data = {"name": "sr", "filter_column": "is_sr", "schema_version": 1}
            manifest_data = {
                "manifest_version": 1,
                "skim": None,
                "histograms": None,
                "metadata": None,
                "cutflow": None,
                "law_artifacts": [],
                "framework_hash": "fw_test",
                "user_repo_hash": None,
                "config_mtime": None,
                "dataset_manifest_provenance": None,
                "regions": [region_data],
                "nuisance_groups": [],
            }
            mpath = os.path.join(tmpdir, "output_manifest.yaml")
            with open(mpath, "w") as fh:
                yaml.dump(manifest_data, fh)

            cfg_path = os.path.join(tmpdir, "plots.json")
            with open(cfg_path, "w") as fh:
                json.dump([{"outputFile": "pt.pdf", "processes": []}], fh)

            import manifest_plot_tasks as mpt
            orig = mpt.ManifestPlotTask._plots_dir.fget
            mpt.ManifestPlotTask._plots_dir = property(lambda self: tmpdir)
            try:
                task = mod.ManifestPlotTask(
                    name="region_test",
                    manifest_path=mpath,
                    plot_config=cfg_path,
                )
                task.run()
                # Expect a stub file under sr/ subdirectory
                stub = os.path.join(tmpdir, "sr", "pt.pdf.stub.json")
                self.assertTrue(os.path.exists(stub))
            finally:
                mpt.ManifestPlotTask._plots_dir = property(orig)


if __name__ == "__main__":
    unittest.main(verbosity=2)
