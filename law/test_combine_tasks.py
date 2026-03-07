#!/usr/bin/env python3
"""
Tests for the combine_tasks law module.

These tests verify task construction, parameter handling, output path
computation, and the helper utilities without requiring a running law
scheduler, an HTCondor cluster, or a CMS Combine binary.
"""

import importlib
import os
import sys
import tempfile
import shutil
import subprocess
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Make sure law/combine_tasks is importable
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optionally import law / luigi so we can skip when they are absent
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestFindCombine(unittest.TestCase):
    """Unit tests for the _find_combine helper."""

    def _import(self):
        import combine_tasks
        return combine_tasks

    def test_explicit_path_ok(self):
        """_find_combine returns the explicit path when it is executable."""
        mod = self._import()
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = tmp.name
        os.chmod(tmp_path, 0o755)
        try:
            result = mod._find_combine(tmp_path)
            self.assertEqual(result, tmp_path)
        finally:
            os.unlink(tmp_path)

    def test_explicit_path_missing_raises(self):
        """_find_combine raises RuntimeError for a non-existent path."""
        mod = self._import()
        with self.assertRaises(RuntimeError):
            mod._find_combine("/nonexistent/path/to/combine")

    def test_system_path_found(self):
        """_find_combine finds `combine` via system PATH when a mock is available."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_combine = os.path.join(tmpdir, "combine")
            Path(fake_combine).write_text("#!/bin/sh\n")
            os.chmod(fake_combine, 0o755)
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = tmpdir + ":" + old_path
            try:
                result = mod._find_combine("")
                self.assertEqual(result, fake_combine)
            finally:
                os.environ["PATH"] = old_path

    def test_not_found_raises(self):
        """_find_combine raises RuntimeError when combine cannot be located."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Use an empty directory so shutil.which finds nothing
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = tmpdir
            try:
                with self.assertRaises(RuntimeError) as ctx:
                    mod._find_combine("")
                self.assertIn("Combine binary not found", str(ctx.exception))
            finally:
                os.environ["PATH"] = old_path


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestCreateDatacardTask(unittest.TestCase):
    """Tests for the CreateDatacard task."""

    def _make_task(self, **kwargs):
        import combine_tasks
        defaults = dict(datacard_config="dummy.yaml", name="test_run")
        defaults.update(kwargs)
        return combine_tasks.CreateDatacard(**defaults)

    def test_output_path(self):
        """CreateDatacard.output() returns a directory target under combineRun_<name>."""
        task = self._make_task(name="myRun")
        out = task.output()
        import law
        self.assertIsInstance(out, law.LocalDirectoryTarget)
        self.assertIn("combineRun_myRun", out.path)
        self.assertTrue(out.path.endswith("datacards"))

    def test_run_dir_property(self):
        """_run_dir is workspace/combineRun_<name>."""
        import combine_tasks
        task = self._make_task(name="run42")
        expected = os.path.join(combine_tasks.WORKSPACE, "combineRun_run42")
        self.assertEqual(task._run_dir, expected)

    def test_missing_config_raises(self):
        """run() raises RuntimeError when the config file does not exist."""
        task = self._make_task(datacard_config="/nonexistent/config.yaml")
        with self.assertRaises(RuntimeError) as ctx:
            task.run()
        self.assertIn("not found", str(ctx.exception).lower())

    def test_run_calls_generator(self):
        """run() calls DatacardGenerator.run() with the output_dir overridden."""
        import combine_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "dc_config.yaml")
            Path(config_path).write_text("output_dir: original_dir\n")

            # Create a fake datacard so the post-run check passes
            task = self._make_task(datacard_config=config_path, name="trun")
            Path(task._datacard_dir).mkdir(parents=True, exist_ok=True)
            fake_card = os.path.join(task._datacard_dir, "datacard_sr.txt")
            Path(fake_card).write_text("# fake datacard\n")

            mock_generator = MagicMock()
            MockClass = MagicMock(return_value=mock_generator)

            with patch.dict(sys.modules, {"create_datacards": MagicMock(DatacardGenerator=MockClass)}):
                # Re-import to pick up mock
                mod = importlib.reload(combine_tasks)
                task2 = mod.CreateDatacard(datacard_config=config_path, name="trun")

                task2.run()
                MockClass.assert_called_once_with(config_path)
                mock_generator.run.assert_called_once()

            shutil.rmtree(task._run_dir, ignore_errors=True)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestRunCombineTask(unittest.TestCase):
    """Tests for the RunCombine task."""

    def _make_task(self, **kwargs):
        import combine_tasks
        defaults = dict(
            datacard_config="dummy.yaml",
            name="test_run",
            method="AsymptoticLimits",
            datacard_path="",
            combine_exe="",
            combine_options="",
        )
        defaults.update(kwargs)
        return combine_tasks.RunCombine(**defaults)

    def test_output_path(self):
        """RunCombine.output() returns a directory target under combineRun_<name>."""
        task = self._make_task(name="myRun2")
        out = task.output()
        import law
        self.assertIsInstance(out, law.LocalDirectoryTarget)
        self.assertIn("combineRun_myRun2", out.path)
        self.assertTrue(out.path.endswith("combine_results"))

    def test_requires_create_datacard_when_no_path(self):
        """requires() returns a CreateDatacard instance when no explicit datacard path."""
        import combine_tasks
        task = self._make_task(datacard_path="")
        req = task.requires()
        self.assertIsInstance(req, combine_tasks.CreateDatacard)

    def test_requires_none_when_path_given(self):
        """requires() returns None when an explicit datacard path is provided."""
        task = self._make_task(datacard_path="/some/datacard.txt")
        req = task.requires()
        self.assertIsNone(req)

    def test_collect_datacards_explicit(self):
        """_collect_datacards returns the explicit path when --datacard-path is set."""
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False) as tmp:
            tmp_path = tmp.name
        try:
            task = self._make_task(datacard_path=tmp_path)
            cards = task._collect_datacards()
            self.assertEqual(cards, [tmp_path])
        finally:
            os.unlink(tmp_path)

    def test_collect_datacards_explicit_missing_raises(self):
        """_collect_datacards raises RuntimeError for a missing explicit path."""
        task = self._make_task(datacard_path="/nonexistent/datacard.txt")
        with self.assertRaises(RuntimeError):
            task._collect_datacards()

    def test_collect_datacards_from_dir(self):
        """_collect_datacards globs datacard_*.txt from the CreateDatacard output dir."""
        import combine_tasks
        task = self._make_task(datacard_path="", name="glob_test")
        with tempfile.TemporaryDirectory() as tmpdir:
            # Override _datacard_dir via monkeypatching
            orig = combine_tasks.CombineMixin._datacard_dir.fget
            combine_tasks.CombineMixin._datacard_dir = property(lambda self: tmpdir)
            try:
                Path(os.path.join(tmpdir, "datacard_sr.txt")).write_text("# card\n")
                Path(os.path.join(tmpdir, "datacard_cr.txt")).write_text("# card\n")
                task2 = combine_tasks.RunCombine(
                    datacard_config="dummy.yaml", name="glob_test",
                    method="AsymptoticLimits", datacard_path="",
                    combine_exe="", combine_options="",
                )
                cards = task2._collect_datacards()
                self.assertEqual(len(cards), 2)
                self.assertTrue(all(c.endswith(".txt") for c in cards))
            finally:
                combine_tasks.CombineMixin._datacard_dir = property(orig)

    def test_collect_datacards_empty_dir_raises(self):
        """_collect_datacards raises RuntimeError when no datacards are found."""
        import combine_tasks
        task = self._make_task(datacard_path="", name="empty_test")
        with tempfile.TemporaryDirectory() as tmpdir:
            orig = combine_tasks.CombineMixin._datacard_dir.fget
            combine_tasks.CombineMixin._datacard_dir = property(lambda self: tmpdir)
            try:
                task2 = combine_tasks.RunCombine(
                    datacard_config="dummy.yaml", name="empty_test",
                    method="AsymptoticLimits", datacard_path="",
                    combine_exe="", combine_options="",
                )
                with self.assertRaises(RuntimeError):
                    task2._collect_datacards()
            finally:
                combine_tasks.CombineMixin._datacard_dir = property(orig)

    def test_run_combine_invoked(self):
        """run() calls the combine binary for each discovered datacard."""
        import combine_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake combine binary that always exits 0
            fake_bin = os.path.join(tmpdir, "combine")
            Path(fake_bin).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(fake_bin, 0o755)

            # Create a fake datacard
            dc_dir = os.path.join(tmpdir, "datacards")
            Path(dc_dir).mkdir()
            Path(os.path.join(dc_dir, "datacard_sr.txt")).write_text("# card\n")

            task = combine_tasks.RunCombine(
                datacard_config="dummy.yaml",
                name="run_test",
                method="AsymptoticLimits",
                datacard_path=os.path.join(dc_dir, "datacard_sr.txt"),
                combine_exe=fake_bin,
                combine_options="",
            )
            results_dir = os.path.join(tmpdir, "results")
            Path(results_dir).mkdir()

            # Override _results_dir
            orig_results = combine_tasks.CombineMixin._results_dir.fget
            combine_tasks.CombineMixin._results_dir = property(lambda self: results_dir)
            try:
                task2 = combine_tasks.RunCombine(
                    datacard_config="dummy.yaml",
                    name="run_test",
                    method="AsymptoticLimits",
                    datacard_path=os.path.join(dc_dir, "datacard_sr.txt"),
                    combine_exe=fake_bin,
                    combine_options="",
                )
                task2.run()
                # summary.txt must be written
                self.assertTrue(os.path.exists(os.path.join(results_dir, "summary.txt")))
            finally:
                combine_tasks.CombineMixin._results_dir = property(orig_results)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestCombineTasksModule(unittest.TestCase):
    """Smoke tests for the combine_tasks module itself."""

    def test_importable(self):
        """combine_tasks can be imported without errors."""
        import combine_tasks  # noqa: F401

    def test_tasks_defined(self):
        """CreateDatacard and RunCombine are defined in the module."""
        import combine_tasks
        self.assertTrue(hasattr(combine_tasks, "CreateDatacard"))
        self.assertTrue(hasattr(combine_tasks, "RunCombine"))

    def test_task_namespaces(self):
        """Tasks use an empty task_namespace for easy law run invocation."""
        import combine_tasks
        self.assertEqual(combine_tasks.CreateDatacard.task_namespace, "")
        self.assertEqual(combine_tasks.RunCombine.task_namespace, "")

    def test_method_parameter_default(self):
        """RunCombine.method defaults to AsymptoticLimits."""
        import combine_tasks
        task = combine_tasks.RunCombine(
            datacard_config="dummy.yaml",
            name="default_method_test",
        )
        self.assertEqual(task.method, "AsymptoticLimits")


if __name__ == "__main__":
    unittest.main(verbosity=2)
