#!/usr/bin/env python3
"""
Tests for the workflow executor module and the RunNANOJobs / RunOpenDataJobs
tasks introduced to support local, HTCondor, and Dask execution.

These tests do NOT require a running HTCondor cluster, Dask scheduler,
or CMS/ATLAS software environment.  They verify:

  * workflow_executors.py imports and class hierarchy
  * DaskWorkflow structure (workflow_type, proxy class, parameters)
  * _run_analysis_job helper (success + failure paths)
  * RunNANOJobs task construction, output path, branch map, and executor config
  * RunOpenDataJobs task construction, output path, and executor config
  * law.cfg contains the required executor sections
"""

from __future__ import annotations

import os
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Path setup – make law/ and core/python importable
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR   = os.path.join(_REPO_ROOT, "law")
_CORE_PY   = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optional dependency check
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


# ---------------------------------------------------------------------------
# Shared test helpers
# ---------------------------------------------------------------------------

def _patch_task_dirs_and_call(task, tmpdir, shared_dir, job_dir, fn):
    """
    Patch the four task directory properties used by get_dask_work() tests and
    invoke *fn(task)*, returning its result.

    Using this helper avoids repeating the four-level ``with patch.object(...)``
    nesting in each test method.
    """
    with patch.object(
        type(task), "_main_dir",
        new_callable=lambda: property(lambda s: tmpdir),
    ), patch.object(
        type(task), "_shared_dir",
        new_callable=lambda: property(lambda s: shared_dir),
    ), patch.object(
        type(task), "branch_data",
        new_callable=lambda: property(lambda s: job_dir),
    ), patch.object(
        type(task), "_root_setup_content",
        new_callable=lambda: property(lambda s: ""),
    ):
        return fn(task)


# ===========================================================================
# workflow_executors module
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestWorkflowExecutorsImport(unittest.TestCase):
    """Basic import and structure checks for workflow_executors."""

    def _import(self):
        import workflow_executors
        return workflow_executors

    def test_module_importable(self):
        mod = self._import()
        self.assertIsNotNone(mod)

    def test_run_analysis_job_callable(self):
        mod = self._import()
        self.assertTrue(callable(mod._run_analysis_job))

    def test_dask_workflow_class_exists(self):
        mod = self._import()
        self.assertTrue(hasattr(mod, "DaskWorkflow"))
        self.assertTrue(hasattr(mod, "DaskWorkflowProxy"))

    def test_htcondor_workflow_re_exported(self):
        """HTCondorWorkflow is re-exported for callers that import from this module."""
        mod = self._import()
        self.assertTrue(hasattr(mod, "HTCondorWorkflow"))

    def test_dask_workflow_type(self):
        mod = self._import()
        self.assertEqual(mod.DaskWorkflowProxy.workflow_type, "dask")

    def test_dask_workflow_proxy_cls(self):
        mod = self._import()
        self.assertIs(mod.DaskWorkflow.workflow_proxy_cls, mod.DaskWorkflowProxy)

    def test_dask_workflow_has_scheduler_param(self):
        mod = self._import()
        self.assertTrue(hasattr(mod.DaskWorkflow, "dask_scheduler"))
        self.assertTrue(hasattr(mod.DaskWorkflow, "dask_workers"))

    def test_get_dask_work_raises_not_implemented(self):
        """The base DaskWorkflow.get_dask_work() should raise NotImplementedError."""
        import workflow_executors

        # Create a minimal concrete subclass with all required methods
        class _MinimalTask(workflow_executors.DaskWorkflow):
            task_namespace = ""
            def create_branch_map(self):
                return {}
            def output(self):
                return None
            def run(self):
                pass

        task = _MinimalTask()
        with self.assertRaises(NotImplementedError):
            task.get_dask_work(0, None)


# ===========================================================================
# _run_analysis_job
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestRunAnalysisJob(unittest.TestCase):
    """Unit tests for the _run_analysis_job pure helper function."""

    def _fn(self):
        from workflow_executors import _run_analysis_job
        return _run_analysis_job

    def test_success(self):
        """A job that exits 0 returns 'done:<job_dir>'."""
        fn = self._fn()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a minimal submit_config.txt and a fake exe
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text("batch=true\n")
            exe = os.path.join(tmpdir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)

            result = fn(exe_path=exe, job_dir=tmpdir)
            self.assertIn("done:", result)
            self.assertIn(tmpdir, result)

    def test_failure_raises(self):
        """A job that exits non-zero raises RuntimeError."""
        fn = self._fn()
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text("batch=true\n")
            exe = os.path.join(tmpdir, "bad_exe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 42\n")
            os.chmod(exe, stat.S_IRWXU)

            with self.assertRaises(RuntimeError) as ctx:
                fn(exe_path=exe, job_dir=tmpdir)
            self.assertIn("42", str(ctx.exception))

    def test_root_setup_sourced(self):
        """When root_setup is provided, it is sourced before the exe."""
        fn = self._fn()
        with tempfile.TemporaryDirectory() as tmpdir:
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text("batch=true\n")
            # exe exits 0 only if MY_VAR is set (by root_setup)
            exe = os.path.join(tmpdir, "check_env.sh")
            Path(exe).write_text(
                "#!/bin/sh\n[ \"$MY_VAR\" = \"hello\" ] || exit 1\nexit 0\n"
            )
            os.chmod(exe, stat.S_IRWXU)

            root_setup = "export MY_VAR=hello"
            result = fn(exe_path=exe, job_dir=tmpdir, root_setup=root_setup)
            self.assertIn("done:", result)


# ===========================================================================
# law.cfg executor sections
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestLawCfgExecutorSections(unittest.TestCase):
    """Verify law.cfg contains the required executor configuration sections."""

    def _read_cfg(self):
        cfg_path = os.path.join(_LAW_DIR, "law.cfg")
        import configparser
        # law.cfg uses module entries without values (e.g. "nano_tasks" with no "=")
        # so allow_no_value=True is required.
        cfg = configparser.RawConfigParser(allow_no_value=True)
        cfg.read(cfg_path)
        return cfg

    def test_htcondor_workflow_section_exists(self):
        cfg = self._read_cfg()
        self.assertIn("htcondor_workflow", cfg.sections(),
                      "law.cfg must contain an [htcondor_workflow] section")

    def test_dask_workflow_section_exists(self):
        cfg = self._read_cfg()
        self.assertIn("dask_workflow", cfg.sections(),
                      "law.cfg must contain a [dask_workflow] section")

    def test_job_section_exists(self):
        cfg = self._read_cfg()
        self.assertIn("job", cfg.sections(),
                      "law.cfg must contain a [job] section for file directory config")

    def test_htcondor_workflow_has_poll_interval(self):
        cfg = self._read_cfg()
        self.assertTrue(cfg.has_option("htcondor_workflow", "poll_interval"))

    def test_dask_workflow_has_scheduler(self):
        cfg = self._read_cfg()
        self.assertTrue(cfg.has_option("dask_workflow", "scheduler"))


# ===========================================================================
# RunNANOJobs task
# ===========================================================================
