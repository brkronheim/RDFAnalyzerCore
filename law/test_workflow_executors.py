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

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestRunNANOJobsTask(unittest.TestCase):
    """Tests for RunNANOJobs construction and configuration."""

    def _import(self):
        import unittest.mock as mock
        # Stub out Rucio if absent
        if "rucio" not in sys.modules:
            sys.modules["rucio"] = mock.MagicMock()
            sys.modules["rucio.client"] = mock.MagicMock()
        import law
        law.contrib.load("htcondor")
        import nano_tasks
        return nano_tasks

    def _make_task(self, **kwargs):
        mod = self._import()
        defaults = dict(
            submit_config="dummy.txt",
            name="test_run",
            x509="/tmp/x509",
            exe="/tmp/myexe",
        )
        defaults.update(kwargs)
        return mod.RunNANOJobs(**defaults)

    def test_task_importable(self):
        mod = self._import()
        self.assertTrue(hasattr(mod, "RunNANOJobs"))

    def test_task_namespace(self):
        mod = self._import()
        self.assertEqual(mod.RunNANOJobs.task_namespace, "")

    def test_workflow_bases_include_local(self):
        mod = self._import()
        import law
        self.assertTrue(
            issubclass(mod.RunNANOJobs, law.LocalWorkflow),
            "RunNANOJobs must inherit from law.LocalWorkflow",
        )

    def test_workflow_bases_include_htcondor(self):
        import workflow_executors
        mod = self._import()
        self.assertTrue(
            issubclass(mod.RunNANOJobs, workflow_executors.HTCondorWorkflow),
            "RunNANOJobs must inherit from HTCondorWorkflow",
        )

    def test_workflow_bases_include_dask(self):
        import workflow_executors
        mod = self._import()
        self.assertTrue(
            issubclass(mod.RunNANOJobs, workflow_executors.DaskWorkflow),
            "RunNANOJobs must inherit from DaskWorkflow",
        )

    def test_has_dask_scheduler_param(self):
        task = self._make_task()
        self.assertTrue(
            hasattr(task, "dask_scheduler"),
            "RunNANOJobs must expose a dask_scheduler parameter",
        )

    def test_output_path_structure(self):
        """output() returns a LocalFileTarget under condorSub_<name>/job_outputs/."""
        task = self._make_task(name="myRun", branch=3)
        import law
        out = task.output()
        self.assertIsInstance(out, law.LocalFileTarget)
        self.assertIn("condorSub_myRun", out.path)
        self.assertIn("job_outputs", out.path)
        self.assertIn("job_3.done", out.path)

    def test_get_dask_work_returns_callable(self):
        """get_dask_work returns (_run_analysis_job, args, kwargs)."""
        import workflow_executors
        with tempfile.TemporaryDirectory() as tmpdir:
            # Simulate a shared_inputs dir and a job dir
            shared_dir = os.path.join(tmpdir, "shared_inputs")
            os.makedirs(shared_dir)
            exe = os.path.join(shared_dir, "myexe")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)

            task = self._make_task(name="dask_test", branch=0, exe=exe)
            func, args, kwargs = _patch_task_dirs_and_call(
                task, tmpdir, shared_dir, job_dir,
                lambda t: t.get_dask_work(0, job_dir),
            )
            self.assertIs(func, workflow_executors._run_analysis_job)
            self.assertEqual(args[1], job_dir)

    def test_htcondor_output_directory_under_main_dir(self):
        """htcondor_output_directory() returns a path inside condorSub_<name>."""
        task = self._make_task(name="condorRun")
        import law
        out_dir = task.htcondor_output_directory()
        self.assertIsInstance(out_dir, law.LocalDirectoryTarget)
        self.assertIn("condorSub_condorRun", out_dir.path)

    def test_htcondor_job_config_sets_resources(self):
        """htcondor_job_config appends resource request entries."""
        task = self._make_task()
        # Create a minimal config mock
        config = MagicMock()
        config.custom_content = []
        result = task.htcondor_job_config(config, job_num=0, branches=[0])
        keys = [item[0] for item in result.custom_content]
        self.assertIn("+RequestMemory", keys)
        self.assertIn("+MaxRuntime", keys)
        self.assertIn("request_cpus", keys)


# ===========================================================================
# RunOpenDataJobs task
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestRunOpenDataJobsTask(unittest.TestCase):
    """Tests for RunOpenDataJobs construction and configuration."""

    def _import(self):
        import law
        law.contrib.load("htcondor")
        import opendata_tasks
        return opendata_tasks

    def _make_task(self, **kwargs):
        mod = self._import()
        defaults = dict(
            submit_config="dummy.txt",
            name="test_od_run",
            exe="/tmp/myexe",
        )
        defaults.update(kwargs)
        return mod.RunOpenDataJobs(**defaults)

    def test_task_importable(self):
        mod = self._import()
        self.assertTrue(hasattr(mod, "RunOpenDataJobs"))

    def test_task_namespace(self):
        mod = self._import()
        self.assertEqual(mod.RunOpenDataJobs.task_namespace, "")

    def test_workflow_bases_include_all_three(self):
        import law
        import workflow_executors
        mod = self._import()
        cls = mod.RunOpenDataJobs
        self.assertTrue(issubclass(cls, law.LocalWorkflow))
        self.assertTrue(issubclass(cls, workflow_executors.HTCondorWorkflow))
        self.assertTrue(issubclass(cls, workflow_executors.DaskWorkflow))

    def test_output_path_structure(self):
        task = self._make_task(name="odRun", branch=7)
        import law
        out = task.output()
        self.assertIsInstance(out, law.LocalFileTarget)
        self.assertIn("condorSub_odRun", out.path)
        self.assertIn("job_outputs", out.path)
        self.assertIn("job_7.done", out.path)

    def test_get_dask_work_returns_callable(self):
        import workflow_executors
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir = os.path.join(tmpdir, "shared_inputs")
            os.makedirs(shared_dir)
            exe = os.path.join(shared_dir, "myexe")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)

            task = self._make_task(name="dask_od_test", branch=0, exe=exe)
            func, args, kwargs = _patch_task_dirs_and_call(
                task, tmpdir, shared_dir, job_dir,
                lambda t: t.get_dask_work(0, job_dir),
            )
            self.assertIs(func, workflow_executors._run_analysis_job)
            self.assertEqual(args[1], job_dir)

    def test_htcondor_job_config_sets_resources(self):
        task = self._make_task()
        config = MagicMock()
        config.custom_content = []
        result = task.htcondor_job_config(config, job_num=0, branches=[0])
        keys = [item[0] for item in result.custom_content]
        self.assertIn("+RequestMemory", keys)
        self.assertIn("+MaxRuntime", keys)

    def test_dask_scheduler_param_exposed(self):
        task = self._make_task()
        self.assertTrue(hasattr(task, "dask_scheduler"))


# ===========================================================================
# Interchangeability smoke test
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestWorkflowInterchangeability(unittest.TestCase):
    """Verify that the three executor modes share the same task interface."""

    def _import(self):
        import unittest.mock as mock
        if "rucio" not in sys.modules:
            sys.modules["rucio"] = mock.MagicMock()
            sys.modules["rucio.client"] = mock.MagicMock()
        import law
        law.contrib.load("htcondor")
        import nano_tasks
        return nano_tasks

    def test_same_output_regardless_of_workflow(self):
        """
        RunNANOJobs produces the same output path irrespective of the selected
        workflow type.  This verifies the three executors are truly
        interchangeable from the output perspective.
        """
        mod = self._import()
        common_kwargs = dict(
            submit_config="dummy.txt",
            name="shared_run",
            x509="/tmp/x509",
            exe="/tmp/myexe",
            branch=2,
        )
        # Instantiate with different workflow values and compare output paths
        task_local   = mod.RunNANOJobs(workflow="local", **common_kwargs)
        task_condor  = mod.RunNANOJobs(workflow="htcondor", **common_kwargs)
        task_dask    = mod.RunNANOJobs(workflow="dask", **common_kwargs)

        path_local  = task_local.output().path
        path_condor = task_condor.output().path
        path_dask   = task_dask.output().path

        self.assertEqual(path_local, path_condor,
                         "local and htcondor outputs must be identical")
        self.assertEqual(path_local, path_dask,
                         "local and dask outputs must be identical")


if __name__ == "__main__":
    unittest.main(verbosity=2)
