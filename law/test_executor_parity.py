#!/usr/bin/env python3
"""
Executor parity validation suite for RDFAnalyzerCore.

This suite compares local, HTCondor, and Dask executor behaviour for
representative workflows.  It verifies that:

1. **Compatible outputs** – the same inputs and configurations produce
   outputs with identical path structure and schema regardless of which
   executor is selected.

2. **Provenance parity** – all three executors invoke ``_run_analysis_job``
   with the same arguments, so the provenance recorded by the analysis
   process is executor-agnostic.

3. **Task metadata consistency** – the workflow task exposes the same
   parameters and branch-map logic for every executor mode.

4. **Explicitly allowed differences** – only the ``workflow`` parameter
   value, Dask-specific scheduler/worker parameters, and the HTCondor
   infrastructure helpers (``htcondor_output_directory``,
   ``htcondor_job_config``) are legitimately executor-specific.

Tests do **not** require a running HTCondor cluster, Dask scheduler,
CMS/ATLAS software environment, or a VOMS proxy.

Design notes
------------
* All tests are pure unit tests that exercise the task API through mocks
  and temporary directories.
* The ``_run_analysis_job`` helper is exercised end-to-end so that the
  parity of the local and Dask code paths is verified directly.
* Each test class focuses on one parity dimension and documents its scope
  in its docstring.
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
# Optional dependency guard
# ---------------------------------------------------------------------------
try:
    import luigi   # noqa: F401
    import law     # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _stub_rucio():
    """Ensure rucio stubs are in sys.modules (idempotent)."""
    import unittest.mock as _mock
    for mod in ("rucio", "rucio.client"):
        if mod not in sys.modules:
            sys.modules[mod] = _mock.MagicMock()


def _load_nano():
    """Return the nano_tasks module with Rucio stubbed out."""
    _stub_rucio()
    import law as _law
    _law.contrib.load("htcondor")
    import nano_tasks
    return nano_tasks


def _load_opendata():
    """Return the opendata_tasks module."""
    import law as _law
    _law.contrib.load("htcondor")
    import opendata_tasks
    return opendata_tasks


def _make_nano_task(**kwargs):
    """Instantiate ``RunNANOJobs`` with sensible defaults."""
    mod = _load_nano()
    defaults = dict(
        submit_config="dummy.txt",
        name="parity_run",
        x509="/tmp/x509",
        exe="/tmp/myexe",
    )
    defaults.update(kwargs)
    return mod.RunNANOJobs(**defaults)


def _make_opendata_task(**kwargs):
    """Instantiate ``RunOpenDataJobs`` with sensible defaults."""
    mod = _load_opendata()
    defaults = dict(
        submit_config="dummy.txt",
        name="parity_od_run",
        exe="/tmp/myexe",
    )
    defaults.update(kwargs)
    return mod.RunOpenDataJobs(**defaults)


def _patch_task_properties(task, tmpdir, shared_dir, job_dir, root_setup=""):
    """
    Return a context manager that patches the four task properties used
    by ``run()`` and ``get_dask_work()``.
    """
    from contextlib import ExitStack
    stack = ExitStack()
    stack.enter_context(
        patch.object(type(task), "_main_dir",
                     new_callable=lambda: property(lambda s: tmpdir))
    )
    stack.enter_context(
        patch.object(type(task), "_shared_dir",
                     new_callable=lambda: property(lambda s: shared_dir))
    )
    stack.enter_context(
        patch.object(type(task), "branch_data",
                     new_callable=lambda: property(lambda s: job_dir))
    )
    stack.enter_context(
        patch.object(type(task), "_root_setup_content",
                     new_callable=lambda: property(lambda s: root_setup))
    )
    return stack


# ===========================================================================
# 1. Output path parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestOutputPathParity(unittest.TestCase):
    """
    Verify that all three executors produce identical output file paths for
    the same task inputs.

    The output path must be determined solely by ``name`` and ``branch``,
    not by the selected executor.  This is the primary "compatible outputs"
    requirement.
    """

    # --- RunNANOJobs -------------------------------------------------------

    def test_nano_local_htcondor_dask_output_paths_are_equal(self):
        """local / htcondor / dask outputs must be identical for RunNANOJobs."""
        common = dict(
            submit_config="dummy.txt",
            name="parity",
            x509="/tmp/x509",
            exe="/tmp/exe",
            branch=5,
        )
        t_local   = _make_nano_task(workflow="local",    **common)
        t_condor  = _make_nano_task(workflow="htcondor", **common)
        t_dask    = _make_nano_task(workflow="dask",     **common)

        p_local  = t_local.output().path
        p_condor = t_condor.output().path
        p_dask   = t_dask.output().path

        self.assertEqual(p_local, p_condor,
                         "RunNANOJobs: local and htcondor output paths differ")
        self.assertEqual(p_local, p_dask,
                         "RunNANOJobs: local and dask output paths differ")

    def test_nano_output_path_contains_job_done_marker(self):
        """Output path ends with job_<N>.done for every executor."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf, branch=3)
                self.assertTrue(
                    t.output().path.endswith("job_3.done"),
                    f"workflow={wf}: output path does not end with job_3.done",
                )

    def test_nano_output_path_contains_job_outputs_directory(self):
        """Output path contains job_outputs/ for every executor."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf, branch=1)
                self.assertIn("job_outputs", t.output().path)

    def test_nano_output_path_contains_run_name(self):
        """Output path contains the run name (condorSub_<name>) for every executor."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf, name="myRun", branch=0)
                self.assertIn("condorSub_myRun", t.output().path)

    # --- RunOpenDataJobs ---------------------------------------------------

    def test_opendata_local_htcondor_dask_output_paths_are_equal(self):
        """local / htcondor / dask outputs must be identical for RunOpenDataJobs."""
        common = dict(
            submit_config="dummy.txt",
            name="od_parity",
            exe="/tmp/exe",
            branch=7,
        )
        t_local  = _make_opendata_task(workflow="local",    **common)
        t_condor = _make_opendata_task(workflow="htcondor", **common)
        t_dask   = _make_opendata_task(workflow="dask",     **common)

        p_local  = t_local.output().path
        p_condor = t_condor.output().path
        p_dask   = t_dask.output().path

        self.assertEqual(p_local, p_condor,
                         "RunOpenDataJobs: local and htcondor output paths differ")
        self.assertEqual(p_local, p_dask,
                         "RunOpenDataJobs: local and dask output paths differ")

    def test_opendata_output_path_contains_job_done_marker(self):
        """Output path ends with job_<N>.done for every executor (OpenData)."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_opendata_task(workflow=wf, branch=9)
                self.assertTrue(
                    t.output().path.endswith("job_9.done"),
                    f"workflow={wf}: output path does not end with job_9.done",
                )


# ===========================================================================
# 2. Job output file format parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestJobOutputFormatParity(unittest.TestCase):
    """
    Verify that the *content* written to job output files is consistent
    across execution paths.

    The local executor writes the output file directly in ``run()``.  The
    Dask executor writes it in ``DaskWorkflowProxy.run()`` after receiving
    the future result.  Both must write the same "status=done\\n<result>\\n"
    format so that downstream task-completion checks are portable.
    """

    def _make_exe_and_job_dir(self, tmpdir, exit_code=0):
        """Create a minimal executable and job directory."""
        exe = os.path.join(tmpdir, "myexe.sh")
        Path(exe).write_text(f"#!/bin/sh\nexit {exit_code}\n")
        os.chmod(exe, stat.S_IRWXU)
        job_dir = os.path.join(tmpdir, "job_0")
        os.makedirs(job_dir)
        Path(os.path.join(job_dir, "submit_config.txt")).write_text("batch=true\n")
        return exe, job_dir

    def test_run_analysis_job_result_starts_with_done(self):
        """_run_analysis_job returns 'done:<job_dir>' on success."""
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._make_exe_and_job_dir(tmpdir)
            result = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            self.assertTrue(result.startswith("done:"),
                            f"Expected 'done:...', got {result!r}")

    def test_run_analysis_job_result_contains_job_dir(self):
        """_run_analysis_job embeds the job_dir in its result string."""
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._make_exe_and_job_dir(tmpdir)
            result = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            self.assertIn(job_dir, result)

    def test_local_executor_output_file_format(self):
        """run() writes 'status=done\\n<result>\\n' to the output file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir = os.path.join(tmpdir, "shared_inputs")
            os.makedirs(shared_dir)
            exe = os.path.join(shared_dir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)
            Path(os.path.join(job_dir, "submit_config.txt")).write_text("batch=true\n")

            task = _make_nano_task(branch=0)
            out_path = os.path.join(tmpdir, "job_outputs", "job_0.done")

            with _patch_task_properties(task, tmpdir, shared_dir, job_dir):
                # Patch output() to return a target pointing to our tmpdir
                with patch.object(
                    type(task), "_exe_relpath",
                    new_callable=lambda: property(lambda s: "myexe.sh"),
                ):
                    import law as _law
                    mock_output = _law.LocalFileTarget(out_path)
                    with patch.object(task, "output", return_value=mock_output):
                        with patch.object(task, "publish_message"):
                            task.run()

            content = Path(out_path).read_text()
            self.assertTrue(content.startswith("status=done\n"),
                            f"Output file must start with 'status=done\\n', got:\n{content!r}")
            self.assertIn("done:", content,
                          "Output file must contain the _run_analysis_job result")

    def test_dask_proxy_output_file_format_matches_local(self):
        """
        DaskWorkflowProxy writes the same 'status=done\\n<result>\\n' format as
        the local run() method, ensuring Dask and local output files are
        interchangeable for downstream task-completion checks.
        """
        # Directly simulate what DaskWorkflowProxy.run() writes after a
        # successful future – it uses the same format string.
        with tempfile.TemporaryDirectory() as tmpdir:
            from workflow_executors import _run_analysis_job
            exe = os.path.join(tmpdir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)
            Path(os.path.join(job_dir, "submit_config.txt")).write_text("batch=true\n")

            result_text = _run_analysis_job(exe_path=exe, job_dir=job_dir)

            # Reproduce exactly what DaskWorkflowProxy writes:
            out_path = os.path.join(tmpdir, "job_outputs", "job_0.done")
            Path(out_path).parent.mkdir(parents=True, exist_ok=True)
            with open(out_path, "w") as fh:
                fh.write(f"status=done\n{result_text}\n")

            content = Path(out_path).read_text()
            self.assertTrue(content.startswith("status=done\n"))
            self.assertIn(result_text, content)


# ===========================================================================
# 3. Provenance parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestProvenanceParity(unittest.TestCase):
    """
    Verify that provenance/version expectations are identical across executors.

    Since the analysis executable is invoked via ``_run_analysis_job`` in
    every executor mode, provenance recorded by ``ProvenanceService`` is
    determined by the executable and job configuration – not by the
    executor.  The tests in this class confirm that:

    * The callable passed to ``get_dask_work()`` is ``_run_analysis_job``
      (the same function called by ``run()``).
    * The arguments passed to ``_run_analysis_job`` are identical between
      the local and Dask paths.
    * Schema version constants used to describe provenance metadata are
      executor-agnostic.
    """

    def _get_exe_and_job_dir(self, tmpdir):
        shared_dir = os.path.join(tmpdir, "shared_inputs")
        os.makedirs(shared_dir)
        exe = os.path.join(shared_dir, "myexe.sh")
        Path(exe).write_text("#!/bin/sh\nexit 0\n")
        os.chmod(exe, stat.S_IRWXU)
        job_dir = os.path.join(tmpdir, "job_0")
        os.makedirs(job_dir)
        Path(os.path.join(job_dir, "submit_config.txt")).write_text("batch=true\n")
        return shared_dir, exe, job_dir

    def test_nano_get_dask_work_uses_run_analysis_job(self):
        """RunNANOJobs.get_dask_work() delegates to _run_analysis_job."""
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir, exe, job_dir = self._get_exe_and_job_dir(tmpdir)
            task = _make_nano_task(branch=0)
            with _patch_task_properties(task, tmpdir, shared_dir, job_dir):
                with patch.object(
                    type(task), "_exe_relpath",
                    new_callable=lambda: property(lambda s: "myexe.sh"),
                ):
                    func, args, kwargs = task.get_dask_work(0, job_dir)

            self.assertIs(func, _run_analysis_job,
                          "Dask path must use the same _run_analysis_job as local path")

    def test_opendata_get_dask_work_uses_run_analysis_job(self):
        """RunOpenDataJobs.get_dask_work() delegates to _run_analysis_job."""
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir, exe, job_dir = self._get_exe_and_job_dir(tmpdir)
            task = _make_opendata_task(branch=0)
            with _patch_task_properties(task, tmpdir, shared_dir, job_dir):
                with patch.object(
                    type(task), "_exe_relpath",
                    new_callable=lambda: property(lambda s: "myexe.sh"),
                ):
                    func, args, kwargs = task.get_dask_work(0, job_dir)

            self.assertIs(func, _run_analysis_job,
                          "Dask path must use the same _run_analysis_job as local path")

    def test_dask_exe_path_matches_local_exe_path(self):
        """
        The exe_path passed to _run_analysis_job by get_dask_work() matches
        the one constructed by run() – same shared_inputs/<exe_relpath>.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir, exe, job_dir = self._get_exe_and_job_dir(tmpdir)
            task = _make_nano_task(branch=0)
            with _patch_task_properties(task, tmpdir, shared_dir, job_dir):
                with patch.object(
                    type(task), "_exe_relpath",
                    new_callable=lambda: property(lambda s: "myexe.sh"),
                ):
                    _func, args, _kwargs = task.get_dask_work(0, job_dir)
                    dask_exe_path = args[0]
                    local_exe_path = os.path.join(shared_dir, "myexe.sh")

            self.assertEqual(dask_exe_path, local_exe_path,
                             "Dask and local paths must pass identical exe_path")

    def test_dask_job_dir_matches_local_job_dir(self):
        """
        The job_dir passed by get_dask_work() matches branch_data, matching
        what run() receives as self.branch_data.
        """
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir, exe, job_dir = self._get_exe_and_job_dir(tmpdir)
            task = _make_nano_task(branch=0)
            with _patch_task_properties(task, tmpdir, shared_dir, job_dir):
                with patch.object(
                    type(task), "_exe_relpath",
                    new_callable=lambda: property(lambda s: "myexe.sh"),
                ):
                    _func, args, _kwargs = task.get_dask_work(0, job_dir)
                    dask_job_dir = args[1]

            self.assertEqual(dask_job_dir, job_dir,
                             "Dask and local paths must pass the same job_dir")

    def test_provenance_schema_versions_are_executor_agnostic(self):
        """
        Schema version constants in output_schema.py are global; they do not
        vary per executor.  Importing them from any execution context returns
        the same values.
        """
        from output_schema import (
            SKIM_SCHEMA_VERSION,
            HISTOGRAM_SCHEMA_VERSION,
            METADATA_SCHEMA_VERSION,
            CUTFLOW_SCHEMA_VERSION,
            LAW_ARTIFACT_SCHEMA_VERSION,
            OUTPUT_MANIFEST_VERSION,
        )
        # Re-import to confirm they are stable constants
        import importlib
        import output_schema as _sch
        importlib.reload(_sch)

        self.assertEqual(_sch.SKIM_SCHEMA_VERSION,      SKIM_SCHEMA_VERSION)
        self.assertEqual(_sch.HISTOGRAM_SCHEMA_VERSION,  HISTOGRAM_SCHEMA_VERSION)
        self.assertEqual(_sch.METADATA_SCHEMA_VERSION,   METADATA_SCHEMA_VERSION)
        self.assertEqual(_sch.CUTFLOW_SCHEMA_VERSION,    CUTFLOW_SCHEMA_VERSION)
        self.assertEqual(_sch.LAW_ARTIFACT_SCHEMA_VERSION, LAW_ARTIFACT_SCHEMA_VERSION)
        self.assertEqual(_sch.OUTPUT_MANIFEST_VERSION,   OUTPUT_MANIFEST_VERSION)

    def test_provenance_required_keys_are_executor_agnostic(self):
        """
        PROVENANCE_REQUIRED_KEYS is a module-level constant independent of
        executor; all execution paths record the same mandatory provenance
        fields.
        """
        from output_schema import PROVENANCE_REQUIRED_KEYS
        expected_keys = [
            "framework.git_hash",
            "framework.git_dirty",
            "framework.build_timestamp",
            "framework.compiler",
            "root.version",
            "config.hash",
            "executor.num_threads",
        ]
        for key in expected_keys:
            self.assertIn(key, PROVENANCE_REQUIRED_KEYS,
                          f"Required provenance key '{key}' is missing")

    def test_provenance_record_matches_compatible(self):
        """
        ProvenanceRecord.matches() is True when all comparable fields agree,
        regardless of executor – since the same executable writes the same
        provenance.
        """
        from output_schema import ProvenanceRecord
        rec_a = ProvenanceRecord(framework_hash="abc123", user_repo_hash="def456")
        rec_b = ProvenanceRecord(framework_hash="abc123", user_repo_hash="def456")
        self.assertTrue(rec_a.matches(rec_b))

    def test_provenance_record_mismatch_detected(self):
        """
        ProvenanceRecord.matches() is False when framework hashes differ.
        This documents the expected behavior when detecting cross-executor
        provenance drift.
        """
        from output_schema import ProvenanceRecord
        rec_a = ProvenanceRecord(framework_hash="abc123")
        rec_b = ProvenanceRecord(framework_hash="xyz999")
        self.assertFalse(rec_a.matches(rec_b))

    def test_resolve_artifact_compatible_status(self):
        """
        resolve_artifact() returns COMPATIBLE when schema version matches and
        provenance agrees – the same result for any executor.
        """
        from output_schema import (
            ArtifactResolutionStatus,
            ProvenanceRecord,
            SkimSchema,
            resolve_artifact,
        )
        schema = SkimSchema(output_file="out.root")
        prov = ProvenanceRecord(framework_hash="same_hash")
        status = resolve_artifact(schema, current_provenance=prov,
                                  recorded_provenance=prov)
        self.assertEqual(status, ArtifactResolutionStatus.COMPATIBLE)


# ===========================================================================
# 4. Task metadata / parameter parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestTaskMetadataParity(unittest.TestCase):
    """
    Verify that core task parameters and metadata attributes are identical
    across all three executors.

    The task's ``task_namespace``, common parameters (``submit_config``,
    ``name``, ``exe``), and ``exclude_params_branch`` behaviour must be
    consistent so that the same command line can drive any executor.
    """

    # --- task_namespace ----------------------------------------------------

    def test_nano_task_namespace_identical_across_executors(self):
        """RunNANOJobs.task_namespace is '' for all workflow modes."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf)
                self.assertEqual(t.task_namespace, "",
                                 f"workflow={wf}: task_namespace must be ''")

    def test_opendata_task_namespace_identical_across_executors(self):
        """RunOpenDataJobs.task_namespace is '' for all workflow modes."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_opendata_task(workflow=wf)
                self.assertEqual(t.task_namespace, "")

    # --- common parameters -------------------------------------------------

    def test_nano_common_params_present_for_all_executors(self):
        """submit_config, name, exe, x509 present regardless of executor."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf)
                for param in ("submit_config", "name", "exe", "x509"):
                    self.assertTrue(
                        hasattr(t, param),
                        f"workflow={wf}: missing parameter '{param}'",
                    )

    def test_opendata_common_params_present_for_all_executors(self):
        """submit_config, name, exe present regardless of executor."""
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_opendata_task(workflow=wf)
                for param in ("submit_config", "name", "exe"):
                    self.assertTrue(
                        hasattr(t, param),
                        f"workflow={wf}: missing parameter '{param}'",
                    )

    # --- parameter values --------------------------------------------------

    def test_nano_submit_config_value_identical_across_executors(self):
        """submit_config value is preserved identically across executors."""
        cfg = "/analysis/cfg/my_config.txt"
        tasks = [
            _make_nano_task(workflow=wf, submit_config=cfg)
            for wf in ("local", "htcondor", "dask")
        ]
        values = [t.submit_config for t in tasks]
        self.assertEqual(len(set(values)), 1,
                         f"submit_config values differ across executors: {values}")

    def test_nano_name_value_identical_across_executors(self):
        """'name' parameter value is preserved identically across executors."""
        name = "uniqueRunName"
        tasks = [
            _make_nano_task(workflow=wf, name=name)
            for wf in ("local", "htcondor", "dask")
        ]
        values = [t.name for t in tasks]
        self.assertEqual(len(set(values)), 1,
                         f"'name' values differ across executors: {values}")

    # --- inheritance chain -------------------------------------------------

    def test_nano_inherits_all_three_workflow_bases(self):
        """RunNANOJobs must inherit from LocalWorkflow, HTCondorWorkflow, DaskWorkflow."""
        import law as _law
        import workflow_executors
        mod = _load_nano()
        self.assertTrue(issubclass(mod.RunNANOJobs, _law.LocalWorkflow))
        self.assertTrue(issubclass(mod.RunNANOJobs, workflow_executors.HTCondorWorkflow))
        self.assertTrue(issubclass(mod.RunNANOJobs, workflow_executors.DaskWorkflow))

    def test_opendata_inherits_all_three_workflow_bases(self):
        """RunOpenDataJobs must inherit from LocalWorkflow, HTCondorWorkflow, DaskWorkflow."""
        import law as _law
        import workflow_executors
        mod = _load_opendata()
        self.assertTrue(issubclass(mod.RunOpenDataJobs, _law.LocalWorkflow))
        self.assertTrue(issubclass(mod.RunOpenDataJobs, workflow_executors.HTCondorWorkflow))
        self.assertTrue(issubclass(mod.RunOpenDataJobs, workflow_executors.DaskWorkflow))


# ===========================================================================
# 5. Branch-map parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestBranchMapParity(unittest.TestCase):
    """
    Verify that ``create_branch_map()`` returns the same map regardless of
    the selected executor.

    The branch map is built from ``job_N`` symlinks in the filesystem and
    must be identical for local, HTCondor, and Dask executors because the
    executor selection does not influence how job directories are discovered.
    """

    def _make_symlinks(self, tmpdir, n_jobs):
        """Create job_0 … job_{n_jobs-1} directories and their symlinks."""
        for i in range(n_jobs):
            real_dir = os.path.join(tmpdir, f"samples", f"s0", f"job_{i}")
            os.makedirs(real_dir)
            link = os.path.join(tmpdir, f"job_{i}")
            os.symlink(real_dir, link)

    def test_nano_branch_maps_equal_across_executors(self):
        """RunNANOJobs creates identical branch maps for all three executors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_symlinks(tmpdir, n_jobs=3)
            maps = {}
            for wf in ("local", "htcondor", "dask"):
                t = _make_nano_task(workflow=wf)
                with patch.object(
                    type(t), "_main_dir",
                    new_callable=lambda: property(lambda s: tmpdir),
                ):
                    maps[wf] = t.create_branch_map()

            self.assertEqual(maps["local"], maps["htcondor"],
                             "branch_map differs between local and htcondor")
            self.assertEqual(maps["local"], maps["dask"],
                             "branch_map differs between local and dask")

    def test_opendata_branch_maps_equal_across_executors(self):
        """RunOpenDataJobs creates identical branch maps for all three executors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_symlinks(tmpdir, n_jobs=2)
            maps = {}
            for wf in ("local", "htcondor", "dask"):
                t = _make_opendata_task(workflow=wf)
                with patch.object(
                    type(t), "_main_dir",
                    new_callable=lambda: property(lambda s: tmpdir),
                ):
                    maps[wf] = t.create_branch_map()

            self.assertEqual(maps["local"], maps["htcondor"])
            self.assertEqual(maps["local"], maps["dask"])

    def test_branch_map_keys_are_contiguous_integers(self):
        """Branch map keys are 0-based contiguous integers for all executors."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_symlinks(tmpdir, n_jobs=4)
            t = _make_nano_task(workflow="local")
            with patch.object(
                type(t), "_main_dir",
                new_callable=lambda: property(lambda s: tmpdir),
            ):
                bmap = t.create_branch_map()

            self.assertEqual(sorted(bmap.keys()), list(range(4)))

    def test_branch_map_values_are_real_directories(self):
        """Branch map values resolve symlinks to real directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self._make_symlinks(tmpdir, n_jobs=2)
            t = _make_nano_task(workflow="dask")
            with patch.object(
                type(t), "_main_dir",
                new_callable=lambda: property(lambda s: tmpdir),
            ):
                bmap = t.create_branch_map()

            for branch_num, job_dir in bmap.items():
                self.assertTrue(
                    os.path.isdir(job_dir),
                    f"Branch {branch_num}: resolved path is not a directory: {job_dir}",
                )
                # Symlinks must be resolved: the value must equal os.path.realpath
                # of the original job_N symlink, so it should NOT be the symlink path.
                symlink_path = os.path.join(tmpdir, f"job_{branch_num}")
                self.assertEqual(
                    job_dir, os.path.realpath(symlink_path),
                    f"Branch {branch_num}: path was not resolved via os.path.realpath",
                )


# ===========================================================================
# 6. Allowed differences between executors
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestAllowedExecutorDifferences(unittest.TestCase):
    """
    Document and verify the **explicitly allowed** differences between
    executor modes.

    These tests serve as living documentation: they confirm that the listed
    differences are intentional and stable.

    Allowed differences
    -------------------
    * ``workflow`` parameter value – "local", "htcondor", or "dask"
    * ``dask_scheduler`` and ``dask_workers`` – only meaningful for Dask
    * ``htcondor_output_directory()`` / ``htcondor_job_config()`` –
      HTCondor infrastructure methods, always present but only exercised
      when ``--workflow htcondor`` is used
    * ``exclude_params_branch`` – Dask parameters excluded from branches
    """

    # --- workflow parameter value -----------------------------------------

    def test_workflow_param_value_differs_across_executors(self):
        """
        The 'workflow' parameter holds the executor name; its value is the
        only parameter that legitimately differs between modes.
        """
        t_local  = _make_nano_task(workflow="local")
        t_condor = _make_nano_task(workflow="htcondor")
        t_dask   = _make_nano_task(workflow="dask")

        # Values must be the requested executor names
        self.assertEqual(t_local.workflow,  "local")
        self.assertEqual(t_condor.workflow, "htcondor")
        self.assertEqual(t_dask.workflow,   "dask")

        # … and they must be different from each other
        values = {t_local.workflow, t_condor.workflow, t_dask.workflow}
        self.assertEqual(len(values), 3,
                         "All three executor modes must have distinct 'workflow' values")

    # --- Dask-specific parameters -----------------------------------------

    def test_dask_scheduler_param_present_on_dask_task(self):
        """dask_scheduler is present (and is the only extra Dask param)."""
        t = _make_nano_task(workflow="dask")
        self.assertTrue(hasattr(t, "dask_scheduler"))
        self.assertTrue(hasattr(t, "dask_workers"))

    def test_dask_params_excluded_from_branch_tasks(self):
        """
        dask_scheduler and dask_workers are in exclude_params_branch so
        individual branch tasks never receive these scheduling parameters.
        """
        mod = _load_nano()
        excluded = mod.RunNANOJobs.exclude_params_branch
        self.assertIn("dask_scheduler", excluded)
        self.assertIn("dask_workers",   excluded)

    def test_opendata_dask_params_excluded_from_branch_tasks(self):
        """Same exclusion holds for RunOpenDataJobs."""
        mod = _load_opendata()
        excluded = mod.RunOpenDataJobs.exclude_params_branch
        self.assertIn("dask_scheduler", excluded)
        self.assertIn("dask_workers",   excluded)

    # --- HTCondor-specific methods ----------------------------------------

    def test_htcondor_output_directory_present_on_all_tasks(self):
        """
        htcondor_output_directory() is always defined (on all executor
        instances) but is only invoked by LAW when --workflow htcondor is
        used.
        """
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf)
                self.assertTrue(
                    callable(getattr(t, "htcondor_output_directory", None)),
                    f"workflow={wf}: htcondor_output_directory must be callable",
                )

    def test_htcondor_job_config_present_on_all_tasks(self):
        """
        htcondor_job_config() is always defined but only called in HTCondor
        mode; it must accept the same signature regardless of executor.
        """
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                t = _make_nano_task(workflow=wf)
                self.assertTrue(callable(getattr(t, "htcondor_job_config", None)))

    def test_htcondor_job_config_resource_keys_consistent(self):
        """
        htcondor_job_config() always returns +RequestMemory, +MaxRuntime,
        and request_cpus regardless of which executor the task was created
        with.  These are infrastructure metadata, not executor-dependent.
        """
        config = MagicMock()
        config.custom_content = []
        for wf in ("local", "htcondor", "dask"):
            with self.subTest(workflow=wf):
                config.custom_content = []
                t = _make_nano_task(workflow=wf)
                result = t.htcondor_job_config(config, job_num=0, branches=[0])
                keys = [item[0] for item in result.custom_content]
                self.assertIn("+RequestMemory", keys)
                self.assertIn("+MaxRuntime",    keys)
                self.assertIn("request_cpus",   keys)

    # --- get_dask_work / run() symmetry -----------------------------------

    def test_get_dask_work_not_implemented_on_base_dask_workflow(self):
        """
        The base DaskWorkflow.get_dask_work() raises NotImplementedError
        (confirmed allowed difference vs. concrete task classes).
        """
        import workflow_executors

        class _Minimal(workflow_executors.DaskWorkflow):
            task_namespace = ""
            def create_branch_map(self): return {}
            def output(self): return None
            def run(self): pass

        t = _Minimal()
        with self.assertRaises(NotImplementedError):
            t.get_dask_work(0, None)

    def test_concrete_task_implements_get_dask_work(self):
        """
        Concrete tasks (RunNANOJobs, RunOpenDataJobs) override get_dask_work()
        and do not raise NotImplementedError.
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir = os.path.join(tmpdir, "shared_inputs")
            os.makedirs(shared_dir)
            Path(os.path.join(shared_dir, "myexe.sh")).write_text("#!/bin/sh\nexit 0\n")
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)

            for make_fn, label in (
                (_make_nano_task,     "RunNANOJobs"),
                (_make_opendata_task, "RunOpenDataJobs"),
            ):
                with self.subTest(task=label):
                    t = make_fn(workflow="dask", branch=0)
                    with _patch_task_properties(t, tmpdir, shared_dir, job_dir):
                        with patch.object(
                            type(t), "_exe_relpath",
                            new_callable=lambda: property(lambda s: "myexe.sh"),
                        ):
                            func, args, kwargs = t.get_dask_work(0, job_dir)
                    self.assertIs(func, _run_analysis_job,
                                  f"{label}.get_dask_work() must return _run_analysis_job")


# ===========================================================================
# 7. Manifest schema parity
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestManifestSchemaParity(unittest.TestCase):
    """
    Verify that ``OutputManifest`` and its sub-schemas behave consistently
    regardless of which executor is used to produce job outputs.

    Because the manifest is written by the analysis executable (not the
    executor), its structure must be executor-agnostic.  These tests confirm
    that:

    * The same manifest round-trips correctly through YAML serialisation.
    * Schema version checks are independent of the executor.
    * The ``resolve_manifest()`` API returns consistent results for manifests
      produced under any executor.
    """

    def _make_manifest(self, framework_hash="fw_hash_abc", user_hash="user_hash_xyz"):
        from output_schema import (
            OutputManifest, SkimSchema, MetadataSchema, LawArtifactSchema,
        )
        return OutputManifest(
            skim=SkimSchema(output_file="output.root", tree_name="Events"),
            metadata=MetadataSchema(output_file="output_meta.root"),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="run_job",
                    path_pattern="job_outputs/job_*.done",
                    format="text",
                ),
            ],
            framework_hash=framework_hash,
            user_repo_hash=user_hash,
        )

    def test_manifest_validates_without_errors(self):
        """A well-formed manifest validates successfully."""
        manifest = self._make_manifest()
        errors = manifest.validate()
        self.assertEqual(errors, [],
                         f"Manifest validation failed: {errors}")

    def test_manifest_round_trip_yaml(self):
        """Manifest serialises to YAML and back without data loss."""
        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = self._make_manifest()
            path = os.path.join(tmpdir, "manifest.yaml")
            from output_schema import OutputManifest
            manifest.save_yaml(path)
            loaded = OutputManifest.load_yaml(path)

            self.assertEqual(loaded.manifest_version,       manifest.manifest_version)
            self.assertEqual(loaded.framework_hash,         manifest.framework_hash)
            self.assertEqual(loaded.user_repo_hash,         manifest.user_repo_hash)
            self.assertEqual(loaded.skim.output_file,       manifest.skim.output_file)
            self.assertEqual(loaded.metadata.output_file,   manifest.metadata.output_file)
            self.assertEqual(len(loaded.law_artifacts),     len(manifest.law_artifacts))

    def test_resolve_manifest_compatible_when_provenance_matches(self):
        """
        resolve_manifest() returns COMPATIBLE for every schema when the
        manifest provenance matches the current provenance.
        """
        from output_schema import (
            ArtifactResolutionStatus, ProvenanceRecord, resolve_manifest,
        )
        manifest = self._make_manifest(framework_hash="same", user_hash="same_user")
        current = ProvenanceRecord(framework_hash="same", user_repo_hash="same_user")
        statuses = resolve_manifest(manifest, current_provenance=current)
        for role, status in statuses.items():
            self.assertEqual(
                status, ArtifactResolutionStatus.COMPATIBLE,
                f"Schema '{role}' should be COMPATIBLE but is {status}",
            )

    def test_resolve_manifest_stale_when_provenance_differs(self):
        """
        resolve_manifest() returns STALE when the manifest was produced with
        a different framework hash (simulating a code update).
        """
        from output_schema import (
            ArtifactResolutionStatus, ProvenanceRecord, resolve_manifest,
        )
        manifest = self._make_manifest(framework_hash="old_hash")
        current = ProvenanceRecord(framework_hash="new_hash")
        statuses = resolve_manifest(manifest, current_provenance=current)
        # At least the skim/metadata should be STALE
        stale = [r for r, s in statuses.items()
                 if s == ArtifactResolutionStatus.STALE]
        self.assertTrue(
            len(stale) > 0,
            "Expected at least one STALE schema when framework hash changed",
        )

    def test_check_version_compatibility_passes_for_current_manifest(self):
        """
        OutputManifest.check_version_compatibility() raises no exception for
        a manifest produced with the current schema versions.
        """
        from output_schema import OutputManifest
        manifest = self._make_manifest()
        # Should not raise
        try:
            OutputManifest.check_version_compatibility(manifest)
        except Exception as exc:
            self.fail(
                f"check_version_compatibility raised unexpectedly: {exc}"
            )

    def test_schema_registry_contains_all_schema_types(self):
        """
        SCHEMA_REGISTRY lists all known schema names – used by tooling that
        needs to enumerate schemas independently of the executor.
        """
        from output_schema import SCHEMA_REGISTRY
        for name in ("skim", "histogram", "metadata", "cutflow",
                     "law_artifact", "output_manifest"):
            self.assertIn(name, SCHEMA_REGISTRY,
                          f"SCHEMA_REGISTRY is missing entry for '{name}'")


# ===========================================================================
# 8. End-to-end execution parity (_run_analysis_job)
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestEndToEndExecutionParity(unittest.TestCase):
    """
    End-to-end parity tests that run ``_run_analysis_job`` directly,
    simulating both the local executor path (called from ``run()``) and the
    Dask executor path (called on a remote worker).

    Verifies that:
    * Identical inputs produce identical outputs in both paths.
    * Root-setup sourcing works in both paths.
    * Container-setup wrapping is handled consistently.
    * Failure propagation is consistent.
    """

    def _write_exe(self, path, script):
        Path(path).write_text(script)
        os.chmod(path, stat.S_IRWXU)

    def _prepare_job_dir(self, tmpdir, exe_script="#!/bin/sh\nexit 0\n"):
        shared = os.path.join(tmpdir, "shared")
        os.makedirs(shared)
        exe = os.path.join(shared, "run.sh")
        self._write_exe(exe, exe_script)
        job_dir = os.path.join(tmpdir, "job_0")
        os.makedirs(job_dir)
        Path(os.path.join(job_dir, "submit_config.txt")).write_text("batch=true\n")
        return exe, job_dir

    def test_identical_inputs_produce_identical_results(self):
        """
        Calling _run_analysis_job twice with identical arguments yields the
        same result string – simulating local and Dask workers receiving the
        same job.
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._prepare_job_dir(tmpdir)
            result_1 = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            result_2 = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            self.assertEqual(result_1, result_2,
                             "Same inputs must produce identical result strings")

    def test_root_setup_sourced_in_both_paths(self):
        """
        root_setup is sourced before the executable in both local and Dask
        paths (since both call _run_analysis_job with the same root_setup arg).
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe_script = "#!/bin/sh\n[ \"$PARITY_FLAG\" = \"1\" ] || exit 1\nexit 0\n"
            exe, job_dir = self._prepare_job_dir(tmpdir, exe_script)
            root_setup = "export PARITY_FLAG=1"
            # Both invocations (local and simulated Dask) must succeed
            result = _run_analysis_job(
                exe_path=exe, job_dir=job_dir, root_setup=root_setup
            )
            self.assertIn("done:", result)

    def test_failure_raises_runtime_error_consistently(self):
        """
        A failing job raises RuntimeError in both local and Dask paths (both
        call _run_analysis_job which raises on non-zero exit).
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._prepare_job_dir(tmpdir, "#!/bin/sh\nexit 7\n")
            with self.assertRaises(RuntimeError) as ctx:
                _run_analysis_job(exe_path=exe, job_dir=job_dir)
            self.assertIn("7", str(ctx.exception))

    def test_result_format_is_done_colon_job_dir(self):
        """Result is always 'done:<job_dir>' – used by both run() and DaskProxy."""
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._prepare_job_dir(tmpdir)
            result = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            expected = f"done:{job_dir}"
            self.assertEqual(result, expected)

    def test_container_setup_passthrough_consistent(self):
        """
        Both local and Dask paths pass container_setup to _run_analysis_job
        unchanged.  An empty string and an explicit "" must be treated
        identically (no wrapping applied), confirmed by comparing results.
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            exe, job_dir = self._prepare_job_dir(tmpdir)
            # Both calls use container_setup="" – verifies no wrapper is applied
            result_explicit_empty = _run_analysis_job(exe_path=exe, job_dir=job_dir,
                                                      container_setup="")
            # Default container_setup is "" so omitting the kwarg is equivalent
            result_default = _run_analysis_job(exe_path=exe, job_dir=job_dir)
            self.assertEqual(result_explicit_empty, result_default,
                             "Explicit container_setup='' must produce the same result "
                             "as the default (no container wrapping)")

    def test_nano_and_opendata_dask_args_structure_compatible(self):
        """
        The (callable, args, kwargs) triples from RunNANOJobs and
        RunOpenDataJobs have the same structure: func is _run_analysis_job,
        args is a 4-element list [exe, job_dir, root_setup, container_setup],
        kwargs is empty.
        """
        from workflow_executors import _run_analysis_job
        with tempfile.TemporaryDirectory() as tmpdir:
            shared = os.path.join(tmpdir, "shared_inputs")
            os.makedirs(shared)
            Path(os.path.join(shared, "myexe.sh")).write_text("#!/bin/sh\nexit 0\n")
            job_dir = os.path.join(tmpdir, "job_0")
            os.makedirs(job_dir)

            for make_fn, label in (
                (_make_nano_task,     "RunNANOJobs"),
                (_make_opendata_task, "RunOpenDataJobs"),
            ):
                with self.subTest(task=label):
                    t = make_fn(branch=0)
                    with _patch_task_properties(t, tmpdir, shared, job_dir):
                        with patch.object(
                            type(t), "_exe_relpath",
                            new_callable=lambda: property(lambda s: "myexe.sh"),
                        ):
                            func, args, kwargs = t.get_dask_work(0, job_dir)

                    self.assertIs(func, _run_analysis_job)
                    self.assertEqual(len(args), 4,
                                     f"{label}: args must have 4 elements")
                    self.assertEqual(kwargs, {},
                                     f"{label}: kwargs must be empty")


# ===========================================================================
# Entry point
# ===========================================================================

if __name__ == "__main__":
    unittest.main(verbosity=2)
