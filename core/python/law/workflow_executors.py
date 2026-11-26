"""
Workflow executor support for RDFAnalyzerCore law tasks.

This module provides:

  DaskWorkflow   – a LAW workflow base-class that dispatches branches to a
                   Dask distributed cluster when ``--workflow dask`` is used.
                   Inherit from it alongside ``law.LocalWorkflow`` and/or
                   ``law.HTCondorWorkflow`` to make a task support all three
                   execution back-ends.

  _run_analysis_job  – a pure, picklable function that executes one analysis
                       job on a remote worker (used by the Dask proxy).

The Dask proxy integrates with :mod:`failure_handler` to classify each branch
failure, apply per-category retry policies, and collect a
:class:`~failure_handler.DiagnosticSummary` that is printed at the end of the
run.  See :mod:`failure_handler` for details on failure categories and retry
policies.

Usage in a task file
--------------------
::

    import law
    from workflow_executors import DaskWorkflow, _run_analysis_job

    class RunMyJobs(MyMixin, law.LocalWorkflow, law.HTCondorWorkflow, DaskWorkflow):

        def create_branch_map(self):
            ...

        def output(self):
            return law.LocalFileTarget(...)

        def run(self):
            # Runs for --workflow local AND --workflow htcondor (branch task)
            ...

        # --- HTCondor config -------------------------------------------
        def htcondor_output_directory(self):
            return law.LocalDirectoryTarget(...)

        def htcondor_job_config(self, config, job_num, branches):
            config.custom_content.append(("+RequestMemory", "2000"))
            return config

        # --- Dask config -----------------------------------------------
        def get_dask_work(self, branch_num, branch_data):
            exe = os.path.join(self._shared_dir, self._exe_relpath)
            return _run_analysis_job, [exe, branch_data, "", ""], {}
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import warnings
import traceback
from pathlib import Path
from typing import Optional

import luigi  # type: ignore
import law  # type: ignore
from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.contrib.htcondor import HTCondorWorkflow  # noqa: F401  – re-exported for callers

# Direct LAW users import this file from the law directory.
_core_python = str(Path(__file__).resolve().parents[1])
if _core_python not in sys.path:
    sys.path.insert(0, _core_python)

from rdfproduction.executors import _IOThroughputMonitor, _optimize_job_config_xrootd, _run_analysis_job, dispatch_dask
from rdfproduction.retry import DEFAULT_RETRY_POLICIES, DiagnosticSummary, FailureCategory, FailureRecord, RetryPolicy


# ---------------------------------------------------------------------------
# DaskWorkflow
# ---------------------------------------------------------------------------

class DaskWorkflowProxy(BaseWorkflowProxy):
    """
    Workflow proxy that dispatches branch tasks to a Dask distributed cluster.

    The proxy calls ``task.get_dask_work(branch_num, branch_data)`` for each
    incomplete branch to obtain a ``(callable, args, kwargs)`` triple, which it
    submits to the Dask cluster.  When the future resolves, the proxy writes
    the branch output file so that LAW considers the branch complete.

    Performance recording
    ---------------------
    After each future resolves the proxy reads ``job.perf.json`` from the job
    directory (written on the Dask worker by :func:`_run_analysis_job`) and
    copies it to the branch output directory alongside the ``.done`` file,
    so performance data is available at a consistent location for all execution
    backends (local, HTCondor, Dask).

    See :class:`DaskWorkflow` for the full documentation.
    """

    workflow_type = "dask"

    def requires(self):
        reqs = super().requires()
        dask_reqs = self.task.dask_workflow_requires()
        if dask_reqs:
            reqs.update(dask_reqs)
        return reqs

    def run(self):
        task = self.task

        def branch(index):
            return task.req(task, branch=index)

        dispatch_dask(
            task.branch_map,
            complete=lambda index: branch(index).complete(),
            work=task.get_dask_work,
            output_path=lambda index: branch(index).output().path,
            publish=task.publish_message,
            scheduler=getattr(task, "dask_scheduler", ""),
            workers=getattr(task, "dask_workers", 1),
            threads_per_worker=getattr(task, "dask_threads_per_worker", None),
            retry_policies=getattr(task, "dask_retry_policies", DEFAULT_RETRY_POLICIES),
        )


class DaskWorkflow(BaseWorkflow):
    """
    LAW workflow base class for Dask distributed execution.

    Inherit from this class alongside ``law.LocalWorkflow`` and/or
    ``law.HTCondorWorkflow`` to add ``--workflow dask`` support to any LAW
    workflow task.

    Required override
    -----------------
    ``get_dask_work(branch_num, branch_data)``
        Return ``(callable, args, kwargs)`` describing the work to submit to
        a Dask worker for branch *branch_num*.  The callable must be picklable
        (i.e. a module-level function, not a lambda or closure).

    Optional overrides
    ------------------
    ``dask_workflow_requires()``
        Additional task requirements that must be satisfied before Dask
        submission begins.

    ``dask_retry_policies``
        A ``dict[FailureCategory, RetryPolicy]`` attribute (not a method) that
        overrides the per-category retry policies used by the Dask proxy.
        Defaults to :data:`~failure_handler.DEFAULT_RETRY_POLICIES`.

    Parameters
    ----------
    dask_scheduler : str
        Dask scheduler address, e.g. ``tcp://scheduler-host:8786``.
        Leave empty to use a local (in-process) Dask cluster – useful for
        testing or single-machine workflows.
    dask_workers : int
        When ``dask_scheduler`` is empty (local cluster), this controls how
        many worker processes are launched.  Defaults to ``1``.

    Example
    -------
    ::

        class RunMyJobs(MyMixin, law.LocalWorkflow, law.HTCondorWorkflow, DaskWorkflow):

            def create_branch_map(self):
                return {i: job_dir for i, job_dir in enumerate(self.all_job_dirs)}

            def output(self):
                return law.LocalFileTarget(
                    os.path.join(self._main_dir, "outputs", f"job_{self.branch}.done")
                )

            def run(self):
                # Called for --workflow local and --workflow htcondor branches
                ...

            # --- HTCondor --------------------------------------------------
            def htcondor_output_directory(self):
                return law.LocalDirectoryTarget(
                    os.path.join(self._main_dir, "law_htcondor")
                )

            def htcondor_job_config(self, config, job_num, branches):
                config.custom_content.append(("+RequestMemory", "2000"))
                return config

            # --- Dask ------------------------------------------------------
            def get_dask_work(self, branch_num, branch_data):
                exe = os.path.join(self._shared_dir, self._exe_relpath)
                return _run_analysis_job, [exe, branch_data, "", ""], {}

    Usage
    -----
    ::

        # Run all branches locally (default)
        law run RunMyJobs --workflow local

        # Dispatch branches to an HTCondor cluster via LAW
        law run RunMyJobs --workflow htcondor

        # Dispatch branches to a Dask cluster
        law run RunMyJobs --workflow dask --dask-scheduler tcp://scheduler:8786
    """

    workflow_proxy_cls = DaskWorkflowProxy

    dask_scheduler = luigi.Parameter(
        default="",
        significant=False,
        description=(
            "Dask scheduler address for --workflow dask "
            "(e.g. tcp://scheduler-host:8786). "
            "Leave empty to start a local Dask cluster (testing only)."
        ),
    )
    dask_workers = luigi.IntParameter(
        default=1,
        significant=False,
        description=(
            "Number of workers in the local Dask cluster when "
            "--dask-scheduler is empty (default: 1)."
        ),
    )
    dask_threads_per_worker = luigi.OptionalIntParameter(
        default=None, significant=False,
        description="Task slots per worker in a local Dask cluster; unset uses Dask's default.",
    )

    #: Per-category retry policies used by :class:`DaskWorkflowProxy`.
    #: Override in a subclass to customise retry behaviour, e.g.::
    #:
    #:     from failure_handler import DEFAULT_RETRY_POLICIES, FailureCategory, RetryPolicy
    #:     dask_retry_policies = {
    #:         **DEFAULT_RETRY_POLICIES,
    #:         FailureCategory.TRANSIENT_IO: RetryPolicy(max_retries=10, base_delay=5.0),
    #:     }
    dask_retry_policies: dict[FailureCategory, RetryPolicy] = DEFAULT_RETRY_POLICIES

    # These parameters must not propagate to branch tasks.
    # Subclasses that extend exclude_params_branch should merge rather than
    # replace this set to preserve the Dask scheduler exclusion, e.g.:
    #   exclude_params_branch = DaskWorkflow.exclude_params_branch | {"extra_param"}
    exclude_params_branch = {"dask_scheduler", "dask_workers", "dask_threads_per_worker"}
    exclude_index = True

    def dask_workflow_requires(self):
        """
        Additional requirements that must be satisfied before Dask submission.
        Override to add custom requirements.
        """
        from law.util import DotDict  # type: ignore
        return DotDict()

    def get_dask_work(self, branch_num: int, branch_data) -> tuple:
        """
        Return ``(callable, args, kwargs)`` for branch *branch_num*.

        The callable will be submitted to a Dask worker.  It must be picklable
        (module-level function, not a lambda or method).

        Subclasses **must** override this method when using ``--workflow dask``.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_dask_work() "
            "to support --workflow dask."
        )
