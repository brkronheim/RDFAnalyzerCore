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
import shlex
import shutil
import subprocess
import sys
import tempfile
import warnings
import traceback
from pathlib import Path

import luigi  # type: ignore
import law  # type: ignore
from law.workflow.base import BaseWorkflow, BaseWorkflowProxy
from law.contrib.htcondor import HTCondorWorkflow  # noqa: F401  – re-exported for callers

# ---------------------------------------------------------------------------
# Optional performance recording (best-effort; skipped if not importable).
#
# This try/except runs once at module-import time.  On Dask workers the same
# code path executes, so performance_recorder.py must be accessible on their
# filesystem (typically via the shared EOS/NFS mount that also holds the
# analysis executables and job directories).  If the import fails on a worker,
# recording is silently skipped – the analysis job still runs normally.
# ---------------------------------------------------------------------------
_here = os.path.dirname(os.path.abspath(__file__))
if _here not in sys.path:
    sys.path.insert(0, _here)

try:
    from performance_recorder import (  # type: ignore
        PerformanceRecorder as _PerformanceRecorder,
        estimate_job_input_bytes as _estimate_job_input_bytes,
        perf_path_for as _perf_path_for,
    )
except ImportError:
    _PerformanceRecorder = None  # type: ignore[assignment,misc]
    _estimate_job_input_bytes = None  # type: ignore[assignment]
    _perf_path_for = None  # type: ignore[assignment]
from failure_handler import (  # noqa: E402
    DiagnosticSummary,
    FailureCategory,
    FailureRecord,
    RetryPolicy,
    DEFAULT_RETRY_POLICIES,
    classify_failure,
    run_with_retries,
)


# ---------------------------------------------------------------------------
# Pure function: execute one analysis job (picklable → usable on Dask workers)
# ---------------------------------------------------------------------------

def _run_analysis_job(
    exe_path: str,
    job_dir: str,
    root_setup: str = "",
    container_setup: str = "",
    config_relpath: str = "submit_config.txt",
) -> str:
    """
    Run a single analysis job in *job_dir* by invoking *exe_path* with a
    job config path such as ``submit_config.txt`` or ``cfg/submit_config.txt``.

    This function is intentionally free of LAW/Luigi imports so that it can be
    pickled and sent to a remote Dask worker without shipping the full task
    graph.

    Performance metrics (wall time, peak RSS, throughput) are recorded to
    ``job.perf.json`` inside *job_dir* via
    :class:`performance_recorder.PerformanceRecorder`.  The job directory is
    on a shared filesystem (EOS/NFS), so ``job.perf.json`` is accessible from
    both the worker node and the submit node after the job completes.

    For **Dask** jobs, :class:`DaskWorkflowProxy` reads this file after the
    future resolves and copies it to the branch output location
    (``job_outputs/job_N.perf.json``) so all execution paths produce
    performance data at a consistent location.

    Parameters
    ----------
    exe_path:
        Absolute path to the compiled analysis executable (accessible from the
        worker via a shared filesystem, e.g. EOS/NFS).
    job_dir:
        Directory that contains ``submit_config.txt`` and all auxiliary files.
    root_setup:
        Shell commands (multi-line string) to source before running the
        executable.  Written to a temporary file and sourced inside the job
        shell.
    container_setup:
        Optional container command prefix (e.g. ``cmssw-el9``) used to wrap the
        entire inner command.  The string ``{cmd}`` is replaced with the inner
        command when present; otherwise the inner command is appended after
        ``--command-to-run`` (for CMSSW wrappers) or ``bash -c``.
    config_relpath:
        Config path relative to *job_dir* passed to the analysis executable.

    Returns
    -------
    str
        A short status string (``"done:<job_dir>"``).

    Raises
    ------
    RuntimeError
        If the analysis process exits with a non-zero return code.
    """
    cmd_parts: list[str] = []

    # Optional ROOT / CMSSW environment setup
    if root_setup:
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix="_root_setup.sh",
            dir=job_dir,
            delete=False,
        )
        tmp.write(root_setup)
        tmp.close()
        cmd_parts.append(f"source {shlex.quote(tmp.name)}")

    cmd_parts.extend([
        f"cd {shlex.quote(job_dir)}",
        "if [ -f x509 ]; then export X509_USER_PROXY=$PWD/x509; fi",
        f"chmod +x {shlex.quote(exe_path)}",
        f"{shlex.quote(exe_path)} {shlex.quote(config_relpath)}",
    ])

    inner_cmd = " && ".join(cmd_parts)

    container = container_setup.strip()
    if container:
        if "{cmd}" in container:
            full_cmd = container.replace("{cmd}", f"bash -c {shlex.quote(inner_cmd)}")
        elif "cmssw-el" in container and "--command-to-run" not in container:
            full_cmd = f"{container} --command-to-run {shlex.quote(inner_cmd)}"
        elif "--command-to-run" in container:
            full_cmd = f"{container} {shlex.quote(inner_cmd)}"
        else:
            full_cmd = f"{container} bash -c {shlex.quote(inner_cmd)}"
    else:
        full_cmd = f"bash -c {shlex.quote(inner_cmd)}"

    task_label = f"analysis_job:{os.path.basename(job_dir)}"

    if _PerformanceRecorder is not None:
        input_bytes = _estimate_job_input_bytes(job_dir) if _estimate_job_input_bytes else 0
        with _PerformanceRecorder(task_label) as rec:
            proc = subprocess.Popen(
                full_cmd,
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            rec.monitor_process(proc.pid)
            stdout_data, stderr_data = proc.communicate()
            returncode = proc.returncode
            rec.set_throughput(input_bytes)

        # Write performance metrics (best-effort; log a warning on failure)
        try:
            rec.save(os.path.join(job_dir, "job.perf.json"))
        except OSError as exc:
            warnings.warn(
                f"Could not write job.perf.json in {job_dir!r}: {exc}",
                RuntimeWarning,
                stacklevel=2,
            )

        if returncode != 0:
            stderr_tail = stderr_data[-2000:] if stderr_data else ""
            raise RuntimeError(
                f"Analysis job failed (exit {returncode}) in {job_dir!r}."
                + (f"\nstderr:\n{stderr_tail}" if stderr_tail else "")
            )
    else:
        # Fallback: original subprocess.run path (no performance recording)
        result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
        if result.returncode != 0:
            stderr_tail = result.stderr[-2000:] if result.stderr else ""
            raise RuntimeError(
                f"Analysis job failed (exit {result.returncode}) in {job_dir!r}."
                + (f"\nstderr:\n{stderr_tail}" if stderr_tail else "")
            )

    return f"done:{job_dir}"


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
        try:
            from distributed import Client  # type: ignore
            from distributed import as_completed as dask_ac  # type: ignore
        except ImportError as exc:
            raise RuntimeError(
                "--workflow dask requires dask[distributed]. "
                "Install with:  pip install dask[distributed]"
            ) from exc

        task = self.task
        scheduler: str = getattr(task, "dask_scheduler", "") or ""
        # Allow the task to supply custom retry policies; fall back to defaults.
        retry_policies: dict[FailureCategory, RetryPolicy] = getattr(
            task, "dask_retry_policies", DEFAULT_RETRY_POLICIES
        )

        if scheduler:
            client = Client(scheduler)
        else:
            # Fall back to a local Dask cluster (useful for testing)
            client = Client()

        try:
            branch_map = task.branch_map
            futures_to_branch: dict = {}

            for branch_num, branch_data in branch_map.items():
                branch_task = task.req(task, branch=branch_num)
                if branch_task.complete():
                    task.publish_message(
                        f"Branch {branch_num}: already complete, skipping."
                    )
                    continue

                func, args, kwargs = task.get_dask_work(branch_num, branch_data)
                future = client.submit(func, *args, **kwargs, pure=False)
                futures_to_branch[future] = (branch_num, branch_task, func, args, kwargs)

            if not futures_to_branch:
                task.publish_message("All branches are already complete.")
                return

            task.publish_message(
                f"Submitted {len(futures_to_branch)} branch(es) to Dask "
                f"scheduler {scheduler or '(local)'}."
            )

            summary = DiagnosticSummary()
            errors: list[str] = []

            for future in dask_ac(list(futures_to_branch.keys())):
                branch_num, branch_task, func, args, kwargs = futures_to_branch[future]
                try:
                    # Retrieve the result from the completed future; if it
                    # failed, run_with_retries will classify and retry it
                    # locally (re-submitting to Dask for each retry).
                    def _submit_and_wait(f, a, k, _client=client):
                        fut = _client.submit(f, *a, **k, pure=False)
                        return fut.result()

                    try:
                        result_text = future.result()
                    except Exception as first_exc:  # noqa: BLE001
                        # The initial future failed; use smart retry for
                        # subsequent attempts, submitting new futures to Dask.
                        result_text = run_with_retries(
                            fn=_submit_and_wait,
                            args=[func, args, kwargs],
                            branch_num=branch_num,
                            summary=summary,
                            policies=retry_policies,
                        )
                        # Record the first failure manually (run_with_retries
                        # only records retried attempts).
                        stderr = ""
                        msg_str = str(first_exc)
                        if "\nstderr:\n" in msg_str:
                            stderr = msg_str.split("\nstderr:\n", 1)[1]
                        category = classify_failure(first_exc, stderr=stderr)
                        task.publish_message(
                            f"Branch {branch_num}: classified failure as "
                            f"{category.value!r}; retrying…"
                        )

                    # Write the branch output so LAW marks the branch complete
                    out = branch_task.output()
                    Path(out.path).parent.mkdir(parents=True, exist_ok=True)
                    with open(out.path, "w") as fh:
                        fh.write(f"status=done\n{result_text}\n")

                    # Copy job.perf.json from the job directory (written on the
                    # Dask worker via _run_analysis_job) to alongside the .done
                    # file so downstream tools find performance data at the
                    # standard location even for Dask-dispatched jobs.
                    if _perf_path_for is not None and result_text.startswith("done:"):
                        job_dir = result_text[len("done:"):]
                        src_perf = os.path.join(job_dir, "job.perf.json")
                        if os.path.isfile(src_perf):
                            try:
                                shutil.copy2(src_perf, _perf_path_for(out.path))
                            except OSError:
                                pass

                    task.publish_message(
                        f"Branch {branch_num}: complete ({result_text})."
                    )
                except Exception as exc:  # noqa: BLE001
                    tb = traceback.format_exc()
                    msg = f"Branch {branch_num} failed: {exc}"
                    if tb and tb.strip() != "NoneType: None":
                        msg = f"{msg}\n{tb}"
                    errors.append(msg)
                    task.publish_message(f"ERROR: {msg}")

            # Always emit the diagnostic summary so operators can inspect it.
            if summary.total_failures() > 0:
                task.publish_message(summary.report())

            if errors:
                raise RuntimeError(
                    f"Dask execution: {len(errors)} branch(es) failed.\n"
                    + "\n".join(errors)
                )
        finally:
            client.close()


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
    exclude_params_branch = {"dask_scheduler", "dask_workers"}
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
