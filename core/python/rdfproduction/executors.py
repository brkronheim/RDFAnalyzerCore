"""Local analysis subprocesses and Dask dispatch shared by production front ends."""

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

from .performance import (
    PerformanceRecorder as _PerformanceRecorder,
    estimate_job_input_bytes as _estimate_job_input_bytes,
    perf_path_for as _perf_path_for,
)
from .retry import DEFAULT_RETRY_POLICIES, DiagnosticSummary, classify_failure, run_with_retries

# ---------------------------------------------------------------------------
# Runtime I/O throughput monitor (Linux only)
# ---------------------------------------------------------------------------

class _IOThroughputMonitor:
    """Background thread that tracks a process's read throughput via /proc.

    Reads ``/proc/{pid}/io`` (Linux only) at a configurable interval and
    computes a rolling average throughput.  When the sustained throughput
    falls below *threshold_mbs* the :attr:`was_slow` flag is set.

    This provides a passive signal for slow XRootD sources.  Since the
    analysis executable reads files in its own thread(s), this monitor works
    for single-threaded and multi-threaded workloads without any code changes
    to the compiled binary.

    Parameters
    ----------
    pid:
        PID of the subprocess to monitor.
    threshold_mbs:
        Throughput (MB/s) below which the job is considered slow.
    poll_interval:
        Seconds between ``/proc`` samples.
    grace_s:
        Seconds of consistently low throughput before ``was_slow`` is set.
    """

    def __init__(
        self,
        pid: int,
        threshold_mbs: float = 1.0,
        poll_interval: float = 5.0,
        grace_s: float = 60.0,
    ) -> None:
        self._pid = pid
        self._threshold = threshold_mbs
        self._interval = poll_interval
        self._grace = grace_s
        self.was_slow: bool = False
        self.avg_throughput_mbs: float = 0.0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the background monitoring thread."""
        if sys.platform == "linux":
            self._thread.start()

    def stop(self) -> None:
        """Stop the monitoring thread."""
        self._stop.set()
        self._thread.join(timeout=max(self._interval * 2, 5.0))

    def _read_bytes_read(self) -> int:
        """Return cumulative bytes read by *pid* from ``/proc``, or -1."""
        try:
            with open(f"/proc/{self._pid}/io") as fh:
                for line in fh:
                    if line.startswith("read_bytes:"):
                        return int(line.split(":")[1].strip())
        except (OSError, ValueError):
            pass
        return -1

    def _run(self) -> None:
        prev_bytes = self._read_bytes_read()
        if prev_bytes < 0:
            return
        prev_time = time.monotonic()
        below_since: Optional[float] = None
        samples: list[float] = []

        while not self._stop.is_set():
            self._stop.wait(self._interval)
            now_bytes = self._read_bytes_read()
            if now_bytes < 0:
                break
            now_time = time.monotonic()
            delta_bytes = now_bytes - prev_bytes
            delta_time = now_time - prev_time
            if delta_time > 0 and delta_bytes >= 0:
                tput = (delta_bytes / (1024.0 * 1024.0)) / delta_time
                samples.append(tput)
                if samples:
                    self.avg_throughput_mbs = sum(samples) / len(samples)
                if tput < self._threshold:
                    if below_since is None:
                        below_since = now_time
                    elif (now_time - below_since) >= self._grace:
                        self.was_slow = True
                else:
                    below_since = None
            prev_bytes = now_bytes
            prev_time = now_time


# ---------------------------------------------------------------------------
# XRootD site optimisation helper
# ---------------------------------------------------------------------------

def _optimize_job_config_xrootd(config_path: str) -> None:
    """Rewrite XRootD URLs in *config_path* to use the fastest available site.

    Reads the ``fileList`` key from the job config, calls
    :func:`xrootd_site_selector.optimize_file_list` to probe all known CMS
    redirectors in parallel, and overwrites the config with the best URLs.

    This is a best-effort optimisation: if the site selector is not
    importable, no redirectors are reachable, or the config has no XRootD
    files, the config is left unchanged and no error is raised.

    Parameters
    ----------
    config_path:
        Absolute path to the per-job ``submit_config.yaml`` (or ``.yaml``).
    """
    if not os.path.isfile(config_path):
        return

    # Lazy import so the function remains picklable even when the selector
    # module is not available on the submit node at serialisation time.
    try:
        from xrootd_site_selector import optimize_file_list  # type: ignore[import]
    except ImportError:
        return  # selector not available; skip optimisation silently

    from native_config_io import read_config, write_config, runtime_yaml_path
    cfg = read_config(config_path)
    file_list_raw = cfg.get("fileList", "")
    if not file_list_raw:
        return
    if str(cfg.get("xrootdRedirector", "") or cfg.get("redirector", "")).strip():
        return

    files = [f.strip() for f in file_list_raw.split(",") if f.strip()]
    optimizable_indices = [
        i for i, f in enumerate(files)
        if f.startswith("/store/") or f.startswith("/eos/")
    ]
    if not optimizable_indices:
        return  # nothing to optimise

    # XRDFS discovery already chose the redirector for fully qualified
    # root:// URLs. Preserve those URLs as-is and only optimise bare LFNs.
    optimizable_files = [files[i] for i in optimizable_indices]
    optimized_files = optimize_file_list(optimizable_files)
    if optimized_files == optimizable_files:
        return  # no change

    optimized = list(files)
    for index, optimized_file in zip(optimizable_indices, optimized_files):
        optimized[index] = optimized_file

    cfg["fileList"] = ",".join(optimized)

    write_config(cfg, runtime_yaml_path(config_path))


# ---------------------------------------------------------------------------
# Pure function: execute one analysis job (picklable → usable on Dask workers)
# ---------------------------------------------------------------------------

def _run_analysis_job(
    exe_path: str,
    job_dir: str,
    root_setup: str = "",
    container_setup: str = "",
    config_relpath: Optional[str] = None,
    stream_output: bool = False,
    worker_number: Optional[int] = None,
) -> str:
    """
    Run a single analysis job in *job_dir* by invoking *exe_path* with a
    job config path such as ``submit_config.yaml`` or ``cfg/submit_config.yaml``.

    Remote Dask workers can deserialize this callable independently of the controller.

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
        Directory that contains ``submit_config.yaml`` and all auxiliary files.
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
    stream_output:
        When ``True``, stream the child process stdout/stderr live to the
        current terminal while keeping a bounded stderr tail for error
        reporting. Intended for interactive local runs.

    Returns
    -------
    str
        A short status string (``"done:<job_dir>"``).

    Raises
    ------
    RuntimeError
        If the analysis process exits with a non-zero return code.
    """
    from native_config_io import job_config_path, materialize_runtime_config
    if config_relpath is None:
        config_relpath = job_config_path(job_dir).name
    source = os.path.join(job_dir, config_relpath)
    if os.path.isfile(source):
        config_relpath = os.path.relpath(materialize_runtime_config(source), job_dir)

    # ------------------------------------------------------------------
    # XRootD site optimisation (best-effort; never aborts the job)
    # ------------------------------------------------------------------
    # When the config contains XRootD file URLs, probe all known CMS
    # redirectors from the current worker node and substitute the fastest
    # site-specific URL before the analysis executable opens the files.
    # The optimisation is skipped when XRD_OPTIMIZE_SITES=0 is set.
    if os.environ.get("XRD_OPTIMIZE_SITES", "1") != "0":
        config_abs = os.path.join(job_dir, config_relpath)
        try:
            _optimize_job_config_xrootd(config_abs)
        except Exception as _xrd_exc:
            warnings.warn(
                f"XRootD site optimisation failed (will use original URLs): {_xrd_exc}",
                RuntimeWarning,
                stacklevel=2,
            )

    cmd_parts: list[str] = []

    gpu_devices = [
        device.strip()
        for device in os.environ.get("RDF_GPU_DEVICE_IDS", os.environ.get("RDF_VH_GPU_DEVICE_IDS", "")).split(",")
        if device.strip()
    ]
    gpu_setup: list[str] = []
    if worker_number is not None and gpu_devices:
        gpu_device = gpu_devices[worker_number % len(gpu_devices)]
        gpu_setup.extend(
            [
                f"export CUDA_VISIBLE_DEVICES={shlex.quote(gpu_device)}",
                f"echo {shlex.quote(f'[RDF] worker {worker_number} uses CUDA device {gpu_device}')}",
            ]
        )

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

    # Apply the assignment after ROOT/CMSSW setup in case that setup script
    # changes CUDA_VISIBLE_DEVICES.  The analysis then sees its chosen GPU as
    # CUDA device 0, which is what the CUDA kinematic-fit manager uses.
    cmd_parts.extend(gpu_setup)

    cmd_parts.extend([
        f"cd {shlex.quote(job_dir)}",
        'export LD_LIBRARY_PATH="$PWD${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"',
        'export ROOT_INCLUDE_PATH="$PWD${ROOT_INCLUDE_PATH:+:$ROOT_INCLUDE_PATH}"',
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

    def _append_tail(buf: bytearray, chunk: bytes, max_bytes: int = 65536) -> None:
        buf.extend(chunk)
        overflow = len(buf) - max_bytes
        if overflow > 0:
            del buf[:overflow]

    def _write_stream(target, chunk: bytes) -> None:
        buffer = getattr(target, "buffer", None)
        if buffer is not None:
            buffer.write(chunk)
            buffer.flush()
            return
        target.write(chunk.decode(errors="replace"))
        target.flush()

    def _stream_subprocess_output(
        proc: subprocess.Popen,
        *,
        emit: bool,
    ) -> tuple[str, str]:
        stdout_tail = bytearray()
        stderr_tail = bytearray()

        def _drain_stream(stream, sink, tail_buf: bytearray) -> None:
            try:
                while True:
                    chunk = stream.read(4096)
                    if not chunk:
                        break
                    _append_tail(tail_buf, chunk)
                    if emit:
                        _write_stream(sink, chunk)
            finally:
                stream.close()

        stdout_thread = threading.Thread(
            target=_drain_stream,
            args=(proc.stdout, sys.stdout, stdout_tail),
            daemon=True,
        )
        stderr_thread = threading.Thread(
            target=_drain_stream,
            args=(proc.stderr, sys.stderr, stderr_tail),
            daemon=True,
        )
        stdout_thread.start()
        stderr_thread.start()
        stdout_thread.join()
        stderr_thread.join()

        return (
            stdout_tail.decode(errors="replace"),
            stderr_tail.decode(errors="replace"),
        )

    if _PerformanceRecorder is not None:
        input_bytes = _estimate_job_input_bytes(job_dir) if _estimate_job_input_bytes else 0
        with _PerformanceRecorder(task_label) as rec:
            popen_kwargs = {
                "args": full_cmd,
                "shell": True,
                "stdout": subprocess.PIPE,
                "stderr": subprocess.PIPE,
                "bufsize": 0,
            }
            proc = subprocess.Popen(**popen_kwargs)
            rec.monitor_process(proc.pid)
            slow_detector = _IOThroughputMonitor(proc.pid)
            slow_detector.start()
            stdout_data, stderr_data = _stream_subprocess_output(
                proc, emit=stream_output
            )
            returncode = proc.wait()
            slow_detector.stop()
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

        if slow_detector.was_slow:
            warnings.warn(
                f"Slow XRootD throughput detected during job in {job_dir!r} "
                f"(avg {slow_detector.avg_throughput_mbs:.2f} MB/s).  "
                "Consider re-running with a different site or enabling stage-in.",
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
        proc = subprocess.Popen(
            full_cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
        )
        stdout_data, stderr_data = _stream_subprocess_output(
            proc, emit=stream_output
        )
        returncode = proc.wait()
        stderr_tail = stderr_data[-2000:] if stderr_data else ""
        if returncode != 0:
            raise RuntimeError(
                f"Analysis job failed (exit {returncode}) in {job_dir!r}."
                + (f"\nstderr:\n{stderr_tail}" if stderr_tail else "")
            )

    if os.environ.get("RDF_JOB_SUMMARY", os.environ.get("RDF_VH_JOB_SUMMARY", "")).strip().lower() in {
        "1", "true", "yes", "on",
    }:
        output = "\n".join((stdout_data or "", stderr_data or ""))
        event_match = re.search(
            r"^RDF_ANALYZER_EVENTS=(\d+)\s*$", output, re.MULTILINE
        )
        timing_match = re.search(
            r"^RDF_ANALYZER_TIMING\s+setup_s=([0-9.eE+-]+)"
            r"\s+execution_s=([0-9.eE+-]+)\s*$",
            output,
            re.MULTILINE,
        )

        if timing_match:
            setup_seconds = float(timing_match.group(1))
            execution_seconds = float(timing_match.group(2))
            timing_text = (
                f"setup={setup_seconds:.2f}s "
                f"execution={execution_seconds:.2f}s"
            )
        else:
            timing_text = "setup=unknown execution=unknown"

        if event_match:
            events = int(event_match.group(1))
            rate = events / execution_seconds if execution_seconds > 0 else 0.0
            event_text = f"events={events:,} rate={rate:,.1f} events/s"
        else:
            event_text = "events=unknown rate=unknown"

        job_name = os.path.basename(job_dir)
        if job_name.startswith("job_"):
            job_name = os.path.join(os.path.basename(os.path.dirname(job_dir)), job_name)
        print(
            f"[RDF_JOB_FINISHED] {job_name}: {timing_text} {event_text}",
            flush=True,
        )

    return f"done:{job_dir}"


def write_dask_result(path, result):
    """Record a completed branch and copy performance measurements if present."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(f"status=done\n{result}\n")
    if result.startswith("done:"):
        src = Path(result.removeprefix("done:")) / "job.perf.json"
        if src.is_file():
            try:
                shutil.copy2(src, _perf_path_for(str(path)))
            except OSError:
                pass


def dispatch_dask(branch_map, *, complete, work, output_path, publish=print,
                  scheduler="", workers=1, threads_per_worker=None,
                  retry_policies=DEFAULT_RETRY_POLICIES):
    """Dispatch picklable branch calls and record verified completions locally."""
    pending = {}
    for index, entry in branch_map.items():
        if complete(index):
            publish(f"Branch {index}: already complete, skipping.")
        else:
            pending[index] = entry
    if not pending:
        publish("All branches are already complete.")
        return

    try:
        from distributed import Client  # type: ignore
        from distributed import as_completed as dask_ac  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "--workflow dask requires dask[distributed]. "
            "Install with:  pip install dask[distributed]"
        ) from exc

    if scheduler:
        if workers != 1:
            publish(
                "dask_workers applies only to a local Dask cluster; "
                "the external scheduler controls its worker count."
            )
        client = Client(scheduler)
    else:
        # Fall back to a local Dask cluster (useful for testing)
        if type(workers) is not int or workers < 1:
            raise ValueError("dask_workers must be a positive integer for a local Dask cluster")
        threads = threads_per_worker
        options = {}
        if threads is not None:
            if type(threads) is not int or threads < 1:
                raise ValueError("dask_threads_per_worker must be a positive integer")
            options["threads_per_worker"] = threads
        client = Client(n_workers=workers, **options)

    try:
        futures_to_branch: dict = {}

        for branch_num, branch_data in pending.items():
            func, args, kwargs = work(branch_num, branch_data)
            future = client.submit(func, *args, **kwargs, pure=False)
            futures_to_branch[future] = (branch_num, func, args, kwargs)

        publish(
            f"Submitted {len(futures_to_branch)} branch(es) to Dask "
            f"scheduler {scheduler or '(local)'}."
        )

        summary = DiagnosticSummary()
        errors: list[str] = []

        for future in dask_ac(list(futures_to_branch.keys())):
            branch_num, func, args, kwargs = futures_to_branch[future]
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
                    publish(
                        f"Branch {branch_num}: classified failure as "
                        f"{category.value!r}; retrying…"
                    )

                write_dask_result(output_path(branch_num), result_text)

                publish(
                    f"Branch {branch_num}: complete ({result_text})."
                )
            except Exception as exc:  # noqa: BLE001
                tb = traceback.format_exc()
                msg = f"Branch {branch_num} failed: {exc}"
                if tb and tb.strip() != "NoneType: None":
                    msg = f"{msg}\n{tb}"
                errors.append(msg)
                publish(f"ERROR: {msg}")

        # Always emit the diagnostic summary so operators can inspect it.
        if summary.total_failures() > 0:
            publish(summary.report())

        if errors:
            raise RuntimeError(
                f"Dask execution: {len(errors)} branch(es) failed.\n"
                + "\n".join(errors)
            )
    finally:
        client.close()
