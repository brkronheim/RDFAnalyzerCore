"""
Performance recording utilities for RDFAnalyzerCore law tasks.

Provides :class:`PerformanceRecorder`, a context manager that captures:

  - ``start_time`` / ``end_time`` – ISO 8601 UTC timestamps.
  - ``wall_time_s`` – elapsed wall-clock seconds (always recorded).
  - ``peak_rss_mb`` – peak resident-set-size memory of a monitored
    subprocess in megabytes (Linux only; ``None`` on other platforms or
    when :meth:`PerformanceRecorder.monitor_process` is not called).
  - ``throughput_mbs`` – input-data throughput in MB/s (only set when
    :meth:`PerformanceRecorder.set_throughput` is called with a positive
    ``input_bytes`` value).

Typical usage inside a law task ``run()`` method::

    from performance_recorder import PerformanceRecorder

    with PerformanceRecorder("MyTask[branch=3]") as rec:
        # ... do the work ...
        rec.set_throughput(input_bytes=total_bytes)
    rec.save("/path/to/my_output.perf.json")

For subprocesses, start memory monitoring *after* the process has been
launched::

    with PerformanceRecorder("analysis_job") as rec:
        proc = subprocess.Popen(cmd, shell=True, ...)
        rec.monitor_process(proc.pid)
        proc.communicate()
        rec.set_throughput(input_bytes=job_input_bytes)
    rec.save(os.path.join(job_dir, "job.perf.json"))
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional


# ---------------------------------------------------------------------------
# Internal helper: background RAM-monitoring thread
# ---------------------------------------------------------------------------

class _SubprocessMemoryMonitor:
    """Poll ``/proc/{pid}/status`` and its direct children for peak RSS.

    Started as a daemon thread; monitoring stops when :meth:`stop` is called.
    Only functional on Linux where the ``/proc`` filesystem is available.

    Parameters
    ----------
    pid:
        PID of the process to monitor (typically a shell wrapper launched
        with ``shell=True``).
    poll_interval:
        Seconds between memory samples.  Defaults to 0.25 s.
    """

    def __init__(self, pid: int, poll_interval: float = 0.25) -> None:
        self._pid = pid
        self._interval = poll_interval
        self.peak_rss_kb: int = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> None:
        """Start the background polling thread."""
        self._thread.start()

    def stop(self) -> None:
        """Signal the polling thread to stop and wait for it to finish."""
        self._stop.set()
        self._thread.join(timeout=max(self._interval * 4, 2.0))

    # ------------------------------------------------------------------ internals

    def _rss_kb(self, pid: int) -> int:
        """Return the current RSS of *pid* in kibibytes, or 0 on failure."""
        try:
            with open(f"/proc/{pid}/status") as fh:
                for line in fh:
                    if line.startswith("VmRSS:"):
                        return int(line.split()[1])
        except (OSError, ValueError):
            pass
        return 0

    def _direct_children(self, pid: int) -> list[int]:
        """Return the list of direct child PIDs of *pid*, or [] on failure."""
        try:
            with open(f"/proc/{pid}/task/{pid}/children") as fh:
                return [int(p) for p in fh.read().split() if p.strip()]
        except (OSError, ValueError):
            return []

    def _run(self) -> None:
        while not self._stop.is_set():
            total = self._rss_kb(self._pid)
            for child_pid in self._direct_children(self._pid):
                total += self._rss_kb(child_pid)
            if total > self.peak_rss_kb:
                self.peak_rss_kb = total
            self._stop.wait(self._interval)


# ---------------------------------------------------------------------------
# Public API: PerformanceRecorder
# ---------------------------------------------------------------------------

class PerformanceRecorder:
    """Context manager that records execution performance metrics.

    Captures wall-clock time, peak subprocess RSS (Linux only), and
    optionally input-data throughput.

    Attributes
    ----------
    task_name : str
        Descriptive label included in all serialised outputs.
    start_time : str or None
        ISO 8601 UTC timestamp at context entry.
    end_time : str or None
        ISO 8601 UTC timestamp at context exit.
    wall_time_s : float or None
        Elapsed wall-clock seconds between context entry and exit.
    peak_rss_mb : float or None
        Peak resident set size in MB, as measured by
        :meth:`monitor_process`.  ``None`` when not monitored.
    throughput_mbs : float or None
        Input-data throughput in MB/s, set by :meth:`set_throughput`.
        ``None`` when not computed.

    Examples
    --------
    Basic timing only::

        with PerformanceRecorder("BuildSubmission") as rec:
            _do_build_work()
        rec.save("/shared/build.perf.json")

    Subprocess with memory monitoring::

        with PerformanceRecorder("analysis_job:job_7") as rec:
            proc = subprocess.Popen(cmd, shell=True)
            rec.monitor_process(proc.pid)
            proc.communicate()
            rec.set_throughput(input_bytes=estimate_job_input_bytes(job_dir))
        rec.save(os.path.join(job_dir, "job.perf.json"))
    """

    def __init__(self, task_name: str) -> None:
        self.task_name: str = task_name
        self.start_time: Optional[str] = None
        self.end_time: Optional[str] = None
        self.wall_time_s: Optional[float] = None
        self.peak_rss_mb: Optional[float] = None
        self.throughput_mbs: Optional[float] = None
        self._t0: float = 0.0
        self._monitor: Optional[_SubprocessMemoryMonitor] = None

    # ------------------------------------------------------------------ context protocol

    def __enter__(self) -> "PerformanceRecorder":
        self._t0 = time.perf_counter()
        self.start_time = datetime.now(timezone.utc).isoformat()
        return self

    def __exit__(self, *_args) -> bool:
        self.wall_time_s = round(time.perf_counter() - self._t0, 3)
        self.end_time = datetime.now(timezone.utc).isoformat()
        if self._monitor is not None:
            self._monitor.stop()
            rss_kb = self._monitor.peak_rss_kb
            if rss_kb > 0:
                self.peak_rss_mb = round(rss_kb / 1024.0, 1)
        return False  # never suppress exceptions

    # ------------------------------------------------------------------ public helpers

    def monitor_process(self, pid: int, poll_interval: float = 0.25) -> None:
        """Start background RAM monitoring for *pid* and its direct children.

        Must be called *inside* the ``with`` block after the subprocess has
        been launched.  Monitoring stops automatically when the context exits.

        Only effective on Linux where the ``/proc`` filesystem is available.
        On other platforms this method is a no-op.

        Parameters
        ----------
        pid:
            PID of the process to monitor.
        poll_interval:
            Seconds between memory samples.  Defaults to 0.25 s.
        """
        if sys.platform != "linux":
            return
        self._monitor = _SubprocessMemoryMonitor(pid, poll_interval)
        self._monitor.start()

    def set_throughput(self, input_bytes: int) -> None:
        """Compute and store processing throughput.

        The throughput is calculated as ``input_bytes / wall_time_s`` and
        stored in :attr:`throughput_mbs` (MB/s).

        Must be called while the context is still active (before
        ``__exit__`` has run) or immediately after if :attr:`wall_time_s`
        has already been set.

        Parameters
        ----------
        input_bytes:
            Total size of the input data processed, in bytes.  When zero
            or negative, no throughput is recorded.
        """
        wall = (
            self.wall_time_s
            if self.wall_time_s is not None
            else time.perf_counter() - self._t0
        )
        if input_bytes > 0 and wall > 0.0:
            self.throughput_mbs = round(
                (input_bytes / (1024.0 * 1024.0)) / wall, 3
            )

    # ------------------------------------------------------------------ serialisation

    def to_dict(self) -> dict:
        """Return a JSON-serialisable dictionary of all recorded metrics."""
        return {
            "task_name": self.task_name,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "wall_time_s": self.wall_time_s,
            "peak_rss_mb": self.peak_rss_mb,
            "throughput_mbs": self.throughput_mbs,
        }

    def save(self, path: str) -> None:
        """Write metrics to *path* as a JSON file.

        Parent directories are created automatically.  The file ends with a
        trailing newline so it can be concatenated and diff'ed cleanly.

        Parameters
        ----------
        path:
            Destination file path for the JSON metrics.
        """
        parent = os.path.dirname(os.path.abspath(path))
        os.makedirs(parent, exist_ok=True)
        with open(path, "w") as fh:
            json.dump(self.to_dict(), fh, indent=2)
            fh.write("\n")


# ---------------------------------------------------------------------------
# Convenience helpers
# ---------------------------------------------------------------------------

def estimate_job_input_bytes(job_dir: str) -> int:
    """Estimate the total size of input ROOT files for a prepared job.

    Reads ``submit_config.txt`` inside *job_dir* and looks for the
    ``fileList`` key.  Each file path that exists on the local filesystem
    contributes its size; remote XRootD paths are skipped.

    Parameters
    ----------
    job_dir:
        Path to the prepared job directory that contains
        ``submit_config.txt``.

    Returns
    -------
    int
        Total byte count of locally accessible input files.  Returns
        ``0`` when ``submit_config.txt`` is absent, malformed, or all
        files are remote.
    """
    config_path = os.path.join(job_dir, "submit_config.txt")
    try:
        with open(config_path) as fh:
            for line in fh:
                key, _, val = line.partition("=")
                if key.strip() == "fileList":
                    total = 0
                    for fpath in val.strip().split(","):
                        fpath = fpath.strip()
                        if fpath and os.path.isfile(fpath):
                            total += os.path.getsize(fpath)
                    return total
    except (OSError, ValueError):
        pass
    return 0


def perf_path_for(output_path: str) -> str:
    """Return the performance JSON path corresponding to *output_path*.

    Strips a ``.done``, ``.json``, or ``.txt`` suffix (if present) and
    appends ``.perf.json``.

    Parameters
    ----------
    output_path:
        Path to the task's primary output file.

    Returns
    -------
    str
        Path of the co-located ``*.perf.json`` file.

    Examples
    --------
    >>> perf_path_for("/jobs/job_3.done")
    '/jobs/job_3.perf.json'
    >>> perf_path_for("/jobs/sample_0.json")
    '/jobs/sample_0.perf.json'
    >>> perf_path_for("/run/submitted.txt")
    '/run/submitted.perf.json'
    """
    base = output_path
    for suffix in (".done", ".json", ".txt"):
        if base.endswith(suffix):
            base = base[: -len(suffix)]
            break
    return base + ".perf.json"
