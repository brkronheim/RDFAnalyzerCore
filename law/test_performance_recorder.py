#!/usr/bin/env python3
"""
Tests for performance_recorder.py

These tests verify the PerformanceRecorder context manager, the
_SubprocessMemoryMonitor helper, and the convenience utilities
(estimate_job_input_bytes, perf_path_for).  They do NOT require LAW, Luigi,
or any HEP-specific software and run as plain unittest tests.
"""

from __future__ import annotations

import json
import os
import stat
import sys
import tempfile
import time
import unittest
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup – make law/ importable
# ---------------------------------------------------------------------------
_LAW_DIR = os.path.dirname(os.path.abspath(__file__))
if _LAW_DIR not in sys.path:
    sys.path.insert(0, _LAW_DIR)

from performance_recorder import (  # noqa: E402
    PerformanceRecorder,
    _SubprocessMemoryMonitor,
    estimate_job_input_bytes,
    perf_path_for,
)


# ===========================================================================
# PerformanceRecorder – basic context manager behaviour
# ===========================================================================

class TestPerformanceRecorderBasic(unittest.TestCase):
    """Basic context manager contract: timing fields are always populated."""

    def test_wall_time_recorded(self):
        """wall_time_s must be set and positive after context exits."""
        with PerformanceRecorder("test") as rec:
            time.sleep(0.05)

        self.assertIsNotNone(rec.wall_time_s)
        self.assertGreater(rec.wall_time_s, 0.0)

    def test_wall_time_at_least_sleep_duration(self):
        """wall_time_s ≥ the sleep duration (allowing for platform jitter)."""
        with PerformanceRecorder("sleep_test") as rec:
            time.sleep(0.1)

        self.assertGreaterEqual(rec.wall_time_s, 0.08)

    def test_start_end_times_set(self):
        """start_time and end_time are ISO-8601 UTC strings."""
        with PerformanceRecorder("ts_test") as rec:
            pass

        self.assertIsNotNone(rec.start_time)
        self.assertIsNotNone(rec.end_time)
        # Both should contain a timezone offset or 'Z' (isoformat)
        self.assertIn("T", rec.start_time)
        self.assertIn("T", rec.end_time)

    def test_task_name_preserved(self):
        """task_name attribute matches the constructor argument."""
        with PerformanceRecorder("MyTask[branch=7]") as rec:
            pass

        self.assertEqual(rec.task_name, "MyTask[branch=7]")

    def test_peak_rss_none_without_monitoring(self):
        """peak_rss_mb is None when monitor_process() was never called."""
        with PerformanceRecorder("no_monitor") as rec:
            time.sleep(0.01)

        self.assertIsNone(rec.peak_rss_mb)

    def test_throughput_none_without_call(self):
        """throughput_mbs is None when set_throughput() was never called."""
        with PerformanceRecorder("no_throughput") as rec:
            pass

        self.assertIsNone(rec.throughput_mbs)

    def test_exception_not_suppressed(self):
        """Exceptions raised inside the context are propagated normally."""
        with self.assertRaises(ValueError):
            with PerformanceRecorder("exc_test"):
                raise ValueError("deliberate error")

    def test_wall_time_set_even_after_exception(self):
        """wall_time_s is set even when the context exits via an exception."""
        try:
            with PerformanceRecorder("exc_timing") as rec:
                time.sleep(0.02)
                raise RuntimeError("deliberate")
        except RuntimeError:
            pass

        self.assertIsNotNone(rec.wall_time_s)
        self.assertGreater(rec.wall_time_s, 0.0)

    def test_reuse_same_recorder(self):
        """Re-entering the same recorder object resets timing fields."""
        with PerformanceRecorder("first_use") as rec:
            time.sleep(0.02)
        t1 = rec.wall_time_s

        with rec:
            time.sleep(0.05)
        t2 = rec.wall_time_s

        self.assertIsNotNone(t1)
        self.assertIsNotNone(t2)
        self.assertNotEqual(t1, t2)


# ===========================================================================
# PerformanceRecorder – to_dict / save / JSON round-trip
# ===========================================================================

class TestPerformanceRecorderSerialisation(unittest.TestCase):
    """Serialisation: to_dict() and save() produce well-formed JSON."""

    def _make_rec(self, **kwargs):
        rec = PerformanceRecorder("serialise_test")
        with rec:
            pass
        for k, v in kwargs.items():
            setattr(rec, k, v)
        return rec

    def test_to_dict_keys(self):
        """to_dict() must contain all expected keys."""
        rec = self._make_rec()
        d = rec.to_dict()
        required = {"task_name", "start_time", "end_time", "wall_time_s",
                    "peak_rss_mb", "throughput_mbs"}
        self.assertEqual(set(d.keys()), required)

    def test_to_dict_values_serialisable(self):
        """to_dict() must be JSON-serialisable without errors."""
        rec = self._make_rec(peak_rss_mb=512.0, throughput_mbs=1.5)
        d = rec.to_dict()
        json_str = json.dumps(d)  # should not raise
        loaded = json.loads(json_str)
        self.assertEqual(loaded["peak_rss_mb"], 512.0)
        self.assertEqual(loaded["throughput_mbs"], 1.5)

    def test_save_creates_file(self):
        """save() must create a JSON file at the specified path."""
        rec = self._make_rec()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "test.perf.json")
            rec.save(path)
            self.assertTrue(os.path.isfile(path))

    def test_save_creates_parent_directories(self):
        """save() creates intermediate directories automatically."""
        rec = self._make_rec()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "sub", "dir", "out.perf.json")
            rec.save(path)
            self.assertTrue(os.path.isfile(path))

    def test_save_valid_json(self):
        """The file written by save() must be valid JSON."""
        rec = self._make_rec()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "rec.json")
            rec.save(path)
            with open(path) as fh:
                data = json.load(fh)
            self.assertIn("wall_time_s", data)

    def test_save_ends_with_newline(self):
        """The file written by save() must end with a newline character."""
        rec = self._make_rec()
        with tempfile.TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "rec.json")
            rec.save(path)
            with open(path, "rb") as fh:
                content = fh.read()
            self.assertEqual(content[-1:], b"\n")

    def test_null_fields_serialised_as_null(self):
        """None fields become JSON null."""
        rec = self._make_rec()
        d = rec.to_dict()
        json_str = json.dumps(d)
        # peak_rss_mb and throughput_mbs should be null (None)
        loaded = json.loads(json_str)
        self.assertIsNone(loaded["peak_rss_mb"])
        self.assertIsNone(loaded["throughput_mbs"])


# ===========================================================================
# PerformanceRecorder – set_throughput
# ===========================================================================

class TestSetThroughput(unittest.TestCase):
    """set_throughput() correctly computes MB/s from input_bytes."""

    def test_throughput_computed_inside_context(self):
        """set_throughput() called inside context uses in-flight wall time."""
        with PerformanceRecorder("throughput_test") as rec:
            time.sleep(0.05)
            rec.set_throughput(input_bytes=1024 * 1024)  # 1 MiB

        # throughput should be roughly 1 MB / wall_time_s
        self.assertIsNotNone(rec.throughput_mbs)
        self.assertGreater(rec.throughput_mbs, 0.0)

    def test_throughput_computed_after_context(self):
        """set_throughput() called after context uses recorded wall_time_s."""
        with PerformanceRecorder("tp_after") as rec:
            time.sleep(0.05)

        rec.set_throughput(input_bytes=10 * 1024 * 1024)  # 10 MiB
        self.assertIsNotNone(rec.throughput_mbs)
        self.assertGreater(rec.throughput_mbs, 0.0)

    def test_zero_bytes_no_throughput(self):
        """set_throughput(0) must NOT set throughput_mbs."""
        with PerformanceRecorder("zero_tp") as rec:
            pass
        rec.set_throughput(input_bytes=0)
        self.assertIsNone(rec.throughput_mbs)

    def test_negative_bytes_no_throughput(self):
        """set_throughput(-1) must NOT set throughput_mbs."""
        with PerformanceRecorder("neg_tp") as rec:
            pass
        rec.set_throughput(input_bytes=-100)
        self.assertIsNone(rec.throughput_mbs)

    def test_throughput_value_reasonable(self):
        """10 MiB / 0.1s should give ~100 MB/s."""
        with PerformanceRecorder("tp_value") as rec:
            time.sleep(0.1)

        rec.set_throughput(input_bytes=10 * 1024 * 1024)
        # Allow wide tolerance: 50–500 MB/s on any reasonable hardware
        self.assertGreater(rec.throughput_mbs, 10.0)
        self.assertLess(rec.throughput_mbs, 1000.0)


# ===========================================================================
# PerformanceRecorder – monitor_process (Linux /proc only)
# ===========================================================================

class TestMonitorProcess(unittest.TestCase):
    """monitor_process() behaviour (graceful no-op on non-Linux)."""

    def test_monitor_process_noop_on_non_linux(self):
        """On non-Linux platforms monitor_process() should not raise."""
        orig = sys.platform
        try:
            sys.platform = "darwin"
            with PerformanceRecorder("noop_monitor") as rec:
                rec.monitor_process(os.getpid())
        finally:
            sys.platform = orig
        # peak_rss_mb should remain None (monitor was skipped)
        self.assertIsNone(rec.peak_rss_mb)

    @unittest.skipUnless(sys.platform == "linux", "Linux /proc required")
    def test_monitor_process_subprocess(self):
        """On Linux, a subprocess that allocates memory is tracked."""
        import subprocess

        with tempfile.TemporaryDirectory() as tmpdir:
            # A short script that allocates ~10 MB
            script = os.path.join(tmpdir, "alloc.py")
            Path(script).write_text(
                "import time\n"
                "data = bytearray(10 * 1024 * 1024)\n"
                "time.sleep(0.3)\n"
            )
            with PerformanceRecorder("mem_test") as rec:
                proc = subprocess.Popen(
                    [sys.executable, script],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                rec.monitor_process(proc.pid)
                proc.wait()

        # The monitor may or may not see the allocation depending on poll
        # timing, but peak_rss_mb should be non-None and positive.
        if rec.peak_rss_mb is not None:
            self.assertGreater(rec.peak_rss_mb, 0.0)


# ===========================================================================
# _SubprocessMemoryMonitor – internals
# ===========================================================================

class TestSubprocessMemoryMonitor(unittest.TestCase):
    """Unit tests for the background memory monitor helper."""

    def test_start_stop_no_process(self):
        """Monitor can be started/stopped even when the PID no longer exists."""
        monitor = _SubprocessMemoryMonitor(pid=999999999)
        monitor.start()
        time.sleep(0.1)
        monitor.stop()
        # peak_rss_kb should remain 0 (fake PID)
        self.assertEqual(monitor.peak_rss_kb, 0)

    @unittest.skipUnless(sys.platform == "linux", "Linux /proc required")
    def test_rss_own_process(self):
        """Polling our own PID returns a positive RSS value."""
        monitor = _SubprocessMemoryMonitor(pid=os.getpid(), poll_interval=0.05)
        monitor.start()
        time.sleep(0.2)
        monitor.stop()
        self.assertGreater(monitor.peak_rss_kb, 0)


# ===========================================================================
# estimate_job_input_bytes
# ===========================================================================

class TestEstimateJobInputBytes(unittest.TestCase):
    """estimate_job_input_bytes() returns correct local-file sizes."""

    def test_no_config_returns_zero(self):
        """Returns 0 when submit_config.txt is absent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertEqual(estimate_job_input_bytes(tmpdir), 0)

    def test_no_filelist_key_returns_zero(self):
        """Returns 0 when submit_config.txt has no fileList key."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "submit_config.txt")
            Path(cfg).write_text("batch=true\nsaveFile=out.root\n")
            self.assertEqual(estimate_job_input_bytes(tmpdir), 0)

    def test_local_files_summed(self):
        """Returns sum of sizes for local files that exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            f1 = os.path.join(tmpdir, "a.root")
            f2 = os.path.join(tmpdir, "b.root")
            Path(f1).write_bytes(b"X" * 1000)
            Path(f2).write_bytes(b"Y" * 2000)
            cfg = os.path.join(tmpdir, "submit_config.txt")
            Path(cfg).write_text(f"fileList={f1},{f2}\n")
            total = estimate_job_input_bytes(tmpdir)
            self.assertEqual(total, 3000)

    def test_remote_xrootd_files_ignored(self):
        """XRootD paths (not local files) contribute 0 to the total."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "submit_config.txt")
            Path(cfg).write_text(
                "fileList=root://xrootd-cms.infn.it//store/file1.root,"
                "root://xrootd-cms.infn.it//store/file2.root\n"
            )
            self.assertEqual(estimate_job_input_bytes(tmpdir), 0)

    def test_mixed_local_and_remote(self):
        """Only the local file contributes to the total."""
        with tempfile.TemporaryDirectory() as tmpdir:
            local_f = os.path.join(tmpdir, "local.root")
            Path(local_f).write_bytes(b"Z" * 500)
            cfg = os.path.join(tmpdir, "submit_config.txt")
            Path(cfg).write_text(
                f"fileList={local_f},root://redirector.cern.ch//remote.root\n"
            )
            self.assertEqual(estimate_job_input_bytes(tmpdir), 500)

    def test_empty_filelist_returns_zero(self):
        """Returns 0 when fileList value is empty."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cfg = os.path.join(tmpdir, "submit_config.txt")
            Path(cfg).write_text("fileList=\n")
            self.assertEqual(estimate_job_input_bytes(tmpdir), 0)


# ===========================================================================
# perf_path_for
# ===========================================================================

class TestPerfPathFor(unittest.TestCase):
    """perf_path_for() derives correct sibling .perf.json path."""

    def test_done_suffix(self):
        self.assertEqual(
            perf_path_for("/jobs/job_3.done"),
            "/jobs/job_3.perf.json",
        )

    def test_json_suffix(self):
        self.assertEqual(
            perf_path_for("/branch_outputs/sample_0.json"),
            "/branch_outputs/sample_0.perf.json",
        )

    def test_txt_suffix(self):
        self.assertEqual(
            perf_path_for("/run/submitted.txt"),
            "/run/submitted.perf.json",
        )

    def test_no_known_suffix(self):
        """Paths without a known suffix get .perf.json appended directly."""
        self.assertEqual(
            perf_path_for("/data/output"),
            "/data/output.perf.json",
        )

    def test_nested_path(self):
        self.assertEqual(
            perf_path_for("/condorSub_myRun/job_outputs/job_5.done"),
            "/condorSub_myRun/job_outputs/job_5.perf.json",
        )

    def test_preserves_directory(self):
        """The directory component of the path is preserved."""
        result = perf_path_for("/a/b/c/x.done")
        self.assertTrue(result.startswith("/a/b/c/"))


# ===========================================================================
# Integration: _run_analysis_job writes job.perf.json (no law required)
# ===========================================================================

try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestRunAnalysisJobPerformanceOutput(unittest.TestCase):
    """
    Verify that _run_analysis_job writes job.perf.json to the job directory
    when performance_recorder is importable.
    """

    def test_perf_file_written_on_success(self):
        """A successful job should produce job.perf.json in the job dir."""
        from workflow_executors import _run_analysis_job

        with tempfile.TemporaryDirectory() as tmpdir:
            # Minimal job setup
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text(
                "batch=true\n"
            )
            exe = os.path.join(tmpdir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)

            _run_analysis_job(exe_path=exe, job_dir=tmpdir)

            perf_path = os.path.join(tmpdir, "job.perf.json")
            self.assertTrue(
                os.path.isfile(perf_path),
                "job.perf.json should be written to the job directory",
            )

    def test_perf_file_valid_json(self):
        """job.perf.json must be valid JSON with the expected keys."""
        from workflow_executors import _run_analysis_job

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text(
                "batch=true\n"
            )
            exe = os.path.join(tmpdir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)

            _run_analysis_job(exe_path=exe, job_dir=tmpdir)

            perf_path = os.path.join(tmpdir, "job.perf.json")
            with open(perf_path) as fh:
                data = json.load(fh)

            self.assertIn("task_name", data)
            self.assertIn("wall_time_s", data)
            self.assertIn("start_time", data)
            self.assertIn("end_time", data)
            self.assertIn("peak_rss_mb", data)
            self.assertIn("throughput_mbs", data)

    def test_perf_wall_time_positive(self):
        """wall_time_s in the perf file should be a positive number."""
        from workflow_executors import _run_analysis_job

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text(
                "batch=true\n"
            )
            exe = os.path.join(tmpdir, "myexe.sh")
            Path(exe).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe, stat.S_IRWXU)

            _run_analysis_job(exe_path=exe, job_dir=tmpdir)

            with open(os.path.join(tmpdir, "job.perf.json")) as fh:
                data = json.load(fh)

            self.assertIsNotNone(data["wall_time_s"])
            self.assertGreater(data["wall_time_s"], 0.0)

    def test_perf_file_not_written_on_failure(self):
        """
        A failing job should NOT produce job.perf.json (exception is raised
        before the write, though this may vary by implementation).

        NOTE: The current implementation writes the file before re-raising.
        This test verifies that the exception IS raised and the return value
        is NOT the success string.
        """
        from workflow_executors import _run_analysis_job

        with tempfile.TemporaryDirectory() as tmpdir:
            Path(os.path.join(tmpdir, "submit_config.txt")).write_text(
                "batch=true\n"
            )
            exe = os.path.join(tmpdir, "bad.sh")
            Path(exe).write_text("#!/bin/sh\nexit 1\n")
            os.chmod(exe, stat.S_IRWXU)

            with self.assertRaises(RuntimeError):
                _run_analysis_job(exe_path=exe, job_dir=tmpdir)


if __name__ == "__main__":
    unittest.main(verbosity=2)
