#!/usr/bin/env python3
"""
Tests for the failure_handler module.

Tests cover:
  * FailureCategory enum values
  * classify_failure – exception-type-based and pattern-based classification
  * RetryPolicy – delay calculation
  * DEFAULT_RETRY_POLICIES – presence and reasonable values
  * FailureRecord – creation and field defaults
  * DiagnosticSummary – add, report, by_category, failed_branches
  * run_with_retries – success on first try, retry logic, exhaustion
"""

from __future__ import annotations

import os
import sys
import unittest

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR   = os.path.join(_REPO_ROOT, "law")
for _p in (_LAW_DIR,):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from failure_handler import (
    DEFAULT_RETRY_POLICIES,
    DiagnosticSummary,
    FailureCategory,
    FailureRecord,
    RetryPolicy,
    classify_failure,
    run_with_retries,
)


# ===========================================================================
# FailureCategory
# ===========================================================================

class TestFailureCategory(unittest.TestCase):
    def test_all_categories_exist(self):
        expected = {
            "transient_io", "executor", "configuration",
            "corrupted_input", "analysis_crash", "unknown",
        }
        actual = {cat.value for cat in FailureCategory}
        self.assertEqual(actual, expected)

    def test_enum_members_count(self):
        self.assertEqual(len(FailureCategory), 6)


# ===========================================================================
# classify_failure – exception-type-based
# ===========================================================================

class TestClassifyFailureByExceptionType(unittest.TestCase):
    def test_file_not_found_is_configuration(self):
        exc = FileNotFoundError("no such file")
        self.assertEqual(classify_failure(exc), FailureCategory.CONFIGURATION)

    def test_permission_error_is_configuration(self):
        exc = PermissionError("permission denied")
        self.assertEqual(classify_failure(exc), FailureCategory.CONFIGURATION)

    def test_os_error_is_transient_io(self):
        exc = OSError("some OS error")
        self.assertEqual(classify_failure(exc), FailureCategory.TRANSIENT_IO)

    def test_connection_error_is_transient_io(self):
        exc = ConnectionError("connection refused")
        self.assertEqual(classify_failure(exc), FailureCategory.TRANSIENT_IO)

    def test_timeout_error_is_transient_io(self):
        exc = TimeoutError("timed out")
        self.assertEqual(classify_failure(exc), FailureCategory.TRANSIENT_IO)

    def test_memory_error_is_executor(self):
        exc = MemoryError()
        self.assertEqual(classify_failure(exc), FailureCategory.EXECUTOR)


# ===========================================================================
# classify_failure – pattern-based (via RuntimeError + stderr)
# ===========================================================================

class TestClassifyFailureByPattern(unittest.TestCase):
    def _classify(self, msg="", stderr=""):
        return classify_failure(RuntimeError(msg), stderr=stderr)

    # --- Transient I/O ---
    def test_xrootd_in_message(self):
        self.assertEqual(self._classify(msg="XRootD connection error"), FailureCategory.TRANSIENT_IO)

    def test_connection_refused_in_stderr(self):
        self.assertEqual(self._classify(stderr="connection refused by host"), FailureCategory.TRANSIENT_IO)

    def test_io_error_in_message(self):
        self.assertEqual(self._classify(msg="I/O error reading file"), FailureCategory.TRANSIENT_IO)

    def test_network_unreachable(self):
        self.assertEqual(self._classify(stderr="network unreachable"), FailureCategory.TRANSIENT_IO)

    def test_cvmfs_in_stderr(self):
        self.assertEqual(self._classify(stderr="CVMFS mount failed"), FailureCategory.TRANSIENT_IO)

    def test_temporary_failure(self):
        self.assertEqual(self._classify(stderr="Temporary failure in name resolution"), FailureCategory.TRANSIENT_IO)

    # --- Executor ---
    def test_worker_lost(self):
        self.assertEqual(self._classify(msg="Worker lost during job"), FailureCategory.EXECUTOR)

    def test_oom_kill(self):
        self.assertEqual(self._classify(stderr="OOM kill signal 9"), FailureCategory.EXECUTOR)

    def test_job_held(self):
        self.assertEqual(self._classify(msg="job held by condor"), FailureCategory.EXECUTOR)

    def test_memory_exceeded(self):
        self.assertEqual(self._classify(stderr="memory limit exceeded"), FailureCategory.EXECUTOR)

    def test_preempt(self):
        self.assertEqual(self._classify(stderr="preempted by higher priority job"), FailureCategory.EXECUTOR)

    # --- Configuration ---
    def test_config_error_in_message(self):
        self.assertEqual(self._classify(msg="Configuration error: missing key"), FailureCategory.CONFIGURATION)

    def test_submit_config_not_found(self):
        self.assertEqual(self._classify(msg="submit_config.txt not found"), FailureCategory.CONFIGURATION)

    def test_parse_error(self):
        self.assertEqual(self._classify(stderr="parse error at line 42"), FailureCategory.CONFIGURATION)

    def test_yaml_error(self):
        self.assertEqual(self._classify(stderr="yaml error: invalid syntax"), FailureCategory.CONFIGURATION)

    def test_invalid_parameter(self):
        self.assertEqual(self._classify(msg="invalid parameter: foo=bar"), FailureCategory.CONFIGURATION)

    # --- Corrupted input ---
    def test_root_file_corrupt(self):
        self.assertEqual(self._classify(stderr="TFile: corrupt file"), FailureCategory.CORRUPTED_INPUT)

    def test_checksum_mismatch(self):
        self.assertEqual(self._classify(msg="checksum mismatch"), FailureCategory.CORRUPTED_INPUT)

    def test_not_a_root_file(self):
        self.assertEqual(self._classify(stderr="Not a ROOT file"), FailureCategory.CORRUPTED_INPUT)

    def test_premature_eof(self):
        self.assertEqual(self._classify(stderr="premature EOF"), FailureCategory.CORRUPTED_INPUT)

    def test_decompression_error(self):
        self.assertEqual(self._classify(msg="decompression error in block"), FailureCategory.CORRUPTED_INPUT)

    # --- Analysis crash ---
    def test_segfault(self):
        self.assertEqual(self._classify(stderr="Segmentation fault"), FailureCategory.ANALYSIS_CRASH)

    def test_abort(self):
        self.assertEqual(self._classify(stderr="Aborted (core dumped)"), FailureCategory.ANALYSIS_CRASH)

    def test_assertion_failed(self):
        self.assertEqual(self._classify(msg="Assertion failed: x > 0"), FailureCategory.ANALYSIS_CRASH)

    def test_bad_alloc(self):
        self.assertEqual(self._classify(stderr="std::bad_alloc"), FailureCategory.ANALYSIS_CRASH)

    def test_terminate_called(self):
        self.assertEqual(self._classify(stderr="terminate called after throwing an instance"), FailureCategory.ANALYSIS_CRASH)

    def test_stack_overflow(self):
        self.assertEqual(self._classify(stderr="stack overflow detected"), FailureCategory.ANALYSIS_CRASH)

    # --- Unknown ---
    def test_unknown_generic_runtime_error(self):
        self.assertEqual(self._classify(msg="some mysterious error 42"), FailureCategory.UNKNOWN)

    def test_unknown_empty_message(self):
        self.assertEqual(self._classify(msg="", stderr=""), FailureCategory.UNKNOWN)

    # --- Analysis crash takes priority over transient I/O ---
    def test_crash_priority_over_io(self):
        # stderr mentions both xrootd and segfault; crash should win
        self.assertEqual(
            self._classify(stderr="xrootd error\nSegmentation fault"),
            FailureCategory.ANALYSIS_CRASH,
        )


# ===========================================================================
# RetryPolicy
# ===========================================================================

class TestRetryPolicy(unittest.TestCase):
    def test_delay_attempt_1_equals_base_delay_without_jitter(self):
        policy = RetryPolicy(max_retries=3, base_delay=10.0, jitter=False)
        self.assertAlmostEqual(policy.delay_for_attempt(1), 10.0)

    def test_delay_exponential_backoff(self):
        policy = RetryPolicy(max_retries=5, base_delay=5.0, backoff_factor=3.0, jitter=False)
        self.assertAlmostEqual(policy.delay_for_attempt(1), 5.0)
        self.assertAlmostEqual(policy.delay_for_attempt(2), 15.0)
        self.assertAlmostEqual(policy.delay_for_attempt(3), 45.0)

    def test_delay_with_jitter_is_greater_or_equal_base(self):
        policy = RetryPolicy(max_retries=3, base_delay=10.0, jitter=True)
        for attempt in range(1, 4):
            delay = policy.delay_for_attempt(attempt)
            base = 10.0 * (policy.backoff_factor ** (attempt - 1))
            self.assertGreaterEqual(delay, base)

    def test_constant_delay_with_backoff_factor_one(self):
        policy = RetryPolicy(max_retries=3, base_delay=7.0, backoff_factor=1.0, jitter=False)
        for attempt in range(1, 4):
            self.assertAlmostEqual(policy.delay_for_attempt(attempt), 7.0)


# ===========================================================================
# DEFAULT_RETRY_POLICIES
# ===========================================================================

class TestDefaultRetryPolicies(unittest.TestCase):
    def test_all_categories_have_policy(self):
        for cat in FailureCategory:
            self.assertIn(cat, DEFAULT_RETRY_POLICIES, f"Missing policy for {cat}")

    def test_transient_io_allows_many_retries(self):
        policy = DEFAULT_RETRY_POLICIES[FailureCategory.TRANSIENT_IO]
        self.assertGreaterEqual(policy.max_retries, 3)

    def test_configuration_retries_low(self):
        policy = DEFAULT_RETRY_POLICIES[FailureCategory.CONFIGURATION]
        self.assertLessEqual(policy.max_retries, 2)

    def test_all_base_delays_positive(self):
        for cat, policy in DEFAULT_RETRY_POLICIES.items():
            self.assertGreater(policy.base_delay, 0, f"base_delay must be positive for {cat}")


# ===========================================================================
# FailureRecord
# ===========================================================================

class TestFailureRecord(unittest.TestCase):
    def test_basic_creation(self):
        rec = FailureRecord(
            branch_num=5,
            attempt=2,
            category=FailureCategory.TRANSIENT_IO,
            message="XRootD timeout",
        )
        self.assertEqual(rec.branch_num, 5)
        self.assertEqual(rec.attempt, 2)
        self.assertEqual(rec.category, FailureCategory.TRANSIENT_IO)
        self.assertEqual(rec.message, "XRootD timeout")
        self.assertEqual(rec.traceback_text, "")

    def test_timestamp_is_set_automatically(self):
        rec = FailureRecord(
            branch_num=0, attempt=1,
            category=FailureCategory.UNKNOWN, message="x",
        )
        self.assertIsInstance(rec.timestamp, str)
        self.assertTrue(rec.timestamp.endswith("Z"))

    def test_optional_traceback(self):
        rec = FailureRecord(
            branch_num=1, attempt=1,
            category=FailureCategory.ANALYSIS_CRASH,
            message="crash",
            traceback_text="Traceback (most recent call last):\n  ...",
        )
        self.assertIn("Traceback", rec.traceback_text)


# ===========================================================================
# DiagnosticSummary
# ===========================================================================

class TestDiagnosticSummary(unittest.TestCase):
    def _make_record(self, branch=0, attempt=1, category=FailureCategory.UNKNOWN, msg="err"):
        return FailureRecord(
            branch_num=branch,
            attempt=attempt,
            category=category,
            message=msg,
        )

    def test_empty_summary_report(self):
        s = DiagnosticSummary()
        report = s.report()
        self.assertIn("no failures", report)

    def test_add_and_total_failures(self):
        s = DiagnosticSummary()
        s.add(self._make_record(branch=0))
        s.add(self._make_record(branch=1))
        self.assertEqual(s.total_failures(), 2)

    def test_failed_branches(self):
        s = DiagnosticSummary()
        s.add(self._make_record(branch=3))
        s.add(self._make_record(branch=7))
        s.add(self._make_record(branch=3))  # duplicate branch, different attempt
        self.assertEqual(s.failed_branches(), {3, 7})

    def test_by_category(self):
        s = DiagnosticSummary()
        s.add(self._make_record(category=FailureCategory.TRANSIENT_IO))
        s.add(self._make_record(category=FailureCategory.TRANSIENT_IO))
        s.add(self._make_record(category=FailureCategory.EXECUTOR))
        by_cat = s.by_category()
        self.assertEqual(len(by_cat[FailureCategory.TRANSIENT_IO]), 2)
        self.assertEqual(len(by_cat[FailureCategory.EXECUTOR]), 1)
        self.assertNotIn(FailureCategory.CONFIGURATION, by_cat)

    def test_report_contains_category(self):
        s = DiagnosticSummary()
        s.add(self._make_record(category=FailureCategory.CORRUPTED_INPUT, msg="bad root"))
        report = s.report()
        self.assertIn("corrupted_input", report)
        self.assertIn("bad root", report)

    def test_report_contains_branch_and_attempt(self):
        s = DiagnosticSummary()
        s.add(self._make_record(branch=42, attempt=3))
        report = s.report()
        self.assertIn("42", report)
        self.assertIn("3", report)

    def test_records_property_is_copy(self):
        s = DiagnosticSummary()
        s.add(self._make_record())
        records = s.records
        records.clear()
        # Clearing the copy must not affect the summary
        self.assertEqual(s.total_failures(), 1)


# ===========================================================================
# run_with_retries
# ===========================================================================

class TestRunWithRetries(unittest.TestCase):
    def test_success_on_first_try(self):
        calls = []

        def ok_fn():
            calls.append(1)
            return "done"

        result = run_with_retries(fn=ok_fn, args=[], sleep_fn=lambda _: None)
        self.assertEqual(result, "done")
        self.assertEqual(len(calls), 1)

    def test_success_after_one_retry(self):
        attempts = [0]

        def flaky_fn():
            attempts[0] += 1
            if attempts[0] < 2:
                raise RuntimeError("XRootD connection refused")
            return "done"

        summary = DiagnosticSummary()
        result = run_with_retries(
            fn=flaky_fn,
            args=[],
            branch_num=5,
            summary=summary,
            sleep_fn=lambda _: None,
        )
        self.assertEqual(result, "done")
        self.assertEqual(summary.total_failures(), 1)
        rec = summary.records[0]
        self.assertEqual(rec.branch_num, 5)
        self.assertEqual(rec.category, FailureCategory.TRANSIENT_IO)

    def test_retries_exhausted_raises(self):
        def always_fail():
            raise RuntimeError("OOM kill")

        summary = DiagnosticSummary()
        # Only 1 retry allowed for executor failures by default
        with self.assertRaises(RuntimeError):
            run_with_retries(
                fn=always_fail,
                args=[],
                summary=summary,
                sleep_fn=lambda _: None,
            )
        # Should have (1 initial + N retries) records
        self.assertGreater(summary.total_failures(), 0)

    def test_correct_number_of_attempts(self):
        attempts = [0]

        def always_fail():
            attempts[0] += 1
            raise ValueError("configuration error: invalid parameter")

        summary = DiagnosticSummary()
        with self.assertRaises(ValueError):
            run_with_retries(
                fn=always_fail,
                args=[],
                summary=summary,
                sleep_fn=lambda _: None,
            )
        # CONFIGURATION max_retries=1 → 1 initial + 1 retry = 2 total
        policy = DEFAULT_RETRY_POLICIES[FailureCategory.CONFIGURATION]
        expected_attempts = policy.max_retries + 1
        self.assertEqual(attempts[0], expected_attempts)
        self.assertEqual(summary.total_failures(), expected_attempts)

    def test_custom_policies_respected(self):
        attempts = [0]

        def always_fail():
            attempts[0] += 1
            raise RuntimeError("XRootD error")

        custom_policies = {
            cat: RetryPolicy(max_retries=0, base_delay=1.0)
            for cat in FailureCategory
        }
        summary = DiagnosticSummary()
        with self.assertRaises(RuntimeError):
            run_with_retries(
                fn=always_fail,
                args=[],
                summary=summary,
                policies=custom_policies,
                sleep_fn=lambda _: None,
            )
        # max_retries=0 → only 1 attempt
        self.assertEqual(attempts[0], 1)

    def test_kwargs_passed_through(self):
        def fn_with_kwargs(x, y=0):
            return x + y

        result = run_with_retries(
            fn=fn_with_kwargs,
            args=[10],
            kwargs={"y": 5},
            sleep_fn=lambda _: None,
        )
        self.assertEqual(result, 15)

    def test_sleep_called_between_retries(self):
        delays_seen = []
        attempts = [0]

        def flaky():
            attempts[0] += 1
            if attempts[0] < 3:
                raise RuntimeError("connection refused")
            return "ok"

        run_with_retries(
            fn=flaky,
            args=[],
            sleep_fn=delays_seen.append,
        )
        # Two failures → two sleeps
        self.assertEqual(len(delays_seen), 2)
        # Delays should be non-negative
        for d in delays_seen:
            self.assertGreaterEqual(d, 0)

    def test_auto_creates_summary_if_none(self):
        """run_with_retries should not crash when summary=None."""
        result = run_with_retries(fn=lambda: "x", args=[], sleep_fn=lambda _: None)
        self.assertEqual(result, "x")

    def test_stderr_extracted_from_runtime_error(self):
        """Failure category is inferred from the embedded stderr block."""
        summary = DiagnosticSummary()

        def crash():
            raise RuntimeError(
                "Analysis job failed (exit 139) in '/tmp/job'.\n"
                "stderr:\nSegmentation fault"
            )

        with self.assertRaises(RuntimeError):
            run_with_retries(
                fn=crash,
                args=[],
                summary=summary,
                sleep_fn=lambda _: None,
            )
        self.assertEqual(summary.records[0].category, FailureCategory.ANALYSIS_CRASH)

    def test_failure_record_attempt_increments(self):
        attempts = [0]

        def always_fail():
            attempts[0] += 1
            raise RuntimeError("xrootd error")

        summary = DiagnosticSummary()
        with self.assertRaises(RuntimeError):
            run_with_retries(
                fn=always_fail,
                args=[],
                branch_num=99,
                summary=summary,
                sleep_fn=lambda _: None,
            )
        attempt_numbers = [r.attempt for r in summary.records]
        self.assertEqual(attempt_numbers, list(range(1, len(attempt_numbers) + 1)))


# ===========================================================================
# workflow_executors re-exports failure_handler symbols
# ===========================================================================

class TestWorkflowExecutorsExports(unittest.TestCase):
    def test_failure_category_imported_in_executors(self):
        import workflow_executors
        self.assertTrue(hasattr(workflow_executors, "FailureCategory"))

    def test_retry_policy_imported_in_executors(self):
        import workflow_executors
        self.assertTrue(hasattr(workflow_executors, "RetryPolicy"))

    def test_diagnostic_summary_imported_in_executors(self):
        import workflow_executors
        self.assertTrue(hasattr(workflow_executors, "DiagnosticSummary"))

    def test_default_retry_policies_imported_in_executors(self):
        import workflow_executors
        self.assertTrue(hasattr(workflow_executors, "DEFAULT_RETRY_POLICIES"))

    def test_dask_workflow_has_retry_policies_attr(self):
        import workflow_executors
        self.assertTrue(hasattr(workflow_executors.DaskWorkflow, "dask_retry_policies"))

    def test_dask_workflow_retry_policies_match_defaults(self):
        import workflow_executors
        self.assertIs(
            workflow_executors.DaskWorkflow.dask_retry_policies,
            DEFAULT_RETRY_POLICIES,
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
