"""
Failure classification and smart retry handling for RDFAnalyzerCore law workflows.

This module provides:

  FailureCategory     – Enum of failure categories (transient I/O, executor,
                        configuration, corrupted input, analysis crash, unknown).

  classify_failure    – Classify an exception (and optional stderr text) into a
                        FailureCategory by examining error messages and exception
                        types.

  RetryPolicy         – Dataclass describing how many times to retry and the
                        exponential-backoff parameters for a given category.

  DEFAULT_RETRY_POLICIES – Mapping from FailureCategory to a sensible default
                        RetryPolicy.

  FailureRecord       – Dataclass capturing one failure event (branch, attempt,
                        category, message, timestamp).

  DiagnosticSummary   – Accumulates FailureRecords and produces human-readable
                        and structured summaries.

  run_with_retries    – Convenience helper that executes a callable with
                        smart per-category retry logic, recording each attempt
                        in a DiagnosticSummary.

Usage
-----
::

    from failure_handler import (
        classify_failure,
        run_with_retries,
        DiagnosticSummary,
        DEFAULT_RETRY_POLICIES,
    )

    summary = DiagnosticSummary()

    result = run_with_retries(
        fn=my_job_callable,
        args=[arg1, arg2],
        branch_num=42,
        summary=summary,
        policies=DEFAULT_RETRY_POLICIES,
    )

    print(summary.report())
"""

from __future__ import annotations

import enum
import re
import time
import datetime
import random
import traceback
from dataclasses import dataclass, field
from typing import Any, Callable


# ---------------------------------------------------------------------------
# FailureCategory
# ---------------------------------------------------------------------------

class FailureCategory(enum.Enum):
    """Categories of job failures used to select the appropriate retry policy."""

    #: Temporary network or storage errors (XRootD, EOS, CVMFS I/O timeouts,
    #: connection refused, etc.).  Usually worth retrying several times.
    TRANSIENT_IO = "transient_io"

    #: Failures in the execution infrastructure (Dask worker crash, HTCondor
    #: hold, out-of-memory kill, preemption).  Worth retrying with a longer
    #: backoff.
    EXECUTOR = "executor"

    #: Errors caused by bad configuration files, missing parameters, or
    #: invalid submit_config.txt entries.  Retrying without fixing the config
    #: is unlikely to help; only one retry is attempted.
    CONFIGURATION = "configuration"

    #: The input data file(s) appear to be corrupt or incomplete (ROOT file
    #: errors, checksum mismatches, truncated files).  Not worth retrying
    #: unless the file can be re-staged from a different replica.
    CORRUPTED_INPUT = "corrupted_input"

    #: The analysis C++ executable crashed (segfault, assertion, uncaught
    #: exception inside user code).  Retrying rarely helps unless the crash
    #: was due to a race condition; retry once.
    ANALYSIS_CRASH = "analysis_crash"

    #: Unclassified / unexpected failure.  Apply a conservative retry policy.
    UNKNOWN = "unknown"


# ---------------------------------------------------------------------------
# RetryPolicy
# ---------------------------------------------------------------------------

@dataclass
class RetryPolicy:
    """
    Retry behaviour for a specific :class:`FailureCategory`.

    Parameters
    ----------
    max_retries:
        Maximum number of *additional* attempts after the first failure
        (0 = no retries).
    base_delay:
        Initial delay in seconds before the first retry.
    backoff_factor:
        Multiplicative factor applied to *base_delay* after each retry
        (exponential backoff).  Set to 1.0 for constant delays.
    jitter:
        When ``True``, add a random fraction of the current delay to avoid
        thundering-herd effects when many branches retry simultaneously.
    """

    max_retries: int
    base_delay: float
    backoff_factor: float = 2.0
    jitter: bool = True

    def delay_for_attempt(self, attempt: int) -> float:
        """
        Return the delay (seconds) before retry number *attempt* (1-based).

        Example: attempt=1 → base_delay, attempt=2 → base_delay * backoff_factor, …
        """
        delay = self.base_delay * (self.backoff_factor ** (attempt - 1))
        if self.jitter:
            delay += random.uniform(0, delay * 0.25)
        return delay


# ---------------------------------------------------------------------------
# Default policies
# ---------------------------------------------------------------------------

#: Sensible default :class:`RetryPolicy` for each :class:`FailureCategory`.
DEFAULT_RETRY_POLICIES: dict[FailureCategory, RetryPolicy] = {
    FailureCategory.TRANSIENT_IO:    RetryPolicy(max_retries=5, base_delay=10.0, backoff_factor=2.0),
    FailureCategory.EXECUTOR:        RetryPolicy(max_retries=3, base_delay=30.0, backoff_factor=2.0),
    FailureCategory.CONFIGURATION:   RetryPolicy(max_retries=1, base_delay=5.0,  backoff_factor=1.0),
    FailureCategory.CORRUPTED_INPUT: RetryPolicy(max_retries=1, base_delay=5.0,  backoff_factor=1.0),
    FailureCategory.ANALYSIS_CRASH:  RetryPolicy(max_retries=1, base_delay=5.0,  backoff_factor=1.0),
    FailureCategory.UNKNOWN:         RetryPolicy(max_retries=2, base_delay=15.0, backoff_factor=2.0),
}


# ---------------------------------------------------------------------------
# classify_failure
# ---------------------------------------------------------------------------

# Patterns associated with each category, checked against the combined string
# ``str(exc) + "\n" + stderr``.
_TRANSIENT_IO_PATTERNS: tuple[str, ...] = (
    r"connection\s+refused",
    r"connection\s+timed?\s*out",
    r"name\s+or\s+service\s+not\s+known",
    r"no\s+route\s+to\s+host",
    r"xrootd",
    r"xrdfs",
    r"eos\s+error",
    r"stageout\s+failed",
    r"i/?o\s+error",
    r"errno\s*=?\s*5\b",           # Linux EIO
    r"errno\s*=?\s*11\b",          # Linux EAGAIN
    r"errno\s*=?\s*110\b",         # Linux ETIMEDOUT
    r"errno\s*=?\s*111\b",         # Linux ECONNREFUSED
    r"network\s+unreachable",
    r"temporary\s+failure",
    r"resource\s+temporarily\s+unavailable",
    r"cvmfs",
    r"nfs\s+",
    r"afs\s+",
    r"ssh.*failed",
    r"gfal",
    r"srm\s+error",
)

_EXECUTOR_PATTERNS: tuple[str, ...] = (
    r"worker\s+(lost|died|crashed|disconnected)",
    r"dask.*scheduler.*unavailable",
    r"killed\s+(by\s+signal|due\s+to)",
    r"signal\s+9\b",               # SIGKILL
    r"signal\s+11\b",              # SIGSEGV from executor kill
    r"memory\s+(limit|exceeded|oom)",
    r"out\s+of\s+memory",
    r"oom\s+kill",
    r"job\s+held",
    r"condor.*hold",
    r"preempt",
    r"evicted",
    r"node\s+failure",
    r"executor\s+error",
    r"job\s+was\s+aborted",
    r"batch\s+system",
    r"slurm\s+error",
    r"pbs\s+error",
    r"bsub\s+error",
)

_CONFIGURATION_PATTERNS: tuple[str, ...] = (
    r"no\s+such\s+file\s+or\s+directory",
    r"permission\s+denied",
    r"submit_config\.txt\s+(not\s+found|missing|invalid)",
    r"config(uration)?\s+(error|invalid|not\s+found|missing)",
    r"missing\s+(required\s+)?parameter",
    r"invalid\s+(parameter|option|value)",
    r"parse\s+error",
    r"yaml\s+(error|parse)",
    r"json\s+(error|decode)",
    r"key\s+error",
    r"attribute\s+error",
    r"not\s+implemented",
    r"exe.*not\s+found",
    r"executable\s+(not\s+found|missing|not\s+executable)",
    r"chmod.*failed",
    r"setup\s+script.*not\s+found",
)

_CORRUPTED_INPUT_PATTERNS: tuple[str, ...] = (
    r"root\s+(file|error|i/?o)",
    r"tfile.*\b(corrupt|error|truncat|invalid)\b",
    r"not\s+a\s+root\s+file",
    r"checksum\s+(mismatch|error|fail)",
    r"corrupt(ed)?\s+(file|data|input|record)",
    r"invalid\s+(root|tfile|tree|branch)\b",
    r"end\s+of\s+file",
    r"premature\s+eof",
    r"decompression\s+(error|fail)",
    r"cannot\s+read\s+(branch|tree|leaf)",
    r"bad\s+(magic|header|block)",
    r"malformed",
)

_ANALYSIS_CRASH_PATTERNS: tuple[str, ...] = (
    r"segmentation\s+fault",
    r"segfault",
    r"signal\s+11\b",
    r"signal\s+6\b",               # SIGABRT
    r"abort(?:ed)?\s*\(",
    r"assertion\s+(failed|error)",
    r"std::bad_alloc",
    r"std::exception",
    r"terminate\s+called",
    r"stack\s+overflow",
    r"illegal\s+instruction",
    r"bus\s+error",
    r"floating\s+point\s+exception",
    r"double\s+free",
    r"heap\s+(corruption|overflow)",
    r"analysis\s+(code\s+)?crash",
    r"exception\s+caught\s+in\s+analysis",
    r"user\s+code\s+threw",
    r"roofit\s+error",
    r"root\s+error\s+message",
    r"tthread",
)


def _compile(patterns: tuple[str, ...]) -> re.Pattern:
    return re.compile("|".join(patterns), re.IGNORECASE)


_RE_TRANSIENT_IO    = _compile(_TRANSIENT_IO_PATTERNS)
_RE_EXECUTOR        = _compile(_EXECUTOR_PATTERNS)
_RE_CONFIGURATION   = _compile(_CONFIGURATION_PATTERNS)
_RE_CORRUPTED_INPUT = _compile(_CORRUPTED_INPUT_PATTERNS)
_RE_ANALYSIS_CRASH  = _compile(_ANALYSIS_CRASH_PATTERNS)


def classify_failure(
    exc: BaseException,
    stderr: str = "",
) -> FailureCategory:
    """
    Classify *exc* (and optional *stderr* text) into a :class:`FailureCategory`.

    The classification is heuristic: it searches for known error-message
    patterns in the combined string ``str(exc) + "\\n" + stderr``.  The
    categories are tested in priority order so that more specific categories
    take precedence over generic ones.

    Parameters
    ----------
    exc:
        The exception that was raised.
    stderr:
        Optional stderr output captured from the failed process.

    Returns
    -------
    FailureCategory
        The most likely category, or :attr:`FailureCategory.UNKNOWN` if no
        pattern matched.
    """
    text = f"{exc}\n{stderr}".lower()

    # Check exception type first for quick wins
    if isinstance(exc, (FileNotFoundError, PermissionError)):
        return FailureCategory.CONFIGURATION
    if isinstance(exc, (OSError, ConnectionError, TimeoutError)):
        return FailureCategory.TRANSIENT_IO
    if isinstance(exc, MemoryError):
        return FailureCategory.EXECUTOR

    # Pattern-based classification (ordered by priority)
    if _RE_ANALYSIS_CRASH.search(text):
        return FailureCategory.ANALYSIS_CRASH
    if _RE_CORRUPTED_INPUT.search(text):
        return FailureCategory.CORRUPTED_INPUT
    if _RE_TRANSIENT_IO.search(text):
        return FailureCategory.TRANSIENT_IO
    if _RE_EXECUTOR.search(text):
        return FailureCategory.EXECUTOR
    if _RE_CONFIGURATION.search(text):
        return FailureCategory.CONFIGURATION

    return FailureCategory.UNKNOWN


# ---------------------------------------------------------------------------
# FailureRecord
# ---------------------------------------------------------------------------

@dataclass
class FailureRecord:
    """
    A single failure event captured during workflow execution.

    Attributes
    ----------
    branch_num:
        The workflow branch index that failed.
    attempt:
        The attempt number (1 = first try, 2 = first retry, …).
    category:
        Classified failure category.
    message:
        Human-readable error message (usually ``str(exc)``).
    traceback_text:
        Full Python traceback, or empty string if not available.
    timestamp:
        ISO-8601 UTC timestamp of when the failure was recorded.
    """

    branch_num: int
    attempt: int
    category: FailureCategory
    message: str
    traceback_text: str = ""
    timestamp: str = field(
        default_factory=lambda: datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# DiagnosticSummary
# ---------------------------------------------------------------------------

class DiagnosticSummary:
    """
    Accumulates :class:`FailureRecord` objects and produces structured
    diagnostic reports.

    This class is deliberately simple and thread-safe via a list-append
    operation (the GIL ensures atomicity for single appends in CPython).

    Example
    -------
    ::

        summary = DiagnosticSummary()
        summary.add(FailureRecord(branch_num=3, attempt=1,
                                  category=FailureCategory.TRANSIENT_IO,
                                  message="XRootD timeout"))
        print(summary.report())
    """

    def __init__(self) -> None:
        self._records: list[FailureRecord] = []

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add(self, record: FailureRecord) -> None:
        """Append *record* to the summary."""
        self._records.append(record)

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    @property
    def records(self) -> list[FailureRecord]:
        """All recorded failure events (read-only view)."""
        return list(self._records)

    def by_category(self) -> dict[FailureCategory, list[FailureRecord]]:
        """Return a dict mapping each :class:`FailureCategory` to its records."""
        result: dict[FailureCategory, list[FailureRecord]] = {}
        for rec in self._records:
            result.setdefault(rec.category, []).append(rec)
        return result

    def failed_branches(self) -> set[int]:
        """Return the set of branch numbers that have at least one failure."""
        return {rec.branch_num for rec in self._records}

    def total_failures(self) -> int:
        """Total number of recorded failure events."""
        return len(self._records)

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------

    def report(self) -> str:
        """
        Return a human-readable multi-line diagnostic summary.

        The report includes:
        - A per-category breakdown with counts
        - Details of each failure (branch, attempt, category, message)
        """
        if not self._records:
            return "DiagnosticSummary: no failures recorded."

        lines: list[str] = [
            f"DiagnosticSummary: {self.total_failures()} failure event(s) "
            f"across {len(self.failed_branches())} branch(es).",
        ]

        by_cat = self.by_category()
        lines.append("\nFailures by category:")
        for cat in FailureCategory:
            recs = by_cat.get(cat, [])
            if recs:
                lines.append(f"  {cat.value:20s}: {len(recs):4d} event(s)")

        lines.append("\nIndividual failure events:")
        for rec in self._records:
            lines.append(
                f"  [{rec.timestamp}] branch={rec.branch_num} "
                f"attempt={rec.attempt} category={rec.category.value}"
            )
            if rec.message:
                # Indent and truncate long messages
                msg = rec.message[:300] + ("…" if len(rec.message) > 300 else "")
                for line in msg.splitlines():
                    lines.append(f"    {line}")

        return "\n".join(lines)


# ---------------------------------------------------------------------------
# run_with_retries
# ---------------------------------------------------------------------------

def run_with_retries(
    fn: Callable,
    args: list,
    kwargs: dict | None = None,
    branch_num: int = -1,
    summary: DiagnosticSummary | None = None,
    policies: dict[FailureCategory, RetryPolicy] | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> Any:
    """
    Execute ``fn(*args, **kwargs)`` with smart per-category retry logic.

    On each failure the exception is classified via :func:`classify_failure`
    and the retry policy for its category is consulted.  If retries are
    exhausted the final exception is re-raised.

    Parameters
    ----------
    fn:
        Callable to execute.
    args:
        Positional arguments for *fn*.
    kwargs:
        Keyword arguments for *fn* (default: empty dict).
    branch_num:
        Branch index to embed in :class:`FailureRecord` entries (use ``-1``
        if not applicable).
    summary:
        :class:`DiagnosticSummary` to record each failure into.  A new one
        is created if ``None``.
    policies:
        Mapping from :class:`FailureCategory` to :class:`RetryPolicy`.
        Defaults to :data:`DEFAULT_RETRY_POLICIES`.
    sleep_fn:
        Callable used for delays between retries.  Defaults to
        ``time.sleep``; override in tests to avoid real sleeps.

    Returns
    -------
    Any
        The return value of *fn* on success.

    Raises
    ------
    Exception
        The final exception after all retries are exhausted.
    """
    if kwargs is None:
        kwargs = {}
    if summary is None:
        summary = DiagnosticSummary()
    if policies is None:
        policies = DEFAULT_RETRY_POLICIES

    attempt = 1
    while True:
        try:
            return fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001
            tb_text = traceback.format_exc()

            # Extract stderr from RuntimeError messages produced by
            # _run_analysis_job (format: "…\nstderr:\n<text>")
            stderr = ""
            msg = str(exc)
            if "\nstderr:\n" in msg:
                stderr = msg.split("\nstderr:\n", 1)[1]

            category = classify_failure(exc, stderr=stderr)
            policy = policies.get(category, DEFAULT_RETRY_POLICIES[FailureCategory.UNKNOWN])

            record = FailureRecord(
                branch_num=branch_num,
                attempt=attempt,
                category=category,
                message=msg,
                traceback_text=tb_text,
            )
            summary.add(record)

            if attempt > policy.max_retries:
                # All retries exhausted – re-raise the last exception
                raise

            delay = policy.delay_for_attempt(attempt)
            attempt += 1
            sleep_fn(delay)
