"""Experiment-independent names and bounded concurrent workflow launching."""
from concurrent.futures import ThreadPoolExecutor, as_completed
import re
import subprocess


class CollectionError(RuntimeError):
    pass


def isolated_name(base, child):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", str(child)) or str(child) in {".", ".."}:
        raise ValueError(f"invalid collection member: {child!r}")
    return f"{base}_{child}"


def isolated_output(base, child):
    isolated_name("validate", child)
    return f"{base.rstrip('/')}/{child}"


def run_commands(commands, *, environments=None, workers=None, sequential=False):
    """Launch already-validated children and report failures after all finish."""
    commands = list(commands)
    if not commands:
        raise ValueError("a run collection must contain at least one workflow")
    if workers is not None and workers < 1:
        raise ValueError("collection workers must be positive")
    if environments is not None and len(environments) != len(commands):
        raise ValueError("each workflow requires exactly one environment")

    def run(index):
        kwargs = {"check": True}
        if environments is not None:
            kwargs["env"] = environments[index]
        return subprocess.run(commands[index], **kwargs)

    if sequential:
        return [run(index) for index in range(len(commands))]
    failures, results = [], [None] * len(commands)
    with ThreadPoolExecutor(max_workers=min(workers or len(commands), len(commands))) as pool:
        futures = {pool.submit(run, index): index for index in range(len(commands))}
        for future in as_completed(futures):
            index = futures[future]
            try:
                results[index] = future.result()
            except (OSError, subprocess.CalledProcessError) as exc:
                failures.append(f"workflow {index}: {exc}")
    if failures:
        raise CollectionError("; ".join(failures))
    return results
