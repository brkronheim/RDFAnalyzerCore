"""Minimal framework shim for RDFAnalyzerCore LAW plot tasks.

The legacy `law/plot_tasks.py` module imports `framework.Task`, `framework.PlotTask`
and `view_output_plots`. In this repo, those symbols are implemented as thin
wrappers around the installed `law` task base class plus a no-op view decorator.
"""

from __future__ import annotations

from law.task.base import Task as BaseTask


class Task(BaseTask):
    """Task base class used by RDFAnalyzerCore LAW task wrappers."""


class PlotTask(Task):
    """Plot task base class used by RDFAnalyzerCore LAW plot wrappers."""


def view_output_plots(func):
    """No-op decorator for plot task output visualization hooks."""
    return func
