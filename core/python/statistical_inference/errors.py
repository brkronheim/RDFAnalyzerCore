"""Errors shared by statistical backends and analysis adapters."""


class FitError(RuntimeError):
    """The requested model or fit cannot be represented or validated."""
