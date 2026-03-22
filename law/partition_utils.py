"""
Fine-grained partitioning utilities for RDFAnalyzerCore law tasks.

Provides :func:`_make_partitions` (split a file list into job partitions) and
:func:`_query_tree_entries` (query TTree entry counts via ``uproot``) used by
both :mod:`nano_tasks` and :mod:`opendata_tasks`.

Partition modes
---------------
``"file_group"``  (default)
    Groups files by count only.  Up to *files_per_job* files per partition.
    The result is deterministic: input URLs are sorted before chunking.

``"file"``
    One file per partition.  Equivalent to *files_per_job* = 1.  Enables
    the finest possible parallelism without querying ROOT file metadata.

``"entry_range"``
    One partition per contiguous entry-range chunk within each file.
    Requires ``uproot`` to be installed on the submit node.  Each partition
    contains exactly one file and explicit *firstEntry* / *lastEntry* indices.
    The number of partitions per file equals
    ``ceil(total_entries / entries_per_job)``.

    .. note::
       Entry-range partitions are applied by the C++ ``DataManager`` via
       ``ROOT::RDataFrame::Range(firstEntry, lastEntry)``.  This disables
       implicit multi-threading for the processing job when ROOT < 6.28 is
       used.  When running with ``--threads > 1``, prefer ``"file"`` or
       ``"file_group"`` mode unless all workers use ROOT ≥ 6.28.

Determinism and reproducibility
--------------------------------
All three modes sort the input URL list before partitioning, so the same
input always produces the same partition layout.  Failed partitions can be
rerun by specifying the corresponding law branch number.
"""

from __future__ import annotations


def _query_tree_entries(url: str, tree_name: str = "Events") -> int:
    """Return the number of TTree entries in *url*.

    Tries ``uproot`` (pure-Python, no ROOT installation required on the
    submit node).  Raises :class:`RuntimeError` with installation
    instructions when uproot is not available or when the file cannot be
    opened.

    Parameters
    ----------
    url:
        XRootD URL or local path to a ROOT file.
    tree_name:
        Name of the TTree whose entry count to return.

    Raises
    ------
    RuntimeError
        If ``uproot`` is not installed, or the tree cannot be found, or the
        file cannot be opened.
    """
    try:
        import uproot  # type: ignore
    except ImportError as exc:
        raise RuntimeError(
            "partition='entry_range' requires uproot.\n"
            "Install it with:  pip install uproot"
        ) from exc

    try:
        with uproot.open(url) as f:
            # uproot keys include cycle numbers (e.g. "Events;1") – strip them
            keys_no_cycle = [k.split(";")[0] for k in f.keys()]
            if tree_name in keys_no_cycle:
                return f[tree_name].num_entries
            raise KeyError(
                f"Tree {tree_name!r} not found in {url!r}. "
                f"Available keys: {keys_no_cycle}"
            )
    except Exception as exc:
        raise RuntimeError(
            f"Failed to query entry count for {url!r}: {exc}"
        ) from exc


def _make_partitions(
    urls: list[str],
    mode: str,
    files_per_job: int,
    entries_per_job: int,
    tree_name: str = "Events",
) -> list[dict]:
    """Split a URL list into job partitions.

    Parameters
    ----------
    urls:
        Input file URLs.  Sorted internally so the result is deterministic.
    mode:
        Partitioning strategy.  One of ``"file_group"``, ``"file"``, or
        ``"entry_range"``.
    files_per_job:
        Maximum number of files per partition (``"file_group"`` mode only).
    entries_per_job:
        Maximum TTree entries per partition (``"entry_range"`` mode only).
    tree_name:
        TTree name used to query entry counts in ``"entry_range"`` mode.

    Returns
    -------
    list[dict]
        Each dict contains:

        * ``"files"``       – comma-separated URL string for the partition.
        * ``"first_entry"`` – first entry index (0 for non-entry-range modes).
        * ``"last_entry"``  – one-past-last entry index (0 for
          non-entry-range modes, meaning "process all entries").

    Raises
    ------
    ValueError
        If *mode* is not one of the recognised values.
    RuntimeError
        If ``"entry_range"`` mode is requested but ``uproot`` is not
        installed, or a file's entry count cannot be determined.
    """
    sorted_urls = sorted(set(urls))
    print("sorted_urls", sorted_urls)

    if mode == "file":
        print("mode file", [{"files": u, "first_entry": 0, "last_entry": 0}
            for u in sorted_urls])
        sfsdfds
        return [
            {"files": u, "first_entry": 0, "last_entry": 0}
            for u in sorted_urls
        ]

    if mode == "file_group":
        partitions: list[dict] = []
        chunk: list[str] = []
        for u in sorted_urls:
            chunk.append(u)
            if len(chunk) >= files_per_job:
                partitions.append({
                    "files": ",".join(chunk),
                    "first_entry": 0,
                    "last_entry": 0,
                })
                chunk = []
        if chunk:
            partitions.append({
                "files": ",".join(chunk),
                "first_entry": 0,
                "last_entry": 0,
            })
        print("mode file_group", partitions)
        sfsdfds
        return partitions

    if mode == "entry_range":
        partitions = []
        for u in sorted_urls:
            n_entries = _query_tree_entries(u, tree_name)
            if n_entries <= 0:
                # File is empty or entry count is unavailable.  Emit a single
                # partition with last_entry=0 so the C++ job will process the
                # file in full (no Range() applied), which is a safe no-op for
                # an empty file and avoids silently discarding the file.
                partitions.append({"files": u, "first_entry": 0, "last_entry": 0})
                continue
            n_parts = max(1, (n_entries + entries_per_job - 1) // entries_per_job)
            for i in range(n_parts):
                first = i * entries_per_job
                last = min((i + 1) * entries_per_job, n_entries)
                partitions.append({
                    "files": u,
                    "first_entry": first,
                    "last_entry": last,
                })
        return partitions

    raise ValueError(
        f"Unknown partition mode {mode!r}. "
        "Valid modes: 'file_group', 'file', 'entry_range'."
    )
