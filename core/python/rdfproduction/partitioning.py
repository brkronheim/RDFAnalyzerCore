"""
Fine-grained partitioning utilities for RDFAnalyzerCore law tasks.

Provides :func:`_make_partitions` (split a file list into job partitions) and
:func:`_query_tree_entries` (query TTree entry counts via ``uproot``) used by
both :mod:`rucio_tasks` and :mod:`opendata_tasks`.

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

``"events"``
    Pack whole files up to an event budget and file-count cap; split oversized
    files into half-open entry ranges. Optional cached counts avoid repeat opens.
    Ranged jobs require one RDF thread. ``entryRangeMode=source`` can seek
    directly on ROOT 6.40.x or 6.42.00-rc1. The default logical Range walks rejected prefixes.

Determinism and reproducibility
--------------------------------
All modes sort the input URL list before partitioning, so the same
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
            "Event-based partitioning requires uproot.\n"
            "Install it with:  pip install uproot"
        ) from exc

    try:
        with uproot.open(url, timeout=30) as f:
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
    entry_cache=None,
) -> list[dict]:
    """Split a URL list into job partitions.

    Parameters
    ----------
    urls:
        Input file URLs.  Sorted internally so the result is deterministic.
    mode:
        Partitioning strategy: ``file_group``, ``file``, ``entry_range`` or ``events``.
    files_per_job:
        Maximum files per partition in ``file_group``/``events`` modes.
    entries_per_job:
        Maximum TTree entries in ``entry_range``/``events`` modes.
    tree_name:
        TTree name used to query entry counts in event-based modes.

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
    if mode not in {"file", "file_group", "entry_range", "events"}:
        raise ValueError(f"Unknown partition mode {mode!r}. Valid modes: file_group, file, entry_range, events.")
    if isinstance(files_per_job, bool) or not isinstance(files_per_job, int) or files_per_job < 1:
        raise ValueError("files_per_job must be positive")
    if mode in {"entry_range", "events"} and (isinstance(entries_per_job, bool) or not isinstance(entries_per_job, int) or entries_per_job < 1):
        raise ValueError("entries_per_job must be positive")
    sorted_urls = sorted(set(urls))

    def entries(url):
        value = entry_cache.get(url, tree_name) if entry_cache else None
        if value is None:
            value = _query_tree_entries(url, tree_name)
            if entry_cache is not None:
                entry_cache.put(url, value, tree_name)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"Invalid entry count for {url}: {value!r}")
        return value

    if mode == "events":
        partitions, chunk, count = [], [], 0

        def flush():
            nonlocal chunk, count
            if chunk:
                partitions.append({"files": ",".join(chunk), "first_entry": 0,
                                   "last_entry": 0, "planned_entries": count})
                chunk, count = [], 0

        for url in sorted_urls:
            n = entries(url)
            if n > entries_per_job:
                flush()
                for first in range(0, n, entries_per_job):
                    last = min(n, first + entries_per_job)
                    partitions.append({"files": url, "first_entry": first,
                                       "last_entry": last, "planned_entries": last-first})
                continue
            if chunk and (count + n > entries_per_job or len(chunk) >= files_per_job):
                flush()
            chunk.append(url)
            count += n
        flush()
        return partitions

    if mode == "file":
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
        return partitions

    if mode == "entry_range":
        partitions = []
        for u in sorted_urls:
            n_entries = entries(u)
            if n_entries <= 0:
                # File is empty. Emit a single
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
