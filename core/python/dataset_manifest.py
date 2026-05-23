"""
Dataset manifest model for RDFAnalyzerCore.

Provides a rich metadata model for HEP datasets that supports year, era,
campaign, process, sample grouping, stitching metadata, cross sections, sum of
weights, filter efficiencies, data/MC typing, and derived dataset lineage.

Analyses and law tasks can query datasets through this metadata rather than
relying only on flat file lists.

Usage
-----
Load a YAML manifest and query it::

    from dataset_manifest import DatasetManifest

    manifest = DatasetManifest.load("datasets.yaml")

    # Retrieve a single dataset by name
    entry = manifest.by_name("ttbar_powheg_2022")

    # Query all 2022 MC ttbar samples
    samples = manifest.query(year=2022, dtype="mc", process="ttbar")

    # Query datasets across multiple years at once
    multi_year = manifest.query(year=[2022, 2023], dtype="mc", process="ttbar")

    # Query an entire stitched group
    wjets = manifest.query(group="wjets_ht")

    # Get the luminosity for a specific year (uses lumi_by_year if available)
    lumi_2022 = manifest.lumi_for(year=2022)

    # Validate the manifest for duplicate names and missing parent references
    errors = manifest.validate()
    if errors:
        for err in errors:
            print(err)

    # Convert to the legacy key=value dict expected by submission scripts
    for sample in samples:
        d = sample.to_legacy_dict()
        print(d["name"], d["xsec"], d["das"])
"""

from __future__ import annotations

import hashlib
import os
import warnings
from dataclasses import dataclass, field, fields, asdict
from typing import Any, Dict, List, Optional, Union

import yaml


# ---------------------------------------------------------------------------
# FriendTreeConfig
# ---------------------------------------------------------------------------

@dataclass
class FriendTreeConfig:
    """Configuration for a friend tree or sidecar input attached to a dataset.

    A friend tree provides additional columns (branches) from one or more
    separate ROOT files.  The framework attaches the friend TChain to the
    main chain *before* the RDataFrame is created so that all friend branches
    are immediately accessible.

    Both local file paths and XRootD remote URLs (``root://...``) are
    supported for the ``files`` list, enabling use with grid storage.

    Parameters
    ----------
    alias : str
        Alias used to access friend tree branches.  In ROOT, branches from a
        friend tree registered as ``"calib"`` are accessible as ``calib.pt``,
        ``calib.eta``, etc.
    tree_name : str
        Name of the TTree inside the friend ROOT file(s).  Defaults to
        ``"Events"``.
    files : list[str]
        Explicit list of ROOT file paths or XRootD URLs.  Takes precedence
        over ``directory`` when both are set.
    directory : str or None
        Path to a local directory that will be scanned recursively for ROOT
        files.  Used when an explicit ``files`` list is not provided.
    globs : list[str]
        Filename patterns to *include* when scanning ``directory``
        (default: ``[".root"]``).
    antiglobs : list[str]
        Filename patterns to *exclude* when scanning ``directory``
        (default: empty).
    index_branches : list[str]
        Branch names used as event identifiers for index-based matching.
        When non-empty, the framework calls ``TChain::BuildIndex`` using the
        first two entries as the major and minor index keys.  This ensures
        correct event matching even when file ordering or entry counts differ
        between the main tree and the friend tree.

        Common configurations:

        * ``["run", "luminosityBlock"]`` — match on run and lumi block
        * ``["run", "event"]``           — match on run and event number

        Leave empty for position-based (sequential-order) matching, which is
        the default ROOT friend tree behaviour.

    Example
    -------
    ::

        FriendTreeConfig(
            alias="calib",
            tree_name="Events",
            files=[
                "/data/calib_2022.root",
                "root://eosserver.cern.ch//eos/data/calib_remote.root",
            ],
            index_branches=["run", "luminosityBlock"],
        )
    """

    alias: str
    tree_name: str = "Events"
    files: List[str] = field(default_factory=list)
    directory: Optional[str] = None
    globs: List[str] = field(default_factory=lambda: [".root"])
    antiglobs: List[str] = field(default_factory=list)
    index_branches: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """Return the config as a plain Python dict suitable for YAML serialisation."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FriendTreeConfig":
        """Construct a :class:`FriendTreeConfig` from a plain dict.

        Unknown keys are silently ignored for forward-compatibility.
        """
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)


# ---------------------------------------------------------------------------
# DatasetEntry
# ---------------------------------------------------------------------------

@dataclass
class DatasetEntry:
    """Metadata record for a single HEP dataset sample.

    Mandatory fields
    ----------------
    name : str
        Unique sample identifier used by submission scripts and law tasks.

    Optional metadata fields
    ------------------------
    year : int or None
        Data-taking / MC production year (e.g. 2022, 2023).
    era : str or None
        Run era within the year (e.g. "C", "D"). Set to None for MC.
    campaign : str or None
        MC production campaign tag (e.g. "Run3Summer22NanoAODv12").
    process : str or None
        Physics process label (e.g. "ttbar", "wjets", "data").
    group : str or None
        Sample grouping label used to collect stitched / HT-binned samples
        (e.g. "wjets_ht").  All samples sharing a group label can be queried
        together via ``DatasetManifest.query(group=...)``.
    stitch_id : int or None
        Integer stitching code written into the event stream and read back by
        the CounterService ``counterIntWeightBranch`` mechanism.
    sample_type : int or None
        Optional analysis-specific integer sample code preserved for legacy
        submission workflows. This stays separate from ``dtype`` so YAML
        manifests can retain framework-level ``mc`` / ``data`` typing while
        still passing numeric sample codes into per-job configs.

    Physics normalisation
    ---------------------
    xsec : float or None
        Cross-section in pb. Leave None for real data.
    filter_efficiency : float
        Generator-level filter efficiency (fraction surviving the generator
        filter). Defaults to 1.0.
    kfac : float
        QCD k-factor to scale the LO cross-section to (N)NLO. Defaults to 1.0.
    extra_scale : float
        Additional arbitrary scale factor. Defaults to 1.0.
    sum_weights : float or None
        Sum of generator weights (computed at run-time if None).

    Dataset type
    ------------
    dtype : str
        ``"mc"`` (simulation) or ``"data"`` (real data). Defaults to ``"mc"``.

    File discovery
    --------------
    das : str or None
        Comma-separated DAS path(s) for CMS NanoAOD Rucio discovery
        (e.g. ``/TTto2L2Nu.../NANOAODSIM``).
    files : list[str]
        Explicit list of ROOT file paths or XRootD URLs.  Used instead of
        ``das`` for open-data or local file inputs.

    Provenance
    ----------
    parent : str or None
        Name of the parent ``DatasetEntry`` from which this dataset was
        derived.  Enables lineage tracking for skimmed / filtered datasets.
    site : str or None
        Optional site override used by legacy submission flows for remote
        discovery preferences.
    """

    name: str

    # -- metadata ---
    year: Optional[int] = None
    era: Optional[str] = None
    campaign: Optional[str] = None
    process: Optional[str] = None
    group: Optional[str] = None
    stitch_id: Optional[int] = None
    sample_type: Optional[int] = None

    # -- normalisation ---
    xsec: Optional[float] = None
    filter_efficiency: float = 1.0
    kfac: float = 1.0
    extra_scale: float = 1.0
    sum_weights: Optional[float] = None
    energy: float = 13600.0

    # -- type ---
    dtype: str = "mc"  # "mc" | "data"

    # -- file discovery ---
    das: Optional[str] = None
    files: List[str] = field(default_factory=list)

    # -- provenance ---
    parent: Optional[str] = None
    site: Optional[str] = None

    # -- extensible per-dataset metadata ---
    extra: Dict[str, Any] = field(default_factory=dict)

    # -- friend trees / sidecar inputs ---
    friend_trees: List[FriendTreeConfig] = field(default_factory=list)
    """Friend tree / sidecar configurations attached to this dataset.

    Each entry describes a separate ROOT file (or set of files) whose
    branches are merged into the main event stream via ROOT's friend-tree
    mechanism.  Typical use cases:

    * Per-dataset calibration corrections (e.g. jet energy corrections)
    * External tagger outputs or auxiliary reconstructions
    * Derived sidecar files produced by a previous analysis step

    These configurations are serialised into the YAML manifest and can be
    read back by analysis code to automatically attach the correct sidecar
    files when building a DataManager.

    See :class:`FriendTreeConfig` for the full list of supported options.
    """

    # ------------------------------------------------------------------ helpers

    def to_legacy_dict(self) -> Dict[str, str]:
        """Return a ``{key: str}`` dict with all non-``None`` values converted to
        strings.  Used when writing legacy on-disk ``submit_config.txt`` job
        configs or emitting per-job float/int config files.
        """
        d: Dict[str, str] = {"name": self.name}

        if self.xsec is not None:
            d["xsec"] = str(self.xsec)
        if self.kfac != 1.0:
            d["kfac"] = str(self.kfac)
        if self.extra_scale != 1.0:
            d["extraScale"] = str(self.extra_scale)
        if self.filter_efficiency != 1.0:
            d["filterEfficiency"] = str(self.filter_efficiency)
        if self.sum_weights is not None:
            d["norm"] = str(self.sum_weights)
        if self.das is not None:
            d["das"] = self.das
        if self.files:
            d["fileList"] = ",".join(self.files)
        # "type" is used by the submission scripts for MC/data branching
        d["type"] = self.dtype

        # Carry extra fields used by stitching and open-data workflows
        if self.stitch_id is not None:
            d["stitch_id"] = str(self.stitch_id)
        if self.sample_type is not None:
            d["sample_type"] = str(self.sample_type)
        if self.year is not None:
            d["year"] = str(self.year)
        if self.era is not None:
            d["era"] = self.era
        if self.campaign is not None:
            d["campaign"] = self.campaign
        if self.process is not None:
            d["process"] = self.process
        if self.group is not None:
            d["group"] = self.group
        if self.parent is not None:
            d["parent"] = self.parent
        if self.site is not None:
            d["site"] = self.site
        return d

    def to_dict(self) -> Dict[str, Any]:
        """Return the full entry as a plain Python dict (suitable for YAML
        serialisation).  ``None`` values are preserved."""
        return asdict(self)

    @classmethod
    def from_legacy_dict(cls, data: Dict[str, str]) -> "DatasetEntry":
        """Construct a typed entry from a legacy ``key=value`` sample dict."""
        return _entry_from_legacy_kv(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatasetEntry":
        """Construct a ``DatasetEntry`` from a plain dict (e.g. loaded from
        YAML).

        Unknown top-level keys are preserved under ``extra`` for forward-
        compatibility and to support compact manifests where analysis-specific
        metadata (e.g. ``ptcut``) is declared directly on the dataset entry.
        """
        known = {f.name for f in fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        extra: Dict[str, Any] = {}
        raw_extra = filtered.get("extra")
        if isinstance(raw_extra, dict):
            extra.update(raw_extra)

        for key, value in data.items():
            if key not in known:
                extra[key] = value

        if extra:
            filtered["extra"] = extra

        # Deserialise nested FriendTreeConfig objects
        if "friend_trees" in filtered and isinstance(filtered["friend_trees"], list):
            filtered["friend_trees"] = [
                FriendTreeConfig.from_dict(ft) if isinstance(ft, dict) else ft
                for ft in filtered["friend_trees"]
            ]
        return cls(**filtered)


# ---------------------------------------------------------------------------
# DatasetManifest
# ---------------------------------------------------------------------------

class DatasetManifest:
    r"""Container for a collection of :class:`DatasetEntry` objects, with
    rich querying capabilities.

    Attributes
    ----------
    datasets : list[DatasetEntry]
        All registered datasets.
    lumi : float
        Integrated luminosity in fb^-1.  Used as the default when computing
        event weights inside submission scripts.  When *no* year is specified
        in a ``lumi_for()`` call (or the requested year is absent from
        ``lumi_by_year``), this global value is returned.  If ``lumi_by_year``
        contains an entry for a given year, ``lumi_for(year=...)`` returns that
        value instead.
    lumi_by_year : dict[int, float]
        Optional per-year luminosity mapping (year → fb^-1).  Takes precedence
        over the global ``lumi`` when querying via ``lumi_for()``.  Useful for
        multi-year analyses where each year has a distinct luminosity (e.g.
        ``{2022: 38.01, 2023: 62.32}``).
    whitelist : list[str]
        Site whitelist passed to Rucio queries (empty = all sites allowed).
    blacklist : list[str]
        Site blacklist passed to Rucio queries.
    """

    def __init__(
        self,
        datasets: Optional[List[DatasetEntry]] = None,
        lumi: float = 1.0,
        whitelist: Optional[List[str]] = None,
        blacklist: Optional[List[str]] = None,
        lumi_by_year: Optional[Dict[int, float]] = None,
    ) -> None:
        self.datasets: List[DatasetEntry] = datasets or []
        self.lumi: float = lumi
        self.whitelist: List[str] = whitelist or []
        self.blacklist: List[str] = blacklist or []
        self.lumi_by_year: Dict[int, float] = lumi_by_year or {}

    # ------------------------------------------------------------------ identity / provenance

    @staticmethod
    def file_hash(path: str) -> str:
        """Return the SHA-256 hex digest of the manifest file at *path*.

        The hash is computed over the raw file bytes and can be used to
        uniquely identify the manifest version used in a workflow task.
        Recording this alongside the selected query parameters and the
        resolved entry names makes the dataset selection fully reproducible.

        Parameters
        ----------
        path : str
            Path to the manifest file (YAML or legacy text).

        Returns
        -------
        str
            64-character lowercase hex digest, or ``"<not found>"`` if the
            file cannot be opened.

        Example
        -------
        ::

            h = DatasetManifest.file_hash("datasets.yaml")
            # "3a5f2b..." – stable identifier for this manifest revision
        """
        try:
            h = hashlib.sha256()
            with open(path, "rb") as fh:
                for chunk in iter(lambda: fh.read(65536), b""):
                    h.update(chunk)
            return h.hexdigest()
        except OSError:
            return "<not found>"

    # ------------------------------------------------------------------ I/O

    @classmethod
    def load(cls, path: str) -> "DatasetManifest":
        """Load a YAML dataset manifest file.

        * Files ending in ``.yaml`` / ``.yml`` are loaded as YAML manifests.
        * All other file types are no longer supported.

        Parameters
        ----------
        path : str
            Path to the YAML manifest file.
        """
        ext = os.path.splitext(path)[1].lower()
        if ext in (".yaml", ".yml"):
            return cls.load_yaml(path)
        raise ValueError(
            "DatasetManifest.load() only supports YAML manifest files with "
            ".yaml or .yml extensions. Legacy text format support has been removed."
        )

    @classmethod
    def load_yaml(cls, path: str) -> "DatasetManifest":
        """Load a YAML manifest file.

        The expected top-level structure is::

            lumi: 59.7          # optional (global / fallback luminosity)
            lumi_by_year:       # optional per-year luminosities
              2022: 38.01
              2023: 27.01
            whitelist: []       # optional
            blacklist: []       # optional
            datasets:
              - name: ttbar_2022
                year: 2022
                ...

        Parameters
        ----------
        path : str
            Path to the YAML manifest file.
        """
        with open(path) as fh:
            raw = yaml.safe_load(fh)

        if not isinstance(raw, dict):
            raise ValueError(
                f"Dataset manifest '{path}' must be a YAML mapping at the top level."
            )

        _validate_manifest_yaml(raw, path)

        lumi = float(raw.get("lumi", 1.0))
        whitelist = raw.get("whitelist") or []
        blacklist = raw.get("blacklist") or []

        # lumi_by_year: {year (int): lumi (float)} – optional per-year luminosities
        raw_lumi_by_year = raw.get("lumi_by_year") or {}
        lumi_by_year: Dict[int, float] = {int(k): float(v) for k, v in raw_lumi_by_year.items()}

        entries: List[DatasetEntry] = []
        for item in raw.get("datasets", []):
            if not isinstance(item, dict):
                raise ValueError(
                    f"Each entry under 'datasets' must be a YAML mapping; got {type(item).__name__}."
                )
            entries.append(DatasetEntry.from_dict(item))

        return cls(datasets=entries, lumi=lumi, whitelist=whitelist, blacklist=blacklist, lumi_by_year=lumi_by_year)

    def save_yaml(self, path: str) -> None:
        """Serialise the manifest to a YAML file.

        Parameters
        ----------
        path : str
            Destination file path (created / overwritten).
        """
        raw: Dict[str, Any] = {
            "lumi": self.lumi,
            "whitelist": self.whitelist,
            "blacklist": self.blacklist,
            "datasets": [e.to_dict() for e in self.datasets],
        }
        if self.lumi_by_year:
            raw["lumi_by_year"] = dict(self.lumi_by_year)
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w") as fh:
            yaml.dump(raw, fh, default_flow_style=False, sort_keys=False, allow_unicode=True)

    # ------------------------------------------------------------------ query

    def by_name(self, name: str) -> Optional[DatasetEntry]:
        """Return the :class:`DatasetEntry` with the given ``name``, or
        ``None`` if not found."""
        for entry in self.datasets:
            if entry.name == name:
                return entry
        return None

    def query(
        self,
        *,
        year: Optional[Union[int, List[int]]] = None,
        era: Optional[Union[str, List[str]]] = None,
        campaign: Optional[Union[str, List[str]]] = None,
        process: Optional[Union[str, List[str]]] = None,
        group: Optional[Union[str, List[str]]] = None,
        dtype: Optional[Union[str, List[str]]] = None,
        parent: Optional[Union[str, List[str]]] = None,
    ) -> List[DatasetEntry]:
        """Filter datasets by metadata criteria.

        All supplied keyword arguments are combined with logical AND; omitting
        a keyword means *don't filter on that field*.  Each argument may be a
        single value **or a list of values** — a list matches any entry whose
        field is contained in that list (logical OR within the field).

        Parameters
        ----------
        year : int or list[int], optional
            Require ``entry.year == year``, or ``entry.year in year``.
        era : str or list[str], optional
            Require ``entry.era == era``, or ``entry.era in era``.
        campaign : str or list[str], optional
            Require ``entry.campaign == campaign``, or ``entry.campaign in campaign``.
        process : str or list[str], optional
            Require ``entry.process == process``, or ``entry.process in process``.
        group : str or list[str], optional
            Require ``entry.group == group``, or ``entry.group in group``.
        dtype : str or list[str], optional
            Require ``entry.dtype == dtype`` (``"mc"`` or ``"data"``), or
            ``entry.dtype in dtype``.
        parent : str or list[str], optional
            Require ``entry.parent == parent``, or ``entry.parent in parent``.

        Returns
        -------
        list[DatasetEntry]
            Matching entries (empty list if none match).
        """
        def _matches(entry_val, filter_val):
            """Return True if *entry_val* satisfies *filter_val* (scalar or list)."""
            if isinstance(filter_val, list):
                return entry_val in filter_val
            return entry_val == filter_val

        result = []
        for entry in self.datasets:
            if year is not None and not _matches(entry.year, year):
                continue
            if era is not None and not _matches(entry.era, era):
                continue
            if campaign is not None and not _matches(entry.campaign, campaign):
                continue
            if process is not None and not _matches(entry.process, process):
                continue
            if group is not None and not _matches(entry.group, group):
                continue
            if dtype is not None and not _matches(entry.dtype, dtype):
                continue
            if parent is not None and not _matches(entry.parent, parent):
                continue
            result.append(entry)
        return result

    def get_groups(self) -> List[str]:
        """Return a sorted list of unique group labels."""
        return sorted({e.group for e in self.datasets if e.group is not None})

    def get_processes(self) -> List[str]:
        """Return a sorted list of unique process labels."""
        return sorted({e.process for e in self.datasets if e.process is not None})

    def get_years(self) -> List[int]:
        """Return a sorted list of unique years."""
        return sorted({e.year for e in self.datasets if e.year is not None})

    def get_eras(self) -> List[str]:
        """Return a sorted list of unique era labels across all datasets."""
        return sorted({e.era for e in self.datasets if e.era is not None})

    def get_eras_for_year(self, year: int) -> List[str]:
        """Return a sorted list of unique era labels for the given *year*.

        Parameters
        ----------
        year : int
            The data-taking year to filter on (e.g. 2022).

        Returns
        -------
        list[str]
            Sorted unique eras that appear in datasets with ``entry.year == year``.
        """
        return sorted({e.era for e in self.datasets if e.year == year and e.era is not None})

    def lumi_for(self, year: Optional[int] = None, era: Optional[str] = None) -> float:
        """Return the luminosity in fb^-1 for the given year (and optional era).

        Look-up order:

        1. If *year* is given and present in ``lumi_by_year``, return that value.
        2. Otherwise fall back to the global ``lumi`` (including when no arguments
           are passed).

        .. note::
           The *era* parameter is reserved for a future per-era luminosity
           look-up and is **not** currently used in the resolution logic.
           Passing it has no effect on the returned value.

        Parameters
        ----------
        year : int, optional
            Data-taking year.  If ``None`` or not in ``lumi_by_year``, the
            global ``lumi`` is returned.
        era : str, optional
            Run era.  Reserved for future per-era look-up; currently ignored.

        Returns
        -------
        float
            Luminosity in fb^-1.
        """
        if year is not None and year in self.lumi_by_year:
            return self.lumi_by_year[year]
        return self.lumi

    def validate(self) -> List[str]:
        """Check the manifest for consistency and return a list of error messages.

        The following checks are performed:

        * **Duplicate names** — every ``DatasetEntry.name`` must be unique.
        * **Missing parent references** — if ``entry.parent`` is set, a
          ``DatasetEntry`` with that name must exist in the manifest.
        * **Unknown lumi_by_year keys** — years in ``lumi_by_year`` that do not
          appear in any dataset are reported as warnings (prefixed with
          ``"Warning:"``).

        Returns
        -------
        list[str]
            Error / warning messages.  An empty list means the manifest is
            consistent.
        """
        errors: List[str] = []
        names: set = set()
        all_names: set = {e.name for e in self.datasets}

        for entry in self.datasets:
            if entry.name in names:
                errors.append(f"Duplicate dataset name: {entry.name!r}")
            names.add(entry.name)
            if entry.parent is not None and entry.parent not in all_names:
                errors.append(
                    f"Dataset {entry.name!r}: parent {entry.parent!r} not found in manifest"
                )

        # Warn about lumi_by_year entries that have no matching datasets
        dataset_years = {e.year for e in self.datasets if e.year is not None}
        for yr in self.lumi_by_year:
            if yr not in dataset_years:
                errors.append(
                    f"Warning: lumi_by_year contains year {yr} but no dataset has that year"
                )

        return errors

    def __len__(self) -> int:
        return len(self.datasets)

    def __iter__(self):
        return iter(self.datasets)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"DatasetManifest(datasets={len(self.datasets)}, lumi={self.lumi}, "
            f"lumi_by_year={self.lumi_by_year!r}, "
            f"whitelist={self.whitelist!r}, blacklist={self.blacklist!r})"
        )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _validate_manifest_yaml(raw: Dict[str, Any], path: str) -> None:
    """Validate the top-level YAML manifest schema and dataset structure."""
    allowed_top_keys = {"lumi", "lumi_by_year", "whitelist", "blacklist", "datasets"}
    extra_top_keys = set(raw) - allowed_top_keys
    if extra_top_keys:
        raise ValueError(
            f"Dataset manifest '{path}' contains unknown top-level keys: "
            f"{', '.join(sorted(extra_top_keys))}"
        )

    if "datasets" not in raw:
        raise ValueError(f"Dataset manifest '{path}' must contain a top-level 'datasets' list.")

    if not isinstance(raw["datasets"], list):
        raise ValueError(
            f"Dataset manifest '{path}' 'datasets' field must be a YAML sequence."
        )

    if "whitelist" in raw and raw["whitelist"] is not None:
        if not isinstance(raw["whitelist"], list) or any(not isinstance(item, str) for item in raw["whitelist"]):
            raise ValueError(
                f"Dataset manifest '{path}' 'whitelist' must be a list of strings."
            )

    if "blacklist" in raw and raw["blacklist"] is not None:
        if not isinstance(raw["blacklist"], list) or any(not isinstance(item, str) for item in raw["blacklist"]):
            raise ValueError(
                f"Dataset manifest '{path}' 'blacklist' must be a list of strings."
            )

    if "lumi_by_year" in raw and raw["lumi_by_year"] is not None:
        if not isinstance(raw["lumi_by_year"], dict):
            raise ValueError(
                f"Dataset manifest '{path}' 'lumi_by_year' must be a mapping from year to luminosity."
            )
        for key, value in raw["lumi_by_year"].items():
            try:
                int(key)
            except (TypeError, ValueError):
                raise ValueError(
                    f"Dataset manifest '{path}' 'lumi_by_year' keys must be integers, got {key!r}."
                )
            try:
                float(value)
            except (TypeError, ValueError):
                raise ValueError(
                    f"Dataset manifest '{path}' 'lumi_by_year[{key}]' must be numeric."
                )

    for dataset_index, item in enumerate(raw["datasets"]):
        if not isinstance(item, dict):
            raise ValueError(
                f"Dataset manifest '{path}' entry #{dataset_index} must be a YAML mapping."
            )
        _validate_dataset_entry(item, path, dataset_index)


def _validate_dataset_entry(raw_entry: Dict[str, Any], path: str, index: int) -> None:
    """Validate a single dataset entry inside the YAML manifest."""
    dataset_label = raw_entry.get("name", f"entry #{index}")

    if "name" not in raw_entry or not isinstance(raw_entry["name"], str):
        raise ValueError(
            f"Dataset manifest '{path}' entry #{index} must contain a string 'name' field."
        )

    if "extra" in raw_entry and raw_entry["extra"] is not None:
        if not isinstance(raw_entry["extra"], dict):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}': 'extra' must be a YAML mapping."
            )

    if "dtype" in raw_entry and raw_entry["dtype"] is not None:
        if not isinstance(raw_entry["dtype"], str):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}': 'dtype' must be a string."
            )
        if raw_entry["dtype"] not in {"mc", "data"}:
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}' contains invalid dtype {raw_entry['dtype']!r}."
            )

    if "files" in raw_entry and raw_entry["files"] is not None:
        if not isinstance(raw_entry["files"], list) or any(not isinstance(item, str) for item in raw_entry["files"]):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}': 'files' must be a list of strings."
            )

    if "friend_trees" in raw_entry and raw_entry["friend_trees"] is not None:
        if not isinstance(raw_entry["friend_trees"], list):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}': 'friend_trees' must be a YAML sequence."
            )
        for ft_index, ft in enumerate(raw_entry["friend_trees"]):
            if not isinstance(ft, dict):
                raise ValueError(
                    f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{ft_index}] must be a YAML mapping."
                )
            _validate_friend_tree_config(ft, path, dataset_label, ft_index)


def _validate_friend_tree_config(raw_friend_tree: Dict[str, Any], path: str, dataset_label: str, index: int) -> None:
    """Validate a single FriendTreeConfig mapping in a dataset entry."""
    if "alias" not in raw_friend_tree or not isinstance(raw_friend_tree["alias"], str):
        raise ValueError(
            f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{index}] must contain a string 'alias' field."
        )

    if "tree_name" in raw_friend_tree and raw_friend_tree["tree_name"] is not None:
        if not isinstance(raw_friend_tree["tree_name"], str):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{index}] 'tree_name' must be a string."
            )

    if "files" in raw_friend_tree and raw_friend_tree["files"] is not None:
        if not isinstance(raw_friend_tree["files"], list) or any(not isinstance(item, str) for item in raw_friend_tree["files"]):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{index}] 'files' must be a list of strings."
            )

    if "directory" in raw_friend_tree and raw_friend_tree["directory"] is not None:
        if not isinstance(raw_friend_tree["directory"], str):
            raise ValueError(
                f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{index}] 'directory' must be a string."
            )

    for list_key in ("globs", "antiglobs", "index_branches"):
        if list_key in raw_friend_tree and raw_friend_tree[list_key] is not None:
            if not isinstance(raw_friend_tree[list_key], list) or any(not isinstance(item, str) for item in raw_friend_tree[list_key]):
                raise ValueError(
                    f"Dataset manifest '{path}' entry '{dataset_label}' friend_trees[{index}] '{list_key}' must be a list of strings."
                )


def _entry_from_legacy_kv(kv: Dict[str, str]) -> DatasetEntry:
    """Construct a :class:`DatasetEntry` from a legacy key=value dict."""

    def _float(key: str, default: Optional[float] = None) -> Optional[float]:
        v = kv.get(key)
        if v is None:
            return default
        try:
            return float(v)
        except ValueError:
            return default

    def _int(key: str) -> Optional[int]:
        v = kv.get(key)
        if v is None:
            return None
        try:
            return int(v)
        except ValueError:
            return None

    raw_type = kv.get("type")
    dtype = kv.get("dtype", "mc")
    sample_type = _int("sample_type")
    if raw_type is not None:
        lowered = raw_type.lower()
        if lowered in {"mc", "data"}:
            dtype = lowered
        else:
            try:
                sample_type = int(raw_type)
            except ValueError:
                dtype = raw_type

    return DatasetEntry(
        name=kv["name"],
        year=_int("year"),
        era=kv.get("era"),
        campaign=kv.get("campaign"),
        process=kv.get("process"),
        group=kv.get("group"),
        stitch_id=_int("stitch_id"),
        sample_type=sample_type,
        xsec=_float("xsec"),
        filter_efficiency=_float("filterEfficiency", 1.0),
        kfac=_float("kfac", 1.0),
        extra_scale=_float("extraScale", 1.0),
        sum_weights=_float("norm"),
        energy=_float("energy", 13600.0),
        dtype=dtype,
        das=kv.get("das"),
        files=[f for f in kv.get("fileList", "").split(",") if f],
        parent=kv.get("parent"),
        site=kv.get("site"),
        extra={},
    )
