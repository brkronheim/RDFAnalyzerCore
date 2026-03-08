"""
Output schema definitions for RDFAnalyzerCore.

Provides explicit, versioned schema definitions for all framework output types:

  - **Skims** – ROOT TTree event-level data written by ``RootOutputSink``.
  - **Histograms** – ROOT ``THnSparseF`` objects saved by ``NDHistogramManager``.
  - **Metadata / provenance** – ``TNamed`` objects in the ``"provenance"``
    ``TDirectory`` of the meta-output ROOT file, written by ``ProvenanceService``.
  - **Cutflows** – Event-count histograms written by ``CounterService``.
  - **LAW artifacts** – Task output files produced by the law workflow tasks
    in ``law/nano_tasks.py`` and ``law/opendata_tasks.py``.

Each schema class carries a ``CURRENT_VERSION`` class attribute (integer).
Bumping this integer is the canonical way to signal a breaking change in that
output's structure.  The ``schema_version`` *instance* field on every persisted
schema must equal ``CURRENT_VERSION``; a mismatch is surfaced by
:meth:`OutputManifest.check_version_compatibility`.

Version history
---------------
.. list-table::
   :header-rows: 1

   * - Schema
     - Version
     - Notes
   * - ``SkimSchema``
     - 1
     - Initial definition: output_file, tree_name, branches.
   * - ``HistogramSchema``
     - 1
     - Initial definition: output_file, histogram_names, axes.
   * - ``MetadataSchema``
     - 1
     - Initial definition: output_file, provenance_dir, required/optional keys.
   * - ``CutflowSchema``
     - 1
     - Initial definition: output_file, counter_keys.
   * - ``LawArtifactSchema``
     - 1
     - Initial definition: artifact_type, path_pattern, format.
   * - ``OutputManifest``
     - 1
     - Initial definition: combines all schema types.

Usage
-----
Build a manifest from configuration and validate it::

    from output_schema import (
        OutputManifest, SkimSchema, HistogramSchema, MetadataSchema,
        CutflowSchema, LawArtifactSchema,
    )

    manifest = OutputManifest(
        skim=SkimSchema(output_file="output.root", tree_name="Events"),
        metadata=MetadataSchema(output_file="output_meta.root"),
    )
    errors = manifest.validate()
    if errors:
        raise ValueError("Output schema validation failed:\\n" + "\\n".join(errors))

    # Persist the manifest so downstream tools can read the format contract.
    manifest.save_yaml("output_manifest.yaml")

    # Load back and verify the versions have not diverged from the current code.
    loaded = OutputManifest.load_yaml("output_manifest.yaml")
    OutputManifest.check_version_compatibility(loaded)

Provenance and version resolution (canonical API)::

    from output_schema import (
        OutputManifest,
        ProvenanceRecord,
        resolve_artifact,
        resolve_manifest,
        ArtifactResolutionStatus,
    )

    current = ProvenanceRecord(
        framework_hash="new_hash",
        user_repo_hash="user_hash",
    )
    manifest = OutputManifest.load_yaml("job_42/output_manifest.yaml")
    recorded = manifest.provenance()
    statuses = resolve_manifest(manifest, current_provenance=current)
    # statuses is a dict of role -> ArtifactResolutionStatus
"""

from __future__ import annotations

import datetime
import enum
import os
from dataclasses import dataclass, field, asdict, fields
from typing import Any, ClassVar, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Schema version constants
# Bump a constant here (and add a note to the version table above) whenever
# you make a breaking change to the corresponding output format.
# ---------------------------------------------------------------------------

#: Current version of :class:`SkimSchema`.
SKIM_SCHEMA_VERSION: int = 1
#: Current version of :class:`HistogramSchema`.
HISTOGRAM_SCHEMA_VERSION: int = 1
#: Current version of :class:`MetadataSchema`.
METADATA_SCHEMA_VERSION: int = 1
#: Current version of :class:`CutflowSchema`.
CUTFLOW_SCHEMA_VERSION: int = 1
#: Current version of :class:`LawArtifactSchema`.
LAW_ARTIFACT_SCHEMA_VERSION: int = 1
#: Current version of :class:`OutputManifest`.
OUTPUT_MANIFEST_VERSION: int = 1

#: Registry mapping schema name → current version for programmatic queries.
SCHEMA_REGISTRY: Dict[str, int] = {
    "skim": SKIM_SCHEMA_VERSION,
    "histogram": HISTOGRAM_SCHEMA_VERSION,
    "metadata": METADATA_SCHEMA_VERSION,
    "cutflow": CUTFLOW_SCHEMA_VERSION,
    "law_artifact": LAW_ARTIFACT_SCHEMA_VERSION,
    "output_manifest": OUTPUT_MANIFEST_VERSION,
}

# ---------------------------------------------------------------------------
# Known provenance keys (from ProvenanceService)
# ---------------------------------------------------------------------------

#: Provenance keys that ProvenanceService always writes.
PROVENANCE_REQUIRED_KEYS: List[str] = [
    "framework.git_hash",
    "framework.git_dirty",
    "framework.build_timestamp",
    "framework.compiler",
    "root.version",
    "config.hash",
    "executor.num_threads",
]

#: Provenance keys written by ProvenanceService when available.
PROVENANCE_OPTIONAL_KEYS: List[str] = [
    "analysis.git_hash",
    "analysis.git_dirty",
    "env.container_tag",
    "filelist.hash",
    "dataset_manifest.file_hash",
    "dataset_manifest.query_params",
    "dataset_manifest.resolved_entries",
]

# ---------------------------------------------------------------------------
# Known LAW artifact types
# ---------------------------------------------------------------------------

#: Recognised values for :attr:`LawArtifactSchema.artifact_type`.
LAW_ARTIFACT_TYPES: List[str] = [
    "prepare_sample",
    "build_submission",
    "submit_jobs",
    "monitor_jobs",
    "run_job",
    "monitor_state",
]

# ---------------------------------------------------------------------------
# Provenance and resolution types
# ---------------------------------------------------------------------------


class ArtifactResolutionStatus(enum.Enum):
    """Resolution status returned by the canonical version-resolution API.

    Use :func:`resolve_artifact` or :func:`resolve_manifest` to obtain a
    status for one or all schemas in a manifest.

    Attributes
    ----------
    COMPATIBLE:
        The schema version matches ``CURRENT_VERSION`` **and** the recorded
        provenance matches the current provenance (or no current provenance
        was supplied for comparison).  No regeneration is needed.
    STALE:
        The schema version is current, but the recorded provenance differs
        from the current provenance (e.g. a newer git commit or updated
        config file exists).  The artifact *can* still be used but should be
        regenerated when convenient.
    MUST_REGENERATE:
        The schema version does **not** match ``CURRENT_VERSION``.  The
        artifact is incompatible with the current codebase and **must** be
        regenerated before use.
    """

    COMPATIBLE = "compatible"
    STALE = "stale"
    MUST_REGENERATE = "must_regenerate"


class ProvenanceRecord:
    """Snapshot of the versioning context at the time an artifact was produced.

    A ``ProvenanceRecord`` captures the information needed to decide whether
    a previously produced artifact is still up-to-date:

    * **framework_hash** - git commit hash of the RDFAnalyzerCore framework.
    * **user_repo_hash** - git commit hash of the user analysis repository.
    * **config_mtime** - UTC modification time (ISO 8601) of the job
      configuration file that triggered the job.

    Any field may be ``None`` when the information is unavailable.  Two
    records are considered *matching* when every field that is non-``None`` in
    **both** records has the same value.

    Parameters
    ----------
    framework_hash:
        Git hash of the RDFAnalyzerCore framework, or ``None``.
    user_repo_hash:
        Git hash of the user analysis repository, or ``None``.
    config_mtime:
        UTC modification time of the configuration file in ISO 8601 format,
        or ``None``.

    Examples
    --------
    Compare a manifest's recorded provenance with the current environment::

        recorded = manifest.provenance()
        current = ProvenanceRecord(framework_hash="new_fw_hash")
        if not recorded.matches(current):
            print("Artifact is stale - consider regenerating")
    """

    def __init__(
        self,
        framework_hash: Optional[str] = None,
        user_repo_hash: Optional[str] = None,
        config_mtime: Optional[str] = None,
    ) -> None:
        self.framework_hash: Optional[str] = framework_hash
        self.user_repo_hash: Optional[str] = user_repo_hash
        self.config_mtime: Optional[str] = config_mtime

    def matches(self, other: "ProvenanceRecord") -> bool:
        """Return ``True`` when all comparable fields agree.

        A field is *comparable* when it is non-``None`` in **both** records.
        Fields that are ``None`` in either record are ignored (treated as
        "unknown") and do not cause a mismatch.

        Parameters
        ----------
        other:
            Another :class:`ProvenanceRecord` to compare against.

        Returns
        -------
        bool
            ``True`` if no comparable field differs between *self* and *other*.
        """
        for attr in ("framework_hash", "user_repo_hash", "config_mtime"):
            self_val = getattr(self, attr)
            other_val = getattr(other, attr)
            if self_val is not None and other_val is not None:
                if self_val != other_val:
                    return False
        return True

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON/YAML-serialisable dictionary representation."""
        return {
            "framework_hash": self.framework_hash,
            "user_repo_hash": self.user_repo_hash,
            "config_mtime": self.config_mtime,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ProvenanceRecord":
        """Deserialise from a dict produced by :meth:`to_dict`."""
        return cls(
            framework_hash=d.get("framework_hash"),
            user_repo_hash=d.get("user_repo_hash"),
            config_mtime=d.get("config_mtime"),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ProvenanceRecord("
            f"framework_hash={self.framework_hash!r}, "
            f"user_repo_hash={self.user_repo_hash!r}, "
            f"config_mtime={self.config_mtime!r})"
        )


# ---------------------------------------------------------------------------
# DatasetManifestProvenance
# ---------------------------------------------------------------------------


class DatasetManifestProvenance:
    """Identity and selection record for a dataset manifest used in a task.

    Records enough information to reproduce the exact dataset selection that
    was used when a workflow task ran:

    * **manifest_path** – path to the manifest file on disk.
    * **manifest_hash** – SHA-256 hex digest of the manifest file, as
      returned by :meth:`DatasetManifest.file_hash`.  Uniquely identifies
      the manifest revision.
    * **query_params** – keyword arguments passed to
      :meth:`DatasetManifest.query` (e.g. ``{"year": 2022, "dtype": "mc"}``).
      When ``None``, the full manifest was used (no query filter was applied).
    * **resolved_entry_names** – ordered list of :attr:`DatasetEntry.name`
      values that were selected by the query.  Given the manifest hash and
      query parameters, this list should be fully reproducible, but it is
      recorded explicitly for immediate human inspection.

    Any field may be ``None`` when the information is unavailable.

    Parameters
    ----------
    manifest_path:
        Path to the dataset manifest file, or ``None``.
    manifest_hash:
        SHA-256 hex digest of the manifest file, or ``None``.
    query_params:
        Dict of keyword arguments passed to ``DatasetManifest.query()``,
        or ``None`` when the full manifest was used.
    resolved_entry_names:
        Ordered list of dataset entry names selected by the query,
        or ``None``.

    Examples
    --------
    Record the manifest used for a specific query::

        from dataset_manifest import DatasetManifest
        from output_schema import DatasetManifestProvenance, OutputManifest

        manifest = DatasetManifest.load("datasets.yaml")
        query = {"year": 2022, "dtype": "mc", "process": "ttbar"}
        entries = manifest.query(**query)

        prov = DatasetManifestProvenance(
            manifest_path="datasets.yaml",
            manifest_hash=DatasetManifest.file_hash("datasets.yaml"),
            query_params=query,
            resolved_entry_names=[e.name for e in entries],
        )
        output = OutputManifest(
            skim=...,
            dataset_manifest_provenance=prov,
        )
    """

    def __init__(
        self,
        manifest_path: Optional[str] = None,
        manifest_hash: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
        resolved_entry_names: Optional[List[str]] = None,
    ) -> None:
        self.manifest_path: Optional[str] = manifest_path
        self.manifest_hash: Optional[str] = manifest_hash
        self.query_params: Optional[Dict[str, Any]] = query_params
        self.resolved_entry_names: Optional[List[str]] = resolved_entry_names

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON/YAML-serialisable dictionary representation."""
        return {
            "manifest_path": self.manifest_path,
            "manifest_hash": self.manifest_hash,
            "query_params": self.query_params,
            "resolved_entry_names": self.resolved_entry_names,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "DatasetManifestProvenance":
        """Deserialise from a dict produced by :meth:`to_dict`."""
        return cls(
            manifest_path=d.get("manifest_path"),
            manifest_hash=d.get("manifest_hash"),
            query_params=d.get("query_params"),
            resolved_entry_names=d.get("resolved_entry_names"),
        )

    def __repr__(self) -> str:  # pragma: no cover
        entry_count = len(self.resolved_entry_names) if self.resolved_entry_names is not None else None
        return (
            f"DatasetManifestProvenance("
            f"manifest_path={self.manifest_path!r}, "
            f"manifest_hash={self.manifest_hash!r}, "
            f"query_params={self.query_params!r}, "
            f"resolved_entry_names=<{entry_count} entries>)"
        )


# ---------------------------------------------------------------------------
# SkimSchema
# ---------------------------------------------------------------------------


@dataclass
class SkimSchema:
    """Schema definition for ROOT skim output files.

    A *skim* is a ROOT ``TTree`` written by ``RootOutputSink`` that contains
    one row per selected event.  This schema records the expected file name,
    tree name, and (optionally) the set of branches that must be present.

    Attributes
    ----------
    schema_version : int
        Schema format version.  Must equal :attr:`CURRENT_VERSION`.
    output_file : str
        Path (or pattern) of the output ROOT file.
    tree_name : str
        Name of the ``TTree`` inside the ROOT file. Defaults to ``"Events"``.
    branches : list[str]
        Expected branch names.  An empty list means *all* branches are
        accepted and no branch-level validation is performed.
    """

    CURRENT_VERSION: ClassVar[int] = SKIM_SCHEMA_VERSION

    schema_version: int = SKIM_SCHEMA_VERSION
    output_file: str = ""
    tree_name: str = "Events"
    branches: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain Python dict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "SkimSchema":
        """Construct a :class:`SkimSchema` from a dict, ignoring unknown keys."""
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        """Return a list of validation error strings (empty = valid)."""
        errors: List[str] = []
        if self.schema_version != self.CURRENT_VERSION:
            errors.append(
                f"SkimSchema version mismatch: file has {self.schema_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )
        if not self.output_file:
            errors.append("SkimSchema.output_file must not be empty.")
        if not self.tree_name:
            errors.append("SkimSchema.tree_name must not be empty.")
        return errors


# ---------------------------------------------------------------------------
# HistogramAxisSpec
# ---------------------------------------------------------------------------


@dataclass
class HistogramAxisSpec:
    """Axis specification for a single dimension of a histogram.

    Attributes
    ----------
    variable : str
        Name of the branch / column used to fill this axis.
    bins : int
        Number of bins.
    lower_bound : float
        Lower edge of the axis range.
    upper_bound : float
        Upper edge of the axis range.
    label : str
        Human-readable axis label.
    """

    variable: str = ""
    bins: int = 0
    lower_bound: float = 0.0
    upper_bound: float = 1.0
    label: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HistogramAxisSpec":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    def validate(self) -> List[str]:
        errors: List[str] = []
        if not self.variable:
            errors.append("HistogramAxisSpec.variable must not be empty.")
        if self.bins <= 0:
            errors.append(
                f"HistogramAxisSpec.bins must be > 0 (got {self.bins})."
            )
        if self.lower_bound >= self.upper_bound:
            errors.append(
                f"HistogramAxisSpec.lower_bound ({self.lower_bound}) must be "
                f"less than upper_bound ({self.upper_bound})."
            )
        return errors


# ---------------------------------------------------------------------------
# HistogramSchema
# ---------------------------------------------------------------------------


@dataclass
class HistogramSchema:
    """Schema definition for ROOT histogram output files.

    ``NDHistogramManager`` saves N-dimensional ``THnSparseF`` objects into the
    *meta* output ROOT file.  This schema records the expected file name, the
    histogram names, and the axis specifications that downstream tools can
    rely on.

    Attributes
    ----------
    schema_version : int
        Schema format version.  Must equal :attr:`CURRENT_VERSION`.
    output_file : str
        Path (or pattern) of the meta-output ROOT file that contains the
        histograms.
    histogram_names : list[str]
        Names of the ``THnSparseF`` objects expected in the file.
    axes : list[HistogramAxisSpec]
        Axis specifications shared across all histograms in this schema.
        Individual histograms may add extra axes; these represent the common
        (required) axes.
    """

    CURRENT_VERSION: ClassVar[int] = HISTOGRAM_SCHEMA_VERSION

    schema_version: int = HISTOGRAM_SCHEMA_VERSION
    output_file: str = ""
    histogram_names: List[str] = field(default_factory=list)
    axes: List[HistogramAxisSpec] = field(default_factory=list)

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        d = {
            "schema_version": self.schema_version,
            "output_file": self.output_file,
            "histogram_names": list(self.histogram_names),
            "axes": [ax.to_dict() for ax in self.axes],
        }
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "HistogramSchema":
        axes = [
            HistogramAxisSpec.from_dict(ax)
            for ax in data.get("axes", [])
        ]
        return cls(
            schema_version=data.get("schema_version", HISTOGRAM_SCHEMA_VERSION),
            output_file=data.get("output_file", ""),
            histogram_names=list(data.get("histogram_names", [])),
            axes=axes,
        )

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        errors: List[str] = []
        if self.schema_version != self.CURRENT_VERSION:
            errors.append(
                f"HistogramSchema version mismatch: file has {self.schema_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )
        if not self.output_file:
            errors.append("HistogramSchema.output_file must not be empty.")
        for i, ax in enumerate(self.axes):
            for e in ax.validate():
                errors.append(f"HistogramSchema.axes[{i}]: {e}")
        return errors


# ---------------------------------------------------------------------------
# MetadataSchema
# ---------------------------------------------------------------------------


@dataclass
class MetadataSchema:
    """Schema definition for the provenance metadata output.

    ``ProvenanceService`` writes ``TNamed`` objects into a ``TDirectory``
    named ``"provenance"`` inside the meta-output ROOT file.  This schema
    records the expected file name, directory name, and required/optional
    provenance keys.

    Attributes
    ----------
    schema_version : int
        Schema format version.  Must equal :attr:`CURRENT_VERSION`.
    output_file : str
        Path (or pattern) of the meta-output ROOT file.
    provenance_dir : str
        Name of the ``TDirectory`` that holds the provenance objects.
        Defaults to ``"provenance"``.
    required_keys : list[str]
        Provenance keys that must be present.  Defaults to
        :data:`PROVENANCE_REQUIRED_KEYS`.
    optional_keys : list[str]
        Provenance keys that may be present.  Defaults to
        :data:`PROVENANCE_OPTIONAL_KEYS`.
    """

    CURRENT_VERSION: ClassVar[int] = METADATA_SCHEMA_VERSION

    schema_version: int = METADATA_SCHEMA_VERSION
    output_file: str = ""
    provenance_dir: str = "provenance"
    required_keys: List[str] = field(
        default_factory=lambda: list(PROVENANCE_REQUIRED_KEYS)
    )
    optional_keys: List[str] = field(
        default_factory=lambda: list(PROVENANCE_OPTIONAL_KEYS)
    )

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MetadataSchema":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        errors: List[str] = []
        if self.schema_version != self.CURRENT_VERSION:
            errors.append(
                f"MetadataSchema version mismatch: file has {self.schema_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )
        if not self.output_file:
            errors.append("MetadataSchema.output_file must not be empty.")
        if not self.provenance_dir:
            errors.append("MetadataSchema.provenance_dir must not be empty.")
        return errors


# ---------------------------------------------------------------------------
# CutflowSchema
# ---------------------------------------------------------------------------


@dataclass
class CutflowSchema:
    """Schema definition for cutflow / event-count output.

    ``CounterService`` books and writes event-count histograms for each
    registered sample.  The cutflow schema records the expected output file,
    and the ordered list of counter keys that downstream tools expect to
    find (e.g. ``"<sample>.total"``, ``"<sample>.weighted"``).

    Attributes
    ----------
    schema_version : int
        Schema format version.  Must equal :attr:`CURRENT_VERSION`.
    output_file : str
        Path (or pattern) of the ROOT file that contains the counter objects.
        This is typically the same as the meta-output file.
    counter_keys : list[str]
        Ordered counter key names expected in the output.  An empty list
        means no key-level validation is performed.
    """

    CURRENT_VERSION: ClassVar[int] = CUTFLOW_SCHEMA_VERSION

    schema_version: int = CUTFLOW_SCHEMA_VERSION
    output_file: str = ""
    counter_keys: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CutflowSchema":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        errors: List[str] = []
        if self.schema_version != self.CURRENT_VERSION:
            errors.append(
                f"CutflowSchema version mismatch: file has {self.schema_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )
        if not self.output_file:
            errors.append("CutflowSchema.output_file must not be empty.")
        return errors


# ---------------------------------------------------------------------------
# LawArtifactSchema
# ---------------------------------------------------------------------------


@dataclass
class LawArtifactSchema:
    """Schema definition for a single LAW task artifact.

    Law workflow tasks (``PrepareNANOSample``, ``BuildNANOSubmission``,
    ``SubmitNANOJobs``, ``MonitorNANOJobs``, ``RunNANOAnalysisJob``, …)
    each produce one or more output files.  This schema records the expected
    artifact type, path pattern, and serialisation format.

    Attributes
    ----------
    schema_version : int
        Schema format version.  Must equal :attr:`CURRENT_VERSION`.
    artifact_type : str
        Logical type of the artifact.  Must be one of
        :data:`LAW_ARTIFACT_TYPES`.
    path_pattern : str
        Glob-compatible path pattern for the expected output file(s),
        e.g. ``"branch_outputs/sample_*.json"``.
    format : str
        Serialisation format of the artifact file.  One of
        ``"json"``, ``"text"``, ``"shell"``, or ``"root"``.
    """

    CURRENT_VERSION: ClassVar[int] = LAW_ARTIFACT_SCHEMA_VERSION

    schema_version: int = LAW_ARTIFACT_SCHEMA_VERSION
    artifact_type: str = ""
    path_pattern: str = ""
    format: str = "text"  # noqa: A003 (shadows built-in 'format')

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "LawArtifactSchema":
        known = {f.name for f in fields(cls)}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        errors: List[str] = []
        if self.schema_version != self.CURRENT_VERSION:
            errors.append(
                f"LawArtifactSchema version mismatch: file has {self.schema_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )
        if not self.artifact_type:
            errors.append("LawArtifactSchema.artifact_type must not be empty.")
        elif self.artifact_type not in LAW_ARTIFACT_TYPES:
            errors.append(
                f"LawArtifactSchema.artifact_type '{self.artifact_type}' is not "
                f"a recognised type.  Known types: {LAW_ARTIFACT_TYPES}."
            )
        if not self.path_pattern:
            errors.append("LawArtifactSchema.path_pattern must not be empty.")
        valid_formats = {"json", "text", "shell", "root"}
        if self.format not in valid_formats:
            errors.append(
                f"LawArtifactSchema.format '{self.format}' is not recognised.  "
                f"Valid formats: {sorted(valid_formats)}."
            )
        return errors


# ---------------------------------------------------------------------------
# OutputManifest
# ---------------------------------------------------------------------------


class OutputManifest:
    r"""Manifest that combines the schema definitions for a single analysis job.

    An :class:`OutputManifest` can be serialised to YAML so that downstream
    tools (datacards, plotting scripts, statistical frameworks) can discover
    the exact format contract of the outputs they consume.

    Attributes
    ----------
    manifest_version : int
        Manifest container version (not a per-schema version).  Must equal
        :attr:`CURRENT_VERSION`.
    skim : SkimSchema or None
        Schema for the skim ROOT file, if this job produces one.
    histograms : HistogramSchema or None
        Schema for the histogram ROOT file, if applicable.
    metadata : MetadataSchema or None
        Schema for the provenance metadata, if applicable.
    cutflow : CutflowSchema or None
        Schema for the cutflow output, if applicable.
    law_artifacts : list[LawArtifactSchema]
        Schemas for any LAW task artifacts produced by this job.
    framework_hash : str or None
        Git commit hash of the RDFAnalyzerCore framework at job-submission
        time.  Used by :meth:`provenance` and :func:`resolve_manifest` to
        detect environment drift.
    user_repo_hash : str or None
        Git commit hash of the user analysis repository at job-submission
        time.
    config_mtime : str or None
        UTC modification time (ISO 8601) of the job configuration file.
    dataset_manifest_provenance : DatasetManifestProvenance or None
        Identity and selection record for the dataset manifest used in this
        job.  Records the manifest file hash, the query parameters applied
        to select a subset of datasets, and the names of the resolved
        dataset entries.  ``None`` when no dataset manifest was used or the
        information was not captured.
    """

    CURRENT_VERSION: ClassVar[int] = OUTPUT_MANIFEST_VERSION

    def __init__(
        self,
        manifest_version: int = OUTPUT_MANIFEST_VERSION,
        skim: Optional[SkimSchema] = None,
        histograms: Optional[HistogramSchema] = None,
        metadata: Optional[MetadataSchema] = None,
        cutflow: Optional[CutflowSchema] = None,
        law_artifacts: Optional[List[LawArtifactSchema]] = None,
        framework_hash: Optional[str] = None,
        user_repo_hash: Optional[str] = None,
        config_mtime: Optional[str] = None,
        dataset_manifest_provenance: Optional["DatasetManifestProvenance"] = None,
    ) -> None:
        self.manifest_version: int = manifest_version
        self.skim: Optional[SkimSchema] = skim
        self.histograms: Optional[HistogramSchema] = histograms
        self.metadata: Optional[MetadataSchema] = metadata
        self.cutflow: Optional[CutflowSchema] = cutflow
        self.law_artifacts: List[LawArtifactSchema] = law_artifacts or []
        self.framework_hash: Optional[str] = framework_hash
        self.user_repo_hash: Optional[str] = user_repo_hash
        self.config_mtime: Optional[str] = config_mtime
        self.dataset_manifest_provenance: Optional[DatasetManifestProvenance] = (
            dataset_manifest_provenance
        )

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the manifest to a plain Python dict."""
        return {
            "manifest_version": self.manifest_version,
            "skim": self.skim.to_dict() if self.skim is not None else None,
            "histograms": (
                self.histograms.to_dict() if self.histograms is not None else None
            ),
            "metadata": (
                self.metadata.to_dict() if self.metadata is not None else None
            ),
            "cutflow": (
                self.cutflow.to_dict() if self.cutflow is not None else None
            ),
            "law_artifacts": [a.to_dict() for a in self.law_artifacts],
            "framework_hash": self.framework_hash,
            "user_repo_hash": self.user_repo_hash,
            "config_mtime": self.config_mtime,
            "dataset_manifest_provenance": (
                self.dataset_manifest_provenance.to_dict()
                if self.dataset_manifest_provenance is not None
                else None
            ),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "OutputManifest":
        """Construct an :class:`OutputManifest` from a plain dict."""
        skim = (
            SkimSchema.from_dict(data["skim"])
            if data.get("skim") is not None
            else None
        )
        histograms = (
            HistogramSchema.from_dict(data["histograms"])
            if data.get("histograms") is not None
            else None
        )
        metadata = (
            MetadataSchema.from_dict(data["metadata"])
            if data.get("metadata") is not None
            else None
        )
        cutflow = (
            CutflowSchema.from_dict(data["cutflow"])
            if data.get("cutflow") is not None
            else None
        )
        law_artifacts = [
            LawArtifactSchema.from_dict(a)
            for a in data.get("law_artifacts", [])
        ]
        dmp_data = data.get("dataset_manifest_provenance")
        dataset_manifest_provenance = (
            DatasetManifestProvenance.from_dict(dmp_data)
            if dmp_data is not None
            else None
        )
        return cls(
            manifest_version=data.get("manifest_version", OUTPUT_MANIFEST_VERSION),
            skim=skim,
            histograms=histograms,
            metadata=metadata,
            cutflow=cutflow,
            law_artifacts=law_artifacts,
            framework_hash=data.get("framework_hash"),
            user_repo_hash=data.get("user_repo_hash"),
            config_mtime=data.get("config_mtime"),
            dataset_manifest_provenance=dataset_manifest_provenance,
        )

    def save_yaml(self, path: str) -> None:
        """Serialise the manifest to a YAML file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w") as fh:
            yaml.dump(
                self.to_dict(),
                fh,
                default_flow_style=False,
                sort_keys=False,
                allow_unicode=True,
            )

    @classmethod
    def load_yaml(cls, path: str) -> "OutputManifest":
        """Load an :class:`OutputManifest` from a YAML file.

        Parameters
        ----------
        path : str
            Path to the manifest YAML file.

        Raises
        ------
        ValueError
            If the file content is not a YAML mapping.
        """
        with open(path) as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, dict):
            raise ValueError(
                f"Output manifest '{path}' must be a YAML mapping at the top level."
            )
        return cls.from_dict(raw)

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        """Validate all contained schemas.

        Returns
        -------
        list[str]
            A (possibly empty) list of error strings.  An empty list means
            the manifest is valid.
        """
        errors: List[str] = []

        if self.manifest_version != self.CURRENT_VERSION:
            errors.append(
                f"OutputManifest version mismatch: file has {self.manifest_version}, "
                f"code expects {self.CURRENT_VERSION}."
            )

        if all(
            x is None
            for x in (self.skim, self.histograms, self.metadata, self.cutflow)
        ) and not self.law_artifacts:
            errors.append(
                "OutputManifest must define at least one output schema "
                "(skim, histograms, metadata, cutflow, or law_artifacts)."
            )

        for schema in (self.skim, self.histograms, self.metadata, self.cutflow):
            if schema is not None:
                errors.extend(schema.validate())

        for i, artifact in enumerate(self.law_artifacts):
            for e in artifact.validate():
                errors.append(f"law_artifacts[{i}]: {e}")

        return errors

    @staticmethod
    def check_version_compatibility(manifest: "OutputManifest") -> None:
        """Raise :class:`SchemaVersionError` if any schema in the manifest has
        a version that does not match the current code.

        This is intended to be called when loading an existing manifest before
        consuming its outputs, so that version mismatches are surfaced clearly
        rather than silently ignored.

        Parameters
        ----------
        manifest : OutputManifest
            The loaded manifest to check.

        Raises
        ------
        SchemaVersionError
            If any schema version does not match the corresponding
            ``CURRENT_VERSION``.
        """
        mismatches: List[str] = []

        if manifest.manifest_version != OutputManifest.CURRENT_VERSION:
            mismatches.append(
                f"OutputManifest: stored={manifest.manifest_version}, "
                f"current={OutputManifest.CURRENT_VERSION}"
            )
        for name, schema, current in (
            ("skim", manifest.skim, SKIM_SCHEMA_VERSION),
            ("histograms", manifest.histograms, HISTOGRAM_SCHEMA_VERSION),
            ("metadata", manifest.metadata, METADATA_SCHEMA_VERSION),
            ("cutflow", manifest.cutflow, CUTFLOW_SCHEMA_VERSION),
        ):
            if schema is not None and schema.schema_version != current:
                mismatches.append(
                    f"{name}: stored={schema.schema_version}, current={current}"
                )
        for i, artifact in enumerate(manifest.law_artifacts):
            if artifact.schema_version != LAW_ARTIFACT_SCHEMA_VERSION:
                mismatches.append(
                    f"law_artifacts[{i}]: stored={artifact.schema_version}, "
                    f"current={LAW_ARTIFACT_SCHEMA_VERSION}"
                )

        if mismatches:
            raise SchemaVersionError(
                "Output schema version mismatch(es) detected:\n"
                + "\n".join(f"  {m}" for m in mismatches)
            )

    def __repr__(self) -> str:  # pragma: no cover
        parts = [
            f"manifest_version={self.manifest_version}",
            f"skim={'set' if self.skim else 'None'}",
            f"histograms={'set' if self.histograms else 'None'}",
            f"metadata={'set' if self.metadata else 'None'}",
            f"cutflow={'set' if self.cutflow else 'None'}",
            f"law_artifacts={len(self.law_artifacts)}",
        ]
        return f"OutputManifest({', '.join(parts)})"

    # ------------------------------------------------------------------ provenance

    def provenance(self) -> "ProvenanceRecord":
        """Return the provenance recorded in this manifest.

        Constructs a :class:`ProvenanceRecord` from the
        ``framework_hash``, ``user_repo_hash``, and ``config_mtime``
        fields stored in the manifest.

        Returns
        -------
        ProvenanceRecord
            The provenance snapshot recorded when this manifest was written.
            Fields that were not recorded will be ``None``.
        """
        return ProvenanceRecord(
            framework_hash=self.framework_hash,
            user_repo_hash=self.user_repo_hash,
            config_mtime=self.config_mtime,
        )

    def resolve(
        self,
        current_provenance: Optional["ProvenanceRecord"] = None,
    ) -> Dict[str, "ArtifactResolutionStatus"]:
        """Resolve all schemas using the canonical version-resolution model.

        This is a convenience wrapper around :func:`resolve_manifest` that
        uses the provenance stored in *this* manifest as the recorded
        provenance.

        Parameters
        ----------
        current_provenance:
            A :class:`ProvenanceRecord` representing the current state of
            the environment (e.g. current git hashes).  When ``None`` the
            resolution only considers schema-version compatibility.

        Returns
        -------
        dict[str, ArtifactResolutionStatus]
            Mapping of schema role to its :class:`ArtifactResolutionStatus`.
        """
        return resolve_manifest(self, current_provenance=current_provenance)


# ---------------------------------------------------------------------------
# SchemaVersionError
# ---------------------------------------------------------------------------


class SchemaVersionError(RuntimeError):
    """Raised when a loaded schema version does not match the current code.

    Catching :class:`SchemaVersionError` separately from other
    :class:`RuntimeError` exceptions allows callers to apply migration logic
    or emit clear user-facing messages about incompatible output formats.
    """


# ---------------------------------------------------------------------------
# Canonical resolution API
# ---------------------------------------------------------------------------


def resolve_artifact(
    artifact: Any,
    recorded_provenance: Optional[ProvenanceRecord] = None,
    current_provenance: Optional[ProvenanceRecord] = None,
) -> ArtifactResolutionStatus:
    """Determine whether a single schema artifact is compatible, stale, or must be regenerated.

    This is the **canonical API** for artifact version resolution.  It is
    designed to be called by LAW tasks, caching layers, validation tools, and
    any downstream consumer that needs to decide whether to reuse or
    regenerate an artifact.

    Resolution rules (applied in order):

    1. **MUST_REGENERATE** - the artifact's ``schema_version`` does not match
       its ``CURRENT_VERSION`` class attribute.  The artifact is incompatible
       with the current codebase and **must** be regenerated.
    2. **STALE** - the schema version is current, but *recorded_provenance*
       and *current_provenance* are both provided and
       :meth:`ProvenanceRecord.matches` returns ``False``.  The artifact can
       still be used but should be regenerated when convenient.
    3. **COMPATIBLE** - the schema version is current and either no provenance
       comparison was requested or the provenances match.

    Parameters
    ----------
    artifact:
        A schema object with ``schema_version`` and ``CURRENT_VERSION``
        attributes (e.g. :class:`SkimSchema`, :class:`HistogramSchema`,
        :class:`LawArtifactSchema`).
    recorded_provenance:
        The :class:`ProvenanceRecord` that was captured when the artifact was
        produced (e.g. extracted from :meth:`OutputManifest.provenance`).
        May be ``None`` to skip provenance comparison.
    current_provenance:
        A :class:`ProvenanceRecord` representing the current environment
        state.  May be ``None`` to skip provenance comparison.

    Returns
    -------
    ArtifactResolutionStatus
        One of :attr:`~ArtifactResolutionStatus.COMPATIBLE`,
        :attr:`~ArtifactResolutionStatus.STALE`, or
        :attr:`~ArtifactResolutionStatus.MUST_REGENERATE`.
    """
    current_version = getattr(artifact, "CURRENT_VERSION", None)
    schema_version = getattr(artifact, "schema_version", None)
    if current_version is None or schema_version != current_version:
        return ArtifactResolutionStatus.MUST_REGENERATE

    if (
        recorded_provenance is not None
        and current_provenance is not None
        and not recorded_provenance.matches(current_provenance)
    ):
        return ArtifactResolutionStatus.STALE

    return ArtifactResolutionStatus.COMPATIBLE


def resolve_manifest(
    manifest: "OutputManifest",
    current_provenance: Optional[ProvenanceRecord] = None,
) -> Dict[str, ArtifactResolutionStatus]:
    """Resolve all schemas in a manifest using the canonical resolution model.

    Iterates over every schema in *manifest* and calls
    :func:`resolve_artifact` for each one, using the provenance stored in the
    manifest as the *recorded_provenance*.

    Parameters
    ----------
    manifest:
        The :class:`OutputManifest` whose schemas should be resolved.
    current_provenance:
        A :class:`ProvenanceRecord` representing the current environment
        state.  When ``None`` the resolution only considers schema-version
        compatibility (provenance staleness is not checked).

    Returns
    -------
    dict[str, ArtifactResolutionStatus]
        Mapping of schema role to its :class:`ArtifactResolutionStatus`.
        The roles used are ``"skim"``, ``"histograms"``, ``"metadata"``,
        ``"cutflow"``, and ``"law_artifacts[N]"`` for the Nth LAW artifact.
    """
    recorded = manifest.provenance()
    statuses: Dict[str, ArtifactResolutionStatus] = {}

    for role, schema in (
        ("skim", manifest.skim),
        ("histograms", manifest.histograms),
        ("metadata", manifest.metadata),
        ("cutflow", manifest.cutflow),
    ):
        if schema is not None:
            statuses[role] = resolve_artifact(schema, recorded, current_provenance)

    for i, artifact in enumerate(manifest.law_artifacts):
        statuses[f"law_artifacts[{i}]"] = resolve_artifact(
            artifact, recorded, current_provenance
        )

    return statuses


# ---------------------------------------------------------------------------
# Schema-aware merge and validation contracts
# ---------------------------------------------------------------------------


class MergeInputValidationError(RuntimeError):
    """Raised when one or more merge inputs fail schema validation.

    This exception bundles all diagnostics from all invalid inputs so that
    callers can report every problem in a single error message rather than
    surfacing failures one at a time.

    Catching :class:`MergeInputValidationError` separately from other
    :class:`RuntimeError` exceptions allows callers to apply fallback logic
    or emit structured user-facing messages about incompatible merge inputs.
    """


def validate_merge_inputs(
    manifests: List["OutputManifest"],
    required_roles: Optional[List[str]] = None,
) -> List[str]:
    """Validate a collection of manifests for merge compatibility.

    This is the **canonical pre-merge validation API**.  Call this function
    before executing any merge or reduction step (e.g. ``hadd``, histogram
    addition) to ensure that all inputs agree on schema versions and artifact
    structure.  If the returned list is non-empty, the merge must not proceed.

    Checks performed (in order):

    1. At least one manifest is provided.
    2. Every manifest passes its own :meth:`OutputManifest.validate` check
       (no empty or malformed manifests).
    3. Every manifest passes :meth:`OutputManifest.check_version_compatibility`
       (no schema versions that differ from the current code).
    4. All manifests expose the **same set** of scalar artifact roles
       (``skim``, ``histograms``, ``metadata``, ``cutflow``): a role that is
       present in one manifest must be present in all of them.
    5. For each shared scalar role, all manifests carry the **same**
       ``schema_version`` value.
    6. All manifests contain the **same number** of ``law_artifacts``.
    7. Corresponding ``law_artifacts`` entries share the same
       ``schema_version``.
    8. If *required_roles* is supplied, every listed role must be present in
       every manifest.

    Parameters
    ----------
    manifests:
        Sequence of :class:`OutputManifest` objects to validate before
        merging.  Must be non-empty.
    required_roles:
        Optional list of role names that must be present in every manifest.
        Valid scalar role names are ``"skim"``, ``"histograms"``,
        ``"metadata"``, ``"cutflow"``.  Use ``"law_artifacts"`` to require
        at least one LAW artifact in every manifest.

    Returns
    -------
    list[str]
        A (possibly empty) list of human-readable error strings.  An empty
        list means all inputs are compatible and the merge may proceed.

    Examples
    --------
    Validate before merging histogram outputs::

        from output_schema import validate_merge_inputs, MergeInputValidationError

        errors = validate_merge_inputs(manifests, required_roles=["histograms"])
        if errors:
            raise MergeInputValidationError(
                "Pre-merge validation failed:\n" + "\n".join(errors)
            )
        # safe to proceed with hadd / histogram addition
    """
    errors: List[str] = []

    if not manifests:
        errors.append("validate_merge_inputs: no manifests provided.")
        return errors

    # ------------------------------------------------------------------
    # Rule 2: per-manifest structural validation
    # ------------------------------------------------------------------
    for i, m in enumerate(manifests):
        per_errors = m.validate()
        for e in per_errors:
            errors.append(f"manifest[{i}]: {e}")

    # ------------------------------------------------------------------
    # Rule 3: per-manifest schema version compatibility
    # ------------------------------------------------------------------
    for i, m in enumerate(manifests):
        try:
            OutputManifest.check_version_compatibility(m)
        except SchemaVersionError as exc:
            errors.append(
                f"manifest[{i}] has incompatible schema version(s): {exc}"
            )

    # ------------------------------------------------------------------
    # Rules 4–5: scalar role consistency
    # ------------------------------------------------------------------
    _scalar_roles = ("skim", "histograms", "metadata", "cutflow")
    ref = manifests[0]

    for role in _scalar_roles:
        ref_schema = getattr(ref, role)
        ref_present = ref_schema is not None
        for i, m in enumerate(manifests[1:], start=1):
            m_schema = getattr(m, role)
            m_present = m_schema is not None
            if ref_present != m_present:
                errors.append(
                    f"Inconsistent presence of '{role}' across inputs: "
                    f"manifest[0]={'present' if ref_present else 'absent'}, "
                    f"manifest[{i}]={'present' if m_present else 'absent'}."
                )
            elif ref_present and m_present:
                if ref_schema.schema_version != m_schema.schema_version:
                    errors.append(
                        f"Inconsistent schema_version for '{role}': "
                        f"manifest[0]={ref_schema.schema_version}, "
                        f"manifest[{i}]={m_schema.schema_version}."
                    )

    # ------------------------------------------------------------------
    # Rules 6–7: law_artifacts count and version consistency
    # ------------------------------------------------------------------
    ref_count = len(ref.law_artifacts)
    for i, m in enumerate(manifests[1:], start=1):
        if len(m.law_artifacts) != ref_count:
            errors.append(
                f"Inconsistent law_artifacts count: "
                f"manifest[0]={ref_count}, manifest[{i}]={len(m.law_artifacts)}."
            )
        else:
            for j, (ref_a, m_a) in enumerate(
                zip(ref.law_artifacts, m.law_artifacts)
            ):
                if ref_a.schema_version != m_a.schema_version:
                    errors.append(
                        f"Inconsistent schema_version for law_artifacts[{j}]: "
                        f"manifest[0]={ref_a.schema_version}, "
                        f"manifest[{i}]={m_a.schema_version}."
                    )

    # ------------------------------------------------------------------
    # Rule 8: required roles
    # ------------------------------------------------------------------
    if required_roles:
        _valid_scalar = set(_scalar_roles)
        for i, m in enumerate(manifests):
            for role in required_roles:
                if role in _valid_scalar:
                    if getattr(m, role) is None:
                        errors.append(
                            f"manifest[{i}]: required role '{role}' is not present."
                        )
                elif role == "law_artifacts":
                    if not m.law_artifacts:
                        errors.append(
                            f"manifest[{i}]: required role 'law_artifacts' is empty."
                        )
                else:
                    errors.append(
                        f"Unknown required role '{role}'. "
                        f"Valid roles: {sorted(_valid_scalar | {'law_artifacts'})}."
                    )

    return errors


def merge_manifests(
    manifests: List["OutputManifest"],
    framework_hash: Optional[str] = None,
    user_repo_hash: Optional[str] = None,
    required_roles: Optional[List[str]] = None,
) -> "OutputManifest":
    """Build a merged :class:`OutputManifest` from a collection of validated inputs.

    Validates all inputs with :func:`validate_merge_inputs` before proceeding.
    The merged manifest carries the same schema definitions as the inputs
    (schema types and versions must all agree) and is tagged with the supplied
    provenance information.

    The schema definitions (including ``output_file`` path patterns) are
    copied from the **first** manifest.  After the merge operation (e.g.
    ``hadd``) has written its output files, callers should update the
    ``output_file`` fields on the returned manifest's schemas to point to the
    merged output path before serialising the manifest.

    Parameters
    ----------
    manifests:
        Non-empty sequence of :class:`OutputManifest` objects to merge.
        Every manifest must pass :func:`validate_merge_inputs`; if any
        validation check fails a :exc:`MergeInputValidationError` is raised
        **before** any merge is attempted.
    framework_hash:
        Git hash to record in the merged manifest's provenance, or ``None``.
    user_repo_hash:
        User-repository git hash for the merged manifest's provenance,
        or ``None``.
    required_roles:
        Forwarded to :func:`validate_merge_inputs`.  Use this to assert that
        specific artifact types (e.g. ``"histograms"``) must be present in
        every input.

    Returns
    -------
    OutputManifest
        A new manifest describing the merged output.  Schema definitions are
        copied from the first input (all inputs must agree on schema versions
        after validation).

    Raises
    ------
    MergeInputValidationError
        If :func:`validate_merge_inputs` returns any errors.

    Examples
    --------
    Merge histogram manifests from multiple batch jobs::

        from output_schema import (
            OutputManifest, merge_manifests, MergeInputValidationError
        )

        manifests = [OutputManifest.load_yaml(p) for p in manifest_paths]
        try:
            merged = merge_manifests(
                manifests,
                framework_hash=current_fw_hash,
                required_roles=["histograms"],
            )
        except MergeInputValidationError as exc:
            print(f"Cannot merge: {exc}")
            raise
        # Now run hadd / histogram addition …
        merged.histograms.output_file = "merged_meta.root"
        merged.save_yaml("merged/output_manifest.yaml")
    """
    errors = validate_merge_inputs(manifests, required_roles=required_roles)
    if errors:
        raise MergeInputValidationError(
            "Merge inputs failed schema validation:\n"
            + "\n".join(f"  {e}" for e in errors)
        )

    # Use the first manifest as the structural template — all inputs have
    # agreed on schema versions and roles after validation above.
    source = manifests[0]
    return OutputManifest(
        skim=source.skim,
        histograms=source.histograms,
        metadata=source.metadata,
        cutflow=source.cutflow,
        law_artifacts=list(source.law_artifacts),
        framework_hash=framework_hash,
        user_repo_hash=user_repo_hash,
    )


# ---------------------------------------------------------------------------
# Cached artifact sidecar format
# ---------------------------------------------------------------------------

#: Filename suffix appended to any artifact path to form its cache sidecar.
#: For example, ``"presel.root"`` gets a sidecar at ``"presel.root.cache.yaml"``.
CACHE_SIDECAR_SUFFIX: str = ".cache.yaml"


class CachedArtifact:
    """A cached artifact with attached schema and provenance metadata.

    Represents an intermediate result that has been written to disk alongside
    a sidecar file recording its schema type, version, and the provenance
    context at the time of caching.  Use :func:`write_cache_sidecar` to
    persist and :func:`read_cache_sidecar` to reload.

    Cache validity is determined by :func:`check_cache_validity`, which
    checks **both** schema-version compatibility and provenance/version
    compatibility — not only file timestamps or filenames.

    Parameters
    ----------
    artifact_path:
        Absolute or relative path to the cached artifact file.
    manifest:
        :class:`OutputManifest` describing the schemas of all artifacts
        produced in the same job/task as this cached artifact.
    cached_at:
        ISO 8601 timestamp string (UTC) recording when the artifact was
        cached.  Defaults to the current UTC time when ``None``.

    Examples
    --------
    Write a cache entry after producing an intermediate artifact::

        from output_schema import (
            OutputManifest, SkimSchema, ProvenanceRecord,
            write_cache_sidecar, check_cache_validity,
            ArtifactResolutionStatus,
        )

        manifest = OutputManifest(
            skim=SkimSchema(output_file="presel.root"),
            framework_hash="abc123",
        )
        write_cache_sidecar("presel.root", manifest)

        # Later, check whether the cache is still valid:
        current = ProvenanceRecord(framework_hash="abc123")
        status = check_cache_validity("presel.root", current_provenance=current)
        if status == ArtifactResolutionStatus.COMPATIBLE:
            pass  # reuse the cache
        elif status == ArtifactResolutionStatus.MUST_REGENERATE:
            pass  # must regenerate
    """

    def __init__(
        self,
        artifact_path: str,
        manifest: "OutputManifest",
        cached_at: Optional[str] = None,
    ) -> None:
        self.artifact_path: str = artifact_path
        self.manifest: OutputManifest = manifest
        self.cached_at: str = (
            cached_at
            or datetime.datetime.now(datetime.timezone.utc).isoformat()
        )

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON/YAML-serialisable dictionary representation."""
        return {
            "artifact_path": self.artifact_path,
            "cached_at": self.cached_at,
            "manifest": self.manifest.to_dict(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "CachedArtifact":
        """Construct from a dict produced by :meth:`to_dict`."""
        manifest = OutputManifest.from_dict(data["manifest"])
        return cls(
            artifact_path=data.get("artifact_path", ""),
            manifest=manifest,
            cached_at=data.get("cached_at"),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CachedArtifact("
            f"artifact_path={self.artifact_path!r}, "
            f"cached_at={self.cached_at!r})"
        )


def write_cache_sidecar(
    artifact_path: str,
    manifest: "OutputManifest",
    cached_at: Optional[str] = None,
) -> str:
    """Write a schema-and-provenance sidecar file alongside a cached artifact.

    Records the artifact's schema and provenance metadata in a YAML sidecar
    file at ``{artifact_path}{CACHE_SIDECAR_SUFFIX}`` so that downstream
    consumers can validate the cache without relying on file timestamps or
    filenames alone.

    Parameters
    ----------
    artifact_path:
        Path to the artifact file.  The sidecar is written to
        ``{artifact_path}{CACHE_SIDECAR_SUFFIX}`` (e.g.
        ``"output/sample_0.root.cache.yaml"``).
    manifest:
        :class:`OutputManifest` describing the schemas and provenance for
        the artifact.
    cached_at:
        Optional ISO 8601 UTC timestamp string for the cache entry.
        Defaults to the current UTC time.

    Returns
    -------
    str
        The absolute path of the written sidecar file.
    """
    entry = CachedArtifact(
        artifact_path=artifact_path,
        manifest=manifest,
        cached_at=cached_at,
    )
    sidecar_path = artifact_path + CACHE_SIDECAR_SUFFIX
    os.makedirs(os.path.dirname(os.path.abspath(sidecar_path)), exist_ok=True)
    with open(sidecar_path, "w") as fh:
        yaml.dump(
            entry.to_dict(),
            fh,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
    return sidecar_path


def read_cache_sidecar(artifact_path: str) -> "CachedArtifact":
    """Read the cache sidecar for an artifact.

    Parameters
    ----------
    artifact_path:
        Path to the artifact file.  The sidecar is expected at
        ``{artifact_path}{CACHE_SIDECAR_SUFFIX}``.

    Returns
    -------
    CachedArtifact
        The cached artifact metadata loaded from the sidecar.

    Raises
    ------
    FileNotFoundError
        If no sidecar file exists for *artifact_path*.
    ValueError
        If the sidecar file content is not a valid YAML mapping.
    """
    sidecar_path = artifact_path + CACHE_SIDECAR_SUFFIX
    if not os.path.exists(sidecar_path):
        raise FileNotFoundError(
            f"No cache sidecar found for {artifact_path!r}. "
            f"Expected sidecar at {sidecar_path!r}."
        )
    with open(sidecar_path) as fh:
        data = yaml.safe_load(fh)
    if not isinstance(data, dict):
        raise ValueError(
            f"Cache sidecar {sidecar_path!r} must be a YAML mapping at the "
            f"top level."
        )
    return CachedArtifact.from_dict(data)


def check_cache_validity(
    artifact_path: str,
    current_provenance: Optional["ProvenanceRecord"] = None,
    strict: bool = False,
) -> "ArtifactResolutionStatus":
    """Check whether a cached artifact is still valid.

    Reads the cache sidecar for *artifact_path* and uses the canonical
    resolution model (:func:`resolve_manifest`) to determine whether the
    artifact is compatible, stale, or must be regenerated.  Cache validity
    depends on **both** schema-version compatibility and provenance/version
    compatibility — not only file timestamps or filenames.

    If no sidecar or artifact file exists the result is
    :attr:`~ArtifactResolutionStatus.MUST_REGENERATE`.

    Resolution rules (applied in order):

    1. **MUST_REGENERATE** – the artifact file or its sidecar does not exist.
    2. **MUST_REGENERATE** – any schema version recorded in the sidecar does
       not match the current code's ``CURRENT_VERSION``.
    3. **STALE** – all schema versions are current, but the recorded
       provenance does not match *current_provenance*.
    4. **COMPATIBLE** – all schema versions are current and provenance
       matches (or no *current_provenance* was supplied).

    When *strict* is ``True``, :attr:`~ArtifactResolutionStatus.STALE` is
    promoted to :attr:`~ArtifactResolutionStatus.MUST_REGENERATE`, so callers
    that require exact provenance matching will re-generate stale caches.

    Parameters
    ----------
    artifact_path:
        Path to the artifact file.
    current_provenance:
        A :class:`ProvenanceRecord` representing the current environment
        state (e.g. current git hashes).  When ``None`` only schema-version
        compatibility is checked; provenance staleness is not reported.
    strict:
        When ``True``, treat :attr:`~ArtifactResolutionStatus.STALE` as
        :attr:`~ArtifactResolutionStatus.MUST_REGENERATE`.

    Returns
    -------
    ArtifactResolutionStatus
        One of :attr:`~ArtifactResolutionStatus.COMPATIBLE`,
        :attr:`~ArtifactResolutionStatus.STALE`, or
        :attr:`~ArtifactResolutionStatus.MUST_REGENERATE`.
    """
    if not os.path.exists(artifact_path):
        return ArtifactResolutionStatus.MUST_REGENERATE

    try:
        entry = read_cache_sidecar(artifact_path)
    except (FileNotFoundError, ValueError):
        return ArtifactResolutionStatus.MUST_REGENERATE

    statuses = resolve_manifest(entry.manifest, current_provenance=current_provenance)

    if not statuses:
        return ArtifactResolutionStatus.MUST_REGENERATE

    if any(s == ArtifactResolutionStatus.MUST_REGENERATE for s in statuses.values()):
        return ArtifactResolutionStatus.MUST_REGENERATE

    if any(s == ArtifactResolutionStatus.STALE for s in statuses.values()):
        if strict:
            return ArtifactResolutionStatus.MUST_REGENERATE
        return ArtifactResolutionStatus.STALE

    return ArtifactResolutionStatus.COMPATIBLE
