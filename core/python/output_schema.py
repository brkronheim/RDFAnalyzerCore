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
"""

from __future__ import annotations

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
    ) -> None:
        self.manifest_version: int = manifest_version
        self.skim: Optional[SkimSchema] = skim
        self.histograms: Optional[HistogramSchema] = histograms
        self.metadata: Optional[MetadataSchema] = metadata
        self.cutflow: Optional[CutflowSchema] = cutflow
        self.law_artifacts: List[LawArtifactSchema] = law_artifacts or []

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
        return cls(
            manifest_version=data.get("manifest_version", OUTPUT_MANIFEST_VERSION),
            skim=skim,
            histograms=histograms,
            metadata=metadata,
            cutflow=cutflow,
            law_artifacts=law_artifacts,
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


# ---------------------------------------------------------------------------
# SchemaVersionError
# ---------------------------------------------------------------------------


class SchemaVersionError(RuntimeError):
    """Raised when a loaded schema version does not match the current code.

    Catching :class:`SchemaVersionError` separately from other
    :class:`RuntimeError` exceptions allows callers to apply migration logic
    or emit clear user-facing messages about incompatible output formats.
    """
