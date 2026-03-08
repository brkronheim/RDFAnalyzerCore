"""Output schema definitions for RDFAnalyzerCore workflow artifacts.

All production stages (batch submission scripts, datacard generation) call
:func:`emit_output_manifest` to write an ``output_manifest.json`` file
alongside their outputs.  Downstream consumers call
:func:`OutputManifest.load` to read the manifest and
:meth:`OutputManifest.validate` to check that schema versions match the
current :data:`SCHEMA_REGISTRY` — without any custom discovery logic.

Typical producer usage::

    from output_schema import emit_output_manifest

    emit_output_manifest(
        job_dir,
        skim_path="output/sample_0.root",
        histogram_path="output/sample_0_meta.root",
        framework_hash=version_info.get("framework_hash"),
        user_repo_hash=version_info.get("user_repo_hash"),
    )

Typical consumer usage::

    from output_schema import OutputManifest

    manifest = OutputManifest.load("job_42/output_manifest.json")
    manifest.validate()
    hist_artifacts = manifest.find_artifacts("histogram")
"""

from __future__ import annotations

import datetime
import json
import os
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Schema registry
# ---------------------------------------------------------------------------

#: Maps artifact type name -> current schema version.
#: Bump a version here whenever the corresponding output format changes in an
#: incompatible way so that older consumers can detect the mismatch.
SCHEMA_REGISTRY: Dict[str, str] = {
    "skim": "1.0.0",
    "histogram": "1.0.0",
    "metadata": "1.0.0",
    "cutflow": "1.0.0",
    "law_artifact": "1.0.0",
}

#: Fixed file-name written by :func:`emit_output_manifest` and read by
#: :meth:`OutputManifest.load`.
MANIFEST_FILENAME: str = "output_manifest.json"

#: Version of the *manifest format itself* — independent of per-artifact
#: schema versions.
MANIFEST_FORMAT_VERSION: str = "1.0.0"


class SchemaVersionError(Exception):
    """Raised when a manifest artifact version does not match the registry."""


# ---------------------------------------------------------------------------
# Artifact schema classes
# ---------------------------------------------------------------------------


class ArtifactSchema:
    """Describes a single output artifact and the schema version it follows.

    Parameters
    ----------
    schema_type:
        Identifier string that must be a key in :data:`SCHEMA_REGISTRY`
        (e.g. ``"skim"``, ``"histogram"``).
    schema_version:
        Semantic version string for this artifact's schema.
    path:
        Absolute or relative path to the artifact file.
    """

    def __init__(self, schema_type: str, schema_version: str, path: str) -> None:
        self.schema_type: str = schema_type
        self.schema_version: str = schema_version
        self.path: str = path

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_version(self) -> None:
        """Raise :exc:`SchemaVersionError` if the version is not current.

        Checks ``self.schema_version`` against the value in
        :data:`SCHEMA_REGISTRY` for ``self.schema_type``.
        """
        expected = SCHEMA_REGISTRY.get(self.schema_type)
        if expected is None:
            raise SchemaVersionError(
                f"Unknown schema type {self.schema_type!r}. "
                f"Known types: {sorted(SCHEMA_REGISTRY)}"
            )
        if self.schema_version != expected:
            raise SchemaVersionError(
                f"Schema version mismatch for {self.schema_type!r}: "
                f"manifest has {self.schema_version!r} "
                f"but registry expects {expected!r}."
            )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable dictionary representation."""
        return {
            "schema_type": self.schema_type,
            "schema_version": self.schema_version,
            "path": self.path,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ArtifactSchema":
        """Deserialise from the dict produced by :meth:`to_dict`.

        Returns an instance of the most specific subclass matching
        ``d["schema_type"]``, falling back to :class:`ArtifactSchema` for
        unknown types.
        """
        _type_map: Dict[str, type] = {
            "skim": SkimSchema,
            "histogram": HistogramSchema,
            "metadata": MetadataSchema,
            "cutflow": CutflowSchema,
            "law_artifact": LawArtifactSchema,
        }
        schema_type = d.get("schema_type", "")
        klass = _type_map.get(schema_type, cls)
        return klass._from_dict(d)

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "ArtifactSchema":
        return cls(
            schema_type=d["schema_type"],
            schema_version=d["schema_version"],
            path=d.get("path", ""),
        )

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"{self.__class__.__name__}("
            f"schema_type={self.schema_type!r}, "
            f"schema_version={self.schema_version!r}, "
            f"path={self.path!r})"
        )


class SkimSchema(ArtifactSchema):
    """Schema for a skim output (ROOT file containing a TTree).

    Parameters
    ----------
    path:
        Path to the skim ROOT file.
    tree_name:
        Name of the TTree inside the file (default: ``"Events"``).
    schema_version:
        Override the schema version (defaults to the registry value).
    """

    def __init__(
        self,
        path: str,
        tree_name: str = "Events",
        schema_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            schema_type="skim",
            schema_version=schema_version or SCHEMA_REGISTRY["skim"],
            path=path,
        )
        self.tree_name: str = tree_name

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        d["tree_name"] = self.tree_name
        return d

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "SkimSchema":
        return cls(
            path=d.get("path", ""),
            tree_name=d.get("tree_name", "Events"),
            schema_version=d.get("schema_version"),
        )


class HistogramSchema(ArtifactSchema):
    """Schema for a histogram output (ROOT file with TH1/TH2 objects).

    Parameters
    ----------
    path:
        Path to the histogram ROOT file.
    schema_version:
        Override the schema version (defaults to the registry value).
    """

    def __init__(
        self,
        path: str,
        schema_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            schema_type="histogram",
            schema_version=schema_version or SCHEMA_REGISTRY["histogram"],
            path=path,
        )

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "HistogramSchema":
        return cls(
            path=d.get("path", ""),
            schema_version=d.get("schema_version"),
        )


class MetadataSchema(ArtifactSchema):
    """Schema for a metadata output (JSON or ROOT file with provenance/run info).

    Parameters
    ----------
    path:
        Path to the metadata file.
    schema_version:
        Override the schema version (defaults to the registry value).
    """

    def __init__(
        self,
        path: str,
        schema_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            schema_type="metadata",
            schema_version=schema_version or SCHEMA_REGISTRY["metadata"],
            path=path,
        )

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "MetadataSchema":
        return cls(
            path=d.get("path", ""),
            schema_version=d.get("schema_version"),
        )


class CutflowSchema(ArtifactSchema):
    """Schema for a cutflow output.

    Parameters
    ----------
    path:
        Path to the cutflow file.
    schema_version:
        Override the schema version (defaults to the registry value).
    """

    def __init__(
        self,
        path: str,
        schema_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            schema_type="cutflow",
            schema_version=schema_version or SCHEMA_REGISTRY["cutflow"],
            path=path,
        )

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "CutflowSchema":
        return cls(
            path=d.get("path", ""),
            schema_version=d.get("schema_version"),
        )


class LawArtifactSchema(ArtifactSchema):
    """Schema for a LAW workflow artifact (e.g. Combine datacard, shapes ROOT file).

    Parameters
    ----------
    path:
        Path to the artifact file.
    artifact_type:
        Human-readable sub-type (e.g. ``"datacard"``, ``"shapes"``).
    schema_version:
        Override the schema version (defaults to the registry value).
    """

    def __init__(
        self,
        path: str,
        artifact_type: str = "",
        schema_version: Optional[str] = None,
    ) -> None:
        super().__init__(
            schema_type="law_artifact",
            schema_version=schema_version or SCHEMA_REGISTRY["law_artifact"],
            path=path,
        )
        self.artifact_type: str = artifact_type

    def to_dict(self) -> Dict[str, Any]:
        d = super().to_dict()
        d["artifact_type"] = self.artifact_type
        return d

    @classmethod
    def _from_dict(cls, d: Dict[str, Any]) -> "LawArtifactSchema":
        return cls(
            path=d.get("path", ""),
            artifact_type=d.get("artifact_type", ""),
            schema_version=d.get("schema_version"),
        )


# ---------------------------------------------------------------------------
# OutputManifest
# ---------------------------------------------------------------------------


class OutputManifest:
    """Aggregates artifact schemas into a single, discoverable manifest file.

    The manifest is written as :data:`MANIFEST_FILENAME` (``output_manifest.json``)
    at a well-known location so that downstream tasks can find it without any
    custom discovery logic.

    Example — producer::

        manifest = OutputManifest(framework_hash="abc123")
        manifest.add_artifact("skim", SkimSchema(path="sample_0.root"))
        manifest.add_artifact("histogram", HistogramSchema(path="sample_0_meta.root"))
        manifest.write("job_0/output_manifest.json")

    Example — consumer::

        manifest = OutputManifest.load("job_0/output_manifest.json")
        manifest.validate()
        for artifact in manifest.find_artifacts("histogram"):
            process(artifact.path)

    Parameters
    ----------
    created_at:
        ISO 8601 timestamp; defaults to the current UTC time.
    framework_hash:
        Git hash of the RDFAnalyzerCore framework.
    user_repo_hash:
        Git hash of the user analysis repository.
    """

    def __init__(
        self,
        created_at: Optional[str] = None,
        framework_hash: Optional[str] = None,
        user_repo_hash: Optional[str] = None,
    ) -> None:
        self.format_version: str = MANIFEST_FORMAT_VERSION
        self.created_at: str = (
            created_at
            or datetime.datetime.now(datetime.timezone.utc).isoformat()
        )
        self.framework_hash: Optional[str] = framework_hash
        self.user_repo_hash: Optional[str] = user_repo_hash
        #: role (str) -> :class:`ArtifactSchema`
        self.artifacts: Dict[str, ArtifactSchema] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def add_artifact(self, role: str, artifact: ArtifactSchema) -> None:
        """Register *artifact* under the given *role*.

        Parameters
        ----------
        role:
            Logical name for this artifact in the manifest (e.g. ``"skim"``,
            ``"histogram"``, ``"datacard_sr"``).
        artifact:
            An :class:`ArtifactSchema` (or subclass) instance.
        """
        self.artifacts[role] = artifact

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def find_artifacts(self, schema_type: str) -> List[ArtifactSchema]:
        """Return all artifacts whose ``schema_type`` matches *schema_type*.

        Parameters
        ----------
        schema_type:
            One of the keys in :data:`SCHEMA_REGISTRY`
            (e.g. ``"skim"``, ``"histogram"``).

        Returns
        -------
        list[ArtifactSchema]
            Possibly empty list of matching artifacts.
        """
        return [
            a
            for a in self.artifacts.values()
            if isinstance(a, ArtifactSchema) and a.schema_type == schema_type
        ]

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self) -> None:
        """Validate every artifact's schema version against the registry.

        Raises
        ------
        SchemaVersionError
            On the first version mismatch found.
        """
        for role, artifact in self.artifacts.items():
            if isinstance(artifact, ArtifactSchema):
                artifact.validate_version()

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> Dict[str, Any]:
        """Return a JSON-serialisable dictionary representation."""
        return {
            "format_version": self.format_version,
            "created_at": self.created_at,
            "framework_hash": self.framework_hash,
            "user_repo_hash": self.user_repo_hash,
            "artifacts": {
                role: (
                    artifact.to_dict()
                    if isinstance(artifact, ArtifactSchema)
                    else artifact
                )
                for role, artifact in self.artifacts.items()
            },
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "OutputManifest":
        """Deserialise from the dict produced by :meth:`to_dict`."""
        manifest = cls(
            created_at=d.get("created_at"),
            framework_hash=d.get("framework_hash"),
            user_repo_hash=d.get("user_repo_hash"),
        )
        manifest.format_version = d.get("format_version", MANIFEST_FORMAT_VERSION)
        for role, artifact_dict in d.get("artifacts", {}).items():
            if isinstance(artifact_dict, dict) and "schema_type" in artifact_dict:
                manifest.artifacts[role] = ArtifactSchema.from_dict(artifact_dict)
            else:
                manifest.artifacts[role] = artifact_dict  # type: ignore[assignment]
        return manifest

    def write(self, path: str) -> None:
        """Write the manifest as JSON to *path*.

        Parent directories are created automatically.
        """
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        with open(path, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
            f.write("\n")

    @classmethod
    def load(cls, path: str) -> "OutputManifest":
        """Load and return an :class:`OutputManifest` from *path*.

        Parameters
        ----------
        path:
            Path to an ``output_manifest.json`` file.
        """
        with open(path) as f:
            d = json.load(f)
        return cls.from_dict(d)


# ---------------------------------------------------------------------------
# Convenience helper for production stages
# ---------------------------------------------------------------------------


def emit_output_manifest(
    output_dir: str,
    skim_path: Optional[str] = None,
    histogram_path: Optional[str] = None,
    tree_name: str = "Events",
    framework_hash: Optional[str] = None,
    user_repo_hash: Optional[str] = None,
    extra_artifacts: Optional[Dict[str, ArtifactSchema]] = None,
) -> str:
    """Build and write an :data:`MANIFEST_FILENAME` in *output_dir*.

    This is the primary entry-point for production stages.  Call it once per
    job directory (or output directory) to make schema information
    discoverable.

    Parameters
    ----------
    output_dir:
        Directory in which ``output_manifest.json`` will be written.
    skim_path:
        Path (absolute or relative) to the skim ROOT file, if produced.
    histogram_path:
        Path (absolute or relative) to the histogram/meta ROOT file, if
        produced.
    tree_name:
        Name of the TTree inside the skim file (default: ``"Events"``).
    framework_hash:
        Git hash of the RDFAnalyzerCore framework.
    user_repo_hash:
        Git hash of the user analysis repository.
    extra_artifacts:
        Additional ``role -> ArtifactSchema`` mappings to include in the
        manifest (e.g. cutflow or LAW artifacts).

    Returns
    -------
    str
        Absolute path to the written manifest file.
    """
    manifest = OutputManifest(
        framework_hash=framework_hash,
        user_repo_hash=user_repo_hash,
    )
    if skim_path:
        manifest.add_artifact("skim", SkimSchema(path=skim_path, tree_name=tree_name))
    if histogram_path:
        manifest.add_artifact("histogram", HistogramSchema(path=histogram_path))
    if extra_artifacts:
        for role, artifact in extra_artifacts.items():
            manifest.add_artifact(role, artifact)
    manifest_path = os.path.join(output_dir, MANIFEST_FILENAME)
    manifest.write(manifest_path)
    return manifest_path
