"""Tests for core/python/output_schema.py.

Covers:
- Schema class construction and serialisation round-trips
- SCHEMA_REGISTRY version validation (match and mismatch)
- OutputManifest add/find/validate/write/load
- emit_output_manifest convenience helper
- Downstream discovery pattern (load → validate → find_artifacts)
"""

import json
import os
import sys
import tempfile

import pytest

# Ensure core/python is importable regardless of working directory.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from output_schema import (
    MANIFEST_FILENAME,
    MANIFEST_FORMAT_VERSION,
    SCHEMA_REGISTRY,
    ArtifactResolutionStatus,
    ArtifactSchema,
    CutflowSchema,
    HistogramSchema,
    LawArtifactSchema,
    MetadataSchema,
    OutputManifest,
    ProvenanceRecord,
    SchemaVersionError,
    SkimSchema,
    emit_output_manifest,
    resolve_artifact,
    resolve_manifest,
)


# ---------------------------------------------------------------------------
# ArtifactSchema base class
# ---------------------------------------------------------------------------


class TestArtifactSchema:
    def test_to_dict_round_trip(self):
        artifact = ArtifactSchema(
            schema_type="skim", schema_version="1.0.0", path="out.root"
        )
        d = artifact.to_dict()
        assert d == {
            "schema_type": "skim",
            "schema_version": "1.0.0",
            "path": "out.root",
        }

    def test_from_dict_dispatches_to_skim(self):
        d = {"schema_type": "skim", "schema_version": "1.0.0", "path": "a.root"}
        artifact = ArtifactSchema.from_dict(d)
        assert isinstance(artifact, SkimSchema)

    def test_from_dict_dispatches_to_histogram(self):
        d = {"schema_type": "histogram", "schema_version": "1.0.0", "path": "b.root"}
        artifact = ArtifactSchema.from_dict(d)
        assert isinstance(artifact, HistogramSchema)

    def test_from_dict_dispatches_to_law_artifact(self):
        d = {
            "schema_type": "law_artifact",
            "schema_version": "1.0.0",
            "path": "card.txt",
            "artifact_type": "datacard",
        }
        artifact = ArtifactSchema.from_dict(d)
        assert isinstance(artifact, LawArtifactSchema)
        assert artifact.artifact_type == "datacard"

    def test_from_dict_unknown_type_falls_back(self):
        d = {"schema_type": "unknown_xyz", "schema_version": "9.9.9", "path": "x"}
        artifact = ArtifactSchema.from_dict(d)
        assert isinstance(artifact, ArtifactSchema)
        assert artifact.schema_type == "unknown_xyz"

    def test_validate_version_ok(self):
        artifact = ArtifactSchema(
            schema_type="skim",
            schema_version=SCHEMA_REGISTRY["skim"],
            path="",
        )
        artifact.validate_version()  # must not raise

    def test_validate_version_mismatch_raises(self):
        artifact = ArtifactSchema(
            schema_type="skim", schema_version="0.0.1", path=""
        )
        with pytest.raises(SchemaVersionError, match="skim"):
            artifact.validate_version()

    def test_validate_version_unknown_type_raises(self):
        artifact = ArtifactSchema(
            schema_type="no_such_type", schema_version="1.0.0", path=""
        )
        with pytest.raises(SchemaVersionError, match="no_such_type"):
            artifact.validate_version()


# ---------------------------------------------------------------------------
# Concrete schema classes
# ---------------------------------------------------------------------------


class TestSkimSchema:
    def test_defaults(self):
        s = SkimSchema(path="skim.root")
        assert s.schema_type == "skim"
        assert s.schema_version == SCHEMA_REGISTRY["skim"]
        assert s.tree_name == "Events"

    def test_custom_tree_name(self):
        s = SkimSchema(path="skim.root", tree_name="Muons")
        assert s.tree_name == "Muons"

    def test_to_dict_includes_tree_name(self):
        s = SkimSchema(path="s.root", tree_name="T")
        d = s.to_dict()
        assert d["tree_name"] == "T"

    def test_round_trip(self):
        s = SkimSchema(path="s.root", tree_name="Tau")
        s2 = ArtifactSchema.from_dict(s.to_dict())
        assert isinstance(s2, SkimSchema)
        assert s2.tree_name == "Tau"
        assert s2.path == "s.root"

    def test_explicit_version(self):
        s = SkimSchema(path="x.root", schema_version="1.0.0")
        assert s.schema_version == "1.0.0"
        s.validate_version()


class TestHistogramSchema:
    def test_defaults(self):
        h = HistogramSchema(path="hist.root")
        assert h.schema_type == "histogram"
        assert h.schema_version == SCHEMA_REGISTRY["histogram"]

    def test_round_trip(self):
        h = HistogramSchema(path="h.root")
        h2 = ArtifactSchema.from_dict(h.to_dict())
        assert isinstance(h2, HistogramSchema)
        assert h2.path == "h.root"


class TestMetadataSchema:
    def test_defaults(self):
        m = MetadataSchema(path="meta.json")
        assert m.schema_type == "metadata"

    def test_round_trip(self):
        m = MetadataSchema(path="m.json")
        m2 = ArtifactSchema.from_dict(m.to_dict())
        assert isinstance(m2, MetadataSchema)


class TestCutflowSchema:
    def test_defaults(self):
        c = CutflowSchema(path="cutflow.json")
        assert c.schema_type == "cutflow"

    def test_round_trip(self):
        c = CutflowSchema(path="c.txt")
        c2 = ArtifactSchema.from_dict(c.to_dict())
        assert isinstance(c2, CutflowSchema)


class TestLawArtifactSchema:
    def test_defaults(self):
        l = LawArtifactSchema(path="dc.txt")
        assert l.schema_type == "law_artifact"
        assert l.artifact_type == ""

    def test_artifact_type_preserved(self):
        l = LawArtifactSchema(path="dc.txt", artifact_type="datacard")
        assert l.artifact_type == "datacard"

    def test_to_dict_includes_artifact_type(self):
        l = LawArtifactSchema(path="dc.txt", artifact_type="shapes")
        d = l.to_dict()
        assert d["artifact_type"] == "shapes"

    def test_round_trip(self):
        l = LawArtifactSchema(path="s.root", artifact_type="shapes")
        l2 = ArtifactSchema.from_dict(l.to_dict())
        assert isinstance(l2, LawArtifactSchema)
        assert l2.artifact_type == "shapes"


# ---------------------------------------------------------------------------
# OutputManifest
# ---------------------------------------------------------------------------


class TestOutputManifest:
    def test_empty_manifest_validates(self):
        m = OutputManifest()
        m.validate()  # no artifacts → no errors

    def test_add_and_find_artifacts(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path="s.root"))
        m.add_artifact("histogram", HistogramSchema(path="h.root"))
        m.add_artifact("histogram2", HistogramSchema(path="h2.root"))

        skims = m.find_artifacts("skim")
        hists = m.find_artifacts("histogram")
        assert len(skims) == 1
        assert skims[0].path == "s.root"
        assert len(hists) == 2

    def test_find_artifacts_empty(self):
        m = OutputManifest()
        assert m.find_artifacts("skim") == []

    def test_validate_ok(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path=""))
        m.add_artifact("histogram", HistogramSchema(path=""))
        m.validate()  # must not raise

    def test_validate_fails_on_bad_version(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path="", schema_version="0.0.1"))
        with pytest.raises(SchemaVersionError):
            m.validate()

    def test_to_dict_and_from_dict_round_trip(self):
        m = OutputManifest(framework_hash="abc", user_repo_hash="def")
        m.add_artifact("skim", SkimSchema(path="s.root"))
        m.add_artifact("law", LawArtifactSchema(path="dc.txt", artifact_type="datacard"))

        d = m.to_dict()
        assert d["framework_hash"] == "abc"
        assert "skim" in d["artifacts"]
        assert "law" in d["artifacts"]

        m2 = OutputManifest.from_dict(d)
        assert m2.framework_hash == "abc"
        assert isinstance(m2.artifacts["skim"], SkimSchema)
        assert isinstance(m2.artifacts["law"], LawArtifactSchema)
        m2.validate()

    def test_write_and_load(self, tmp_path):
        m = OutputManifest(framework_hash="hash1")
        m.add_artifact("skim", SkimSchema(path="output.root"))
        manifest_path = str(tmp_path / MANIFEST_FILENAME)
        m.write(manifest_path)

        assert os.path.exists(manifest_path)
        m2 = OutputManifest.load(manifest_path)
        assert m2.framework_hash == "hash1"
        m2.validate()

    def test_write_creates_parent_dirs(self, tmp_path):
        m = OutputManifest()
        nested_path = str(tmp_path / "a" / "b" / MANIFEST_FILENAME)
        m.write(nested_path)
        assert os.path.exists(nested_path)

    def test_manifest_json_is_well_formed(self, tmp_path):
        m = OutputManifest()
        m.add_artifact("histogram", HistogramSchema(path="h.root"))
        path = str(tmp_path / MANIFEST_FILENAME)
        m.write(path)
        with open(path) as f:
            data = json.load(f)
        assert data["format_version"] == MANIFEST_FORMAT_VERSION
        assert "artifacts" in data

    def test_format_version_preserved_on_round_trip(self, tmp_path):
        m = OutputManifest()
        path = str(tmp_path / MANIFEST_FILENAME)
        m.write(path)
        m2 = OutputManifest.load(path)
        assert m2.format_version == MANIFEST_FORMAT_VERSION

    def test_created_at_is_set(self):
        m = OutputManifest()
        assert m.created_at  # non-empty string

    def test_explicit_created_at(self):
        ts = "2024-01-01T00:00:00+00:00"
        m = OutputManifest(created_at=ts)
        assert m.created_at == ts


# ---------------------------------------------------------------------------
# emit_output_manifest helper
# ---------------------------------------------------------------------------


class TestEmitOutputManifest:
    def test_writes_manifest_file(self, tmp_path):
        out_dir = str(tmp_path)
        path = emit_output_manifest(
            out_dir,
            skim_path="output.root",
            histogram_path="output_meta.root",
        )
        assert path == os.path.join(out_dir, MANIFEST_FILENAME)
        assert os.path.exists(path)

    def test_manifest_contains_skim_and_histogram(self, tmp_path):
        emit_output_manifest(
            str(tmp_path),
            skim_path="s.root",
            histogram_path="h.root",
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert len(m.find_artifacts("skim")) == 1
        assert len(m.find_artifacts("histogram")) == 1

    def test_skim_path_only(self, tmp_path):
        emit_output_manifest(str(tmp_path), skim_path="s.root")
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert len(m.find_artifacts("skim")) == 1
        assert len(m.find_artifacts("histogram")) == 0

    def test_tree_name_forwarded(self, tmp_path):
        emit_output_manifest(
            str(tmp_path), skim_path="s.root", tree_name="Muons"
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        skim = m.find_artifacts("skim")[0]
        assert isinstance(skim, SkimSchema)
        assert skim.tree_name == "Muons"

    def test_hashes_forwarded(self, tmp_path):
        emit_output_manifest(
            str(tmp_path),
            skim_path="s.root",
            framework_hash="fw_hash",
            user_repo_hash="user_hash",
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert m.framework_hash == "fw_hash"
        assert m.user_repo_hash == "user_hash"

    def test_extra_artifacts_included(self, tmp_path):
        law = LawArtifactSchema(path="dc.txt", artifact_type="datacard")
        emit_output_manifest(
            str(tmp_path),
            extra_artifacts={"datacard_sr": law},
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert len(m.find_artifacts("law_artifact")) == 1
        loaded = m.find_artifacts("law_artifact")[0]
        assert isinstance(loaded, LawArtifactSchema)
        assert loaded.artifact_type == "datacard"

    def test_validates_after_load(self, tmp_path):
        emit_output_manifest(
            str(tmp_path),
            skim_path="s.root",
            histogram_path="h.root",
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        m.validate()  # must not raise

    def test_no_artifacts_when_no_paths(self, tmp_path):
        emit_output_manifest(str(tmp_path))
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert m.artifacts == {}


# ---------------------------------------------------------------------------
# Downstream discovery scenario
# ---------------------------------------------------------------------------


class TestDownstreamDiscovery:
    """Simulate the downstream consumer pattern: load → validate → query."""

    def test_full_discovery_workflow(self, tmp_path):
        # Producer writes the manifest.
        emit_output_manifest(
            str(tmp_path),
            skim_path="/data/sample_0.root",
            histogram_path="/data/sample_0_meta.root",
            framework_hash="deadbeef",
        )

        # Downstream consumer discovers without custom logic.
        manifest_path = os.path.join(str(tmp_path), MANIFEST_FILENAME)
        assert os.path.exists(manifest_path), (
            "Downstream consumer could not find manifest at well-known path"
        )
        m = OutputManifest.load(manifest_path)
        m.validate()

        hists = m.find_artifacts("histogram")
        assert len(hists) == 1
        assert hists[0].path == "/data/sample_0_meta.root"

        skims = m.find_artifacts("skim")
        assert len(skims) == 1
        assert skims[0].path == "/data/sample_0.root"

    def test_version_mismatch_detected_on_load(self, tmp_path):
        """Manually written manifest with wrong version is rejected by validate()."""
        bad_manifest = {
            "format_version": MANIFEST_FORMAT_VERSION,
            "created_at": "2024-01-01T00:00:00+00:00",
            "framework_hash": None,
            "user_repo_hash": None,
            "artifacts": {
                "skim": {
                    "schema_type": "skim",
                    "schema_version": "0.0.1",  # intentionally wrong
                    "path": "s.root",
                    "tree_name": "Events",
                }
            },
        }
        path = str(tmp_path / MANIFEST_FILENAME)
        with open(path, "w") as f:
            json.dump(bad_manifest, f)

        m = OutputManifest.load(path)
        with pytest.raises(SchemaVersionError):
            m.validate()


# ---------------------------------------------------------------------------
# ProvenanceRecord
# ---------------------------------------------------------------------------


class TestProvenanceRecord:
    def test_defaults(self):
        p = ProvenanceRecord()
        assert p.framework_hash is None
        assert p.user_repo_hash is None
        assert p.config_mtime is None

    def test_fields_set(self):
        p = ProvenanceRecord(
            framework_hash="fw1",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        assert p.framework_hash == "fw1"
        assert p.user_repo_hash == "ur1"
        assert p.config_mtime == "2024-01-01T00:00:00+00:00"

    def test_matches_identical(self):
        p1 = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        p2 = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        assert p1.matches(p2)

    def test_matches_different_hashes(self):
        p1 = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        p2 = ProvenanceRecord(framework_hash="fw2", user_repo_hash="ur1")
        assert not p1.matches(p2)

    def test_matches_none_fields_ignored(self):
        # None fields are skipped — no mismatch
        p1 = ProvenanceRecord(framework_hash="fw1", user_repo_hash=None)
        p2 = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur2")
        assert p1.matches(p2)

    def test_matches_both_none_fields_ignored(self):
        p1 = ProvenanceRecord(framework_hash=None)
        p2 = ProvenanceRecord(framework_hash=None)
        assert p1.matches(p2)

    def test_matches_config_mtime(self):
        p1 = ProvenanceRecord(config_mtime="2024-01-01T00:00:00+00:00")
        p2 = ProvenanceRecord(config_mtime="2024-06-01T00:00:00+00:00")
        assert not p1.matches(p2)

    def test_to_dict(self):
        p = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        d = p.to_dict()
        assert d["framework_hash"] == "fw1"
        assert d["user_repo_hash"] == "ur1"
        assert d["config_mtime"] is None

    def test_from_dict_round_trip(self):
        p = ProvenanceRecord(
            framework_hash="fw1",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        p2 = ProvenanceRecord.from_dict(p.to_dict())
        assert p2.framework_hash == "fw1"
        assert p2.user_repo_hash == "ur1"
        assert p2.config_mtime == "2024-01-01T00:00:00+00:00"

    def test_from_dict_empty(self):
        p = ProvenanceRecord.from_dict({})
        assert p.framework_hash is None
        assert p.user_repo_hash is None
        assert p.config_mtime is None


# ---------------------------------------------------------------------------
# ArtifactResolutionStatus enum
# ---------------------------------------------------------------------------


class TestArtifactResolutionStatus:
    def test_values_exist(self):
        assert ArtifactResolutionStatus.COMPATIBLE.value == "compatible"
        assert ArtifactResolutionStatus.STALE.value == "stale"
        assert ArtifactResolutionStatus.MUST_REGENERATE.value == "must_regenerate"


# ---------------------------------------------------------------------------
# resolve_artifact
# ---------------------------------------------------------------------------


class TestResolveArtifact:
    def test_compatible_no_provenance(self):
        artifact = SkimSchema(path="s.root")
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_must_regenerate_version_mismatch(self):
        artifact = SkimSchema(path="s.root", schema_version="0.0.1")
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_must_regenerate_unknown_type(self):
        artifact = ArtifactSchema(
            schema_type="nonexistent_type", schema_version="1.0.0", path=""
        )
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_compatible_matching_provenance(self):
        artifact = SkimSchema(path="s.root")
        recorded = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        current = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_stale_changed_framework_hash(self):
        artifact = SkimSchema(path="s.root")
        recorded = ProvenanceRecord(framework_hash="fw_old")
        current = ProvenanceRecord(framework_hash="fw_new")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_stale_changed_user_repo_hash(self):
        artifact = HistogramSchema(path="h.root")
        recorded = ProvenanceRecord(user_repo_hash="ur_old")
        current = ProvenanceRecord(user_repo_hash="ur_new")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_stale_changed_config_mtime(self):
        artifact = CutflowSchema(path="c.json")
        recorded = ProvenanceRecord(config_mtime="2024-01-01T00:00:00+00:00")
        current = ProvenanceRecord(config_mtime="2025-01-01T00:00:00+00:00")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_compatible_when_only_recorded_provenance_given(self):
        # Without current_provenance there is nothing to compare against.
        artifact = SkimSchema(path="s.root")
        recorded = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, recorded_provenance=recorded)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_compatible_when_only_current_provenance_given(self):
        artifact = SkimSchema(path="s.root")
        current = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, current_provenance=current)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_must_regenerate_takes_priority_over_provenance(self):
        # Even if provenance matches, version mismatch → MUST_REGENERATE.
        artifact = SkimSchema(path="s.root", schema_version="0.0.1")
        recorded = ProvenanceRecord(framework_hash="fw1")
        current = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_law_artifact_compatible(self):
        artifact = LawArtifactSchema(path="dc.txt", artifact_type="datacard")
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE


# ---------------------------------------------------------------------------
# resolve_manifest
# ---------------------------------------------------------------------------


class TestResolveManifest:
    def test_empty_manifest(self):
        m = OutputManifest()
        assert resolve_manifest(m) == {}

    def test_all_compatible_no_provenance(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path="s.root"))
        m.add_artifact("histogram", HistogramSchema(path="h.root"))
        statuses = resolve_manifest(m)
        assert statuses["skim"] == ArtifactResolutionStatus.COMPATIBLE
        assert statuses["histogram"] == ArtifactResolutionStatus.COMPATIBLE

    def test_stale_when_hash_changes(self):
        m = OutputManifest(framework_hash="fw_old")
        m.add_artifact("skim", SkimSchema(path="s.root"))
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = resolve_manifest(m, current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.STALE

    def test_must_regenerate_version_mismatch(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path="s.root", schema_version="0.0.1"))
        statuses = resolve_manifest(m)
        assert statuses["skim"] == ArtifactResolutionStatus.MUST_REGENERATE

    def test_mixed_statuses(self):
        m = OutputManifest(framework_hash="fw_old")
        m.add_artifact("skim", SkimSchema(path="s.root"))
        m.add_artifact("bad", SkimSchema(path="b.root", schema_version="0.0.1"))
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = resolve_manifest(m, current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.STALE
        assert statuses["bad"] == ArtifactResolutionStatus.MUST_REGENERATE

    def test_roles_are_present(self):
        m = OutputManifest()
        m.add_artifact("datacard_sr", LawArtifactSchema(path="dc.txt"))
        statuses = resolve_manifest(m)
        assert "datacard_sr" in statuses


# ---------------------------------------------------------------------------
# OutputManifest.provenance() and resolve()
# ---------------------------------------------------------------------------


class TestOutputManifestProvenance:
    def test_provenance_returns_record(self):
        m = OutputManifest(
            framework_hash="fw1",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        p = m.provenance()
        assert isinstance(p, ProvenanceRecord)
        assert p.framework_hash == "fw1"
        assert p.user_repo_hash == "ur1"
        assert p.config_mtime == "2024-01-01T00:00:00+00:00"

    def test_provenance_nones_when_not_set(self):
        m = OutputManifest()
        p = m.provenance()
        assert p.framework_hash is None
        assert p.user_repo_hash is None
        assert p.config_mtime is None

    def test_resolve_delegates_to_resolve_manifest(self):
        m = OutputManifest(framework_hash="fw_old")
        m.add_artifact("skim", SkimSchema(path="s.root"))
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = m.resolve(current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.STALE

    def test_resolve_without_provenance(self):
        m = OutputManifest()
        m.add_artifact("skim", SkimSchema(path="s.root"))
        statuses = m.resolve()
        assert statuses["skim"] == ArtifactResolutionStatus.COMPATIBLE

    def test_config_mtime_round_trips_through_dict(self):
        m = OutputManifest(config_mtime="2024-06-01T12:00:00+00:00")
        d = m.to_dict()
        assert d["config_mtime"] == "2024-06-01T12:00:00+00:00"
        m2 = OutputManifest.from_dict(d)
        assert m2.config_mtime == "2024-06-01T12:00:00+00:00"

    def test_config_mtime_round_trips_through_file(self, tmp_path):
        m = OutputManifest(config_mtime="2024-06-01T12:00:00+00:00")
        path = str(tmp_path / MANIFEST_FILENAME)
        m.write(path)
        m2 = OutputManifest.load(path)
        assert m2.config_mtime == "2024-06-01T12:00:00+00:00"


# ---------------------------------------------------------------------------
# emit_output_manifest with config_mtime
# ---------------------------------------------------------------------------


class TestEmitOutputManifestProvenance:
    def test_config_mtime_forwarded(self, tmp_path):
        emit_output_manifest(
            str(tmp_path),
            skim_path="s.root",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))
        assert m.config_mtime == "2024-01-01T00:00:00+00:00"

    def test_full_provenance_resolution_workflow(self, tmp_path):
        """End-to-end: produce manifest, then resolve with updated env."""
        emit_output_manifest(
            str(tmp_path),
            skim_path="s.root",
            histogram_path="h.root",
            framework_hash="fw_old",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        m = OutputManifest.load(str(tmp_path / MANIFEST_FILENAME))

        # Same environment — all COMPATIBLE.
        same = ProvenanceRecord(
            framework_hash="fw_old",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        for status in m.resolve(same).values():
            assert status == ArtifactResolutionStatus.COMPATIBLE

        # Updated framework hash — all STALE.
        updated = ProvenanceRecord(
            framework_hash="fw_new",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        for status in m.resolve(updated).values():
            assert status == ArtifactResolutionStatus.STALE
