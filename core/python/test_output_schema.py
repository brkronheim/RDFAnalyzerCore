"""
Tests for core/python/output_schema.py.

Covers:
- Schema version constants and SCHEMA_REGISTRY
- SkimSchema construction, validation, to_dict / from_dict round-trips
- HistogramAxisSpec construction, validation, to_dict / from_dict round-trips
- HistogramSchema construction, validation, to_dict / from_dict round-trips
  (including nested axes)
- MetadataSchema construction, validation, to_dict / from_dict round-trips
- CutflowSchema construction, validation, to_dict / from_dict round-trips
- LawArtifactSchema construction, validation, to_dict / from_dict round-trips
- OutputManifest construction, to_dict / from_dict / save_yaml / load_yaml
  round-trips, validate(), and check_version_compatibility()
- SchemaVersionError is raised for version mismatches
"""
from __future__ import annotations

import os
import sys
import textwrap

import pytest
import yaml

# Make core/python importable when running from the repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from output_schema import (
    CACHE_SIDECAR_SUFFIX,
    CUTFLOW_SCHEMA_VERSION,
    HISTOGRAM_SCHEMA_VERSION,
    LAW_ARTIFACT_SCHEMA_VERSION,
    LAW_ARTIFACT_TYPES,
    METADATA_SCHEMA_VERSION,
    OUTPUT_MANIFEST_VERSION,
    PROVENANCE_OPTIONAL_KEYS,
    PROVENANCE_REQUIRED_KEYS,
    SCHEMA_REGISTRY,
    SKIM_SCHEMA_VERSION,
    ArtifactResolutionStatus,
    CachedArtifact,
    CutflowSchema,
    DatasetManifestProvenance,
    HistogramAxisSpec,
    HistogramSchema,
    LawArtifactSchema,
    MergeInputValidationError,
    MetadataSchema,
    OutputManifest,
    ProvenanceRecord,
    SchemaVersionError,
    SkimSchema,
    check_cache_validity,
    merge_manifests,
    read_cache_sidecar,
    resolve_artifact,
    resolve_manifest,
    validate_merge_inputs,
    write_cache_sidecar,
)


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_schema_registry_keys(self):
        expected = {
            "skim",
            "histogram",
            "metadata",
            "cutflow",
            "law_artifact",
            "output_manifest",
        }
        assert set(SCHEMA_REGISTRY.keys()) == expected

    def test_schema_registry_versions_match_constants(self):
        assert SCHEMA_REGISTRY["skim"] == SKIM_SCHEMA_VERSION
        assert SCHEMA_REGISTRY["histogram"] == HISTOGRAM_SCHEMA_VERSION
        assert SCHEMA_REGISTRY["metadata"] == METADATA_SCHEMA_VERSION
        assert SCHEMA_REGISTRY["cutflow"] == CUTFLOW_SCHEMA_VERSION
        assert SCHEMA_REGISTRY["law_artifact"] == LAW_ARTIFACT_SCHEMA_VERSION
        assert SCHEMA_REGISTRY["output_manifest"] == OUTPUT_MANIFEST_VERSION

    def test_provenance_required_keys_non_empty(self):
        assert len(PROVENANCE_REQUIRED_KEYS) > 0
        assert "framework.git_hash" in PROVENANCE_REQUIRED_KEYS
        assert "config.hash" in PROVENANCE_REQUIRED_KEYS

    def test_provenance_optional_keys_non_empty(self):
        assert len(PROVENANCE_OPTIONAL_KEYS) > 0
        assert "analysis.git_hash" in PROVENANCE_OPTIONAL_KEYS

    def test_law_artifact_types_non_empty(self):
        assert len(LAW_ARTIFACT_TYPES) > 0
        assert "prepare_sample" in LAW_ARTIFACT_TYPES
        assert "submit_jobs" in LAW_ARTIFACT_TYPES


# ---------------------------------------------------------------------------
# SkimSchema
# ---------------------------------------------------------------------------


class TestSkimSchema:
    def test_defaults(self):
        s = SkimSchema()
        assert s.schema_version == SKIM_SCHEMA_VERSION
        assert s.tree_name == "Events"
        assert s.branches == []

    def test_custom_construction(self):
        s = SkimSchema(output_file="out.root", tree_name="MyTree", branches=["pt", "eta"])
        assert s.output_file == "out.root"
        assert s.tree_name == "MyTree"
        assert s.branches == ["pt", "eta"]

    def test_current_version_matches_constant(self):
        assert SkimSchema.CURRENT_VERSION == SKIM_SCHEMA_VERSION

    def test_validate_valid(self):
        s = SkimSchema(output_file="out.root", tree_name="Events")
        assert s.validate() == []

    def test_validate_empty_output_file(self):
        s = SkimSchema(output_file="", tree_name="Events")
        errors = s.validate()
        assert any("output_file" in e for e in errors)

    def test_validate_empty_tree_name(self):
        s = SkimSchema(output_file="out.root", tree_name="")
        errors = s.validate()
        assert any("tree_name" in e for e in errors)

    def test_validate_version_mismatch(self):
        s = SkimSchema(output_file="out.root", tree_name="Events")
        s.schema_version = 999
        errors = s.validate()
        assert any("version mismatch" in e for e in errors)

    def test_to_dict_round_trip(self):
        s = SkimSchema(output_file="out.root", tree_name="Events", branches=["pt"])
        d = s.to_dict()
        s2 = SkimSchema.from_dict(d)
        assert s2.output_file == s.output_file
        assert s2.tree_name == s.tree_name
        assert s2.branches == s.branches
        assert s2.schema_version == s.schema_version

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "schema_version": SKIM_SCHEMA_VERSION,
            "output_file": "x.root",
            "tree_name": "Events",
            "branches": [],
            "future_unknown_field": "value",
        }
        s = SkimSchema.from_dict(d)
        assert s.output_file == "x.root"


# ---------------------------------------------------------------------------
# HistogramAxisSpec
# ---------------------------------------------------------------------------


class TestHistogramAxisSpec:
    def test_defaults(self):
        ax = HistogramAxisSpec()
        assert ax.variable == ""
        assert ax.bins == 0
        assert ax.lower_bound == 0.0
        assert ax.upper_bound == 1.0
        assert ax.label == ""

    def test_custom_construction(self):
        ax = HistogramAxisSpec(variable="pt", bins=50, lower_bound=0.0, upper_bound=500.0, label="p_{T}")
        assert ax.variable == "pt"
        assert ax.bins == 50

    def test_validate_valid(self):
        ax = HistogramAxisSpec(variable="pt", bins=50, lower_bound=0.0, upper_bound=500.0)
        assert ax.validate() == []

    def test_validate_empty_variable(self):
        ax = HistogramAxisSpec(variable="", bins=10, lower_bound=0.0, upper_bound=1.0)
        errors = ax.validate()
        assert any("variable" in e for e in errors)

    def test_validate_zero_bins(self):
        ax = HistogramAxisSpec(variable="pt", bins=0, lower_bound=0.0, upper_bound=1.0)
        errors = ax.validate()
        assert any("bins" in e for e in errors)

    def test_validate_negative_bins(self):
        ax = HistogramAxisSpec(variable="pt", bins=-1, lower_bound=0.0, upper_bound=1.0)
        errors = ax.validate()
        assert any("bins" in e for e in errors)

    def test_validate_inverted_bounds(self):
        ax = HistogramAxisSpec(variable="pt", bins=10, lower_bound=1.0, upper_bound=0.0)
        errors = ax.validate()
        assert any("lower_bound" in e for e in errors)

    def test_validate_equal_bounds(self):
        ax = HistogramAxisSpec(variable="pt", bins=10, lower_bound=1.0, upper_bound=1.0)
        errors = ax.validate()
        assert any("lower_bound" in e for e in errors)

    def test_to_dict_round_trip(self):
        ax = HistogramAxisSpec(variable="eta", bins=20, lower_bound=-5.0, upper_bound=5.0, label="η")
        d = ax.to_dict()
        ax2 = HistogramAxisSpec.from_dict(d)
        assert ax2.variable == ax.variable
        assert ax2.bins == ax.bins
        assert ax2.lower_bound == ax.lower_bound
        assert ax2.upper_bound == ax.upper_bound
        assert ax2.label == ax.label


# ---------------------------------------------------------------------------
# HistogramSchema
# ---------------------------------------------------------------------------


class TestHistogramSchema:
    def _make_axis(self):
        return HistogramAxisSpec(variable="pt", bins=50, lower_bound=0.0, upper_bound=500.0)

    def test_defaults(self):
        h = HistogramSchema()
        assert h.schema_version == HISTOGRAM_SCHEMA_VERSION
        assert h.output_file == ""
        assert h.histogram_names == []
        assert h.axes == []

    def test_current_version_matches_constant(self):
        assert HistogramSchema.CURRENT_VERSION == HISTOGRAM_SCHEMA_VERSION

    def test_validate_valid(self):
        h = HistogramSchema(
            output_file="meta.root",
            histogram_names=["h_pt"],
            axes=[self._make_axis()],
        )
        assert h.validate() == []

    def test_validate_empty_output_file(self):
        h = HistogramSchema(output_file="")
        errors = h.validate()
        assert any("output_file" in e for e in errors)

    def test_validate_version_mismatch(self):
        h = HistogramSchema(output_file="meta.root")
        h.schema_version = 999
        errors = h.validate()
        assert any("version mismatch" in e for e in errors)

    def test_validate_propagates_axis_errors(self):
        bad_axis = HistogramAxisSpec(variable="", bins=0, lower_bound=1.0, upper_bound=0.0)
        h = HistogramSchema(output_file="meta.root", axes=[bad_axis])
        errors = h.validate()
        assert any("axes[0]" in e for e in errors)

    def test_to_dict_round_trip(self):
        h = HistogramSchema(
            output_file="meta.root",
            histogram_names=["h_pt", "h_eta"],
            axes=[self._make_axis()],
        )
        d = h.to_dict()
        h2 = HistogramSchema.from_dict(d)
        assert h2.output_file == h.output_file
        assert h2.histogram_names == h.histogram_names
        assert len(h2.axes) == 1
        assert h2.axes[0].variable == "pt"
        assert h2.schema_version == h.schema_version

    def test_from_dict_empty_axes(self):
        h = HistogramSchema.from_dict({"output_file": "x.root"})
        assert h.axes == []


# ---------------------------------------------------------------------------
# MetadataSchema
# ---------------------------------------------------------------------------


class TestMetadataSchema:
    def test_defaults(self):
        m = MetadataSchema()
        assert m.schema_version == METADATA_SCHEMA_VERSION
        assert m.provenance_dir == "provenance"
        assert set(m.required_keys) == set(PROVENANCE_REQUIRED_KEYS)
        assert set(m.optional_keys) == set(PROVENANCE_OPTIONAL_KEYS)

    def test_current_version_matches_constant(self):
        assert MetadataSchema.CURRENT_VERSION == METADATA_SCHEMA_VERSION

    def test_validate_valid(self):
        m = MetadataSchema(output_file="meta.root")
        assert m.validate() == []

    def test_validate_empty_output_file(self):
        m = MetadataSchema(output_file="")
        errors = m.validate()
        assert any("output_file" in e for e in errors)

    def test_validate_empty_provenance_dir(self):
        m = MetadataSchema(output_file="meta.root", provenance_dir="")
        errors = m.validate()
        assert any("provenance_dir" in e for e in errors)

    def test_validate_version_mismatch(self):
        m = MetadataSchema(output_file="meta.root")
        m.schema_version = 999
        errors = m.validate()
        assert any("version mismatch" in e for e in errors)

    def test_to_dict_round_trip(self):
        m = MetadataSchema(
            output_file="meta.root",
            provenance_dir="provenance",
            required_keys=["framework.git_hash"],
            optional_keys=["analysis.git_hash"],
        )
        d = m.to_dict()
        m2 = MetadataSchema.from_dict(d)
        assert m2.output_file == m.output_file
        assert m2.provenance_dir == m.provenance_dir
        assert m2.required_keys == m.required_keys
        assert m2.optional_keys == m.optional_keys

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "schema_version": METADATA_SCHEMA_VERSION,
            "output_file": "x.root",
            "provenance_dir": "provenance",
            "required_keys": [],
            "optional_keys": [],
            "future_field": "ignored",
        }
        m = MetadataSchema.from_dict(d)
        assert m.output_file == "x.root"

    def test_independent_default_lists(self):
        """Mutating one instance's lists must not affect another instance."""
        m1 = MetadataSchema(output_file="a.root")
        m2 = MetadataSchema(output_file="b.root")
        m1.required_keys.append("extra.key")
        assert "extra.key" not in m2.required_keys


# ---------------------------------------------------------------------------
# CutflowSchema
# ---------------------------------------------------------------------------


class TestCutflowSchema:
    def test_defaults(self):
        c = CutflowSchema()
        assert c.schema_version == CUTFLOW_SCHEMA_VERSION
        assert c.output_file == ""
        assert c.counter_keys == []

    def test_current_version_matches_constant(self):
        assert CutflowSchema.CURRENT_VERSION == CUTFLOW_SCHEMA_VERSION

    def test_validate_valid(self):
        c = CutflowSchema(output_file="meta.root", counter_keys=["sample.total", "sample.weighted"])
        assert c.validate() == []

    def test_validate_empty_output_file(self):
        c = CutflowSchema(output_file="")
        errors = c.validate()
        assert any("output_file" in e for e in errors)

    def test_validate_version_mismatch(self):
        c = CutflowSchema(output_file="meta.root")
        c.schema_version = 999
        errors = c.validate()
        assert any("version mismatch" in e for e in errors)

    def test_to_dict_round_trip(self):
        c = CutflowSchema(
            output_file="meta.root",
            counter_keys=["sample.total", "sample.weighted"],
        )
        d = c.to_dict()
        c2 = CutflowSchema.from_dict(d)
        assert c2.output_file == c.output_file
        assert c2.counter_keys == c.counter_keys

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "schema_version": CUTFLOW_SCHEMA_VERSION,
            "output_file": "x.root",
            "counter_keys": [],
            "unknown": "value",
        }
        c = CutflowSchema.from_dict(d)
        assert c.output_file == "x.root"


# ---------------------------------------------------------------------------
# LawArtifactSchema
# ---------------------------------------------------------------------------


class TestLawArtifactSchema:
    def test_defaults(self):
        a = LawArtifactSchema()
        assert a.schema_version == LAW_ARTIFACT_SCHEMA_VERSION
        assert a.artifact_type == ""
        assert a.path_pattern == ""
        assert a.format == "text"

    def test_current_version_matches_constant(self):
        assert LawArtifactSchema.CURRENT_VERSION == LAW_ARTIFACT_SCHEMA_VERSION

    def test_validate_valid(self):
        a = LawArtifactSchema(
            artifact_type="prepare_sample",
            path_pattern="branch_outputs/sample_*.json",
            format="json",
        )
        assert a.validate() == []

    def test_validate_empty_artifact_type(self):
        a = LawArtifactSchema(artifact_type="", path_pattern="x.txt", format="text")
        errors = a.validate()
        assert any("artifact_type" in e for e in errors)

    def test_validate_unknown_artifact_type(self):
        a = LawArtifactSchema(artifact_type="unknown_type", path_pattern="x.txt", format="text")
        errors = a.validate()
        assert any("recognised" in e for e in errors)

    def test_validate_empty_path_pattern(self):
        a = LawArtifactSchema(artifact_type="submit_jobs", path_pattern="", format="text")
        errors = a.validate()
        assert any("path_pattern" in e for e in errors)

    def test_validate_unknown_format(self):
        a = LawArtifactSchema(
            artifact_type="submit_jobs",
            path_pattern="submitted.txt",
            format="xml",
        )
        errors = a.validate()
        assert any("format" in e for e in errors)

    def test_validate_version_mismatch(self):
        a = LawArtifactSchema(
            artifact_type="submit_jobs", path_pattern="x.txt", format="text"
        )
        a.schema_version = 999
        errors = a.validate()
        assert any("version mismatch" in e for e in errors)

    def test_all_known_artifact_types_are_valid(self):
        for at in LAW_ARTIFACT_TYPES:
            a = LawArtifactSchema(artifact_type=at, path_pattern="x.txt", format="text")
            errors = a.validate()
            assert not any("recognised" in e for e in errors), f"Type '{at}' should be valid"

    def test_to_dict_round_trip(self):
        a = LawArtifactSchema(
            artifact_type="run_job",
            path_pattern="job_outputs/job_*.done",
            format="text",
        )
        d = a.to_dict()
        a2 = LawArtifactSchema.from_dict(d)
        assert a2.artifact_type == a.artifact_type
        assert a2.path_pattern == a.path_pattern
        assert a2.format == a.format

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "schema_version": LAW_ARTIFACT_SCHEMA_VERSION,
            "artifact_type": "monitor_jobs",
            "path_pattern": "all_outputs_verified.txt",
            "format": "text",
            "future_field": "ignored",
        }
        a = LawArtifactSchema.from_dict(d)
        assert a.artifact_type == "monitor_jobs"


# ---------------------------------------------------------------------------
# OutputManifest
# ---------------------------------------------------------------------------


class TestOutputManifestConstruction:
    def test_empty_manifest(self):
        m = OutputManifest()
        assert m.manifest_version == OUTPUT_MANIFEST_VERSION
        assert m.skim is None
        assert m.histograms is None
        assert m.metadata is None
        assert m.cutflow is None
        assert m.law_artifacts == []

    def test_with_skim(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        assert m.skim is not None
        assert m.skim.output_file == "out.root"

    def test_with_all_schemas(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(output_file="meta.root"),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="meta.root"),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="prepare_sample",
                    path_pattern="branch_outputs/sample_*.json",
                    format="json",
                )
            ],
        )
        assert m.skim is not None
        assert m.histograms is not None
        assert m.metadata is not None
        assert m.cutflow is not None
        assert len(m.law_artifacts) == 1


class TestOutputManifestValidation:
    def test_valid_skim_only(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        assert m.validate() == []

    def test_valid_full(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            metadata=MetadataSchema(output_file="meta.root"),
        )
        assert m.validate() == []

    def test_empty_manifest_invalid(self):
        m = OutputManifest()
        errors = m.validate()
        assert any("at least one" in e for e in errors)

    def test_version_mismatch_surfaced(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        m.manifest_version = 999
        errors = m.validate()
        assert any("version mismatch" in e for e in errors)

    def test_nested_schema_errors_propagated(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="", tree_name=""),  # two errors
        )
        errors = m.validate()
        assert len(errors) >= 2

    def test_law_artifact_errors_indexed(self):
        m = OutputManifest(
            metadata=MetadataSchema(output_file="meta.root"),
            law_artifacts=[
                LawArtifactSchema(artifact_type="", path_pattern="", format="text"),
            ],
        )
        errors = m.validate()
        assert any("law_artifacts[0]" in e for e in errors)


class TestOutputManifestSerialization:
    def test_to_dict_none_schemas_preserved(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        d = m.to_dict()
        assert d["skim"] is not None
        assert d["histograms"] is None
        assert d["metadata"] is None
        assert d["cutflow"] is None
        assert d["law_artifacts"] == []

    def test_to_dict_round_trip(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root", tree_name="Events", branches=["pt"]),
            histograms=HistogramSchema(
                output_file="meta.root",
                histogram_names=["h_pt"],
                axes=[HistogramAxisSpec(variable="pt", bins=50, lower_bound=0.0, upper_bound=500.0)],
            ),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="meta.root", counter_keys=["s.total"]),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="prepare_sample",
                    path_pattern="branch_outputs/sample_*.json",
                    format="json",
                )
            ],
        )
        d = m.to_dict()
        m2 = OutputManifest.from_dict(d)

        assert m2.manifest_version == m.manifest_version
        assert m2.skim is not None
        assert m2.skim.output_file == "out.root"
        assert m2.skim.branches == ["pt"]
        assert m2.histograms is not None
        assert m2.histograms.histogram_names == ["h_pt"]
        assert len(m2.histograms.axes) == 1
        assert m2.histograms.axes[0].variable == "pt"
        assert m2.metadata is not None
        assert m2.cutflow is not None
        assert m2.cutflow.counter_keys == ["s.total"]
        assert len(m2.law_artifacts) == 1
        assert m2.law_artifacts[0].artifact_type == "prepare_sample"

    def test_from_dict_none_schemas(self):
        d = {
            "manifest_version": OUTPUT_MANIFEST_VERSION,
            "skim": None,
            "histograms": None,
            "metadata": None,
            "cutflow": None,
            "law_artifacts": [],
        }
        m = OutputManifest.from_dict(d)
        assert m.skim is None
        assert m.histograms is None


class TestOutputManifestYAML:
    def test_save_and_load_yaml(self, tmp_path):
        original = OutputManifest(
            skim=SkimSchema(output_file="out.root", tree_name="Events", branches=["pt", "eta"]),
            metadata=MetadataSchema(output_file="meta.root"),
        )
        path = str(tmp_path / "manifest.yaml")
        original.save_yaml(path)
        loaded = OutputManifest.load_yaml(path)

        assert loaded.manifest_version == original.manifest_version
        assert loaded.skim is not None
        assert loaded.skim.output_file == "out.root"
        assert loaded.skim.branches == ["pt", "eta"]
        assert loaded.metadata is not None
        assert loaded.metadata.output_file == "meta.root"
        assert loaded.histograms is None
        assert loaded.cutflow is None
        assert loaded.law_artifacts == []

    def test_load_yaml_invalid_type_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("- a\n- b\n")
        with pytest.raises(ValueError, match="mapping"):
            OutputManifest.load_yaml(str(p))

    def test_yaml_contains_version(self, tmp_path):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)
        with open(path) as fh:
            raw = yaml.safe_load(fh)
        assert "manifest_version" in raw
        assert raw["manifest_version"] == OUTPUT_MANIFEST_VERSION

    def test_full_manifest_yaml_round_trip(self, tmp_path):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(
                output_file="meta.root",
                axes=[HistogramAxisSpec(variable="pt", bins=100, lower_bound=0.0, upper_bound=1000.0)],
            ),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="meta.root"),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="monitor_state",
                    path_pattern="monitor_state.json",
                    format="json",
                )
            ],
        )
        path = str(tmp_path / "full_manifest.yaml")
        m.save_yaml(path)
        m2 = OutputManifest.load_yaml(path)
        assert m2.histograms is not None
        assert m2.histograms.axes[0].variable == "pt"
        assert m2.law_artifacts[0].format == "json"


class TestOutputManifestVersionCheck:
    def test_compatible_manifest_passes(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        # Should not raise
        OutputManifest.check_version_compatibility(m)

    def test_manifest_version_mismatch_raises(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        m.manifest_version = 999
        with pytest.raises(SchemaVersionError, match="OutputManifest"):
            OutputManifest.check_version_compatibility(m)

    def test_skim_version_mismatch_raises(self):
        s = SkimSchema(output_file="out.root")
        s.schema_version = 999
        m = OutputManifest(skim=s)
        with pytest.raises(SchemaVersionError, match="skim"):
            OutputManifest.check_version_compatibility(m)

    def test_histogram_version_mismatch_raises(self):
        h = HistogramSchema(output_file="meta.root")
        h.schema_version = 999
        m = OutputManifest(histograms=h)
        with pytest.raises(SchemaVersionError, match="histograms"):
            OutputManifest.check_version_compatibility(m)

    def test_metadata_version_mismatch_raises(self):
        md = MetadataSchema(output_file="meta.root")
        md.schema_version = 999
        m = OutputManifest(metadata=md)
        with pytest.raises(SchemaVersionError, match="metadata"):
            OutputManifest.check_version_compatibility(m)

    def test_cutflow_version_mismatch_raises(self):
        c = CutflowSchema(output_file="meta.root")
        c.schema_version = 999
        m = OutputManifest(cutflow=c)
        with pytest.raises(SchemaVersionError, match="cutflow"):
            OutputManifest.check_version_compatibility(m)

    def test_law_artifact_version_mismatch_raises(self):
        a = LawArtifactSchema(
            artifact_type="submit_jobs", path_pattern="submitted.txt", format="text"
        )
        a.schema_version = 999
        m = OutputManifest(metadata=MetadataSchema(output_file="meta.root"), law_artifacts=[a])
        with pytest.raises(SchemaVersionError, match="law_artifacts"):
            OutputManifest.check_version_compatibility(m)

    def test_schema_version_error_is_runtime_error(self):
        assert issubclass(SchemaVersionError, RuntimeError)

    def test_error_message_lists_all_mismatches(self):
        s = SkimSchema(output_file="out.root")
        s.schema_version = 999
        h = HistogramSchema(output_file="meta.root")
        h.schema_version = 888
        m = OutputManifest(skim=s, histograms=h)
        with pytest.raises(SchemaVersionError) as exc_info:
            OutputManifest.check_version_compatibility(m)
        msg = str(exc_info.value)
        assert "skim" in msg
        assert "histograms" in msg


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
        artifact = SkimSchema(output_file="s.root")
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_must_regenerate_version_mismatch(self):
        artifact = SkimSchema(output_file="s.root")
        artifact.schema_version = 999
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_must_regenerate_object_without_current_version(self):
        class BareArtifact:
            schema_version = 1
        status = resolve_artifact(BareArtifact())
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_compatible_matching_provenance(self):
        artifact = SkimSchema(output_file="s.root")
        recorded = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        current = ProvenanceRecord(framework_hash="fw1", user_repo_hash="ur1")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_stale_changed_framework_hash(self):
        artifact = SkimSchema(output_file="s.root")
        recorded = ProvenanceRecord(framework_hash="fw_old")
        current = ProvenanceRecord(framework_hash="fw_new")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_stale_changed_user_repo_hash(self):
        artifact = HistogramSchema(output_file="h.root")
        recorded = ProvenanceRecord(user_repo_hash="ur_old")
        current = ProvenanceRecord(user_repo_hash="ur_new")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_stale_changed_config_mtime(self):
        artifact = CutflowSchema(output_file="c.root")
        recorded = ProvenanceRecord(config_mtime="2024-01-01T00:00:00+00:00")
        current = ProvenanceRecord(config_mtime="2025-01-01T00:00:00+00:00")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.STALE

    def test_compatible_when_only_recorded_provenance_given(self):
        artifact = SkimSchema(output_file="s.root")
        recorded = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, recorded_provenance=recorded)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_compatible_when_only_current_provenance_given(self):
        artifact = SkimSchema(output_file="s.root")
        current = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, current_provenance=current)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_must_regenerate_takes_priority_over_provenance(self):
        artifact = SkimSchema(output_file="s.root")
        artifact.schema_version = 999
        recorded = ProvenanceRecord(framework_hash="fw1")
        current = ProvenanceRecord(framework_hash="fw1")
        status = resolve_artifact(artifact, recorded, current)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_law_artifact_compatible(self):
        artifact = LawArtifactSchema(
            artifact_type="submit_jobs",
            path_pattern="submitted.txt",
            format="text",
        )
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_metadata_schema_compatible(self):
        artifact = MetadataSchema(output_file="meta.root")
        status = resolve_artifact(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE


# ---------------------------------------------------------------------------
# resolve_manifest
# ---------------------------------------------------------------------------


class TestResolveManifest:
    def test_empty_manifest(self):
        m = OutputManifest()
        assert resolve_manifest(m) == {}

    def test_skim_only_compatible(self):
        m = OutputManifest(skim=SkimSchema(output_file="s.root"))
        statuses = resolve_manifest(m)
        assert statuses["skim"] == ArtifactResolutionStatus.COMPATIBLE

    def test_all_schemas_compatible_no_provenance(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
            histograms=HistogramSchema(output_file="h.root"),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="meta.root"),
        )
        statuses = resolve_manifest(m)
        for role, status in statuses.items():
            assert status == ArtifactResolutionStatus.COMPATIBLE, role

    def test_stale_when_hash_changes(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
            framework_hash="fw_old",
        )
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = resolve_manifest(m, current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.STALE

    def test_must_regenerate_version_mismatch(self):
        s = SkimSchema(output_file="s.root")
        s.schema_version = 999
        m = OutputManifest(skim=s)
        statuses = resolve_manifest(m)
        assert statuses["skim"] == ArtifactResolutionStatus.MUST_REGENERATE

    def test_law_artifacts_indexed_roles(self):
        law1 = LawArtifactSchema(
            artifact_type="prepare_sample",
            path_pattern="branch_outputs/sample_*.json",
            format="json",
        )
        law2 = LawArtifactSchema(
            artifact_type="submit_jobs",
            path_pattern="submitted.txt",
            format="text",
        )
        m = OutputManifest(
            metadata=MetadataSchema(output_file="meta.root"),
            law_artifacts=[law1, law2],
        )
        statuses = resolve_manifest(m)
        assert "law_artifacts[0]" in statuses
        assert "law_artifacts[1]" in statuses
        assert statuses["law_artifacts[0]"] == ArtifactResolutionStatus.COMPATIBLE

    def test_none_schemas_not_included(self):
        m = OutputManifest(skim=SkimSchema(output_file="s.root"))
        statuses = resolve_manifest(m)
        assert "histograms" not in statuses
        assert "metadata" not in statuses
        assert "cutflow" not in statuses

    def test_mixed_statuses(self):
        s = SkimSchema(output_file="s.root")
        s.schema_version = 999
        m = OutputManifest(
            skim=s,
            histograms=HistogramSchema(output_file="h.root"),
            framework_hash="fw_old",
        )
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = resolve_manifest(m, current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.MUST_REGENERATE
        assert statuses["histograms"] == ArtifactResolutionStatus.STALE


# ---------------------------------------------------------------------------
# OutputManifest.provenance() and resolve()
# ---------------------------------------------------------------------------


class TestOutputManifestProvenance:
    def test_provenance_returns_record(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
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
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
            framework_hash="fw_old",
        )
        current = ProvenanceRecord(framework_hash="fw_new")
        statuses = m.resolve(current_provenance=current)
        assert statuses["skim"] == ArtifactResolutionStatus.STALE

    def test_resolve_without_provenance(self):
        m = OutputManifest(skim=SkimSchema(output_file="s.root"))
        statuses = m.resolve()
        assert statuses["skim"] == ArtifactResolutionStatus.COMPATIBLE

    def test_provenance_fields_round_trip_via_yaml(self, tmp_path):
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
            framework_hash="fw1",
            user_repo_hash="ur1",
            config_mtime="2024-06-01T12:00:00+00:00",
        )
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)
        m2 = OutputManifest.load_yaml(path)
        assert m2.framework_hash == "fw1"
        assert m2.user_repo_hash == "ur1"
        assert m2.config_mtime == "2024-06-01T12:00:00+00:00"

    def test_provenance_fields_default_none_in_yaml(self, tmp_path):
        m = OutputManifest(skim=SkimSchema(output_file="s.root"))
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)
        m2 = OutputManifest.load_yaml(path)
        assert m2.framework_hash is None
        assert m2.config_mtime is None

    def test_full_resolution_workflow(self, tmp_path):
        """End-to-end: produce manifest, then resolve with updated env."""
        m = OutputManifest(
            skim=SkimSchema(output_file="s.root"),
            histograms=HistogramSchema(output_file="h.root"),
            framework_hash="fw_old",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)

        m2 = OutputManifest.load_yaml(path)

        # Same environment - all COMPATIBLE.
        same = ProvenanceRecord(
            framework_hash="fw_old",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        for status in m2.resolve(same).values():
            assert status == ArtifactResolutionStatus.COMPATIBLE

        # Updated framework hash - all STALE.
        updated = ProvenanceRecord(
            framework_hash="fw_new",
            user_repo_hash="ur1",
            config_mtime="2024-01-01T00:00:00+00:00",
        )
        for status in m2.resolve(updated).values():
            assert status == ArtifactResolutionStatus.STALE


# ---------------------------------------------------------------------------
# DatasetManifestProvenance
# ---------------------------------------------------------------------------


class TestDatasetManifestProvenance:
    def test_defaults(self):
        p = DatasetManifestProvenance()
        assert p.manifest_path is None
        assert p.manifest_hash is None
        assert p.query_params is None
        assert p.resolved_entry_names is None

    def test_full_construction(self):
        p = DatasetManifestProvenance(
            manifest_path="datasets.yaml",
            manifest_hash="abc123",
            query_params={"year": 2022, "dtype": "mc"},
            resolved_entry_names=["ttbar_2022", "wjets_2022"],
        )
        assert p.manifest_path == "datasets.yaml"
        assert p.manifest_hash == "abc123"
        assert p.query_params == {"year": 2022, "dtype": "mc"}
        assert p.resolved_entry_names == ["ttbar_2022", "wjets_2022"]

    def test_to_dict_contains_all_fields(self):
        p = DatasetManifestProvenance(
            manifest_path="d.yaml",
            manifest_hash="deadbeef",
            query_params={"year": 2023},
            resolved_entry_names=["a", "b"],
        )
        d = p.to_dict()
        assert d["manifest_path"] == "d.yaml"
        assert d["manifest_hash"] == "deadbeef"
        assert d["query_params"] == {"year": 2023}
        assert d["resolved_entry_names"] == ["a", "b"]

    def test_to_dict_none_fields(self):
        p = DatasetManifestProvenance()
        d = p.to_dict()
        assert d["manifest_path"] is None
        assert d["manifest_hash"] is None
        assert d["query_params"] is None
        assert d["resolved_entry_names"] is None

    def test_from_dict_round_trip(self):
        p = DatasetManifestProvenance(
            manifest_path="datasets.yaml",
            manifest_hash="abc123",
            query_params={"year": 2022},
            resolved_entry_names=["ttbar"],
        )
        p2 = DatasetManifestProvenance.from_dict(p.to_dict())
        assert p2.manifest_path == p.manifest_path
        assert p2.manifest_hash == p.manifest_hash
        assert p2.query_params == p.query_params
        assert p2.resolved_entry_names == p.resolved_entry_names

    def test_from_dict_partial(self):
        d = {"manifest_hash": "xyz"}
        p = DatasetManifestProvenance.from_dict(d)
        assert p.manifest_hash == "xyz"
        assert p.manifest_path is None
        assert p.query_params is None
        assert p.resolved_entry_names is None

    def test_from_dict_empty(self):
        p = DatasetManifestProvenance.from_dict({})
        assert p.manifest_path is None
        assert p.manifest_hash is None

    def test_query_params_multi_value(self):
        """query_params may contain list values (multi-year queries)."""
        p = DatasetManifestProvenance(
            manifest_hash="h",
            query_params={"year": [2022, 2023], "dtype": "mc"},
            resolved_entry_names=["a", "b", "c"],
        )
        d = p.to_dict()
        p2 = DatasetManifestProvenance.from_dict(d)
        assert p2.query_params["year"] == [2022, 2023]


# ---------------------------------------------------------------------------
# OutputManifest – dataset_manifest_provenance integration
# ---------------------------------------------------------------------------


class TestOutputManifestDatasetManifestProvenance:
    def _make_prov(self):
        return DatasetManifestProvenance(
            manifest_path="datasets.yaml",
            manifest_hash="abc123",
            query_params={"year": 2022, "dtype": "mc"},
            resolved_entry_names=["ttbar_2022", "wjets_2022"],
        )

    def test_default_is_none(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        assert m.dataset_manifest_provenance is None

    def test_set_on_construction(self):
        prov = self._make_prov()
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            dataset_manifest_provenance=prov,
        )
        assert m.dataset_manifest_provenance is prov

    def test_to_dict_includes_provenance(self):
        prov = self._make_prov()
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            dataset_manifest_provenance=prov,
        )
        d = m.to_dict()
        assert d["dataset_manifest_provenance"] is not None
        assert d["dataset_manifest_provenance"]["manifest_hash"] == "abc123"
        assert d["dataset_manifest_provenance"]["query_params"] == {"year": 2022, "dtype": "mc"}
        assert d["dataset_manifest_provenance"]["resolved_entry_names"] == [
            "ttbar_2022", "wjets_2022"
        ]

    def test_to_dict_none_provenance(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        d = m.to_dict()
        assert d["dataset_manifest_provenance"] is None

    def test_from_dict_round_trip_with_provenance(self):
        prov = self._make_prov()
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            dataset_manifest_provenance=prov,
        )
        m2 = OutputManifest.from_dict(m.to_dict())
        assert m2.dataset_manifest_provenance is not None
        assert m2.dataset_manifest_provenance.manifest_hash == "abc123"
        assert m2.dataset_manifest_provenance.query_params == {"year": 2022, "dtype": "mc"}
        assert m2.dataset_manifest_provenance.resolved_entry_names == [
            "ttbar_2022", "wjets_2022"
        ]

    def test_from_dict_no_provenance_field_returns_none(self):
        """Old manifests without the field should deserialise cleanly."""
        d = {
            "manifest_version": OUTPUT_MANIFEST_VERSION,
            "skim": {"schema_version": SKIM_SCHEMA_VERSION, "output_file": "out.root",
                     "tree_name": "Events", "branches": []},
        }
        m = OutputManifest.from_dict(d)
        assert m.dataset_manifest_provenance is None

    def test_yaml_round_trip_with_provenance(self, tmp_path):
        prov = DatasetManifestProvenance(
            manifest_path="datasets.yaml",
            manifest_hash="deadbeef",
            query_params={"year": 2022},
            resolved_entry_names=["s1", "s2"],
        )
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            dataset_manifest_provenance=prov,
        )
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)
        m2 = OutputManifest.load_yaml(path)
        assert m2.dataset_manifest_provenance is not None
        assert m2.dataset_manifest_provenance.manifest_hash == "deadbeef"
        assert m2.dataset_manifest_provenance.query_params == {"year": 2022}
        assert m2.dataset_manifest_provenance.resolved_entry_names == ["s1", "s2"]

    def test_yaml_round_trip_none_provenance(self, tmp_path):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)
        m2 = OutputManifest.load_yaml(path)
        assert m2.dataset_manifest_provenance is None

    def test_validate_passes_with_provenance(self):
        prov = self._make_prov()
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            dataset_manifest_provenance=prov,
        )
        assert m.validate() == []

    def test_provenance_optional_keys_include_manifest_keys(self):
        assert "dataset_manifest.file_hash" in PROVENANCE_OPTIONAL_KEYS
        assert "dataset_manifest.query_params" in PROVENANCE_OPTIONAL_KEYS
        assert "dataset_manifest.resolved_entries" in PROVENANCE_OPTIONAL_KEYS


# ---------------------------------------------------------------------------
# validate_merge_inputs
# ---------------------------------------------------------------------------


class TestValidateMergeInputs:
    """Tests for the validate_merge_inputs() function."""

    def _make_skim_manifest(self, output_file="out.root"):
        return OutputManifest(skim=SkimSchema(output_file=output_file))

    def _make_histogram_manifest(self, output_file="meta.root"):
        return OutputManifest(histograms=HistogramSchema(output_file=output_file))

    def _make_full_manifest(self, skim_file="out.root", meta_file="meta.root"):
        return OutputManifest(
            skim=SkimSchema(output_file=skim_file),
            histograms=HistogramSchema(output_file=meta_file),
            metadata=MetadataSchema(output_file=meta_file),
            cutflow=CutflowSchema(output_file=meta_file),
        )

    # -- empty input --

    def test_no_manifests_returns_error(self):
        errors = validate_merge_inputs([])
        assert len(errors) == 1
        assert "no manifests provided" in errors[0]

    # -- single valid manifest --

    def test_single_valid_manifest(self):
        m = self._make_skim_manifest()
        assert validate_merge_inputs([m]) == []

    # -- per-manifest structural errors --

    def test_empty_manifest_reported(self):
        m = OutputManifest()  # no schemas — fails validate()
        errors = validate_merge_inputs([m])
        assert any("manifest[0]" in e for e in errors)

    def test_malformed_artifact_reported(self):
        m = OutputManifest(skim=SkimSchema(output_file=""))  # empty output_file
        errors = validate_merge_inputs([m])
        assert any("manifest[0]" in e for e in errors)

    # -- version compatibility errors --

    def test_incompatible_schema_version_reported(self):
        s = SkimSchema(output_file="out.root")
        s.schema_version = 999
        m = OutputManifest(skim=s)
        errors = validate_merge_inputs([m])
        assert any("incompatible schema version" in e for e in errors)

    # -- role consistency across manifests --

    def test_two_compatible_skim_manifests(self):
        m1 = self._make_skim_manifest("a.root")
        m2 = self._make_skim_manifest("b.root")
        assert validate_merge_inputs([m1, m2]) == []

    def test_inconsistent_role_presence_reported(self):
        m1 = self._make_skim_manifest()
        m2 = self._make_histogram_manifest()  # skim absent, histograms present
        errors = validate_merge_inputs([m1, m2])
        # skim present in m1 but absent in m2; histograms absent in m1 but present in m2
        assert any("skim" in e for e in errors)
        assert any("histograms" in e for e in errors)

    def test_inconsistent_schema_version_reported(self):
        m1 = self._make_skim_manifest()
        m2 = OutputManifest(skim=SkimSchema(output_file="b.root"))
        m2.skim.schema_version = 999  # tweak after construction to bypass version check
        errors = validate_merge_inputs([m1, m2])
        # Both version-incompatibility (rule 3) and version mismatch (rule 5) fire.
        assert len(errors) > 0

    def test_multiple_manifests_all_valid(self):
        manifests = [self._make_full_manifest(f"out_{i}.root", f"meta_{i}.root") for i in range(5)]
        assert validate_merge_inputs(manifests) == []

    # -- law_artifacts consistency --

    def _make_law_manifest(self, artifact_type="prepare_sample"):
        return OutputManifest(
            metadata=MetadataSchema(output_file="meta.root"),
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type=artifact_type,
                    path_pattern="branch_outputs/sample_*.json",
                    format="json",
                )
            ],
        )

    def test_law_artifacts_count_mismatch_reported(self):
        m1 = self._make_law_manifest()
        m2 = OutputManifest(
            metadata=MetadataSchema(output_file="meta.root"),
            law_artifacts=[],
        )
        errors = validate_merge_inputs([m1, m2])
        assert any("law_artifacts count" in e for e in errors)

    def test_law_artifacts_version_mismatch_reported(self):
        m1 = self._make_law_manifest()
        m2 = self._make_law_manifest()
        m2.law_artifacts[0].schema_version = 999
        errors = validate_merge_inputs([m1, m2])
        assert any("law_artifacts[0]" in e for e in errors)

    def test_law_artifacts_compatible(self):
        m1 = self._make_law_manifest()
        m2 = self._make_law_manifest()
        assert validate_merge_inputs([m1, m2]) == []

    # -- required_roles enforcement --

    def test_required_role_present(self):
        m = self._make_skim_manifest()
        assert validate_merge_inputs([m], required_roles=["skim"]) == []

    def test_required_role_absent_reported(self):
        m = self._make_histogram_manifest()
        errors = validate_merge_inputs([m], required_roles=["skim"])
        assert any("required role 'skim'" in e for e in errors)

    def test_required_law_artifacts_present(self):
        m = self._make_law_manifest()
        assert validate_merge_inputs([m], required_roles=["law_artifacts"]) == []

    def test_required_law_artifacts_empty_reported(self):
        m = self._make_histogram_manifest()
        errors = validate_merge_inputs([m], required_roles=["law_artifacts"])
        assert any("law_artifacts" in e for e in errors)

    def test_unknown_required_role_reported(self):
        m = self._make_skim_manifest()
        errors = validate_merge_inputs([m], required_roles=["nonexistent_role"])
        assert any("Unknown required role" in e for e in errors)

    def test_required_roles_checked_for_every_manifest(self):
        m1 = self._make_skim_manifest()
        m2 = OutputManifest(histograms=HistogramSchema(output_file="meta.root"))
        # Both manifests should be checked; m2 missing skim
        errors = validate_merge_inputs([m1, m2], required_roles=["skim"])
        skim_errors = [e for e in errors if "required role 'skim'" in e]
        # m2 at index 1 should be reported
        assert any("manifest[1]" in e for e in skim_errors)


# ---------------------------------------------------------------------------
# merge_manifests
# ---------------------------------------------------------------------------


class TestMergeManifests:
    """Tests for the merge_manifests() function."""

    def _make_skim_manifest(self, output_file="out.root"):
        return OutputManifest(skim=SkimSchema(output_file=output_file))

    def _make_full_manifest(self, skim_file="out.root", meta_file="meta.root"):
        return OutputManifest(
            skim=SkimSchema(output_file=skim_file),
            histograms=HistogramSchema(output_file=meta_file),
            metadata=MetadataSchema(output_file=meta_file),
            cutflow=CutflowSchema(output_file=meta_file),
        )

    # -- happy path --

    def test_merge_single_manifest(self):
        m = self._make_skim_manifest()
        result = merge_manifests([m])
        assert isinstance(result, OutputManifest)
        assert result.skim is not None

    def test_merge_two_skim_manifests(self):
        m1 = self._make_skim_manifest("a.root")
        m2 = self._make_skim_manifest("b.root")
        result = merge_manifests([m1, m2])
        assert result.skim is not None
        assert result.histograms is None

    def test_merge_preserves_schema_structure(self):
        manifests = [self._make_full_manifest(f"o{i}.root", f"m{i}.root") for i in range(3)]
        result = merge_manifests(manifests)
        assert result.skim is not None
        assert result.histograms is not None
        assert result.metadata is not None
        assert result.cutflow is not None

    def test_merge_copies_schema_from_first_input(self):
        m1 = OutputManifest(
            skim=SkimSchema(output_file="first.root", tree_name="MyTree", branches=["pt"])
        )
        m2 = OutputManifest(
            skim=SkimSchema(output_file="second.root", tree_name="MyTree", branches=["pt"])
        )
        result = merge_manifests([m1, m2])
        assert result.skim.output_file == "first.root"
        assert result.skim.tree_name == "MyTree"
        assert result.skim.branches == ["pt"]

    def test_merge_sets_framework_hash(self):
        m = self._make_skim_manifest()
        result = merge_manifests([m], framework_hash="fw_merged")
        assert result.framework_hash == "fw_merged"

    def test_merge_sets_user_repo_hash(self):
        m = self._make_skim_manifest()
        result = merge_manifests([m], user_repo_hash="ur_merged")
        assert result.user_repo_hash == "ur_merged"

    def test_merge_hashes_none_by_default(self):
        m = self._make_skim_manifest()
        result = merge_manifests([m])
        assert result.framework_hash is None
        assert result.user_repo_hash is None

    def test_merge_with_law_artifacts(self):
        def make_law():
            return OutputManifest(
                metadata=MetadataSchema(output_file="meta.root"),
                law_artifacts=[
                    LawArtifactSchema(
                        artifact_type="prepare_sample",
                        path_pattern="branch_outputs/sample_*.json",
                        format="json",
                    )
                ],
            )

        result = merge_manifests([make_law(), make_law()])
        assert len(result.law_artifacts) == 1
        assert result.law_artifacts[0].artifact_type == "prepare_sample"

    def test_merged_manifest_is_valid(self):
        manifests = [self._make_full_manifest(f"o{i}.root", f"m{i}.root") for i in range(3)]
        result = merge_manifests(manifests)
        assert result.validate() == []

    def test_merged_manifest_output_file_can_be_updated(self):
        m1 = self._make_skim_manifest("a.root")
        m2 = self._make_skim_manifest("b.root")
        result = merge_manifests([m1, m2])
        # Callers update the output_file after running hadd etc.
        result.skim.output_file = "merged.root"
        assert result.skim.output_file == "merged.root"
        assert result.validate() == []

    # -- error cases --

    def test_merge_empty_list_raises(self):
        with pytest.raises(MergeInputValidationError, match="no manifests provided"):
            merge_manifests([])

    def test_merge_incompatible_roles_raises(self):
        m1 = self._make_skim_manifest()
        m2 = OutputManifest(histograms=HistogramSchema(output_file="meta.root"))
        with pytest.raises(MergeInputValidationError):
            merge_manifests([m1, m2])

    def test_merge_incompatible_version_raises(self):
        m1 = self._make_skim_manifest()
        m2 = OutputManifest(skim=SkimSchema(output_file="b.root"))
        m2.skim.schema_version = 999
        with pytest.raises(MergeInputValidationError):
            merge_manifests([m1, m2])

    def test_merge_required_role_missing_raises(self):
        m = OutputManifest(histograms=HistogramSchema(output_file="meta.root"))
        with pytest.raises(MergeInputValidationError, match="required role"):
            merge_manifests([m], required_roles=["skim"])

    def test_merge_input_validation_error_is_runtime_error(self):
        assert issubclass(MergeInputValidationError, RuntimeError)

    def test_merge_error_message_contains_diagnostics(self):
        m1 = self._make_skim_manifest()
        m2 = OutputManifest(histograms=HistogramSchema(output_file="meta.root"))
        with pytest.raises(MergeInputValidationError) as exc_info:
            merge_manifests([m1, m2])
        msg = str(exc_info.value)
        assert "skim" in msg

    # -- YAML round-trip --

    def test_merged_manifest_yaml_round_trip(self, tmp_path):
        manifests = [self._make_full_manifest(f"o{i}.root", f"m{i}.root") for i in range(2)]
        result = merge_manifests(manifests, framework_hash="fw_hash")
        result.skim.output_file = "merged.root"
        result.histograms.output_file = "merged_meta.root"
        result.metadata.output_file = "merged_meta.root"
        result.cutflow.output_file = "merged_meta.root"
        path = str(tmp_path / "merged_manifest.yaml")
        result.save_yaml(path)
        loaded = OutputManifest.load_yaml(path)
        assert loaded.framework_hash == "fw_hash"
        assert loaded.skim.output_file == "merged.root"
        assert loaded.histograms.output_file == "merged_meta.root"
        assert loaded.validate() == []


# ---------------------------------------------------------------------------
# CachedArtifact
# ---------------------------------------------------------------------------


class TestCachedArtifact:
    def _make_manifest(self, fw_hash=None):
        return OutputManifest(
            skim=SkimSchema(output_file="presel.root"),
            framework_hash=fw_hash,
        )

    def test_construction_defaults(self):
        m = self._make_manifest()
        ca = CachedArtifact(artifact_path="presel.root", manifest=m)
        assert ca.artifact_path == "presel.root"
        assert ca.manifest is m
        assert ca.cached_at  # non-empty ISO timestamp

    def test_explicit_cached_at(self):
        ts = "2024-06-01T12:00:00+00:00"
        m = self._make_manifest()
        ca = CachedArtifact(artifact_path="presel.root", manifest=m, cached_at=ts)
        assert ca.cached_at == ts

    def test_to_dict_contains_expected_keys(self):
        m = self._make_manifest(fw_hash="abc123")
        ca = CachedArtifact(artifact_path="presel.root", manifest=m, cached_at="2024-01-01T00:00:00+00:00")
        d = ca.to_dict()
        assert d["artifact_path"] == "presel.root"
        assert d["cached_at"] == "2024-01-01T00:00:00+00:00"
        assert "manifest" in d
        assert d["manifest"]["skim"]["output_file"] == "presel.root"

    def test_from_dict_round_trip(self):
        m = self._make_manifest(fw_hash="abc123")
        ca = CachedArtifact(
            artifact_path="presel.root",
            manifest=m,
            cached_at="2024-06-01T12:00:00+00:00",
        )
        ca2 = CachedArtifact.from_dict(ca.to_dict())
        assert ca2.artifact_path == ca.artifact_path
        assert ca2.cached_at == ca.cached_at
        assert ca2.manifest.skim.output_file == "presel.root"
        assert ca2.manifest.framework_hash == "abc123"

    def test_from_dict_empty_artifact_path(self):
        m = self._make_manifest()
        d = CachedArtifact(artifact_path="x.root", manifest=m).to_dict()
        del d["artifact_path"]
        ca = CachedArtifact.from_dict(d)
        assert ca.artifact_path == ""


# ---------------------------------------------------------------------------
# CACHE_SIDECAR_SUFFIX constant
# ---------------------------------------------------------------------------


class TestCacheSidecarSuffix:
    def test_suffix_is_string(self):
        assert isinstance(CACHE_SIDECAR_SUFFIX, str)

    def test_suffix_starts_with_dot(self):
        assert CACHE_SIDECAR_SUFFIX.startswith(".")

    def test_suffix_ends_with_yaml(self):
        assert CACHE_SIDECAR_SUFFIX.endswith(".yaml")


# ---------------------------------------------------------------------------
# write_cache_sidecar / read_cache_sidecar
# ---------------------------------------------------------------------------


class TestWriteReadCacheSidecar:
    def _make_manifest(self, fw_hash="fw1"):
        return OutputManifest(
            skim=SkimSchema(output_file="presel.root"),
            framework_hash=fw_hash,
        )

    def test_write_creates_sidecar(self, tmp_path):
        artifact = str(tmp_path / "presel.root")
        open(artifact, "w").close()
        m = self._make_manifest()
        sidecar_path = write_cache_sidecar(artifact, m)
        assert os.path.isfile(sidecar_path)
        assert sidecar_path == artifact + CACHE_SIDECAR_SUFFIX

    def test_write_returns_sidecar_path(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        m = self._make_manifest()
        result = write_cache_sidecar(artifact, m)
        assert result == artifact + CACHE_SIDECAR_SUFFIX

    def test_read_returns_cached_artifact(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        m = self._make_manifest(fw_hash="xyz")
        write_cache_sidecar(artifact, m)
        ca = read_cache_sidecar(artifact)
        assert ca.artifact_path == artifact
        assert ca.manifest.framework_hash == "xyz"
        assert ca.manifest.skim.output_file == "presel.root"

    def test_read_raises_when_no_sidecar(self, tmp_path):
        artifact = str(tmp_path / "missing.root")
        with pytest.raises(FileNotFoundError, match="cache sidecar"):
            read_cache_sidecar(artifact)

    def test_read_raises_on_invalid_yaml(self, tmp_path):
        artifact = str(tmp_path / "bad.root")
        sidecar = artifact + CACHE_SIDECAR_SUFFIX
        with open(sidecar, "w") as fh:
            fh.write("- item1\n- item2\n")
        with pytest.raises(ValueError, match="mapping"):
            read_cache_sidecar(artifact)

    def test_round_trip_preserves_all_schema_types(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(output_file="meta.root"),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="meta.root"),
            framework_hash="abc123",
            user_repo_hash="ur1",
        )
        write_cache_sidecar(artifact, m)
        ca = read_cache_sidecar(artifact)
        assert ca.manifest.skim.output_file == "out.root"
        assert ca.manifest.histograms.output_file == "meta.root"
        assert ca.manifest.metadata.output_file == "meta.root"
        assert ca.manifest.cutflow.output_file == "meta.root"
        assert ca.manifest.framework_hash == "abc123"
        assert ca.manifest.user_repo_hash == "ur1"

    def test_sidecar_is_valid_yaml_file(self, tmp_path):
        import yaml as _yaml
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        m = self._make_manifest()
        sidecar_path = write_cache_sidecar(artifact, m)
        with open(sidecar_path) as fh:
            data = _yaml.safe_load(fh)
        assert isinstance(data, dict)
        assert "artifact_path" in data
        assert "cached_at" in data
        assert "manifest" in data

    def test_write_with_explicit_cached_at(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        ts = "2024-01-01T00:00:00+00:00"
        m = self._make_manifest()
        write_cache_sidecar(artifact, m, cached_at=ts)
        ca = read_cache_sidecar(artifact)
        assert ca.cached_at == ts

    def test_write_creates_parent_dirs(self, tmp_path):
        artifact = str(tmp_path / "deep" / "subdir" / "out.root")
        os.makedirs(os.path.dirname(artifact), exist_ok=True)
        open(artifact, "w").close()
        m = self._make_manifest()
        sidecar_path = write_cache_sidecar(artifact, m)
        assert os.path.isfile(sidecar_path)


# ---------------------------------------------------------------------------
# check_cache_validity
# ---------------------------------------------------------------------------


class TestCheckCacheValidity:
    def _make_manifest(self, fw_hash=None):
        return OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            framework_hash=fw_hash,
        )

    def _write(self, artifact, manifest, tmp_path=None):
        """Write empty artifact + sidecar, return artifact path."""
        open(artifact, "w").close()
        write_cache_sidecar(artifact, manifest)
        return artifact

    def test_compatible_no_provenance(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        self._write(artifact, self._make_manifest())
        status = check_cache_validity(artifact)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_compatible_matching_provenance(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        self._write(artifact, self._make_manifest(fw_hash="fw1"))
        current = ProvenanceRecord(framework_hash="fw1")
        status = check_cache_validity(artifact, current_provenance=current)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_stale_changed_framework_hash(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        self._write(artifact, self._make_manifest(fw_hash="fw_old"))
        current = ProvenanceRecord(framework_hash="fw_new")
        status = check_cache_validity(artifact, current_provenance=current)
        assert status == ArtifactResolutionStatus.STALE

    def test_must_regenerate_missing_artifact(self, tmp_path):
        artifact = str(tmp_path / "nonexistent.root")
        status = check_cache_validity(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_must_regenerate_missing_sidecar(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        # No sidecar written
        status = check_cache_validity(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_must_regenerate_schema_version_mismatch(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        m = self._make_manifest()
        m.skim.schema_version = 999  # force mismatch
        self._write(artifact, m)
        status = check_cache_validity(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_strict_mode_promotes_stale_to_must_regenerate(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        self._write(artifact, self._make_manifest(fw_hash="fw_old"))
        current = ProvenanceRecord(framework_hash="fw_new")
        status = check_cache_validity(artifact, current_provenance=current, strict=True)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_strict_mode_compatible_stays_compatible(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        self._write(artifact, self._make_manifest(fw_hash="fw1"))
        current = ProvenanceRecord(framework_hash="fw1")
        status = check_cache_validity(artifact, current_provenance=current, strict=True)
        assert status == ArtifactResolutionStatus.COMPATIBLE

    def test_must_regenerate_on_corrupt_sidecar(self, tmp_path):
        artifact = str(tmp_path / "out.root")
        open(artifact, "w").close()
        sidecar = artifact + CACHE_SIDECAR_SUFFIX
        with open(sidecar, "w") as fh:
            fh.write("- not\n- a\n- mapping\n")
        status = check_cache_validity(artifact)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_schema_version_error_causes_must_regenerate(self, tmp_path):
        """Any schema mismatch across multiple schemas → MUST_REGENERATE."""
        artifact = str(tmp_path / "out.root")
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(output_file="meta.root"),
            framework_hash="fw_old",
        )
        m.skim.schema_version = 999  # force mismatch on skim only
        self._write(artifact, m)
        current = ProvenanceRecord(framework_hash="fw_new")
        status = check_cache_validity(artifact, current_provenance=current)
        assert status == ArtifactResolutionStatus.MUST_REGENERATE

    def test_end_to_end_workflow(self, tmp_path):
        """Full workflow: write sidecar, validate under same env, update env."""
        artifact = str(tmp_path / "presel.root")
        m = OutputManifest(
            skim=SkimSchema(output_file="presel.root"),
            framework_hash="fw_v1",
            user_repo_hash="ur_v1",
        )
        self._write(artifact, m)

        # Same environment → COMPATIBLE
        same_env = ProvenanceRecord(framework_hash="fw_v1", user_repo_hash="ur_v1")
        assert check_cache_validity(artifact, same_env) == ArtifactResolutionStatus.COMPATIBLE

        # Updated framework → STALE
        new_fw = ProvenanceRecord(framework_hash="fw_v2", user_repo_hash="ur_v1")
        assert check_cache_validity(artifact, new_fw) == ArtifactResolutionStatus.STALE

        # Strict mode with updated framework → MUST_REGENERATE
        assert check_cache_validity(artifact, new_fw, strict=True) == ArtifactResolutionStatus.MUST_REGENERATE
