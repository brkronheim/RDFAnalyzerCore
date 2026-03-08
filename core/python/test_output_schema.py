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
    CutflowSchema,
    HistogramAxisSpec,
    HistogramSchema,
    LawArtifactSchema,
    MetadataSchema,
    OutputManifest,
    SchemaVersionError,
    SkimSchema,
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
