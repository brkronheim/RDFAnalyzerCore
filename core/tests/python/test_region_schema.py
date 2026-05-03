"""
Tests for the declarative region management system.

Covers:
  - RegionDefinition dataclass (construction, validation, serialisation)
  - validate_region_hierarchy() (missing parent, cycles, duplicates)
  - OutputManifest.regions field (round-trip, validate, check_version_compatibility)
  - RegionEntry dataclass (construction, serialisation)
  - ValidationReport.regions field and add_region() (round-trip, has_errors, to_text)
"""

import sys
import os

import pytest

# Allow direct execution from the python directory without installing packages.
sys.path.insert(0, os.path.dirname(__file__))

from output_schema import (
    OutputManifest,
    RegionDefinition,
    SkimSchema,
    REGION_DEFINITION_VERSION,
    SCHEMA_REGISTRY,
    SchemaVersionError,
    validate_region_hierarchy,
)
from validation_report import (
    ValidationReport,
    RegionEntry,
)


# ---------------------------------------------------------------------------
# RegionDefinition – construction and defaults
# ---------------------------------------------------------------------------


class TestRegionDefinitionDefaults:
    def test_schema_version_default(self):
        r = RegionDefinition()
        assert r.schema_version == REGION_DEFINITION_VERSION

    def test_name_default_empty(self):
        assert RegionDefinition().name == ""

    def test_filter_column_default_empty(self):
        assert RegionDefinition().filter_column == ""

    def test_parent_default_empty(self):
        assert RegionDefinition().parent == ""

    def test_description_default_empty(self):
        assert RegionDefinition().description == ""


class TestRegionDefinitionConstruction:
    def test_full_construction(self):
        r = RegionDefinition(
            name="signal",
            filter_column="pass_signal",
            parent="presel",
            description="Signal region",
        )
        assert r.name == "signal"
        assert r.filter_column == "pass_signal"
        assert r.parent == "presel"
        assert r.description == "Signal region"

    def test_root_region_no_parent(self):
        r = RegionDefinition(name="presel", filter_column="pass_presel")
        assert r.parent == ""


# ---------------------------------------------------------------------------
# RegionDefinition – validation
# ---------------------------------------------------------------------------


class TestRegionDefinitionValidation:
    def test_valid_root_region(self):
        r = RegionDefinition(name="presel", filter_column="pass_presel")
        assert r.validate() == []

    def test_valid_child_region(self):
        r = RegionDefinition(
            name="signal", filter_column="pass_signal", parent="presel"
        )
        assert r.validate() == []

    def test_empty_name_fails(self):
        r = RegionDefinition(filter_column="pass_x")
        errors = r.validate()
        assert any("name" in e.lower() for e in errors)

    def test_empty_filter_column_fails(self):
        r = RegionDefinition(name="signal")
        errors = r.validate()
        assert any("filter_column" in e.lower() for e in errors)

    def test_version_mismatch_fails(self):
        r = RegionDefinition(name="x", filter_column="pass_x", schema_version=999)
        errors = r.validate()
        assert any("version" in e.lower() for e in errors)


# ---------------------------------------------------------------------------
# RegionDefinition – serialisation round-trip
# ---------------------------------------------------------------------------


class TestRegionDefinitionSerialization:
    def test_to_dict_contains_all_fields(self):
        r = RegionDefinition(
            name="signal",
            filter_column="pass_signal",
            parent="presel",
            description="tight signal",
        )
        d = r.to_dict()
        assert d["name"] == "signal"
        assert d["filter_column"] == "pass_signal"
        assert d["parent"] == "presel"
        assert d["description"] == "tight signal"
        assert d["schema_version"] == REGION_DEFINITION_VERSION

    def test_from_dict_round_trip(self):
        original = RegionDefinition(
            name="control",
            filter_column="pass_control",
            parent="",
            description="control region",
        )
        restored = RegionDefinition.from_dict(original.to_dict())
        assert restored.name == original.name
        assert restored.filter_column == original.filter_column
        assert restored.parent == original.parent
        assert restored.description == original.description
        assert restored.schema_version == original.schema_version

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "name": "x",
            "filter_column": "pass_x",
            "unknown_future_field": "value",
        }
        r = RegionDefinition.from_dict(d)
        assert r.name == "x"
        assert r.filter_column == "pass_x"


# ---------------------------------------------------------------------------
# validate_region_hierarchy
# ---------------------------------------------------------------------------


class TestValidateRegionHierarchy:
    def test_empty_list_is_valid(self):
        assert validate_region_hierarchy([]) == []

    def test_single_root_region_valid(self):
        regions = [RegionDefinition(name="presel", filter_column="pass_presel")]
        assert validate_region_hierarchy(regions) == []

    def test_valid_two_level_hierarchy(self):
        regions = [
            RegionDefinition(name="presel", filter_column="pass_presel"),
            RegionDefinition(
                name="signal", filter_column="pass_signal", parent="presel"
            ),
        ]
        assert validate_region_hierarchy(regions) == []

    def test_valid_sibling_regions(self):
        regions = [
            RegionDefinition(name="presel",   filter_column="pass_presel"),
            RegionDefinition(name="signal",   filter_column="pass_signal",   parent="presel"),
            RegionDefinition(name="control",  filter_column="pass_control",  parent="presel"),
            RegionDefinition(name="sideband", filter_column="pass_sideband", parent="presel"),
        ]
        assert validate_region_hierarchy(regions) == []

    def test_missing_parent_is_error(self):
        regions = [
            RegionDefinition(
                name="signal",
                filter_column="pass_signal",
                parent="nonexistent_parent",
            )
        ]
        errors = validate_region_hierarchy(regions)
        assert any("nonexistent_parent" in e for e in errors)

    def test_duplicate_name_is_error(self):
        regions = [
            RegionDefinition(name="signal", filter_column="pass_signal"),
            RegionDefinition(name="signal", filter_column="pass_signal_v2"),
        ]
        errors = validate_region_hierarchy(regions)
        assert any("duplicate" in e.lower() or "signal" in e for e in errors)

    def test_empty_name_from_validate_is_reported(self):
        regions = [RegionDefinition(filter_column="pass_x")]
        errors = validate_region_hierarchy(regions)
        assert len(errors) > 0

    def test_three_level_hierarchy_valid(self):
        regions = [
            RegionDefinition(name="l1", filter_column="pass_l1"),
            RegionDefinition(name="l2", filter_column="pass_l2", parent="l1"),
            RegionDefinition(name="l3", filter_column="pass_l3", parent="l2"),
        ]
        assert validate_region_hierarchy(regions) == []


# ---------------------------------------------------------------------------
# SCHEMA_REGISTRY includes region_definition
# ---------------------------------------------------------------------------


class TestSchemaRegistryHasRegionDefinition:
    def test_region_definition_in_registry(self):
        assert "region_definition" in SCHEMA_REGISTRY

    def test_region_definition_version_matches(self):
        assert SCHEMA_REGISTRY["region_definition"] == REGION_DEFINITION_VERSION


# ---------------------------------------------------------------------------
# OutputManifest – regions field
# ---------------------------------------------------------------------------


def _minimal_manifest(**kwargs):
    """Return a minimal valid manifest with at least one schema set."""
    defaults = {"skim": SkimSchema(output_file="out.root", tree_name="Events")}
    defaults.update(kwargs)
    return OutputManifest(**defaults)


class TestOutputManifestRegions:
    def test_default_regions_empty(self):
        m = _minimal_manifest()
        assert m.regions == []

    def test_regions_stored(self):
        regions = [
            RegionDefinition(name="presel", filter_column="pass_presel"),
            RegionDefinition(name="signal", filter_column="pass_signal", parent="presel"),
        ]
        m = _minimal_manifest(regions=regions)
        assert len(m.regions) == 2
        assert m.regions[0].name == "presel"
        assert m.regions[1].name == "signal"

    def test_to_dict_includes_regions(self):
        regions = [RegionDefinition(name="presel", filter_column="pass_presel")]
        m = _minimal_manifest(regions=regions)
        d = m.to_dict()
        assert "regions" in d
        assert len(d["regions"]) == 1
        assert d["regions"][0]["name"] == "presel"

    def test_from_dict_round_trip_with_regions(self):
        regions = [
            RegionDefinition(name="presel",  filter_column="pass_presel"),
            RegionDefinition(name="signal",  filter_column="pass_signal",  parent="presel"),
            RegionDefinition(name="control", filter_column="pass_control", parent="presel"),
        ]
        original = _minimal_manifest(regions=regions)
        restored = OutputManifest.from_dict(original.to_dict())
        assert len(restored.regions) == 3
        assert restored.regions[1].parent == "presel"

    def test_from_dict_no_regions_key_yields_empty(self):
        d = _minimal_manifest().to_dict()
        d.pop("regions", None)
        m = OutputManifest.from_dict(d)
        assert m.regions == []

    def test_validate_valid_hierarchy(self):
        regions = [
            RegionDefinition(name="presel",  filter_column="pass_presel"),
            RegionDefinition(name="signal",  filter_column="pass_signal",  parent="presel"),
        ]
        m = _minimal_manifest(regions=regions)
        assert m.validate() == []

    def test_validate_reports_missing_parent(self):
        regions = [
            RegionDefinition(name="signal", filter_column="pass_signal", parent="missing"),
        ]
        m = _minimal_manifest(regions=regions)
        errors = m.validate()
        assert any("missing" in e for e in errors)

    def test_validate_reports_empty_filter_column(self):
        regions = [RegionDefinition(name="x")]
        m = _minimal_manifest(regions=regions)
        errors = m.validate()
        assert any("filter_column" in e.lower() for e in errors)

    def test_repr_includes_region_count(self):
        regions = [RegionDefinition(name="r", filter_column="pass_r")]
        m = _minimal_manifest(regions=regions)
        assert "regions=1" in repr(m)

    def test_check_version_compatibility_passes_for_valid_regions(self):
        regions = [RegionDefinition(name="r", filter_column="pass_r")]
        m = _minimal_manifest(regions=regions)
        # Should not raise.
        OutputManifest.check_version_compatibility(m)

    def test_check_version_compatibility_raises_on_version_mismatch(self):
        regions = [RegionDefinition(name="r", filter_column="pass_r", schema_version=999)]
        m = _minimal_manifest(regions=regions)
        with pytest.raises(SchemaVersionError):
            OutputManifest.check_version_compatibility(m)


# ---------------------------------------------------------------------------
# RegionEntry – construction and serialisation
# ---------------------------------------------------------------------------


class TestRegionEntry:
    def test_valid_construction(self):
        e = RegionEntry(
            region_name="signal",
            filter_column="pass_signal",
            parent="presel",
            is_valid=True,
        )
        assert e.region_name == "signal"
        assert e.filter_column == "pass_signal"
        assert e.parent == "presel"
        assert e.is_valid is True
        assert e.issues == []

    def test_invalid_entry_with_issues(self):
        e = RegionEntry(
            region_name="bad",
            filter_column="",
            is_valid=False,
            issues=["filter_column must not be empty"],
        )
        assert not e.is_valid
        assert len(e.issues) == 1

    def test_root_region_default_parent(self):
        e = RegionEntry(region_name="presel", filter_column="pass_presel")
        assert e.parent == ""


# ---------------------------------------------------------------------------
# ValidationReport – regions field
# ---------------------------------------------------------------------------


class TestValidationReportRegions:
    def test_default_regions_empty(self):
        report = ValidationReport(stage="test")
        assert report.regions == []

    def test_add_region(self):
        report = ValidationReport(stage="test")
        entry = RegionEntry(region_name="signal", filter_column="pass_signal")
        report.add_region(entry)
        assert len(report.regions) == 1
        assert report.regions[0].region_name == "signal"

    def test_has_errors_false_when_all_regions_valid(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="r", filter_column="pass_r", is_valid=True)
        )
        assert not report.has_errors

    def test_has_errors_true_when_region_invalid(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="r", filter_column="", is_valid=False)
        )
        assert report.has_errors

    def test_to_dict_includes_regions(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="sig", filter_column="pass_sig", parent="pre")
        )
        d = report.to_dict()
        assert "regions" in d
        assert len(d["regions"]) == 1
        assert d["regions"][0]["region_name"] == "sig"
        assert d["summary"]["n_regions"] == 1

    def test_from_dict_round_trip(self):
        report = ValidationReport(stage="skim")
        report.add_region(
            RegionEntry(
                region_name="control",
                filter_column="pass_control",
                parent="presel",
                is_valid=True,
            )
        )
        report.add_region(
            RegionEntry(
                region_name="bad_region",
                filter_column="",
                is_valid=False,
                issues=["filter_column is empty"],
            )
        )
        restored = ValidationReport.from_dict(report.to_dict())
        assert len(restored.regions) == 2
        assert restored.regions[0].region_name == "control"
        assert restored.regions[0].parent == "presel"
        assert restored.regions[1].is_valid is False
        assert "filter_column is empty" in restored.regions[1].issues

    def test_from_dict_missing_regions_key(self):
        report = ValidationReport(stage="test")
        d = report.to_dict()
        d.pop("regions", None)
        restored = ValidationReport.from_dict(d)
        assert restored.regions == []

    def test_to_text_includes_region_section(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(
                region_name="signal",
                filter_column="pass_signal",
                parent="presel",
                is_valid=True,
            )
        )
        text = report.to_text()
        assert "REGION DEFINITIONS" in text
        assert "signal" in text
        assert "pass_signal" in text
        assert "presel" in text

    def test_to_text_shows_root_region(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="presel", filter_column="pass_presel")
        )
        text = report.to_text()
        assert "(root)" in text

    def test_to_text_shows_fail_for_invalid_region(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(
                region_name="bad",
                filter_column="",
                is_valid=False,
                issues=["filter_column is empty"],
            )
        )
        text = report.to_text()
        assert "FAIL" in text
        assert "filter_column is empty" in text

    def test_yaml_round_trip(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="r", filter_column="pass_r", is_valid=True)
        )
        import yaml
        raw = yaml.safe_load(report.to_yaml())
        restored = ValidationReport.from_dict(raw)
        assert len(restored.regions) == 1
        assert restored.regions[0].region_name == "r"

    def test_json_round_trip(self):
        report = ValidationReport(stage="test")
        report.add_region(
            RegionEntry(region_name="ctrl", filter_column="pass_ctrl", parent="pre")
        )
        import json
        raw = json.loads(report.to_json())
        restored = ValidationReport.from_dict(raw)
        assert restored.regions[0].parent == "pre"
