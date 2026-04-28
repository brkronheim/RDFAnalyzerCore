"""
Tests for core/python/nuisance_groups.py.

Covers:
- NuisanceGroupType and NuisanceGroupOutputUsage enums
- CoverageIssue construction and to_dict / from_dict round-trip
- NuisanceGroup construction, validation, helpers, to_dict / from_dict
- NuisanceGroupRegistry: add_group, query methods, validate, validate_coverage
- NuisanceGroupRegistry: from_config (groups format and flat systematics format)
- NuisanceGroupRegistry: to_dict / from_dict round-trip, save_yaml / load_yaml
- Integration: NuisanceGroupDefinition in output_schema.OutputManifest
- Integration: NuisanceGroupCoverageEntry in validation_report.ValidationReport
"""
from __future__ import annotations

import os
import sys
import tempfile

import pytest
import yaml

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from nuisance_groups import (
    CoverageIssue,
    CoverageSeverity,
    NuisanceGroup,
    NuisanceGroupOutputUsage,
    NuisanceGroupRegistry,
    NuisanceGroupType,
    VALID_GROUP_TYPES,
    VALID_OUTPUT_USAGES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_group(
    name="jet_energy",
    group_type="shape",
    systematics=None,
    processes=None,
    regions=None,
    output_usage=None,
) -> NuisanceGroup:
    return NuisanceGroup(
        name=name,
        group_type=group_type,
        systematics=systematics if systematics is not None else ["JES", "JER"],
        processes=processes if processes is not None else ["signal", "ttbar"],
        regions=regions if regions is not None else ["signal_region"],
        output_usage=output_usage if output_usage is not None else ["datacard", "histogram"],
    )


# ---------------------------------------------------------------------------
# NuisanceGroupType
# ---------------------------------------------------------------------------


class TestNuisanceGroupType:
    def test_values(self):
        assert NuisanceGroupType.SHAPE.value == "shape"
        assert NuisanceGroupType.RATE.value == "rate"
        assert NuisanceGroupType.NORMALIZATION.value == "normalization"
        assert NuisanceGroupType.OTHER.value == "other"

    def test_valid_group_types_set(self):
        assert "shape" in VALID_GROUP_TYPES
        assert "rate" in VALID_GROUP_TYPES
        assert "normalization" in VALID_GROUP_TYPES
        assert "other" in VALID_GROUP_TYPES


# ---------------------------------------------------------------------------
# NuisanceGroupOutputUsage
# ---------------------------------------------------------------------------


class TestNuisanceGroupOutputUsage:
    def test_values(self):
        assert NuisanceGroupOutputUsage.HISTOGRAM.value == "histogram"
        assert NuisanceGroupOutputUsage.DATACARD.value == "datacard"
        assert NuisanceGroupOutputUsage.PLOT.value == "plot"

    def test_valid_output_usages_set(self):
        assert "histogram" in VALID_OUTPUT_USAGES
        assert "datacard" in VALID_OUTPUT_USAGES
        assert "plot" in VALID_OUTPUT_USAGES


# ---------------------------------------------------------------------------
# CoverageIssue
# ---------------------------------------------------------------------------


class TestCoverageIssue:
    def test_construction(self):
        issue = CoverageIssue(
            severity=CoverageSeverity.ERROR,
            group_name="jet_energy",
            systematic_name="JES",
            message="Missing Up variation.",
        )
        assert issue.severity == CoverageSeverity.ERROR
        assert issue.group_name == "jet_energy"
        assert issue.systematic_name == "JES"
        assert "Missing" in issue.message

    def test_to_dict(self):
        issue = CoverageIssue(
            severity=CoverageSeverity.WARNING,
            group_name="lumi",
            systematic_name="",
            message="No systematics declared.",
        )
        d = issue.to_dict()
        assert d["severity"] == "warning"
        assert d["group_name"] == "lumi"
        assert d["systematic_name"] == ""

    def test_from_dict_round_trip(self):
        issue = CoverageIssue(
            severity=CoverageSeverity.ERROR,
            group_name="pu",
            systematic_name="PU",
            message="Missing Down variation.",
        )
        restored = CoverageIssue.from_dict(issue.to_dict())
        assert restored.severity == issue.severity
        assert restored.group_name == issue.group_name
        assert restored.systematic_name == issue.systematic_name
        assert restored.message == issue.message


# ---------------------------------------------------------------------------
# NuisanceGroup
# ---------------------------------------------------------------------------


class TestNuisanceGroup:
    def test_defaults(self):
        g = NuisanceGroup(name="mygroup")
        assert g.group_type == "shape"
        assert g.systematics == []
        assert g.processes == []
        assert g.regions == []
        assert g.output_usage == []
        assert g.description == ""
        assert g.correlation_group == ""

    def test_custom_construction(self):
        g = _make_group()
        assert g.name == "jet_energy"
        assert g.group_type == "shape"
        assert "JES" in g.systematics
        assert "signal" in g.processes
        assert "signal_region" in g.regions
        assert "datacard" in g.output_usage

    def test_applies_to_process_explicit(self):
        g = _make_group(processes=["signal", "ttbar"])
        assert g.applies_to_process("signal") is True
        assert g.applies_to_process("wjets") is False

    def test_applies_to_process_empty_means_all(self):
        g = _make_group(processes=[])
        assert g.applies_to_process("anything") is True

    def test_applies_to_region_explicit(self):
        g = _make_group(regions=["signal_region"])
        assert g.applies_to_region("signal_region") is True
        assert g.applies_to_region("control_region") is False

    def test_applies_to_region_empty_means_all(self):
        g = _make_group(regions=[])
        assert g.applies_to_region("any_region") is True

    def test_used_for_output_explicit(self):
        g = _make_group(output_usage=["datacard"])
        assert g.used_for_output("datacard") is True
        assert g.used_for_output("plot") is False

    def test_used_for_output_empty_means_all(self):
        g = _make_group(output_usage=[])
        assert g.used_for_output("plot") is True
        assert g.used_for_output("histogram") is True

    def test_validate_valid(self):
        g = _make_group()
        assert g.validate() == []

    def test_validate_empty_name(self):
        g = NuisanceGroup(name="")
        errors = g.validate()
        assert any("name" in e for e in errors)

    def test_validate_invalid_group_type(self):
        g = NuisanceGroup(name="x", group_type="nonsense")
        errors = g.validate()
        assert any("group_type" in e for e in errors)

    def test_validate_invalid_output_usage(self):
        g = NuisanceGroup(name="x", output_usage=["invalid_usage"])
        errors = g.validate()
        assert any("output_usage" in e for e in errors)

    def test_validate_multiple_output_usages(self):
        g = NuisanceGroup(name="x", output_usage=["datacard", "histogram", "plot"])
        assert g.validate() == []

    def test_to_dict_round_trip(self):
        g = NuisanceGroup(
            name="jet_energy",
            group_type="shape",
            systematics=["JES", "JER"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["datacard"],
            description="A test group",
            correlation_group="corr_lumi",
        )
        d = g.to_dict()
        restored = NuisanceGroup.from_dict(d)
        assert restored.name == g.name
        assert restored.group_type == g.group_type
        assert restored.systematics == g.systematics
        assert restored.processes == g.processes
        assert restored.regions == g.regions
        assert restored.output_usage == g.output_usage
        assert restored.description == g.description
        assert restored.correlation_group == g.correlation_group

    def test_from_dict_ignores_unknown_keys(self):
        d = {"name": "g", "group_type": "rate", "unknown_field": "ignored"}
        g = NuisanceGroup.from_dict(d)
        assert g.name == "g"
        assert g.group_type == "rate"


# ---------------------------------------------------------------------------
# NuisanceGroupRegistry
# ---------------------------------------------------------------------------


class TestNuisanceGroupRegistry:
    def test_empty(self):
        reg = NuisanceGroupRegistry()
        assert reg.groups == []

    def test_add_group(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("g1"))
        assert len(reg.groups) == 1

    def test_add_group_duplicate_raises(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("g1"))
        with pytest.raises(ValueError, match="g1"):
            reg.add_group(_make_group("g1"))

    def test_get_groups_for_process(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", processes=["signal", "ttbar"]))
        reg.add_group(_make_group("lumi", processes=[]))
        signal_groups = reg.get_groups_for_process("signal")
        assert len(signal_groups) == 2  # both apply
        wjets_groups = reg.get_groups_for_process("wjets")
        assert len(wjets_groups) == 1  # only lumi (empty processes = all)

    def test_get_groups_for_region(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", regions=["signal_region"]))
        reg.add_group(_make_group("lumi", regions=[]))
        sr = reg.get_groups_for_region("signal_region")
        assert len(sr) == 2
        cr = reg.get_groups_for_region("control_region")
        assert len(cr) == 1  # only lumi (empty regions = all)

    def test_get_groups_for_output(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", output_usage=["datacard", "histogram"]))
        reg.add_group(_make_group("theory", output_usage=["plot"]))
        reg.add_group(_make_group("all_outputs", output_usage=[]))
        dc = reg.get_groups_for_output("datacard")
        assert len(dc) == 2  # jet + all_outputs
        plot = reg.get_groups_for_output("plot")
        assert len(plot) == 2  # theory + all_outputs

    def test_get_groups_by_type(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", group_type="shape"))
        reg.add_group(_make_group("lumi", group_type="rate", systematics=["lumi"]))
        shape = reg.get_groups_by_type("shape")
        assert len(shape) == 1
        rate = reg.get_groups_by_type("rate")
        assert len(rate) == 1

    def test_get_systematics_for_process_and_region(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(
            "jet",
            systematics=["JES", "JER"],
            processes=["signal", "ttbar"],
            regions=["signal_region"],
            output_usage=["datacard"],
        ))
        reg.add_group(_make_group(
            "lumi",
            systematics=["lumi"],
            processes=[],
            regions=[],
            output_usage=["datacard"],
        ))

        result = reg.get_systematics_for_process_and_region(
            "signal", "signal_region", output_usage="datacard"
        )
        assert "JES" in result
        assert "JER" in result
        assert "lumi" in result

    def test_get_systematics_for_process_and_region_filtered(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(
            "jet", systematics=["JES"], processes=["ttbar"], regions=["signal_region"]
        ))
        result = reg.get_systematics_for_process_and_region("signal", "signal_region")
        # jet group does not apply to "signal" process
        assert "JES" not in result

    def test_get_systematics_for_process_and_region_output_usage_filter(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(
            "plot_only",
            systematics=["theo"],
            processes=[],
            regions=[],
            output_usage=["plot"],
        ))
        result = reg.get_systematics_for_process_and_region(
            "signal", "sr", output_usage="datacard"
        )
        assert "theo" not in result

    def test_validate_valid(self):
        reg = NuisanceGroupRegistry(groups=[_make_group()])
        assert reg.validate() == []

    def test_validate_duplicate_names(self):
        # Bypass add_group check by passing directly
        g1 = _make_group("g1")
        g2 = _make_group("g1")
        reg = NuisanceGroupRegistry(groups=[g1, g2])
        errors = reg.validate()
        assert any("duplicate" in e for e in errors)

    def test_validate_propagates_group_errors(self):
        bad = NuisanceGroup(name="", group_type="bad")
        reg = NuisanceGroupRegistry(groups=[bad])
        errors = reg.validate()
        assert len(errors) >= 2  # empty name + bad type

    # ------------------------------------------------------------------
    # validate_coverage
    # ------------------------------------------------------------------

    def test_validate_coverage_all_present(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=["JES", "JER"]))
        avail = {"JES": ["JESUp", "JESDown"], "JER": ["JERUp", "JERDown"]}
        issues = reg.validate_coverage(avail)
        assert issues == []

    def test_validate_coverage_missing_up(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=["JES"]))
        avail = {"JES": ["JESDown"]}
        issues = reg.validate_coverage(avail)
        assert len(issues) == 1
        assert issues[0].severity == CoverageSeverity.ERROR
        assert "Up" in issues[0].message

    def test_validate_coverage_missing_down(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=["JES"]))
        avail = {"JES": ["JESUp"]}
        issues = reg.validate_coverage(avail)
        assert len(issues) == 1
        assert issues[0].severity == CoverageSeverity.ERROR
        assert "Down" in issues[0].message

    def test_validate_coverage_not_found(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=["JES"]))
        avail = {}
        issues = reg.validate_coverage(avail)
        assert len(issues) == 1
        assert issues[0].severity == CoverageSeverity.ERROR
        assert "not present" in issues[0].message

    def test_validate_coverage_empty_systematics_warns(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=[]))
        issues = reg.validate_coverage({})
        assert len(issues) == 1
        assert issues[0].severity == CoverageSeverity.WARNING

    def test_validate_coverage_case_insensitive_suffix(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group(systematics=["JES"]))
        # Variations use mixed case suffixes
        avail = {"JES": ["JESup", "JESDOWN"]}
        issues = reg.validate_coverage(avail)
        assert issues == []

    def test_validate_coverage_multiple_groups(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", systematics=["JES", "JER"]))
        reg.add_group(_make_group("lumi", systematics=["lumi"], processes=[]))
        avail = {
            "JES": ["JESUp", "JESDown"],
            "JER": ["JERUp"],  # missing Down
            "lumi": ["lumiUp", "lumiDown"],
        }
        issues = reg.validate_coverage(avail)
        assert len(issues) == 1
        assert issues[0].systematic_name == "JER"

    # ------------------------------------------------------------------
    # from_config – groups format
    # ------------------------------------------------------------------

    def test_from_config_groups_format(self):
        config = {
            "groups": [
                {
                    "name": "jet_energy",
                    "group_type": "shape",
                    "systematics": ["JES", "JER"],
                    "processes": ["signal"],
                    "regions": ["sr"],
                    "output_usage": ["datacard"],
                }
            ]
        }
        reg = NuisanceGroupRegistry.from_config(config)
        assert len(reg.groups) == 1
        g = reg.groups[0]
        assert g.name == "jet_energy"
        assert "JES" in g.systematics

    def test_from_config_flat_systematics(self):
        config = {
            "systematics": {
                "lumi": {
                    "type": "rate",
                    "applies_to": {"signal": True, "ttbar": True, "wjets": False},
                    "regions": ["sr"],
                    "description": "Luminosity",
                },
                "JES": {
                    "type": "shape",
                    "applies_to": {"signal": True},
                    "regions": [],
                },
            }
        }
        reg = NuisanceGroupRegistry.from_config(config)
        assert len(reg.groups) == 2
        names = {g.name for g in reg.groups}
        assert "lumi" in names
        assert "JES" in names

        lumi = next(g for g in reg.groups if g.name == "lumi")
        assert lumi.group_type == "rate"
        assert "signal" in lumi.processes
        assert "ttbar" in lumi.processes
        assert "wjets" not in lumi.processes
        assert lumi.description == "Luminosity"

    def test_from_config_empty(self):
        reg = NuisanceGroupRegistry.from_config({})
        assert reg.groups == []

    # ------------------------------------------------------------------
    # to_dict / from_dict round-trip
    # ------------------------------------------------------------------

    def test_to_dict_from_dict_round_trip(self):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", systematics=["JES"], processes=["signal"]))
        reg.add_group(_make_group("lumi", group_type="rate", systematics=["lumi"], processes=[]))
        d = reg.to_dict()
        restored = NuisanceGroupRegistry.from_dict(d)
        assert len(restored.groups) == 2
        names = {g.name for g in restored.groups}
        assert "jet" in names
        assert "lumi" in names

    def test_to_dict_structure(self):
        reg = NuisanceGroupRegistry(groups=[_make_group()])
        d = reg.to_dict()
        assert "groups" in d
        assert isinstance(d["groups"], list)
        assert d["groups"][0]["name"] == "jet_energy"

    # ------------------------------------------------------------------
    # save_yaml / load_yaml
    # ------------------------------------------------------------------

    def test_save_load_yaml_round_trip(self, tmp_path):
        reg = NuisanceGroupRegistry()
        reg.add_group(_make_group("jet", systematics=["JES", "JER"]))
        path = str(tmp_path / "nuisance_groups.yaml")
        reg.save_yaml(path)

        loaded = NuisanceGroupRegistry.load_yaml(path)
        assert len(loaded.groups) == 1
        g = loaded.groups[0]
        assert g.name == "jet"
        assert "JES" in g.systematics
        assert "JER" in g.systematics

    def test_load_yaml_empty_file(self, tmp_path):
        path = str(tmp_path / "empty.yaml")
        with open(path, "w") as fh:
            fh.write("")
        reg = NuisanceGroupRegistry.load_yaml(path)
        assert reg.groups == []


# ---------------------------------------------------------------------------
# Integration – NuisanceGroupDefinition in output_schema.OutputManifest
# ---------------------------------------------------------------------------


class TestNuisanceGroupDefinitionInOutputManifest:
    def test_import(self):
        from output_schema import (
            NuisanceGroupDefinition,
            NUISANCE_GROUP_DEFINITION_VERSION,
            NUISANCE_GROUP_TYPES,
            NUISANCE_GROUP_OUTPUT_USAGES,
        )
        assert NUISANCE_GROUP_DEFINITION_VERSION == 1
        assert "shape" in NUISANCE_GROUP_TYPES
        assert "datacard" in NUISANCE_GROUP_OUTPUT_USAGES

    def test_nuisance_group_definition_defaults(self):
        from output_schema import NuisanceGroupDefinition, NUISANCE_GROUP_DEFINITION_VERSION
        ng = NuisanceGroupDefinition()
        assert ng.schema_version == NUISANCE_GROUP_DEFINITION_VERSION
        assert ng.name == ""
        assert ng.systematics == []
        assert ng.processes == []

    def test_nuisance_group_definition_validate_valid(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="jet", group_type="shape", systematics=["JES"])
        assert ng.validate() == []

    def test_nuisance_group_definition_validate_empty_name(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="", group_type="shape")
        errors = ng.validate()
        assert any("name" in e for e in errors)

    def test_nuisance_group_definition_validate_bad_type(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="g", group_type="unknown_type")
        errors = ng.validate()
        assert any("group_type" in e for e in errors)

    def test_nuisance_group_definition_validate_bad_output_usage(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="g", group_type="shape", output_usage=["bad"])
        errors = ng.validate()
        assert any("output_usage" in e for e in errors)

    def test_nuisance_group_definition_validate_version_mismatch(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="g", group_type="shape", schema_version=999)
        errors = ng.validate()
        assert any("version" in e for e in errors)

    def test_nuisance_group_definition_to_dict_round_trip(self):
        from output_schema import NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(
            name="jet", group_type="shape",
            systematics=["JES", "JER"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["datacard"],
            description="Jet uncertainties",
        )
        d = ng.to_dict()
        restored = NuisanceGroupDefinition.from_dict(d)
        assert restored.name == ng.name
        assert restored.systematics == ng.systematics
        assert restored.processes == ng.processes

    def test_output_manifest_carries_nuisance_groups(self):
        from output_schema import OutputManifest, NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="jet", group_type="shape", systematics=["JES"])
        m = OutputManifest(nuisance_groups=[ng])
        assert len(m.nuisance_groups) == 1
        assert m.nuisance_groups[0].name == "jet"

    def test_output_manifest_to_dict_round_trip_with_nuisance_groups(self):
        from output_schema import OutputManifest, NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(name="jet", group_type="shape", systematics=["JES"])
        m = OutputManifest(nuisance_groups=[ng])
        d = m.to_dict()
        assert "nuisance_groups" in d
        assert len(d["nuisance_groups"]) == 1

        restored = OutputManifest.from_dict(d)
        assert len(restored.nuisance_groups) == 1
        assert restored.nuisance_groups[0].name == "jet"

    def test_output_manifest_save_load_yaml_with_nuisance_groups(self, tmp_path):
        from output_schema import OutputManifest, NuisanceGroupDefinition
        ng = NuisanceGroupDefinition(
            name="lumi",
            group_type="rate",
            systematics=["lumi"],
            output_usage=["datacard"],
        )
        m = OutputManifest(nuisance_groups=[ng])
        path = str(tmp_path / "manifest.yaml")
        m.save_yaml(path)

        loaded = OutputManifest.load_yaml(path)
        assert len(loaded.nuisance_groups) == 1
        assert loaded.nuisance_groups[0].group_type == "rate"

    def test_output_manifest_validate_duplicate_group_names(self):
        from output_schema import OutputManifest, NuisanceGroupDefinition
        ng1 = NuisanceGroupDefinition(name="jet", group_type="shape")
        ng2 = NuisanceGroupDefinition(name="jet", group_type="rate")
        m = OutputManifest(nuisance_groups=[ng1, ng2])
        errors = m.validate()
        assert any("duplicate" in e for e in errors)

    def test_output_manifest_check_version_compatibility_nuisance_groups(self):
        from output_schema import (
            OutputManifest, NuisanceGroupDefinition, SchemaVersionError
        )
        ng = NuisanceGroupDefinition(name="jet", group_type="shape", schema_version=999)
        m = OutputManifest(nuisance_groups=[ng])
        with pytest.raises(SchemaVersionError):
            OutputManifest.check_version_compatibility(m)

    def test_validate_nuisance_coverage_function(self):
        from output_schema import NuisanceGroupDefinition, validate_nuisance_coverage
        groups = [
            NuisanceGroupDefinition(name="jet", group_type="shape", systematics=["JES", "JER"]),
        ]
        # All present
        avail = {"JES": ["JESUp", "JESDown"], "JER": ["JERUp", "JERDown"]}
        assert validate_nuisance_coverage(groups, avail) == []

        # Missing Up for JER
        avail_incomplete = {"JES": ["JESUp", "JESDown"], "JER": ["JERDown"]}
        errors = validate_nuisance_coverage(groups, avail_incomplete)
        assert len(errors) == 1
        assert "JER" in errors[0]
        assert "Up" in errors[0]

        # JES not present at all
        avail_missing = {"JER": ["JERUp", "JERDown"]}
        errors2 = validate_nuisance_coverage(groups, avail_missing)
        assert any("JES" in e for e in errors2)

        # Empty systematics
        empty_groups = [NuisanceGroupDefinition(name="empty", group_type="shape")]
        errors3 = validate_nuisance_coverage(empty_groups, {})
        assert len(errors3) == 1


# ---------------------------------------------------------------------------
# Integration – NuisanceGroupCoverageEntry in validation_report.ValidationReport
# ---------------------------------------------------------------------------


class TestNuisanceGroupCoverageEntryInValidationReport:
    def test_import(self):
        from validation_report import NuisanceGroupCoverageEntry
        e = NuisanceGroupCoverageEntry(
            group_name="jet",
            group_type="shape",
            systematics=["JES", "JER"],
        )
        assert e.group_name == "jet"
        assert e.is_complete is True

    def test_is_complete_false_when_missing(self):
        from validation_report import NuisanceGroupCoverageEntry
        e = NuisanceGroupCoverageEntry(
            group_name="jet",
            group_type="shape",
            systematics=["JES"],
            missing_up=["JES"],
        )
        assert e.is_complete is False

    def test_add_nuisance_group_coverage(self):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(group_name="jet", group_type="shape")
        )
        assert len(r.nuisance_group_coverage) == 1

    def test_has_errors_from_incomplete_coverage(self):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jet",
                group_type="shape",
                systematics=["JES"],
                missing_up=["JES"],
            )
        )
        assert r.has_errors is True

    def test_to_dict_includes_nuisance_group_coverage(self):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(group_name="jet", group_type="shape")
        )
        d = r.to_dict()
        assert "nuisance_group_coverage" in d
        assert d["nuisance_group_coverage"][0]["group_name"] == "jet"
        assert d["summary"]["n_nuisance_group_coverage"] == 1

    def test_from_dict_restores_nuisance_group_coverage(self):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="lumi",
                group_type="rate",
                systematics=["lumi"],
                missing_down=["lumi"],
            )
        )
        restored = ValidationReport.from_dict(r.to_dict())
        assert len(restored.nuisance_group_coverage) == 1
        e = restored.nuisance_group_coverage[0]
        assert e.group_name == "lumi"
        assert e.missing_down == ["lumi"]
        assert e.is_complete is False

    def test_to_text_includes_nuisance_group_coverage(self):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jet",
                group_type="shape",
                systematics=["JES"],
                missing_up=["JES"],
            )
        )
        text = r.to_text()
        assert "NUISANCE GROUP COVERAGE" in text
        assert "jet" in text
        assert "JES" in text

    def test_save_load_yaml_round_trip(self, tmp_path):
        from validation_report import NuisanceGroupCoverageEntry, ValidationReport
        r = ValidationReport(stage="test")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jet",
                group_type="shape",
                systematics=["JES", "JER"],
                processes=["signal"],
                regions=["sr"],
                output_usage=["datacard"],
            )
        )
        path = str(tmp_path / "report.yaml")
        r.save_yaml(path)
        loaded = ValidationReport.load_yaml(path)
        assert len(loaded.nuisance_group_coverage) == 1
        e = loaded.nuisance_group_coverage[0]
        assert e.group_name == "jet"
        assert e.processes == ["signal"]
