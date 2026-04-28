"""
Tests for core/python/variation_orchestrator.py.

Covers:
- MissingSeverity enum
- WeightVariationSpec construction and helper methods
- SystematicVariationSpec construction and helper methods
- VariationPlan construction, all_required_columns, to_dict
- MissingVariationReport construction and to_dict
- VariationOrchestrator construction, properties, add_weight_variation
- VariationOrchestrator.get_variation_plan – correct filtering by process/region/usage
- VariationOrchestrator.validate_coverage – complete, missing up, missing down, not found
- VariationOrchestrator.build_validation_report – ValidationReport integration
- VariationOrchestrator._infer_processes / _infer_regions edge cases
- Round-trip: VariationPlan.to_dict
"""

from __future__ import annotations

import os
import sys

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry
from validation_report import ValidationReport
from variation_orchestrator import (
    MissingSeverity,
    MissingVariationReport,
    SystematicVariationSpec,
    VariationOrchestrator,
    VariationPlan,
    WeightVariationSpec,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_registry(*groups: NuisanceGroup) -> NuisanceGroupRegistry:
    return NuisanceGroupRegistry(groups=list(groups))


def _make_group(
    name="jes",
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
        output_usage=output_usage if output_usage is not None else ["histogram", "datacard"],
    )


# ---------------------------------------------------------------------------
# MissingSeverity
# ---------------------------------------------------------------------------


class TestMissingSeverity:
    def test_values(self):
        assert MissingSeverity.WARN.value == "warn"
        assert MissingSeverity.ERROR.value == "error"

    def test_from_string(self):
        assert MissingSeverity("warn") is MissingSeverity.WARN
        assert MissingSeverity("error") is MissingSeverity.ERROR

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            MissingSeverity("critical")


# ---------------------------------------------------------------------------
# WeightVariationSpec
# ---------------------------------------------------------------------------


class TestWeightVariationSpec:
    def test_construction(self):
        spec = WeightVariationSpec(
            name="btag",
            nominal_column="w_nom",
            up_column="w_btagUp",
            down_column="w_btagDown",
        )
        assert spec.name == "btag"
        assert spec.nominal_column == "w_nom"
        assert spec.up_column == "w_btagUp"
        assert spec.down_column == "w_btagDown"
        assert spec.processes == []
        assert spec.regions == []
        assert spec.output_usage == []

    def test_applies_to_process_wildcard(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown")
        assert spec.applies_to_process("signal") is True
        assert spec.applies_to_process("anything") is True

    def test_applies_to_process_specific(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown", processes=["signal"])
        assert spec.applies_to_process("signal") is True
        assert spec.applies_to_process("ttbar") is False

    def test_applies_to_region_wildcard(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown")
        assert spec.applies_to_region("signal_region") is True

    def test_applies_to_region_specific(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown", regions=["control"])
        assert spec.applies_to_region("signal_region") is False
        assert spec.applies_to_region("control") is True

    def test_used_for_output_wildcard(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown")
        assert spec.used_for_output("histogram") is True
        assert spec.used_for_output("datacard") is True

    def test_used_for_output_specific(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown", output_usage=["histogram"])
        assert spec.used_for_output("histogram") is True
        assert spec.used_for_output("datacard") is False

    def test_required_columns(self):
        spec = WeightVariationSpec("x", "w", "wUp", "wDown")
        assert spec.required_columns() == ["wUp", "wDown"]

    def test_required_columns_empty(self):
        spec = WeightVariationSpec("x", "w", "", "")
        assert spec.required_columns() == []


# ---------------------------------------------------------------------------
# SystematicVariationSpec
# ---------------------------------------------------------------------------


class TestSystematicVariationSpec:
    def test_construction(self):
        sv = SystematicVariationSpec(
            base_name="JES",
            up_column="JESUp",
            down_column="JESDown",
            group_name="jet_energy",
            group_type="shape",
        )
        assert sv.base_name == "JES"
        assert sv.up_column == "JESUp"
        assert sv.down_column == "JESDown"
        assert sv.group_name == "jet_energy"
        assert sv.group_type == "shape"

    def test_required_columns(self):
        sv = SystematicVariationSpec("JES", "JESUp", "JESDown", "g")
        assert sv.required_columns() == ["JESUp", "JESDown"]


# ---------------------------------------------------------------------------
# VariationPlan
# ---------------------------------------------------------------------------


class TestVariationPlan:
    def _make_plan(self, with_weight=False) -> VariationPlan:
        svs = [
            SystematicVariationSpec("JES", "JESUp", "JESDown", "jes_group", "shape"),
            SystematicVariationSpec("JER", "JERUp", "JERDown", "jer_group", "shape"),
        ]
        wvs = (
            [WeightVariationSpec("btag", "w_nom", "w_btagUp", "w_btagDown")]
            if with_weight
            else []
        )
        return VariationPlan(
            process="signal",
            region="sr",
            output_usage="histogram",
            nominal_weight="w_nom",
            systematic_variations=svs,
            weight_variations=wvs,
        )

    def test_systematic_variation_names(self):
        plan = self._make_plan()
        assert plan.systematic_variation_names() == ["JES", "JER"]

    def test_weight_variation_names(self):
        plan = self._make_plan(with_weight=True)
        assert plan.weight_variation_names() == ["btag"]

    def test_all_required_columns_no_weight_vars(self):
        plan = self._make_plan()
        cols = plan.all_required_columns()
        assert "w_nom" in cols
        assert "JESUp" in cols
        assert "JESDown" in cols
        assert "JERUp" in cols
        assert "JERDown" in cols
        # No duplicates
        assert len(cols) == len(set(cols))

    def test_all_required_columns_with_weight_vars(self):
        plan = self._make_plan(with_weight=True)
        cols = plan.all_required_columns()
        assert "w_btagUp" in cols
        assert "w_btagDown" in cols

    def test_all_required_columns_deduplicates(self):
        # w_nom appears in both nominal_weight and weight variation's nominal_column
        plan = self._make_plan(with_weight=True)
        cols = plan.all_required_columns()
        assert cols.count("w_nom") == 1

    def test_all_required_columns_empty_weight(self):
        plan = VariationPlan("p", "r", "u", nominal_weight="")
        cols = plan.all_required_columns()
        assert cols == []

    def test_to_dict(self):
        plan = self._make_plan(with_weight=True)
        d = plan.to_dict()
        assert d["process"] == "signal"
        assert d["region"] == "sr"
        assert d["output_usage"] == "histogram"
        assert d["nominal_weight"] == "w_nom"
        assert len(d["systematic_variations"]) == 2
        assert len(d["weight_variations"]) == 1
        sv0 = d["systematic_variations"][0]
        assert sv0["base_name"] == "JES"
        assert sv0["up_column"] == "JESUp"


# ---------------------------------------------------------------------------
# MissingVariationReport
# ---------------------------------------------------------------------------


class TestMissingVariationReport:
    def test_construction(self):
        r = MissingVariationReport(
            process="signal",
            region="sr",
            group_name="jes",
            variable="JES",
            missing_up=True,
            missing_down=False,
            not_found=False,
            severity="error",
        )
        assert r.process == "signal"
        assert r.missing_up is True

    def test_to_dict(self):
        r = MissingVariationReport("sig", "sr", "jes", "JES", True, False, False, "warn")
        d = r.to_dict()
        assert d["process"] == "sig"
        assert d["missing_up"] is True
        assert d["severity"] == "warn"


# ---------------------------------------------------------------------------
# VariationOrchestrator construction
# ---------------------------------------------------------------------------


class TestVariationOrchestratorConstruction:
    def test_minimal(self):
        reg = _make_registry()
        orch = VariationOrchestrator(nuisance_registry=reg)
        assert orch.nominal_weight == ""
        assert orch.weight_variations == []
        assert orch.missing_severity == MissingSeverity.ERROR

    def test_with_options(self):
        reg = _make_registry()
        wv = WeightVariationSpec("btag", "w", "wUp", "wDown")
        orch = VariationOrchestrator(
            nuisance_registry=reg,
            weight_variations=[wv],
            nominal_weight="w_nom",
            missing_severity="warn",
        )
        assert orch.nominal_weight == "w_nom"
        assert len(orch.weight_variations) == 1
        assert orch.missing_severity == MissingSeverity.WARN

    def test_missing_severity_enum(self):
        reg = _make_registry()
        orch = VariationOrchestrator(reg, missing_severity=MissingSeverity.WARN)
        assert orch.missing_severity == MissingSeverity.WARN

    def test_add_weight_variation(self):
        reg = _make_registry()
        orch = VariationOrchestrator(reg)
        assert len(orch.weight_variations) == 0
        orch.add_weight_variation(WeightVariationSpec("btag", "w", "wUp", "wDown"))
        assert len(orch.weight_variations) == 1


# ---------------------------------------------------------------------------
# VariationOrchestrator.get_variation_plan
# ---------------------------------------------------------------------------


class TestGetVariationPlan:
    def _make_orch(self) -> VariationOrchestrator:
        group1 = _make_group(
            name="jet_energy",
            systematics=["JES", "JER"],
            processes=["signal", "ttbar"],
            regions=["signal_region"],
            output_usage=["histogram"],
        )
        group2 = _make_group(
            name="lumi",
            group_type="rate",
            systematics=["lumi"],
            processes=[],  # all processes
            regions=[],    # all regions
            output_usage=["datacard"],
        )
        reg = _make_registry(group1, group2)
        wv = WeightVariationSpec(
            "btag", "w_nom", "w_btagUp", "w_btagDown",
            processes=["signal"],
            regions=["signal_region"],
            output_usage=["histogram"],
        )
        return VariationOrchestrator(
            nuisance_registry=reg,
            weight_variations=[wv],
            nominal_weight="w_nom",
        )

    def test_basic_plan(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "signal_region", "histogram")
        assert plan.process == "signal"
        assert plan.region == "signal_region"
        assert plan.output_usage == "histogram"
        assert plan.nominal_weight == "w_nom"

    def test_systematic_variations_filtered_by_process(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("qcd", "signal_region", "histogram")
        # jet_energy only applies to signal/ttbar, not qcd
        names = plan.systematic_variation_names()
        assert "JES" not in names
        assert "JER" not in names

    def test_systematic_variations_included_for_matching_process(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "signal_region", "histogram")
        names = plan.systematic_variation_names()
        assert "JES" in names
        assert "JER" in names

    def test_systematic_variations_filtered_by_region(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "control_region", "histogram")
        # jet_energy only applies to signal_region
        names = plan.systematic_variation_names()
        assert "JES" not in names

    def test_systematic_variations_filtered_by_output_usage(self):
        orch = self._make_orch()
        # jet_energy only applies to histogram; lumi only to datacard
        plan_hist = orch.get_variation_plan("signal", "signal_region", "histogram")
        plan_dc = orch.get_variation_plan("signal", "signal_region", "datacard")
        hist_names = plan_hist.systematic_variation_names()
        dc_names = plan_dc.systematic_variation_names()
        assert "JES" in hist_names
        assert "lumi" not in hist_names
        assert "lumi" in dc_names
        assert "JES" not in dc_names

    def test_wildcard_process_group(self):
        orch = self._make_orch()
        # lumi applies to all processes for datacard
        plan = orch.get_variation_plan("qcd", "anywhere", "datacard")
        assert "lumi" in plan.systematic_variation_names()

    def test_column_names_convention(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "signal_region", "histogram")
        sv = next(sv for sv in plan.systematic_variations if sv.base_name == "JES")
        assert sv.up_column == "JESUp"
        assert sv.down_column == "JESDown"
        assert sv.group_name == "jet_energy"

    def test_weight_variations_included(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "signal_region", "histogram")
        assert len(plan.weight_variations) == 1
        assert plan.weight_variations[0].name == "btag"

    def test_weight_variations_filtered_by_process(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("ttbar", "signal_region", "histogram")
        # btag only applies to signal
        assert len(plan.weight_variations) == 0

    def test_weight_variations_filtered_by_region(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("signal", "control_region", "histogram")
        assert len(plan.weight_variations) == 0

    def test_empty_plan_for_nonmatching_context(self):
        orch = self._make_orch()
        plan = orch.get_variation_plan("qcd", "control_region", "plot")
        assert plan.systematic_variations == []
        assert plan.weight_variations == []


# ---------------------------------------------------------------------------
# VariationOrchestrator.validate_coverage
# ---------------------------------------------------------------------------


class TestValidateCoverage:
    def _make_orch(self) -> VariationOrchestrator:
        group = _make_group(
            name="jet_energy",
            systematics=["JES", "JER"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        reg = _make_registry(group)
        wv = WeightVariationSpec(
            "btag", "w_nom", "w_btagUp", "w_btagDown",
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        return VariationOrchestrator(reg, weight_variations=[wv], nominal_weight="w_nom")

    def test_full_coverage_no_findings(self):
        orch = self._make_orch()
        cols = ["JESUp", "JESDown", "JERUp", "JERDown", "w_btagUp", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        assert findings == []

    def test_missing_up_detected(self):
        orch = self._make_orch()
        cols = ["JESDown", "JERUp", "JERDown", "w_btagUp", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        jes_findings = [f for f in findings if f.variable == "JES"]
        assert len(jes_findings) == 1
        assert jes_findings[0].missing_up is True
        assert jes_findings[0].missing_down is False

    def test_missing_down_detected(self):
        orch = self._make_orch()
        cols = ["JESUp", "JESDown", "JERUp", "w_btagUp", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        jer_findings = [f for f in findings if f.variable == "JER"]
        assert len(jer_findings) == 1
        assert jer_findings[0].missing_up is False
        assert jer_findings[0].missing_down is True

    def test_not_found_detected(self):
        orch = self._make_orch()
        # JES is completely absent
        cols = ["JERUp", "JERDown", "w_btagUp", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        jes_findings = [f for f in findings if f.variable == "JES"]
        assert len(jes_findings) == 1
        assert jes_findings[0].not_found is True
        assert jes_findings[0].missing_up is True
        assert jes_findings[0].missing_down is True

    def test_weight_variation_missing_up(self):
        orch = self._make_orch()
        # w_btagUp absent
        cols = ["JESUp", "JESDown", "JERUp", "JERDown", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        wv_findings = [f for f in findings if f.variable == "btag"]
        assert len(wv_findings) == 1
        assert wv_findings[0].missing_up is True
        assert wv_findings[0].missing_down is False

    def test_severity_override(self):
        orch = self._make_orch()
        cols = ["JERUp", "JERDown", "w_btagUp", "w_btagDown"]
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram",
            severity="warn"
        )
        assert all(f.severity == "warn" for f in findings)

    def test_default_severity(self):
        orch = self._make_orch()
        cols = []
        findings = orch.validate_coverage(
            cols, processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        assert all(f.severity == "error" for f in findings)

    def test_process_not_matching_returns_no_findings(self):
        orch = self._make_orch()
        # qcd is not in jet_energy's processes; no systematics apply
        cols = []
        findings = orch.validate_coverage(
            cols, processes=["qcd"], regions=["sr"], output_usage="histogram"
        )
        assert findings == []

    def test_empty_columns_all_missing(self):
        orch = self._make_orch()
        findings = orch.validate_coverage(
            [], processes=["signal"], regions=["sr"], output_usage="histogram"
        )
        syst_vars = {f.variable for f in findings}
        assert "JES" in syst_vars
        assert "JER" in syst_vars
        assert "btag" in syst_vars

    def test_multiple_processes_and_regions(self):
        group = NuisanceGroup(
            name="energy",
            group_type="shape",
            systematics=["E"],
            processes=[],  # all
            regions=[],    # all
            output_usage=[],  # all
        )
        reg = _make_registry(group)
        orch = VariationOrchestrator(reg)
        # Both processes, both regions should yield findings when columns absent
        findings = orch.validate_coverage(
            [], processes=["a", "b"], regions=["r1", "r2"]
        )
        # 2 processes × 2 regions = 4 (process, region) pairs, each with 1 missing
        assert len(findings) == 4
        processes_found = {f.process for f in findings}
        assert processes_found == {"a", "b"}


# ---------------------------------------------------------------------------
# VariationOrchestrator.build_validation_report
# ---------------------------------------------------------------------------


class TestBuildValidationReport:
    def _make_orch(self) -> VariationOrchestrator:
        group = _make_group(
            name="jet_energy",
            systematics=["JES", "JER"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        reg = _make_registry(group)
        return VariationOrchestrator(reg, nominal_weight="w_nom")

    def test_no_errors_when_complete(self):
        orch = self._make_orch()
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown", "JERUp", "JERDown"],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        assert not report.errors

    def test_errors_added_when_missing(self):
        orch = self._make_orch()
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown"],  # JER missing
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        # JERUp and JERDown are both missing → one error for not_found
        assert report.has_errors

    def test_nuisance_group_coverage_entries_added(self):
        orch = self._make_orch()
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown"],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        assert len(report.nuisance_group_coverage) == 1
        entry = report.nuisance_group_coverage[0]
        assert entry.group_name == "jet_energy"
        assert "JER" in entry.not_found

    def test_warnings_with_warn_severity(self):
        orch = VariationOrchestrator(
            nuisance_registry=_make_registry(
                _make_group(
                    name="jes",
                    systematics=["JES"],
                    processes=["signal"],
                    regions=["sr"],
                    output_usage=["histogram"],
                )
            ),
            missing_severity="warn",
        )
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=[],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
            severity="warn",
        )
        assert not report.errors
        assert report.warnings

    def test_complete_coverage_no_coverage_entries_flagged(self):
        orch = self._make_orch()
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown", "JERUp", "JERDown"],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        assert not report.has_errors
        # All coverage entries should be complete
        assert all(e.is_complete for e in report.nuisance_group_coverage)

    def test_report_text_output_contains_nuisance_section(self):
        orch = self._make_orch()
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=[],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        text = report.to_text()
        assert "NUISANCE GROUP COVERAGE" in text

    def test_weight_variation_error_reported(self):
        group = _make_group(
            name="jes",
            systematics=["JES"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        wv = WeightVariationSpec(
            "btag", "w_nom", "w_btagUp", "w_btagDown",
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        orch = VariationOrchestrator(
            _make_registry(group), weight_variations=[wv]
        )
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown"],  # weight cols missing
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        # Should have errors for missing weight columns
        assert any("btag" in e for e in report.errors)


# ---------------------------------------------------------------------------
# VariationOrchestrator._infer_processes / _infer_regions
# ---------------------------------------------------------------------------


class TestInferProcessesRegions:
    def test_infer_processes_specific(self):
        group = _make_group(processes=["a", "b"])
        orch = VariationOrchestrator(_make_registry(group))
        procs = orch._infer_processes()
        assert set(procs) == {"a", "b"}

    def test_infer_processes_wildcard_only(self):
        group = _make_group(processes=[])
        orch = VariationOrchestrator(_make_registry(group))
        procs = orch._infer_processes()
        # Sentinel "" returned
        assert "" in procs

    def test_infer_processes_mixed(self):
        g1 = _make_group(name="g1", processes=["signal"])
        g2 = _make_group(name="g2", processes=[])
        orch = VariationOrchestrator(_make_registry(g1, g2))
        procs = orch._infer_processes()
        # Should include "signal" and "" sentinel
        assert "signal" in procs
        assert "" in procs

    def test_infer_regions_specific(self):
        group = _make_group(regions=["sr", "cr"])
        orch = VariationOrchestrator(_make_registry(group))
        regs = orch._infer_regions()
        assert set(regs) == {"sr", "cr"}

    def test_infer_regions_wildcard_only(self):
        group = _make_group(regions=[])
        orch = VariationOrchestrator(_make_registry(group))
        regs = orch._infer_regions()
        assert "" in regs

    def test_validate_coverage_uses_inferred_processes(self):
        """validate_coverage with no processes/regions should use inferred values."""
        group = NuisanceGroup(
            name="jes",
            group_type="shape",
            systematics=["JES"],
            processes=["signal"],
            regions=["sr"],
        )
        orch = VariationOrchestrator(_make_registry(group))
        # No processes/regions provided; should infer ["signal"] and ["sr"]
        findings = orch.validate_coverage(available_columns=[])
        assert any(f.variable == "JES" for f in findings)

    def test_empty_registry_no_findings(self):
        orch = VariationOrchestrator(_make_registry())
        findings = orch.validate_coverage(available_columns=[], processes=["x"], regions=["y"])
        assert findings == []
