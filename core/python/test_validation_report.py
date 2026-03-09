"""
Tests for core/python/validation_report.py.

Covers:
- ReportSeverity enum values
- EventCountEntry auto-efficiency computation
- CutflowEntry construction
- MissingBranchEntry.is_complete property
- ConfigMismatchEntry to_dict / from_dict round-trips
- SystematicEntry.is_complete property
- WeightSummaryEntry construction
- OutputIntegrityEntry.is_ok property
- ValidationReport adder methods and summary properties (has_errors, has_warnings)
- ValidationReport to_dict / to_yaml / to_json / to_text round-trips
- ValidationReport save_yaml / save_json / save_text / load_yaml / load_json file I/O
- ValidationReport.from_dict round-trip
- generate_report_from_manifest with various manifest configurations
- CLI main() behaviour (via subprocess-free invocation with patched sys.argv)
"""
from __future__ import annotations

import json
import os
import sys
import textwrap

import pytest
import yaml

# Make core/python importable when running from repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from validation_report import (
    VALIDATION_REPORT_VERSION,
    ConfigMismatchEntry,
    CutflowEntry,
    EventCountEntry,
    MissingBranchEntry,
    NuisanceGroupCoverageEntry,
    OutputIntegrityEntry,
    RegionEntry,
    RegionReferenceEntry,
    ReportSeverity,
    SystematicEntry,
    ValidationReport,
    WeightSummaryEntry,
    generate_report_from_manifest,
    validate_region_references,
)
from output_schema import (
    CutflowSchema,
    HistogramSchema,
    IntermediateArtifactSchema,
    LawArtifactSchema,
    MetadataSchema,
    NuisanceGroupDefinition,
    OutputManifest,
    RegionDefinition,
    SkimSchema,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _minimal_manifest(**kwargs) -> OutputManifest:
    """Return a minimal valid OutputManifest."""
    return OutputManifest(
        skim=SkimSchema(output_file="out.root", tree_name="Events"),
        **kwargs,
    )


# ---------------------------------------------------------------------------
# ReportSeverity
# ---------------------------------------------------------------------------


class TestReportSeverity:
    def test_values(self):
        assert ReportSeverity.INFO.value == "info"
        assert ReportSeverity.WARNING.value == "warning"
        assert ReportSeverity.ERROR.value == "error"

    def test_is_str_subclass(self):
        # ReportSeverity inherits str so it serialises naturally in JSON/YAML
        assert isinstance(ReportSeverity.INFO, str)


# ---------------------------------------------------------------------------
# EventCountEntry
# ---------------------------------------------------------------------------


class TestEventCountEntry:
    def test_auto_efficiency(self):
        e = EventCountEntry(sample="ttbar", stage="presel", total_events=1000, selected_events=250)
        assert e.efficiency == pytest.approx(0.25)

    def test_no_efficiency_when_none_selected(self):
        e = EventCountEntry(sample="ttbar", stage="presel", total_events=1000)
        assert e.efficiency is None

    def test_no_efficiency_when_total_zero(self):
        e = EventCountEntry(sample="ttbar", stage="presel", total_events=0, selected_events=0)
        assert e.efficiency is None

    def test_explicit_efficiency_not_overwritten(self):
        e = EventCountEntry(
            sample="ttbar", stage="presel", total_events=1000, selected_events=100, efficiency=0.99
        )
        assert e.efficiency == pytest.approx(0.99)

    def test_asdict_round_trip(self):
        from dataclasses import asdict
        e = EventCountEntry(sample="s", stage="st", total_events=100, selected_events=50)
        d = asdict(e)
        assert d["sample"] == "s"
        assert d["total_events"] == 100
        assert d["efficiency"] == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# CutflowEntry
# ---------------------------------------------------------------------------


class TestCutflowEntry:
    def test_construction(self):
        c = CutflowEntry(cut_name="trigger", events_passed=900, events_cumulative=900)
        assert c.cut_name == "trigger"
        assert c.events_cumulative == 900

    def test_optional_fields_none(self):
        c = CutflowEntry(cut_name="baseline", events_passed=500)
        assert c.events_cumulative is None
        assert c.relative_efficiency is None
        assert c.cumulative_efficiency is None


# ---------------------------------------------------------------------------
# MissingBranchEntry
# ---------------------------------------------------------------------------


class TestMissingBranchEntry:
    def test_is_complete_true(self):
        m = MissingBranchEntry(
            artifact_role="skim",
            expected_branches=["pt", "eta"],
            missing_branches=[],
        )
        assert m.is_complete is True

    def test_is_complete_false(self):
        m = MissingBranchEntry(
            artifact_role="skim",
            expected_branches=["pt", "eta"],
            missing_branches=["eta"],
        )
        assert m.is_complete is False


# ---------------------------------------------------------------------------
# ConfigMismatchEntry
# ---------------------------------------------------------------------------


class TestConfigMismatchEntry:
    def test_to_dict(self):
        c = ConfigMismatchEntry(key="tree_name", expected="Events", actual="Skimmed")
        d = c.to_dict()
        assert d["key"] == "tree_name"
        assert d["expected"] == "Events"
        assert d["actual"] == "Skimmed"
        assert d["severity"] == "error"

    def test_from_dict_round_trip(self):
        c = ConfigMismatchEntry(
            key="schema_version", expected=1, actual=2, severity=ReportSeverity.WARNING
        )
        restored = ConfigMismatchEntry.from_dict(c.to_dict())
        assert restored.key == c.key
        assert restored.expected == c.expected
        assert restored.actual == c.actual
        assert restored.severity == ReportSeverity.WARNING

    def test_default_severity_is_error(self):
        c = ConfigMismatchEntry(key="x", expected=1, actual=2)
        assert c.severity == ReportSeverity.ERROR


# ---------------------------------------------------------------------------
# SystematicEntry
# ---------------------------------------------------------------------------


class TestSystematicEntry:
    def test_is_complete_true(self):
        s = SystematicEntry(systematic_name="JES", has_up=True, has_down=True)
        assert s.is_complete is True

    def test_is_complete_false_missing_down(self):
        s = SystematicEntry(systematic_name="JES", has_up=True, has_down=False)
        assert s.is_complete is False

    def test_extra_variations_default_empty(self):
        s = SystematicEntry(systematic_name="PU")
        assert s.extra_variations == []


# ---------------------------------------------------------------------------
# WeightSummaryEntry
# ---------------------------------------------------------------------------


class TestWeightSummaryEntry:
    def test_construction(self):
        w = WeightSummaryEntry(
            sample="ttbar",
            sum_weights=500_000.0,
            n_events=1_000_000,
            n_negative_weights=0,
            min_weight=-1.5,
            max_weight=3.2,
        )
        assert w.sum_weights == 500_000.0
        assert w.n_negative_weights == 0

    def test_optional_fields_default_none(self):
        w = WeightSummaryEntry(sample="data", sum_weights=100.0)
        assert w.n_events is None
        assert w.min_weight is None


# ---------------------------------------------------------------------------
# OutputIntegrityEntry
# ---------------------------------------------------------------------------


class TestOutputIntegrityEntry:
    def test_is_ok_true(self):
        e = OutputIntegrityEntry(artifact_role="skim", path="out.root", exists=True, size_bytes=1024)
        assert e.is_ok is True

    def test_is_ok_false_missing(self):
        e = OutputIntegrityEntry(artifact_role="skim", path="missing.root", exists=False)
        assert e.is_ok is False

    def test_is_ok_unchecked_is_ok(self):
        # exists=None means "not checked" – not an error
        e = OutputIntegrityEntry(artifact_role="skim", path="out.root", exists=None)
        assert e.is_ok is True

    def test_is_ok_false_with_issues(self):
        e = OutputIntegrityEntry(
            artifact_role="cutflow", path="meta.root", exists=True, issues=["Tree not found"]
        )
        assert e.is_ok is False


# ---------------------------------------------------------------------------
# ValidationReport – basic construction and adders
# ---------------------------------------------------------------------------


class TestValidationReportConstruction:
    def test_default_attributes(self):
        r = ValidationReport(stage="test")
        assert r.stage == "test"
        assert r.report_version == VALIDATION_REPORT_VERSION
        assert r.event_counts == []
        assert r.errors == []
        assert r.warnings == []
        assert r.has_errors is False
        assert r.has_warnings is False

    def test_timestamp_auto(self):
        r = ValidationReport(stage="test")
        assert r.timestamp  # non-empty string

    def test_timestamp_explicit(self):
        r = ValidationReport(stage="test", timestamp="2024-01-01T00:00:00")
        assert r.timestamp == "2024-01-01T00:00:00"

    def test_add_error(self):
        r = ValidationReport(stage="x")
        r.add_error("something broke")
        assert "something broke" in r.errors
        assert r.has_errors is True

    def test_add_warning(self):
        r = ValidationReport(stage="x")
        r.add_warning("minor issue")
        assert "minor issue" in r.warnings
        assert r.has_warnings is True

    def test_add_event_count(self):
        r = ValidationReport(stage="presel")
        r.add_event_count(EventCountEntry(sample="s", stage="presel", total_events=100))
        assert len(r.event_counts) == 1

    def test_add_cutflow_step(self):
        r = ValidationReport(stage="skim")
        r.add_cutflow_step(CutflowEntry(cut_name="trigger", events_passed=900))
        assert len(r.cutflow) == 1

    def test_add_missing_branches(self):
        r = ValidationReport(stage="x")
        r.add_missing_branches(
            MissingBranchEntry("skim", ["pt", "eta"], ["eta"])
        )
        assert len(r.missing_branches) == 1
        assert r.has_errors is True  # missing branches → error

    def test_add_config_mismatch_error(self):
        r = ValidationReport(stage="x")
        r.add_config_mismatch(
            ConfigMismatchEntry("tree", "Events", "Skimmed", ReportSeverity.ERROR)
        )
        assert r.has_errors is True

    def test_add_config_mismatch_warning(self):
        r = ValidationReport(stage="x")
        r.add_config_mismatch(
            ConfigMismatchEntry("k", "a", "b", ReportSeverity.WARNING)
        )
        assert r.has_warnings is True
        assert r.has_errors is False

    def test_add_systematic(self):
        r = ValidationReport(stage="x")
        r.add_systematic(SystematicEntry("JES", has_up=True, has_down=False))
        assert len(r.systematics) == 1
        assert r.has_warnings is True  # incomplete systematic → warning

    def test_add_weight_summary(self):
        r = ValidationReport(stage="x")
        r.add_weight_summary(WeightSummaryEntry("ttbar", 500_000.0))
        assert len(r.weight_summaries) == 1

    def test_add_output_integrity_ok(self):
        r = ValidationReport(stage="x")
        r.add_output_integrity(OutputIntegrityEntry("skim", "out.root", exists=True))
        assert r.has_errors is False

    def test_add_output_integrity_unchecked_not_error(self):
        r = ValidationReport(stage="x")
        r.add_output_integrity(OutputIntegrityEntry("skim", "out.root", exists=None))
        assert r.has_errors is False

    def test_add_output_integrity_fail(self):
        r = ValidationReport(stage="x")
        r.add_output_integrity(OutputIntegrityEntry("skim", "out.root", exists=False))
        assert r.has_errors is True


# ---------------------------------------------------------------------------
# ValidationReport – serialisation round-trips
# ---------------------------------------------------------------------------


class TestValidationReportSerialisation:
    def _full_report(self) -> ValidationReport:
        r = ValidationReport(stage="full_test", timestamp="2024-06-01T12:00:00")
        r.add_error("a critical error")
        r.add_warning("a minor warning")
        r.add_event_count(EventCountEntry("ttbar", "presel", 1_000_000, 250_000))
        r.add_cutflow_step(CutflowEntry("trigger", 900_000, 900_000, 0.9, 0.9))
        r.add_missing_branches(MissingBranchEntry("skim", ["pt", "eta"], ["eta"]))
        r.add_config_mismatch(
            ConfigMismatchEntry("tree_name", "Events", "Skimmed", ReportSeverity.ERROR)
        )
        r.add_systematic(SystematicEntry("JES", has_up=True, has_down=True))
        r.add_weight_summary(
            WeightSummaryEntry("ttbar", 500_000.0, n_events=1_000_000, n_negative_weights=10)
        )
        r.add_output_integrity(
            OutputIntegrityEntry("skim", "out.root", exists=True, size_bytes=2048)
        )
        return r

    def test_to_dict_keys(self):
        r = self._full_report()
        d = r.to_dict()
        assert "report_version" in d
        assert "stage" in d
        assert "timestamp" in d
        assert "summary" in d
        assert "event_counts" in d
        assert "cutflow" in d
        assert "missing_branches" in d
        assert "config_mismatches" in d
        assert "systematics" in d
        assert "weight_summaries" in d
        assert "output_integrity" in d
        assert "errors" in d
        assert "warnings" in d

    def test_summary_fields(self):
        r = self._full_report()
        d = r.to_dict()
        s = d["summary"]
        assert s["n_event_count_entries"] == 1
        assert s["n_cutflow_steps"] == 1
        assert s["n_systematics"] == 1
        assert s["n_errors"] == 1
        assert s["n_warnings"] == 1

    def test_from_dict_round_trip(self):
        original = self._full_report()
        restored = ValidationReport.from_dict(original.to_dict())
        assert restored.stage == original.stage
        assert restored.timestamp == original.timestamp
        assert restored.errors == original.errors
        assert restored.warnings == original.warnings
        assert len(restored.event_counts) == 1
        assert restored.event_counts[0].sample == "ttbar"
        assert len(restored.cutflow) == 1
        assert len(restored.missing_branches) == 1
        assert len(restored.config_mismatches) == 1
        assert len(restored.systematics) == 1
        assert len(restored.weight_summaries) == 1
        assert len(restored.output_integrity) == 1

    def test_to_yaml_is_valid_yaml(self):
        r = self._full_report()
        raw = r.to_yaml()
        parsed = yaml.safe_load(raw)
        assert isinstance(parsed, dict)
        assert parsed["stage"] == "full_test"

    def test_to_json_is_valid_json(self):
        r = self._full_report()
        raw = r.to_json()
        parsed = json.loads(raw)
        assert isinstance(parsed, dict)
        assert parsed["stage"] == "full_test"

    def test_to_text_contains_stage(self):
        r = self._full_report()
        text = r.to_text()
        assert "full_test" in text
        assert "VALIDATION REPORT" in text

    def test_to_text_status_failed(self):
        r = self._full_report()
        assert "FAILED" in r.to_text()

    def test_to_text_status_ok(self):
        r = ValidationReport(stage="clean", timestamp="2024-01-01T00:00:00")
        r.add_event_count(EventCountEntry("s", "clean", 100, 100))
        assert "OK" in r.to_text()

    def test_to_text_status_warnings(self):
        r = ValidationReport(stage="partial", timestamp="2024-01-01T00:00:00")
        r.add_warning("minor")
        assert "WARNINGS" in r.to_text()

    def test_to_text_contains_event_counts(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_event_count(EventCountEntry("my_sample", "x", 1000, 500))
        text = r.to_text()
        assert "my_sample" in text
        assert "EVENT COUNTS" in text

    def test_to_text_contains_cutflow(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_cutflow_step(CutflowEntry("mycut", 900))
        text = r.to_text()
        assert "mycut" in text
        assert "CUTFLOW" in text

    def test_to_text_contains_systematics(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_systematic(SystematicEntry("JES", has_up=True, has_down=True))
        text = r.to_text()
        assert "JES" in text
        assert "SYSTEMATIC" in text

    def test_to_text_contains_weight_summaries(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_weight_summary(WeightSummaryEntry("ttbar", 123456.0))
        text = r.to_text()
        assert "ttbar" in text
        assert "WEIGHT" in text

    def test_to_text_contains_output_integrity(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_output_integrity(OutputIntegrityEntry("skim", "/data/out.root", exists=True))
        text = r.to_text()
        assert "/data/out.root" in text
        assert "OUTPUT INTEGRITY" in text

    def test_to_text_contains_missing_branches(self):
        r = ValidationReport(stage="x", timestamp="2024-01-01T00:00:00")
        r.add_missing_branches(MissingBranchEntry("skim", ["pt"], ["pt"]))
        text = r.to_text()
        assert "pt" in text
        assert "MISSING BRANCHES" in text


# ---------------------------------------------------------------------------
# ValidationReport – file I/O
# ---------------------------------------------------------------------------


class TestValidationReportFileIO:
    def test_save_and_load_yaml(self, tmp_path):
        r = ValidationReport(stage="io_test", timestamp="2024-01-01T00:00:00")
        r.add_error("test error")
        path = str(tmp_path / "report.yaml")
        r.save_yaml(path)
        loaded = ValidationReport.load_yaml(path)
        assert loaded.stage == "io_test"
        assert loaded.errors == ["test error"]

    def test_save_and_load_json(self, tmp_path):
        r = ValidationReport(stage="io_test", timestamp="2024-01-01T00:00:00")
        r.add_warning("test warning")
        path = str(tmp_path / "report.json")
        r.save_json(path)
        loaded = ValidationReport.load_json(path)
        assert loaded.stage == "io_test"
        assert loaded.warnings == ["test warning"]

    def test_save_text(self, tmp_path):
        r = ValidationReport(stage="io_test", timestamp="2024-01-01T00:00:00")
        path = str(tmp_path / "report.txt")
        r.save_text(path)
        with open(path, encoding="utf-8") as fh:
            content = fh.read()
        assert "io_test" in content
        assert "VALIDATION REPORT" in content

    def test_save_creates_parent_dirs(self, tmp_path):
        r = ValidationReport(stage="nested", timestamp="2024-01-01T00:00:00")
        path = str(tmp_path / "deep" / "nested" / "report.yaml")
        r.save_yaml(path)
        assert os.path.exists(path)

    def test_load_yaml_invalid_content(self, tmp_path):
        path = tmp_path / "bad.yaml"
        path.write_text("- item1\n- item2\n")  # list, not dict
        with pytest.raises(ValueError, match="YAML mapping"):
            ValidationReport.load_yaml(str(path))

    def test_load_json_invalid_content(self, tmp_path):
        path = tmp_path / "bad.json"
        path.write_text("[1,2,3]")  # array, not object
        with pytest.raises(ValueError, match="JSON object"):
            ValidationReport.load_json(str(path))


# ---------------------------------------------------------------------------
# generate_report_from_manifest
# ---------------------------------------------------------------------------


class TestGenerateReportFromManifest:
    def test_minimal_manifest_no_errors(self):
        m = _minimal_manifest()
        report = generate_report_from_manifest(m, stage="skim")
        assert report.stage == "skim"
        # A valid manifest with all files unchecked should produce no errors from
        # the schema validation alone (output_file exists).
        assert not report.errors  # no structural errors in a valid manifest

    def test_invalid_manifest_adds_errors(self):
        # An empty manifest (no schemas at all) is invalid
        m = OutputManifest()  # no schemas set → validate() returns errors
        report = generate_report_from_manifest(m, stage="test")
        assert report.errors  # should contain manifest validation errors

    def test_output_integrity_entries_from_skim(self):
        m = _minimal_manifest()
        report = generate_report_from_manifest(m, stage="skim")
        roles = [e.artifact_role for e in report.output_integrity]
        assert "skim" in roles

    def test_output_integrity_entries_from_all_schemas(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="skim.root"),
            histograms=HistogramSchema(output_file="hist.root"),
            metadata=MetadataSchema(output_file="meta.root"),
            cutflow=CutflowSchema(output_file="cutflow.root"),
        )
        report = generate_report_from_manifest(m, stage="full")
        roles = {e.artifact_role for e in report.output_integrity}
        assert roles == {"skim", "histograms", "metadata", "cutflow"}

    def test_output_integrity_entries_from_law_artifact(self):
        m = OutputManifest(
            law_artifacts=[
                LawArtifactSchema(
                    artifact_type="run_job",
                    path_pattern="outputs/*.root",
                    format="root",
                )
            ]
        )
        report = generate_report_from_manifest(m, stage="law")
        roles = [e.artifact_role for e in report.output_integrity]
        assert "law_artifacts[0]" in roles

    def test_output_integrity_entries_from_intermediate_artifact(self):
        m = OutputManifest(
            intermediate_artifacts=[
                IntermediateArtifactSchema(
                    artifact_kind="preselection",
                    output_file="presel.root",
                )
            ]
        )
        report = generate_report_from_manifest(m, stage="presel")
        roles = [e.artifact_role for e in report.output_integrity]
        assert "intermediate_artifacts[0]" in roles

    def test_missing_branches_stub_from_skim_branches(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root", branches=["pt", "eta", "phi"])
        )
        report = generate_report_from_manifest(m, stage="skim")
        mb_roles = [e.artifact_role for e in report.missing_branches]
        assert "skim" in mb_roles
        skim_mb = next(e for e in report.missing_branches if e.artifact_role == "skim")
        assert set(skim_mb.expected_branches) == {"pt", "eta", "phi"}
        # No actual file check → missing list is empty (caller fills it in)
        assert skim_mb.missing_branches == []

    def test_no_missing_branch_entry_when_branches_empty(self):
        m = _minimal_manifest()  # branches=[] by default
        report = generate_report_from_manifest(m, stage="skim")
        assert report.missing_branches == []

    def test_missing_branches_from_intermediate_artifact(self):
        m = OutputManifest(
            intermediate_artifacts=[
                IntermediateArtifactSchema(
                    artifact_kind="column_snapshot",
                    output_file="snap.root",
                    columns=["MET", "HT"],
                )
            ]
        )
        report = generate_report_from_manifest(m, stage="snap")
        mb = [e for e in report.missing_branches if e.artifact_role == "intermediate_artifacts[0]"]
        assert mb
        assert set(mb[0].expected_branches) == {"MET", "HT"}

    def test_cutflow_steps_from_counter_keys(self):
        m = OutputManifest(
            cutflow=CutflowSchema(
                output_file="meta.root",
                counter_keys=["total", "trigger", "baseline"],
            )
        )
        report = generate_report_from_manifest(m, stage="cutflow")
        cut_names = [e.cut_name for e in report.cutflow]
        assert cut_names == ["total", "trigger", "baseline"]

    def test_no_cutflow_steps_when_no_cutflow_schema(self):
        m = _minimal_manifest()
        report = generate_report_from_manifest(m, stage="skim")
        assert report.cutflow == []

    def test_version_mismatch_recorded_as_config_mismatch(self):
        # Build a manifest with a manually altered schema_version to trigger a mismatch
        m = _minimal_manifest()
        m.skim.schema_version = 999  # force mismatch
        report = generate_report_from_manifest(m, stage="skim")
        keys = [e.key for e in report.config_mismatches]
        assert any("skim" in k for k in keys)

    def test_check_files_false_by_default(self, tmp_path):
        """When check_files=False, existence is not tested (exists=None stub)."""
        m = _minimal_manifest()
        report = generate_report_from_manifest(m, stage="skim", check_files=False)
        # All entries should have exists=None (not checked)
        for e in report.output_integrity:
            assert e.exists is None

    def test_check_files_true_existing_file(self, tmp_path):
        path = str(tmp_path / "real.root")
        with open(path, "wb") as fh:
            fh.write(b"FAKE_ROOT_CONTENT")
        m = OutputManifest(skim=SkimSchema(output_file=path))
        report = generate_report_from_manifest(m, stage="skim", check_files=True)
        skim_entry = next(e for e in report.output_integrity if e.artifact_role == "skim")
        assert skim_entry.exists is True
        assert skim_entry.size_bytes == len(b"FAKE_ROOT_CONTENT")

    def test_check_files_true_missing_file(self, tmp_path):
        m = OutputManifest(
            skim=SkimSchema(output_file=str(tmp_path / "nonexistent.root"))
        )
        report = generate_report_from_manifest(m, stage="skim", check_files=True)
        skim_entry = next(e for e in report.output_integrity if e.artifact_role == "skim")
        assert skim_entry.exists is False

    def test_check_files_empty_file_adds_issue(self, tmp_path):
        path = str(tmp_path / "empty.root")
        open(path, "wb").close()  # create 0-byte file
        m = OutputManifest(skim=SkimSchema(output_file=path))
        report = generate_report_from_manifest(m, stage="skim", check_files=True)
        skim_entry = next(e for e in report.output_integrity if e.artifact_role == "skim")
        assert skim_entry.exists is True
        assert skim_entry.size_bytes == 0
        assert any("empty" in issue.lower() or "0" in issue for issue in skim_entry.issues)


# ---------------------------------------------------------------------------
# CLI main() – basic invocation tests
# ---------------------------------------------------------------------------


class TestCLIMain:
    """Test the CLI main() function without forking a subprocess."""

    def _run_main(self, argv):
        """Invoke main() with given argv, capturing SystemExit code."""
        import validation_report as vr
        sys.argv = ["validation_report"] + argv
        with pytest.raises(SystemExit) as exc_info:
            vr.main()
        return exc_info.value.code

    def test_valid_manifest_exits_0(self, tmp_path):
        m = _minimal_manifest()
        manifest_path = str(tmp_path / "manifest.yaml")
        m.save_yaml(manifest_path)
        code = self._run_main([manifest_path, "--stage", "skim"])
        assert code == 0

    def test_invalid_manifest_exits_1(self, tmp_path):
        # A manifest with an empty output_file is invalid
        m = OutputManifest(skim=SkimSchema(output_file=""))
        manifest_path = str(tmp_path / "bad_manifest.yaml")
        m.save_yaml(manifest_path)
        code = self._run_main([manifest_path, "--stage", "skim"])
        assert code == 1

    def test_missing_manifest_file_exits_1(self, tmp_path):
        code = self._run_main([str(tmp_path / "nonexistent.yaml"), "--stage", "x"])
        assert code == 1

    def test_out_yaml_written(self, tmp_path):
        m = _minimal_manifest()
        manifest_path = str(tmp_path / "manifest.yaml")
        m.save_yaml(manifest_path)
        out_yaml = str(tmp_path / "report.yaml")
        self._run_main([manifest_path, "--out-yaml", out_yaml])
        assert os.path.exists(out_yaml)
        with open(out_yaml) as fh:
            content = yaml.safe_load(fh)
        assert "stage" in content

    def test_out_json_written(self, tmp_path):
        m = _minimal_manifest()
        manifest_path = str(tmp_path / "manifest.yaml")
        m.save_yaml(manifest_path)
        out_json = str(tmp_path / "report.json")
        self._run_main([manifest_path, "--out-json", out_json])
        assert os.path.exists(out_json)
        with open(out_json) as fh:
            content = json.load(fh)
        assert "stage" in content

    def test_out_text_written(self, tmp_path):
        m = _minimal_manifest()
        manifest_path = str(tmp_path / "manifest.yaml")
        m.save_yaml(manifest_path)
        out_text = str(tmp_path / "report.txt")
        self._run_main([manifest_path, "--out-text", out_text])
        assert os.path.exists(out_text)
        with open(out_text) as fh:
            content = fh.read()
        assert "VALIDATION REPORT" in content


# ---------------------------------------------------------------------------
# RegionReferenceEntry
# ---------------------------------------------------------------------------


class TestRegionReferenceEntry:
    def test_known_reference_is_valid(self):
        e = RegionReferenceEntry(
            config_type="histogram",
            config_name="pt",
            referenced_region="signal",
            is_known=True,
        )
        assert e.is_valid is True
        assert e.is_known is True

    def test_unknown_reference_is_not_valid(self):
        e = RegionReferenceEntry(
            config_type="cutflow",
            config_name="my_cutflow",
            referenced_region="ghost_region",
            is_known=False,
        )
        assert e.is_valid is False
        assert e.is_known is False

    def test_default_is_known_true(self):
        e = RegionReferenceEntry(
            config_type="histogram",
            config_name="eta",
            referenced_region="presel",
        )
        assert e.is_known is True


# ---------------------------------------------------------------------------
# validate_region_references()
# ---------------------------------------------------------------------------


class TestValidateRegionReferences:
    def test_all_known_no_errors(self):
        report = ValidationReport(stage="test")
        validate_region_references(
            report,
            known_regions=["signal", "control", "presel"],
            referenced=[
                {"config_type": "histogram", "config_name": "pt",  "region": "signal"},
                {"config_type": "histogram", "config_name": "eta", "region": "presel"},
            ],
        )
        assert len(report.region_references) == 2
        assert all(e.is_known for e in report.region_references)
        assert report.has_errors is False

    def test_unknown_region_causes_has_errors(self):
        report = ValidationReport(stage="test")
        validate_region_references(
            report,
            known_regions=["signal"],
            referenced=[
                {"config_type": "histogram", "config_name": "pt", "region": "ghost"},
            ],
        )
        assert report.has_errors is True
        assert report.region_references[0].is_known is False

    def test_empty_referenced_list(self):
        report = ValidationReport(stage="test")
        validate_region_references(report, known_regions=["signal"], referenced=[])
        assert len(report.region_references) == 0
        assert report.has_errors is False

    def test_mixed_known_and_unknown(self):
        report = ValidationReport(stage="test")
        validate_region_references(
            report,
            known_regions=["signal"],
            referenced=[
                {"config_type": "histogram", "config_name": "pt", "region": "signal"},
                {"config_type": "cutflow", "config_name": "myCF", "region": "missing"},
            ],
        )
        known   = [e for e in report.region_references if e.is_known]
        unknown = [e for e in report.region_references if not e.is_known]
        assert len(known) == 1
        assert len(unknown) == 1
        assert unknown[0].referenced_region == "missing"


# ---------------------------------------------------------------------------
# ValidationReport: region_references round-trips
# ---------------------------------------------------------------------------


class TestValidationReportRegionReferences:
    def test_add_region_reference(self):
        report = ValidationReport(stage="histogram")
        report.add_region_reference(
            RegionReferenceEntry("histogram", "pt", "signal", True)
        )
        assert len(report.region_references) == 1

    def test_has_errors_unknown_reference(self):
        report = ValidationReport(stage="x")
        report.add_region_reference(
            RegionReferenceEntry("histogram", "pt", "ghost", False)
        )
        assert report.has_errors is True

    def test_to_dict_includes_region_references(self):
        report = ValidationReport(stage="x")
        report.add_region_reference(
            RegionReferenceEntry("cutflow", "cf", "signal", True)
        )
        d = report.to_dict()
        assert "region_references" in d
        assert d["region_references"][0]["referenced_region"] == "signal"
        assert d["summary"]["n_region_references"] == 1

    def test_from_dict_round_trip(self):
        report = ValidationReport(stage="x")
        report.add_region_reference(
            RegionReferenceEntry("histogram", "eta", "control", False)
        )
        d = report.to_dict()
        restored = ValidationReport.from_dict(d)
        assert len(restored.region_references) == 1
        e = restored.region_references[0]
        assert e.config_type == "histogram"
        assert e.config_name == "eta"
        assert e.referenced_region == "control"
        assert e.is_known is False

    def test_to_text_includes_region_references_section(self):
        report = ValidationReport(stage="x")
        report.add_region_reference(
            RegionReferenceEntry("histogram", "pt", "signal", True)
        )
        report.add_region_reference(
            RegionReferenceEntry("cutflow", "cf", "ghost", False)
        )
        text = report.to_text()
        assert "REGION REFERENCES" in text
        assert "signal" in text
        assert "ghost" in text
        assert "UNKNOWN" in text

    def test_to_yaml_round_trip_preserves_region_references(self):
        report = ValidationReport(stage="yaml_rt")
        report.add_region_reference(
            RegionReferenceEntry("histogram", "mass", "presel", True)
        )
        restored = ValidationReport.from_dict(
            __import__("yaml").safe_load(report.to_yaml())
        )
        assert len(restored.region_references) == 1
        assert restored.region_references[0].referenced_region == "presel"


# ---------------------------------------------------------------------------
# NuisanceGroupCoverageEntry – severity field
# ---------------------------------------------------------------------------


class TestNuisanceGroupCoverageEntry:
    def test_default_severity_is_error(self):
        e = NuisanceGroupCoverageEntry(group_name="jes")
        assert e.severity == ReportSeverity.ERROR.value

    def test_complete_entry(self):
        e = NuisanceGroupCoverageEntry(
            group_name="jes",
            systematics=["JES"],
        )
        assert e.is_complete is True

    def test_incomplete_entry_with_not_found(self):
        e = NuisanceGroupCoverageEntry(
            group_name="jes",
            systematics=["JES"],
            not_found=["JES"],
        )
        assert e.is_complete is False

    def test_warn_severity_roundtrip(self):
        e = NuisanceGroupCoverageEntry(
            group_name="jes",
            systematics=["JES"],
            not_found=["JES"],
            severity="warn",
        )
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(e)
        d = r.to_dict()
        restored = ValidationReport.from_dict(d)
        assert restored.nuisance_group_coverage[0].severity == "warn"

    def test_from_dict_defaults_severity_to_error(self):
        """Old serialised entries without severity field default to error."""
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(group_name="g", not_found=["X"])
        )
        d = r.to_dict()
        # Remove the severity key to simulate old format
        d["nuisance_group_coverage"][0].pop("severity", None)
        restored = ValidationReport.from_dict(d)
        assert restored.nuisance_group_coverage[0].severity == ReportSeverity.ERROR.value


# ---------------------------------------------------------------------------
# ValidationReport – has_errors / has_warnings with severity
# ---------------------------------------------------------------------------


class TestHasErrorsWithSeverity:
    def test_incomplete_error_severity_causes_has_errors(self):
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jes",
                systematics=["JES"],
                not_found=["JES"],
                severity=ReportSeverity.ERROR.value,
            )
        )
        assert r.has_errors is True
        assert r.has_warnings is False

    def test_incomplete_warn_severity_does_not_cause_has_errors(self):
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jes",
                systematics=["JES"],
                not_found=["JES"],
                severity=ReportSeverity.WARNING.value,
            )
        )
        assert r.has_errors is False
        assert r.has_warnings is True

    def test_complete_entry_causes_neither(self):
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jes",
                systematics=["JES"],
                severity=ReportSeverity.ERROR.value,
            )
        )
        assert r.has_errors is False
        assert r.has_warnings is False


# ---------------------------------------------------------------------------
# RegionEntry – covered_by field
# ---------------------------------------------------------------------------


class TestRegionEntryCoveredBy:
    def test_default_covered_by_is_empty(self):
        e = RegionEntry(region_name="signal", filter_column="is_signal")
        assert e.covered_by == []

    def test_covered_by_roundtrip(self):
        e = RegionEntry(
            region_name="signal",
            filter_column="is_signal",
            covered_by=["histograms", "cutflow"],
        )
        r = ValidationReport(stage="x")
        r.add_region(e)
        d = r.to_dict()
        restored = ValidationReport.from_dict(d)
        assert restored.regions[0].covered_by == ["histograms", "cutflow"]

    def test_from_dict_defaults_covered_by_to_empty(self):
        """Old serialised entries without covered_by default to empty list."""
        r = ValidationReport(stage="x")
        r.add_region(RegionEntry("signal", "is_signal", covered_by=["histograms"]))
        d = r.to_dict()
        d["regions"][0].pop("covered_by", None)
        restored = ValidationReport.from_dict(d)
        assert restored.regions[0].covered_by == []

    def test_to_text_shows_output_coverage_column(self):
        r = ValidationReport(stage="x")
        r.add_region(
            RegionEntry(
                region_name="signal",
                filter_column="is_signal",
                covered_by=["histograms"],
            )
        )
        r.add_region(
            RegionEntry(
                region_name="control",
                filter_column="is_control",
                covered_by=[],
            )
        )
        text = r.to_text()
        assert "Output Coverage" in text
        assert "histograms" in text
        assert "(none)" in text


# ---------------------------------------------------------------------------
# to_text – nuisance group coverage severity column
# ---------------------------------------------------------------------------


class TestNuisanceCoverageTextOutput:
    def test_severity_shown_in_text_for_complete_entry(self):
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jes",
                group_type="shape",
                systematics=["JES"],
                severity=ReportSeverity.ERROR.value,
            )
        )
        text = r.to_text()
        assert "ERROR" in text

    def test_severity_shown_in_text_for_warn_entry(self):
        r = ValidationReport(stage="x")
        r.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name="jes",
                group_type="shape",
                systematics=["JES"],
                not_found=["JES"],
                severity=ReportSeverity.WARNING.value,
            )
        )
        text = r.to_text()
        assert "WARNING" in text


# ---------------------------------------------------------------------------
# generate_report_from_manifest – regions and nuisance groups
# ---------------------------------------------------------------------------


class TestGenerateReportFromManifestRegions:
    def test_regions_populated_from_manifest(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
                RegionDefinition(name="control", filter_column="is_control"),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert len(report.regions) == 2
        names = {e.region_name for e in report.regions}
        assert names == {"signal", "control"}

    def test_region_is_valid_when_well_formed(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert report.regions[0].is_valid is True
        assert report.regions[0].issues == []

    def test_region_invalid_when_filter_column_empty(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            regions=[
                RegionDefinition(name="bad_region", filter_column=""),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert report.regions[0].is_valid is False
        assert report.regions[0].issues  # non-empty

    def test_region_hierarchy_error_reported(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            regions=[
                RegionDefinition(name="child", filter_column="is_child", parent="nonexistent"),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert report.regions[0].is_valid is False

    def test_no_regions_when_manifest_has_none(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        report = generate_report_from_manifest(m, stage="skim")
        assert report.regions == []

    def test_region_covered_by_histogram_when_name_in_histogram_names(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(
                output_file="meta.root",
                histogram_names=["signal_pt", "signal_eta", "control_pt"],
            ),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
                RegionDefinition(name="control", filter_column="is_control"),
            ],
        )
        report = generate_report_from_manifest(m, stage="histogram")
        signal_entry = next(e for e in report.regions if e.region_name == "signal")
        control_entry = next(e for e in report.regions if e.region_name == "control")
        assert "histograms" in signal_entry.covered_by
        assert "histograms" in control_entry.covered_by

    def test_region_not_covered_when_name_absent_from_histograms(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(
                output_file="meta.root",
                histogram_names=["pt", "eta"],
            ),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
            ],
        )
        report = generate_report_from_manifest(m, stage="histogram")
        assert report.regions[0].covered_by == []

    def test_region_covered_by_cutflow_when_name_in_counter_keys(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            cutflow=CutflowSchema(
                output_file="meta.root",
                counter_keys=["signal_total", "signal_trigger", "control_baseline"],
            ),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
                RegionDefinition(name="control", filter_column="is_control"),
            ],
        )
        report = generate_report_from_manifest(m, stage="cutflow")
        signal_entry = next(e for e in report.regions if e.region_name == "signal")
        control_entry = next(e for e in report.regions if e.region_name == "control")
        assert "cutflow" in signal_entry.covered_by
        assert "cutflow" in control_entry.covered_by

    def test_region_covered_by_both_histogram_and_cutflow(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            histograms=HistogramSchema(
                output_file="meta.root",
                histogram_names=["signal_pt"],
            ),
            cutflow=CutflowSchema(
                output_file="meta.root",
                counter_keys=["signal_total"],
            ),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
            ],
        )
        report = generate_report_from_manifest(m, stage="all")
        covered = report.regions[0].covered_by
        assert "histograms" in covered
        assert "cutflow" in covered

    def test_region_to_text_includes_region_definitions_section(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            regions=[
                RegionDefinition(name="signal", filter_column="is_signal"),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        text = report.to_text()
        assert "REGION DEFINITIONS" in text
        assert "signal" in text


class TestGenerateReportFromManifestNuisanceGroups:
    def test_nuisance_groups_populated_from_manifest(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(
                    name="jet_energy",
                    group_type="shape",
                    systematics=["JES", "JER"],
                ),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert len(report.nuisance_group_coverage) == 1
        entry = report.nuisance_group_coverage[0]
        assert entry.group_name == "jet_energy"
        assert entry.group_type == "shape"
        assert set(entry.systematics) == {"JES", "JER"}

    def test_nuisance_group_coverage_stubs_have_no_missing(self):
        """Stubs from manifest have no coverage gap info (no columns available)."""
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(
                    name="jes",
                    group_type="shape",
                    systematics=["JES"],
                ),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        entry = report.nuisance_group_coverage[0]
        assert entry.missing_up == []
        assert entry.missing_down == []
        assert entry.not_found == []
        assert entry.is_complete is True

    def test_nuisance_group_processes_regions_preserved(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(
                    name="btag",
                    group_type="rate",
                    systematics=["btagSF"],
                    processes=["ttbar", "wjets"],
                    regions=["signal", "control"],
                    output_usage=["histogram"],
                ),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        entry = report.nuisance_group_coverage[0]
        assert entry.processes == ["ttbar", "wjets"]
        assert entry.regions == ["signal", "control"]
        assert entry.output_usage == ["histogram"]

    def test_no_nuisance_groups_when_manifest_has_none(self):
        m = OutputManifest(skim=SkimSchema(output_file="out.root"))
        report = generate_report_from_manifest(m, stage="skim")
        assert report.nuisance_group_coverage == []

    def test_multiple_nuisance_groups_all_added(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(name="jes", systematics=["JES"]),
                NuisanceGroupDefinition(name="jer", systematics=["JER"]),
                NuisanceGroupDefinition(name="btag", systematics=["bSF"]),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert len(report.nuisance_group_coverage) == 3
        names = {e.group_name for e in report.nuisance_group_coverage}
        assert names == {"jes", "jer", "btag"}

    def test_nuisance_group_to_text_includes_coverage_section(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(name="jes", systematics=["JES"]),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        text = report.to_text()
        assert "NUISANCE GROUP COVERAGE" in text
        assert "jes" in text

    def test_nuisance_group_severity_default_is_error(self):
        m = OutputManifest(
            skim=SkimSchema(output_file="out.root"),
            nuisance_groups=[
                NuisanceGroupDefinition(name="jes", systematics=["JES"]),
            ],
        )
        report = generate_report_from_manifest(m, stage="skim")
        assert report.nuisance_group_coverage[0].severity == ReportSeverity.ERROR.value


# ---------------------------------------------------------------------------
# VariationOrchestrator – severity propagated to NuisanceGroupCoverageEntry
# ---------------------------------------------------------------------------


class TestVariationOrchestratorSeverityPropagation:
    """Ensure build_validation_report sets severity field on coverage entries."""

    def test_error_severity_set_on_incomplete_entry(self):
        from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry
        from variation_orchestrator import VariationOrchestrator

        group = NuisanceGroup(
            name="jes",
            group_type="shape",
            systematics=["JES"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        registry = NuisanceGroupRegistry([group])
        orch = VariationOrchestrator(registry, missing_severity="error")
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=[],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        assert len(report.nuisance_group_coverage) == 1
        entry = report.nuisance_group_coverage[0]
        assert entry.severity == "error"
        assert report.has_errors is True

    def test_warn_severity_set_on_incomplete_entry(self):
        from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry
        from variation_orchestrator import VariationOrchestrator

        group = NuisanceGroup(
            name="jes",
            group_type="shape",
            systematics=["JES"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        registry = NuisanceGroupRegistry([group])
        orch = VariationOrchestrator(registry, missing_severity="warn")
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=[],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
            severity="warn",
        )
        assert len(report.nuisance_group_coverage) == 1
        entry = report.nuisance_group_coverage[0]
        assert entry.severity == "warn"
        assert report.has_errors is False
        assert report.has_warnings is True

    def test_complete_coverage_entry_no_error_regardless_of_severity(self):
        from nuisance_groups import NuisanceGroup, NuisanceGroupRegistry
        from variation_orchestrator import VariationOrchestrator

        group = NuisanceGroup(
            name="jes",
            group_type="shape",
            systematics=["JES"],
            processes=["signal"],
            regions=["sr"],
            output_usage=["histogram"],
        )
        registry = NuisanceGroupRegistry([group])
        orch = VariationOrchestrator(registry, missing_severity="error")
        report = ValidationReport(stage="test")
        orch.build_validation_report(
            report=report,
            available_columns=["JESUp", "JESDown"],
            processes=["signal"],
            regions=["sr"],
            output_usage="histogram",
        )
        assert report.has_errors is False
        assert report.has_warnings is False
