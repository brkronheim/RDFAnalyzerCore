"""
Validation and audit report generation for RDFAnalyzerCore production stages.

This module provides structured validation reports that summarize the key
diagnostics for each production stage:

- **Event counts** – total and selected event tallies per sample and stage.
- **Cutflows** – ordered selection cuts with per-step event counts and
  efficiencies.
- **Missing branches** – branches declared in the output schema but absent
  from the actual output.
- **Configuration mismatches** – discrepancies between expected and recorded
  configuration values.
- **Systematic coverage** – which systematic variations are present and
  whether they are complete (both up and down shifts).
- **Weight summaries** – sum-of-weights statistics per sample including
  negative-weight diagnostics.
- **Output integrity** – file existence, size, and any structural issues for
  every declared output artifact.

Reports can be serialised to both machine-readable (JSON and YAML) and
human-readable (plain text) formats.

Usage
-----
Build a report programmatically::

    from validation_report import ValidationReport, EventCountEntry

    report = ValidationReport(stage="preselection")
    report.add_event_count(EventCountEntry(
        sample="ttbar", total_events=1_000_000, selected_events=123_456
    ))
    report.add_warning("Sum of weights not recorded for sample 'wjets'")

    # Machine-readable
    report.save_yaml("reports/preselection.report.yaml")
    report.save_json("reports/preselection.report.json")

    # Human-readable
    report.save_text("reports/preselection.report.txt")
    print(report.to_text())

Generate a report from an :class:`~output_schema.OutputManifest`::

    from output_schema import OutputManifest
    from validation_report import generate_report_from_manifest

    manifest = OutputManifest.load_yaml("job_42/output_manifest.yaml")
    report = generate_report_from_manifest(manifest, stage="skim")
    report.save_yaml("job_42/validation_report.yaml")
"""

from __future__ import annotations

import argparse
import datetime
import enum
import json
import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional

import yaml


# ---------------------------------------------------------------------------
# Severity enum
# ---------------------------------------------------------------------------


class ReportSeverity(str, enum.Enum):
    """Severity level for report findings.

    Attributes
    ----------
    INFO:
        Informational note; no action required.
    WARNING:
        Non-critical issue that should be reviewed.
    ERROR:
        Critical issue that must be resolved before the outputs can be used.
    """

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


# ---------------------------------------------------------------------------
# Per-section entry dataclasses
# ---------------------------------------------------------------------------


@dataclass
class EventCountEntry:
    """Event count record for a single sample at a single production stage.

    Attributes
    ----------
    sample : str
        Dataset / sample identifier.
    stage : str
        Production stage name (e.g. ``"preselection"``, ``"skim"``).
    total_events : int
        Total number of input events before any selection.
    selected_events : int or None
        Number of events surviving the stage selection.  ``None`` when the
        count was not recorded.
    efficiency : float or None
        Fraction ``selected_events / total_events``.  Computed automatically
        when both counts are available and ``efficiency`` is ``None``.
    """

    sample: str
    stage: str
    total_events: int
    selected_events: Optional[int] = None
    efficiency: Optional[float] = None

    def __post_init__(self) -> None:
        if (
            self.efficiency is None
            and self.selected_events is not None
            and self.total_events > 0
        ):
            self.efficiency = self.selected_events / self.total_events


@dataclass
class CutflowEntry:
    """A single step in a cutflow table.

    Attributes
    ----------
    cut_name : str
        Human-readable name of the selection cut.
    events_passed : int
        Number of events that passed **this** cut (absolute count).
    events_cumulative : int or None
        Cumulative events remaining after all cuts up to and including this
        one.  When ``None`` the cumulative count was not recorded.
    relative_efficiency : float or None
        Fraction passing this cut relative to the previous cut.
    cumulative_efficiency : float or None
        Fraction passing all cuts up to and including this cut relative to
        the first cut.
    """

    cut_name: str
    events_passed: int
    events_cumulative: Optional[int] = None
    relative_efficiency: Optional[float] = None
    cumulative_efficiency: Optional[float] = None


@dataclass
class MissingBranchEntry:
    """Record of branches expected in a schema but absent from the output.

    Attributes
    ----------
    artifact_role : str
        The schema role (e.g. ``"skim"``, ``"intermediate_artifacts[0]"``).
    expected_branches : list[str]
        Complete list of branches declared in the schema.
    missing_branches : list[str]
        Subset of ``expected_branches`` not found in the output.
    """

    artifact_role: str
    expected_branches: List[str]
    missing_branches: List[str]

    @property
    def is_complete(self) -> bool:
        """Return ``True`` when no branches are missing."""
        return len(self.missing_branches) == 0


@dataclass
class ConfigMismatchEntry:
    """A single configuration key whose value differs from what was expected.

    Attributes
    ----------
    key : str
        Configuration parameter name.
    expected : any
        The value declared in the schema or reference configuration.
    actual : any
        The value found in the running job or output artifact.
    severity : ReportSeverity
        Severity of this mismatch.
    """

    key: str
    expected: Any
    actual: Any
    severity: ReportSeverity = ReportSeverity.ERROR

    def to_dict(self) -> Dict[str, Any]:
        return {
            "key": self.key,
            "expected": self.expected,
            "actual": self.actual,
            "severity": self.severity.value,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "ConfigMismatchEntry":
        return cls(
            key=d["key"],
            expected=d.get("expected"),
            actual=d.get("actual"),
            severity=ReportSeverity(d.get("severity", ReportSeverity.ERROR.value)),
        )


@dataclass
class SystematicEntry:
    """Coverage record for a single systematic variation.

    Attributes
    ----------
    systematic_name : str
        Name of the systematic variation (e.g. ``"JES"``, ``"PU"``).
    has_up : bool
        Whether the up-shift variation is present in the output.
    has_down : bool
        Whether the down-shift variation is present in the output.
    extra_variations : list[str]
        Any additional variation tags beyond ``"up"`` and ``"down"``.
    """

    systematic_name: str
    has_up: bool = False
    has_down: bool = False
    extra_variations: List[str] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        """Return ``True`` when both up and down variations are present."""
        return self.has_up and self.has_down


@dataclass
class WeightSummaryEntry:
    """Weight statistics for a single sample.

    Attributes
    ----------
    sample : str
        Dataset / sample identifier.
    sum_weights : float
        Sum of event weights.
    sum_weights_squared : float or None
        Sum of squared event weights (useful for uncertainty estimates).
    n_events : int or None
        Total number of events used to compute the weight sum.
    n_negative_weights : int or None
        Number of events with a negative weight.
    min_weight : float or None
        Minimum event weight observed.
    max_weight : float or None
        Maximum event weight observed.
    """

    sample: str
    sum_weights: float
    sum_weights_squared: Optional[float] = None
    n_events: Optional[int] = None
    n_negative_weights: Optional[int] = None
    min_weight: Optional[float] = None
    max_weight: Optional[float] = None


@dataclass
class OutputIntegrityEntry:
    """Integrity record for a single declared output artifact.

    Attributes
    ----------
    artifact_role : str
        The schema role (e.g. ``"skim"``, ``"cutflow"``).
    path : str
        Expected filesystem path of the artifact.
    exists : bool or None
        Whether the file was found on disk at ``path``.  ``None`` means the
        file was not checked (i.e. ``check_files=False`` was used).
    size_bytes : int or None
        File size in bytes.  ``None`` when the file does not exist, was not
        checked, or the size could not be determined.
    issues : list[str]
        Structural or format issues discovered during the integrity check.
    """

    artifact_role: str
    path: str
    exists: Optional[bool] = None
    size_bytes: Optional[int] = None
    issues: List[str] = field(default_factory=list)

    @property
    def is_ok(self) -> bool:
        """Return ``True`` when the file exists and has no issues.

        Returns ``True`` (not an error) when ``exists`` is ``None`` (the file
        was not checked) and there are no recorded issues.
        """
        if self.exists is False:
            return False
        return len(self.issues) == 0


@dataclass
class RegionEntry:
    """Validation record for a single declared analysis region.

    Attributes
    ----------
    region_name : str
        The unique name of the region (e.g. ``"signal"``,
        ``"control_wjets"``).
    filter_column : str
        Name of the boolean dataframe column that selects events in this
        region.
    parent : str
        Name of the parent region, or empty string for a root region.
    is_valid : bool
        ``True`` when the region definition passed all validation checks.
    issues : list[str]
        Validation error or warning strings for this region.
    covered_by : list[str]
        Artifact roles (e.g. ``"histograms"``, ``"cutflow"``) that reference
        this region in the output manifest.  An empty list means no output
        artifact was found that covers this region.
    """

    region_name: str
    filter_column: str
    parent: str = ""
    is_valid: bool = True
    issues: List[str] = field(default_factory=list)
    covered_by: List[str] = field(default_factory=list)


@dataclass
class NuisanceGroupCoverageEntry:
    """Coverage record for a single nuisance group.

    Attributes
    ----------
    group_name : str
        Name of the nuisance group (e.g. ``"jet_energy_scale"``).
    group_type : str
        Type of the group (``"shape"``, ``"rate"``, etc.).
    systematics : list[str]
        Base names of the systematic variations belonging to this group.
    processes : list[str]
        Physics processes this group applies to.  An empty list means all.
    regions : list[str]
        Analysis regions this group applies to.  An empty list means all.
    output_usage : list[str]
        Downstream tools that consume this group.  An empty list means all.
    missing_up : list[str]
        Systematics missing the Up variation.
    missing_down : list[str]
        Systematics missing the Down variation.
    not_found : list[str]
        Systematics not present in the output at all.
    severity : str
        Severity level for coverage gaps: ``"error"`` (default) causes
        :attr:`~ValidationReport.has_errors` to be ``True`` when coverage is
        incomplete; ``"warn"`` records the gap as a warning only.
    """

    group_name: str
    group_type: str = "shape"
    systematics: List[str] = field(default_factory=list)
    processes: List[str] = field(default_factory=list)
    regions: List[str] = field(default_factory=list)
    output_usage: List[str] = field(default_factory=list)
    missing_up: List[str] = field(default_factory=list)
    missing_down: List[str] = field(default_factory=list)
    not_found: List[str] = field(default_factory=list)
    severity: str = ReportSeverity.ERROR.value

    @property
    def is_complete(self) -> bool:
        """Return ``True`` when all systematics have both up and down shifts."""
        return not self.missing_up and not self.missing_down and not self.not_found


@dataclass
class RegionReferenceEntry:
    """Validation record for a region reference in a histogram or cutflow config.

    Histogram and cutflow configurations can name analysis regions explicitly
    (e.g. via ``channelRegions`` or by binding to a :class:`RegionManager`).
    This entry tracks whether each referenced region name is actually declared
    in the analysis.

    Attributes
    ----------
    config_type : str
        Kind of config that contains the reference: ``"histogram"`` or
        ``"cutflow"``.
    config_name : str
        Name of the histogram or cutflow entry that references the region.
    referenced_region : str
        The region name that was referenced.
    is_known : bool
        ``True`` when the region was found in the set of declared regions.
    """

    config_type: str
    config_name: str
    referenced_region: str
    is_known: bool = True

    @property
    def is_valid(self) -> bool:
        """Alias for ``is_known``; follows the naming convention of other entries."""
        return self.is_known


# ---------------------------------------------------------------------------
# ValidationReport
# ---------------------------------------------------------------------------

#: Schema version for :class:`ValidationReport` serialised output.
VALIDATION_REPORT_VERSION: int = 1


class ValidationReport:
    """Structured validation and audit report for a single production stage.

    A :class:`ValidationReport` aggregates all diagnostics produced during
    validation of one production stage and can be serialised to both
    machine-readable (JSON / YAML) and human-readable (plain text) formats.

    Parameters
    ----------
    stage : str
        Name of the production stage this report covers
        (e.g. ``"preselection"``, ``"skim"``, ``"histogram"``).
    timestamp : str or None
        UTC timestamp (ISO 8601) of when the report was created.  Defaults
        to the current time when ``None``.

    Attributes
    ----------
    event_counts : list[EventCountEntry]
        Event count records per sample.
    cutflow : list[CutflowEntry]
        Ordered cutflow steps.
    missing_branches : list[MissingBranchEntry]
        Missing-branch records per artifact role.
    config_mismatches : list[ConfigMismatchEntry]
        Configuration mismatch records.
    systematics : list[SystematicEntry]
        Systematic coverage records.
    weight_summaries : list[WeightSummaryEntry]
        Weight statistic records per sample.
    output_integrity : list[OutputIntegrityEntry]
        Integrity records per output artifact.
    regions : list[RegionEntry]
        Validation records for declared analysis regions.
    nuisance_group_coverage : list[NuisanceGroupCoverageEntry]
        Coverage validation records for declared nuisance groups.
    region_references : list[RegionReferenceEntry]
        Validation records for region names referenced in histogram and
        cutflow configurations.  Entries with ``is_known=False`` indicate
        references to regions that were never declared.
    errors : list[str]
        Free-form error messages not captured by a specific section.
    warnings : list[str]
        Free-form warning messages not captured by a specific section.
    """

    def __init__(
        self,
        stage: str,
        timestamp: Optional[str] = None,
    ) -> None:
        self.stage: str = stage
        self.timestamp: str = (
            timestamp or datetime.datetime.now(tz=datetime.timezone.utc).isoformat(timespec="seconds")
        )
        self.report_version: int = VALIDATION_REPORT_VERSION

        self.event_counts: List[EventCountEntry] = []
        self.cutflow: List[CutflowEntry] = []
        self.missing_branches: List[MissingBranchEntry] = []
        self.config_mismatches: List[ConfigMismatchEntry] = []
        self.systematics: List[SystematicEntry] = []
        self.weight_summaries: List[WeightSummaryEntry] = []
        self.output_integrity: List[OutputIntegrityEntry] = []
        self.regions: List[RegionEntry] = []
        self.nuisance_group_coverage: List[NuisanceGroupCoverageEntry] = []
        self.region_references: List[RegionReferenceEntry] = []
        self.errors: List[str] = []
        self.warnings: List[str] = []

    # ------------------------------------------------------------------ adders

    def add_event_count(self, entry: EventCountEntry) -> None:
        """Append an :class:`EventCountEntry` to the report."""
        self.event_counts.append(entry)

    def add_cutflow_step(self, entry: CutflowEntry) -> None:
        """Append a :class:`CutflowEntry` to the report."""
        self.cutflow.append(entry)

    def add_missing_branches(self, entry: MissingBranchEntry) -> None:
        """Append a :class:`MissingBranchEntry` to the report."""
        self.missing_branches.append(entry)

    def add_config_mismatch(self, entry: ConfigMismatchEntry) -> None:
        """Append a :class:`ConfigMismatchEntry` to the report."""
        self.config_mismatches.append(entry)

    def add_systematic(self, entry: SystematicEntry) -> None:
        """Append a :class:`SystematicEntry` to the report."""
        self.systematics.append(entry)

    def add_weight_summary(self, entry: WeightSummaryEntry) -> None:
        """Append a :class:`WeightSummaryEntry` to the report."""
        self.weight_summaries.append(entry)

    def add_output_integrity(self, entry: OutputIntegrityEntry) -> None:
        """Append an :class:`OutputIntegrityEntry` to the report."""
        self.output_integrity.append(entry)

    def add_region(self, entry: RegionEntry) -> None:
        """Append a :class:`RegionEntry` to the report."""
        self.regions.append(entry)

    def add_nuisance_group_coverage(self, entry: "NuisanceGroupCoverageEntry") -> None:
        """Append a :class:`NuisanceGroupCoverageEntry` to the report."""
        self.nuisance_group_coverage.append(entry)

    def add_region_reference(self, entry: "RegionReferenceEntry") -> None:
        """Append a :class:`RegionReferenceEntry` to the report."""
        self.region_references.append(entry)

    def add_error(self, message: str) -> None:
        """Append a free-form error message."""
        self.errors.append(message)

    def add_warning(self, message: str) -> None:
        """Append a free-form warning message."""
        self.warnings.append(message)

    # ------------------------------------------------------------------ summary

    @property
    def has_errors(self) -> bool:
        """Return ``True`` when the report contains any error-level findings."""
        if self.errors:
            return True
        if any(e.severity == ReportSeverity.ERROR for e in self.config_mismatches):
            return True
        if any(not e.is_ok for e in self.output_integrity):
            return True
        if any(e.missing_branches for e in self.missing_branches):
            return True
        if any(not e.is_valid for e in self.regions):
            return True
        if any(
            not e.is_complete and e.severity == ReportSeverity.ERROR.value
            for e in self.nuisance_group_coverage
        ):
            return True
        if any(not e.is_known for e in self.region_references):
            return True
        return False

    @property
    def has_warnings(self) -> bool:
        """Return ``True`` when the report contains any warning-level findings."""
        if self.warnings:
            return True
        if any(e.severity == ReportSeverity.WARNING for e in self.config_mismatches):
            return True
        if any(not e.is_complete for e in self.systematics):
            return True
        if any(
            not e.is_complete and e.severity != ReportSeverity.ERROR.value
            for e in self.nuisance_group_coverage
        ):
            return True
        return False

    # ------------------------------------------------------------------ serialisation

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the report to a plain Python dict.

        The returned dict is JSON- and YAML-serialisable and can be used as
        the machine-readable representation of the report.

        Returns
        -------
        dict
            Full report as a nested dictionary.
        """
        return {
            "report_version": self.report_version,
            "stage": self.stage,
            "timestamp": self.timestamp,
            "summary": {
                "has_errors": self.has_errors,
                "has_warnings": self.has_warnings,
                "n_event_count_entries": len(self.event_counts),
                "n_cutflow_steps": len(self.cutflow),
                "n_missing_branch_roles": len(self.missing_branches),
                "n_config_mismatches": len(self.config_mismatches),
                "n_systematics": len(self.systematics),
                "n_weight_summaries": len(self.weight_summaries),
                "n_output_integrity_entries": len(self.output_integrity),
                "n_regions": len(self.regions),
                "n_nuisance_group_coverage": len(self.nuisance_group_coverage),
                "n_region_references": len(self.region_references),
                "n_errors": len(self.errors),
                "n_warnings": len(self.warnings),
            },
            "event_counts": [asdict(e) for e in self.event_counts],
            "cutflow": [asdict(e) for e in self.cutflow],
            "missing_branches": [asdict(e) for e in self.missing_branches],
            "config_mismatches": [e.to_dict() for e in self.config_mismatches],
            "systematics": [asdict(e) for e in self.systematics],
            "weight_summaries": [asdict(e) for e in self.weight_summaries],
            "output_integrity": [asdict(e) for e in self.output_integrity],
            "regions": [asdict(e) for e in self.regions],
            "nuisance_group_coverage": [asdict(e) for e in self.nuisance_group_coverage],
            "region_references": [asdict(e) for e in self.region_references],
            "errors": list(self.errors),
            "warnings": list(self.warnings),
        }

    def to_yaml(self) -> str:
        """Return the report serialised as a YAML string.

        Returns
        -------
        str
            YAML representation of the full report.
        """
        return yaml.dump(
            self.to_dict(),
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )

    def to_json(self, indent: int = 2) -> str:
        """Return the report serialised as a JSON string.

        Parameters
        ----------
        indent : int
            Number of spaces used for JSON indentation.  Defaults to ``2``.

        Returns
        -------
        str
            JSON representation of the full report.
        """
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)

    def to_text(self) -> str:
        """Return a human-readable plain-text representation of the report.

        The text output uses simple ASCII formatting with section headers
        and tabular layouts where appropriate.  It is intended for direct
        terminal display or inclusion in log files.

        Returns
        -------
        str
            Multi-line human-readable report string.
        """
        lines: List[str] = []

        def _header(title: str) -> None:
            lines.append("")
            lines.append("=" * 60)
            lines.append(f"  {title}")
            lines.append("=" * 60)

        def _subheader(title: str) -> None:
            lines.append("")
            lines.append(f"  -- {title} --")

        # Title block
        lines.append("=" * 60)
        lines.append("  VALIDATION REPORT")
        lines.append(f"  Stage     : {self.stage}")
        lines.append(f"  Timestamp : {self.timestamp}")
        status = "FAILED" if self.has_errors else ("WARNINGS" if self.has_warnings else "OK")
        lines.append(f"  Status    : {status}")
        lines.append("=" * 60)

        # Errors & Warnings
        if self.errors:
            _header("ERRORS")
            for e in self.errors:
                lines.append(f"  [ERROR]   {e}")

        if self.warnings:
            _header("WARNINGS")
            for w in self.warnings:
                lines.append(f"  [WARNING] {w}")

        # Event counts
        if self.event_counts:
            _header("EVENT COUNTS")
            col_w = (18, 16, 14, 14, 10)
            header = (
                f"  {'Sample':<{col_w[0]}}"
                f"{'Stage':<{col_w[1]}}"
                f"{'Total':>{col_w[2]}}"
                f"{'Selected':>{col_w[3]}}"
                f"{'Eff.':>{col_w[4]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * (sum(col_w)))
            for e in self.event_counts:
                eff_str = (
                    f"{e.efficiency:.3%}" if e.efficiency is not None else "n/a"
                )
                sel_str = str(e.selected_events) if e.selected_events is not None else "n/a"
                lines.append(
                    f"  {e.sample:<{col_w[0]}}"
                    f"{e.stage:<{col_w[1]}}"
                    f"{e.total_events:>{col_w[2]}}"
                    f"{sel_str:>{col_w[3]}}"
                    f"{eff_str:>{col_w[4]}}"
                )

        # Cutflow
        if self.cutflow:
            _header("CUTFLOW")
            col_w = (24, 14, 14, 10, 10)
            header = (
                f"  {'Cut':<{col_w[0]}}"
                f"{'Passed':>{col_w[1]}}"
                f"{'Cumulative':>{col_w[2]}}"
                f"{'Rel.Eff':>{col_w[3]}}"
                f"{'Cum.Eff':>{col_w[4]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * (sum(col_w)))
            for e in self.cutflow:
                cum_str = str(e.events_cumulative) if e.events_cumulative is not None else "n/a"
                rel_str = f"{e.relative_efficiency:.3%}" if e.relative_efficiency is not None else "n/a"
                cum_eff_str = f"{e.cumulative_efficiency:.3%}" if e.cumulative_efficiency is not None else "n/a"
                lines.append(
                    f"  {e.cut_name:<{col_w[0]}}"
                    f"{e.events_passed:>{col_w[1]}}"
                    f"{cum_str:>{col_w[2]}}"
                    f"{rel_str:>{col_w[3]}}"
                    f"{cum_eff_str:>{col_w[4]}}"
                )

        # Missing branches
        if self.missing_branches:
            _header("MISSING BRANCHES")
            for e in self.missing_branches:
                if e.missing_branches:
                    lines.append(f"  Role: {e.artifact_role}")
                    lines.append(
                        f"    Expected {len(e.expected_branches)} branch(es), "
                        f"missing {len(e.missing_branches)}:"
                    )
                    for b in e.missing_branches:
                        lines.append(f"      - {b}")
                else:
                    lines.append(f"  Role: {e.artifact_role}  [all branches present]")

        # Config mismatches
        if self.config_mismatches:
            _header("CONFIGURATION MISMATCHES")
            for e in self.config_mismatches:
                lines.append(
                    f"  [{e.severity.value.upper()}] {e.key}: "
                    f"expected={e.expected!r}, actual={e.actual!r}"
                )

        # Systematic coverage
        if self.systematics:
            _header("SYSTEMATIC COVERAGE")
            col_w = (24, 8, 8, 12)
            header = (
                f"  {'Systematic':<{col_w[0]}}"
                f"{'Up':>{col_w[1]}}"
                f"{'Down':>{col_w[2]}}"
                f"{'Complete':>{col_w[3]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * (sum(col_w)))
            for e in self.systematics:
                up_str = "yes" if e.has_up else "no"
                down_str = "yes" if e.has_down else "no"
                complete_str = "yes" if e.is_complete else "MISSING"
                lines.append(
                    f"  {e.systematic_name:<{col_w[0]}}"
                    f"{up_str:>{col_w[1]}}"
                    f"{down_str:>{col_w[2]}}"
                    f"{complete_str:>{col_w[3]}}"
                )

        # Weight summaries
        if self.weight_summaries:
            _header("WEIGHT SUMMARIES")
            col_w = (20, 16, 10, 10, 14)
            header = (
                f"  {'Sample':<{col_w[0]}}"
                f"{'Sum W':>{col_w[1]}}"
                f"{'N events':>{col_w[2]}}"
                f"{'Neg. W':>{col_w[3]}}"
                f"{'Min / Max W':>{col_w[4]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * (sum(col_w)))
            for e in self.weight_summaries:
                n_str = str(e.n_events) if e.n_events is not None else "n/a"
                neg_str = str(e.n_negative_weights) if e.n_negative_weights is not None else "n/a"
                minmax_str = (
                    f"{e.min_weight:.3g}/{e.max_weight:.3g}"
                    if e.min_weight is not None and e.max_weight is not None
                    else "n/a"
                )
                lines.append(
                    f"  {e.sample:<{col_w[0]}}"
                    f"{e.sum_weights:>{col_w[1]}.6g}"
                    f"{n_str:>{col_w[2]}}"
                    f"{neg_str:>{col_w[3]}}"
                    f"{minmax_str:>{col_w[4]}}"
                )

        # Output integrity
        if self.output_integrity:
            _header("OUTPUT INTEGRITY")
            for e in self.output_integrity:
                status_str = "OK" if e.is_ok else "FAIL"
                size_str = (
                    f"{e.size_bytes:,} B" if e.size_bytes is not None else "n/a"
                )
                lines.append(
                    f"  [{status_str}] {e.artifact_role}: {e.path} ({size_str})"
                )
                for issue in e.issues:
                    lines.append(f"          issue: {issue}")

        # Region definitions
        if self.regions:
            _header("REGION DEFINITIONS")
            col_w = (20, 24, 16, 10, 20)
            header = (
                f"  {'Region':<{col_w[0]}}"
                f"{'Filter Column':<{col_w[1]}}"
                f"{'Parent':<{col_w[2]}}"
                f"{'Valid':>{col_w[3]}}"
                f"  {'Output Coverage':<{col_w[4]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * sum(col_w))
            for e in self.regions:
                valid_str = "yes" if e.is_valid else "FAIL"
                parent_str = e.parent if e.parent else "(root)"
                cov_str = ", ".join(sorted(e.covered_by)) if e.covered_by else "(none)"
                lines.append(
                    f"  {e.region_name:<{col_w[0]}}"
                    f"{e.filter_column:<{col_w[1]}}"
                    f"{parent_str:<{col_w[2]}}"
                    f"{valid_str:>{col_w[3]}}"
                    f"  {cov_str:<{col_w[4]}}"
                )
                for issue in e.issues:
                    lines.append(f"      issue: {issue}")

        # Nuisance group coverage
        if self.nuisance_group_coverage:
            _header("NUISANCE GROUP COVERAGE")
            col_w = (24, 14, 8, 10)
            header = (
                f"  {'Group':<{col_w[0]}}"
                f"{'Type':<{col_w[1]}}"
                f"{'Complete':>{col_w[2]}}"
                f"  {'Severity':<{col_w[3]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * sum(col_w))
            for e in self.nuisance_group_coverage:
                complete_str = "yes" if e.is_complete else "FAIL"
                sev_str = e.severity.upper()
                lines.append(
                    f"  {e.group_name:<{col_w[0]}}"
                    f"{e.group_type:<{col_w[1]}}"
                    f"{complete_str:>{col_w[2]}}"
                    f"  {sev_str:<{col_w[3]}}"
                )
                for syst in e.not_found:
                    lines.append(f"      not found:    {syst}")
                for syst in e.missing_up:
                    lines.append(f"      missing Up:   {syst}")
                for syst in e.missing_down:
                    lines.append(f"      missing Down: {syst}")

        # Region references
        if self.region_references:
            _header("REGION REFERENCES")
            col_w = (16, 24, 24, 10)
            header = (
                f"  {'Type':<{col_w[0]}}"
                f"{'Config Name':<{col_w[1]}}"
                f"{'Referenced Region':<{col_w[2]}}"
                f"{'Known':>{col_w[3]}}"
            )
            lines.append(header)
            lines.append("  " + "-" * sum(col_w))
            for e in self.region_references:
                known_str = "yes" if e.is_known else "UNKNOWN"
                lines.append(
                    f"  {e.config_type:<{col_w[0]}}"
                    f"{e.config_name:<{col_w[1]}}"
                    f"{e.referenced_region:<{col_w[2]}}"
                    f"{known_str:>{col_w[3]}}"
                )

        lines.append("")
        lines.append("=" * 60)
        lines.append("  END OF REPORT")
        lines.append("=" * 60)

        return "\n".join(lines)

    # ------------------------------------------------------------------ file I/O

    def save_yaml(self, path: str) -> None:
        """Save the machine-readable report to a YAML file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_yaml())

    def save_json(self, path: str) -> None:
        """Save the machine-readable report to a JSON file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_json())

    def save_text(self, path: str) -> None:
        """Save the human-readable report to a plain-text file.

        Parameters
        ----------
        path : str
            Destination file path (created or overwritten).
        """
        _ensure_dir(path)
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(self.to_text())

    # ------------------------------------------------------------------ deserialisation

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ValidationReport":
        """Deserialise a :class:`ValidationReport` from a plain dict.

        Parameters
        ----------
        data : dict
            Dictionary as produced by :meth:`to_dict`.

        Returns
        -------
        ValidationReport
        """
        report = cls(
            stage=data.get("stage", "unknown"),
            timestamp=data.get("timestamp"),
        )
        report.report_version = data.get("report_version", VALIDATION_REPORT_VERSION)

        for raw in data.get("event_counts", []):
            report.event_counts.append(
                EventCountEntry(
                    sample=raw["sample"],
                    stage=raw["stage"],
                    total_events=raw["total_events"],
                    selected_events=raw.get("selected_events"),
                    efficiency=raw.get("efficiency"),
                )
            )

        for raw in data.get("cutflow", []):
            report.cutflow.append(
                CutflowEntry(
                    cut_name=raw["cut_name"],
                    events_passed=raw["events_passed"],
                    events_cumulative=raw.get("events_cumulative"),
                    relative_efficiency=raw.get("relative_efficiency"),
                    cumulative_efficiency=raw.get("cumulative_efficiency"),
                )
            )

        for raw in data.get("missing_branches", []):
            report.missing_branches.append(
                MissingBranchEntry(
                    artifact_role=raw["artifact_role"],
                    expected_branches=list(raw.get("expected_branches", [])),
                    missing_branches=list(raw.get("missing_branches", [])),
                )
            )

        for raw in data.get("config_mismatches", []):
            report.config_mismatches.append(ConfigMismatchEntry.from_dict(raw))

        for raw in data.get("systematics", []):
            report.systematics.append(
                SystematicEntry(
                    systematic_name=raw["systematic_name"],
                    has_up=raw.get("has_up", False),
                    has_down=raw.get("has_down", False),
                    extra_variations=list(raw.get("extra_variations", [])),
                )
            )

        for raw in data.get("weight_summaries", []):
            report.weight_summaries.append(
                WeightSummaryEntry(
                    sample=raw["sample"],
                    sum_weights=raw["sum_weights"],
                    sum_weights_squared=raw.get("sum_weights_squared"),
                    n_events=raw.get("n_events"),
                    n_negative_weights=raw.get("n_negative_weights"),
                    min_weight=raw.get("min_weight"),
                    max_weight=raw.get("max_weight"),
                )
            )

        for raw in data.get("output_integrity", []):
            report.output_integrity.append(
                OutputIntegrityEntry(
                    artifact_role=raw["artifact_role"],
                    path=raw["path"],
                    exists=raw.get("exists"),
                    size_bytes=raw.get("size_bytes"),
                    issues=list(raw.get("issues", [])),
                )
            )

        for raw in data.get("regions", []):
            report.regions.append(
                RegionEntry(
                    region_name=raw["region_name"],
                    filter_column=raw.get("filter_column", ""),
                    parent=raw.get("parent", ""),
                    is_valid=raw.get("is_valid", True),
                    issues=list(raw.get("issues", [])),
                    covered_by=list(raw.get("covered_by", [])),
                )
            )

        for raw in data.get("nuisance_group_coverage", []):
            report.nuisance_group_coverage.append(
                NuisanceGroupCoverageEntry(
                    group_name=raw["group_name"],
                    group_type=raw.get("group_type", "shape"),
                    systematics=list(raw.get("systematics", [])),
                    processes=list(raw.get("processes", [])),
                    regions=list(raw.get("regions", [])),
                    output_usage=list(raw.get("output_usage", [])),
                    missing_up=list(raw.get("missing_up", [])),
                    missing_down=list(raw.get("missing_down", [])),
                    not_found=list(raw.get("not_found", [])),
                    severity=raw.get("severity", ReportSeverity.ERROR.value),
                )
            )

        for raw in data.get("region_references", []):
            report.region_references.append(
                RegionReferenceEntry(
                    config_type=raw["config_type"],
                    config_name=raw["config_name"],
                    referenced_region=raw["referenced_region"],
                    is_known=raw.get("is_known", True),
                )
            )

        report.errors = list(data.get("errors", []))
        report.warnings = list(data.get("warnings", []))
        return report

    @classmethod
    def load_yaml(cls, path: str) -> "ValidationReport":
        """Load a :class:`ValidationReport` from a YAML file.

        Parameters
        ----------
        path : str
            Path to the YAML report file produced by :meth:`save_yaml`.

        Returns
        -------
        ValidationReport
        """
        with open(path, encoding="utf-8") as fh:
            raw = yaml.safe_load(fh)
        if not isinstance(raw, dict):
            raise ValueError(
                f"Validation report file '{path}' must be a YAML mapping."
            )
        return cls.from_dict(raw)

    @classmethod
    def load_json(cls, path: str) -> "ValidationReport":
        """Load a :class:`ValidationReport` from a JSON file.

        Parameters
        ----------
        path : str
            Path to the JSON report file produced by :meth:`save_json`.

        Returns
        -------
        ValidationReport
        """
        with open(path, encoding="utf-8") as fh:
            raw = json.load(fh)
        if not isinstance(raw, dict):
            raise ValueError(
                f"Validation report file '{path}' must be a JSON object."
            )
        return cls.from_dict(raw)

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"ValidationReport(stage={self.stage!r}, "
            f"errors={len(self.errors)}, warnings={len(self.warnings)})"
        )


# ---------------------------------------------------------------------------
# Helper: validate region references
# ---------------------------------------------------------------------------


def validate_region_references(
    report: "ValidationReport",
    known_regions: List[str],
    referenced: List[Dict[str, str]],
) -> None:
    """Populate *report* with :class:`RegionReferenceEntry` records.

    For each ``(config_type, config_name, region_name)`` triple in *referenced*
    check whether *region_name* is present in *known_regions* and append a
    :class:`RegionReferenceEntry` to *report*.

    Parameters
    ----------
    report : ValidationReport
        Report to populate.
    known_regions : list[str]
        Region names that have been declared (e.g. from a RegionManager).
    referenced : list[dict]
        Each dict must have keys ``"config_type"`` (``"histogram"`` or
        ``"cutflow"``), ``"config_name"``, and ``"region"``.

    Example
    -------
    .. code-block:: python

        validate_region_references(
            report,
            known_regions=["presel", "signal", "control"],
            referenced=[
                {"config_type": "histogram", "config_name": "pt",
                 "region": "signal"},
                {"config_type": "histogram", "config_name": "pt",
                 "region": "unknown_region"},   # will be flagged as ERROR
            ],
        )
    """
    known_set = set(known_regions)
    for item in referenced:
        region_name = item["region"]
        report.add_region_reference(
            RegionReferenceEntry(
                config_type=item["config_type"],
                config_name=item["config_name"],
                referenced_region=region_name,
                is_known=(region_name in known_set),
            )
        )


# ---------------------------------------------------------------------------
# Helper: generate report from an OutputManifest
# ---------------------------------------------------------------------------


def generate_report_from_manifest(
    manifest: Any,
    stage: str = "unknown",
    check_files: bool = False,
) -> "ValidationReport":
    """Generate a :class:`ValidationReport` from an
    :class:`~output_schema.OutputManifest`.

    This function inspects the manifest's declared schemas and populates the
    corresponding report sections:

    * **Output integrity** – one entry per declared output file.  When
      ``check_files=True`` the function checks whether each file exists on
      disk and records its size.
    * **Missing branches** – for every schema that declares a branch / column
      list, a :class:`MissingBranchEntry` with an empty ``missing_branches``
      list is recorded (no filesystem access is needed to compute this at
      report-generation time; the caller is expected to populate the missing
      list after comparing against the actual file).
    * **Config mismatches** – schema version mismatches are recorded as
      ``ConfigMismatchEntry`` items with ``severity=ERROR``.
    * **Cutflow** – when the manifest includes a
      :class:`~output_schema.CutflowSchema`, the declared counter keys are
      listed as cutflow step names (counts are not available without reading
      the ROOT file).
    * **Regions** – one :class:`RegionEntry` per declared region, with
      validation status, hierarchy issues, and output-coverage information
      (which artifact roles reference that region by name).
    * **Nuisance group coverage** – one :class:`NuisanceGroupCoverageEntry`
      per declared nuisance group, showing the expected systematics.
      Coverage gaps (missing Up/Down columns) cannot be determined without
      actual dataframe columns; callers that have column information should
      call :meth:`~variation_orchestrator.VariationOrchestrator.build_validation_report`
      to fill in the missing-column details.
    * **Errors / warnings** – any errors returned by
      :meth:`~output_schema.OutputManifest.validate` are appended to the
      report's error list.

    Parameters
    ----------
    manifest : OutputManifest
        The manifest to inspect.
    stage : str
        Name of the production stage (used as the report's ``stage`` field).
    check_files : bool
        When ``True``, test whether each declared output file exists on disk
        and record its size in the :class:`OutputIntegrityEntry`.  Defaults
        to ``False``.

    Returns
    -------
    ValidationReport
        Populated report.  The caller can extend it further by calling any
        of the ``add_*`` methods.
    """
    report = ValidationReport(stage=stage)

    # ---- validation errors from the manifest itself ----
    for err in manifest.validate():
        report.add_error(err)

    # ---- output integrity ----
    _add_integrity_from_manifest(report, manifest, check_files=check_files)

    # ---- missing branches ----
    _add_missing_branches_from_manifest(report, manifest)

    # ---- schema version mismatches as config mismatches ----
    _add_version_mismatches_from_manifest(report, manifest)

    # ---- cutflow counter keys ----
    if manifest.cutflow is not None:
        for key in manifest.cutflow.counter_keys:
            report.add_cutflow_step(
                CutflowEntry(cut_name=key, events_passed=0)
            )

    # ---- region definitions and output coverage ----
    _add_regions_from_manifest(report, manifest)

    # ---- nuisance group coverage stubs ----
    _add_nuisance_coverage_from_manifest(report, manifest)

    return report


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _ensure_dir(path: str) -> None:
    """Create parent directories for *path* if they do not exist."""
    parent = os.path.dirname(os.path.abspath(path))
    os.makedirs(parent, exist_ok=True)


def _file_integrity(role: str, path: str, check_files: bool) -> "OutputIntegrityEntry":
    """Build an :class:`OutputIntegrityEntry` for *path*."""
    if not check_files:
        return OutputIntegrityEntry(artifact_role=role, path=path, exists=None)
    exists = os.path.isfile(path)
    size: Optional[int] = None
    issues: List[str] = []
    if exists:
        try:
            size = os.path.getsize(path)
        except OSError as exc:
            issues.append(f"Could not determine file size: {exc}")
        if size is not None and size == 0:
            issues.append("File is empty (0 bytes)")
    return OutputIntegrityEntry(
        artifact_role=role, path=path, exists=exists, size_bytes=size, issues=issues
    )


def _add_integrity_from_manifest(
    report: "ValidationReport", manifest: Any, check_files: bool
) -> None:
    """Populate output integrity entries from manifest schemas."""
    for role, schema in (
        ("skim", manifest.skim),
        ("histograms", manifest.histograms),
        ("metadata", manifest.metadata),
        ("cutflow", manifest.cutflow),
    ):
        if schema is not None and getattr(schema, "output_file", ""):
            report.add_output_integrity(
                _file_integrity(role, schema.output_file, check_files)
            )

    for i, artifact in enumerate(manifest.law_artifacts):
        role = f"law_artifacts[{i}]"
        if artifact.path_pattern:
            report.add_output_integrity(
                _file_integrity(role, artifact.path_pattern, check_files)
            )

    for i, artifact in enumerate(manifest.intermediate_artifacts):
        role = f"intermediate_artifacts[{i}]"
        if getattr(artifact, "output_file", ""):
            report.add_output_integrity(
                _file_integrity(role, artifact.output_file, check_files)
            )


def _add_missing_branches_from_manifest(
    report: "ValidationReport", manifest: Any
) -> None:
    """Add MissingBranchEntry stubs from declared branch/column lists."""
    if manifest.skim is not None and manifest.skim.branches:
        report.add_missing_branches(
            MissingBranchEntry(
                artifact_role="skim",
                expected_branches=list(manifest.skim.branches),
                missing_branches=[],
            )
        )

    for i, artifact in enumerate(manifest.intermediate_artifacts):
        if artifact.columns:
            report.add_missing_branches(
                MissingBranchEntry(
                    artifact_role=f"intermediate_artifacts[{i}]",
                    expected_branches=list(artifact.columns),
                    missing_branches=[],
                )
            )


def _add_version_mismatches_from_manifest(
    report: "ValidationReport", manifest: Any
) -> None:
    """Record schema version mismatches as ConfigMismatchEntry items."""
    # Import lazily to avoid circular imports when output_schema imports this module.
    try:
        from output_schema import (
            CUTFLOW_SCHEMA_VERSION,
            HISTOGRAM_SCHEMA_VERSION,
            INTERMEDIATE_ARTIFACT_SCHEMA_VERSION,
            LAW_ARTIFACT_SCHEMA_VERSION,
            METADATA_SCHEMA_VERSION,
            OUTPUT_MANIFEST_VERSION,
            SKIM_SCHEMA_VERSION,
        )
    except ImportError:
        return

    checks = [
        ("manifest.manifest_version", OUTPUT_MANIFEST_VERSION, manifest.manifest_version),
    ]
    for role, schema, expected_ver in (
        ("skim.schema_version", manifest.skim, SKIM_SCHEMA_VERSION),
        ("histograms.schema_version", manifest.histograms, HISTOGRAM_SCHEMA_VERSION),
        ("metadata.schema_version", manifest.metadata, METADATA_SCHEMA_VERSION),
        ("cutflow.schema_version", manifest.cutflow, CUTFLOW_SCHEMA_VERSION),
    ):
        if schema is not None:
            checks.append((role, expected_ver, schema.schema_version))

    for i, artifact in enumerate(manifest.law_artifacts):
        checks.append(
            (
                f"law_artifacts[{i}].schema_version",
                LAW_ARTIFACT_SCHEMA_VERSION,
                artifact.schema_version,
            )
        )

    for i, artifact in enumerate(manifest.intermediate_artifacts):
        checks.append(
            (
                f"intermediate_artifacts[{i}].schema_version",
                INTERMEDIATE_ARTIFACT_SCHEMA_VERSION,
                artifact.schema_version,
            )
        )

    for key, expected, actual in checks:
        if expected != actual:
            report.add_config_mismatch(
                ConfigMismatchEntry(
                    key=key,
                    expected=expected,
                    actual=actual,
                    severity=ReportSeverity.ERROR,
                )
            )


def _add_regions_from_manifest(
    report: "ValidationReport", manifest: Any
) -> None:
    """Populate :class:`RegionEntry` records from manifest region definitions.

    For each declared region the entry is validated and region-hierarchy
    errors are recorded.  The ``covered_by`` field lists artifact roles
    (``"histograms"``, ``"cutflow"``) that reference the region by name
    (i.e. the region name appears as a substring in the artifact's named
    outputs).
    """
    if not manifest.regions:
        return

    # Compute hierarchy errors and index them to regions
    try:
        from output_schema import validate_region_hierarchy
        hierarchy_errors: List[str] = validate_region_hierarchy(manifest.regions)
    except ImportError:
        hierarchy_errors = []

    # Build name lists for coverage checks once (not per-region)
    histogram_names: List[str] = (
        list(manifest.histograms.histogram_names)
        if manifest.histograms is not None
        else []
    )
    cutflow_keys: List[str] = (
        list(manifest.cutflow.counter_keys)
        if manifest.cutflow is not None
        else []
    )

    def _region_in_names(region_name: str, names: List[str]) -> bool:
        """Return True when *region_name* appears as a substring in any of *names*."""
        return any(region_name in n for n in names)

    for i, region in enumerate(manifest.regions):
        # Per-region validation errors
        per_region_issues = region.validate()
        # Also surface hierarchy errors that mention this region by name
        for herr in hierarchy_errors:
            if region.name and region.name in herr:
                if herr not in per_region_issues:
                    per_region_issues.append(herr)

        is_valid = len(per_region_issues) == 0

        # Determine output coverage: which artifacts reference this region
        covered_by: List[str] = []
        if region.name:
            if _region_in_names(region.name, histogram_names):
                covered_by.append("histograms")
            if _region_in_names(region.name, cutflow_keys):
                covered_by.append("cutflow")

        report.add_region(
            RegionEntry(
                region_name=region.name,
                filter_column=region.filter_column,
                parent=region.parent,
                is_valid=is_valid,
                issues=per_region_issues,
                covered_by=covered_by,
            )
        )


def _add_nuisance_coverage_from_manifest(
    report: "ValidationReport", manifest: Any
) -> None:
    """Populate :class:`NuisanceGroupCoverageEntry` stubs from manifest nuisance groups.

    One entry is added per declared nuisance group showing the expected
    systematics, processes, regions, and output usage.  The
    ``missing_up``, ``missing_down``, and ``not_found`` fields are left
    empty because available dataframe columns are not accessible at
    manifest-inspection time.  Callers that have column information should
    call
    :meth:`~variation_orchestrator.VariationOrchestrator.build_validation_report`
    to fill in the missing-column details.
    """
    for ng in manifest.nuisance_groups:
        report.add_nuisance_group_coverage(
            NuisanceGroupCoverageEntry(
                group_name=ng.name,
                group_type=ng.group_type,
                systematics=list(ng.systematics),
                processes=list(ng.processes),
                regions=list(ng.regions),
                output_usage=list(ng.output_usage),
                missing_up=[],
                missing_down=[],
                not_found=[],
                severity=ReportSeverity.ERROR.value,
            )
        )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    """CLI entry point for generating validation reports from manifest files."""
    parser = argparse.ArgumentParser(
        description="Generate a validation report from an output manifest YAML file.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "manifest",
        help="Path to the output_manifest.yaml file to inspect.",
    )
    parser.add_argument(
        "--stage",
        default="unknown",
        help="Production stage name to embed in the report (default: unknown).",
    )
    parser.add_argument(
        "--check-files",
        action="store_true",
        default=False,
        help="Check whether declared output files exist on disk.",
    )
    parser.add_argument(
        "--out-yaml",
        metavar="PATH",
        help="Write machine-readable YAML report to PATH.",
    )
    parser.add_argument(
        "--out-json",
        metavar="PATH",
        help="Write machine-readable JSON report to PATH.",
    )
    parser.add_argument(
        "--out-text",
        metavar="PATH",
        help="Write human-readable text report to PATH.",
    )
    parser.add_argument(
        "--print",
        dest="print_text",
        action="store_true",
        default=False,
        help="Print the human-readable report to stdout.",
    )
    args = parser.parse_args()

    try:
        from output_schema import OutputManifest
        manifest = OutputManifest.load_yaml(args.manifest)
    except (FileNotFoundError, OSError, ValueError, yaml.YAMLError) as exc:
        print(f"Error loading manifest '{args.manifest}': {exc}")
        raise SystemExit(1)

    report = generate_report_from_manifest(
        manifest, stage=args.stage, check_files=args.check_files
    )

    if args.out_yaml:
        report.save_yaml(args.out_yaml)
        print(f"YAML report written to: {args.out_yaml}")

    if args.out_json:
        report.save_json(args.out_json)
        print(f"JSON report written to: {args.out_json}")

    if args.out_text:
        report.save_text(args.out_text)
        print(f"Text report written to: {args.out_text}")

    if args.print_text or not any([args.out_yaml, args.out_json, args.out_text]):
        print(report.to_text())

    raise SystemExit(1 if report.has_errors else 0)


if __name__ == "__main__":
    main()
