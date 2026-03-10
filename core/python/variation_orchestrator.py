"""
Unified orchestration layer for Region × NuisanceGroups × Weights.

This module provides a single API that answers:

- For a given ``(process, region, output_usage)`` context, **which systematic
  variation columns** should be considered (from nuisance groups)?
- **Which weight columns** should be used (nominal and per-variation)?
- **Where is coverage missing** – i.e. which requested variation columns are
  absent from the available dataframe columns or produced outputs?

The central class is :class:`VariationOrchestrator`.  It is initialised once
with a :class:`~nuisance_groups.NuisanceGroupRegistry` (and optionally a list
of :class:`WeightVariationSpec` objects) and then consumed by histogram
filling, datacard creation, and validation tooling without each tool needing
to re-derive "which variations apply".

Design principles
-----------------
* **No changes to the "systematics are columns" model** – the orchestrator
  only maps logical names to expected column names; it does not modify the
  dataframe.
* **Single RDF pass** – the orchestrator produces a :class:`VariationPlan`
  enumerating all required columns *upfront* so that downstream tools can
  book all histograms in one pass.
* **Larger, higher-dimensional histograms preferred** – the orchestrator
  returns a flat list of variation specifications rather than driving
  repeated per-variation passes.

Usage
-----
Build an orchestrator::

    from nuisance_groups import NuisanceGroupRegistry
    from variation_orchestrator import VariationOrchestrator, WeightVariationSpec

    registry = NuisanceGroupRegistry.from_config(config)
    weight_vars = [
        WeightVariationSpec("btag", "weight_nominal", "weight_btagUp", "weight_btagDown"),
    ]

    orch = VariationOrchestrator(
        nuisance_registry=registry,
        weight_variations=weight_vars,
        nominal_weight="weight_nominal",
    )

Query for a context::

    plan = orch.get_variation_plan(
        process="signal", region="signal_region", output_usage="histogram"
    )
    for sv in plan.systematic_variations:
        print(sv.base_name, sv.up_column, sv.down_column)

Validate available columns and get a report::

    from validation_report import ValidationReport
    report = ValidationReport(stage="histogram")
    orch.build_validation_report(
        report=report,
        available_columns=df_columns,
        processes=["signal", "ttbar"],
        regions=["signal_region", "control"],
        output_usage="histogram",
    )
    print(report.to_text())
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Set

from nuisance_groups import (
    CoverageSeverity,
    NuisanceGroup,
    NuisanceGroupRegistry,
)


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class MissingSeverity(str, enum.Enum):
    """Configurable severity for missing-variation findings.

    Attributes
    ----------
    WARN:
        Report missing variations as warnings; do not fail validation.
    ERROR:
        Report missing variations as errors; fail validation.
    """

    WARN = "warn"
    ERROR = "error"


# ---------------------------------------------------------------------------
# Specification dataclasses
# ---------------------------------------------------------------------------


@dataclass
class WeightVariationSpec:
    """Specification for a single named weight variation.

    The framework expects that weight variations are represented as separate
    dataframe columns (consistent with the "systematics are columns" model).

    Parameters
    ----------
    name : str
        Logical name for this weight variation (e.g. ``"btag"``).
    nominal_column : str
        Name of the nominal weight column used together with this variation
        (e.g. ``"weight_nominal"``).  May be the same as the orchestrator's
        global ``nominal_weight`` for simple cases.
    up_column : str
        Name of the up-shifted weight column (e.g. ``"weight_btagUp"``).
    down_column : str
        Name of the down-shifted weight column (e.g. ``"weight_btagDown"``).
    processes : list[str]
        Physics processes this weight variation applies to.  An empty list
        means it applies to **all** processes.
    regions : list[str]
        Analysis regions this weight variation applies to.  An empty list
        means it applies to **all** regions.
    output_usage : list[str]
        Downstream tools that consume this variation.  An empty list means
        **all** outputs.
    """

    name: str
    nominal_column: str
    up_column: str
    down_column: str
    processes: List[str] = field(default_factory=list)
    regions: List[str] = field(default_factory=list)
    output_usage: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ helpers

    def applies_to_process(self, process: str) -> bool:
        """Return ``True`` when this spec applies to *process*."""
        return not self.processes or process in self.processes

    def applies_to_region(self, region: str) -> bool:
        """Return ``True`` when this spec applies to *region*."""
        return not self.regions or region in self.regions

    def used_for_output(self, usage: str) -> bool:
        """Return ``True`` when this spec is consumed by *usage*."""
        return not self.output_usage or usage in self.output_usage

    def required_columns(self) -> List[str]:
        """Return the list of dataframe columns required by this spec."""
        cols = []
        if self.up_column:
            cols.append(self.up_column)
        if self.down_column:
            cols.append(self.down_column)
        return cols


@dataclass
class SystematicVariationSpec:
    """Specification for a single systematic variation (shape or rate).

    The up and down column names follow the framework convention
    ``<base_name>Up`` / ``<base_name>Down`` by default, but may be overridden.

    Parameters
    ----------
    base_name : str
        Base systematic name (e.g. ``"JES"``).
    up_column : str
        Expected dataframe column name for the up shift (e.g. ``"JESUp"``).
    down_column : str
        Expected dataframe column name for the down shift (e.g. ``"JESDown"``).
    group_name : str
        Name of the :class:`~nuisance_groups.NuisanceGroup` that owns this
        variation.
    group_type : str
        Group type (``"shape"``, ``"rate"``, etc.).
    """

    base_name: str
    up_column: str
    down_column: str
    group_name: str
    group_type: str = "shape"

    def required_columns(self) -> List[str]:
        """Return the list of dataframe columns required by this spec."""
        return [self.up_column, self.down_column]


# ---------------------------------------------------------------------------
# VariationPlan
# ---------------------------------------------------------------------------


@dataclass
class VariationPlan:
    """The complete set of variations for a ``(process, region, output_usage)`` context.

    A :class:`VariationPlan` is produced by
    :meth:`VariationOrchestrator.get_variation_plan` and consumed by histogram
    filling, datacard generation, and plotting tools.

    Attributes
    ----------
    process : str
        Physics process / sample name.
    region : str
        Analysis region name.
    output_usage : str
        Downstream tool for which this plan was created.
    nominal_weight : str
        Name of the nominal weight column (empty string if unweighted).
    systematic_variations : list[SystematicVariationSpec]
        Ordered list of systematic shape/rate variations.
    weight_variations : list[WeightVariationSpec]
        Ordered list of weight variations applicable in this context.
    """

    process: str
    region: str
    output_usage: str
    nominal_weight: str
    systematic_variations: List[SystematicVariationSpec] = field(default_factory=list)
    weight_variations: List[WeightVariationSpec] = field(default_factory=list)

    # ------------------------------------------------------------------ helpers

    def all_required_columns(self) -> List[str]:
        """Return the deduplicated list of all dataframe columns needed.

        This includes the nominal weight, all systematic variation columns,
        and all weight variation columns.  The result is suitable for passing
        to a pre-flight column check before booking histograms.
        """
        seen: Set[str] = set()
        result: List[str] = []
        for col in (
            [self.nominal_weight]
            + [c for sv in self.systematic_variations for c in sv.required_columns()]
            + [c for wv in self.weight_variations for c in wv.required_columns()]
        ):
            if col and col not in seen:
                seen.add(col)
                result.append(col)
        return result

    def systematic_variation_names(self) -> List[str]:
        """Return the list of base systematic names in this plan."""
        return [sv.base_name for sv in self.systematic_variations]

    def weight_variation_names(self) -> List[str]:
        """Return the list of weight variation names in this plan."""
        return [wv.name for wv in self.weight_variations]

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain Python dict."""
        return {
            "process": self.process,
            "region": self.region,
            "output_usage": self.output_usage,
            "nominal_weight": self.nominal_weight,
            "systematic_variations": [
                {
                    "base_name": sv.base_name,
                    "up_column": sv.up_column,
                    "down_column": sv.down_column,
                    "group_name": sv.group_name,
                    "group_type": sv.group_type,
                }
                for sv in self.systematic_variations
            ],
            "weight_variations": [
                {
                    "name": wv.name,
                    "nominal_column": wv.nominal_column,
                    "up_column": wv.up_column,
                    "down_column": wv.down_column,
                }
                for wv in self.weight_variations
            ],
        }


# ---------------------------------------------------------------------------
# MissingVariationReport
# ---------------------------------------------------------------------------


@dataclass
class MissingVariationReport:
    """A coverage finding for a single systematic or weight variation.

    Attributes
    ----------
    process : str
        Physics process that was checked.
    region : str
        Analysis region that was checked.
    group_name : str
        Name of the :class:`~nuisance_groups.NuisanceGroup` or weight
        variation that is missing coverage.  For weight variations this is
        set to ``"(weight)"`` followed by the variation name.
    variable : str
        The base variation name (systematic or weight variation name).
    missing_up : bool
        ``True`` when the up-shift column is absent.
    missing_down : bool
        ``True`` when the down-shift column is absent.
    not_found : bool
        ``True`` when *neither* up nor down shift column is present.
    severity : str
        ``"error"`` or ``"warn"`` – configurable at orchestrator level.
    """

    process: str
    region: str
    group_name: str
    variable: str
    missing_up: bool = False
    missing_down: bool = False
    not_found: bool = False
    severity: str = MissingSeverity.ERROR.value

    def to_dict(self) -> Dict[str, Any]:
        return {
            "process": self.process,
            "region": self.region,
            "group_name": self.group_name,
            "variable": self.variable,
            "missing_up": self.missing_up,
            "missing_down": self.missing_down,
            "not_found": self.not_found,
            "severity": self.severity,
        }


# ---------------------------------------------------------------------------
# VariationOrchestrator
# ---------------------------------------------------------------------------


class VariationOrchestrator:
    """Unified orchestration layer for Region × NuisanceGroups × Weights.

    A single instance defines "which variations apply" for the whole analysis.
    Downstream tools (histogram filling, datacards, plotting) call
    :meth:`get_variation_plan` to obtain the concrete column names to use for
    a given ``(process, region, output_usage)`` combination.

    Parameters
    ----------
    nuisance_registry : NuisanceGroupRegistry
        Registry of all declared nuisance groups.
    weight_variations : list[WeightVariationSpec] or None
        Optional list of weight-variation specifications.  When ``None`` no
        weight variations are tracked.
    nominal_weight : str
        Name of the nominal event-weight column.  An empty string means
        unweighted (all weights = 1).
    missing_severity : MissingSeverity or str
        Default severity applied to missing-variation findings produced by
        :meth:`validate_coverage`.  Accepts the enum or its string value
        (``"error"`` / ``"warn"``).  Defaults to ``MissingSeverity.ERROR``.

    Examples
    --------
    Typical usage::

        orch = VariationOrchestrator(
            nuisance_registry=NuisanceGroupRegistry.from_config(cfg),
            weight_variations=[
                WeightVariationSpec("btag", "w_nom", "w_btagUp", "w_btagDown"),
            ],
            nominal_weight="w_nom",
        )

        plan = orch.get_variation_plan("signal", "signal_region", "histogram")
        for sv in plan.systematic_variations:
            # book histogram for sv.up_column, sv.down_column, …
            ...
    """

    def __init__(
        self,
        nuisance_registry: NuisanceGroupRegistry,
        weight_variations: Optional[List[WeightVariationSpec]] = None,
        nominal_weight: str = "",
        missing_severity: MissingSeverity | str = MissingSeverity.ERROR,
    ) -> None:
        self._registry = nuisance_registry
        self._weight_variations: List[WeightVariationSpec] = (
            list(weight_variations) if weight_variations else []
        )
        self._nominal_weight = nominal_weight
        if isinstance(missing_severity, MissingSeverity):
            self._missing_severity = missing_severity
        else:
            self._missing_severity = MissingSeverity(missing_severity)

    # ------------------------------------------------------------------ properties

    @property
    def nuisance_registry(self) -> NuisanceGroupRegistry:
        """The :class:`~nuisance_groups.NuisanceGroupRegistry` backing this orchestrator."""
        return self._registry

    @property
    def weight_variations(self) -> List[WeightVariationSpec]:
        """Read-only list of registered :class:`WeightVariationSpec` objects."""
        return list(self._weight_variations)

    @property
    def nominal_weight(self) -> str:
        """Name of the global nominal weight column."""
        return self._nominal_weight

    @property
    def missing_severity(self) -> MissingSeverity:
        """Default severity for missing-variation findings."""
        return self._missing_severity

    # ------------------------------------------------------------------ mutation

    def add_weight_variation(self, spec: WeightVariationSpec) -> None:
        """Register an additional :class:`WeightVariationSpec`.

        Parameters
        ----------
        spec : WeightVariationSpec
            The weight variation specification to add.
        """
        self._weight_variations.append(spec)

    # ------------------------------------------------------------------ core query

    def get_variation_plan(
        self,
        process: str,
        region: str,
        output_usage: str,
    ) -> VariationPlan:
        """Build the :class:`VariationPlan` for a given context.

        Parameters
        ----------
        process : str
            Physics process / sample name (e.g. ``"signal"``).
        region : str
            Analysis region name (e.g. ``"signal_region"``).
        output_usage : str
            Downstream tool identifier (e.g. ``"histogram"``, ``"datacard"``,
            ``"plot"``).

        Returns
        -------
        VariationPlan
            Contains all systematic and weight variation column names that
            apply for this ``(process, region, output_usage)`` combination.
            The caller can iterate ``plan.systematic_variations`` and
            ``plan.weight_variations`` to book histograms, build datacards,
            etc., without needing to re-query the registry.
        """
        # ------ systematic variations from nuisance groups ------
        syst_map: Dict[str, NuisanceGroup] = (
            self._registry.get_systematics_for_process_and_region(
                process=process,
                region=region,
                output_usage=output_usage,
            )
        )
        systematic_variations: List[SystematicVariationSpec] = [
            SystematicVariationSpec(
                base_name=syst_name,
                up_column=f"{syst_name}Up",
                down_column=f"{syst_name}Down",
                group_name=group.name,
                group_type=group.group_type,
            )
            for syst_name, group in syst_map.items()
        ]

        # ------ weight variations ------
        weight_variations: List[WeightVariationSpec] = [
            wv
            for wv in self._weight_variations
            if (
                wv.applies_to_process(process)
                and wv.applies_to_region(region)
                and wv.used_for_output(output_usage)
            )
        ]

        return VariationPlan(
            process=process,
            region=region,
            output_usage=output_usage,
            nominal_weight=self._nominal_weight,
            systematic_variations=systematic_variations,
            weight_variations=weight_variations,
        )

    # ------------------------------------------------------------------ validation

    def validate_coverage(
        self,
        available_columns: Sequence[str],
        processes: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        output_usage: Optional[str] = None,
        severity: Optional[MissingSeverity | str] = None,
    ) -> List[MissingVariationReport]:
        """Validate that all required columns are present in *available_columns*.

        For every ``(process, region)`` pair (taken from the Cartesian product
        of *processes* × *regions*) and for each systematic / weight variation
        that applies in that context, the method checks whether the expected
        ``<name>Up`` and ``<name>Down`` columns are present.

        Parameters
        ----------
        available_columns : sequence of str
            Column names present in the dataframe or produced output.  The
            check is case-sensitive (matching the "systematics are columns"
            convention).
        processes : list[str] or None
            Processes to check.  When ``None``, all processes declared in the
            nuisance registry are checked (inferred from each group's
            ``processes`` list; groups with an empty ``processes`` list apply
            to all, so at least the empty string ``""`` is used as a
            placeholder if *processes* is also ``None``).
        regions : list[str] or None
            Regions to check.  Same fallback logic as *processes*.
        output_usage : str or None
            Restrict the check to groups that apply to *output_usage*.  When
            ``None``, all output usages are considered.
        severity : MissingSeverity or str or None
            Override the default :attr:`missing_severity` for this call.

        Returns
        -------
        list[MissingVariationReport]
            One entry per missing or incomplete variation.  An empty list
            means full coverage.
        """
        sev_value = self._resolve_severity(severity).value
        col_set: Set[str] = set(available_columns)

        effective_processes = processes if processes is not None else self._infer_processes()
        effective_regions = regions if regions is not None else self._infer_regions()

        findings: List[MissingVariationReport] = []

        for process in effective_processes:
            for region in effective_regions:
                plan = self.get_variation_plan(
                    process=process,
                    region=region,
                    output_usage=output_usage or "",
                )

                # Check systematic variations
                for sv in plan.systematic_variations:
                    has_up = sv.up_column in col_set
                    has_down = sv.down_column in col_set
                    if not has_up or not has_down:
                        findings.append(
                            MissingVariationReport(
                                process=process,
                                region=region,
                                group_name=sv.group_name,
                                variable=sv.base_name,
                                missing_up=not has_up,
                                missing_down=not has_down,
                                not_found=not has_up and not has_down,
                                severity=sev_value,
                            )
                        )

                # Check weight variations
                for wv in plan.weight_variations:
                    has_up = not wv.up_column or wv.up_column in col_set
                    has_down = not wv.down_column or wv.down_column in col_set
                    if not has_up or not has_down:
                        findings.append(
                            MissingVariationReport(
                                process=process,
                                region=region,
                                group_name=f"(weight) {wv.name}",
                                variable=wv.name,
                                missing_up=not has_up,
                                missing_down=not has_down,
                                not_found=not has_up and not has_down,
                                severity=sev_value,
                            )
                        )

        return findings

    def build_validation_report(
        self,
        report: Any,
        available_columns: Sequence[str],
        processes: Optional[List[str]] = None,
        regions: Optional[List[str]] = None,
        output_usage: Optional[str] = None,
        severity: Optional[MissingSeverity | str] = None,
    ) -> None:
        """Populate *report* with nuisance-group coverage entries.

        This method calls :meth:`validate_coverage` and appends the results to
        *report* as :class:`~validation_report.NuisanceGroupCoverageEntry`
        records, one per nuisance group per ``(process, region)`` pair.  It
        also appends free-form error / warning messages for every
        :class:`MissingVariationReport` found.

        Parameters
        ----------
        report : ValidationReport
            The report to populate (imported from ``validation_report``).
        available_columns : sequence of str
            Column names present in the dataframe / output.
        processes : list[str] or None
            Processes to check (see :meth:`validate_coverage`).
        regions : list[str] or None
            Regions to check (see :meth:`validate_coverage`).
        output_usage : str or None
            Restrict to this output usage (see :meth:`validate_coverage`).
        severity : MissingSeverity or str or None
            Override the default severity for this call.
        """
        # Import here to avoid circular dependency
        try:
            from validation_report import NuisanceGroupCoverageEntry
        except ImportError:  # pragma: no cover
            NuisanceGroupCoverageEntry = None  # type: ignore[assignment, misc]

        sev_value = self._resolve_severity(severity).value
        col_set: Set[str] = set(available_columns)

        effective_processes = processes if processes is not None else self._infer_processes()
        effective_regions = regions if regions is not None else self._infer_regions()
        effective_usage = output_usage or ""

        for process in effective_processes:
            for region in effective_regions:
                plan = self.get_variation_plan(
                    process=process,
                    region=region,
                    output_usage=effective_usage,
                )

                # Collect per-group coverage
                group_data: Dict[str, Dict[str, Any]] = {}

                for sv in plan.systematic_variations:
                    gkey = sv.group_name
                    if gkey not in group_data:
                        # Find the group for metadata
                        grp_obj = self._find_group(sv.group_name)
                        group_data[gkey] = {
                            "group_type": sv.group_type,
                            "systematics": [],
                            "processes": grp_obj.processes if grp_obj else [],
                            "regions": grp_obj.regions if grp_obj else [],
                            "output_usage": grp_obj.output_usage if grp_obj else [],
                            "missing_up": [],
                            "missing_down": [],
                            "not_found": [],
                        }
                    group_data[gkey]["systematics"].append(sv.base_name)

                    has_up = sv.up_column in col_set
                    has_down = sv.down_column in col_set
                    if not has_up and not has_down:
                        group_data[gkey]["not_found"].append(sv.base_name)
                    elif not has_up:
                        group_data[gkey]["missing_up"].append(sv.base_name)
                    elif not has_down:
                        group_data[gkey]["missing_down"].append(sv.base_name)

                for gname, gdata in group_data.items():
                    if NuisanceGroupCoverageEntry is not None:
                        entry = NuisanceGroupCoverageEntry(
                            group_name=gname,
                            group_type=gdata["group_type"],
                            systematics=gdata["systematics"],
                            processes=gdata["processes"],
                            regions=gdata["regions"],
                            output_usage=gdata["output_usage"],
                            missing_up=gdata["missing_up"],
                            missing_down=gdata["missing_down"],
                            not_found=gdata["not_found"],
                            severity=sev_value,
                        )
                        report.add_nuisance_group_coverage(entry)

                    # Free-form messages
                    for syst in gdata["not_found"]:
                        msg = (
                            f"[{process}/{region}] Systematic '{syst}' (group '{gname}') "
                            f"has no Up or Down column in available columns."
                        )
                        if sev_value == MissingSeverity.ERROR.value:
                            report.add_error(msg)
                        else:
                            report.add_warning(msg)

                    for syst in gdata["missing_up"]:
                        msg = (
                            f"[{process}/{region}] Systematic '{syst}' (group '{gname}') "
                            f"is missing its Up column."
                        )
                        if sev_value == MissingSeverity.ERROR.value:
                            report.add_error(msg)
                        else:
                            report.add_warning(msg)

                    for syst in gdata["missing_down"]:
                        msg = (
                            f"[{process}/{region}] Systematic '{syst}' (group '{gname}') "
                            f"is missing its Down column."
                        )
                        if sev_value == MissingSeverity.ERROR.value:
                            report.add_error(msg)
                        else:
                            report.add_warning(msg)

                # Weight variations
                for wv in plan.weight_variations:
                    has_up = not wv.up_column or wv.up_column in col_set
                    has_down = not wv.down_column or wv.down_column in col_set
                    if not has_up or not has_down:
                        msg = (
                            f"[{process}/{region}] Weight variation '{wv.name}' "
                            f"is missing "
                            + (
                                "Up and Down columns"
                                if not has_up and not has_down
                                else ("Up column" if not has_up else "Down column")
                            )
                            + "."
                        )
                        if sev_value == MissingSeverity.ERROR.value:
                            report.add_error(msg)
                        else:
                            report.add_warning(msg)

    # ------------------------------------------------------------------ helpers

    def _resolve_severity(
        self, override: Optional[MissingSeverity | str]
    ) -> MissingSeverity:
        """Return the effective severity, applying *override* if provided."""
        if override is None:
            return self._missing_severity
        if isinstance(override, MissingSeverity):
            return override
        return MissingSeverity(override)

    def _infer_processes(self) -> List[str]:
        """Return the union of process names declared across all groups.

        When a group declares an empty ``processes`` list it is treated as
        "all processes".  In that case ``""`` (empty string) is included so
        the caller still exercises those groups.
        """
        all_procs: Set[str] = set()
        has_wildcard = False
        for group in self._registry.groups:
            if group.processes:
                all_procs.update(group.processes)
            else:
                has_wildcard = True
        if has_wildcard and not all_procs:
            # All groups apply to all processes; return a sentinel so coverage
            # is still evaluated.
            return [""]
        if has_wildcard:
            # Some groups are wildcards: we check against the declared procs
            # plus the sentinel so wildcard-only groups are also exercised.
            all_procs.add("")
        return sorted(all_procs)

    def _infer_regions(self) -> List[str]:
        """Return the union of region names declared across all groups.

        Same logic as :meth:`_infer_processes`.
        """
        all_regions: Set[str] = set()
        has_wildcard = False
        for group in self._registry.groups:
            if group.regions:
                all_regions.update(group.regions)
            else:
                has_wildcard = True
        if has_wildcard and not all_regions:
            return [""]
        if has_wildcard:
            all_regions.add("")
        return sorted(all_regions)

    def _find_group(self, group_name: str) -> Optional[NuisanceGroup]:
        """Return the :class:`~nuisance_groups.NuisanceGroup` with *group_name*."""
        for group in self._registry.groups:
            if group.name == group_name:
                return group
        return None
