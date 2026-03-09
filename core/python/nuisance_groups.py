"""
Nuisance and systematic grouping framework for RDFAnalyzerCore.

This module provides a higher-level abstraction for grouping systematic
variations.  A :class:`NuisanceGroup` collects related systematic variations
and carries metadata about:

- **type** – whether the group contributes shape or rate uncertainties.
- **process applicability** – which physics processes (samples) the group
  applies to.
- **region applicability** – which analysis regions the group applies to.
- **output usage** – which downstream tools consume this group (histogram
  production, datacard generation, plotting).

A :class:`NuisanceGroupRegistry` stores and queries a collection of
:class:`NuisanceGroup` objects and can validate that all declared systematic
variations have both up and down shifts present in the analysis output.

Usage
-----
Build a registry from a configuration dictionary (as read from YAML)::

    from nuisance_groups import NuisanceGroupRegistry

    config = {
        "groups": [
            {
                "name": "jet_energy",
                "group_type": "shape",
                "systematics": ["JES", "JER"],
                "processes": ["signal", "ttbar"],
                "regions": ["signal_region"],
                "output_usage": ["datacard", "histogram"],
            },
            {
                "name": "luminosity",
                "group_type": "rate",
                "systematics": ["lumi"],
                "processes": [],
                "regions": [],
                "output_usage": ["datacard"],
            },
        ]
    }
    registry = NuisanceGroupRegistry.from_config(config)

    # Query
    datacard_groups = registry.get_groups_for_output("datacard")
    signal_groups = registry.get_groups_for_process("signal")

    # Validate that up/down variations exist
    available = {
        "JES": ["JESUp", "JESDown"],
        "JER": ["JERUp"],  # missing Down!
    }
    issues = registry.validate_coverage(available)
    for issue in issues:
        print(issue.severity, issue.message)

Build a registry from YAML file::

    registry = NuisanceGroupRegistry.load_yaml("nuisance_groups.yaml")

    # Persist
    registry.save_yaml("nuisance_groups.yaml")
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Set

import yaml


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class NuisanceGroupType(str, enum.Enum):
    """Type classification for a :class:`NuisanceGroup`.

    Attributes
    ----------
    SHAPE:
        The group contributes a shape uncertainty (the histogram morphing
        differs between the up and down shifts).
    RATE:
        The group contributes only a rate (normalisation) uncertainty.
    NORMALIZATION:
        Alias for ``RATE``; kept for compatibility with existing YAML configs
        that use the ``"normalization"`` label.
    OTHER:
        Catch-all for custom or non-standard variation types.
    """

    SHAPE = "shape"
    RATE = "rate"
    NORMALIZATION = "normalization"
    OTHER = "other"


class NuisanceGroupOutputUsage(str, enum.Enum):
    """Downstream tools that should consume a :class:`NuisanceGroup`.

    Attributes
    ----------
    HISTOGRAM:
        Variations from this group are used when filling analysis histograms.
    DATACARD:
        Variations from this group appear in CMS-Combine datacards.
    PLOT:
        Variations from this group are shown on analysis plots.
    """

    HISTOGRAM = "histogram"
    DATACARD = "datacard"
    PLOT = "plot"


# ---------------------------------------------------------------------------
# CoverageIssue – result of validate_coverage()
# ---------------------------------------------------------------------------


class CoverageSeverity(str, enum.Enum):
    """Severity level for a :class:`CoverageIssue`."""

    WARNING = "warning"
    ERROR = "error"


@dataclass
class CoverageIssue:
    """A single finding from :meth:`NuisanceGroupRegistry.validate_coverage`.

    Attributes
    ----------
    severity : CoverageSeverity
        ``ERROR`` for issues that prevent the output from being used;
        ``WARNING`` for issues that should be reviewed but are not fatal.
    group_name : str
        Name of the :class:`NuisanceGroup` that has the issue.
    systematic_name : str
        Name of the systematic within the group (empty when the issue applies
        to the whole group).
    message : str
        Human-readable description of the issue.
    """

    severity: CoverageSeverity
    group_name: str
    systematic_name: str
    message: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "severity": self.severity.value,
            "group_name": self.group_name,
            "systematic_name": self.systematic_name,
            "message": self.message,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CoverageIssue":
        return cls(
            severity=CoverageSeverity(d["severity"]),
            group_name=d["group_name"],
            systematic_name=d.get("systematic_name", ""),
            message=d["message"],
        )


# ---------------------------------------------------------------------------
# NuisanceGroup
# ---------------------------------------------------------------------------

#: Valid string values accepted for :attr:`NuisanceGroup.group_type`.
VALID_GROUP_TYPES: Set[str] = {t.value for t in NuisanceGroupType}

#: Valid string values accepted for :attr:`NuisanceGroup.output_usage` entries.
VALID_OUTPUT_USAGES: Set[str] = {u.value for u in NuisanceGroupOutputUsage}


@dataclass
class NuisanceGroup:
    """A named group of related systematic variations.

    Parameters
    ----------
    name : str
        Unique name for this group (e.g. ``"jet_energy_scale"``).
    group_type : str
        One of :data:`VALID_GROUP_TYPES` (``"shape"``, ``"rate"``,
        ``"normalization"``, ``"other"``).
    systematics : list[str]
        Names of the systematic variations belonging to this group (e.g.
        ``["JES", "JER"]``).  Each name should correspond to a base variation
        name; the framework expects ``<name>Up`` and ``<name>Down`` shifts.
    processes : list[str]
        Physics processes / samples this group applies to.  An empty list
        means the group applies to **all** processes.
    regions : list[str]
        Analysis regions this group applies to.  An empty list means the
        group applies to **all** regions.
    output_usage : list[str]
        Downstream tools that should use this group.  Each entry must be one
        of :data:`VALID_OUTPUT_USAGES`.  An empty list means the group is
        used by **all** outputs.
    description : str
        Optional human-readable description.
    correlation_group : str
        Optional label that groups correlated systematics across NuisanceGroups
        (e.g. ``"lumi_correlated"``).  Empty string means no explicit
        correlation labelling.
    """

    name: str
    group_type: str = NuisanceGroupType.SHAPE.value
    systematics: List[str] = field(default_factory=list)
    processes: List[str] = field(default_factory=list)
    regions: List[str] = field(default_factory=list)
    output_usage: List[str] = field(default_factory=list)
    description: str = ""
    correlation_group: str = ""

    # ------------------------------------------------------------------ helpers

    def applies_to_process(self, process: str) -> bool:
        """Return ``True`` when this group applies to *process*.

        A group with an empty :attr:`processes` list applies to all processes.
        """
        return not self.processes or process in self.processes

    def applies_to_region(self, region: str) -> bool:
        """Return ``True`` when this group applies to *region*.

        A group with an empty :attr:`regions` list applies to all regions.
        """
        return not self.regions or region in self.regions

    def used_for_output(self, usage: str) -> bool:
        """Return ``True`` when this group is consumed by *usage*.

        A group with an empty :attr:`output_usage` list is consumed by all
        outputs.
        """
        return not self.output_usage or usage in self.output_usage

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        """Serialise to a plain Python dict."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NuisanceGroup":
        """Construct from a dict, ignoring unknown keys."""
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in data.items() if k in known})

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        """Return a list of validation error strings (empty = valid)."""
        errors: List[str] = []
        if not self.name:
            errors.append("NuisanceGroup.name must not be empty.")
        if self.group_type not in VALID_GROUP_TYPES:
            errors.append(
                f"NuisanceGroup '{self.name}': group_type "
                f"'{self.group_type}' is not one of {sorted(VALID_GROUP_TYPES)}."
            )
        for usage in self.output_usage:
            if usage not in VALID_OUTPUT_USAGES:
                errors.append(
                    f"NuisanceGroup '{self.name}': output_usage entry "
                    f"'{usage}' is not one of {sorted(VALID_OUTPUT_USAGES)}."
                )
        return errors


# ---------------------------------------------------------------------------
# NuisanceGroupRegistry
# ---------------------------------------------------------------------------


class NuisanceGroupRegistry:
    """Registry that stores and queries a collection of :class:`NuisanceGroup` objects.

    Parameters
    ----------
    groups : list[NuisanceGroup] or None
        Initial list of groups.  Defaults to an empty list.

    Examples
    --------
    Build a registry from a YAML config dict and validate coverage::

        registry = NuisanceGroupRegistry.from_config(config)
        issues = registry.validate_coverage(available_variations)
        for issue in issues:
            print(f"[{issue.severity}] {issue.group_name}/{issue.systematic_name}: {issue.message}")
    """

    def __init__(self, groups: Optional[List[NuisanceGroup]] = None) -> None:
        self._groups: List[NuisanceGroup] = list(groups) if groups else []

    # ------------------------------------------------------------------ mutation

    def add_group(self, group: NuisanceGroup) -> None:
        """Append *group* to the registry.

        Raises
        ------
        ValueError
            When a group with the same name already exists.
        """
        existing_names = {g.name for g in self._groups}
        if group.name in existing_names:
            raise ValueError(
                f"NuisanceGroupRegistry: a group named '{group.name}' already exists."
            )
        self._groups.append(group)

    # ------------------------------------------------------------------ query

    @property
    def groups(self) -> List[NuisanceGroup]:
        """Read-only view of all registered groups."""
        return list(self._groups)

    def get_groups_for_process(self, process: str) -> List[NuisanceGroup]:
        """Return all groups that apply to *process*."""
        return [g for g in self._groups if g.applies_to_process(process)]

    def get_groups_for_region(self, region: str) -> List[NuisanceGroup]:
        """Return all groups that apply to *region*."""
        return [g for g in self._groups if g.applies_to_region(region)]

    def get_groups_for_output(self, usage: str) -> List[NuisanceGroup]:
        """Return all groups consumed by *usage* (e.g. ``"datacard"``)."""
        return [g for g in self._groups if g.used_for_output(usage)]

    def get_groups_by_type(self, group_type: str) -> List[NuisanceGroup]:
        """Return all groups whose :attr:`~NuisanceGroup.group_type` matches."""
        return [g for g in self._groups if g.group_type == group_type]

    def get_systematics_for_process_and_region(
        self, process: str, region: str, output_usage: Optional[str] = None
    ) -> Dict[str, NuisanceGroup]:
        """Return a mapping of systematic name → :class:`NuisanceGroup` for the
        given *process* / *region* combination.

        Parameters
        ----------
        process : str
            Physics process name.
        region : str
            Analysis region name.
        output_usage : str or None
            When provided, only groups consumed by *output_usage* are
            returned.

        Returns
        -------
        dict[str, NuisanceGroup]
            Keys are individual systematic variation names (e.g. ``"JES"``).
            Values are the owning :class:`NuisanceGroup`.  When the same
            systematic appears in multiple groups only the first match is
            returned.
        """
        result: Dict[str, NuisanceGroup] = {}
        for group in self._groups:
            if not group.applies_to_process(process):
                continue
            if not group.applies_to_region(region):
                continue
            if output_usage is not None and not group.used_for_output(output_usage):
                continue
            for syst in group.systematics:
                if syst not in result:
                    result[syst] = group
        return result

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        """Validate all :class:`NuisanceGroup` definitions in the registry.

        Returns
        -------
        list[str]
            Validation error strings.  An empty list means all definitions
            are valid.
        """
        errors: List[str] = []
        seen_names: Set[str] = set()
        for group in self._groups:
            if group.name in seen_names:
                errors.append(
                    f"NuisanceGroupRegistry: duplicate group name '{group.name}'."
                )
            seen_names.add(group.name)
            errors.extend(group.validate())
        return errors

    def validate_coverage(
        self, available_variations: Dict[str, List[str]]
    ) -> List[CoverageIssue]:
        """Check that all declared systematics have up and down shifts present.

        Parameters
        ----------
        available_variations : dict[str, list[str]]
            Mapping of base systematic name (e.g. ``"JES"``) to the list of
            actual variation column/histogram names found in the output (e.g.
            ``["JESUp", "JESDown"]``).  Column names are compared
            case-insensitively when the suffix matching falls back to a
            normalised comparison.

        Returns
        -------
        list[CoverageIssue]
            Issues found.  An empty list means all declared systematics have
            complete up+down coverage.
        """
        issues: List[CoverageIssue] = []

        for group in self._groups:
            if not group.systematics:
                issues.append(
                    CoverageIssue(
                        severity=CoverageSeverity.WARNING,
                        group_name=group.name,
                        systematic_name="",
                        message=(
                            f"NuisanceGroup '{group.name}' has no systematics declared."
                        ),
                    )
                )
                continue

            for syst in group.systematics:
                variations = available_variations.get(syst, [])

                has_up = _has_variation(variations, syst, "up")
                has_down = _has_variation(variations, syst, "down")

                if not has_up and not has_down:
                    if syst not in available_variations:
                        issues.append(
                            CoverageIssue(
                                severity=CoverageSeverity.ERROR,
                                group_name=group.name,
                                systematic_name=syst,
                                message=(
                                    f"Systematic '{syst}' in group '{group.name}' "
                                    f"is not present in the available variations at all."
                                ),
                            )
                        )
                    else:
                        issues.append(
                            CoverageIssue(
                                severity=CoverageSeverity.ERROR,
                                group_name=group.name,
                                systematic_name=syst,
                                message=(
                                    f"Systematic '{syst}' in group '{group.name}' "
                                    f"has neither Up nor Down variation."
                                ),
                            )
                        )
                elif not has_up:
                    issues.append(
                        CoverageIssue(
                            severity=CoverageSeverity.ERROR,
                            group_name=group.name,
                            systematic_name=syst,
                            message=(
                                f"Systematic '{syst}' in group '{group.name}' "
                                f"is missing the Up variation."
                            ),
                        )
                    )
                elif not has_down:
                    issues.append(
                        CoverageIssue(
                            severity=CoverageSeverity.ERROR,
                            group_name=group.name,
                            systematic_name=syst,
                            message=(
                                f"Systematic '{syst}' in group '{group.name}' "
                                f"is missing the Down variation."
                            ),
                        )
                    )

        return issues

    # ------------------------------------------------------------------ I/O

    def to_dict(self) -> Dict[str, Any]:
        """Serialise the registry to a plain Python dict."""
        return {"groups": [g.to_dict() for g in self._groups]}

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NuisanceGroupRegistry":
        """Construct from a plain dict (as produced by :meth:`to_dict`)."""
        groups = [NuisanceGroup.from_dict(g) for g in data.get("groups", [])]
        return cls(groups=groups)

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "NuisanceGroupRegistry":
        """Construct from a user-supplied configuration dict.

        The config dict may contain either a top-level ``"groups"`` key (same
        format as :meth:`from_dict`) or a top-level ``"systematics"`` key
        using the flat dictionary format used by
        :class:`~create_datacards.DatacardGenerator`.  Both formats are
        recognised; the ``"groups"`` key takes precedence.

        Parameters
        ----------
        config : dict
            Configuration dictionary.

        Returns
        -------
        NuisanceGroupRegistry
        """
        if "groups" in config:
            return cls.from_dict(config)

        # Flat systematics dict format (from datacard YAML config)
        groups: List[NuisanceGroup] = []
        for syst_name, syst_cfg in config.get("systematics", {}).items():
            applies_to = syst_cfg.get("applies_to", {})
            if isinstance(applies_to, dict):
                processes = [p for p, v in applies_to.items() if v]
            else:
                processes = list(applies_to) if applies_to else []

            groups.append(
                NuisanceGroup(
                    name=syst_name,
                    group_type=syst_cfg.get("type", NuisanceGroupType.SHAPE.value),
                    systematics=[syst_name],
                    processes=processes,
                    regions=list(syst_cfg.get("regions", [])),
                    output_usage=list(syst_cfg.get("output_usage", [])),
                    description=syst_cfg.get("description", ""),
                    correlation_group=syst_cfg.get("correlation_group", ""),
                )
            )
        return cls(groups=groups)

    def save_yaml(self, path: str) -> None:
        """Write the registry to a YAML file at *path*."""
        with open(path, "w") as fh:
            yaml.dump(self.to_dict(), fh, default_flow_style=False, sort_keys=False)

    @classmethod
    def load_yaml(cls, path: str) -> "NuisanceGroupRegistry":
        """Load a registry from the YAML file at *path*."""
        with open(path) as fh:
            data = yaml.safe_load(fh)
        return cls.from_dict(data or {})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _has_variation(variation_names: List[str], base: str, direction: str) -> bool:
    """Return ``True`` when *variation_names* contains an up or down shift.

    The check is case-insensitive on the direction suffix (``Up``/``Down``).

    Parameters
    ----------
    variation_names : list[str]
        List of variation column/histogram names to search.
    base : str
        Base systematic name.
    direction : str
        ``"up"`` or ``"down"``.
    """
    suffix_canonical = direction.lower()
    for name in variation_names:
        if name.lower() == (base + suffix_canonical):
            return True
        if name.lower() == (base.lower() + suffix_canonical):
            return True
    return False
