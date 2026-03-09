"""
Manifest-driven branch map generation for analysis workflow tasks.

This module provides :func:`generate_branch_map`, which creates a
reproducible LAW-compatible branch map (``{int: BranchMapEntry}``) from
a :class:`~dataset_manifest.DatasetManifest` and an optional
:class:`~output_schema.OutputManifest`.

By default workflow tasks branch **only over datasets** – the same
behaviour as today.  Additional branching dimensions can be enabled via
:class:`BranchingPolicy`:

* **DATASET** – one branch per :class:`~dataset_manifest.DatasetEntry`
  (always active; included for completeness).
* **REGION** – one branch per named region declared in
  ``OutputManifest.regions``.  Expands the branch count by the number of
  regions.
* **SYSTEMATIC_SCOPE** – one branch per nuisance-group scope declared in
  ``OutputManifest.nuisance_groups``.  Can be filtered to a specific
  output usage (e.g. ``"datacard"``).  Expands the branch count by the
  number of matching groups.

Scaling guidance
----------------
The three dimensions multiply, so use caution:

+---------------+---------+---------+---------------+--------------------+
| Datasets (D)  | Regions | Groups  | Total branches | Recommended policy |
+===============+=========+=========+================+====================+
| 10            | 1       | 1       | 10             | ``DATASET`` only   |
+---------------+---------+---------+---------------+--------------------+
| 10            | 5       | 1       | 50             | ``DATASET+REGION`` |
+---------------+---------+---------+---------------+--------------------+
| 10            | 5       | 8       | 400            | split systematics  |
+---------------+---------+---------+---------------+--------------------+
| 50            | 10      | 20      | 10 000         | **not recommended**|
+---------------+---------+---------+---------------+--------------------+

Use :attr:`BranchingPolicy.max_branches` to impose a hard cap.  The
default cap is ``None`` (no limit); set it conservatively when deploying
to a grid cluster with limited job slots.

Usage example
-------------
::

    from branch_map_policy import BranchingPolicy, BranchingDimension, generate_branch_map
    from dataset_manifest import DatasetManifest
    from output_schema import OutputManifest

    policy = BranchingPolicy(
        dimensions=[BranchingDimension.DATASET, BranchingDimension.REGION],
        max_branches=500,
    )

    ds_manifest = DatasetManifest.load("datasets.yaml")
    out_manifest = OutputManifest.load_yaml("output_manifest.yaml")

    branch_map = generate_branch_map(policy, ds_manifest, out_manifest)
    # {0: BranchMapEntry(dataset=..., region="signal", systematic_scope=None), ...}

In a LAW workflow task::

    def create_branch_map(self):
        policy = BranchingPolicy.from_config_str(self.branching_policy)
        ds_manifest = DatasetManifest.load(self.dataset_manifest)
        out_manifest = (
            OutputManifest.load_yaml(self.output_manifest)
            if self.output_manifest
            else None
        )
        return generate_branch_map(policy, ds_manifest, out_manifest)
"""

from __future__ import annotations

import enum
import itertools
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence

# ---------------------------------------------------------------------------
# Public API – re-exported symbols
# ---------------------------------------------------------------------------

__all__ = [
    "BranchingDimension",
    "BranchMapEntry",
    "BranchingPolicy",
    "generate_branch_map",
    "BranchMapGenerationError",
]


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class BranchMapGenerationError(RuntimeError):
    """Raised when branch map generation fails or would exceed policy limits."""


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------

class BranchingDimension(str, enum.Enum):
    """Dimensions over which workflow tasks can branch.

    Attributes
    ----------
    DATASET:
        Branch per :class:`~dataset_manifest.DatasetEntry` in the dataset
        manifest.  This dimension is **always** active regardless of which
        dimensions appear in :attr:`BranchingPolicy.dimensions`.
    REGION:
        Branch per named region declared in ``OutputManifest.regions``.
        Requires a non-``None`` ``output_manifest`` to be passed to
        :func:`generate_branch_map`, otherwise this dimension is silently
        skipped (no explosion).
    SYSTEMATIC_SCOPE:
        Branch per nuisance-group scope declared in
        ``OutputManifest.nuisance_groups``.  Can be narrowed to groups that
        match a specific ``output_usage`` string via
        :attr:`BranchingPolicy.systematic_output_usage`.  Requires a
        non-``None`` ``output_manifest``; silently skipped otherwise.
    """

    DATASET = "dataset"
    REGION = "region"
    SYSTEMATIC_SCOPE = "systematic_scope"


# ---------------------------------------------------------------------------
# Branch entry
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class BranchMapEntry:
    """Data carried by a single analysis branch.

    A branch entry is a fully-qualified coordinate in the analysis
    parameter space.  All three fields together uniquely identify a
    unit of work.

    Attributes
    ----------
    dataset_name : str
        Name of the :class:`~dataset_manifest.DatasetEntry` assigned to
        this branch.
    region : str or None
        Name of the analysis region for this branch, or ``None`` when
        branching does **not** include the :attr:`~BranchingDimension.REGION`
        dimension (i.e. a region-agnostic job that processes all regions
        internally).
    systematic_scope : str or None
        Name of the nuisance group assigned to this branch, or ``None``
        when systematic variations are handled internally by the job
        (the default for most analysis passes).
    """

    dataset_name: str
    region: Optional[str] = None
    systematic_scope: Optional[str] = None

    def __repr__(self) -> str:  # pragma: no cover
        parts = [f"dataset={self.dataset_name!r}"]
        if self.region is not None:
            parts.append(f"region={self.region!r}")
        if self.systematic_scope is not None:
            parts.append(f"systematic_scope={self.systematic_scope!r}")
        return f"BranchMapEntry({', '.join(parts)})"


# ---------------------------------------------------------------------------
# Policy
# ---------------------------------------------------------------------------

@dataclass
class BranchingPolicy:
    """Controls how analysis tasks expand into LAW branches.

    The policy declares which *dimensions* drive the branching and
    imposes optional safeguards against unintentional combinatorial
    explosion.

    Attributes
    ----------
    dimensions : list[BranchingDimension]
        Ordered list of dimensions to branch over.  The
        :attr:`~BranchingDimension.DATASET` dimension is always implicit
        even if omitted here.  Duplicate entries are ignored.

        **Safe defaults** – start with ``[BranchingDimension.DATASET]``
        and only add ``REGION`` or ``SYSTEMATIC_SCOPE`` when the cluster
        has sufficient job slots and the manifests have been reviewed.

    max_branches : int or None
        Hard upper limit on the total number of branches.  When the
        generated branch count would exceed this value
        :func:`generate_branch_map` raises
        :class:`BranchMapGenerationError` rather than silently submitting
        thousands of jobs.  ``None`` (the default) disables the cap.

    systematic_output_usage : str or None
        When ``SYSTEMATIC_SCOPE`` is an active dimension, only include
        nuisance groups whose ``output_usage`` list contains this string
        (or groups with an empty ``output_usage`` list, which means "all
        usages").  Typical values: ``"datacard"``, ``"histogram"``,
        ``"plot"``.  ``None`` means include all groups.

    systematic_group_names : list[str]
        When non-empty, only nuisance groups whose ``name`` appears in
        this list will be expanded as separate branches.  This is useful
        for re-running a targeted subset of systematics without recreating
        all branches.  An empty list (the default) includes all groups.
    """

    dimensions: List[BranchingDimension] = field(
        default_factory=lambda: [BranchingDimension.DATASET]
    )
    max_branches: Optional[int] = None
    systematic_output_usage: Optional[str] = None
    systematic_group_names: List[str] = field(default_factory=list)

    # ------------------------------------------------------------------ factories

    @classmethod
    def dataset_only(cls) -> "BranchingPolicy":
        """Return the minimal safe-default policy: one branch per dataset.

        This replicates the pre-existing ``create_branch_map`` behaviour
        for :class:`SkimTask` and :class:`HistFillTask`.
        """
        return cls(dimensions=[BranchingDimension.DATASET])

    @classmethod
    def dataset_and_regions(
        cls,
        max_branches: Optional[int] = None,
    ) -> "BranchingPolicy":
        """Return a policy that branches over datasets × regions.

        Parameters
        ----------
        max_branches:
            Optional cap.  Recommend setting this when the number of
            datasets × regions is unknown (e.g. during iterative
            development).
        """
        return cls(
            dimensions=[BranchingDimension.DATASET, BranchingDimension.REGION],
            max_branches=max_branches,
        )

    @classmethod
    def dataset_regions_and_systematics(
        cls,
        systematic_output_usage: Optional[str] = None,
        systematic_group_names: Optional[Sequence[str]] = None,
        max_branches: Optional[int] = None,
    ) -> "BranchingPolicy":
        """Return a policy that branches over datasets × regions × systematic scopes.

        This is the most granular policy and can produce a very large
        number of branches.  Always set ``max_branches`` when using it.

        Parameters
        ----------
        systematic_output_usage:
            Restrict systematic groups to those that serve this output
            (e.g. ``"datacard"``).  ``None`` includes all groups.
        systematic_group_names:
            Explicit allow-list of group names.  Empty / ``None`` = all.
        max_branches:
            Hard cap.  Strongly recommended.
        """
        return cls(
            dimensions=[
                BranchingDimension.DATASET,
                BranchingDimension.REGION,
                BranchingDimension.SYSTEMATIC_SCOPE,
            ],
            max_branches=max_branches,
            systematic_output_usage=systematic_output_usage,
            systematic_group_names=list(systematic_group_names or []),
        )

    @classmethod
    def from_config_str(cls, config_str: str) -> "BranchingPolicy":
        """Parse a compact policy descriptor string.

        The string encodes the policy as a comma-separated list of
        ``key=value`` pairs.  The following keys are recognised:

        ``dims``
            Colon-separated list of dimension names.  Valid values are
            ``dataset``, ``region``, ``systematic_scope``.
            Example: ``dims=dataset:region``.
        ``max_branches``
            Integer hard cap, or ``none`` for no cap.
            Example: ``max_branches=200``.
        ``systematic_usage``
            Value passed to :attr:`systematic_output_usage`.
            Example: ``systematic_usage=datacard``.
        ``systematic_groups``
            Colon-separated allow-list of nuisance group names.
            Example: ``systematic_groups=jet_energy:b_tagging``.

        If *config_str* is empty or ``"default"`` the
        :meth:`dataset_only` policy is returned.

        Examples
        --------
        ``"dims=dataset:region,max_branches=100"``
            Branch over datasets and regions with a cap of 100.

        ``"dims=dataset:region:systematic_scope,systematic_usage=datacard"``
            Full branching but restricted to datacard systematics.
        """
        config_str = config_str.strip()
        if not config_str or config_str == "default":
            return cls.dataset_only()

        kwargs: Dict = {}
        for token in config_str.split(","):
            token = token.strip()
            if not token:
                continue
            if "=" not in token:
                raise BranchMapGenerationError(
                    f"Invalid policy token {token!r}: expected 'key=value'."
                )
            key, _, value = token.partition("=")
            key = key.strip()
            value = value.strip()

            if key == "dims":
                raw_dims = [d.strip() for d in value.split(":") if d.strip()]
                try:
                    kwargs["dimensions"] = [BranchingDimension(d) for d in raw_dims]
                except ValueError as exc:
                    valid = [d.value for d in BranchingDimension]
                    raise BranchMapGenerationError(
                        f"Unknown branching dimension in policy string: {exc}.  "
                        f"Valid values: {valid}."
                    ) from exc

            elif key == "max_branches":
                if value.lower() == "none":
                    kwargs["max_branches"] = None
                else:
                    try:
                        kwargs["max_branches"] = int(value)
                    except ValueError:
                        raise BranchMapGenerationError(
                            f"max_branches must be an integer or 'none', got {value!r}."
                        )

            elif key == "systematic_usage":
                kwargs["systematic_output_usage"] = value or None

            elif key == "systematic_groups":
                kwargs["systematic_group_names"] = [
                    g.strip() for g in value.split(":") if g.strip()
                ]

            else:
                raise BranchMapGenerationError(
                    f"Unknown policy key {key!r}.  Valid keys: "
                    "dims, max_branches, systematic_usage, systematic_groups."
                )

        return cls(**kwargs)

    # ------------------------------------------------------------------ validation

    def validate(self) -> List[str]:
        """Return a list of validation error strings (empty = valid).

        Does **not** raise; callers decide whether to treat warnings as
        hard failures.
        """
        errors: List[str] = []
        if self.max_branches is not None and self.max_branches < 1:
            errors.append(
                f"BranchingPolicy.max_branches must be >= 1 or None, "
                f"got {self.max_branches}."
            )

        seen: set = set()
        for dim in self.dimensions:
            if not isinstance(dim, BranchingDimension):
                errors.append(
                    f"BranchingPolicy.dimensions contains invalid value "
                    f"{dim!r} (expected a BranchingDimension)."
                )
            elif dim in seen:
                errors.append(
                    f"BranchingPolicy.dimensions has duplicate entry {dim!r}."
                )
            seen.add(dim)

        if (
            BranchingDimension.SYSTEMATIC_SCOPE in self.dimensions
            and BranchingDimension.DATASET not in self.dimensions
        ):
            errors.append(
                "SYSTEMATIC_SCOPE requires DATASET to also be in dimensions."
            )
        return errors


# ---------------------------------------------------------------------------
# Core generation function
# ---------------------------------------------------------------------------

def generate_branch_map(
    policy: BranchingPolicy,
    dataset_manifest: "DatasetManifest",  # noqa: F821  (imported at call site)
    output_manifest: "Optional[OutputManifest]" = None,  # noqa: F821
) -> Dict[int, BranchMapEntry]:
    """Generate a reproducible LAW branch map from analysis manifests.

    Parameters
    ----------
    policy : BranchingPolicy
        Controls which dimensions are active and applies safety limits.
    dataset_manifest : DatasetManifest
        Loaded dataset manifest providing the ordered list of
        :class:`~dataset_manifest.DatasetEntry` objects.
    output_manifest : OutputManifest or None
        Loaded output manifest providing ``regions`` and
        ``nuisance_groups``.  When ``None`` the :attr:`~BranchingDimension.REGION`
        and :attr:`~BranchingDimension.SYSTEMATIC_SCOPE` dimensions are
        silently treated as empty (producing ``region=None`` /
        ``systematic_scope=None`` entries).

    Returns
    -------
    dict[int, BranchMapEntry]
        Mapping of integer branch indices (0-based, contiguous) to
        :class:`BranchMapEntry` objects.  The mapping is fully
        reproducible: given the same inputs and policy the same map is
        always produced.

    Raises
    ------
    BranchMapGenerationError
        When policy validation fails or the computed branch count exceeds
        :attr:`BranchingPolicy.max_branches`.

    Examples
    --------
    >>> from branch_map_policy import BranchingPolicy, generate_branch_map
    >>> policy = BranchingPolicy.dataset_only()
    >>> branch_map = generate_branch_map(policy, dataset_manifest)
    >>> branch_map[0]
    BranchMapEntry(dataset='ttbar')
    """
    # ---- validate policy ---------------------------------------------------
    policy_errors = policy.validate()
    if policy_errors:
        raise BranchMapGenerationError(
            "BranchingPolicy is invalid:\n" + "\n".join(f"  - {e}" for e in policy_errors)
        )

    active_dims = set(policy.dimensions) | {BranchingDimension.DATASET}

    # ---- datasets ----------------------------------------------------------
    datasets: List[str] = sorted(
        entry.name for entry in dataset_manifest.datasets
    )
    if not datasets:
        return {}

    # ---- regions -----------------------------------------------------------
    regions: List[Optional[str]]
    if BranchingDimension.REGION in active_dims:
        raw_regions: List[str] = []
        if output_manifest is not None and output_manifest.regions:
            raw_regions = sorted(r.name for r in output_manifest.regions if r.name)
        if raw_regions:
            regions = raw_regions  # type: ignore[assignment]
        else:
            # No regions declared – produce region=None entries so the
            # branch count stays at D rather than 0.
            regions = [None]
    else:
        regions = [None]

    # ---- systematic scopes -------------------------------------------------
    systematic_scopes: List[Optional[str]]
    if BranchingDimension.SYSTEMATIC_SCOPE in active_dims:
        raw_groups: List[str] = []
        if output_manifest is not None and output_manifest.nuisance_groups:
            for ng in output_manifest.nuisance_groups:
                if not ng.name:
                    continue
                # Filter by output usage if requested.
                if policy.systematic_output_usage is not None:
                    usages = ng.output_usage
                    if usages and policy.systematic_output_usage not in usages:
                        continue
                # Filter by explicit group name allow-list.
                if (
                    policy.systematic_group_names
                    and ng.name not in policy.systematic_group_names
                ):
                    continue
                raw_groups.append(ng.name)
        raw_groups.sort()
        if raw_groups:
            systematic_scopes = raw_groups  # type: ignore[assignment]
        else:
            systematic_scopes = [None]
    else:
        systematic_scopes = [None]

    # ---- build cross-product -----------------------------------------------
    # Order: datasets (outer) → regions → systematic_scopes (inner).
    # Sorting each axis ensures a reproducible ordering.
    entries: List[BranchMapEntry] = [
        BranchMapEntry(
            dataset_name=ds,
            region=reg,
            systematic_scope=scope,
        )
        for ds, reg, scope in itertools.product(datasets, regions, systematic_scopes)
    ]

    # ---- apply max_branches guard ------------------------------------------
    if policy.max_branches is not None and len(entries) > policy.max_branches:
        raise BranchMapGenerationError(
            f"Branch map would contain {len(entries)} branches, which exceeds "
            f"BranchingPolicy.max_branches={policy.max_branches}.  "
            "Increase the cap or narrow the policy dimensions to avoid "
            "unintentional combinatorial explosion."
        )

    return {i: entry for i, entry in enumerate(entries)}
