#!/usr/bin/env python3
"""
Tests for the branch_map_policy module.

Covers:
  - BranchingDimension enum values
  - BranchMapEntry: frozen dataclass, fields, repr
  - BranchingPolicy: defaults, factory methods, from_config_str, validate
  - generate_branch_map:
      * dataset-only (safe default, replicates legacy create_branch_map)
      * dataset + region
      * dataset + region + systematic_scope
      * max_branches enforcement
      * output_manifest=None graceful fallback
      * empty manifests
      * systematic_output_usage filter
      * systematic_group_names allow-list
      * reproducibility (same inputs → same map)
  - BranchMapGenerationError raised correctly
"""

from __future__ import annotations

import os
import sys
import unittest
from dataclasses import dataclass
from typing import List, Optional
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PY = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from branch_map_policy import (  # noqa: E402
    BranchingDimension,
    BranchMapEntry,
    BranchingPolicy,
    BranchMapGenerationError,
    generate_branch_map,
)


# ---------------------------------------------------------------------------
# Helpers – lightweight stubs so tests don't need real YAML or ROOT
# ---------------------------------------------------------------------------

def _make_dataset_manifest(names: List[str]):
    """Return a mock DatasetManifest with the given dataset names."""
    manifest = MagicMock()

    @dataclass
    class _Entry:
        name: str

    manifest.datasets = [_Entry(name=n) for n in names]
    return manifest


def _make_output_manifest(
    regions: Optional[List[str]] = None,
    nuisance_groups: Optional[List[dict]] = None,
):
    """Return a mock OutputManifest with the given region/nuisance data."""
    from output_schema import OutputManifest, RegionDefinition, NuisanceGroupDefinition

    region_defs = [
        RegionDefinition(name=r, filter_column=f"is_{r}")
        for r in (regions or [])
    ]
    group_defs = []
    for g in (nuisance_groups or []):
        group_defs.append(
            NuisanceGroupDefinition(
                name=g["name"],
                group_type=g.get("group_type", "shape"),
                systematics=g.get("systematics", []),
                output_usage=g.get("output_usage", []),
            )
        )
    return OutputManifest(regions=region_defs, nuisance_groups=group_defs)


# ===========================================================================
# BranchingDimension
# ===========================================================================

class TestBranchingDimension(unittest.TestCase):
    def test_values(self):
        self.assertEqual(BranchingDimension.DATASET.value, "dataset")
        self.assertEqual(BranchingDimension.REGION.value, "region")
        self.assertEqual(BranchingDimension.SYSTEMATIC_SCOPE.value, "systematic_scope")

    def test_str_enum(self):
        """BranchingDimension should be comparable to plain strings."""
        self.assertEqual(BranchingDimension.DATASET, "dataset")


# ===========================================================================
# BranchMapEntry
# ===========================================================================

class TestBranchMapEntry(unittest.TestCase):
    def test_defaults(self):
        e = BranchMapEntry(dataset_name="ttbar")
        self.assertEqual(e.dataset_name, "ttbar")
        self.assertIsNone(e.region)
        self.assertIsNone(e.systematic_scope)

    def test_full(self):
        e = BranchMapEntry(
            dataset_name="ttbar",
            region="signal",
            systematic_scope="jet_energy",
        )
        self.assertEqual(e.region, "signal")
        self.assertEqual(e.systematic_scope, "jet_energy")

    def test_frozen(self):
        """BranchMapEntry must be immutable."""
        e = BranchMapEntry(dataset_name="ttbar")
        with self.assertRaises(Exception):
            e.dataset_name = "other"  # type: ignore[misc]

    def test_hashable(self):
        """Frozen dataclasses must be usable as dict keys."""
        e1 = BranchMapEntry(dataset_name="a", region="r1")
        e2 = BranchMapEntry(dataset_name="a", region="r1")
        self.assertEqual(e1, e2)
        self.assertIn(e1, {e2})


# ===========================================================================
# BranchingPolicy – construction and validation
# ===========================================================================

class TestBranchingPolicy(unittest.TestCase):

    def test_defaults(self):
        p = BranchingPolicy()
        self.assertEqual(p.dimensions, [BranchingDimension.DATASET])
        self.assertIsNone(p.max_branches)
        self.assertIsNone(p.systematic_output_usage)
        self.assertEqual(p.systematic_group_names, [])

    def test_dataset_only_factory(self):
        p = BranchingPolicy.dataset_only()
        self.assertEqual(p.dimensions, [BranchingDimension.DATASET])
        self.assertIsNone(p.max_branches)

    def test_dataset_and_regions_factory(self):
        p = BranchingPolicy.dataset_and_regions(max_branches=50)
        self.assertIn(BranchingDimension.REGION, p.dimensions)
        self.assertEqual(p.max_branches, 50)

    def test_dataset_regions_and_systematics_factory(self):
        p = BranchingPolicy.dataset_regions_and_systematics(
            systematic_output_usage="datacard",
            systematic_group_names=["jes", "jer"],
            max_branches=200,
        )
        self.assertIn(BranchingDimension.SYSTEMATIC_SCOPE, p.dimensions)
        self.assertEqual(p.systematic_output_usage, "datacard")
        self.assertEqual(p.systematic_group_names, ["jes", "jer"])
        self.assertEqual(p.max_branches, 200)

    def test_validate_ok(self):
        p = BranchingPolicy.dataset_only()
        self.assertEqual(p.validate(), [])

    def test_validate_bad_max_branches(self):
        p = BranchingPolicy(max_branches=0)
        errs = p.validate()
        self.assertTrue(any("max_branches" in e for e in errs))

    def test_validate_duplicate_dim(self):
        p = BranchingPolicy(
            dimensions=[BranchingDimension.DATASET, BranchingDimension.DATASET]
        )
        errs = p.validate()
        self.assertTrue(any("duplicate" in e.lower() for e in errs))

    def test_validate_systematic_without_dataset(self):
        p = BranchingPolicy(
            dimensions=[BranchingDimension.SYSTEMATIC_SCOPE]
        )
        errs = p.validate()
        self.assertTrue(any("SYSTEMATIC_SCOPE requires DATASET" in e for e in errs))


# ===========================================================================
# BranchingPolicy.from_config_str
# ===========================================================================

class TestBranchingPolicyFromConfigStr(unittest.TestCase):

    def test_empty_string_gives_default(self):
        p = BranchingPolicy.from_config_str("")
        self.assertEqual(p.dimensions, [BranchingDimension.DATASET])

    def test_default_keyword(self):
        p = BranchingPolicy.from_config_str("default")
        self.assertEqual(p.dimensions, [BranchingDimension.DATASET])

    def test_dims_dataset_region(self):
        p = BranchingPolicy.from_config_str("dims=dataset:region")
        self.assertIn(BranchingDimension.DATASET, p.dimensions)
        self.assertIn(BranchingDimension.REGION, p.dimensions)

    def test_dims_all_three(self):
        p = BranchingPolicy.from_config_str(
            "dims=dataset:region:systematic_scope"
        )
        self.assertEqual(len(p.dimensions), 3)

    def test_max_branches(self):
        p = BranchingPolicy.from_config_str("max_branches=42")
        self.assertEqual(p.max_branches, 42)

    def test_max_branches_none(self):
        p = BranchingPolicy.from_config_str("max_branches=none")
        self.assertIsNone(p.max_branches)

    def test_systematic_usage(self):
        p = BranchingPolicy.from_config_str("systematic_usage=datacard")
        self.assertEqual(p.systematic_output_usage, "datacard")

    def test_systematic_groups(self):
        p = BranchingPolicy.from_config_str("systematic_groups=jes:jer:lumi")
        self.assertEqual(p.systematic_group_names, ["jes", "jer", "lumi"])

    def test_combined_string(self):
        p = BranchingPolicy.from_config_str(
            "dims=dataset:region,max_branches=100,systematic_usage=histogram"
        )
        self.assertIn(BranchingDimension.REGION, p.dimensions)
        self.assertEqual(p.max_branches, 100)
        self.assertEqual(p.systematic_output_usage, "histogram")

    def test_unknown_key_raises(self):
        with self.assertRaises(BranchMapGenerationError):
            BranchingPolicy.from_config_str("unknownkey=value")

    def test_bad_dimension_raises(self):
        with self.assertRaises(BranchMapGenerationError):
            BranchingPolicy.from_config_str("dims=dataset:notadimension")

    def test_bad_max_branches_raises(self):
        with self.assertRaises(BranchMapGenerationError):
            BranchingPolicy.from_config_str("max_branches=abc")

    def test_missing_equals_raises(self):
        with self.assertRaises(BranchMapGenerationError):
            BranchingPolicy.from_config_str("dims")


# ===========================================================================
# generate_branch_map – dataset-only (legacy parity)
# ===========================================================================

class TestGenerateBranchMapDatasetOnly(unittest.TestCase):

    def _policy(self):
        return BranchingPolicy.dataset_only()

    def test_empty_manifest(self):
        ds = _make_dataset_manifest([])
        bm = generate_branch_map(self._policy(), ds)
        self.assertEqual(bm, {})

    def test_single_dataset(self):
        ds = _make_dataset_manifest(["ttbar"])
        bm = generate_branch_map(self._policy(), ds)
        self.assertEqual(len(bm), 1)
        self.assertEqual(bm[0].dataset_name, "ttbar")
        self.assertIsNone(bm[0].region)
        self.assertIsNone(bm[0].systematic_scope)

    def test_multiple_datasets_sorted(self):
        ds = _make_dataset_manifest(["wjets", "ttbar", "dy"])
        bm = generate_branch_map(self._policy(), ds)
        self.assertEqual(len(bm), 3)
        names = [bm[i].dataset_name for i in range(3)]
        self.assertEqual(names, sorted(names))

    def test_keys_are_contiguous_integers(self):
        ds = _make_dataset_manifest(["a", "b", "c"])
        bm = generate_branch_map(self._policy(), ds)
        self.assertEqual(set(bm.keys()), {0, 1, 2})

    def test_output_manifest_none_no_error(self):
        ds = _make_dataset_manifest(["ttbar"])
        bm = generate_branch_map(self._policy(), ds, output_manifest=None)
        self.assertEqual(len(bm), 1)

    def test_output_manifest_with_regions_ignored(self):
        """REGION dimension not active → regions are ignored."""
        ds = _make_dataset_manifest(["ttbar"])
        out = _make_output_manifest(regions=["signal", "control"])
        bm = generate_branch_map(self._policy(), ds, output_manifest=out)
        self.assertEqual(len(bm), 1)
        self.assertIsNone(bm[0].region)


# ===========================================================================
# generate_branch_map – dataset + region
# ===========================================================================

class TestGenerateBranchMapDatasetRegion(unittest.TestCase):

    def _policy(self, **kw):
        return BranchingPolicy.dataset_and_regions(**kw)

    def test_two_datasets_two_regions(self):
        ds = _make_dataset_manifest(["ttbar", "wjets"])
        out = _make_output_manifest(regions=["signal", "control"])
        bm = generate_branch_map(self._policy(), ds, out)
        self.assertEqual(len(bm), 4)  # 2 × 2

    def test_region_dimension_missing_from_manifest(self):
        """When no regions are in the output manifest, region=None entries are produced."""
        ds = _make_dataset_manifest(["ttbar", "wjets"])
        out = _make_output_manifest(regions=[])  # empty
        bm = generate_branch_map(self._policy(), ds, out)
        self.assertEqual(len(bm), 2)  # falls back to region=None
        for entry in bm.values():
            self.assertIsNone(entry.region)

    def test_region_dimension_no_output_manifest(self):
        """When output_manifest=None, region dimension silently becomes [None]."""
        ds = _make_dataset_manifest(["ttbar"])
        bm = generate_branch_map(self._policy(), ds, output_manifest=None)
        self.assertEqual(len(bm), 1)
        self.assertIsNone(bm[0].region)

    def test_entries_are_sorted(self):
        """Entries should be ordered: datasets (outer) × regions (inner)."""
        ds = _make_dataset_manifest(["b_dataset", "a_dataset"])
        out = _make_output_manifest(regions=["z_region", "a_region"])
        bm = generate_branch_map(self._policy(), ds, out)
        # Outer loop: sorted datasets; inner loop: sorted regions.
        expected_ds = ["a_dataset", "a_dataset", "b_dataset", "b_dataset"]
        expected_reg = ["a_region", "z_region", "a_region", "z_region"]
        for i in range(4):
            self.assertEqual(bm[i].dataset_name, expected_ds[i])
            self.assertEqual(bm[i].region, expected_reg[i])

    def test_max_branches_respected(self):
        ds = _make_dataset_manifest(["a", "b", "c"])
        out = _make_output_manifest(regions=["r1", "r2"])
        policy = self._policy(max_branches=5)  # 3×2=6 > 5
        with self.assertRaises(BranchMapGenerationError) as ctx:
            generate_branch_map(policy, ds, out)
        self.assertIn("max_branches", str(ctx.exception))

    def test_max_branches_exactly_met(self):
        ds = _make_dataset_manifest(["a", "b", "c"])
        out = _make_output_manifest(regions=["r1", "r2"])
        policy = self._policy(max_branches=6)  # exactly 3×2=6
        bm = generate_branch_map(policy, ds, out)
        self.assertEqual(len(bm), 6)


# ===========================================================================
# generate_branch_map – dataset + region + systematic_scope
# ===========================================================================

class TestGenerateBranchMapSystematicScope(unittest.TestCase):

    _GROUPS = [
        {"name": "jet_energy", "output_usage": ["datacard", "histogram"]},
        {"name": "b_tagging",  "output_usage": ["datacard"]},
        {"name": "lumi",       "output_usage": []},
    ]

    def _policy(self, **kw):
        return BranchingPolicy.dataset_regions_and_systematics(**kw)

    def test_full_expansion(self):
        ds = _make_dataset_manifest(["ttbar", "wjets"])
        out = _make_output_manifest(
            regions=["signal"],
            nuisance_groups=self._GROUPS,
        )
        bm = generate_branch_map(self._policy(max_branches=100), ds, out)
        # 2 datasets × 1 region × 3 groups = 6
        self.assertEqual(len(bm), 6)

    def test_systematic_output_usage_filter(self):
        ds = _make_dataset_manifest(["ttbar"])
        out = _make_output_manifest(
            regions=["signal"],
            nuisance_groups=self._GROUPS,
        )
        policy = self._policy(systematic_output_usage="datacard")
        bm = generate_branch_map(policy, ds, out)
        # jet_energy (datacard+histogram) and b_tagging (datacard) qualify;
        # lumi (empty = all) also qualifies
        scopes = {e.systematic_scope for e in bm.values()}
        self.assertIn("jet_energy", scopes)
        self.assertIn("b_tagging", scopes)
        self.assertIn("lumi", scopes)  # empty output_usage → applies to all

    def test_systematic_output_usage_filter_histogram_only(self):
        ds = _make_dataset_manifest(["ttbar"])
        out = _make_output_manifest(
            regions=["signal"],
            nuisance_groups=self._GROUPS,
        )
        policy = self._policy(systematic_output_usage="histogram")
        bm = generate_branch_map(policy, ds, out)
        scopes = {e.systematic_scope for e in bm.values()}
        self.assertIn("jet_energy", scopes)   # has histogram
        self.assertNotIn("b_tagging", scopes) # datacard only
        self.assertIn("lumi", scopes)         # empty = all

    def test_systematic_group_names_allow_list(self):
        ds = _make_dataset_manifest(["ttbar"])
        out = _make_output_manifest(
            regions=["signal"],
            nuisance_groups=self._GROUPS,
        )
        policy = self._policy(systematic_group_names=["b_tagging"])
        bm = generate_branch_map(policy, ds, out)
        scopes = {e.systematic_scope for e in bm.values()}
        self.assertEqual(scopes, {"b_tagging"})

    def test_no_groups_falls_back_to_none(self):
        """When no matching groups exist, systematic_scope=None is used."""
        ds = _make_dataset_manifest(["ttbar"])
        out = _make_output_manifest(regions=["signal"], nuisance_groups=[])
        bm = generate_branch_map(self._policy(), ds, out)
        self.assertEqual(len(bm), 1)
        self.assertIsNone(bm[0].systematic_scope)

    def test_no_output_manifest_falls_back(self):
        ds = _make_dataset_manifest(["ttbar"])
        bm = generate_branch_map(self._policy(), ds, output_manifest=None)
        self.assertEqual(len(bm), 1)
        self.assertIsNone(bm[0].region)
        self.assertIsNone(bm[0].systematic_scope)

    def test_max_branches_guard(self):
        groups = [{"name": f"g{i}", "output_usage": []} for i in range(10)]
        ds = _make_dataset_manifest([f"ds{i}" for i in range(5)])
        out = _make_output_manifest(
            regions=[f"r{i}" for i in range(4)],
            nuisance_groups=groups,
        )
        policy = self._policy(max_branches=100)  # 5×4×10=200 > 100
        with self.assertRaises(BranchMapGenerationError):
            generate_branch_map(policy, ds, out)


# ===========================================================================
# Reproducibility
# ===========================================================================

class TestReproducibility(unittest.TestCase):

    def test_same_inputs_same_map(self):
        """generate_branch_map must be deterministic."""
        ds = _make_dataset_manifest(["c", "a", "b"])
        out = _make_output_manifest(
            regions=["r2", "r1"],
            nuisance_groups=[
                {"name": "ng2", "output_usage": []},
                {"name": "ng1", "output_usage": []},
            ],
        )
        policy = BranchingPolicy.dataset_regions_and_systematics()
        bm1 = generate_branch_map(policy, ds, out)
        bm2 = generate_branch_map(policy, ds, out)
        self.assertEqual(len(bm1), len(bm2))
        for i in bm1:
            self.assertEqual(bm1[i], bm2[i])

    def test_sorting_is_alphabetical(self):
        """Dataset, region, and systematic names should be sorted alphabetically."""
        ds = _make_dataset_manifest(["z_ds", "a_ds", "m_ds"])
        out = _make_output_manifest(
            regions=["z_reg", "a_reg"],
            nuisance_groups=[
                {"name": "z_group", "output_usage": []},
                {"name": "a_group", "output_usage": []},
            ],
        )
        policy = BranchingPolicy.dataset_regions_and_systematics()
        bm = generate_branch_map(policy, ds, out)
        # First entries should be a_ds × a_reg × a_group
        self.assertEqual(bm[0].dataset_name, "a_ds")
        self.assertEqual(bm[0].region, "a_reg")
        self.assertEqual(bm[0].systematic_scope, "a_group")


# ===========================================================================
# Error cases
# ===========================================================================

class TestBranchMapGenerationErrors(unittest.TestCase):

    def test_invalid_policy_raises(self):
        p = BranchingPolicy(max_branches=0)
        ds = _make_dataset_manifest(["ttbar"])
        with self.assertRaises(BranchMapGenerationError):
            generate_branch_map(p, ds)

    def test_max_branches_exceeded_message(self):
        ds = _make_dataset_manifest(["a", "b"])
        out = _make_output_manifest(regions=["r1", "r2"])
        policy = BranchingPolicy.dataset_and_regions(max_branches=2)
        with self.assertRaises(BranchMapGenerationError) as ctx:
            generate_branch_map(policy, ds, out)
        self.assertIn("4", str(ctx.exception))  # 2×2 = 4 in error message


# ===========================================================================
# from_config_str round-trip
# ===========================================================================

class TestFromConfigStrRoundTrip(unittest.TestCase):

    def test_config_str_drives_branch_count(self):
        """Ensure from_config_str correctly drives generate_branch_map."""
        ds = _make_dataset_manifest(["ttbar", "wjets"])
        out = _make_output_manifest(regions=["signal", "control"])
        policy = BranchingPolicy.from_config_str("dims=dataset:region")
        bm = generate_branch_map(policy, ds, out)
        self.assertEqual(len(bm), 4)  # 2 × 2


if __name__ == "__main__":
    unittest.main()
