#!/usr/bin/env python3
"""
Tests for the nano_tasks Law module.

These tests verify the XRootD redirector utility is correctly enforced on
all NANO dataset file URIs, without requiring a running Law scheduler, an
HTCondor cluster, Rucio, or a VOMS proxy.
"""

from __future__ import annotations

import os
import sys
import unittest

# ---------------------------------------------------------------------------
# Make sure law/ and core/python are importable
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PYTHON):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optionally import law / luigi so we can skip when they are absent
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestQueryRucioRedirectorEnforcement(unittest.TestCase):
    """Tests that _query_rucio produces XRootD URLs via ensure_xrootd_redirector."""

    def _import(self):
        import nano_tasks
        return nano_tasks

    def _make_rucio_entry(self, name, states=None, size_bytes=1_000_000):
        return {
            "name": name,
            "states": states or {},
            "bytes": size_bytes,
        }

    def test_bare_lfn_gets_redirector(self):
        """A bare LFN from Rucio gets the default CMS redirector prepended."""
        mod = self._import()
        lfn = "/store/data/Run2018A/Charmonium/MINIAOD/17Sep2018-v1/110000/foo.root"
        entry = self._make_rucio_entry(lfn)

        from unittest.mock import MagicMock
        client = MagicMock()
        client.list_replicas.return_value = iter([entry])

        groups = mod._query_rucio(
            "/store/data/Run2018A/Charmonium/MINIAOD/17Sep2018-v1",
            file_split_gb=10.0,
            whitelist=[],
            blacklist=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertTrue(
                url.startswith("root://"),
                f"Expected root:// prefix but got: {url!r}",
            )

    def test_full_xrootd_lfn_not_double_prefixed(self):
        """If Rucio returns a name already starting with root://, it is not double-prefixed."""
        mod = self._import()
        full_url = "root://xrootd-cms.infn.it//store/data/Run2018A/foo.root"
        entry = self._make_rucio_entry(full_url)

        from unittest.mock import MagicMock
        client = MagicMock()
        client.list_replicas.return_value = iter([entry])

        groups = mod._query_rucio(
            "/store/data/Run2018A/MINIAOD",
            file_split_gb=10.0,
            whitelist=[],
            blacklist=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertNotIn("root://root://", url, "Double-prefix detected")
            self.assertTrue(url.startswith("root://"))

    def test_site_override_redirector_applied(self):
        """When a file is available at a whitelisted site, the site-specific
        redirector is used instead of the generic CMS global redirector."""
        mod = self._import()
        lfn = "/store/data/Run2018A/Charmonium/foo.root"
        entry = self._make_rucio_entry(lfn, states={"T2_US_Nebraska_Disk": "AVAILABLE"})

        from unittest.mock import MagicMock
        client = MagicMock()
        client.list_replicas.return_value = iter([entry])

        groups = mod._query_rucio(
            "/store/data/Run2018A/MINIAOD",
            file_split_gb=10.0,
            whitelist=["T2_US_Nebraska"],
            blacklist=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertTrue(
                url.startswith("root://"),
                f"Expected root:// prefix but got: {url!r}",
            )
            # Must use the site-specific redirector path, not the generic CMS one
            self.assertIn(
                "T2_US_Nebraska",
                url,
                f"Expected site-specific redirector for T2_US_Nebraska but got: {url!r}",
            )
            self.assertNotIn(
                "cms-xrd-global.cern.ch",
                url,
                f"Expected site-specific redirector but got generic CMS global: {url!r}",
            )

    def test_existing_redirector_stripped_before_site_specific_applied(self):
        """When Rucio returns a full XRootD URL with an existing redirector and a
        whitelisted site is available, the existing redirector is stripped first
        and the site-specific one is applied instead."""
        mod = self._import()
        # Rucio returns a full URL with an old/generic redirector already embedded
        full_url = "root://cms-xrd-global.cern.ch//store/data/Run2018A/foo.root"
        entry = self._make_rucio_entry(
            full_url, states={"T2_US_Nebraska_Disk": "AVAILABLE"}
        )

        from unittest.mock import MagicMock
        client = MagicMock()
        client.list_replicas.return_value = iter([entry])

        groups = mod._query_rucio(
            "/store/data/Run2018A/MINIAOD",
            file_split_gb=10.0,
            whitelist=["T2_US_Nebraska"],
            blacklist=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertTrue(url.startswith("root://"))
            # The result must not contain a double redirector (old one + new one)
            self.assertNotIn("root://root://", url, "Double-prefix detected")
            # The original generic redirector must have been removed
            self.assertNotIn(
                "cms-xrd-global.cern.ch//store/data",
                url,
                f"Original generic redirector was not stripped: {url!r}",
            )
            # The site-specific redirector must now be present, and the LFN
            # path must appear after the site name without a second redirector
            # host in between.
            self.assertRegex(
                url,
                r"root://xrootd-cms\.infn\.it//store/test/xrootd/T2_US_Nebraska//store/data",
                f"Expected site-specific redirector pattern but got: {url!r}",
            )


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestSubmitSingleJobRemoteIndependence(unittest.TestCase):
    """Verify _submit_single_job generates a submit file that transfers
    the whole shared_inputs/ directory rather than individual files."""
    

class TestMakePartitions(unittest.TestCase):
    """Tests for the _make_partitions utility in partition_utils."""

    def _import(self):
        import partition_utils
        return partition_utils

    # ------------------------------------------------------------------
    # file mode
    # ------------------------------------------------------------------

    def test_file_mode_one_partition_per_file(self):
        """'file' mode creates exactly one partition per unique URL."""
        mod = self._import()
        urls = ["root://x//a.root", "root://x//b.root", "root://x//c.root"]
        parts = mod._make_partitions(urls, mode="file", files_per_job=50, entries_per_job=100)
        self.assertEqual(len(parts), 3)
        for p in parts:
            self.assertEqual(len(p["files"].split(",")), 1)
            self.assertEqual(p["first_entry"], 0)
            self.assertEqual(p["last_entry"], 0)

    def test_file_mode_sorted_order(self):
        """'file' mode returns partitions in sorted URL order."""
        mod = self._import()
        urls = ["root://x//c.root", "root://x//a.root", "root://x//b.root"]
        parts = mod._make_partitions(urls, mode="file", files_per_job=50, entries_per_job=100)
        self.assertEqual([p["files"] for p in parts],
                         ["root://x//a.root", "root://x//b.root", "root://x//c.root"])

    def test_file_mode_deduplicates_urls(self):
        """'file' mode deduplicates the input URL list."""
        mod = self._import()
        urls = ["root://x//a.root", "root://x//a.root", "root://x//b.root"]
        parts = mod._make_partitions(urls, mode="file", files_per_job=50, entries_per_job=100)
        self.assertEqual(len(parts), 2)

    # ------------------------------------------------------------------
    # file_group mode
    # ------------------------------------------------------------------

    def test_file_group_respects_files_per_job(self):
        """'file_group' mode groups up to files_per_job files per partition."""
        mod = self._import()
        urls = [f"root://x//file{i}.root" for i in range(10)]
        parts = mod._make_partitions(urls, mode="file_group", files_per_job=3, entries_per_job=100)
        # 10 files ÷ 3 = 3 full groups + 1 partial
        self.assertEqual(len(parts), 4)
        for p in parts[:-1]:
            self.assertEqual(len(p["files"].split(",")), 3)
        # Last group has remaining files (10 % 3 == 1)
        self.assertEqual(len(parts[-1]["files"].split(",")), 1)

    def test_file_group_no_entry_range_keys(self):
        """'file_group' partitions have first_entry == last_entry == 0."""
        mod = self._import()
        urls = ["root://x//a.root", "root://x//b.root"]
        parts = mod._make_partitions(urls, mode="file_group", files_per_job=5, entries_per_job=100)
        for p in parts:
            self.assertEqual(p["first_entry"], 0)
            self.assertEqual(p["last_entry"], 0)

    def test_file_group_single_file(self):
        """'file_group' with one file produces one partition."""
        mod = self._import()
        parts = mod._make_partitions(
            ["root://x//only.root"], mode="file_group", files_per_job=50, entries_per_job=100
        )
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0]["files"], "root://x//only.root")

    def test_file_group_exactly_files_per_job(self):
        """Exactly files_per_job files fit in one group."""
        mod = self._import()
        urls = [f"root://x//f{i}.root" for i in range(5)]
        parts = mod._make_partitions(urls, mode="file_group", files_per_job=5, entries_per_job=100)
        self.assertEqual(len(parts), 1)
        self.assertEqual(len(parts[0]["files"].split(",")), 5)

    def test_file_group_sorted_within_groups(self):
        """File URLs within each group are in sorted order."""
        mod = self._import()
        urls = ["root://x//z.root", "root://x//a.root", "root://x//m.root"]
        parts = mod._make_partitions(urls, mode="file_group", files_per_job=5, entries_per_job=100)
        self.assertEqual(len(parts), 1)
        files = parts[0]["files"].split(",")
        self.assertEqual(files, sorted(files))

    def test_file_group_deterministic(self):
        """Calling _make_partitions twice yields the same result."""
        mod = self._import()
        urls = ["root://x//c.root", "root://x//a.root", "root://x//b.root"]
        p1 = mod._make_partitions(urls, mode="file_group", files_per_job=2, entries_per_job=100)
        p2 = mod._make_partitions(urls, mode="file_group", files_per_job=2, entries_per_job=100)
        self.assertEqual(p1, p2)

    # ------------------------------------------------------------------
    # entry_range mode
    # ------------------------------------------------------------------

    def test_entry_range_requires_uproot(self):
        """'entry_range' mode raises RuntimeError when uproot is absent."""
        import sys
        import unittest.mock as mock
        mod = self._import()

        # Remove uproot from sys.modules (if present) so the lazy import inside
        # _query_tree_entries() sees it as missing.
        saved = sys.modules.pop("uproot", None)
        try:
            with mock.patch.dict(sys.modules, {"uproot": None}):
                with self.assertRaises(RuntimeError) as ctx:
                    mod._query_tree_entries("fake.root", "Events")
            self.assertIn("uproot", str(ctx.exception).lower())
        finally:
            if saved is not None:
                sys.modules["uproot"] = saved

    def test_entry_range_splits_correctly(self):
        """'entry_range' mode produces correct ranges for a known entry count."""
        mod = self._import()
        import unittest.mock as mock

        # 250 entries split into chunks of 100 → 3 partitions
        with mock.patch.object(mod, "_query_tree_entries", return_value=250):
            parts = mod._make_partitions(
                ["root://x//a.root"],
                mode="entry_range",
                files_per_job=50,
                entries_per_job=100,
            )

        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0]["first_entry"], 0)
        self.assertEqual(parts[0]["last_entry"], 100)
        self.assertEqual(parts[1]["first_entry"], 100)
        self.assertEqual(parts[1]["last_entry"], 200)
        self.assertEqual(parts[2]["first_entry"], 200)
        self.assertEqual(parts[2]["last_entry"], 250)

    def test_entry_range_exact_multiple(self):
        """'entry_range' mode handles entry count that is an exact multiple."""
        mod = self._import()
        import unittest.mock as mock

        with mock.patch.object(mod, "_query_tree_entries", return_value=200):
            parts = mod._make_partitions(
                ["root://x//a.root"],
                mode="entry_range",
                files_per_job=50,
                entries_per_job=100,
            )
        self.assertEqual(len(parts), 2)
        self.assertEqual(parts[0]["last_entry"], 100)
        self.assertEqual(parts[1]["last_entry"], 200)

    def test_entry_range_single_chunk(self):
        """A file smaller than entries_per_job produces one partition."""
        mod = self._import()
        import unittest.mock as mock

        with mock.patch.object(mod, "_query_tree_entries", return_value=50):
            parts = mod._make_partitions(
                ["root://x//small.root"],
                mode="entry_range",
                files_per_job=50,
                entries_per_job=100,
            )
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0]["first_entry"], 0)
        self.assertEqual(parts[0]["last_entry"], 50)

    def test_entry_range_per_file_partitions(self):
        """Each file in 'entry_range' mode gets its own independent partitions."""
        mod = self._import()
        import unittest.mock as mock

        # Two files with different entry counts
        def _fake_entries(url, tree_name="Events"):
            return 150 if "a" in url else 80

        with mock.patch.object(mod, "_query_tree_entries", side_effect=_fake_entries):
            parts = mod._make_partitions(
                ["root://x//a.root", "root://x//b.root"],
                mode="entry_range",
                files_per_job=50,
                entries_per_job=100,
            )

        # a.root: 150 entries → 2 partitions (0-100, 100-150)
        # b.root:  80 entries → 1 partition  (0-80)
        self.assertEqual(len(parts), 3)
        a_parts = [p for p in parts if "a.root" in p["files"]]
        b_parts = [p for p in parts if "b.root" in p["files"]]
        self.assertEqual(len(a_parts), 2)
        self.assertEqual(len(b_parts), 1)

    # ------------------------------------------------------------------
    # invalid mode
    # ------------------------------------------------------------------

    def test_invalid_mode_raises_value_error(self):
        """An unrecognised mode raises ValueError."""
        mod = self._import()
        with self.assertRaises(ValueError):
            mod._make_partitions([], mode="nonsense", files_per_job=10, entries_per_job=100)
