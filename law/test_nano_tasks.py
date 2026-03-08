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
class TestNanoEnsureXrootdRedirector(unittest.TestCase):
    """Unit tests for the _ensure_xrootd_redirector helper in nano_tasks."""

    def _import(self):
        import nano_tasks
        return nano_tasks

    def test_already_has_redirector_unchanged(self):
        """URIs that already start with root:// are returned unchanged."""
        mod = self._import()
        uri = "root://xrootd-cms.infn.it//store/data/Run2018A/Charmonium/foo.root"
        self.assertEqual(mod._ensure_xrootd_redirector(uri), uri)

    def test_bare_das_path_gets_nano_redirector(self):
        """A bare /store/... DAS path gets the NANO_REDIRECTOR prepended."""
        mod = self._import()
        uri = "/store/data/Run2018A/Charmonium/foo.root"
        result = mod._ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://"))
        self.assertIn("/store/data/Run2018A/Charmonium/foo.root", result)

    def test_custom_redirector(self):
        """A custom redirector is used when explicitly provided."""
        mod = self._import()
        uri = "/store/data/Run2016G/foo.root"
        custom = "root://cms-xrd-global.cern.ch/"
        result = mod._ensure_xrootd_redirector(uri, redirector=custom)
        self.assertTrue(result.startswith(custom))
        self.assertIn("store/data/Run2016G/foo.root", result)

    def test_empty_string_returned_unchanged(self):
        """An empty URI is returned unchanged."""
        mod = self._import()
        self.assertEqual(mod._ensure_xrootd_redirector(""), "")

    def test_non_path_uri_returned_unchanged(self):
        """A relative URI with no scheme is returned unchanged."""
        mod = self._import()
        uri = "some_relative_file.root"
        self.assertEqual(mod._ensure_xrootd_redirector(uri), uri)

    def test_nano_redirector_constant_exported(self):
        """NANO_REDIRECTOR constant exists and starts with root://."""
        mod = self._import()
        self.assertTrue(hasattr(mod, "NANO_REDIRECTOR"))
        self.assertTrue(mod.NANO_REDIRECTOR.startswith("root://"))


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
            WL=[],
            BL=[],
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
            WL=[],
            BL=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertNotIn("root://root://", url, "Double-prefix detected")
            self.assertTrue(url.startswith("root://"))

    def test_site_override_redirector_applied(self):
        """With a site override the site-specific redirector is still an XRootD URL."""
        mod = self._import()
        lfn = "/store/data/Run2018A/Charmonium/foo.root"
        entry = self._make_rucio_entry(lfn, states={"T2_US_Nebraska_Disk": "AVAILABLE"})

        from unittest.mock import MagicMock
        client = MagicMock()
        client.list_replicas.return_value = iter([entry])

        groups = mod._query_rucio(
            "/store/data/Run2018A/MINIAOD",
            file_split_gb=10.0,
            WL=["T2_US_Nebraska"],
            BL=[],
            site_override="",
            client=client,
        )
        all_urls = ",".join(groups.values()).split(",")
        for url in all_urls:
            self.assertTrue(
                url.startswith("root://"),
                f"Expected root:// prefix but got: {url!r}",
            )


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestNanoTasksModule(unittest.TestCase):
    """Smoke tests for the nano_tasks module itself."""

    def test_importable(self):
        """nano_tasks can be imported without errors."""
        import nano_tasks  # noqa: F401

    def test_tasks_defined(self):
        """Core task classes are present in the module."""
        import nano_tasks
        for name in (
            "PrepareNANOSample",
            "BuildNANOSubmission",
            "SubmitNANOJobs",
            "MonitorNANOJobs",
        ):
            self.assertTrue(hasattr(nano_tasks, name), f"{name} not found")

    def test_task_namespaces(self):
        """Tasks use an empty task_namespace for easy law run invocation."""
        import nano_tasks
        for cls in (
            nano_tasks.PrepareNANOSample,
            nano_tasks.BuildNANOSubmission,
            nano_tasks.SubmitNANOJobs,
            nano_tasks.MonitorNANOJobs,
        ):
            self.assertEqual(cls.task_namespace, "")

    def test_nano_mixin_has_dataset_parameter(self):
        """NANOMixin exposes the 'dataset' luigi parameter."""
        import nano_tasks
        self.assertTrue(
            hasattr(nano_tasks.NANOMixin, "dataset"),
            "NANOMixin is missing the 'dataset' parameter",
        )


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestNanoDatasetFilter(unittest.TestCase):
    """Tests for the per-dataset execution feature in nano_tasks."""

    def _import(self):
        import nano_tasks
        return nano_tasks

    def _make_sample_list(self, names):
        """Return a minimal fake sample_list dict keyed by dataset name."""
        return {n: {"name": n, "das": "/foo/bar", "xsec": "1.0", "type": "0",
                    "norm": "1.0", "kfac": "1.0", "extraScale": "1.0"}
                for n in names}

    # ------------------------------------------------------------------
    # _main_dir path logic
    # ------------------------------------------------------------------

    def test_main_dir_without_dataset_uses_name_only(self):
        """When --dataset is empty, _main_dir is condorSub_{name}."""
        mod = self._import()
        mixin = mod.NANOMixin()
        mixin.name = "myRun"
        mixin.dataset = ""
        self.assertTrue(mixin._main_dir.endswith("condorSub_myRun"))

    def test_main_dir_with_dataset_includes_dataset(self):
        """When --dataset is set, _main_dir is condorSub_{name}_{dataset}."""
        mod = self._import()
        mixin = mod.NANOMixin()
        mixin.name = "myRun"
        mixin.dataset = "ttbar"
        expected_suffix = "condorSub_myRun_ttbar"
        self.assertTrue(
            mixin._main_dir.endswith(expected_suffix),
            f"Expected _main_dir to end with {expected_suffix!r}, got {mixin._main_dir!r}",
        )

    def test_main_dir_different_datasets_are_different_dirs(self):
        """Two different --dataset values produce two different _main_dir paths."""
        mod = self._import()
        mixin1 = mod.NANOMixin()
        mixin1.name = "run22"
        mixin1.dataset = "signal"
        mixin2 = mod.NANOMixin()
        mixin2.name = "run22"
        mixin2.dataset = "background"
        self.assertNotEqual(mixin1._main_dir, mixin2._main_dir)

    # ------------------------------------------------------------------
    # create_branch_map filtering
    # ------------------------------------------------------------------

    def test_branch_map_without_dataset_returns_all(self):
        """When --dataset is empty, create_branch_map returns all samples."""
        mod = self._import()
        sample_list = self._make_sample_list(["alpha", "beta", "gamma"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareNANOSample.__new__(mod.PrepareNANOSample)
        task.dataset = ""
        with patch.object(mod.NANOMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("nano_tasks._get_sample_list",
                       return_value=(sample_list, [], 1.0, [], [])):
                branch_map = task.create_branch_map()
        self.assertEqual(set(branch_map.values()), {"alpha", "beta", "gamma"})

    def test_branch_map_with_dataset_returns_single_entry(self):
        """When --dataset is set, create_branch_map returns only that sample."""
        mod = self._import()
        sample_list = self._make_sample_list(["alpha", "beta", "gamma"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareNANOSample.__new__(mod.PrepareNANOSample)
        task.dataset = "beta"
        with patch.object(mod.NANOMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("nano_tasks._get_sample_list",
                       return_value=(sample_list, [], 1.0, [], [])):
                branch_map = task.create_branch_map()
        self.assertEqual(branch_map, {0: "beta"})

    def test_branch_map_unknown_dataset_raises(self):
        """When --dataset names a dataset not in the config, ValueError is raised."""
        mod = self._import()
        sample_list = self._make_sample_list(["alpha", "beta"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareNANOSample.__new__(mod.PrepareNANOSample)
        task.dataset = "nonexistent"
        with patch.object(mod.NANOMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("nano_tasks._get_sample_list",
                       return_value=(sample_list, [], 1.0, [], [])):
                with self.assertRaises(ValueError):
                    task.create_branch_map()


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
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
        import builtins
        import importlib
        mod = self._import()

        real_import = builtins.__import__

        def _mock_import(name, *args, **kwargs):
            if name == "uproot":
                raise ImportError("uproot not installed")
            return real_import(name, *args, **kwargs)

        import unittest.mock as mock
        with mock.patch("builtins.__import__", side_effect=_mock_import):
            with self.assertRaises(RuntimeError) as ctx:
                mod._query_tree_entries("fake.root", "Events")
        self.assertIn("uproot", str(ctx.exception).lower())

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


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestNANOMixinPartitionParams(unittest.TestCase):
    """Tests that NANOMixin exposes the new partitioning parameters."""

    def _import(self):
        import nano_tasks
        return nano_tasks

    def test_partition_param_exists(self):
        """NANOMixin has a 'partition' luigi Parameter."""
        mod = self._import()
        self.assertTrue(hasattr(mod.NANOMixin, "partition"))

    def test_files_per_job_param_exists(self):
        """NANOMixin has a 'files_per_job' luigi Parameter."""
        mod = self._import()
        self.assertTrue(hasattr(mod.NANOMixin, "files_per_job"))

    def test_entries_per_job_param_exists(self):
        """NANOMixin has an 'entries_per_job' luigi Parameter."""
        mod = self._import()
        self.assertTrue(hasattr(mod.NANOMixin, "entries_per_job"))

    def test_partition_default_is_file_group(self):
        """The default partition mode is 'file_group'."""
        mod = self._import()
        import luigi
        default = mod.NANOMixin.partition.task_value(
            mod.PrepareNANOSample.get_task_family(), "partition"
        )
        self.assertEqual(default, "file_group")

    def test_files_per_job_default_is_50(self):
        """The default files_per_job is 50."""
        mod = self._import()
        default = mod.NANOMixin.files_per_job.task_value(
            mod.PrepareNANOSample.get_task_family(), "files_per_job"
        )
        self.assertEqual(default, 50)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestOpenDataMixinPartitionParams(unittest.TestCase):
    """Tests that OpenDataMixin exposes the new partitioning parameters."""

    def _import(self):
        import opendata_tasks
        return opendata_tasks

    def test_partition_param_exists(self):
        """OpenDataMixin has a 'partition' luigi Parameter."""
        mod = self._import()
        self.assertTrue(hasattr(mod.OpenDataMixin, "partition"))

    def test_entries_per_job_param_exists(self):
        """OpenDataMixin has an 'entries_per_job' luigi Parameter."""
        mod = self._import()
        self.assertTrue(hasattr(mod.OpenDataMixin, "entries_per_job"))

    def test_partition_default_is_file_group(self):
        """The default partition mode for OpenData is 'file_group'."""
        mod = self._import()
        default = mod.OpenDataMixin.partition.task_value(
            mod.PrepareOpenDataSample.get_task_family(), "partition"
        )
        self.assertEqual(default, "file_group")


if __name__ == "__main__":
    unittest.main(verbosity=2)
