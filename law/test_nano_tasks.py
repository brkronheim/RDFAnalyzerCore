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


if __name__ == "__main__":
    unittest.main(verbosity=2)
