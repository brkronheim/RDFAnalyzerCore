#!/usr/bin/env python3
"""
Tests for the opendata_tasks law module.

These tests verify the XRootD redirector utility and the metadata-processing
helpers without requiring a running law scheduler, an HTCondor cluster, or
network access to the CERN Open Data portal.
"""

from __future__ import annotations

import os
import sys
import unittest
from unittest.mock import patch, MagicMock

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


class TestEnsureXrootdRedirectorBackend(unittest.TestCase):
    """Unit tests for the canonical ensure_xrootd_redirector in submission_backend."""

    def _import(self):
        import submission_backend
        return submission_backend

    def test_already_has_redirector_unchanged(self):
        """URIs that already start with root:// are returned unchanged."""
        mod = self._import()
        uri = "root://eospublic.cern.ch//eos/opendata/cms/Run2016G/foo.root"
        self.assertEqual(mod.ensure_xrootd_redirector(uri), uri)

    def test_bare_eos_path_gets_redirector(self):
        """A bare /eos/... path gets the default redirector prepended."""
        mod = self._import()
        uri = "/eos/opendata/cms/Run2016G/foo.root"
        result = mod.ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://"))
        self.assertIn("//eos/opendata/cms/Run2016G/foo.root", result)

    def test_bare_path_no_double_slash(self):
        """Prepending the redirector does not create a triple-slash artefact."""
        mod = self._import()
        uri = "/eos/opendata/cms/foo.root"
        result = mod.ensure_xrootd_redirector(uri)
        self.assertIn("//eos/opendata/cms/foo.root", result)
        self.assertNotIn("///", result)

    def test_bare_path_missing_leading_slash(self):
        """A path missing the leading slash still gets the redirector prepended."""
        mod = self._import()
        uri = "store/data/Run2016G/foo.root"
        result = mod.ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://"))
        self.assertIn("//store/data/Run2016G/foo.root", result)

    def test_custom_redirector(self):
        """A custom redirector is used when explicitly provided."""
        mod = self._import()
        uri = "/store/data/Run2016G/foo.root"
        result = mod.ensure_xrootd_redirector(uri, redirector="root://xrootd-cms.infn.it/")
        self.assertTrue(result.startswith("root://xrootd-cms.infn.it//"))
        self.assertIn("//store/data/Run2016G/foo.root", result)

    def test_normalizes_single_slash_root_url(self):
        """root://host/store/... is normalized to root://host//store/..."""
        mod = self._import()
        uri = "root://xrootd-cms.infn.it/store/data/Run2016G/foo.root"
        result = mod.ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://xrootd-cms.infn.it//"))

    def test_empty_string_returned_unchanged(self):
        """An empty URI is returned unchanged."""
        mod = self._import()
        self.assertEqual(mod.ensure_xrootd_redirector(""), "")

    def test_non_path_uri_returned_unchanged(self):
        """A relative URI with no scheme is returned unchanged."""
        mod = self._import()
        uri = "some_relative_file.root"
        self.assertEqual(mod.ensure_xrootd_redirector(uri), uri)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestProcessMetadata(unittest.TestCase):
    """Unit tests for _process_metadata – verifies redirector is applied."""

    def _import(self):
        import opendata_tasks
        return opendata_tasks

    def _make_file_indices(self, uris, key="cms_dataset"):
        """Build a mock _file_indices structure like the Open Data API returns."""
        return [
            {
                "files": [
                    {
                        "uri": uri,
                        "key": f"{key}_file_index",
                    }
                    for uri in uris
                ]
            }
        ]

    def test_uris_with_redirector_unchanged(self):
        """URIs that already have root:// are stored unchanged."""
        mod = self._import()
        uris = ["root://eospublic.cern.ch//eos/opendata/cms/foo.root"]
        sample_names = {"cms_dataset": "my_sample"}
        file_indices = self._make_file_indices(uris)
        with patch.object(mod, "_load_file_indices", return_value=file_indices):
            result = mod._process_metadata("12345", sample_names)
        self.assertEqual(result["my_sample"], uris)

    def test_bare_eos_uris_get_redirector(self):
        """Bare /eos/... URIs returned by the API get the redirector attached."""
        mod = self._import()
        bare_uri = "/eos/opendata/cms/Run2016G/foo.root"
        sample_names = {"cms_dataset": "my_sample"}
        file_indices = self._make_file_indices([bare_uri])
        with patch.object(mod, "_load_file_indices", return_value=file_indices):
            result = mod._process_metadata("12345", sample_names)
        self.assertEqual(len(result["my_sample"]), 1)
        self.assertTrue(result["my_sample"][0].startswith("root://"))

    def test_unknown_key_skipped(self):
        """Files whose key is not in sample_names are skipped."""
        mod = self._import()
        uris = ["root://eospublic.cern.ch//eos/opendata/cms/foo.root"]
        sample_names = {}  # nothing mapped
        file_indices = self._make_file_indices(uris)
        with patch.object(mod, "_load_file_indices", return_value=file_indices):
            result = mod._process_metadata("12345", sample_names)
        self.assertEqual(result, {})
