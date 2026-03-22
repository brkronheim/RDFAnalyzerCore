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
class TestEnsureXrootdRedirector(unittest.TestCase):
    """Unit tests for the _ensure_xrootd_redirector helper in opendata_tasks."""

    def _import(self):
        import opendata_tasks
        return opendata_tasks

    def test_already_has_redirector_unchanged(self):
        """URIs that already start with root:// are returned unchanged."""
        mod = self._import()
        uri = "root://eospublic.cern.ch//eos/opendata/cms/Run2016G/foo.root"
        self.assertEqual(mod._ensure_xrootd_redirector(uri), uri)

    def test_bare_eos_path_gets_redirector(self):
        """A bare /eos/... path gets the default redirector prepended."""
        mod = self._import()
        uri = "/eos/opendata/cms/Run2016G/foo.root"
        result = mod._ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://"))
        self.assertIn("//eos/opendata/cms/Run2016G/foo.root", result)

    def test_bare_path_no_double_slash(self):
        """Prepending the redirector does not create a triple-slash artefact."""
        mod = self._import()
        uri = "/eos/opendata/cms/foo.root"
        result = mod._ensure_xrootd_redirector(uri)
        self.assertIn("//eos/opendata/cms/foo.root", result)
        self.assertNotIn("///", result)

    def test_bare_path_missing_leading_slash(self):
        """A path missing the leading slash still gets the redirector prepended."""
        mod = self._import()
        uri = "store/data/Run2016G/foo.root"
        result = mod._ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://"))
        self.assertIn("//store/data/Run2016G/foo.root", result)

    def test_custom_redirector(self):
        """A custom redirector is used when explicitly provided."""
        mod = self._import()
        uri = "/store/data/Run2016G/foo.root"
        result = mod._ensure_xrootd_redirector(uri, redirector="root://xrootd-cms.infn.it/")
        self.assertTrue(result.startswith("root://xrootd-cms.infn.it//"))
        self.assertIn("//store/data/Run2016G/foo.root", result)

    def test_normalizes_single_slash_root_url(self):
        """root://host/store/... is normalized to root://host//store/..."""
        mod = self._import()
        uri = "root://xrootd-cms.infn.it/store/data/Run2016G/foo.root"
        result = mod._ensure_xrootd_redirector(uri)
        self.assertTrue(result.startswith("root://xrootd-cms.infn.it//"))

    def test_empty_string_returned_unchanged(self):
        """An empty URI is returned unchanged."""
        mod = self._import()
        self.assertEqual(mod._ensure_xrootd_redirector(""), "")

    def test_non_path_uri_returned_unchanged(self):
        """A relative URI with no scheme is returned unchanged."""
        mod = self._import()
        uri = "some_relative_file.root"
        self.assertEqual(mod._ensure_xrootd_redirector(uri), uri)

    def test_redirector_constant_exported(self):
        """OPENDATA_REDIRECTOR constant exists and starts with root://."""
        mod = self._import()
        self.assertTrue(hasattr(mod, "OPENDATA_REDIRECTOR"))
        self.assertTrue(mod.OPENDATA_REDIRECTOR.startswith("root://"))


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


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestOpendataTasksModule(unittest.TestCase):
    """Smoke tests for the opendata_tasks module itself."""

    def test_importable(self):
        """opendata_tasks can be imported without errors."""
        import opendata_tasks  # noqa: F401

    def test_tasks_defined(self):
        """Core task classes are present in the module."""
        import opendata_tasks
        for name in (
            "PrepareOpenDataSample",
            "BuildOpenDataSubmission",
            "SubmitOpenDataJobs",
            "MonitorOpenDataJobs",
        ):
            self.assertTrue(hasattr(opendata_tasks, name), f"{name} not found")

    def test_task_namespaces(self):
        """Tasks use an empty task_namespace for easy law run invocation."""
        import opendata_tasks
        for cls in (
            opendata_tasks.PrepareOpenDataSample,
            opendata_tasks.BuildOpenDataSubmission,
            opendata_tasks.SubmitOpenDataJobs,
            opendata_tasks.MonitorOpenDataJobs,
        ):
            self.assertEqual(cls.task_namespace, "")

    def test_opendata_mixin_has_dataset_parameter(self):
        """OpenDataMixin exposes the 'dataset' luigi parameter."""
        import opendata_tasks
        self.assertTrue(
            hasattr(opendata_tasks.OpenDataMixin, "dataset"),
            "OpenDataMixin is missing the 'dataset' parameter",
        )


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestOpendataDatasetFilter(unittest.TestCase):
    """Tests for the per-dataset execution feature in opendata_tasks."""

    def _import(self):
        import opendata_tasks
        return opendata_tasks

    def _make_samples(self, names):
        """Return a minimal fake samples dict keyed by dataset name."""
        return {n: {"name": n, "das": "cms_dataset", "xsec": "1.0", "type": "0",
                    "norm": "1.0", "kfac": "1.0", "extraScale": "1.0"}
                for n in names}

    # ------------------------------------------------------------------
    # _main_dir path logic
    # ------------------------------------------------------------------

    def test_main_dir_without_dataset_uses_name_only(self):
        """When --dataset is empty, _main_dir is condorSub_{name}."""
        mod = self._import()
        mixin = mod.OpenDataMixin()
        mixin.name = "myRun"
        mixin.dataset = ""
        self.assertTrue(mixin._main_dir.endswith("condorSub_myRun"))

    def test_main_dir_with_dataset_includes_dataset(self):
        """When --dataset is set, _main_dir is condorSub_{name}_{dataset}."""
        mod = self._import()
        mixin = mod.OpenDataMixin()
        mixin.name = "myRun"
        mixin.dataset = "dy"
        expected_suffix = "condorSub_myRun_dy"
        self.assertTrue(
            mixin._main_dir.endswith(expected_suffix),
            f"Expected _main_dir to end with {expected_suffix!r}, got {mixin._main_dir!r}",
        )

    def test_main_dir_different_datasets_are_different_dirs(self):
        """Two different --dataset values produce two different _main_dir paths."""
        mod = self._import()
        mixin1 = mod.OpenDataMixin()
        mixin1.name = "run16"
        mixin1.dataset = "signal"
        mixin2 = mod.OpenDataMixin()
        mixin2.name = "run16"
        mixin2.dataset = "background"
        self.assertNotEqual(mixin1._main_dir, mixin2._main_dir)

    # ------------------------------------------------------------------
    # create_branch_map filtering
    # ------------------------------------------------------------------

    def test_branch_map_without_dataset_returns_all(self):
        """When --dataset is empty, create_branch_map returns all samples."""
        mod = self._import()
        samples = self._make_samples(["alpha", "beta", "gamma"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareOpenDataSample.__new__(mod.PrepareOpenDataSample)
        task.dataset = ""
        with patch.object(mod.OpenDataMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("opendata_tasks._parse_opendata_config",
                       return_value=(samples, [], 1.0)):
                branch_map = task.create_branch_map()
        self.assertEqual(set(branch_map.values()), {"alpha", "beta", "gamma"})

    def test_branch_map_with_dataset_returns_single_entry(self):
        """When --dataset is set, create_branch_map returns only that sample."""
        mod = self._import()
        samples = self._make_samples(["alpha", "beta", "gamma"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareOpenDataSample.__new__(mod.PrepareOpenDataSample)
        task.dataset = "gamma"
        with patch.object(mod.OpenDataMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("opendata_tasks._parse_opendata_config",
                       return_value=(samples, [], 1.0)):
                branch_map = task.create_branch_map()
        self.assertEqual(branch_map, {0: "gamma"})

    def test_branch_map_unknown_dataset_raises(self):
        """When --dataset names a dataset not in the config, ValueError is raised."""
        mod = self._import()
        samples = self._make_samples(["alpha", "beta"])
        from unittest.mock import patch, PropertyMock
        task = mod.PrepareOpenDataSample.__new__(mod.PrepareOpenDataSample)
        task.dataset = "nonexistent"
        with patch.object(mod.OpenDataMixin, "_sample_config", new_callable=PropertyMock,
                          return_value="fake.txt"):
            with patch("opendata_tasks._parse_opendata_config",
                       return_value=(samples, [], 1.0)):
                with self.assertRaises(ValueError):
                    task.create_branch_map()


if __name__ == "__main__":
    unittest.main(verbosity=2)
