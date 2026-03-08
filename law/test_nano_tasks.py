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
class TestSubmitSingleJobRemoteIndependence(unittest.TestCase):
    """Verify _submit_single_job generates a submit file that transfers
    the whole shared_inputs/ directory rather than individual files."""

    def _import(self):
        import nano_tasks
        return nano_tasks

    def test_shared_inputs_dir_in_transfer(self):
        """transfer_input_files should reference shared_inputs/ as a directory,
        not individual files inside it."""
        import tempfile
        mod = self._import()

        with tempfile.TemporaryDirectory() as tmp:
            main_dir    = tmp
            shared_dir  = os.path.join(main_dir, "shared_inputs")
            os.makedirs(shared_dir)
            job_dir     = os.path.join(main_dir, "job_0")
            os.makedirs(job_dir)
            for fname in ("submit_config.txt", "floats.txt", "ints.txt"):
                open(os.path.join(job_dir, fname), "w").close()
            open(os.path.join(shared_dir, "myexe"), "w").close()

            # Capture sub_path written by _submit_single_job
            import io
            from unittest.mock import patch, MagicMock

            written_content = {}
            real_open = open
            def fake_open(path, mode="r", *a, **kw):
                if "resub_job0.sub" in path and "w" in mode:
                    buf = io.StringIO()
                    # intercept write; use __enter__/__exit__ wrapper
                    class _Wrapper:
                        def write(self, s): buf.write(s)
                        def __enter__(self): return self
                        def __exit__(self, *_): written_content["sub"] = buf.getvalue()
                    return _Wrapper()
                return real_open(path, mode, *a, **kw)

            with patch("builtins.open", side_effect=fake_open):
                with patch("nano_tasks.subprocess.run") as mock_run:
                    mock_run.return_value = MagicMock(returncode=0, stdout="1 job(s) submitted to cluster 99.")
                    mod._submit_single_job(
                        job_index=0,
                        main_dir=main_dir,
                        exe_relpath="myexe",
                        x509loc=None,
                        stage_inputs=False,
                        max_runtime=3600,
                        request_memory=2000,
                        shared_dir_name="shared_inputs",
                        config_dict={},
                    )

            sub_text = written_content.get("sub", "")
            self.assertIn(shared_dir, sub_text,
                          "shared_inputs directory should appear in transfer_input_files")
            # Individual files inside shared_inputs must NOT be listed separately
            self.assertNotIn(os.path.join(shared_dir, "myexe"), sub_text)

    def test_condor_proc_env_in_resubmission(self):
        """Resubmission submit file must include CONDOR_PROC environment variable."""
        import tempfile
        mod = self._import()

        with tempfile.TemporaryDirectory() as tmp:
            main_dir   = tmp
            shared_dir = os.path.join(main_dir, "shared_inputs")
            os.makedirs(shared_dir)
            job_dir    = os.path.join(main_dir, "job_0")
            os.makedirs(job_dir)
            for fname in ("submit_config.txt", "floats.txt", "ints.txt"):
                open(os.path.join(job_dir, fname), "w").close()

            sub_path = os.path.join(main_dir, "resubmissions", "resub_job0.sub")
            from unittest.mock import patch, MagicMock

            with patch("nano_tasks.subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(returncode=0, stdout="submitted to cluster 99.")
                mod._submit_single_job(
                    job_index=0,
                    main_dir=main_dir,
                    exe_relpath="myexe",
                    x509loc=None,
                    stage_inputs=False,
                    max_runtime=3600,
                    request_memory=2000,
                    shared_dir_name="shared_inputs",
                    config_dict={},
                )

            with open(sub_path) as fh:
                content = fh.read()

            self.assertIn("CONDOR_PROC=$(Process)", content)
            self.assertIn("CONDOR_CLUSTER=$(Cluster)", content)


if __name__ == "__main__":
    unittest.main(verbosity=2)
