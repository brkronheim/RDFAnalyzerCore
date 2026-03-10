#!/usr/bin/env python3
"""
Tests for the merge_tasks law module.

These tests verify:
  - Module structure and imports
  - Helper functions (_find_hadd, _run_hadd, _group_files_by_basename)
  - MergeMixin parameter defaults and derived properties
  - MergeSkims / MergeHistograms / MergeCutflows / MergeMetadata output paths
  - MergeAll requires logic based on schema presence
  - Manifest discovery, loading, and validation paths
  - Provenance preservation in merged manifests
  - Systematic variation / multi-region grouping

These tests do NOT require a running law scheduler, HTCondor cluster, or
ROOT installation.  hadd calls are mocked where ROOT files are required.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, call

# ---------------------------------------------------------------------------
# Path setup
# ---------------------------------------------------------------------------
_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
_LAW_DIR = os.path.join(_REPO_ROOT, "law")
_CORE_PY = os.path.join(_REPO_ROOT, "core", "python")
for _p in (_LAW_DIR, _CORE_PY):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Optional dependency check
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law  # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

_SKIP_MSG = "law and luigi packages not available"


# ===========================================================================
# Helper: write a minimal OutputManifest YAML to a temp directory
# ===========================================================================

def _write_manifest(directory: str, **kwargs) -> str:
    """Write an output_manifest.yaml in *directory* and return its path.

    Keyword arguments are merged into a minimal valid manifest dict.
    """
    import yaml
    manifest = {
        "manifest_version": 1,
        "skim": None,
        "histograms": None,
        "metadata": None,
        "cutflow": None,
        "law_artifacts": [],
        "framework_hash": None,
        "user_repo_hash": None,
        "config_mtime": None,
        "dataset_manifest_provenance": None,
    }
    manifest.update(kwargs)
    path = os.path.join(directory, "output_manifest.yaml")
    os.makedirs(directory, exist_ok=True)
    with open(path, "w") as fh:
        yaml.dump(manifest, fh, default_flow_style=False)
    return path


def _make_skim_manifest(directory: str, output_file: str = "skim.root") -> str:
    return _write_manifest(
        directory,
        skim={
            "schema_version": 1,
            "output_file": output_file,
            "tree_name": "Events",
            "branches": [],
        },
    )


def _make_histogram_manifest(directory: str, output_file: str = "histograms.root") -> str:
    return _write_manifest(
        directory,
        histograms={
            "schema_version": 1,
            "output_file": output_file,
            "histogram_names": ["h_pt"],
            "axes": [],
        },
    )


def _make_cutflow_manifest(directory: str, output_file: str = "cutflow.root") -> str:
    return _write_manifest(
        directory,
        cutflow={
            "schema_version": 1,
            "output_file": output_file,
            "counter_keys": [],
        },
    )


def _make_metadata_manifest(directory: str, output_file: str = "meta.root") -> str:
    return _write_manifest(
        directory,
        metadata={
            "schema_version": 1,
            "output_file": output_file,
            "provenance_dir": "provenance",
            "required_keys": [],
            "optional_keys": [],
        },
    )


def _make_full_manifest(directory: str) -> str:
    """Write a manifest with skim, histograms, cutflow, and metadata schemas."""
    return _write_manifest(
        directory,
        skim={
            "schema_version": 1,
            "output_file": "skim.root",
            "tree_name": "Events",
            "branches": [],
        },
        histograms={
            "schema_version": 1,
            "output_file": "histograms.root",
            "histogram_names": ["h_pt"],
            "axes": [],
        },
        cutflow={
            "schema_version": 1,
            "output_file": "cutflow.root",
            "counter_keys": [],
        },
        metadata={
            "schema_version": 1,
            "output_file": "meta.root",
            "provenance_dir": "provenance",
            "required_keys": [],
            "optional_keys": [],
        },
        framework_hash="abc123",
        user_repo_hash="def456",
    )


# ===========================================================================
# Tests for helper functions (no law/luigi required)
# ===========================================================================


class TestGroupFilesByBasename(unittest.TestCase):
    """Tests for _group_files_by_basename."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_single_file(self):
        mod = self._import()
        result = mod._group_files_by_basename(["/a/b/skim.root"])
        self.assertEqual(result, {"skim.root": ["/a/b/skim.root"]})

    def test_same_basename_grouped(self):
        mod = self._import()
        paths = [
            "/job_0/skim.root",
            "/job_1/skim.root",
            "/job_2/skim.root",
        ]
        result = mod._group_files_by_basename(paths)
        self.assertIn("skim.root", result)
        self.assertEqual(len(result["skim.root"]), 3)

    def test_systematic_variations_separate_groups(self):
        """Files with different basenames land in separate groups."""
        mod = self._import()
        paths = [
            "/job_0/skim_jesUp.root",
            "/job_1/skim_jesUp.root",
            "/job_0/skim_jesDown.root",
            "/job_1/skim_jesDown.root",
        ]
        result = mod._group_files_by_basename(paths)
        self.assertIn("skim_jesUp.root", result)
        self.assertIn("skim_jesDown.root", result)
        self.assertEqual(len(result["skim_jesUp.root"]), 2)
        self.assertEqual(len(result["skim_jesDown.root"]), 2)

    def test_multi_region_separate_groups(self):
        """Files for different regions land in separate groups."""
        mod = self._import()
        paths = [
            "/job_0/histograms_SR.root",
            "/job_1/histograms_SR.root",
            "/job_0/histograms_CR.root",
            "/job_1/histograms_CR.root",
        ]
        result = mod._group_files_by_basename(paths)
        self.assertIn("histograms_SR.root", result)
        self.assertIn("histograms_CR.root", result)

    def test_sorted_within_groups(self):
        """Files within each group are sorted for determinism."""
        mod = self._import()
        paths = ["/b/skim.root", "/a/skim.root", "/c/skim.root"]
        result = mod._group_files_by_basename(paths)
        self.assertEqual(result["skim.root"], sorted(paths))

    def test_empty_list(self):
        mod = self._import()
        result = mod._group_files_by_basename([])
        self.assertEqual(result, {})


class TestFindHadd(unittest.TestCase):
    """Tests for _find_hadd."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_finds_hadd_on_path(self):
        """_find_hadd finds hadd via system PATH when a mock is available."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_hadd = os.path.join(tmpdir, "hadd")
            Path(fake_hadd).write_text("#!/bin/sh\n")
            os.chmod(fake_hadd, 0o755)
            old_path = os.environ.get("PATH", "")
            os.environ["PATH"] = tmpdir + ":" + old_path
            try:
                result = mod._find_hadd()
                self.assertEqual(result, fake_hadd)
            finally:
                os.environ["PATH"] = old_path

    def test_raises_when_not_found(self):
        """_find_hadd raises RuntimeError when hadd is not on PATH."""
        mod = self._import()
        with patch.dict(os.environ, {"PATH": ""}):
            with self.assertRaises(RuntimeError):
                mod._find_hadd()


class TestRunHadd(unittest.TestCase):
    """Tests for _run_hadd."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_raises_on_empty_inputs(self):
        mod = self._import()
        with self.assertRaises(ValueError):
            mod._run_hadd("/tmp/out.root", [])

    def test_calls_hadd_with_correct_args(self):
        mod = self._import()
        with patch.object(mod, "_find_hadd", return_value="/usr/bin/hadd"), \
             patch("subprocess.run") as mock_run, \
             tempfile.TemporaryDirectory() as tmpdir:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            out = os.path.join(tmpdir, "merged.root")
            mod._run_hadd(out, ["/a/f.root", "/b/f.root"])
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            self.assertEqual(cmd[0], "/usr/bin/hadd")
            self.assertIn("-f", cmd)
            self.assertIn(out, cmd)
            self.assertIn("/a/f.root", cmd)
            self.assertIn("/b/f.root", cmd)

    def test_raises_on_nonzero_exit(self):
        mod = self._import()
        with patch.object(mod, "_find_hadd", return_value="/usr/bin/hadd"), \
             patch("subprocess.run") as mock_run, \
             tempfile.TemporaryDirectory() as tmpdir:
            mock_run.return_value = MagicMock(returncode=1, stderr="hadd: error")
            with self.assertRaises(RuntimeError):
                mod._run_hadd(os.path.join(tmpdir, "out.root"), ["/in.root"])

    def test_creates_parent_directories(self):
        mod = self._import()
        with patch.object(mod, "_find_hadd", return_value="/usr/bin/hadd"), \
             patch("subprocess.run") as mock_run, \
             tempfile.TemporaryDirectory() as tmpdir:
            mock_run.return_value = MagicMock(returncode=0, stderr="")
            nested_out = os.path.join(tmpdir, "deep", "nested", "out.root")
            mod._run_hadd(nested_out, ["/in.root"])
            # Parent dirs must have been created
            self.assertTrue(os.path.isdir(os.path.join(tmpdir, "deep", "nested")))


# ===========================================================================
# Tests requiring law/luigi
# ===========================================================================


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeMixinProperties(unittest.TestCase):
    """Tests for MergeMixin parameter defaults and derived properties."""

    def _make_task(self, **params):
        import merge_tasks
        # Use MergeMetadata (simplest concrete task) to exercise MergeMixin
        return merge_tasks.MergeMetadata(**params)

    def test_merge_dir_default(self):
        """_merge_dir defaults to <workspace>/mergeRun_<name>."""
        import merge_tasks
        task = self._make_task(name="testRun")
        expected = os.path.join(merge_tasks.WORKSPACE, "mergeRun_testRun")
        self.assertEqual(task._merge_dir, expected)

    def test_merge_dir_custom(self):
        """_merge_dir uses --output-dir when provided."""
        task = self._make_task(name="testRun", output_dir="/custom/out")
        self.assertEqual(task._merge_dir, "/custom/out")

    def test_effective_input_dir_default(self):
        """_effective_input_dir defaults to <workspace>/condorSub_<name>."""
        import merge_tasks
        task = self._make_task(name="testRun")
        expected = os.path.join(merge_tasks.WORKSPACE, "condorSub_testRun")
        self.assertEqual(task._effective_input_dir, expected)

    def test_effective_input_dir_custom(self):
        """_effective_input_dir uses --input-dir when provided."""
        with tempfile.TemporaryDirectory() as tmpdir:
            task = self._make_task(name="testRun", input_dir=tmpdir)
            self.assertEqual(task._effective_input_dir, os.path.abspath(tmpdir))

    def test_framework_hash_empty_by_default(self):
        task = self._make_task(name="r")
        self.assertEqual(task.framework_hash, "")

    def test_manifest_glob_default(self):
        import merge_tasks
        task = self._make_task(name="r")
        self.assertEqual(task.manifest_glob, merge_tasks.DEFAULT_MANIFEST_GLOB)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeMixinFindManifests(unittest.TestCase):
    """Tests for MergeMixin._find_manifest_paths."""

    def _make_task(self, input_dir, **extra):
        import merge_tasks
        return merge_tasks.MergeMetadata(name="r", input_dir=input_dir, **extra)

    def test_raises_when_input_dir_missing(self):
        """Raises RuntimeError when input directory does not exist."""
        import merge_tasks
        task = merge_tasks.MergeMetadata(name="nonexistent")
        with self.assertRaises(RuntimeError) as ctx:
            task._find_manifest_paths()
        self.assertIn("Input directory not found", str(ctx.exception))

    def test_raises_when_no_manifests_found(self):
        """Raises RuntimeError when the directory exists but has no manifests."""
        import merge_tasks
        with tempfile.TemporaryDirectory() as tmpdir:
            task = merge_tasks.MergeMetadata(name="r", input_dir=tmpdir)
            with self.assertRaises(RuntimeError) as ctx:
                task._find_manifest_paths()
            self.assertIn("No manifests found", str(ctx.exception))

    def test_finds_nested_manifests(self):
        """Discovers manifests recursively."""
        import merge_tasks
        with tempfile.TemporaryDirectory() as tmpdir:
            # Create two nested manifests
            job0 = os.path.join(tmpdir, "samples", "s1", "job_0")
            job1 = os.path.join(tmpdir, "samples", "s1", "job_1")
            _make_full_manifest(job0)
            _make_full_manifest(job1)
            task = merge_tasks.MergeMetadata(name="r", input_dir=tmpdir)
            paths = task._find_manifest_paths()
            self.assertEqual(len(paths), 2)

    def test_custom_manifest_glob(self):
        """Custom glob pattern restricts which manifests are found."""
        import merge_tasks
        with tempfile.TemporaryDirectory() as tmpdir:
            job0 = os.path.join(tmpdir, "job_0")
            job1 = os.path.join(tmpdir, "job_1")
            os.makedirs(job0)
            os.makedirs(job1)
            _make_full_manifest(job0)
            # job1 only has a non-standard name
            _write_manifest(job1)
            task = merge_tasks.MergeMetadata(
                name="r",
                input_dir=tmpdir,
                manifest_glob="job_0/output_manifest.yaml",
            )
            paths = task._find_manifest_paths()
            self.assertEqual(len(paths), 1)
            self.assertIn("job_0", paths[0])


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeMixinLoadManifests(unittest.TestCase):
    """Tests for MergeMixin._load_manifests."""

    def _make_task(self):
        import merge_tasks
        return merge_tasks.MergeMetadata(name="r")

    def test_loads_valid_manifests(self):
        import merge_tasks
        with tempfile.TemporaryDirectory() as tmpdir:
            job0 = os.path.join(tmpdir, "job_0")
            p0 = _make_full_manifest(job0)
            task = self._make_task()
            loaded = task._load_manifests([p0])
            self.assertEqual(len(loaded), 1)
            path, manifest = loaded[0]
            self.assertEqual(path, p0)
            self.assertIsNotNone(manifest)

    def test_raises_on_corrupt_manifest(self):
        """Raises RuntimeError when a manifest file cannot be loaded."""
        import merge_tasks
        with tempfile.TemporaryDirectory() as tmpdir:
            bad_path = os.path.join(tmpdir, "output_manifest.yaml")
            with open(bad_path, "w") as fh:
                fh.write("not: valid: yaml: [\n")
            task = self._make_task()
            with self.assertRaises(RuntimeError) as ctx:
                task._load_manifests([bad_path])
            self.assertIn("Failed to load", str(ctx.exception))


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeSkimsTask(unittest.TestCase):
    """Tests for MergeSkims task construction and output paths."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_output_path(self):
        mod = self._import()
        task = mod.MergeSkims(name="myRun")
        expected = os.path.join(
            mod.WORKSPACE, "mergeRun_myRun", "skims", "output_manifest.yaml"
        )
        self.assertEqual(task.output().path, expected)

    def test_output_path_custom_output_dir(self):
        mod = self._import()
        task = mod.MergeSkims(name="myRun", output_dir="/custom/out")
        self.assertEqual(
            task.output().path, "/custom/out/skims/output_manifest.yaml"
        )

    def test_raises_when_no_skim_manifests(self):
        """MergeSkims raises RuntimeError in run() when no manifests have a skim schema."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            # Write a manifest with ONLY a histogram schema (no skim)
            job0 = os.path.join(tmpdir, "job_0")
            _make_histogram_manifest(job0)
            Path(os.path.join(job0, "histograms.root")).touch()

            task = mod.MergeSkims(name="r", input_dir=tmpdir, output_dir=out_dir)
            with self.assertRaises(RuntimeError) as ctx:
                with patch.object(task, "publish_message", return_value=None):
                    task.run()
            self.assertIn("skim", str(ctx.exception).lower())

    def test_run_merges_and_writes_manifest(self):
        """MergeSkims.run() calls hadd and writes merged manifest."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            # Create two job directories with skim manifests + fake ROOT files
            for i in range(2):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                _make_skim_manifest(job_dir, output_file="skim.root")
                # Create a fake skim file so the path check passes
                Path(os.path.join(job_dir, "skim.root")).touch()

            task = mod.MergeSkims(
                name="r", input_dir=tmpdir, output_dir=out_dir
            )

            # Mock hadd so we don't need ROOT; mock publish_message
            with patch.object(mod, "_run_hadd") as mock_hadd, \
                 patch.object(task, "publish_message", return_value=None):
                # Make hadd appear to create the output file
                def fake_hadd(out_path, in_paths):
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(out_path).touch()
                mock_hadd.side_effect = fake_hadd
                task.run()

            # Merged manifest must exist
            merged_manifest_path = task.output().path
            self.assertTrue(os.path.isfile(merged_manifest_path))

            # Sidecar JSON must exist
            groups_json = os.path.join(out_dir, "skims", "merged_skim_groups.json")
            self.assertTrue(os.path.isfile(groups_json))
            with open(groups_json) as fh:
                groups = json.load(fh)
            self.assertIn("skim.root", groups)

            # hadd must have been called once (one group: skim.root)
            mock_hadd.assert_called_once()

    def test_run_handles_systematic_variations(self):
        """MergeSkims groups systematic variation files separately."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            # Two jobs, each producing two systematic-variation skims
            for i in range(2):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                for syst in ("skim_jesUp.root", "skim_jesDown.root"):
                    os.makedirs(job_dir, exist_ok=True)
                    Path(os.path.join(job_dir, syst)).touch()

            # For this test we exercise _group_files_by_basename directly
            all_files = [
                os.path.join(tmpdir, f"job_{i}", syst)
                for i in range(2)
                for syst in ("skim_jesUp.root", "skim_jesDown.root")
            ]
            groups = mod._group_files_by_basename(all_files)
            self.assertIn("skim_jesUp.root", groups)
            self.assertIn("skim_jesDown.root", groups)
            self.assertEqual(len(groups["skim_jesUp.root"]), 2)
            self.assertEqual(len(groups["skim_jesDown.root"]), 2)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeHistogramsTask(unittest.TestCase):
    """Tests for MergeHistograms task construction and output paths."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_output_path(self):
        mod = self._import()
        task = mod.MergeHistograms(name="myRun")
        expected = os.path.join(
            mod.WORKSPACE, "mergeRun_myRun", "histograms", "output_manifest.yaml"
        )
        self.assertEqual(task.output().path, expected)

    def test_run_merges_and_writes_manifest(self):
        """MergeHistograms.run() calls hadd and writes merged manifest."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            for i in range(3):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                _make_histogram_manifest(job_dir)
                Path(os.path.join(job_dir, "histograms.root")).touch()

            task = mod.MergeHistograms(name="r", input_dir=tmpdir, output_dir=out_dir)
            with patch.object(mod, "_run_hadd") as mock_hadd, \
                 patch.object(task, "publish_message", return_value=None):
                def fake_hadd(out_path, in_paths):
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(out_path).touch()
                mock_hadd.side_effect = fake_hadd
                task.run()

            self.assertTrue(os.path.isfile(task.output().path))
            mock_hadd.assert_called_once()


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeCutflowsTask(unittest.TestCase):
    """Tests for MergeCutflows task construction and output paths."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_output_path(self):
        mod = self._import()
        task = mod.MergeCutflows(name="myRun")
        expected = os.path.join(
            mod.WORKSPACE, "mergeRun_myRun", "cutflows", "output_manifest.yaml"
        )
        self.assertEqual(task.output().path, expected)

    def test_run_merges_and_writes_manifest(self):
        """MergeCutflows.run() calls hadd and writes merged manifest."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            for i in range(2):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                _make_cutflow_manifest(job_dir)
                Path(os.path.join(job_dir, "cutflow.root")).touch()

            task = mod.MergeCutflows(name="r", input_dir=tmpdir, output_dir=out_dir)
            with patch.object(mod, "_run_hadd") as mock_hadd, \
                 patch.object(task, "publish_message", return_value=None):
                def fake_hadd(out_path, in_paths):
                    Path(out_path).parent.mkdir(parents=True, exist_ok=True)
                    Path(out_path).touch()
                mock_hadd.side_effect = fake_hadd
                task.run()

            self.assertTrue(os.path.isfile(task.output().path))
            mock_hadd.assert_called_once()


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeMetadataTask(unittest.TestCase):
    """Tests for MergeMetadata task construction and run logic."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_output_path(self):
        mod = self._import()
        task = mod.MergeMetadata(name="myRun")
        expected = os.path.join(
            mod.WORKSPACE, "mergeRun_myRun", "metadata", "output_manifest.yaml"
        )
        self.assertEqual(task.output().path, expected)

    def test_run_writes_provenance_summary(self):
        """MergeMetadata.run() writes provenance_summary.json."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            for i in range(3):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                _make_full_manifest(job_dir)

            task = mod.MergeMetadata(
                name="r", input_dir=tmpdir, output_dir=out_dir,
                framework_hash="fw_hash_xyz",
                user_repo_hash="user_hash_xyz",
            )
            with patch.object(task, "publish_message", return_value=None):
                task.run()

            # Merged manifest
            self.assertTrue(os.path.isfile(task.output().path))

            # Provenance summary
            summary_path = os.path.join(out_dir, "metadata", "provenance_summary.json")
            self.assertTrue(os.path.isfile(summary_path))
            with open(summary_path) as fh:
                summary = json.load(fh)
            self.assertEqual(summary["n_inputs"], 3)
            self.assertEqual(len(summary["provenance_records"]), 3)
            # framework_hash recorded in each record
            for rec in summary["provenance_records"]:
                self.assertEqual(rec["framework_hash"], "abc123")

    def test_provenance_preserved_in_merged_manifest(self):
        """Merged manifest records framework_hash from MergeMetadata parameters."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            _make_full_manifest(os.path.join(tmpdir, "job_0"))

            task = mod.MergeMetadata(
                name="r", input_dir=tmpdir, output_dir=out_dir,
                framework_hash="merged_fw_hash",
                user_repo_hash="merged_user_hash",
            )
            with patch.object(task, "publish_message", return_value=None):
                task.run()

            from output_schema import OutputManifest
            merged = OutputManifest.load_yaml(task.output().path)
            self.assertEqual(merged.framework_hash, "merged_fw_hash")
            self.assertEqual(merged.user_repo_hash, "merged_user_hash")

    def test_validation_failure_raises(self):
        """MergeMetadata raises MergeInputValidationError on incompatible manifests."""
        mod = self._import()
        from output_schema import OutputManifest, HistogramSchema
        # Create two manifests with incompatible histogram schema versions
        import yaml
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            for i in range(2):
                job_dir = os.path.join(tmpdir, f"job_{i}")
                os.makedirs(job_dir)
                manifest = {
                    "manifest_version": 1,
                    "skim": None,
                    "histograms": {
                        "schema_version": 1 + i,  # mismatched versions
                        "output_file": "h.root",
                        "histogram_names": [],
                        "axes": [],
                    },
                    "metadata": None,
                    "cutflow": None,
                    "law_artifacts": [],
                    "framework_hash": None,
                    "user_repo_hash": None,
                    "config_mtime": None,
                    "dataset_manifest_provenance": None,
                }
                with open(os.path.join(job_dir, "output_manifest.yaml"), "w") as fh:
                    yaml.dump(manifest, fh)

            task = mod.MergeMetadata(name="r", input_dir=tmpdir, output_dir=out_dir)
            from output_schema import MergeInputValidationError
            with self.assertRaises(MergeInputValidationError):
                with patch.object(task, "publish_message", return_value=None):
                    task.run()


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeAllTask(unittest.TestCase):
    """Tests for MergeAll task construction, requires logic, and output path."""

    def _import(self):
        import merge_tasks
        return merge_tasks

    def test_output_path(self):
        mod = self._import()
        task = mod.MergeAll(name="myRun")
        expected = os.path.join(mod.WORKSPACE, "mergeRun_myRun", "merge_all.done")
        self.assertEqual(task.output().path, expected)

    def test_requires_only_metadata_when_no_input_dir(self):
        """MergeAll.requires() includes only MergeMetadata when input_dir
        does not exist (schema probing returns all-False, but MergeMetadata
        is unconditionally added unless skip_metadata=True)."""
        mod = self._import()
        task = mod.MergeAll(name="nonexistent_run_xyz")
        reqs = task.requires()
        task_types = {type(r).__name__ for r in reqs}
        # MergeMetadata is always present unless explicitly skipped
        self.assertIn("MergeMetadata", task_types)
        # No skim/histogram/cutflow tasks without manifests
        self.assertNotIn("MergeSkims", task_types)
        self.assertNotIn("MergeHistograms", task_types)
        self.assertNotIn("MergeCutflows", task_types)

    def test_requires_metadata_by_default(self):
        """MergeAll always requires MergeMetadata unless skip_metadata=True."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only full manifests (skim + hist + cutflow + meta)
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertIn("MergeMetadata", task_types)

    def test_requires_all_tasks_when_all_schemas_present(self):
        """MergeAll requires skim/histogram/cutflow/metadata sub-tasks."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertIn("MergeSkims", task_types)
            self.assertIn("MergeHistograms", task_types)
            self.assertIn("MergeCutflows", task_types)
            self.assertIn("MergeMetadata", task_types)

    def test_skips_skim_when_no_skim_schema(self):
        """MergeAll does not require MergeSkims when no skim schema exists."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            # Only histogram manifests, no skim
            _make_histogram_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertNotIn("MergeSkims", task_types)
            self.assertIn("MergeHistograms", task_types)

    def test_skip_histograms_flag(self):
        """MergeAll respects --skip-histograms flag."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir, skip_histograms=True)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertNotIn("MergeHistograms", task_types)

    def test_skip_skims_flag(self):
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir, skip_skims=True)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertNotIn("MergeSkims", task_types)

    def test_skip_cutflows_flag(self):
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir, skip_cutflows=True)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertNotIn("MergeCutflows", task_types)

    def test_skip_metadata_flag(self):
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            _make_full_manifest(os.path.join(tmpdir, "job_0"))
            task = mod.MergeAll(name="r", input_dir=tmpdir, skip_metadata=True)
            reqs = task.requires()
            task_types = {type(r).__name__ for r in reqs}
            self.assertNotIn("MergeMetadata", task_types)

    def test_run_writes_done_file(self):
        """MergeAll.run() writes merge_all.done when sub-tasks are complete."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "output")
            _make_full_manifest(os.path.join(tmpdir, "job_0"))

            task = mod.MergeAll(name="r", input_dir=tmpdir, output_dir=out_dir)

            # Mock sub-task outputs so we don't actually need them to run
            mock_sub_outputs = []
            for sub_task in task.requires():
                mock_out = MagicMock()
                mock_out.path = os.path.join(out_dir, f"{type(sub_task).__name__}_done")
                mock_sub_outputs.append(mock_out)

            with patch.object(task, "requires", return_value=task.requires()), \
                 patch.object(task, "publish_message", return_value=None):
                task.run()

            done_file = task.output().path
            self.assertTrue(os.path.isfile(done_file))
            with open(done_file) as fh:
                data = json.load(fh)
            self.assertEqual(data["name"], "r")
            self.assertIn("merge_dir", data)

    def test_sub_tasks_inherit_parameters(self):
        """Sub-tasks created by MergeAll inherit name, input_dir, output_dir."""
        mod = self._import()
        with tempfile.TemporaryDirectory() as tmpdir:
            out_dir = os.path.join(tmpdir, "out")
            _make_full_manifest(os.path.join(tmpdir, "job_0"))

            task = mod.MergeAll(
                name="run42",
                input_dir=tmpdir,
                output_dir=out_dir,
                framework_hash="fw_hash_test",
            )
            for sub in task.requires():
                self.assertEqual(sub.name, "run42")
                self.assertEqual(sub.input_dir, tmpdir)
                self.assertEqual(sub.output_dir, out_dir)
                self.assertEqual(sub.framework_hash, "fw_hash_test")


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestLawCfgRegistration(unittest.TestCase):
    """Verify that merge_tasks is registered in law.cfg."""

    def test_merge_tasks_in_law_cfg(self):
        law_cfg = os.path.join(_LAW_DIR, "law.cfg")
        self.assertTrue(os.path.isfile(law_cfg), "law.cfg not found")
        with open(law_cfg) as fh:
            content = fh.read()
        self.assertIn("merge_tasks", content)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class TestMergeTasksModuleImports(unittest.TestCase):
    """Basic smoke tests for the merge_tasks module."""

    def test_module_importable(self):
        import merge_tasks
        self.assertIsNotNone(merge_tasks)

    def test_all_task_classes_present(self):
        import merge_tasks
        for cls_name in (
            "MergeMixin",
            "MergeSkims",
            "MergeHistograms",
            "MergeCutflows",
            "MergeMetadata",
            "MergeAll",
        ):
            self.assertTrue(
                hasattr(merge_tasks, cls_name),
                f"merge_tasks.{cls_name} not found",
            )

    def test_helper_functions_present(self):
        import merge_tasks
        for fn_name in ("_find_hadd", "_run_hadd", "_group_files_by_basename"):
            self.assertTrue(
                hasattr(merge_tasks, fn_name),
                f"merge_tasks.{fn_name} not found",
            )

    def test_task_namespace_is_empty_string(self):
        """All concrete tasks use task_namespace = '' for law CLI registration."""
        import merge_tasks
        for cls in (
            merge_tasks.MergeSkims,
            merge_tasks.MergeHistograms,
            merge_tasks.MergeCutflows,
            merge_tasks.MergeMetadata,
            merge_tasks.MergeAll,
        ):
            self.assertEqual(
                cls.task_namespace, "",
                f"{cls.__name__}.task_namespace must be ''",
            )

    def test_all_tasks_inherit_from_law_task(self):
        import merge_tasks
        for cls in (
            merge_tasks.MergeSkims,
            merge_tasks.MergeHistograms,
            merge_tasks.MergeCutflows,
            merge_tasks.MergeMetadata,
            merge_tasks.MergeAll,
        ):
            self.assertTrue(
                issubclass(cls, law.Task),
                f"{cls.__name__} must be a law.Task subclass",
            )

    def test_all_tasks_inherit_from_merge_mixin(self):
        import merge_tasks
        for cls in (
            merge_tasks.MergeSkims,
            merge_tasks.MergeHistograms,
            merge_tasks.MergeCutflows,
            merge_tasks.MergeMetadata,
            merge_tasks.MergeAll,
        ):
            self.assertTrue(
                issubclass(cls, merge_tasks.MergeMixin),
                f"{cls.__name__} must be a MergeMixin subclass",
            )


if __name__ == "__main__":
    unittest.main()
