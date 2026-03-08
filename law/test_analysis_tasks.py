#!/usr/bin/env python3
"""
Tests for the analysis_tasks law module.

Covers:
  - SkimTask: output path, job-dir setup, missing exe / files detection
  - HistFillTask: output path, skim dependency wiring, skim file lookup
  - _write_job_config: config key overrides, auxiliary file copying
  - _read_intweight_sign_hist: uproot reader + histogram-not-found error
  - _derive_stitch_weights: formula correctness, division-by-zero guard,
    histogram length mismatch, missing meta-file detection, tail trimming
  - _build_correctionlib_cset: structure, evaluation, trimming
  - StitchingDerivationTask: output path, missing config, bad config, end-to-end
    correctionlib JSON production

These tests do NOT require a running LAW scheduler, HTCondor cluster, ROOT
installation, or real analysis executables.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

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
# Optional dependency guard
# ---------------------------------------------------------------------------
try:
    import luigi  # noqa: F401
    import law    # noqa: F401
    _LAW_AVAILABLE = True
except ImportError:
    _LAW_AVAILABLE = False

try:
    import numpy as np
    _NUMPY_AVAILABLE = True
except ImportError:
    _NUMPY_AVAILABLE = False

try:
    import correctionlib  # noqa: F401
    _CORRECTIONLIB_AVAILABLE = True
except ImportError:
    _CORRECTIONLIB_AVAILABLE = False

_SKIP_LAW   = "law and luigi packages not available"
_SKIP_NP    = "numpy not available"
_SKIP_CLIB  = "correctionlib not available"


# ===========================================================================
# Helper: write a minimal YAML dataset manifest
# ===========================================================================

def _write_manifest(tmpdir: str, datasets: list) -> str:
    import yaml
    path = os.path.join(tmpdir, "datasets.yaml")
    with open(path, "w") as fh:
        yaml.dump({"datasets": datasets, "lumi": 1.0}, fh)
    return path


def _write_submit_config_template(tmpdir: str, extra: dict | None = None) -> str:
    cfg = {
        "saveTree": "Events",
        "treeList": "Events",
        "enableCounters": "true",
        "enableSkim": "true",
    }
    if extra:
        cfg.update(extra)
    path = os.path.join(tmpdir, "submit_config.txt")
    with open(path, "w") as fh:
        for k, v in cfg.items():
            fh.write(f"{k}={v}\n")
    return path


# ===========================================================================
# _write_job_config
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestWriteJobConfig(unittest.TestCase):
    """Tests for the _write_job_config helper."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    def test_basic_overrides(self):
        """fileList, saveFile, metaFile, sample and type are injected."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            dataset = DatasetEntry(
                name="ttbar",
                files=["f1.root", "f2.root"],
                dtype="mc",
            )
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(job_dir, template, dataset, out_dir)

            cfg_path = os.path.join(job_dir, "submit_config.txt")
            self.assertTrue(os.path.exists(cfg_path))

            cfg = {}
            with open(cfg_path) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v

            self.assertEqual(cfg["fileList"], "f1.root,f2.root")
            self.assertEqual(cfg["sample"], "ttbar")
            self.assertEqual(cfg["type"], "mc")
            self.assertIn("saveFile", cfg)
            self.assertIn("metaFile", cfg)
            self.assertIn("saveDirectory", cfg)

    def test_das_used_when_no_files(self):
        """fileList is set to the DAS path when files list is empty."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            dataset = DatasetEntry(
                name="wjets",
                das="/WJetsToLNu/RunIISummer20UL18NanoAODv9-106X/NANOAODSIM",
                dtype="mc",
            )
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(job_dir, template, dataset, out_dir)

            cfg = {}
            with open(os.path.join(job_dir, "submit_config.txt")) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v
            self.assertEqual(cfg["fileList"], dataset.das)

    def test_auxiliary_files_are_copied(self):
        """floats.txt and ints.txt referenced in the template are copied."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create auxiliary files alongside the template
            floats_path = os.path.join(tmpdir, "floats.txt")
            Path(floats_path).write_text("normScale=1.0\n")
            ints_path = os.path.join(tmpdir, "ints.txt")
            Path(ints_path).write_text("type=1\n")

            template = _write_submit_config_template(
                tmpdir,
                extra={"floatConfig": "floats.txt", "intConfig": "ints.txt"},
            )
            dataset = DatasetEntry(name="sample", files=["f.root"], dtype="mc")
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(job_dir, template, dataset, out_dir)

            self.assertTrue(os.path.exists(os.path.join(job_dir, "floats.txt")))
            self.assertTrue(os.path.exists(os.path.join(job_dir, "ints.txt")))

    def test_extra_overrides_applied(self):
        """extra_overrides dict is applied on top of computed values."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            dataset = DatasetEntry(name="data", files=["f.root"], dtype="data")
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(
                job_dir, template, dataset, out_dir,
                extra_overrides={"fileList": "/my/skim.root"},
            )

            cfg = {}
            with open(os.path.join(job_dir, "submit_config.txt")) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v
            self.assertEqual(cfg["fileList"], "/my/skim.root")


# ===========================================================================
# SkimTask
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestSkimTask(unittest.TestCase):
    """Unit tests for SkimTask."""

    def _make_branch_task(self, entry, **kwargs):
        """Create a SkimTask branch instance (branch=0) with mocked branch_data."""
        import analysis_tasks
        defaults = dict(
            exe="/fake/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="testSkim",
            branch=0,
        )
        defaults.update(kwargs)
        task = analysis_tasks.SkimTask(**defaults)
        # branch_data is a LAW property; patch it for unit testing
        task._test_branch_data = entry
        return task

    def test_output_path(self):
        """output() returns a LocalFileTarget under skimRun_{name}/job_outputs/."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from unittest.mock import PropertyMock

        entry = DatasetEntry(name="ttbar", files=["f.root"])
        task = self._make_branch_task(entry, name="myRun")

        with patch.object(type(task), "branch_data", new_callable=PropertyMock,
                          return_value=entry):
            out = task.output()

        import law as law_mod
        self.assertIsInstance(out, law_mod.LocalFileTarget)
        self.assertIn("skimRun_myRun", out.path)
        self.assertTrue(out.path.endswith("ttbar.done"))

    def test_run_dir_property(self):
        """_run_dir is workspace/skimRun_{name}."""
        import analysis_tasks
        task = analysis_tasks.SkimTask(
            exe="/fake/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="run42",
        )
        expected = os.path.join(analysis_tasks.WORKSPACE, "skimRun_run42")
        self.assertEqual(task._run_dir, expected)

    def test_run_missing_exe_raises(self):
        """run() raises RuntimeError when the executable does not exist."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from unittest.mock import PropertyMock

        entry = DatasetEntry(name="ttbar", files=["f.root"])
        task = analysis_tasks.SkimTask(
            exe="/nonexistent/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="t",
            branch=0,
        )
        with patch.object(type(task), "branch_data", new_callable=PropertyMock,
                          return_value=entry):
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
        self.assertIn("not found", str(ctx.exception))

    def test_run_no_files_raises(self):
        """run() raises RuntimeError when dataset has no files and no DAS path."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from unittest.mock import PropertyMock

        entry = DatasetEntry(name="nofiles")  # no files, no das

        with tempfile.NamedTemporaryFile(suffix=".exe", delete=False) as exe_f:
            exe_path = exe_f.name
        os.chmod(exe_path, 0o755)
        try:
            task = analysis_tasks.SkimTask(
                exe=exe_path,
                submit_config="/fake/cfg.txt",
                dataset_manifest="/fake/manifest.yaml",
                name="t",
                branch=0,
            )
            with patch.object(type(task), "branch_data", new_callable=PropertyMock,
                              return_value=entry):
                with self.assertRaises(RuntimeError) as ctx:
                    task.run()
            self.assertIn("no files", str(ctx.exception).lower())
        finally:
            os.unlink(exe_path)

    def test_task_namespace(self):
        import analysis_tasks
        task = analysis_tasks.SkimTask(
            exe="/f", submit_config="/f", dataset_manifest="/f", name="t"
        )
        self.assertEqual(task.task_namespace, "")


# ===========================================================================
# HistFillTask
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestHistFillTask(unittest.TestCase):
    """Unit tests for HistFillTask."""

    def _make_task(self, **kwargs):
        import analysis_tasks
        defaults = dict(
            exe="/fake/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="testHist",
        )
        defaults.update(kwargs)
        return analysis_tasks.HistFillTask(**defaults)

    def test_output_path(self):
        """output() returns a LocalFileTarget under histRun_{name}/job_outputs/."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from unittest.mock import PropertyMock

        entry = DatasetEntry(name="wjets", files=["f.root"])
        task = analysis_tasks.HistFillTask(
            exe="/fake/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="myHistRun",
            branch=0,
        )
        with patch.object(type(task), "branch_data", new_callable=PropertyMock,
                          return_value=entry):
            out = task.output()

        import law as law_mod
        self.assertIsInstance(out, law_mod.LocalFileTarget)
        self.assertIn("histRun_myHistRun", out.path)
        self.assertTrue(out.path.endswith("wjets.done"))

    def test_run_dir_property(self):
        import analysis_tasks
        task = self._make_task(name="run7")
        expected = os.path.join(analysis_tasks.WORKSPACE, "histRun_run7")
        self.assertEqual(task._run_dir, expected)

    def test_requires_none_without_skim_name(self):
        """At branch level, requires() returns None when --skim-name is not set."""
        import analysis_tasks
        task = analysis_tasks.HistFillTask(
            exe="/fake/exe", submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml", name="h",
            skim_name="", branch=0,
        )
        self.assertIsNone(task.requires())

    def test_requires_skim_task_when_skim_name_set(self):
        """At branch level, requires() returns a SkimTask when --skim-name is given."""
        import analysis_tasks
        task = analysis_tasks.HistFillTask(
            exe="/fake/exe", submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml", name="h",
            skim_name="mySkimRun", branch=0,
        )
        req = task.requires()
        self.assertIsInstance(req, analysis_tasks.SkimTask)
        self.assertEqual(req.name, "mySkimRun")

    def test_skim_run_dir_property(self):
        import analysis_tasks
        task = self._make_task(skim_name="mySkimRun")
        expected = os.path.join(analysis_tasks.WORKSPACE, "skimRun_mySkimRun")
        self.assertEqual(task._skim_run_dir, expected)

    def test_run_uses_skim_file_when_skim_name_set(self):
        """run() wires the skim output ROOT file as fileList override."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create a fake executable
            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            # Create a fake skim output file
            skim_dir = os.path.join(tmpdir, "skimRun_mySkimRun", "outputs", "wjets")
            Path(skim_dir).mkdir(parents=True)
            skim_file = os.path.join(skim_dir, "skim.root")
            Path(skim_file).write_text("fake ROOT")

            # Create a template config
            template = _write_submit_config_template(tmpdir)

            entry = DatasetEntry(name="wjets", files=["original.root"])
            task = analysis_tasks.HistFillTask(
                exe=exe_path,
                submit_config=template,
                dataset_manifest="/fake/manifest.yaml",
                name="myHistRun",
                branch=0,
                skim_name="mySkimRun",
            )

            # Override _run_dir and _skim_run_dir to use tmpdir
            orig_run = analysis_tasks.HistFillTask._run_dir.fget
            orig_skim = analysis_tasks.HistFillTask._skim_run_dir.fget
            analysis_tasks.HistFillTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"histRun_{self.name}")
            )
            analysis_tasks.HistFillTask._skim_run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.skim_name}")
            )
            try:
                with patch.object(type(task), "branch_data", new_callable=PropertyMock,
                                  return_value=entry):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        task.run()

                # Verify that the job submit_config.txt uses skim.root
                job_cfg = {}
                cfg_path = os.path.join(
                    tmpdir, "histRun_myHistRun", "jobs", "wjets", "submit_config.txt"
                )
                with open(cfg_path) as fh:
                    for line in fh:
                        line = line.strip()
                        if "=" in line:
                            k, v = line.split("=", 1)
                            job_cfg[k] = v
                self.assertEqual(job_cfg["fileList"], skim_file)
            finally:
                analysis_tasks.HistFillTask._run_dir = property(orig_run)
                analysis_tasks.HistFillTask._skim_run_dir = property(orig_skim)

    def test_task_namespace(self):
        self.assertEqual(self._make_task().task_namespace, "")


# ===========================================================================
# _read_intweight_sign_hist (uproot path)
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
@unittest.skipUnless(_NUMPY_AVAILABLE, _SKIP_NP)
class TestReadIntweightSignHist(unittest.TestCase):
    """Tests for the _read_intweight_sign_hist helper."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    def test_uproot_success(self):
        """Returns bin values via uproot when the histogram is present."""
        mod = self._import()

        fake_values = [1.0, 5.0, -2.0, 3.0]
        mock_hist = MagicMock()
        mock_hist.values.return_value = __import__("numpy").array(fake_values)

        mock_file = MagicMock()
        mock_file.keys.return_value = ["counter_intWeightSignSum_sampleA;1"]
        mock_file.__getitem__ = MagicMock(return_value=mock_hist)
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)

        mock_uproot = MagicMock()
        mock_uproot.open.return_value = mock_file

        with patch.dict(sys.modules, {"uproot": mock_uproot}):
            result = mod._read_intweight_sign_hist("/fake/meta.root", "sampleA")

        self.assertEqual(result, fake_values)

    def test_histogram_not_found_raises(self):
        """RuntimeError is raised when the expected histogram is absent."""
        mod = self._import()

        mock_file = MagicMock()
        mock_file.keys.return_value = ["counter_weightSum_sampleA;1"]
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=False)

        mock_uproot = MagicMock()
        mock_uproot.open.return_value = mock_file

        with patch.dict(sys.modules, {"uproot": mock_uproot}):
            with self.assertRaises(RuntimeError) as ctx:
                mod._read_intweight_sign_hist("/fake/meta.root", "sampleA")
        self.assertIn("counter_intWeightSignSum_sampleA", str(ctx.exception))

    def test_no_uproot_no_root_raises(self):
        """RuntimeError is raised when neither uproot nor ROOT is importable."""
        mod = self._import()

        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name in ("uproot", "ROOT"):
                raise ImportError(f"Mocked unavailable: {name}")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with self.assertRaises(RuntimeError) as ctx:
                mod._read_intweight_sign_hist("/fake/meta.root", "s")
        self.assertIn("uproot", str(ctx.exception).lower())


# ===========================================================================
# _derive_stitch_weights
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
@unittest.skipUnless(_NUMPY_AVAILABLE, _SKIP_NP)
class TestDeriveStitchWeights(unittest.TestCase):
    """Tests for the binwise stitching formula _derive_stitch_weights."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    # ---- helper: mock _read_intweight_sign_hist per sample -----------------

    def _mock_read(self, sample_values: dict):
        """Return a side-effect function mapping meta_file→values."""
        def _read(meta_file, sample_name):
            return sample_values[sample_name]
        return _read

    # ---- tests -------------------------------------------------------------

    def test_formula_two_samples(self):
        """b_n(k) = C_n(k) / Σ_m C_m(k) for a two-sample group."""
        mod = self._import()

        # C_0 = [30, 10, 60], C_1 = [70, 90, 40]
        # total  = [100, 100, 100]
        # b_0    = [0.3, 0.1, 0.6]
        # b_1    = [0.7, 0.9, 0.4]
        sample_values = {
            "s0": [30.0, 10.0, 60.0],
            "s1": [70.0, 90.0, 40.0],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            # Build meta_files separately to avoid dict modification during iteration
            meta_files = {
                name: os.path.join(tmpdir, f"{name}.root")
                for name in sample_values
            }
            for p in meta_files.values():
                Path(p).write_text("")

            with patch.object(mod, "_read_intweight_sign_hist",
                              side_effect=self._mock_read(sample_values)):
                result = mod._derive_stitch_weights("grp", meta_files)

        import numpy as np
        np.testing.assert_allclose(result["s0"], [0.3, 0.1, 0.6], atol=1e-10)
        np.testing.assert_allclose(result["s1"], [0.7, 0.9, 0.4], atol=1e-10)

    def test_formula_sum_to_one(self):
        """Scale factors in each bin sum to 1 (or 0 when total = 0)."""
        mod = self._import()

        sample_values = {
            "a": [10.0, 0.0, 5.0],
            "b": [20.0, 0.0, 5.0],
            "c": [70.0, 0.0, 90.0],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            meta_files = {}
            for name in sample_values:
                p = os.path.join(tmpdir, f"{name}.root")
                Path(p).write_text("")
                meta_files[name] = p

            with patch.object(mod, "_read_intweight_sign_hist",
                              side_effect=self._mock_read(sample_values)):
                result = mod._derive_stitch_weights("grp", meta_files)

        import numpy as np
        factors = np.array([result["a"], result["b"], result["c"]])
        col_sums = factors.sum(axis=0)
        # bin 0 and bin 2 must sum to 1; bin 1 has total=0 so factors are 0
        self.assertAlmostEqual(col_sums[0], 1.0, places=10)
        self.assertAlmostEqual(col_sums[1], 0.0, places=10)
        self.assertAlmostEqual(col_sums[2], 1.0, places=10)

    def test_division_by_zero_guard(self):
        """Bins where all C_m(k) = 0 receive scale factor 0.0."""
        mod = self._import()

        sample_values = {"s0": [0.0, 5.0], "s1": [0.0, 5.0]}

        with tempfile.TemporaryDirectory() as tmpdir:
            meta_files = {}
            for name in sample_values:
                p = os.path.join(tmpdir, f"{name}.root")
                Path(p).write_text("")
                meta_files[name] = p

            with patch.object(mod, "_read_intweight_sign_hist",
                              side_effect=self._mock_read(sample_values)):
                result = mod._derive_stitch_weights("grp", meta_files)

        self.assertEqual(result["s0"][0], 0.0)
        self.assertEqual(result["s1"][0], 0.0)

    def test_histogram_length_mismatch_raises(self):
        """RuntimeError is raised when two samples have different histogram lengths."""
        mod = self._import()

        sample_values = {"s0": [1.0, 2.0, 3.0], "s1": [1.0, 2.0]}

        with tempfile.TemporaryDirectory() as tmpdir:
            meta_files = {}
            for name in sample_values:
                p = os.path.join(tmpdir, f"{name}.root")
                Path(p).write_text("")
                meta_files[name] = p

            with patch.object(mod, "_read_intweight_sign_hist",
                              side_effect=self._mock_read(sample_values)):
                with self.assertRaises(RuntimeError) as ctx:
                    mod._derive_stitch_weights("grp", meta_files)
        self.assertIn("mismatch", str(ctx.exception).lower())

    def test_missing_meta_file_raises(self):
        """RuntimeError is raised when a meta file path does not exist."""
        mod = self._import()
        meta_files = {"s0": "/nonexistent/path/meta.root"}
        with self.assertRaises(RuntimeError) as ctx:
            mod._derive_stitch_weights("grp", meta_files)
        self.assertIn("not found", str(ctx.exception).lower())

    def test_trailing_zero_bins_trimmed(self):
        """Trailing all-zero bins are removed from the result arrays."""
        mod = self._import()

        # bins 4 and 5 are zero for all samples
        sample_values = {
            "s0": [10.0, 5.0, 0.0, 8.0, 0.0, 0.0],
            "s1": [90.0, 95.0, 0.0, 92.0, 0.0, 0.0],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            meta_files = {}
            for name in sample_values:
                p = os.path.join(tmpdir, f"{name}.root")
                Path(p).write_text("")
                meta_files[name] = p

            with patch.object(mod, "_read_intweight_sign_hist",
                              side_effect=self._mock_read(sample_values)):
                result = mod._derive_stitch_weights("grp", meta_files)

        # Only 4 bins should remain (last non-zero bin is at index 3)
        self.assertEqual(len(result["s0"]), 4)
        self.assertEqual(len(result["s1"]), 4)


# ===========================================================================
# _build_correctionlib_cset
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
@unittest.skipUnless(_NUMPY_AVAILABLE, _SKIP_NP)
@unittest.skipUnless(_CORRECTIONLIB_AVAILABLE, _SKIP_CLIB)
class TestBuildCorrectionlibCset(unittest.TestCase):
    """Tests for _build_correctionlib_cset."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    def test_one_correction_per_group(self):
        """CorrectionSet contains exactly one Correction per group."""
        mod = self._import()
        all_sf = {
            "grpA": {"s0": [0.3, 0.7], "s1": [0.7, 0.3]},
            "grpB": {"t0": [1.0]},
        }
        cset = mod._build_correctionlib_cset(all_sf)
        names = [c.name for c in cset.corrections]
        self.assertIn("grpA", names)
        self.assertIn("grpB", names)
        self.assertEqual(len(cset.corrections), 2)

    def test_correction_inputs_and_output(self):
        """Each Correction has sample_name + stitch_id inputs and weight output."""
        mod = self._import()
        cset = mod._build_correctionlib_cset(
            {"grp": {"s0": [0.4, 0.6], "s1": [0.6, 0.4]}}
        )
        corr = cset.corrections[0]
        input_names = [v.name for v in corr.inputs]
        self.assertIn("sample_name", input_names)
        self.assertIn("stitch_id", input_names)
        self.assertEqual(corr.output.name, "weight")

    def test_evaluation_correct(self):
        """Evaluating the CorrectionSet returns the expected scale factor."""
        mod = self._import()
        import correctionlib

        all_sf = {"wjets_ht": {"s0": [0.3, 0.1, 0.6], "s1": [0.7, 0.9, 0.4]}}
        cset_schema = mod._build_correctionlib_cset(all_sf)
        # Serialize and reload to ensure the JSON round-trip works
        cset = correctionlib.CorrectionSet.from_string(
            cset_schema.model_dump_json()
        )
        self.assertAlmostEqual(cset["wjets_ht"].evaluate("s0", 0), 0.3, places=10)
        self.assertAlmostEqual(cset["wjets_ht"].evaluate("s1", 2), 0.4, places=10)

    def test_schema_version(self):
        """CorrectionSet schema_version is 2."""
        mod = self._import()
        cset = mod._build_correctionlib_cset({"g": {"s": [1.0]}})
        self.assertEqual(cset.schema_version, 2)

    def test_json_is_valid(self):
        """The serialised JSON is valid and parseable."""
        mod = self._import()
        all_sf = {"grp": {"a": [0.5, 0.5], "b": [0.5, 0.5]}}
        cset = mod._build_correctionlib_cset(all_sf)
        raw = cset.model_dump_json()
        parsed = json.loads(raw)
        self.assertIn("corrections", parsed)
        self.assertEqual(parsed["schema_version"], 2)

    def test_missing_correctionlib_raises(self):
        """RuntimeError is raised when correctionlib is not installed."""
        mod = self._import()

        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "correctionlib.schemav2":
                raise ImportError("Mocked unavailable")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            with self.assertRaises(RuntimeError) as ctx:
                mod._build_correctionlib_cset({"g": {"s": [1.0]}})
        self.assertIn("correctionlib", str(ctx.exception).lower())


# ===========================================================================
# StitchingDerivationTask
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestStitchingDerivationTask(unittest.TestCase):
    """Tests for StitchingDerivationTask."""

    def _make_task(self, **kwargs):
        import analysis_tasks
        defaults = dict(stitch_config="dummy.yaml", name="testStitch")
        defaults.update(kwargs)
        return analysis_tasks.StitchingDerivationTask(**defaults)

    def test_output_path(self):
        """output() returns a LocalFileTarget pointing to stitch_weights.json."""
        import analysis_tasks
        task = self._make_task(name="myStitch")
        out = task.output()
        import law as law_mod
        self.assertIsInstance(out, law_mod.LocalFileTarget)
        self.assertIn("stitchWeights_myStitch", out.path)
        self.assertTrue(out.path.endswith("stitch_weights.json"))

    def test_output_dir_property(self):
        import analysis_tasks
        task = self._make_task(name="run1")
        expected = os.path.join(analysis_tasks.WORKSPACE, "stitchWeights_run1")
        self.assertEqual(task._output_dir, expected)

    def test_missing_config_raises(self):
        """run() raises RuntimeError when the stitch config file is absent."""
        task = self._make_task(stitch_config="/nonexistent/stitch.yaml")
        with self.assertRaises(RuntimeError) as ctx:
            task.run()
        self.assertIn("not found", str(ctx.exception).lower())

    def test_bad_config_no_groups_raises(self):
        """run() raises RuntimeError when config has no 'groups' key."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("foo: bar\n")
            path = f.name
        try:
            task = self._make_task(stitch_config=path)
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
            self.assertIn("groups", str(ctx.exception))
        finally:
            os.unlink(path)

    def test_bad_group_no_meta_files_raises(self):
        """run() raises RuntimeError when a group has no 'meta_files' key."""
        import yaml
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump({"groups": {"g": {"other_key": "value"}}}, f)
            path = f.name
        try:
            task = self._make_task(stitch_config=path)
            with self.assertRaises(RuntimeError) as ctx:
                task.run()
            self.assertIn("meta_files", str(ctx.exception))
        finally:
            os.unlink(path)

    @unittest.skipUnless(_NUMPY_AVAILABLE, _SKIP_NP)
    @unittest.skipUnless(_CORRECTIONLIB_AVAILABLE, _SKIP_CLIB)
    def test_run_produces_correctionlib_json(self):
        """run() writes a valid correctionlib JSON to the output path."""
        import analysis_tasks
        import yaml
        import correctionlib

        with tempfile.TemporaryDirectory() as tmpdir:
            # Create fake meta ROOT files (existence check only)
            meta0 = os.path.join(tmpdir, "s0_meta.root")
            meta1 = os.path.join(tmpdir, "s1_meta.root")
            Path(meta0).write_text("")
            Path(meta1).write_text("")

            stitch_cfg = os.path.join(tmpdir, "stitch.yaml")
            with open(stitch_cfg, "w") as fh:
                yaml.dump(
                    {"groups": {"wjets_ht": {"meta_files": {"s0": meta0, "s1": meta1}}}},
                    fh,
                )

            task = analysis_tasks.StitchingDerivationTask(
                stitch_config=stitch_cfg,
                name="myStitch",
            )

            # Override _output_dir to use tmpdir
            orig = analysis_tasks.StitchingDerivationTask._output_dir.fget
            analysis_tasks.StitchingDerivationTask._output_dir = property(
                lambda self: os.path.join(tmpdir, f"stitchWeights_{self.name}")
            )
            try:
                # Provide fake sign-count data via mocked reader
                fake_values = {"s0": [30.0, 10.0, 0.0], "s1": [70.0, 90.0, 0.0]}
                with patch.object(
                    analysis_tasks,
                    "_read_intweight_sign_hist",
                    side_effect=lambda f, s: fake_values[s],
                ):
                    task.run()

                out_path = task.output().path
                self.assertTrue(os.path.exists(out_path))

                # Must be valid correctionlib JSON with the expected correction
                cset = correctionlib.CorrectionSet.from_file(out_path)
                self.assertIn("wjets_ht", list(cset.keys()))

                # Evaluate a known scale factor: s0 bin 0 = 30/(30+70) = 0.3
                sf = cset["wjets_ht"].evaluate("s0", 0)
                self.assertAlmostEqual(sf, 0.3, places=10)

                # Bin 2 is trimmed (all-zero total); flow='clamp' returns last bin
                # s0 bin 1 = 10/(10+90) = 0.1
                sf_clamped = cset["wjets_ht"].evaluate("s0", 1)
                self.assertAlmostEqual(sf_clamped, 0.1, places=10)

            finally:
                analysis_tasks.StitchingDerivationTask._output_dir = property(orig)

    def test_task_namespace(self):
        self.assertEqual(self._make_task().task_namespace, "")


# ===========================================================================
# Module smoke tests
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestAnalysisTasksModule(unittest.TestCase):
    """Smoke tests confirming the module is correctly structured."""

    def test_importable(self):
        import analysis_tasks  # noqa: F401

    def test_tasks_defined(self):
        import analysis_tasks
        for name in ("SkimTask", "HistFillTask", "StitchingDerivationTask"):
            self.assertTrue(hasattr(analysis_tasks, name), f"{name} not defined")

    def test_helpers_defined(self):
        import analysis_tasks
        for name in (
            "_write_job_config",
            "_read_intweight_sign_hist",
            "_derive_stitch_weights",
            "_build_correctionlib_cset",
        ):
            self.assertTrue(hasattr(analysis_tasks, name), f"{name} not defined")

    def test_task_namespaces(self):
        import analysis_tasks
        for cls_name in ("SkimTask", "HistFillTask", "StitchingDerivationTask"):
            cls = getattr(analysis_tasks, cls_name)
            self.assertEqual(cls.task_namespace, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
