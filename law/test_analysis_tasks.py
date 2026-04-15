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
import shutil
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


def _make_preselection_manifest(skim_file: str):
    """Build a minimal OutputManifest with one preselection IntermediateArtifactSchema."""
    from output_schema import OutputManifest, IntermediateArtifactSchema
    return OutputManifest(
        intermediate_artifacts=[
            IntermediateArtifactSchema(
                artifact_kind="preselection",
                output_file=skim_file,
            )
        ]
    )


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
            # Ensure the DAS path is redirected to XRootD (root://...) for remote access
            self.assertTrue(cfg["fileList"].startswith("root://"))
            self.assertIn(dataset.das, cfg["fileList"])

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

    def test_auxiliary_files_resolve_from_analysis_root(self):
        """cfg/... references are resolved relative to the analysis root."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            Path(cfg_dir).mkdir(parents=True)

            manifest_path = os.path.join(cfg_dir, "dataset_manifest.yaml")
            Path(manifest_path).write_text("datasets: []\nlumi: 1.0\n")
            floats_path = os.path.join(cfg_dir, "floats.yaml")
            Path(floats_path).write_text("normScale: 1.0\n")

            template = _write_submit_config_template(
                cfg_dir,
                extra={
                    "sampleConfig": "cfg/dataset_manifest.yaml",
                    "floatConfig": "cfg/floats.yaml",
                },
            )
            dataset = DatasetEntry(name="sample", files=["f.root"], dtype="mc")
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(job_dir, template, dataset, out_dir)

            self.assertTrue(os.path.exists(os.path.join(job_dir, "dataset_manifest.yaml")))
            self.assertTrue(os.path.exists(os.path.join(job_dir, "floats.yaml")))

            cfg = {}
            with open(os.path.join(job_dir, "submit_config.txt")) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v

            self.assertEqual(cfg["sampleConfig"], "dataset_manifest.yaml")
            self.assertEqual(cfg["floatConfig"], "floats.yaml")

    def test_counter_weight_branch_removed_for_data(self):
        """Data jobs do not keep MC-only counter weight branches."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(
                tmpdir,
                extra={"counterWeightBranch": "Generator_weight"},
            )
            dataset = DatasetEntry(name="data", files=["f.root"], dtype="data")
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

            self.assertNotIn("counterWeightBranch", cfg)

    def test_manifest_metadata_is_injected_into_job_config(self):
        """Per-job configs include scalar dataset-manifest metadata and numeric type."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            dataset = DatasetEntry(
                name="dy0j",
                files=["f.root"],
                dtype="mc",
                year=2022,
                campaign="Run3Summer22",
                process="dy2jets_0J",
                group="dy2jets_2022",
                stitch_id=2,
                sample_type=42,
                xsec=5020.0,
                extra_scale=1.5,
                filter_efficiency=0.75,
                sum_weights=12345.0,
                parent="parent_sample",
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

            self.assertEqual(cfg["sample"], "dy0j")
            self.assertEqual(cfg["name"], "dy0j")
            self.assertEqual(cfg["dtype"], "mc")
            self.assertEqual(cfg["type"], "42")
            self.assertEqual(cfg["stitch_id"], "2")
            self.assertEqual(cfg["sample_type"], "42")
            self.assertEqual(cfg["year"], "2022")
            self.assertEqual(cfg["campaign"], "Run3Summer22")
            self.assertEqual(cfg["process"], "dy2jets_0J")
            self.assertEqual(cfg["group"], "dy2jets_2022")
            self.assertEqual(cfg["xsec"], "5020.0")
            self.assertEqual(cfg["extra_scale"], "1.5")
            self.assertEqual(cfg["extraScale"], "1.5")
            self.assertEqual(cfg["filter_efficiency"], "0.75")
            self.assertEqual(cfg["filterEfficiency"], "0.75")
            self.assertEqual(cfg["sum_weights"], "12345.0")
            self.assertEqual(cfg["norm"], "12345.0")
            self.assertEqual(cfg["parent"], "parent_sample")

    def test_rewrite_job_output_destinations(self):
        """Concrete skim jobs can rewrite their final save targets independently."""
        mod = self._import()

        with tempfile.TemporaryDirectory() as tmpdir:
            job_dir = os.path.join(tmpdir, "job_0")
            Path(job_dir).mkdir()
            cfg_path = os.path.join(job_dir, "submit_config.txt")
            Path(cfg_path).write_text(
                "saveDirectory=/tmp/local/out\n"
                "saveFile=/tmp/local/out/skim.root\n"
                "metaFile=/tmp/local/out/meta.root\n"
                "sample=test\n"
            )

            mod._rewrite_job_output_destinations(
                cfg_path,
                "/eos/user/b/test/sample",
            )

            cfg = {}
            with open(cfg_path) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v

            self.assertEqual(cfg["saveDirectory"], "/eos/user/b/test/sample")
            self.assertEqual(cfg["saveFile"], "/eos/user/b/test/sample/output_0.root")
            self.assertEqual(cfg["metaFile"], "/eos/user/b/test/sample/meta_0.root")
            self.assertEqual(cfg["sample"], "test")

    def test_aux_directory_is_copied_and_rewritten(self):
        """Copied YAML payloads reference the staged aux/ directory."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            aux_dir = os.path.join(analysis_dir, "aux")
            Path(cfg_dir).mkdir(parents=True)
            Path(aux_dir).mkdir(parents=True)

            Path(os.path.join(aux_dir, "muon_scalesmearing.json.gz")).write_text("payload\n")
            Path(os.path.join(aux_dir, "jetvetomaps.json.gz")).write_text("payload\n")
            Path(os.path.join(cfg_dir, "corrections.yaml")).write_text(
                "- file: aux/muon_scalesmearing.json.gz\n"
                "- file: jetvetomaps.json.gz\n"
            )

            template = _write_submit_config_template(
                cfg_dir,
                extra={"correctionConfig": "cfg/corrections.yaml"},
            )
            dataset = DatasetEntry(name="sample", files=["f.root"], dtype="mc")
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(job_dir, template, dataset, out_dir)

            self.assertTrue(os.path.exists(os.path.join(job_dir, "aux", "muon_scalesmearing.json.gz")))
            self.assertTrue(os.path.exists(os.path.join(job_dir, "aux", "jetvetomaps.json.gz")))

            with open(os.path.join(job_dir, "corrections.yaml")) as fh:
                contents = fh.read()

            self.assertIn("file: aux/muon_scalesmearing.json.gz", contents)
            self.assertIn("file: aux/jetvetomaps.json.gz", contents)

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

    def test_shared_config_layout_without_local_staging(self):
        """File-source job configs keep cfg/ references and avoid local payload copies."""
        mod = self._import()
        from dataset_manifest import DatasetEntry

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            aux_dir = os.path.join(analysis_dir, "aux")
            Path(cfg_dir).mkdir(parents=True)
            Path(aux_dir).mkdir(parents=True)

            Path(os.path.join(cfg_dir, "corrections.yaml")).write_text(
                "- file: aux/muon_scalesmearing.json.gz\n"
            )
            Path(os.path.join(aux_dir, "muon_scalesmearing.json.gz")).write_text("payload\n")

            template = _write_submit_config_template(
                cfg_dir,
                extra={"correctionConfig": "cfg/corrections.yaml"},
            )
            dataset = DatasetEntry(name="sample", files=["f.root"], dtype="mc")
            job_dir = os.path.join(tmpdir, "job")
            out_dir = os.path.join(tmpdir, "out")
            Path(job_dir).mkdir()

            mod._write_job_config(
                job_dir,
                template,
                dataset,
                out_dir,
                stage_shared_inputs_locally=False,
            )

            self.assertFalse(os.path.exists(os.path.join(job_dir, "corrections.yaml")))
            self.assertFalse(os.path.exists(os.path.join(job_dir, "aux")))

            cfg = {}
            with open(os.path.join(job_dir, "submit_config.txt")) as fh:
                for line in fh:
                    line = line.strip()
                    if "=" in line:
                        k, v = line.split("=", 1)
                        cfg[k] = v
            self.assertEqual(cfg["correctionConfig"], "cfg/corrections.yaml")


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestRunSkimTestJob(unittest.TestCase):
    """Tests for the local pre-flight skim test job."""

    def test_run_ignores_container_setup(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "ttbar", "files": ["root://cms-xrd-global.cern.ch//store/test.root"]}],
            )

            task = analysis_tasks.RunSkimTestJob(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                container_setup="/cvmfs/unpacked.cern.ch/gitlab-registry.cern.ch/batch-team/containers/plusbatch/el9-full:latest",
            )

            orig_run_dir = analysis_tasks.RunSkimTestJob._run_dir.fget
            analysis_tasks.RunSkimTestJob._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                with patch("analysis_tasks._run_analysis_job", return_value="done:test") as mock_run:
                    task.run()

                self.assertEqual(mock_run.call_count, 1)
                self.assertEqual(mock_run.call_args.kwargs.get("container_setup"), "")
            finally:
                analysis_tasks.RunSkimTestJob._run_dir = property(orig_run_dir)


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

    def test_complete_returns_false_when_prep_missing_in_file_source_mode(self):
        """Workflow complete() should not raise before PrepareSkimJobs runs."""
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )

            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config="/fake/cfg.txt",
                dataset_manifest=manifest_path,
                name="myRun",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                self.assertFalse(task.complete())
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_requires_prepare_jobs_even_without_file_source(self):
        """SkimTask always chains PrepareSkimJobs so all executors share one setup path."""
        import analysis_tasks

        task = analysis_tasks.SkimTask(
            exe="/fake/exe",
            submit_config="/fake/cfg.txt",
            dataset_manifest="/fake/manifest.yaml",
            name="myRun",
            make_test_job=False,
        )

        req = task.requires()
        self.assertIn("prep", req)
        self.assertIsInstance(req["prep"], analysis_tasks.PrepareSkimJobs)

    def test_get_dask_work_uses_prepared_job_runner_for_dict_branch(self):
        """Dict-style branch metadata dispatches through _run_prepared_skim_job."""
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            shared_dir = os.path.join(run_dir, "shared_inputs")
            job_dir = os.path.join(run_dir, "jobs", "sample", "job_0")
            Path(shared_dir).mkdir(parents=True)
            Path(job_dir).mkdir(parents=True)

            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config="/fake/cfg.txt",
                dataset_manifest="/fake/manifest.yaml",
                name="myRun",
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(lambda self: run_dir)
            try:
                func, args, kwargs = task.get_dask_work(
                    0,
                    {
                        "dataset_name": "sample",
                        "job_dir": job_dir,
                        "config_path": os.path.join(job_dir, "submit_config.txt"),
                        "out_dir": os.path.join(run_dir, "outputs", "sample"),
                    },
                )
                self.assertIs(func, analysis_tasks._run_prepared_skim_job)
                self.assertEqual(args[0], shared_dir)
                self.assertEqual(args[1], job_dir)
                self.assertEqual(args[5], os.path.join(job_dir, "submit_config.txt"))
                self.assertEqual(args[6], os.path.join(run_dir, "shared_inputs.tar.gz"))
                self.assertEqual(kwargs, {})
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestPrepareSkimJobs(unittest.TestCase):
    """Tests for PrepareSkimJobs helpers."""

    def test_requires_get_nano_file_list_automatically(self):
        import analysis_tasks
        import nano_tasks

        task = analysis_tasks.PrepareSkimJobs(
            exe="/fake/exe",
            submit_config="/fake/cfg.yaml",
            dataset_manifest="/fake/manifest.yaml",
            name="skimDY2Jet",
            file_source="nano",
            file_source_name="",
            x509="/fake/x509",
        )

        req = task.requires()
        self.assertIsInstance(req, nano_tasks.GetNANOFileList)
        self.assertEqual(req.name, "skimDY2Jet")

    def test_effective_file_source_name_defaults_to_run_name(self):
        import analysis_tasks

        task = analysis_tasks.PrepareSkimJobs(
            exe="/fake/exe",
            submit_config="/fake/cfg.yaml",
            dataset_manifest="/fake/manifest.yaml",
            name="skimDY2Jet",
            file_source="nano",
            file_source_name="",
            x509="/fake/x509",
        )

        self.assertEqual(task._effective_file_source_name, "skimDY2Jet")

    def test_create_test_job_copies_resolved_payloads(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            Path(cfg_dir).mkdir(parents=True)

            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            Path(os.path.join(cfg_dir, "dataset_manifest.yaml")).write_text(
                "datasets: []\nlumi: 1.0\n"
            )
            Path(os.path.join(cfg_dir, "floats.yaml")).write_text("normScale: 1.0\n")

            template = _write_submit_config_template(
                cfg_dir,
                extra={
                    "sampleConfig": "cfg/dataset_manifest.yaml",
                    "floatConfig": "cfg/floats.yaml",
                },
            )
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )

            task = analysis_tasks.PrepareSkimJobs(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="myRun",
            )

            orig_run_dir = analysis_tasks.PrepareSkimJobs._run_dir.fget
            analysis_tasks.PrepareSkimJobs._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                first_job_dir = os.path.join(
                    tmpdir, "skimRun_myRun", "jobs", "sample", "job_0"
                )
                Path(first_job_dir).mkdir(parents=True)
                Path(os.path.join(first_job_dir, "submit_config.txt")).write_text(
                    "fileList=root://server/file.root\n"
                )

                task._create_test_job(
                    exe_path,
                    [{
                        "dataset_name": "sample",
                        "job_dir": first_job_dir,
                        "out_dir": os.path.join(tmpdir, "out"),
                    }],
                )

                test_dir = os.path.join(tmpdir, "skimRun_myRun", "test_job")
                self.assertTrue(os.path.exists(os.path.join(test_dir, "dataset_manifest.yaml")))
                self.assertTrue(os.path.exists(os.path.join(test_dir, "floats.yaml")))

                cfg = {}
                with open(os.path.join(test_dir, "submit_config.txt")) as fh:
                    for line in fh:
                        line = line.strip()
                        if "=" in line:
                            k, v = line.split("=", 1)
                            cfg[k] = v

                self.assertEqual(cfg["sampleConfig"], "dataset_manifest.yaml")
                self.assertEqual(cfg["floatConfig"], "floats.yaml")
                self.assertEqual(cfg["fileList"], "root://server/file.root")
            finally:
                analysis_tasks.PrepareSkimJobs._run_dir = property(orig_run_dir)

    def test_create_test_job_copies_aux_directory(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            aux_dir = os.path.join(analysis_dir, "aux")
            Path(cfg_dir).mkdir(parents=True)
            Path(aux_dir).mkdir(parents=True)

            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            Path(os.path.join(aux_dir, "muon_scalesmearing.json.gz")).write_text("payload\n")
            Path(os.path.join(cfg_dir, "corrections.yaml")).write_text(
                "- file: aux/muon_scalesmearing.json.gz\n"
            )

            template = _write_submit_config_template(
                cfg_dir,
                extra={"correctionConfig": "cfg/corrections.yaml"},
            )
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )

            task = analysis_tasks.PrepareSkimJobs(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="myRun",
            )

            orig_run_dir = analysis_tasks.PrepareSkimJobs._run_dir.fget
            analysis_tasks.PrepareSkimJobs._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                first_job_dir = os.path.join(
                    tmpdir, "skimRun_myRun", "jobs", "sample", "job_0"
                )
                Path(first_job_dir).mkdir(parents=True)
                Path(os.path.join(first_job_dir, "submit_config.txt")).write_text(
                    "fileList=root://server/file.root\n"
                )

                task._create_test_job(
                    exe_path,
                    [{
                        "dataset_name": "sample",
                        "job_dir": first_job_dir,
                        "out_dir": os.path.join(tmpdir, "out"),
                    }],
                )

                test_dir = os.path.join(tmpdir, "skimRun_myRun", "test_job")
                self.assertTrue(os.path.exists(os.path.join(test_dir, "aux", "muon_scalesmearing.json.gz")))

                with open(os.path.join(test_dir, "corrections.yaml")) as fh:
                    contents = fh.read()

                self.assertIn("file: aux/muon_scalesmearing.json.gz", contents)
            finally:
                analysis_tasks.PrepareSkimJobs._run_dir = property(orig_run_dir)

    def test_run_impl_stages_aux_in_shared_inputs(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            analysis_dir = os.path.join(tmpdir, "analysis")
            cfg_dir = os.path.join(analysis_dir, "cfg")
            aux_dir = os.path.join(analysis_dir, "aux")
            Path(cfg_dir).mkdir(parents=True)
            Path(aux_dir).mkdir(parents=True)

            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            Path(os.path.join(aux_dir, "jetvetomaps.json.gz")).write_text("payload\n")
            Path(os.path.join(cfg_dir, "corrections.yaml")).write_text(
                "- file: jetvetomaps.json.gz\n"
            )

            template = _write_submit_config_template(
                cfg_dir,
                extra={"correctionConfig": "cfg/corrections.yaml"},
            )
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )

            task = analysis_tasks.PrepareSkimJobs(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="myRun",
                make_test_job=False,
            )

            orig_run_dir = analysis_tasks.PrepareSkimJobs._run_dir.fget
            analysis_tasks.PrepareSkimJobs._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                with patch("analysis_tasks._collect_skim_shared_libs", return_value={}):
                    task._run_impl()

                shared_dir = os.path.join(tmpdir, "skimRun_myRun", "shared_inputs")
                self.assertTrue(os.path.exists(os.path.join(shared_dir, "aux_bundle.tar.gz")))
                self.assertTrue(os.path.exists(os.path.join(shared_dir, "cfg_bundle.tar.gz")))
                self.assertTrue(os.path.exists(os.path.join(tmpdir, "skimRun_myRun", "shared_inputs.tar.gz")))

                job_cfg = os.path.join(
                    tmpdir,
                    "skimRun_myRun",
                    "jobs",
                    "sample",
                    "submit_config.txt",
                )
                cfg = {}
                with open(job_cfg) as fh:
                    for line in fh:
                        line = line.strip()
                        if "=" in line:
                            k, v = line.split("=", 1)
                            cfg[k] = v

                self.assertEqual(cfg["correctionConfig"], "cfg/corrections.yaml")
                self.assertFalse(os.path.exists(os.path.join(shared_dir, "aux")))
                self.assertFalse(os.path.exists(os.path.join(shared_dir, "corrections.yaml")))
            finally:
                analysis_tasks.PrepareSkimJobs._run_dir = property(orig_run_dir)

    def test_materialize_file_source_runtime_job_unpacks_bundles(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            shared_dir = os.path.join(tmpdir, "shared_inputs")
            job_dir = os.path.join(tmpdir, "job_0")
            Path(shared_dir).mkdir()
            Path(job_dir).mkdir()

            exe_path = os.path.join(shared_dir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)
            Path(os.path.join(job_dir, "submit_config.txt")).write_text(
                "correctionConfig=cfg/corrections.yaml\n"
            )
            Path(os.path.join(job_dir, "cfg")).mkdir()
            Path(os.path.join(job_dir, "cfg", "corrections.yaml")).write_text(
                "- file: aux/override.json.gz\n"
            )

            cfg_src = os.path.join(tmpdir, "cfg_src")
            aux_src = os.path.join(tmpdir, "aux_src")
            Path(os.path.join(cfg_src, "cfg")).mkdir(parents=True)
            Path(os.path.join(aux_src, "aux")).mkdir(parents=True)
            Path(os.path.join(cfg_src, "cfg", "corrections.yaml")).write_text(
                "- file: aux/shared.json.gz\n"
            )
            Path(os.path.join(aux_src, "aux", "shared.json.gz")).write_text("shared\n")

            import tarfile
            with tarfile.open(os.path.join(shared_dir, "cfg_bundle.tar.gz"), "w:gz") as archive:
                archive.add(os.path.join(cfg_src, "cfg"), arcname="cfg")
            with tarfile.open(os.path.join(shared_dir, "aux_bundle.tar.gz"), "w:gz") as archive:
                archive.add(os.path.join(aux_src, "aux"), arcname="aux")

            runtime_dir, runtime_exe = analysis_tasks._materialize_file_source_runtime_job(
                shared_dir=shared_dir,
                source_job_dir=job_dir,
                exe_relpath="exe",
            )
            try:
                self.assertTrue(os.path.exists(os.path.join(runtime_dir, "cfg", "corrections.yaml")))
                self.assertTrue(os.path.exists(os.path.join(runtime_dir, "cfg", "submit_config.txt")))
                self.assertTrue(os.path.exists(os.path.join(runtime_dir, "aux", "shared.json.gz")))
                self.assertEqual(runtime_exe, os.path.join(runtime_dir, "exe"))

                with open(os.path.join(runtime_dir, "cfg", "corrections.yaml")) as fh:
                    contents = fh.read()
                self.assertIn("override.json.gz", contents)
            finally:
                shutil.rmtree(runtime_dir, ignore_errors=True)

    def test_htcondor_job_config_is_explicitly_unsupported(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            job_dir = os.path.join(run_dir, "jobs", "sample", "job_0")
            Path(job_dir).mkdir(parents=True)
            Path(os.path.join(job_dir, "submit_config.txt")).write_text("saveFile=out.root\n")

            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config="/fake/cfg.txt",
                dataset_manifest=manifest_path,
                name="myRun",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(lambda self: run_dir)
            try:
                config = MagicMock()
                config.input_files = {}
                config.custom_content = []
                with patch.object(task, "create_branch_map", return_value={0: {"job_dir": job_dir, "config_path": os.path.join(job_dir, "submit_config.txt")}}):
                    with self.assertRaisesRegex(RuntimeError, "branch-level LAW HTCondor execution is disabled"):
                        task.htcondor_job_config(config, job_num=0, branches=[0])
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_htcondor_workflow_delegates_to_monitor_task(self):
        import analysis_tasks
        import law as law_mod

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config="/fake/cfg.txt",
                dataset_manifest=manifest_path,
                name="myRun",
                workflow="htcondor",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(lambda self: run_dir)
            try:
                delegate = MagicMock()
                delegate.complete.return_value = True
                delegate_output = law_mod.LocalFileTarget(os.path.join(tmpdir, "delegated.done"))
                delegate.output.return_value = delegate_output

                with patch.object(analysis_tasks.MonitorSkimJobs, "req", return_value=delegate):
                    self.assertTrue(task.complete())
                    self.assertEqual(task.output().path, delegate_output.path)
                    reqs = task.workflow_proxy.requires()
                    self.assertIs(reqs["delegate"], delegate)
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_htcondor_remote_helper_methods_are_disabled(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config="/fake/cfg.txt",
                dataset_manifest=manifest_path,
                name="myRun",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(lambda self: run_dir)
            try:
                with self.assertRaisesRegex(RuntimeError, "does not use LAW remote HTCondor job files"):
                    task.htcondor_job_file()
                with self.assertRaisesRegex(RuntimeError, "does not use LAW remote HTCondor wrappers"):
                    task.htcondor_wrapper_file()
                with self.assertRaisesRegex(RuntimeError, "does not use LAW remote HTCondor wrappers"):
                    task.htcondor_group_wrapper_file()
                with self.assertRaisesRegex(RuntimeError, "does not use LAW remote HTCondor job factories"):
                    task.htcondor_create_job_file_factory()
                self.assertIsNone(task.htcondor_bootstrap_file())
                self.assertIsNone(task.htcondor_stageout_file())
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_build_skim_submission_writes_concrete_submit_files(self):
        import analysis_tasks
        import tarfile

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            shared_dir = os.path.join(run_dir, "shared_inputs")
            jobs_dir = os.path.join(run_dir, "jobs", "sample")
            Path(shared_dir).mkdir(parents=True)
            Path(jobs_dir).mkdir(parents=True)

            exe_name = "myanalysis"
            Path(os.path.join(shared_dir, exe_name)).write_text("#!/bin/sh\nexit 0\n")
            Path(os.path.join(shared_dir, "libexample.so")).write_text("fake-so")
            Path(os.path.join(shared_dir, "x509")).write_text("proxy")

            cfg_src = os.path.join(tmpdir, "cfg")
            aux_src = os.path.join(tmpdir, "aux")
            Path(cfg_src).mkdir()
            Path(aux_src).mkdir()
            Path(os.path.join(cfg_src, "floats.yaml")).write_text("x: 1\n")
            Path(os.path.join(aux_src, "weights.json")).write_text("{}\n")
            with tarfile.open(os.path.join(shared_dir, "cfg_bundle.tar.gz"), "w:gz") as tar:
                tar.add(cfg_src, arcname="cfg")
            with tarfile.open(os.path.join(shared_dir, "aux_bundle.tar.gz"), "w:gz") as tar:
                tar.add(aux_src, arcname="aux")
            analysis_tasks._build_shared_inputs_archive(
                shared_dir,
                os.path.join(run_dir, "shared_inputs.tar.gz"),
            )

            job0 = os.path.join(jobs_dir, "job_0")
            job1 = os.path.join(jobs_dir, "job_1")
            Path(job0).mkdir()
            Path(job1).mkdir()
            Path(os.path.join(job0, "submit_config.txt")).write_text("saveFile=out0.root\nmetaFile=meta0.root\n")
            Path(os.path.join(job1, "submit_config.txt")).write_text("saveFile=out1.root\nmetaFile=meta1.root\n")

            with open(os.path.join(run_dir, "prep_submission.json"), "w") as fh:
                json.dump(
                    [
                        {"dataset_name": "sample", "job_dir": job0, "config_path": os.path.join(job0, "submit_config.txt"), "out_dir": os.path.join(run_dir, "outputs", "sample", "job_0")},
                        {"dataset_name": "sample", "job_dir": job1, "config_path": os.path.join(job1, "submit_config.txt"), "out_dir": os.path.join(run_dir, "outputs", "sample", "job_1")},
                    ],
                    fh,
                )

            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )
            submit_config = _write_submit_config_template(tmpdir, extra={"saveDirectory": "/tmp/out"})
            task = analysis_tasks.BuildSkimSubmission(
                exe=os.path.join(tmpdir, exe_name),
                submit_config=submit_config,
                dataset_manifest=manifest_path,
                name="myRun",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.BuildSkimSubmission._run_dir.fget
            analysis_tasks.BuildSkimSubmission._run_dir = property(lambda self: run_dir)
            try:
                task._run_impl()
            finally:
                analysis_tasks.BuildSkimSubmission._run_dir = property(orig_run_dir)

            manifest_path = os.path.join(run_dir, "submission_manifest.json")
            self.assertTrue(os.path.isfile(manifest_path))

            manifest_payload = json.loads(Path(manifest_path).read_text())
            self.assertEqual(len(manifest_payload["datasets"]), 1)
            dataset_payload = manifest_payload["datasets"][0]
            submit_dir = dataset_payload["submission_dir"]
            submit_path = dataset_payload["submit_path"]
            runscript_path = dataset_payload["runscript_path"]

            self.assertTrue(os.path.isfile(submit_path))
            self.assertTrue(os.path.isfile(runscript_path))
            self.assertTrue(os.path.isdir(os.path.join(submit_dir, "job_0")))
            self.assertTrue(os.path.isdir(os.path.join(submit_dir, "job_1")))

            job0_cfg = Path(os.path.join(submit_dir, "job_0", "submit_config.txt")).read_text()
            self.assertIn("saveDirectory=/tmp/out/sample", job0_cfg)
            self.assertIn("saveFile=/tmp/out/sample/output_0.root", job0_cfg)
            self.assertIn("metaFile=/tmp/out/sample/meta_0.root", job0_cfg)

            submit_text = Path(submit_path).read_text()
            self.assertIn("job_$(Process)/submit_config.txt", submit_text)
            self.assertIn(os.path.join(run_dir, "shared_inputs.tar.gz"), submit_text)
            self.assertNotIn("job_$(Process)/myanalysis", submit_text)
            self.assertNotIn(f"transfer_input_files = {os.path.join(submit_dir, 'shared_inputs')}\n", submit_text)
            self.assertNotIn("job_$(Process)/floats.txt", submit_text)
            self.assertNotIn("job_$(Process)/ints.txt", submit_text)
            self.assertNotIn("job_$(Process)/x509", submit_text)
            self.assertIn(os.path.join(submit_dir, "condor_logs", "log_$(Cluster)_$(Process).stdout"), submit_text)
            self.assertIn(os.path.join(submit_dir, "condor_logs", "log_$(Cluster)_$(Process).stderr"), submit_text)
            self.assertIn(os.path.join(submit_dir, "condor_logs", "log_$(Cluster).log"), submit_text)

            runscript_text = Path(runscript_path).read_text()
            self.assertIn('tar -xzf "shared_inputs.tar.gz"', runscript_text)
            self.assertIn('cp -f submit_config.txt cfg/submit_config.txt', runscript_text)
            self.assertIn('./myanalysis cfg/submit_config.txt', runscript_text)
            self.assertIn('chmod +x "myanalysis"', runscript_text)
            self.assertIn('__orig_saveFile', runscript_text)
            self.assertIn('__orig_metaFile', runscript_text)
            self.assertIn('xrdcp_if_exists(save_file, orig_save)', runscript_text)
            self.assertIn('xrdcp_if_exists(meta_file, orig_meta)', runscript_text)

    def test_plan_file_source_jobs_fails_on_empty_file_payloads(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "das": "/Sample/NANO", "dtype": "mc"}],
            )
            file_list_dir = os.path.join(tmpdir, "file_lists")
            Path(file_list_dir).mkdir()
            Path(os.path.join(file_list_dir, "sample.json")).write_text(
                json.dumps({"sample": "sample", "files": []})
            )

            with self.assertRaisesRegex(RuntimeError, "empty payloads"):
                analysis_tasks._plan_file_source_jobs(
                    dataset_manifest_path=manifest_path,
                    file_list_dir=file_list_dir,
                    partition="files",
                    files_per_job=1,
                    entries_per_job=0,
                    jobs_dir=os.path.join(tmpdir, "jobs"),
                    outputs_dir=os.path.join(tmpdir, "outputs"),
                )

    def test_monitor_skim_jobs_discovers_save_and_meta_outputs(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = os.path.join(tmpdir, "skimRun_myRun")
            source_job = os.path.join(run_dir, "condor_submissions", "sample", "job_0")
            Path(source_job).mkdir(parents=True)

            save_path = os.path.join(run_dir, "outputs", "sample", "output_0.root")
            meta_path = os.path.join(run_dir, "outputs", "sample", "meta_0.root")
            Path(os.path.dirname(save_path)).mkdir(parents=True)
            Path(os.path.join(source_job, "submit_config.txt")).write_text(
                f"saveFile={save_path}\nmetaFile={meta_path}\n"
            )

            manifest_path = _write_manifest(
                tmpdir,
                [{"name": "sample", "files": ["root://server/file.root"], "dtype": "mc"}],
            )
            submit_config = _write_submit_config_template(tmpdir)
            task = analysis_tasks.MonitorSkimJobs(
                exe="/fake/exe",
                submit_config=submit_config,
                dataset_manifest=manifest_path,
                name="myRun",
                file_source="nano",
            )

            orig_run_dir = analysis_tasks.MonitorSkimJobs._run_dir.fget
            analysis_tasks.MonitorSkimJobs._run_dir = property(lambda self: run_dir)
            try:
                discovered = task._discover_jobs(os.path.join(run_dir, "condor_submissions", "sample"))
            finally:
                analysis_tasks.MonitorSkimJobs._run_dir = property(orig_run_dir)

            self.assertEqual(discovered[0]["dir"], source_job)
            self.assertEqual(discovered[0]["save"], save_path)
            self.assertEqual(discovered[0]["meta"], meta_path)

    def test_submit_single_skim_job_preserves_container_image(self):
        import analysis_tasks

        with tempfile.TemporaryDirectory() as tmpdir:
            submit_dir = os.path.join(tmpdir, "condor_submissions", "sample")
            job_dir = os.path.join(submit_dir, "job_7")
            shared_inputs = os.path.join(tmpdir, "shared_inputs.tar.gz")
            Path(job_dir).mkdir(parents=True)
            Path(os.path.join(job_dir, "submit_config.txt")).write_text("saveFile=out.root\n")
            Path(shared_inputs).write_text("archive")

            Path(os.path.join(submit_dir, "condor_submit.sub")).write_text(
                "\n".join(
                    [
                        "universe = vanilla",
                        f"Executable = {submit_dir}/condor_runscript.sh",
                        "transfer_input_files = "
                        f"{submit_dir}/job_$(Process)/submit_config.txt,"
                        f"{submit_dir}/job_$(Process)/site_redirectors.json,"
                        f"{shared_inputs},"
                        f"{submit_dir}/condor_runscript_inner.sh",
                        'environment = "CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)"',
                        'MY.SingularityImage = "/cvmfs/unpacked.cern.ch/example/image:latest"',
                        'MY.WantOS = "el9"',
                        "+RequestMemory=2000",
                        "+MaxRuntime=3600",
                        f"Output = {submit_dir}/condor_logs/log_$(Cluster)_$(Process).stdout",
                        f"Error = {submit_dir}/condor_logs/log_$(Cluster)_$(Process).stderr",
                        f"Log = {submit_dir}/condor_logs/log_$(Cluster).log",
                        "queue 10",
                        "",
                    ]
                )
            )

            completed = analysis_tasks.subprocess.CompletedProcess(
                args=["condor_submit", "dummy"],
                returncode=0,
                stdout="Submitting job(s).\n1 job(s) submitted to cluster 12345.\n",
                stderr="",
            )
            with patch("analysis_tasks.subprocess.run", return_value=completed):
                cluster_id = analysis_tasks._submit_single_skim_job(
                    job_index=7,
                    submission_dir=submit_dir,
                    max_runtime=5400,
                    request_memory=3000,
                    extra_transfer_files=[shared_inputs],
                )

            self.assertEqual(cluster_id, "12345")

            resub_text = Path(
                os.path.join(submit_dir, "resubmissions", "resub_job7.sub")
            ).read_text()
            self.assertIn('MY.SingularityImage = "/cvmfs/unpacked.cern.ch/example/image:latest"', resub_text)
            self.assertIn('MY.WantOS = "el9"', resub_text)
            self.assertIn(f"{submit_dir}/job_7/submit_config.txt", resub_text)
            self.assertIn(f"{submit_dir}/job_7/site_redirectors.json", resub_text)
            self.assertIn('environment = "CONDOR_PROC=7 CONDOR_CLUSTER=$(Cluster)"', resub_text)
            self.assertIn("+RequestMemory=3000", resub_text)
            self.assertIn("+MaxRuntime=5400", resub_text)
            self.assertIn(f"Output = {submit_dir}/condor_logs/resub_7_$(Cluster)_$(Process).stdout", resub_text)
            self.assertIn(f"Log = {submit_dir}/condor_logs/resub_7.log", resub_text)
            self.assertRegex(resub_text, r"(?m)^queue 1$")

    def test_output_files_exist_batch_uses_xrootd_for_eos_like_paths(self):
        import analysis_tasks

        eos_path = "/store/user/test/sample/output_0.root"
        local_path = "/tmp/nonexistent-local-output.root"

        with patch("analysis_tasks._eos_files_exist_batch_util", return_value={eos_path: True}) as eos_batch:
            result = analysis_tasks._output_files_exist_batch([eos_path, local_path])

        eos_batch.assert_called_once_with([eos_path])
        self.assertTrue(result[eos_path])
        self.assertFalse(result[local_path])


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
# Cache producer/consumer helpers
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestBuildTaskProvenance(unittest.TestCase):
    """Tests for the _build_task_provenance helper."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    def test_returns_provenance_record(self):
        """_build_task_provenance always returns a ProvenanceRecord."""
        mod = self._import()
        from output_schema import ProvenanceRecord
        prov = mod._build_task_provenance()
        self.assertIsInstance(prov, ProvenanceRecord)

    def test_config_mtime_set_for_existing_file(self):
        """config_mtime is populated when submit_config_path points to a real file."""
        mod = self._import()
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("key=val\n")
            path = f.name
        try:
            prov = mod._build_task_provenance(submit_config_path=path)
            self.assertIsNotNone(prov.config_mtime)
            # Should be a valid ISO 8601 timestamp
            import datetime
            datetime.datetime.fromisoformat(prov.config_mtime)
        finally:
            os.unlink(path)

    def test_config_mtime_none_for_missing_file(self):
        """config_mtime is None when submit_config_path does not exist."""
        mod = self._import()
        prov = mod._build_task_provenance(submit_config_path="/nonexistent/cfg.txt")
        self.assertIsNone(prov.config_mtime)

    def test_manifest_hash_set_for_existing_manifest(self):
        """dataset_manifest_hash is populated when manifest file exists."""
        mod = self._import()
        import yaml
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            yaml.dump({"datasets": [], "lumi": 1.0}, f)
            path = f.name
        try:
            prov = mod._build_task_provenance(dataset_manifest_path=path)
            self.assertIsNotNone(prov.dataset_manifest_hash)
            self.assertIsInstance(prov.dataset_manifest_hash, str)
            self.assertGreater(len(prov.dataset_manifest_hash), 0)
        finally:
            os.unlink(path)

    def test_manifest_hash_none_for_missing_manifest(self):
        """dataset_manifest_hash is None when manifest path does not exist."""
        mod = self._import()
        prov = mod._build_task_provenance(
            dataset_manifest_path="/nonexistent/manifest.yaml"
        )
        self.assertIsNone(prov.dataset_manifest_hash)

    def test_no_args_returns_partial_provenance(self):
        """Called with no arguments returns a ProvenanceRecord (fields may be None)."""
        mod = self._import()
        from output_schema import ProvenanceRecord
        prov = mod._build_task_provenance()
        self.assertIsInstance(prov, ProvenanceRecord)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestBuildSkimManifest(unittest.TestCase):
    """Tests for the _build_skim_manifest helper."""

    def _import(self):
        import analysis_tasks
        return analysis_tasks

    def test_returns_output_manifest(self):
        """_build_skim_manifest returns an OutputManifest."""
        mod = self._import()
        from dataset_manifest import DatasetEntry
        from output_schema import OutputManifest
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")
            with open(manifest_path, "w") as fh:
                yaml.dump({"datasets": [{"name": "ttbar", "files": ["f.root"]}], "lumi": 1.0}, fh)
            config_path = os.path.join(tmpdir, "cfg.txt")
            Path(config_path).write_text("key=val\n")
            skim_file = os.path.join(tmpdir, "skim.root")

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            manifest = mod._build_skim_manifest(
                dataset=entry,
                submit_config_path=config_path,
                dataset_manifest_path=manifest_path,
                skim_output_file=skim_file,
            )

        self.assertIsInstance(manifest, OutputManifest)

    def test_contains_intermediate_artifact(self):
        """Manifest includes one IntermediateArtifactSchema of kind 'preselection'."""
        mod = self._import()
        from dataset_manifest import DatasetEntry
        from output_schema import IntermediateArtifactSchema
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")
            with open(manifest_path, "w") as fh:
                yaml.dump({"datasets": [{"name": "ttbar", "files": ["f.root"]}], "lumi": 1.0}, fh)
            config_path = os.path.join(tmpdir, "cfg.txt")
            Path(config_path).write_text("key=val\n")
            skim_file = os.path.join(tmpdir, "skim.root")

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            manifest = mod._build_skim_manifest(
                dataset=entry,
                submit_config_path=config_path,
                dataset_manifest_path=manifest_path,
                skim_output_file=skim_file,
            )

        self.assertEqual(len(manifest.intermediate_artifacts), 1)
        artifact = manifest.intermediate_artifacts[0]
        self.assertIsInstance(artifact, IntermediateArtifactSchema)
        self.assertEqual(artifact.artifact_kind, "preselection")
        self.assertEqual(artifact.output_file, skim_file)

    def test_contains_dataset_manifest_provenance(self):
        """Manifest records DatasetManifestProvenance with the dataset entry name."""
        mod = self._import()
        from dataset_manifest import DatasetEntry
        from output_schema import DatasetManifestProvenance
        import yaml

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest_path = os.path.join(tmpdir, "manifest.yaml")
            with open(manifest_path, "w") as fh:
                yaml.dump({"datasets": [{"name": "ttbar", "files": ["f.root"]}], "lumi": 1.0}, fh)
            config_path = os.path.join(tmpdir, "cfg.txt")
            Path(config_path).write_text("key=val\n")
            skim_file = os.path.join(tmpdir, "skim.root")

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            manifest = mod._build_skim_manifest(
                dataset=entry,
                submit_config_path=config_path,
                dataset_manifest_path=manifest_path,
                skim_output_file=skim_file,
            )

        dmp = manifest.dataset_manifest_provenance
        self.assertIsNotNone(dmp)
        self.assertIsInstance(dmp, DatasetManifestProvenance)
        self.assertIn("ttbar", dmp.resolved_entry_names)
        self.assertIsNotNone(dmp.manifest_hash)


# ===========================================================================
# SkimTask – cache producer integration
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestSkimTaskCacheProducer(unittest.TestCase):
    """Integration tests for SkimTask's cache producer behaviour."""

    def test_run_writes_cache_sidecar(self):
        """SkimTask.run() writes a .cache.yaml sidecar alongside skim.root."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import CACHE_SIDECAR_SUFFIX
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir, [{"name": "ttbar", "files": ["f.root"]}]
            )

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            task = analysis_tasks.SkimTask(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                branch=0,
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                # Create a fake skim.root so the sidecar write path triggers
                out_dir = os.path.join(tmpdir, "skimRun_testSkim", "outputs", "ttbar")
                Path(out_dir).mkdir(parents=True)
                skim_file = os.path.join(out_dir, "skim.root")
                Path(skim_file).write_text("fake ROOT")

                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        task.run()

                sidecar_path = skim_file + CACHE_SIDECAR_SUFFIX
                self.assertTrue(
                    os.path.exists(sidecar_path),
                    f"Cache sidecar not found at {sidecar_path}",
                )
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_run_sidecar_is_valid_cache_artifact(self):
        """The written sidecar can be read back as a CachedArtifact."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import CACHE_SIDECAR_SUFFIX, read_cache_sidecar, CachedArtifact
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            exe_path = os.path.join(tmpdir, "exe")
            Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
            os.chmod(exe_path, 0o755)

            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir, [{"name": "ttbar", "files": ["f.root"]}]
            )

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            task = analysis_tasks.SkimTask(
                exe=exe_path,
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                branch=0,
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                out_dir = os.path.join(tmpdir, "skimRun_testSkim", "outputs", "ttbar")
                Path(out_dir).mkdir(parents=True)
                skim_file = os.path.join(out_dir, "skim.root")
                Path(skim_file).write_text("fake ROOT")

                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        task.run()

                cached = read_cache_sidecar(skim_file)
                self.assertIsInstance(cached, CachedArtifact)
                self.assertEqual(cached.artifact_path, skim_file)
                self.assertEqual(
                    len(cached.manifest.intermediate_artifacts), 1
                )
                self.assertEqual(
                    cached.manifest.intermediate_artifacts[0].artifact_kind,
                    "preselection",
                )
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_complete_returns_true_for_compatible_cache(self):
        """complete() returns True when .done file exists and cache is COMPATIBLE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import (
            write_cache_sidecar,
            ArtifactResolutionStatus,
        )
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir, [{"name": "ttbar", "files": ["f.root"]}]
            )

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                branch=0,
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                # Write the .done marker
                done_dir = os.path.join(
                    tmpdir, "skimRun_testSkim", "job_outputs"
                )
                Path(done_dir).mkdir(parents=True)
                done_file = os.path.join(done_dir, "ttbar.done")
                Path(done_file).write_text("status=done\n")

                # Write the skim artifact and its sidecar
                out_dir = os.path.join(
                    tmpdir, "skimRun_testSkim", "outputs", "ttbar"
                )
                Path(out_dir).mkdir(parents=True)
                skim_file = os.path.join(out_dir, "skim.root")
                Path(skim_file).write_text("fake ROOT")

                write_cache_sidecar(skim_file, _make_preselection_manifest(skim_file))

                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch(
                        "analysis_tasks.check_cache_validity",
                        return_value=ArtifactResolutionStatus.COMPATIBLE,
                    ):
                        result = task.complete()

                self.assertTrue(result)
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_complete_invalidates_stale_cache(self):
        """complete() removes .done and returns False when cache is STALE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import ArtifactResolutionStatus
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir, [{"name": "ttbar", "files": ["f.root"]}]
            )

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                branch=0,
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                done_dir = os.path.join(
                    tmpdir, "skimRun_testSkim", "job_outputs"
                )
                Path(done_dir).mkdir(parents=True)
                done_file = os.path.join(done_dir, "ttbar.done")
                Path(done_file).write_text("status=done\n")

                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch(
                        "analysis_tasks.check_cache_validity",
                        return_value=ArtifactResolutionStatus.STALE,
                    ):
                        with patch.object(task, "publish_message"):
                            result = task.complete()

                self.assertFalse(result)
                self.assertFalse(
                    os.path.exists(done_file),
                    "The .done marker should be removed for stale caches.",
                )
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)

    def test_complete_invalidates_must_regenerate_cache(self):
        """complete() removes .done and returns False when MUST_REGENERATE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import ArtifactResolutionStatus
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            template = _write_submit_config_template(tmpdir)
            manifest_path = _write_manifest(
                tmpdir, [{"name": "ttbar", "files": ["f.root"]}]
            )

            entry = DatasetEntry(name="ttbar", files=["f.root"])
            task = analysis_tasks.SkimTask(
                exe="/fake/exe",
                submit_config=template,
                dataset_manifest=manifest_path,
                name="testSkim",
                branch=0,
            )

            orig_run_dir = analysis_tasks.SkimTask._run_dir.fget
            analysis_tasks.SkimTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.name}")
            )
            try:
                done_dir = os.path.join(
                    tmpdir, "skimRun_testSkim", "job_outputs"
                )
                Path(done_dir).mkdir(parents=True)
                done_file = os.path.join(done_dir, "ttbar.done")
                Path(done_file).write_text("status=done\n")

                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch(
                        "analysis_tasks.check_cache_validity",
                        return_value=ArtifactResolutionStatus.MUST_REGENERATE,
                    ):
                        with patch.object(task, "publish_message"):
                            result = task.complete()

                self.assertFalse(result)
                self.assertFalse(os.path.exists(done_file))
            finally:
                analysis_tasks.SkimTask._run_dir = property(orig_run_dir)


# ===========================================================================
# HistFillTask – cache consumer integration
# ===========================================================================

@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_LAW)
class TestHistFillTaskCacheConsumer(unittest.TestCase):
    """Integration tests for HistFillTask's cache consumer behaviour."""

    def _make_task_in_tmpdir(self, tmpdir, skim_name="mySkimRun", **extra):
        import analysis_tasks
        exe_path = os.path.join(tmpdir, "exe")
        Path(exe_path).write_text("#!/bin/sh\nexit 0\n")
        os.chmod(exe_path, 0o755)

        template = _write_submit_config_template(tmpdir)
        manifest_path = _write_manifest(
            tmpdir, [{"name": "wjets", "files": ["f.root"]}]
        )

        task = analysis_tasks.HistFillTask(
            exe=exe_path,
            submit_config=template,
            dataset_manifest=manifest_path,
            name="testHist",
            branch=0,
            skim_name=skim_name,
            **extra,
        )
        return task, manifest_path

    def test_run_reports_compatible_cache(self):
        """run() reports 'cache compatible' when the skim sidecar is COMPATIBLE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import (
            ArtifactResolutionStatus,
            OutputManifest, IntermediateArtifactSchema,
            write_cache_sidecar,
        )
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            task, manifest_path = self._make_task_in_tmpdir(tmpdir)

            skim_dir = os.path.join(tmpdir, "skimRun_mySkimRun", "outputs", "wjets")
            Path(skim_dir).mkdir(parents=True)
            skim_file = os.path.join(skim_dir, "skim.root")
            Path(skim_file).write_text("fake ROOT")

            # Write a sidecar so cache check can succeed
            from output_schema import write_cache_sidecar
            write_cache_sidecar(skim_file, _make_preselection_manifest(skim_file))

            orig_run = analysis_tasks.HistFillTask._run_dir.fget
            orig_skim = analysis_tasks.HistFillTask._skim_run_dir.fget
            analysis_tasks.HistFillTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"histRun_{self.name}")
            )
            analysis_tasks.HistFillTask._skim_run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.skim_name}")
            )

            messages = []

            entry = DatasetEntry(name="wjets", files=["f.root"])
            try:
                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        with patch(
                            "analysis_tasks.check_cache_validity",
                            return_value=ArtifactResolutionStatus.COMPATIBLE,
                        ):
                            with patch.object(
                                task, "publish_message",
                                side_effect=messages.append,
                            ):
                                task.run()
            finally:
                analysis_tasks.HistFillTask._run_dir = property(orig_run)
                analysis_tasks.HistFillTask._skim_run_dir = property(orig_skim)

            compatible_msgs = [m for m in messages if "compatible" in m.lower()]
            self.assertTrue(
                len(compatible_msgs) >= 1,
                f"Expected a 'compatible' message; got: {messages}",
            )

    def test_run_warns_on_stale_cache(self):
        """run() emits a WARNING message when the skim sidecar is STALE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import ArtifactResolutionStatus
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            task, _ = self._make_task_in_tmpdir(tmpdir)

            skim_dir = os.path.join(tmpdir, "skimRun_mySkimRun", "outputs", "wjets")
            Path(skim_dir).mkdir(parents=True)
            skim_file = os.path.join(skim_dir, "skim.root")
            Path(skim_file).write_text("fake ROOT")

            orig_run = analysis_tasks.HistFillTask._run_dir.fget
            orig_skim = analysis_tasks.HistFillTask._skim_run_dir.fget
            analysis_tasks.HistFillTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"histRun_{self.name}")
            )
            analysis_tasks.HistFillTask._skim_run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.skim_name}")
            )

            messages = []
            entry = DatasetEntry(name="wjets", files=["f.root"])
            try:
                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        with patch(
                            "analysis_tasks.check_cache_validity",
                            return_value=ArtifactResolutionStatus.STALE,
                        ):
                            with patch.object(
                                task, "publish_message",
                                side_effect=messages.append,
                            ):
                                task.run()
            finally:
                analysis_tasks.HistFillTask._run_dir = property(orig_run)
                analysis_tasks.HistFillTask._skim_run_dir = property(orig_skim)

            warning_msgs = [m for m in messages if "stale" in m.lower() or "warning" in m.lower()]
            self.assertTrue(
                len(warning_msgs) >= 1,
                f"Expected a stale/warning message; got: {messages}",
            )

    def test_run_warns_on_must_regenerate_cache(self):
        """run() emits a warning (not an error) when cache status is MUST_REGENERATE."""
        import analysis_tasks
        from dataset_manifest import DatasetEntry
        from output_schema import ArtifactResolutionStatus
        from unittest.mock import PropertyMock

        with tempfile.TemporaryDirectory() as tmpdir:
            task, _ = self._make_task_in_tmpdir(tmpdir)

            skim_dir = os.path.join(tmpdir, "skimRun_mySkimRun", "outputs", "wjets")
            Path(skim_dir).mkdir(parents=True)
            skim_file = os.path.join(skim_dir, "skim.root")
            Path(skim_file).write_text("fake ROOT")

            orig_run = analysis_tasks.HistFillTask._run_dir.fget
            orig_skim = analysis_tasks.HistFillTask._skim_run_dir.fget
            analysis_tasks.HistFillTask._run_dir = property(
                lambda self: os.path.join(tmpdir, f"histRun_{self.name}")
            )
            analysis_tasks.HistFillTask._skim_run_dir = property(
                lambda self: os.path.join(tmpdir, f"skimRun_{self.skim_name}")
            )

            messages = []
            entry = DatasetEntry(name="wjets", files=["f.root"])
            try:
                with patch.object(
                    type(task), "branch_data",
                    new_callable=PropertyMock, return_value=entry
                ):
                    with patch("analysis_tasks._run_analysis_job",
                               return_value="done:job"):
                        with patch(
                            "analysis_tasks.check_cache_validity",
                            return_value=ArtifactResolutionStatus.MUST_REGENERATE,
                        ):
                            with patch.object(
                                task, "publish_message",
                                side_effect=messages.append,
                            ):
                                # Should NOT raise; only warn
                                task.run()
            finally:
                analysis_tasks.HistFillTask._run_dir = property(orig_run)
                analysis_tasks.HistFillTask._skim_run_dir = property(orig_skim)

            warning_msgs = [
                m for m in messages
                if "warning" in m.lower() or "unverifiable" in m.lower()
            ]
            self.assertTrue(
                len(warning_msgs) >= 1,
                f"Expected a warning message; got: {messages}",
            )


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
            "_build_task_provenance",
            "_build_skim_manifest",
        ):
            self.assertTrue(hasattr(analysis_tasks, name), f"{name} not defined")

    def test_task_namespaces(self):
        import analysis_tasks
        for cls_name in ("SkimTask", "HistFillTask", "StitchingDerivationTask"):
            cls = getattr(analysis_tasks, cls_name)
            self.assertEqual(cls.task_namespace, "")


if __name__ == "__main__":
    unittest.main(verbosity=2)
