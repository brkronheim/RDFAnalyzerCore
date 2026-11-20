"""LAW dispatch must use the current binary even when submission files are cached."""
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "core/python"), str(ROOT / "core/python/law")]
import analysis_tasks as mod


class ExecutableSubmissionTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        for mock in (patch.object(mod, "WORKSPACE", temporary.name),
                     patch.object(mod, "_collect_skim_shared_libs", return_value={})):
            mock.start()
            self.addCleanup(mock.stop)
        self.executable = self.root / "analysis"
        self.executable.write_text("#!/bin/sh\nexit 0\n")
        self.executable.chmod(0o755)
        config = self.root / "submit_config.txt"
        config.write_text("enableSkim=false\n")
        manifest = self.root / "datasets.yaml"
        manifest.write_text("datasets:\n- {name: sample, dtype: data, files: [input.root]}\n")
        self.args = dict(name="executable_check", exe=str(self.executable),
                         submit_config=str(config), dataset_manifest=str(manifest),
                         make_test_job=False)
        self.prep = mod.PrepareSkimJobs(**self.args)
        self.prep._run_impl()
        self.shared = Path(self.prep._shared_dir)
        self.archive = Path(self.prep._shared_archive_path)

    def _change_binary_preserving_stat(self):
        before = self.executable.stat()
        self.executable.write_text("#!/bin/sh\nexit 1\n")
        os.utime(self.executable, ns=(before.st_atime_ns, before.st_mtime_ns))
        self.assertEqual(self.executable.stat().st_size, before.st_size)
        self.assertEqual(self.executable.stat().st_mtime_ns, before.st_mtime_ns)

    def test_matching_archive_and_legacy_loose_executable_pass(self):
        mod._validate_staged_skim_executable(str(self.shared), str(self.executable))
        self.archive.unlink()
        mod._validate_staged_skim_executable(str(self.shared), str(self.executable))
        self._change_binary_preserving_stat()
        with self.assertRaisesRegex(RuntimeError, "SHA-256 differs"):
            mod._validate_staged_skim_executable(str(self.shared), str(self.executable))

    def test_archive_mismatch_is_detected_with_same_size_and_timestamps(self):
        self._change_binary_preserving_stat()
        # Both the loose staged binary and archive timestamp look current.
        shutil.copy2(self.executable, self.shared / "analysis")
        self.archive.touch()
        with self.assertRaisesRegex(RuntimeError, "shared_inputs.tar.gz:analysis.*SHA-256 differs"):
            mod._validate_staged_skim_executable(str(self.shared), str(self.executable))
        self.assertFalse(self.prep.complete())
        self.prep._run_impl()
        self.assertTrue(self.prep.complete())

    def test_missing_source_cannot_fall_back_to_staged_binary(self):
        self.executable.unlink()
        with self.assertRaisesRegex(RuntimeError, "Current analysis executable not found"):
            mod._validate_staged_skim_executable(str(self.shared), str(self.executable))

    def test_missing_archived_binary_cannot_fall_back_to_loose_copy(self):
        (self.shared / "analysis").unlink()
        mod._build_shared_inputs_archive(str(self.shared), str(self.archive))
        shutil.copy2(self.executable, self.shared / "analysis")
        with self.assertRaisesRegex(RuntimeError, "missing or is not a regular file"):
            mod._validate_staged_skim_executable(str(self.shared), str(self.executable))

    def test_unreadable_archive_blocks_validation(self):
        self.archive.write_text("invalid archive")
        with self.assertRaisesRegex(RuntimeError, "Cannot verify staged executable"):
            mod._validate_staged_skim_executable(str(self.shared), str(self.executable))

    def test_build_rejects_stale_binary_before_writing_submission(self):
        self._change_binary_preserving_stat()
        before = self.archive.read_bytes()
        task = mod.BuildSkimSubmission(**self.args)
        with self.assertRaisesRegex(RuntimeError, "SHA-256 differs"):
            task._run_impl()
        self.assertEqual(self.archive.read_bytes(), before)
        self.assertFalse(Path(task.output()["manifest"].path).exists())

    def test_cached_submission_rechecks_binary_before_condor(self):
        build = mod.BuildSkimSubmission(**self.args)
        build._run_impl()
        task = mod.SubmitSkimJobs(**self.args)
        with patch.object(task, "input", return_value={"build": build.output()}), \
                patch.object(mod.subprocess, "run") as submit:
            self._change_binary_preserving_stat()
            with self.assertRaisesRegex(RuntimeError, "SHA-256 differs"):
                task.run()
            submit.assert_not_called()
            self.assertFalse(Path(task.output().path).exists())

            self.prep._run_impl()
            submit.return_value = mod.subprocess.CompletedProcess([], 0,
                stdout="1 job(s) submitted to cluster 12345.\n")
            task.run()
            submit.assert_called_once()
            self.assertEqual(submit.call_args.args[0][0], "condor_submit")
            self.assertEqual(json.loads(Path(task.output().path).read_text())
                             ["datasets"][0]["cluster_id"], "12345")

    def test_retry_rejects_stale_binary_without_changing_state(self):
        task = mod.MonitorSkimJobs(**self.args)
        self._change_binary_preserving_stat()
        state = {"status": "submitted", "retries": 0, "cluster": "123", "proc": 0}
        with patch.object(mod, "_submit_single_skim_job") as retry:
            with self.assertRaisesRegex(RuntimeError, "SHA-256 differs"):
                task._try_resubmit("sample", 0, {}, state, "failed")
            retry.assert_not_called()
        self.assertEqual(state, {"status": "submitted", "retries": 0, "cluster": "123", "proc": 0})
