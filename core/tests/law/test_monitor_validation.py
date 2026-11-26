"""Storage batches and restart-safe skim validation, without a live scheduler."""
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "core/python/law"), str(ROOT / "core/python")]

import production_jobs

from ingestion_utils import eos_file_info_batch, eos_files_exist_batch, eos_file_exists


class TestStorageBatches(unittest.TestCase):
    def test_one_long_listing_per_server_and_directory(self):
        paths = ["root://one//outputs/a.root", "root://one//outputs/b.root",
                 "root://two//outputs/a.root"]
        def listing(args, **kwargs):
            self.assertEqual(args[2:], ["ls", "-l", "/outputs"])
            return SimpleNamespace(returncode=0, stdout=(
                "-rw-r--r-- user group 12 2026-11-09 08:46:55 /outputs/a.root\n"
                "-rw-r--r-- 2026-11-09 08:46:56 34 /outputs/b.root\n"
                "drwxr-xr-x user group 0 2026-11-09 08:46:56 /outputs/subdir\n"))
        with patch("ingestion_utils.subprocess.run", side_effect=listing) as run:
            info = eos_file_info_batch(paths + paths)
        self.assertEqual(run.call_count, 2)
        self.assertEqual(info[paths[0]]["size"], 12)
        self.assertEqual(info[paths[1]]["size"], 34)
        self.assertEqual(info[paths[2]]["mtime"], "2026-11-09 08:46:55")

    def test_missing_paths_are_not_confirmed_by_another_entry(self):
        result = SimpleNamespace(returncode=0, stdout=(
            "-rw-r--r-- user group 12 2026-11-09 08:46:55 /outputs/a.root\n"))
        with patch("ingestion_utils.subprocess.run", return_value=result):
            info = eos_files_exist_batch(["/outputs/a.root", "/outputs/b.root"])
        self.assertEqual(info, {"/outputs/a.root": True, "/outputs/b.root": False})

    def test_unknown_format_falls_back_to_one_plain_listing_without_signature(self):
        def listing(args, **kwargs):
            return SimpleNamespace(returncode=0, stdout=(
                "unsupported metadata /outputs/a.root\n" if "-l" in args
                else "/outputs/a.root\n"))
        with patch("ingestion_utils.subprocess.run", side_effect=listing) as run:
            info = eos_file_info_batch(["/outputs/a.root", "/outputs/missing.root"])
        self.assertEqual(run.call_count, 2)
        self.assertEqual(info, {"/outputs/a.root": {}, "/outputs/missing.root": None})

    def test_failed_listing_stat_fallback_preserves_authority_and_is_uncacheable(self):
        with patch("ingestion_utils.subprocess.run", side_effect=OSError("listing failed")), \
             patch("ingestion_utils.eos_file_exists", return_value=True) as stat:
            self.assertEqual(eos_file_info_batch(["root://other//outputs/a.root"]),
                             {"root://other//outputs/a.root": {}})
        stat.assert_called_once_with("root://other//outputs/a.root", 60)

    def test_stat_fallback_uses_the_url_host(self):
        with patch("ingestion_utils.subprocess.run", return_value=SimpleNamespace(returncode=0)) as run:
            self.assertTrue(eos_file_exists("root://other:1094//outputs/a.root"))
        self.assertEqual(run.call_args.args[0],
                         ["xrdfs", "root://other:1094/", "stat", "/outputs/a.root"])


try:
    import analysis_tasks as tasks
    import law
except ImportError:
    tasks = None


@unittest.skipIf(tasks is None, "LAW/Luigi dependencies unavailable")
class TestValidationReceipts(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.patch = patch.object(tasks, "WORKSPACE", str(self.root))
        self.patch.start()
        self.addCleanup(self.patch.stop)
        self.manifest_path = self.root / "datasets.yaml"
        self.manifest_path.write_text("datasets:\n- name: sample\n  dtype: data\n  files: [input.root]\n")
        self.config = self.root / "submit_config.txt"
        self.config.write_text("enableSkim=true\nsaveTree=Events\n")
        self.task = tasks.MonitorSkimJobs(exe="/fake/exe", name="receipts",
            submit_config=str(self.config), dataset_manifest=str(self.manifest_path))
        self.manifest = tasks.DatasetManifest.load(str(self.manifest_path))
        self.jobs, self.states, self.signatures = {}, {}, {}
        for index in range(2):
            key = ("sample", index)
            info = dict(dir=str(self.root / f"job_{index}"),
                save=f"root://storage//sample/skim_{index}.root",
                meta=f"root://storage//sample/meta_{index}.root", tree_name="Events")
            self.jobs[key] = (info, ("save", "meta"))
            self.states[key] = dict(status="done", cluster="123", proc=index, retries=0,
                job_dir=info["dir"], save=info["save"], meta=info["meta"], tree_name="Events")
            for role in ("save", "meta"):
                self.signatures[info[role]] = dict(size=100, mtime="2026-11-09 12:00:00")
        self.addCleanup(patch.stopall)
        self.batch = patch("rdfproduction.jobs.output_validation._output_file_info_batch", side_effect=lambda paths:
            {p: self.signatures.get(p) for p in paths}).start()
        self.validate = patch("rdfproduction.jobs.output_validation._skim_job_output_errors", return_value=[]).start()
        patch.object(self.task, "publish_message").start()

    def check(self):
        return self.task._validate_output_jobs(self.jobs, self.states, self.manifest)

    def test_repeat_poll_and_restarted_complete_reuse_integrity_receipts(self):
        self.assertFalse(any(self.check().values()))
        self.assertEqual(self.validate.call_count, 2)
        self.assertFalse(any(self.check().values()))
        self.assertEqual(self.validate.call_count, 2)
        run = Path(self.task._run_dir)
        (run / "all_outputs_verified.txt").write_text("done=2\nperm_fail=0\ntotal=2\n")
        (run / "monitor_state.json").write_text(json.dumps({"datasets": {
            "sample": {"jobs": {str(key[1]): value for key, value in self.states.items()}}}}))
        restarted = self.task.clone()
        with patch.object(restarted, "publish_message"):
            self.assertTrue(restarted.complete())
            self.assertTrue(restarted.complete())
        self.assertEqual(self.validate.call_count, 2)
        self.assertEqual(self.batch.call_count, 4)  # fresh storage checks remain

    def test_changed_file_revalidates_only_affected_pair_and_deletion_fails(self):
        self.check()
        path = self.jobs[("sample", 0)][0]["meta"]
        self.signatures[path] = dict(size=101, mtime="2026-11-09 12:00:00")
        self.check()
        self.assertEqual(self.validate.call_count, 3)
        self.signatures[path] = None
        self.assertTrue(self.check()[("sample", 0)])
        self.assertEqual(self.validate.call_count, 3)

    def test_two_real_monitor_polls_validate_each_finished_job_once(self):
        for info, roles in self.jobs.values():
            directory = Path(info["dir"])
            directory.mkdir()
            (directory / "submit_config.txt").write_text(
                f"enableSkim=true\nsaveFile={info['save']}\nmetaFile={info['meta']}\nsaveTree=Events\n")
        submitted = self.root / "submitted.json"
        submitted.write_text(json.dumps({"datasets": [{"dataset_name": "sample",
            "submission_dir": str(self.root), "cluster_id": "123"}]}))
        self.task.poll_interval = 0
        with patch.object(self.task, "input", return_value=law.LocalFileTarget(str(submitted))), \
             patch("rdfproduction.transport.condor_q_ads", side_effect=[{1: {"JobStatus": 2}}, {}]), \
             patch("rdfproduction.transport.condor_history_exit", side_effect=[{0: 0}, {0: 0, 1: 0}]), \
             patch.object(self.task, "_update_site_rankings"), \
             patch.object(self.task, "_update_job_sizing"):
            self.task.run()
        self.assertEqual(self.validate.call_count, 2)
        self.assertTrue(self.task.complete())
        self.assertEqual(self.validate.call_count, 2)
        self.assertEqual({j["status"] for j in self.task._load_state()["datasets"]["sample"]["jobs"].values()}, {"done"})

    def test_changed_attempt_or_tree_cannot_reuse_old_receipt(self):
        self.check()
        self.states[("sample", 0)]["cluster"] = "456"
        self.check()
        self.assertEqual(self.validate.call_count, 3)
        self.jobs[("sample", 1)][0]["tree_name"] = "OtherTree"
        self.check()
        self.assertEqual(self.validate.call_count, 4)

    def test_changed_configuration_or_execution_identity_invalidates_receipts(self):
        self.check()
        self.config.write_text(self.config.read_text() + "newSetting=true\n")
        self.check()
        self.assertEqual(self.validate.call_count, 4)
        (Path(self.task._run_dir) / "execution_identity.json").write_text('{"execution_id":"new"}')
        self.check()
        self.assertEqual(self.validate.call_count, 6)

    def test_unknown_remote_metadata_cannot_skip_integrity(self):
        self.signatures = {path: {} for path in self.signatures}
        self.check()
        self.check()
        self.assertEqual(self.validate.call_count, 4)
        self.assertEqual(self.task._read_validation_cache(), {})

    def test_interrupted_pass_keeps_completed_receipts(self):
        self.validate.side_effect = [[], KeyboardInterrupt()]
        with self.assertRaises(KeyboardInterrupt):
            self.check()
        self.assertEqual(len(self.task._read_validation_cache()), 1)
        self.validate.side_effect = None
        self.check()
        self.assertEqual(self.validate.call_count, 3)

    def test_corrupt_cache_is_rebuilt_and_failed_integrity_is_not_cached(self):
        path = self.task._validation_cache_path
        path.parent.mkdir(parents=True)
        path.write_text("broken JSON")
        self.validate.return_value = ["corrupt ROOT"]
        self.assertTrue(all(self.check().values()))
        self.assertEqual(self.task._read_validation_cache(), {})
        self.validate.return_value = []
        self.check()
        self.assertEqual(len(self.task._read_validation_cache()), 2)

    def test_cache_updates_merge_without_overwriting_other_validations(self):
        self.task._save_validation_updates({"first": {"validated": True}})
        self.task.clone()._save_validation_updates({"second": {"validated": True}})
        self.assertEqual(set(self.task._read_validation_cache()), {"first", "second"})

    def test_unwritable_receipt_cache_does_not_fail_valid_physics_outputs(self):
        with patch.object(self.task, "_save_validation_updates", side_effect=OSError("disk full")):
            self.assertFalse(any(self.check().values()))
        self.assertEqual(self.validate.call_count, 2)

    def test_exists_policy_never_populates_integrity_receipts(self):
        self.task.output_validation = "exists"
        with patch("rdfproduction.jobs.output_validation._output_files_exist_batch", return_value={
                path: True for path in self.signatures}):
            self.assertFalse(any(self.check().values()))
        self.assertEqual(self.task._read_validation_cache(), {})
        self.validate.assert_not_called()
        self.task.output_validation = "integrity"
        self.check()
        self.assertEqual(self.validate.call_count, 2)


if __name__ == "__main__":
    unittest.main()
