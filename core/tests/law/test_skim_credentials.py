"""Proxy expiry must stop LAW dispatch even when prepared outputs are cached."""
import json
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "core/python"), str(ROOT / "core/python/law")]
import analysis_tasks as mod


def _result(seconds):
    return mod.subprocess.CompletedProcess([], 0, stdout=f"{seconds}\n")


class ProxySubmissionTests(unittest.TestCase):
    def test_two_day_boundary_and_validation_errors(self):
        for seconds in (0, 172799, 172800, 604800):
            with self.subTest(seconds=seconds), patch.object(
                mod.subprocess, "run", return_value=_result(seconds)
            ) as run:
                if seconds < 172800:
                    with self.assertRaisesRegex(RuntimeError, "at least 48 hours"):
                        mod._validate_x509_proxy("/staged/x509")
                else:
                    mod._validate_x509_proxy("/staged/x509")
                self.assertEqual(run.call_args.args[0],
                    ["voms-proxy-info", "-file", "/staged/x509", "-timeleft"])

        for failure in (FileNotFoundError("voms-proxy-info"),
                        mod.subprocess.CalledProcessError(1, "voms-proxy-info"),
                        mod.subprocess.TimeoutExpired("voms-proxy-info", 30)):
            with self.subTest(failure=failure), patch.object(
                mod.subprocess, "run", side_effect=failure
            ), self.assertRaisesRegex(RuntimeError, "Cannot verify staged X509 proxy"):
                mod._validate_x509_proxy("/staged/x509")
        with patch.object(mod.subprocess, "run", return_value=_result("invalid")), \
                self.assertRaisesRegex(RuntimeError, "Cannot verify"):
            mod._validate_x509_proxy("/staged/x509")

    def test_archive_is_checked_instead_of_renewed_loose_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            shared = Path(tmp, "shared_inputs")
            shared.mkdir()
            proxy = shared / "x509"
            proxy.write_text("expired archived proxy")
            archive = Path(tmp, "shared_inputs.tar.gz")
            mod._build_shared_inputs_archive(str(shared), str(archive))
            proxy.write_text("renewed loose proxy")
            checked_paths = []

            def check(command, **kwargs):
                checked = Path(command[2])
                checked_paths.append(checked)
                self.assertEqual(checked.read_text(), "expired archived proxy")
                self.assertEqual(checked.stat().st_mode & 0o777, 0o600)
                return _result(0)

            with patch.object(mod.subprocess, "run", side_effect=check), \
                    self.assertRaisesRegex(RuntimeError, "shared_inputs.tar.gz:x509"):
                mod._validate_staged_skim_proxy(str(shared), required=True)
            self.assertTrue(checked_paths)
            self.assertFalse(checked_paths[0].exists())

    def test_proxy_free_and_missing_archived_proxy(self):
        with tempfile.TemporaryDirectory() as tmp:
            shared = Path(tmp, "shared_inputs")
            shared.mkdir()
            mod._build_shared_inputs_archive(str(shared), str(Path(tmp, "shared_inputs.tar.gz")))
            with patch.object(mod.subprocess, "run") as run:
                mod._validate_staged_skim_proxy(str(shared))
                run.assert_not_called()
                with self.assertRaisesRegex(RuntimeError, "missing from"):
                    mod._validate_staged_skim_proxy(str(shared), required=True)
                (shared / "x509").write_text("new proxy")
                with self.assertRaisesRegex(RuntimeError, "missing from"):
                    mod._validate_staged_skim_proxy(str(shared))

    def test_preparation_checks_copied_proxy_and_aborts_before_archive(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(mod, "WORKSPACE", tmp), \
                patch.object(mod, "_collect_skim_shared_libs", return_value={}):
            executable = Path(tmp, "analysis")
            executable.write_text("#!/bin/sh\nexit 0\n")
            proxy = Path(tmp, "source_proxy")
            proxy.write_text("short lived proxy")
            config = Path(tmp, "submit_config.txt")
            config.write_text("enableSkim=false\n")
            manifest = Path(tmp, "datasets.yaml")
            manifest.write_text("datasets:\n- {name: sample, dtype: data, files: [input.root]}\n")
            task = mod.PrepareSkimJobs(name="proxy_check", exe=str(executable),
                submit_config=str(config), dataset_manifest=str(manifest),
                x509=str(proxy), make_test_job=False)
            with patch.object(mod.subprocess, "run", return_value=_result(172799)) as run:
                with self.assertRaisesRegex(RuntimeError, "at least 48 hours"):
                    task._run_impl()
                self.assertEqual(run.call_args.args[0][2], str(Path(task._shared_dir, "x509")))
            self.assertEqual(Path(task._shared_dir, "x509").read_bytes(), proxy.read_bytes())
            self.assertFalse(Path(task._shared_archive_path).exists())
            self.assertFalse(Path(task.output().path).exists())
            proxy.unlink()
            with self.assertRaisesRegex(RuntimeError, "X509 proxy file not found"):
                task._run_impl()

    def test_renewed_source_invalidates_cached_preparation(self):
        with tempfile.TemporaryDirectory() as tmp, \
                patch.object(mod, "WORKSPACE", tmp), \
                patch.object(mod, "_collect_skim_shared_libs", return_value={}):
            executable = Path(tmp, "analysis")
            executable.write_text("#!/bin/sh\nexit 0\n")
            config = Path(tmp, "submit_config.txt")
            config.write_text("enableSkim=false\n")
            manifest = Path(tmp, "datasets.yaml")
            manifest.write_text("datasets:\n- {name: sample, dtype: data, files: [input.root]}\n")
            proxy = Path(tmp, "source_proxy")
            proxy.write_text("valid proxy")
            task = mod.PrepareSkimJobs(name="renewed_proxy", exe=str(executable),
                submit_config=str(config), dataset_manifest=str(manifest),
                x509=str(proxy), make_test_job=False)
            with patch.object(mod.subprocess, "run", return_value=_result(604800)):
                task._run_impl()
                self.assertTrue(task.complete())
                proxy.write_text("renewed valid proxy")
                self.assertFalse(task.complete())
                task._run_impl()
                self.assertTrue(task.complete())
            self.assertEqual(Path(task._shared_dir, "x509").read_bytes(), proxy.read_bytes())

    def test_cached_submission_and_monitor_retry_recheck_archive(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(mod, "WORKSPACE", tmp):
            shared = Path(tmp, "skimRun_cached_proxy", "shared_inputs")
            shared.mkdir(parents=True)
            (shared / "x509").write_text("proxy")
            executable = Path(tmp, "analysis")
            executable.write_text("#!/bin/sh\nexit 0\n")
            (shared / "analysis").write_bytes(executable.read_bytes())
            mod._build_shared_inputs_archive(str(shared), str(shared.parent / "shared_inputs.tar.gz"))
            submit_file = shared.parent / "condor_submit.sub"
            submit_file.write_text("queue 1\n")
            manifest = shared.parent / "submission_manifest.json"
            manifest.write_text(json.dumps({"datasets": [{
                "dataset_name": "sample", "submit_path": str(submit_file), "job_count": 1,
            }]}))
            args = dict(name="cached_proxy", exe=str(executable), submit_config="config.txt",
                        dataset_manifest="datasets.yaml", x509="/new/source/proxy",
                        make_test_job=False)
            task = mod.SubmitSkimJobs(**args)
            monitor = mod.MonitorSkimJobs(**args)
            state = {"status": "submitted", "retries": 0}
            for seconds in (0, 172799, 172800):
                def command_result(command, **kwargs):
                    if command[0] == "voms-proxy-info":
                        return _result(seconds)
                    return mod.subprocess.CompletedProcess(command, 0,
                        stdout="1 job(s) submitted to cluster 12345.\n")

                with self.subTest(seconds=seconds), \
                        patch.object(task, "input", return_value={"build": {
                            "manifest": mod.law.LocalFileTarget(str(manifest)),
                        }}), patch.object(mod.subprocess, "run", side_effect=command_result) as run:
                    if seconds < 172800:
                        with self.assertRaisesRegex(RuntimeError, "at least 48 hours"):
                            task.run()
                        self.assertFalse(Path(task.output().path).exists())
                        with patch.object(mod, "_submit_single_skim_job") as retry:
                            with self.assertRaisesRegex(RuntimeError, "at least 48 hours"):
                                monitor._try_resubmit("sample", 0, {}, state, "failed")
                            retry.assert_not_called()
                        self.assertEqual(state, {"status": "submitted", "retries": 0})
                        self.assertTrue(all(call.args[0][0] != "condor_submit"
                                            for call in run.call_args_list))
                    else:
                        task.run()
                        submitted = json.loads(Path(task.output().path).read_text())
                        self.assertEqual(submitted["datasets"][0]["cluster_id"], "12345")
