import fcntl
import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import Mock, patch

from condor_submission import submit_skim_batches


class CondorSubmissionTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.directory = Path(directory.name)
        self.manifest = self.directory / "submission_manifest.json"
        self.receipt = self.directory / "submitted.json"
        self.journal = self.directory / "submission_journal.json"
        self.context = {"owner": "researcher", "host": "submit.example", "condor": {}}
        context_patch = patch("condor_submission._submission_context", return_value=self.context)
        self.context_mock = context_patch.start()
        self.addCleanup(context_patch.stop)
        run_patch = patch("condor_submission.subprocess.run")
        self.run_tool = run_patch.start()
        self.addCleanup(run_patch.stop)
        self.validate = Mock()
        self.publish = Mock()
        self.manifest.write_text(json.dumps({"datasets": [self._dataset("one", 1), self._dataset("two", 2)]}))

    def _dataset(self, name, count):
        submit_file = self.directory / f"{name}.sub"
        submit_file.write_text(f"queue {count}\n")
        return {"dataset_name": name, "submit_path": str(submit_file), "job_count": count}

    def _submit(self):
        submit_skim_batches(self.manifest, self.receipt, validate=self.validate, publish=self.publish)

    def _ads(self, *procs, label=None, owner="researcher", cluster=102):
        batch = json.loads(self.journal.read_text())["batches"]["two"]
        return [{"ClusterId": cluster, "ProcId": proc, "GlobalJobId": f"submit.example#{cluster}.{proc}#5",
                 "RDFSubmissionId": label or batch["id"], "Owner": owner} for proc in procs]

    def _accept_first_then_interrupt(self, command, **kwargs):
        self.assertEqual(command[0], "condor_submit")
        dataset = Path(command[-1]).stem
        batch = json.loads(self.journal.read_text())["batches"][dataset]
        self.assertEqual(batch["state"], "pending")
        self.assertIn(f'+RDFSubmissionId = "{batch["id"]}"', command)
        if dataset == "one":
            return subprocess.CompletedProcess(command, 0, stdout="101.0 - 101.0\n")
        raise subprocess.CalledProcessError(1, command, stderr="Lost connection to schedd")

    def _interrupt(self):
        self.run_tool.side_effect = self._accept_first_then_interrupt
        with self.assertRaisesRegex(RuntimeError, "acceptance is unresolved for two.*Lost connection"):
            self._submit()
        self.assertFalse(self.receipt.exists())
        batches = json.loads(self.journal.read_text())["batches"]
        self.assertEqual(batches["one"]["cluster_id"], "101")
        self.assertEqual(batches["two"]["state"], "pending")
        self.run_tool.reset_mock()

    def test_resumes_using_queue_and_history_without_resubmitting(self):
        self._interrupt()
        queued = self._ads(0)
        history = self._ads(0, 1)

        def query(command, **kwargs):
            self.assertIn(f'RDFSubmissionId =?= "{queued[0]["RDFSubmissionId"]}"', command[2])
            self.assertIn('Owner =?= "researcher"', command[2])
            ads = queued if command[0] == "condor_q" else history
            return subprocess.CompletedProcess(command, 0, stdout=json.dumps(ads))

        self.run_tool.side_effect = query
        self.validate.reset_mock()
        self._submit()
        self.assertEqual([call.args[0][0] for call in self.run_tool.call_args_list], ["condor_q", "condor_history"])
        self.validate.assert_not_called()
        records = json.loads(self.receipt.read_text())["datasets"]
        self.assertEqual([record["cluster_id"] for record in records], ["101", "102"])

    def test_empty_or_conflicting_scheduler_evidence_keeps_pending_intent(self):
        self._interrupt()
        cases = {
            "empty": ([], []),
            "incomplete": (self._ads(0), []),
            "wrong owner": (self._ads(0, 1, owner="someone-else"), []),
            "wrong label": (self._ads(0, 1, label="foreign-batch"), []),
            "two clusters": (self._ads(0, 1), self._ads(0, 1, cluster=103)),
        }
        saved_journal = self.journal.read_bytes()
        for label, (queued, history) in cases.items():
            with self.subTest(label=label):
                self.run_tool.reset_mock()
                self.run_tool.side_effect = [
                    subprocess.CompletedProcess("condor_q", 0, stdout=json.dumps(queued)),
                    subprocess.CompletedProcess("condor_history", 0, stdout=json.dumps(history)),
                ]
                with self.assertRaisesRegex(RuntimeError, "acceptance is unresolved for two"):
                    self._submit()
                self.assertEqual([call.args[0][0] for call in self.run_tool.call_args_list],
                                 ["condor_q", "condor_history"])
                self.assertFalse(self.receipt.exists())
                self.assertEqual(self.journal.read_bytes(), saved_journal)

    def test_rejects_changed_plan_and_scheduler_before_scheduler_calls(self):
        self._interrupt()
        self.context_mock.return_value = dict(self.context, host="other-host")
        with self.assertRaisesRegex(RuntimeError, "plan or scheduler differs"):
            self._submit()
        self.context_mock.return_value = self.context
        (self.directory / "two.sub").write_text("queue 3\n")
        with self.assertRaisesRegex(RuntimeError, "plan or scheduler differs"):
            self._submit()
        self.run_tool.assert_not_called()

    def test_legacy_receipt_and_active_lock_are_respected(self):
        legacy = '{"datasets": [{"dataset_name": "one", "cluster_id": "42"}]}\n'
        self.receipt.write_text(legacy)
        self._submit()
        self.assertEqual(self.receipt.read_text(), legacy)
        self.context_mock.assert_not_called()
        self.run_tool.assert_not_called()
        self.receipt.unlink()
        with (self.directory / ".submission.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with self.assertRaisesRegex(RuntimeError, "Another controller"):
                self._submit()
        self.run_tool.assert_not_called()
