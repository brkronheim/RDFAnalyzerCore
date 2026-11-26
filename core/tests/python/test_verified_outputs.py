"""Exercise the handoff from frozen Condor inputs to the downstream merger."""
import hashlib
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from partial_merge import merge_completed_outputs, merge_verified_outputs
from job_sizing import EntryCache
from production_artifacts import verify_outputs
from production_stitching import _verified_metadata
from run_bundle import content_digest, execution_identity, file_digest, write_json
from verified_outputs import load_verified_output_index, local_output_signature, write_verified_output_index


class TestVerifiedOutputs(unittest.TestCase):
    def setUp(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.run = self.root / "run"
        self.index_path = self.root / "verified.json"
        self.source = self.run / "jobs" / "DY" / "job_0"
        self.submission = self.run / "condor_submissions" / "DY"
        self.staged = self.submission / "job_0"
        self.meta = self.root / "meta.root"
        self.inputs = dict(manifest_sha256="manifest")
        self.common = dict(inputs=self.inputs, runtime={"executable": "digest"}, options={})
        self.identity = execution_identity("bundle", partitions={"partition": "events"}, **self.common)
        write_json(self.run / "execution_identity.json", self.identity)
        self.source_config = self.write_config(self.source, self.meta, first=0, last=20)
        self.write_config(self.staged, self.meta, first=0, last=20)
        plan = execution_identity("bundle", partitions=[
            {"dataset": "DY", "config_sha256": file_digest(self.source_config)}],
            **self.common)
        write_json(self.run / "execution_plan.json", plan)
        write_json(self.run / "prep_submission.json", [
            {"dataset_name": "DY", "job_dir": str(self.source), "config_path": str(self.source_config)}])
        write_json(self.run / "submitted.json", {"datasets": [{
            "dataset_name": "DY", "submission_dir": str(self.submission),
            "job_count": 1, "cluster_id": "100", "jobs": [{
                "job_index": 0, "global_index": 0, "source_job_dir": str(self.source),
                "job_dir": str(self.staged)}]}]})
        self.jobs = {}
        self.receipts = {}
        self.state_path = self.run / "monitor_state.json"

    @staticmethod
    def write_config(directory, meta, *, first, last, files="root://source//store/fixture.root"):
        path = directory / "submit_config.txt"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"fileList={files}\nfirstEntry={first}\n"
                        f"lastEntry={last}\nmetaFile={meta}\nenableSkim=false\n")
        return path

    def complete_job(self, slot, directory, meta, *, first, last, **options):
        config = self.write_config(directory, meta, first=first, last=last, **options)
        meta.write_bytes(f"output {slot}".encode())
        self.jobs[str(slot)] = {"status": "done", "cluster": "100", "proc": slot, "retries": 0,
                                "job_dir": str(directory), "meta": str(meta), "tree_name": "Events"}
        self.receipts[json.dumps(["DY", str(slot)])] = {
            "contract": {"version": 1, "dataset": "DY", "config_sha256": file_digest(config),
                         "manifest_sha256": "manifest", "cluster": "100", "proc": slot,
                         "retries": 0, "tree_name": "Events", "roles": ["meta"],
                         "execution_sha256": file_digest(self.run / "execution_identity.json")},
            "files": {str(meta): local_output_signature(meta)}}

    def publish_inputs(self):
        write_json(self.state_path, {"datasets": {
            "DY": {"submission_dir": str(self.submission), "jobs": self.jobs}}})
        write_json(self.run / "output_validation.json", {"schema_version": 1, "jobs": self.receipts})

    def publish(self, validate=lambda: True):
        self.publish_inputs()
        return write_verified_output_index(self.run, self.index_path, validate=validate)

    def split(self, first=10, last=20):
        self.complete_job(0, self.submission / "resubmissions" / "job_0_retry1_part0",
                          self.root / "meta_part0.root", first=0, last=10)
        self.complete_job(1, self.submission / "resubmissions" / "job_0_retry1_part1",
                          self.root / "meta_part1.root", first=first, last=last)

    def test_split_coverage_and_stable_publication(self):
        self.split()
        index = self.publish()
        self.assertEqual(set(index["datasets"]["DY"]["jobs"]), {"0", "1"})
        self.assertEqual(index["monitor_state_sha256"], file_digest(self.state_path))
        self.assertEqual(index, load_verified_output_index(self.index_path))
        inode = self.index_path.stat().st_ino
        self.assertEqual(self.publish(), index)
        self.assertEqual(self.index_path.stat().st_ino, inode)

    def test_stitching_reads_only_terminal_metadata_and_rejects_changed_files(self):
        self.split()
        self.meta.write_bytes(b"stale parent")
        self.publish()
        plan = {"task_parameters": {"name": "counters"}}
        write_json(self.run / "rdf_production.json", plan)
        with patch("production_stitching.run_directory", return_value=self.run):
            selected = _verified_metadata(plan, self.index_path)
            expected = [str(self.root / name) for name in ("meta_part0.root", "meta_part1.root")]
            self.assertEqual(selected, {"DY": expected})
            Path(expected[0]).write_bytes(b"changed after verification")
            with self.assertRaisesRegex(ValueError, "Verified output changed"):
                _verified_metadata(plan, self.index_path)

    def test_nested_split_keeps_the_original_range_even_when_the_slot_changes(self):
        self.split()
        self.complete_job(1, self.submission / "resubmissions" / "job_1_retry2_part0",
                          self.root / "meta_part1_part0.root", first=10, last=15)
        self.complete_job(2, self.submission / "resubmissions" / "job_1_retry2_part1",
                          self.root / "meta_part1_part1.root", first=15, last=20)
        index = self.publish()
        actual_originals = {job["original"] for job in index["datasets"]["DY"]["jobs"].values()}
        self.assertEqual(actual_originals, {str(self.meta)})

    def test_whole_file_group_can_have_bounded_terminal_descendants(self):
        first, second = "/store/one.root", "/store/two.root"
        for directory in (self.source, self.staged):
            self.write_config(directory, self.meta, first=0, last=0, files=f"{first},{second}")
        plan = execution_identity("bundle", partitions=[
            {"dataset": "DY", "config_sha256": file_digest(self.source_config)}], **self.common)
        write_json(self.run / "execution_plan.json", plan)
        counts = EntryCache(self.run / "input_metadata.json")
        counts.put(second, 20)
        counts.save()
        retries = self.submission / "resubmissions"
        self.complete_job(0, retries / "job_0_retry1_part0", self.root / "meta_part0.root",
                          first=0, last=0, files=first)
        self.complete_job(1, retries / "job_1_retry2_part0", self.root / "meta_part1_part0.root",
                          first=0, last=10, files=second)
        self.complete_job(2, retries / "job_1_retry2_part1", self.root / "meta_part1_part1.root",
                          first=10, last=20, files=second)
        self.publish()
        self.index_path.unlink()
        (self.run / "input_metadata.json").unlink()
        with self.assertRaisesRegex(ValueError, "Original entry count unavailable"):
            self.publish()

    def test_changed_prepared_input_is_rejected_even_when_worker_receipts_match(self):
        self.split()
        self.source_config.write_text(self.source_config.read_text() + "wantedExtraSetting=true\n")
        with self.assertRaisesRegex(ValueError, "Prepared configuration changed"):
            self.publish()

    def test_gap_or_overlap_and_missing_leaf_rejected(self):
        for first in (9, 11):
            with self.subTest(first=first):
                self.split(first=first)
                with self.assertRaisesRegex(ValueError, "Gap or overlap"):
                    self.publish()
        del self.jobs["1"]
        with self.assertRaisesRegex(ValueError, "Incomplete terminal entry range"):
            self.publish()

    def test_receipt_attempt_identity_and_completion(self):
        self.split()
        self.jobs["0"]["cluster"] = "other"
        with self.assertRaisesRegex(ValueError, "integrity receipt"):
            self.publish()
        self.jobs["0"]["status"] = "held"
        with self.assertRaisesRegex(ValueError, "is incomplete"):
            self.publish()

    def test_partial_index_records_failed_leaf_and_requires_success_for_every_dataset(self):
        self.split()
        self.jobs["1"]["status"] = "perm_fail"
        with self.assertRaisesRegex(ValueError, "is incomplete"):
            self.publish()
        write_verified_output_index(self.run, self.index_path, validate=lambda: True, allow_partial=True)
        group = load_verified_output_index(self.index_path)["datasets"]["DY"]
        self.assertEqual(set(group["jobs"]), {"0"})
        self.assertEqual(set(group["failures"]), {"1"})
        self.assertEqual(group["failures"]["1"]["partition"]["range"], [10, 20])
        self.index_path.unlink()
        self.jobs["0"]["status"] = "perm_fail"
        self.publish_inputs()
        with self.assertRaisesRegex(ValueError, "no successful partitions"):
            write_verified_output_index(self.run, self.index_path, validate=lambda: True, allow_partial=True)

    def test_state_read_around_validation_and_destination_collision(self):
        self.split()

        def validate():
            self.state_path.write_text("{}")
            return True

        with self.assertRaisesRegex(ValueError, "Monitor state changed"):
            self.publish(validate)
        self.index_path = self.run / "execution_plan.json"
        expected = self.index_path.read_bytes()
        with self.assertRaisesRegex(ValueError, "execution identity changed"):
            self.publish()
        self.assertEqual(self.index_path.read_bytes(), expected)

    def test_index_read_rejects_tampering_and_invalid_coverage(self):
        self.split()
        index = self.publish()
        del index["datasets"]["DY"]["jobs"]["1"]
        write_json(self.index_path, index)
        with self.assertRaisesRegex(ValueError, "index digest"):
            load_verified_output_index(self.index_path)
        del index["index_id"]
        index["index_id"] = content_digest(index)
        write_json(self.index_path, index)
        with self.assertRaisesRegex(ValueError, "Incomplete terminal entry range"):
            load_verified_output_index(self.index_path)

    def test_verified_merge_after_monitor_cleanup_and_changed_source(self):
        self.split()
        index = self.publish()
        self.state_path.unlink()
        merger = self.root / "metadata_merger.py"
        merger.write_text("# fixture")
        output = self.root / "merged"
        receipt = self.root / "merge-receipt.json"
        options = dict(hadd_jobs=1, xrdcp_jobs=1, sparse_only=True, metadata_merger=merger,
                       receipt=receipt)

        def merge_stub(command, **kwargs):
            Path(command[2]).write_bytes(b"merged")
            return SimpleNamespace(returncode=0)

        with patch("partial_merge.subprocess.run", side_effect=merge_stub) as calls, \
             patch("metadata_io.metadata_artifact_paths", side_effect=lambda path: [path]):
            snapshots = merge_verified_outputs(self.index_path, output, **options)
            self.assertEqual(calls.call_count, 1)
            snapshot = json.loads(snapshots[0].read_bytes())
            self.assertEqual(snapshot["kind"], "verified-merge")
            self.assertEqual(snapshot["parents"], {"verified_output_index": index["index_id"]})
            merged = json.loads(receipt.read_bytes())
            self.assertEqual(merged["schema"], "rdf-verified-merge-v1")
            self.assertEqual(merged["index_id"], index["index_id"])
            self.assertEqual(merged["snapshots"], {str(snapshots[0]): file_digest(snapshots[0])})
            self.assertEqual(verify_outputs(self.index_path, receipt)["merged_datasets"], 1)
            (output / "DY_partial.root").unlink()
            with self.assertRaisesRegex(ValueError, "derived artifact input missing"):
                verify_outputs(self.index_path, receipt)
            (output / "DY_partial.root").write_bytes(b"merged")
            inode = receipt.stat().st_ino
            self.assertEqual(snapshots, merge_verified_outputs(self.index_path, output, **options))
            self.assertEqual(calls.call_count, 1)
            self.assertEqual(receipt.stat().st_ino, inode)
            merger.write_text("# updated fixture")
            merge_verified_outputs(self.index_path, output, **options)
            self.assertNotEqual(json.loads(receipt.read_bytes())["receipt_id"], merged["receipt_id"])
            self.assertEqual(calls.call_count, 2)
            (self.root / "meta_part0.root").write_bytes(b"X" * len(b"output 0"))
            with self.assertRaisesRegex(ValueError, "changed or is unavailable"):
                merge_verified_outputs(self.index_path, output, **options)
            self.assertEqual(calls.call_count, 2)

    def test_merge_receipt_rejects_unrelated_output_index_or_snapshot_paths(self):
        self.split()
        self.publish()
        output = self.root / "merged"
        unrelated = self.root / "state-or-config.json"
        unrelated.write_text("{}")
        paths = [(self.index_path, "distinct from the output index"),
                 (output / "DY_partial.snapshot.json", "distinct from dataset snapshots"),
                 (unrelated, "Cannot overwrite unrelated file")]
        for path, error in paths:
            with self.subTest(path=path):
                with self.assertRaisesRegex(ValueError, error):
                    merge_verified_outputs(self.index_path, output, receipt=path)
        self.assertEqual(unrelated.read_text(), "{}")

    def test_partial_merge_hashes_the_exact_monitor_state_used(self):
        output = self.root / "merged"
        source = self.root / "sample.root"
        source.write_bytes(b"root")
        state = {"datasets": {"DY": {"jobs": {"0": {"status": "done", "meta": str(source)}}}}}
        write_json(self.state_path, state)
        expected = hashlib.sha256(self.state_path.read_bytes()).hexdigest()
        merger = self.root / "metadata_merger.py"
        merger.write_text("# fixture")

        def copy_stub(source, destination):
            Path(destination).write_bytes(Path(source).read_bytes())
            write_json(self.state_path, {"datasets": {}})

        def merge_stub(command, **kwargs):
            Path(command[2]).write_bytes(b"merged")
            return SimpleNamespace(returncode=0)

        with patch("partial_merge.shutil.copy2", side_effect=copy_stub), \
             patch("partial_merge.subprocess.run", side_effect=merge_stub), \
             patch("metadata_io.metadata_artifact_paths", side_effect=lambda path: [path]):
            merge_completed_outputs(self.state_path, output, hadd_jobs=1, xrdcp_jobs=1,
                                    sparse_only=True, metadata_merger=merger)
        snapshot = json.loads((output / "DY_partial.snapshot.json").read_bytes())
        self.assertEqual(snapshot["parents"], {"monitor_state": expected})
        self.assertNotEqual(file_digest(self.state_path), expected)


if __name__ == "__main__":
    unittest.main()
