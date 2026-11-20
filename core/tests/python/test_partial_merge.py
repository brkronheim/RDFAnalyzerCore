"""Local integration of monitor snapshots with the shared ROOT metadata merger."""
import fcntl
import json
from pathlib import Path
import tempfile
import unittest

from partial_merge import merge_completed_outputs
from run_bundle import file_digest

try:
    import ROOT
    import uproot
    import numpy as np
    ROOT_AVAILABLE = True
except ImportError:
    ROOT_AVAILABLE = False


@unittest.skipUnless(ROOT_AVAILABLE, "configured ROOT and uproot environment required")
class PartialMergeTests(unittest.TestCase):
    def test_reviewed_cache_hash_policy_reaches_cli_and_snapshot(self):
        from metadata_io import JOB_KEY, validate_metadata_bundle
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs = {}
            for index, semantic in enumerate(["original", "cache32"]):
                source = root/f"job{index}.root"
                with uproot.recreate(source) as output:
                    output["h"] = (np.array([index+1.]), np.array([0., 1.]))
                    output[JOB_KEY] = json.dumps({"format": "rdf-job-metadata", "schema_version": 1,
                        "provenance": {"job.logical_id": str(index), "service.counter.sample": "DY",
                                       "config.semantic_hash": semantic}})
                jobs[str(index)] = {"status": "done", "meta": str(source)}
            state = root/"state.json"
            state.write_text(json.dumps({"datasets": {"DY": {"jobs": jobs}}}))
            policy = {"schema_version": 1, "reason": "Reviewed cache-only change",
                      "samples": {"DY": ["original", "cache32"]}}
            policy_path = root/"policy.json"
            policy_path.write_text(json.dumps(policy))
            destination = root/"merged"
            merge_completed_outputs(state, destination, hadd_jobs=1, xrdcp_jobs=1,
                                    semantic_config_compatibility=policy_path)
            output = destination/"DY_partial.root"
            records = validate_metadata_bundle(output)
            self.assertEqual({r["provenance"]["config.semantic_hash"] for r in records}, {"original", "cache32"})
            with uproot.open(output) as merged:
                self.assertEqual(merged["h"].values().tolist(), [3.])
            snapshot = json.loads(output.with_suffix(".snapshot.json").read_text())
            self.assertEqual(snapshot["configuration"]["semantic_config_compatibility"], policy)

    def test_sparse_only_omits_legacy_slices_and_keeps_metadata_bundle(self):
        from array import array
        from metadata_io import validate_metadata_bundle
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source = root/"job.root"
            f = ROOT.TFile.Open(str(source), "RECREATE")
            h = ROOT.THnSparseF("h", "h", 5, array('i', [1]*5),
                               array('d', [0.]*5), array('d', [1.]*5), 16 * 1024)
            h.Sumw2()
            h.Fill(array('d', [.5]*5), 2.)
            for axis, label in enumerate(["signal", "default", "sample", "Nominal"]):
                h.GetAxis(axis).SetBinLabel(1, label)
            f.mkdir("nd_histograms_v1").cd()
            h.Write()
            roles = ["channel", "control", "sample", "systematic", "observable"]
            labels = ["signal", "default", "sample", "Nominal"]
            schema = dict(schema_version=1, directory="nd_histograms_v1", histograms=[
                dict(key="h", name="h", variable="x", label="x", axes=[
                    dict(role=role, bins=1, labels=[labels[i]] if i<4 else [], edges=[0.,1.])
                    for i, role in enumerate(roles)])])
            f.cd()
            ROOT.TObjString(json.dumps(schema)).Write("__rdf_histogram_schema_v1")
            f.Close()
            state = root/"state.json"
            state.write_text(json.dumps({"datasets": {"DY": {"jobs": {
                "0": {"status": "done", "meta": str(source)}}}}}))
            for sparse_only in [True, False]:
                destination = root/str(sparse_only)
                merge_completed_outputs(state, destination, hadd_jobs=1,
                                        xrdcp_jobs=1, sparse_only=sparse_only)
                output = destination/"DY_partial.root"
                validate_metadata_bundle(output)
                with uproot.open(output) as merged:
                    self.assertIn("nd_histograms_v1/h", merged)
                    self.assertEqual("signal_default/sample/h" in merged, not sparse_only)
                snapshot = json.loads(output.with_suffix(".snapshot.json").read_text())
                self.assertEqual(snapshot["configuration"]["sparse_only"], sparse_only)

    def test_sparse_only_and_nominal_only_are_rejected_before_staging(self):
        with self.assertRaisesRegex(ValueError, "cannot be combined"):
            merge_completed_outputs("unused.json", "unused", sparse_only=True,
                                    nominal_only=True)

    def test_completed_snapshot_and_failed_refresh_preserve_previous_result(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            jobs = {}
            for index in range(2):
                source = root/f"job{index}.root"
                with uproot.recreate(source) as output:
                    output["h"] = (np.array([index+1.]),np.array([0.,1.]))
                jobs[str(index)] = dict(status="done",meta=str(source))
            jobs["failed"] = dict(status="failed",meta="must-not-read.root")
            state = root/"state.json"
            state.write_text(json.dumps(dict(datasets=dict(DY=dict(jobs=jobs)))))
            destination=root/"merged"
            merge_completed_outputs(state,destination,hadd_jobs=1,xrdcp_jobs=2)
            output=destination/"DY_partial.root"
            with uproot.open(output) as source:
                self.assertEqual(source["h"].values().tolist(),[3.])
            snapshot=json.loads(output.with_suffix(".snapshot.json").read_text())
            self.assertEqual(set(snapshot["configuration"]["jobs"]),{"0","1"})
            before=file_digest(output)
            bad=root/"bad.root"
            with uproot.recreate(bad) as source:
                source["h"]=(np.array([1.]),np.array([0.,2.]))
            jobs["2"]=dict(status="done",meta=str(bad))
            state.write_text(json.dumps(dict(datasets=dict(DY=dict(jobs=jobs)))))
            with self.assertRaises(SystemExit):
                merge_completed_outputs(state,destination,hadd_jobs=1,xrdcp_jobs=1)
            self.assertEqual(file_digest(output),before)

    def test_merge_lock_covers_the_entire_operation(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            state=root/"state.json"
            state.write_text('{"datasets":{}}')
            destination=root/"merged"
            destination.mkdir()
            with (destination/".mergeResults.lock").open("a") as lock:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
                with self.assertRaisesRegex(RuntimeError,"already running"):
                    merge_completed_outputs(state,destination)


if __name__ == "__main__":
    unittest.main()
