"""Exercise publication, tamper detection, dependency closure and exact resumes."""
import json
from pathlib import Path
import tempfile
import unittest

from run_bundle import (BundleError, atomic_directory, compile_config_bundle,
                        derived_artifact, execution_identity, record_execution,
                        seal_bundle, snapshot_assets, verify_bundle)


class RunBundleTests(unittest.TestCase):
    def test_collection_waits_for_all_children_and_reports_failures(self):
        import subprocess
        from unittest.mock import patch
        from run_collection import CollectionError, run_commands, isolated_output
        calls=[]
        def run(command, **kwargs):
            calls.append(command[0])
            if command[0] == "bad":
                raise subprocess.CalledProcessError(1,command)
            return subprocess.CompletedProcess(command,0)
        with patch("run_collection.subprocess.run",side_effect=run):
            with self.assertRaises(CollectionError):
                run_commands([["bad"],["good"]],workers=2)
        self.assertEqual(set(calls),{"bad","good"})
        with self.assertRaises(ValueError):
            isolated_output("/output","../other")

    def test_preflight_reports_bundle_tampering(self):
        from analysis_preflight import AnalysisPreflight
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/"config").write_text("original")
            seal_bundle(root)
            report=AnalysisPreflight(root)
            report.check_run_bundle(root)
            self.assertTrue(report.result.passed)
            (root/"config").write_text("changed")
            report.check_run_bundle(root)
            self.assertFalse(report.result.passed)

    def test_collection_verifies_child_identities(self):
        from run_bundle import seal_collection, verify_collection
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name in ("era_a","era_b"):
                (root/name).mkdir()
                (root/name/"config").write_text(name)
                seal_bundle(root/name)
            seal_collection(root,{"era_a":"era_a","era_b":"era_b"})
            self.assertEqual(len(verify_collection(root)["children"]),2)
            (root/"era_b/config").write_text("changed")
            with self.assertRaises(BundleError):
                verify_collection(root)

    def test_diagnostic_identity_detects_same_mtime_content_changes(self):
        import os
        from statistical_inference.diagnostics import diagnostic_manifest
        from statistical_inference.errors import FitError
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            fit, workspace = root/"fit.json",root/"workspace.root"
            fit.write_text('{}')
            workspace.write_text("aaaa")
            manifest=root/"diagnostics.json"
            manifest.write_text(json.dumps(diagnostic_manifest(manifest,fit,workspace)))
            stat=workspace.stat()
            workspace.write_text("bbbb")
            os.utime(workspace,ns=(stat.st_atime_ns,stat.st_mtime_ns))
            with self.assertRaises(FitError):
                diagnostic_manifest(manifest,fit,workspace)

    def test_atomic_failure_and_no_overwrite(self):
        with tempfile.TemporaryDirectory() as tmp:
            destination = Path(tmp) / "bundle"
            with self.assertRaisesRegex(RuntimeError, "incomplete"):
                with atomic_directory(destination) as root:
                    (root / "config").write_text("partial")
                    raise RuntimeError("incomplete")
            self.assertFalse(destination.exists())
            with atomic_directory(destination) as root:
                (root / "config").write_text("complete")
                seal_bundle(root, entrypoints={"config": "config"})
            with self.assertRaises(BundleError):
                with atomic_directory(destination):
                    pass
            record = verify_bundle(destination)
            (destination / "config").write_text("changed")
            with self.assertRaisesRegex(BundleError, "changed"):
                verify_bundle(destination)
            self.assertEqual(record["schema"], "rdf-run-bundle-v1")

    def test_nested_dependencies_and_relocation(self):
        import yaml
        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "source"
            source.mkdir()
            (source / "weights.json").write_text('{"value": 2}')
            (source / "trigger.yaml").write_text("- file: weights.json\n")
            (source / "config.yaml").write_text("triggerConfig: trigger.yaml\nfileList: absent.root\n")
            destination = Path(tmp) / "bundle"
            record = compile_config_bundle(source / "config.yaml", destination,
                                            asset_keys=["triggerConfig"])
            (source / "weights.json").unlink()
            relocated = Path(tmp) / "relocated"
            destination.rename(relocated)
            self.assertEqual(verify_bundle(relocated)["bundle_id"], record["bundle_id"])
            config = yaml.safe_load((relocated / "submit_config.yaml").read_text())
            trigger = yaml.safe_load((relocated / config["triggerConfig"]).read_text())
            self.assertEqual(json.loads((relocated / trigger[0]["file"]).read_text()), {"value": 2})
            self.assertEqual(config["fileList"], "absent.root")

    def test_asset_names_do_not_collide(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            for name in ("a", "b"):
                (root / name).mkdir()
                (root / name / "same.json").write_text(name)
            assets = snapshot_assets(root, [root / name / "same.json" for name in ("a", "b")])
            self.assertEqual(len(set(assets.values())), 2)

    def test_resume_requires_the_same_inputs_and_runtime(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "execution.json"
            record = execution_identity("bundle", inputs=["one.root"], partitions=[[0]],
                                        runtime={"exe": "hash"}, options={"threads": 1})
            record_execution(path, record)
            record_execution(path, record)
            other = execution_identity("bundle", inputs=["two.root"], partitions=[[0]],
                                       runtime={"exe": "hash"}, options={"threads": 1})
            with self.assertRaisesRegex(BundleError, "new run name"):
                record_execution(path, other)


if __name__ == "__main__":
    unittest.main()
