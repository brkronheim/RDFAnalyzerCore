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


def test_historical_opaque_recipe_keeps_sealed_asset_paths(tmp_path):
    from rdfproduction.jobs.staging import _ensure_local_analysis_runtime_layout
    source = tmp_path / "old"
    assets = source / "assets"
    assets.mkdir(parents=True)
    payload = assets / "123_weights.json"
    payload.write_bytes(b'{"weight":1}')
    recipe = assets / "456_custom.txt"
    raw = b"custom file=assets/123_weights.json\n"
    recipe.write_bytes(raw)
    main = source / "submit_config.yaml"
    main.write_text("modelConfig: assets/456_custom.txt\n")
    files = {"/original/weights.json": "assets/123_weights.json", "/original/custom.txt": "assets/456_custom.txt"}
    seal_bundle(source, assets=files, entrypoints={"submit_config": "submit_config.yaml"})
    job = tmp_path / "job"
    _ensure_local_analysis_runtime_layout(str(main), {"modelConfig": "assets/456_custom.txt"}, str(job))
    assert (job / "cfg" / recipe.name).read_bytes() == raw
    for base in (job, job / "cfg"):
        assert (base / "assets" / payload.name).read_bytes() == payload.read_bytes()
    verify_bundle(source)


def test_unchanged_domain_yaml_keeps_its_exact_bytes_in_bundle_and_job(tmp_path):
    from rdfproduction.jobs.staging import _ensure_local_analysis_runtime_layout
    recipe = tmp_path / "recoil.yaml"
    original = b"# recoil calibration, hashed by analysis\ncoefficients: [1.250, 0.000]\n"
    recipe.write_bytes(original)
    main = tmp_path / "submit.yaml"
    main.write_text("recoilFile: recoil.yaml\n")
    bundle = tmp_path / "bundle"
    record = compile_config_bundle(main, bundle, asset_keys=["recoilFile"])
    relative = record["assets"][str(recipe)]
    assert (bundle / relative).read_bytes() == original
    job = tmp_path / "job"
    _ensure_local_analysis_runtime_layout(str(bundle / "submit_config.yaml"),
                                         {"recoilFile": relative}, str(job))
    assert (job / "cfg" / Path(relative).name).read_bytes() == original

def test_extension_can_declare_native_config_shape(tmp_path):
    import yaml
    (tmp_path / "fit.txt").write_text("name=zee constraint=91.2\n")
    main = tmp_path / "worker.yaml"
    main.write_text("electronFitConfig: fit.txt\n")
    bundle = tmp_path / "bundle"
    record = compile_config_bundle(main, bundle, asset_keys=["electronFitConfig"],
                                   asset_kinds={"electronFitConfig": "records"})
    path = bundle / record["assets"][str(tmp_path / "fit.txt")]
    assert path.suffix == ".yaml"
    assert yaml.safe_load(path.read_bytes()) == [{"name": "zee", "constraint": "91.2"}]

def test_absent_optional_asset_schema_preserves_prior_bundle_identity(tmp_path):
    main = tmp_path / "worker.yaml"
    main.write_text("threads: 1\n")
    bundle = tmp_path / "bundle"
    old = compile_config_bundle(main, bundle, asset_keys=["optionalFit"])
    same = compile_config_bundle(main, bundle, asset_keys=["optionalFit"],
                                 asset_kinds={"optionalFit": "records"}, reuse=True)
    assert same["bundle_id"] == old["bundle_id"]
