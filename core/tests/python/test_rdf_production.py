from concurrent.futures import Future
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tarfile
import tempfile
from types import ModuleType
import unittest
from unittest.mock import MagicMock, patch

import rdf_production
import production_jobs as jobs
from run_bundle import seal_bundle


class TestProductionStage(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)
        self.bundle = self.root / "bundle"
        self.bundle.mkdir()
        (self.root / "input.root").write_bytes(b"file-backed planning input")
        (self.bundle / "datasets.yaml").write_text(
            f"lumi: 1\ndatasets:\n- name: data\n  dtype: data\n  files: [{self.root / 'input.root'}]\n"
        )
        (self.bundle / "submit.yaml").write_text("enableSkim: false\n")
        seal_bundle(self.bundle)
        self.data = {
            "version": 1,
            "name": "fixture",
            "exe": "/bin/true",
            "submit_config": "bundle/submit.yaml",
            "dataset_manifest": "bundle/datasets.yaml",
            "options": {
                "partition": "file_group",
                "make_test_job": False,
                "output_directory": "root://example.invalid//outputs",
            },
        }
        self.spec = self.root / "batch.json"
        self.write_spec()

    def write_spec(self):
        self.spec.write_text(json.dumps(self.data))

    @unittest.skipUnless(all(shutil.which(tool) for tool in ("cmake", "ctest", "c++", "readelf", "ldd")),
                         "native build toolchain unavailable")
    def test_multi_era_preflight_builds_once_and_gates_each_run(self):
        source = self.root / "src"
        source.mkdir()
        (source / "CMakeLists.txt").write_text(
            "cmake_minimum_required(VERSION 3.16)\nproject(pilot LANGUAGES CXX)\n"
            "enable_testing()\nadd_executable(worker worker.cc)\nadd_test(NAME smoke COMMAND worker)\n"
        )
        (source / "worker.cc").write_text("int main() { return 0; }\n")
        self.data["exe"] = "build/worker"
        self.data["build"] = {
            "source_directory": "src", "build_directory": "build", "target": "worker",
            "ctest_regex": "^smoke$",
        }
        self.write_spec()
        other = self.root / "another-era.json"
        other.write_text(json.dumps({**self.data, "name": "another-era"}))
        self.assertFalse((self.root / "build").exists())
        self.assertIn("build", rdf_production.inspect_spec(self.spec))
        self.assertFalse((self.root / "build").exists())
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError("batch access")):
            results = rdf_production.preflight(self.spec, other)
            first, second = [json.loads(Path(row["runtime_receipt"]).read_text()) for row in results]
            self.assertEqual(first["build_receipt"], second["build_receipt"])
            validation = json.loads(Path(first["build_receipt"]).read_text())["validation"]
            self.assertEqual(validation["ctest_regex"], "^smoke$")
            self.assertTrue(Path(validation["ctest_log"], "stdout.log").is_file())
            self.assertEqual(rdf_production.preflight(self.spec), results[:1])

            # A fresh run still asks CMake to check its sources, even when the
            # edited file was outside the explicitly declared input list.
            (source / "worker.cc").write_text("int main() { volatile int x = 0; return x; }\n")
            fresh = self.root / "fresh.json"
            fresh.write_text(json.dumps({**self.data, "name": "fresh"}))
            new, = rdf_production.preflight(fresh)
            changed = json.loads(Path(new["runtime_receipt"]).read_text())
            self.assertNotEqual(first["executable_sha256"], changed["executable_sha256"])
            with self.assertRaisesRegex(ValueError, "changed since build"):
                rdf_production.preflight(self.spec)

    def test_python_spec_materializes_a_runnable_entrypoint(self):
        self.data.pop("exe")
        (self.root / "analysis.py").write_text("from helpers import read_value\n"
                                             "import sys\nprint(read_value(sys.argv[1]))\n")
        (self.root / "helpers.py").write_text("from pathlib import Path\n"
                                             "def read_value(file): return Path(file).read_text().strip()\n")
        self.data["python"] = {"entrypoint": "analysis.py", "modules": {"helpers.py": "helpers.py"}}
        self.write_spec()
        with patch.object(rdf_production, "_WORKSPACE", self.root):
            result, = rdf_production.preflight(self.spec)
        command = subprocess.run([result["executable"], str(self.bundle / "submit.yaml")],
                                 check=True, capture_output=True, text=True, cwd=self.root)
        self.assertEqual(command.stdout.strip(), "enableSkim: false")
        self.assertEqual(json.loads(Path(result["runtime_receipt"]).read_text())["kind"], "python")

    def test_python_smoke_runs_a_representative_config_without_submitting(self):
        self.data.pop("exe")
        self.data["options"]["make_test_job"] = True
        (self.root / "analysis.py").write_text(
            "from pathlib import Path\n"
            "import sys\n"
            "config = Path(sys.argv[1])\n"
            "assert config.name == 'submit_config_data.yaml' and 'fileList' in config.read_text()\n"
            "Path('worker_ran').write_text(config.name)\n"
        )
        self.data["python"] = {"entrypoint": "analysis.py"}
        self.write_spec()
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError("unexpected submit")):
            marker = rdf_production.run_stage(self.spec, smoke_only=True)
        self.assertIn("test_types=data", marker.read_text())
        self.assertEqual((marker.parent / "worker_ran").read_text(), "submit_config_data.yaml")
        self.assertIn("test_job/test_passed.txt", rdf_production.status(marker.parent.parent)["records"])

    def test_local_production_executes_every_partition_with_the_worker_archive(self):
        self._assert_shared_filesystem_production("local")

    def test_dask_production_dispatches_every_partition_with_the_worker_archive(self):
        distributed = ModuleType("distributed")
        distributed.Client = MagicMock()
        distributed.as_completed = iter

        def submit(function, *args, pure):
            self.assertFalse(pure)
            future = Future()
            future.set_result(function(*args))
            return future

        client = distributed.Client.return_value
        client.submit.side_effect = submit
        with patch.dict(sys.modules, {"distributed": distributed}):
            self._assert_shared_filesystem_production("dask")
        distributed.Client.assert_called_once_with(n_workers=2, threads_per_worker=1)
        self.assertEqual(client.submit.call_count, 2)
        client.close.assert_called_once_with()

    def _assert_shared_filesystem_production(self, backend):
        from verified_outputs import load_verified_output_index, verify_output_signatures

        bundle = self.root / "local"
        bundle.mkdir()
        (bundle / "submit.yaml").write_text("enableSkim: false\n")
        (bundle / "datasets.yaml").write_text(
            "lumi: 1\ndatasets:\n" + "".join(
                f"- name: {sample}\n  dtype: data\n  files: [{self.root / 'input.root'}]\n"
                for sample in ("data_one", "data_two")
            )
        )
        seal_bundle(bundle)
        archive = self.root / "runtime.tar.gz"
        payload = b"VALUE = 'staged dependency'\n"
        with tarfile.open(archive, "w:gz") as tar:
            entry = tarfile.TarInfo("release_dependency.py")
            entry.size = len(payload)
            tar.addfile(entry, io.BytesIO(payload))
        (self.root / "analysis.py").write_text(
            "from pathlib import Path\nimport sys\nfrom release_dependency import VALUE\n"
            "import yaml\ncfg = yaml.safe_load(Path(sys.argv[1]).read_text())\n"
            "Path(cfg['metaFile']).write_text(VALUE + ' ' + cfg['sample'])\n"
        )
        self.data.pop("exe")
        self.data.update(backend=backend, submit_config="local/submit.yaml", dataset_manifest="local/datasets.yaml",
                         python={"entrypoint": "analysis.py", "interpreter": sys.executable})
        self.data["options"].pop("output_directory")
        self.data["options"]["python_env"] = archive.name
        if backend == "dask":
            self.data["options"]["dask_workers"] = 5
        self.write_spec()
        index = self.root / "index.json"
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError("unexpected submit")), \
             patch("rdfproduction.jobs.output_validation._skim_job_output_errors", return_value=[]):
            rdf_production.run_stage(self.spec, output_index=index, local_workers=2)
            first = index.read_bytes()
            rdf_production.run_stage(self.spec, output_index=index)
            self.assertEqual(first, index.read_bytes())
        result = load_verified_output_index(index)
        self.assertEqual(result["backend"], backend)
        self.assertEqual(result["partition_count"], 2)
        self.assertEqual(set(result["datasets"]), {"data_one", "data_two"})
        for sample, record in result["datasets"].items():
            job, = record["jobs"].values()
            self.assertEqual(Path(job["outputs"]["meta"]).read_text(), f"staged dependency {sample}")
        verify_output_signatures(result)
        self.assertFalse((self.root / "skimRun_fixture" / "submission_manifest.json").exists())
        self.assertFalse((self.root / "skimRun_fixture" / "test_job").exists())

    def test_dask_config_is_offline_and_preflight_reports_missing_dependency(self):
        options = self.data["options"]
        options.pop("output_directory")
        options.update(dask_scheduler="tcp://cluster.example:8786")
        self.write_spec()
        with self.assertRaisesRegex(ValueError, "require backend: dask"):
            rdf_production.inspect_spec(self.spec)
        self.data["backend"] = "dask"
        self.write_spec()
        self.assertEqual(rdf_production.inspect_spec(self.spec)["task_parameters"]["dask_scheduler"],
                         "tcp://cluster.example:8786")
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch.object(rdf_production, "import_module", side_effect=ImportError("no distributed")):
            with self.assertRaisesRegex(RuntimeError, r"requires dask\[distributed\]"):
                rdf_production.preflight(self.spec)
        self.assertFalse((self.root / "skimRun_fixture").exists())
        with self.assertRaisesRegex(ValueError, "submit is for HTCondor"):
            rdf_production.run_stage(self.spec, submit_only=True)

    def test_inspect_is_offline_and_binds_both_files_to_one_bundle(self):
        before = sorted(self.root.rglob("*"))
        with patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError):
            spec = rdf_production.inspect_spec(self.spec)
        self.assertEqual(sorted(self.root.rglob("*")), before)
        self.assertEqual(spec["task_parameters"]["dataset_manifest"], str(self.bundle / "datasets.yaml"))
        self.assertEqual(spec["task_parameters"]["output_validation"], "integrity")
        self.assertFalse(spec["task_parameters"]["sizing_pilots"])
        (self.root / "outsider.yaml").write_text("datasets: []")
        self.data["dataset_manifest"] = "outsider.yaml"
        self.write_spec()
        with self.assertRaisesRegex(ValueError, "outside the declared bundle"):
            rdf_production.inspect_spec(self.spec)

    def test_relative_state_root_preserves_law_state_and_is_shared_by_prepare_and_resume(self):
        import yaml

        self.data["state_root"] = "state/controller"
        self.write_spec()
        json_plan = rdf_production.inspect_spec(self.spec)
        self.spec = self.root / "batch.yaml"
        self.spec.write_text(yaml.safe_dump(self.data))
        self.assertEqual(rdf_production.inspect_spec(self.spec), json_plan)
        old_run = self.root / "skimRun_fixture"
        old_run.mkdir()
        (old_run / "law-marker").write_text("still owned")
        plan = rdf_production.inspect_spec(self.spec)
        state = self.root / "state/controller"
        self.assertEqual(plan["state_root"], str(state))
        self.assertFalse(state.exists())
        with patch.object(rdf_production, "_WORKSPACE", self.root):
            rdf_production.preflight(self.spec)
            rdf_production.run_stage(self.spec)
            rdf_production.run_stage(self.spec)
        self.assertEqual(json.loads((state / "skimRun_fixture/rdf_production.json").read_text()), plan)
        self.assertEqual([p.name for p in old_run.iterdir()], ["law-marker"])

    def test_repeated_run_claim_prevents_concurrency_and_reuse_with_changes(self):
        spec = rdf_production.inspect_spec(self.spec)
        with rdf_production._claim_run(self.root, spec) as run:
            with self.assertRaisesRegex(RuntimeError, "controller already owns"):
                with rdf_production._claim_run(self.root, spec):
                    self.fail("concurrent owner acquired")
            claim = (run / "rdf_production.json").read_bytes()
        with rdf_production._claim_run(self.root, spec):
            self.assertEqual((run / "rdf_production.json").read_bytes(), claim)
        self.data["options"]["request_memory"] = 3000
        self.write_spec()
        changed = rdf_production.inspect_spec(self.spec)
        with self.assertRaisesRegex(ValueError, "choose a new run name"):
            with rdf_production._claim_run(self.root, changed):
                self.fail("different configuration acquired the existing run")

    def test_existing_workflow_directory_is_left_alone(self):
        legacy = self.root / "skimRun_fixture"
        legacy.mkdir()
        with self.assertRaisesRegex(RuntimeError, "belongs to another workflow"):
            with rdf_production._claim_run(self.root, rdf_production.inspect_spec(self.spec)):
                self.fail("existing legacy run was acquired")
        self.assertEqual(list(legacy.iterdir()), [])

    def test_prepare_builds_concrete_batch_without_contacting_scheduler(self):
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.transport.collect_local_shared_libs", return_value={}), \
             patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError("unexpected submit")):
            result = rdf_production.run_stage(self.spec)
            self.assertEqual(rdf_production.run_stage(self.spec), result)
        manifest = json.loads(result.read_text())
        self.assertEqual(len(manifest["datasets"]), 1)
        dataset = manifest["datasets"][0]
        self.assertEqual((dataset["dataset_name"], dataset["job_count"]), ("data", 1))
        self.assertTrue(Path(dataset["submit_path"]).is_file())
        self.assertTrue((result.parent / "shared_inputs.tar.gz").is_file())
        self.assertEqual(rdf_production.status(result.parent)["recorded_partition_statuses"], {})

    def test_standalone_prepares_partitions_with_legacy_modules_blocked(self):
        datasets = self.bundle / "datasets.yaml"
        extra = self.root / "extra.root"
        extra.touch()
        datasets.write_text(datasets.read_text().replace("input.root]", f"input.root, {extra}]"))
        (self.bundle / "bundle_manifest.json").unlink()
        seal_bundle(self.bundle)
        self.data["options"].update(file_source="manifest", partition="file")
        self.write_spec()
        from rdfproduction import PYTHON_ROOT

        core = PYTHON_ROOT
        code = """
import sys
from pathlib import Path

class BlockLegacy:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'law', 'luigi', 'analysis_tasks', 'merge_tasks', 'combine_tasks',
            'manifest_plot_tasks', 'workflow_executors',
        }:
            raise ImportError(f'legacy workflow import forbidden: {fullname}')

sys.meta_path.insert(0, BlockLegacy())
import production_downstream
import production_stitching
import rdf_production

rdf_production._WORKSPACE = Path(sys.argv[2])
rdf_production.run_stage(Path(sys.argv[1]))
"""
        paths = [str(core), *[entry for entry in os.environ.get("PYTHONPATH", "").split(os.pathsep)
                              if entry and Path(entry).resolve() != core / "law"]]
        result = subprocess.run([sys.executable, "-c", code, str(self.spec), str(self.root)],
                                cwd=self.root, env={**os.environ, "PYTHONPATH": os.pathsep.join(paths)},
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        manifest = self.root / "skimRun_fixture" / "submission_manifest.json"
        dataset, = json.loads(manifest.read_text())["datasets"]
        self.assertEqual(dataset["job_count"], 2)
        self.assertFalse((manifest.parent / "submitted.json").exists())

    def test_sizing_pilot_spec_checks_mode_and_controls(self):
        options = self.data["options"]
        options.update(sizing_pilots=True, file_source="rucio")
        self.write_spec()
        with self.assertRaisesRegex(ValueError, "requires a file_source and partition"):
            rdf_production.inspect_spec(self.spec)
        options.update(partition="auto", sizing_pilot_safety_factor=0.5)
        self.write_spec()
        with self.assertRaisesRegex(ValueError, "sizing_pilot_safety_factor"):
            rdf_production.inspect_spec(self.spec)
        options.update(sizing_pilot_safety_factor=1.4, sizing_pilot_parallel=2,
                       sizing_pilot_target_seconds=900, sizing_pilot_prefix_entries=2500)
        self.write_spec()
        inspected = rdf_production.inspect_spec(self.spec)["task_parameters"]
        for key in ("sizing_pilots", "sizing_pilot_safety_factor", "sizing_pilot_parallel",
                    "sizing_pilot_target_seconds", "sizing_pilot_prefix_entries"):
            self.assertEqual(inspected[key], options[key])

    def test_prepare_with_sizing_pilots_stays_scheduler_free(self):
        self.data["options"].update(sizing_pilots=True, file_source="rucio", partition="auto")
        self.write_spec()
        with patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError):
            with self.assertRaisesRegex(ValueError, "sizing_pilots submits Condor jobs"):
                rdf_production.run_stage(self.spec)
            with self.assertRaisesRegex(ValueError, "sizing_pilots submits Condor jobs"):
                rdf_production.run_stage(self.spec, smoke_only=True)
        self.assertFalse((self.root / "skimRun_fixture").exists())

    def test_prepare_uses_learned_profile_to_determine_job_ranges(self):
        from job_sizing import EntryCache, seed_pilot_sizing

        self.data["options"].update(file_source="xrdfs", partition="auto")
        self.write_spec()
        parameters = rdf_production.inspect_spec(self.spec)["task_parameters"]
        source = str(self.root / "input.root")
        discovery = self.root / "xrdfsFileList_fixture"
        discovery.mkdir()
        (discovery / "data.json").write_text(json.dumps({"sample": "data", "files": [source]}))
        metadata = EntryCache()
        metadata.put(source, 2400)
        with patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.transport.collect_local_shared_libs", return_value={}), \
             patch("rdfproduction.partitioning._query_tree_entries", side_effect=AssertionError("unexpected input open")), \
             patch("rdfproduction.condor_submission.submit_skim_batches", side_effect=AssertionError("unexpected submit")), \
             patch.dict("os.environ", {"RDF_JOB_SIZING_CACHE": str(self.root / "cache"),
                                       "RDF_JOB_SIZING_ORIGIN": "test-pool"}):
            context = jobs.ProductionRun(**parameters, workspace=str(self.root))._job_sizing_context()
            seed_pilot_sizing(context, {"datasets": {"data": {"entries_per_job": 1000}}}, metadata.data)
            result = rdf_production.run_stage(self.spec)
        selection = json.loads((result.parent / "job_sizing_selection.json").read_text())
        self.assertEqual(selection["source"], "learned")
        dataset, = json.loads(result.read_text())["datasets"]
        self.assertEqual(dataset["job_count"], 3)
        configs = [jobs.read_config(str(Path(job["job_dir"]) / "submit_config.yaml")) for job in dataset["jobs"]]
        self.assertEqual([(int(cfg["firstEntry"]), int(cfg["lastEntry"])) for cfg in configs],
                         [(0, 1000), (1000, 2000), (2000, 2400)])

    def test_prepare_waits_for_verified_pilots_before_bulk_preparation(self):
        self.data["options"].update(sizing_pilots=True, file_source="rucio", partition="auto",
                                    sizing_pilot_parallel=2, sizing_pilot_prefix_entries=1250)
        self.write_spec()
        parent = jobs.ProductionRun(**rdf_production.inspect_spec(self.spec)["task_parameters"],
                                    workspace=str(self.root))
        plan = {"batches": [{"name": "pilot_child", "manifest": parent.dataset_manifest,
                             "datasets": ["data"], "output_directory": parent.output_directory + "/pilot"}]}
        order = []

        def condor(child):
            self.assertEqual(child.name, "pilot_child")
            self.assertFalse(child.sizing_pilots)
            self.assertEqual(child.partition, "file_group")
            order.append("execute pilot")

        with patch("production_discovery.discover_inputs", side_effect=lambda *a, **kw: order.append("discovery")), \
             patch.object(parent, "pilots_complete", return_value=False), \
             patch.object(parent, "_prepare_plan", return_value=plan), \
             patch.object(rdf_production, "_condor", side_effect=condor), \
             patch.object(jobs.ProductionRun, "monitoring_complete", return_value=True), \
             patch.object(parent, "collect_pilot_results", side_effect=lambda p: order.append("record pilots")), \
             patch.object(parent, "preparation_complete", return_value=False), \
             patch.object(parent, "prepare", side_effect=lambda: order.append("prepare production")):
            self.assertTrue(rdf_production._prepare(parent))
        self.assertEqual(order, ["discovery", "execute pilot", "record pilots", "prepare production"])

    def test_execute_uses_verified_outputs_and_reuses_submitted_batch(self):
        from verified_outputs import load_verified_output_index
        from xrd_site_rankings import MAGIC

        index = self.root / "index.json"
        cache = self.root / "ranking-cache.tsv"
        seed = self.root / "ranking-seed.tsv"
        seed.write_text(MAGIC + "\n")

        def complete_batch(record, _batch_id):
            logs = Path(record["submission_dir"]) / "condor_logs"
            logs.mkdir(exist_ok=True)
            (logs / "log_42_0.stdout").write_text(
                "[XrdAdaptor] context origin=franklin execution_host=franklin epoch=100000\n"
                "[XrdAdaptor] site-stat origin=franklin site=t2_test quality_ms=30 "
                "samples=20 read_errors=0 successful_bytes=2000000 epoch=100000\n"
            )
            return "42", "42.0 - 42.0"

        with patch.dict("os.environ", {"RDF_XRD_SITE_RANKING_CACHE": str(cache),
                                      "RDF_XRD_SITE_RANKING_FILE": str(seed),
                                      "RDF_XRD_SITE_RANKING_AUTO_UPDATE": "1"}), \
             patch.object(rdf_production, "_WORKSPACE", self.root), \
             patch("rdfproduction.transport.collect_local_shared_libs", return_value={}), \
             patch("condor_submission._submission_context", return_value={"owner": "fake"}), \
             patch("condor_submission._submit_batch", side_effect=complete_batch) as submit, \
             patch("rdfproduction.transport.condor_q_ads", return_value={}), \
             patch("rdfproduction.transport.condor_history_exit", return_value={0: 0}), \
             patch("rdfproduction.jobs.output_validation._output_file_info_batch",
                          side_effect=lambda paths: {path: {"size": 123, "mtime": 1} for path in paths}), \
             patch("rdfproduction.jobs.output_validation._skim_job_output_errors", return_value=[]) as validate, \
             patch.object(jobs.ProductionRun, "_update_job_sizing"):
            rdf_production.run_stage(self.spec, output_index=index)
            first = index.read_bytes()
            ranking_cache_mtime = cache.stat().st_mtime_ns
            rdf_production.run_stage(self.spec, output_index=index)
        output = load_verified_output_index(index)
        self.assertEqual(output["datasets"]["data"]["jobs"]["0"]["attempt"], ["42", 0, 0])
        self.assertEqual(index.read_bytes(), first)
        submit.assert_called_once()
        validate.assert_called_once()
        ranking = json.loads((self.root / "skimRun_fixture/xrd_site_rankings_update.json").read_text())
        self.assertEqual((ranking["status"], ranking["site_groups"], ranking["site_rows_changed"]),
                         ("updated", 1, 0))
        self.assertEqual(cache.stat().st_mtime_ns, ranking_cache_mtime)
        status = rdf_production.status(self.root / "skimRun_fixture")
        self.assertEqual(status["recorded_partition_statuses"], {"done": 1})
        self.assertEqual(status["recorded_submissions"]["data"]["cluster_id"], "42")
        self.assertEqual(status["recorded_output_index"]["path"], str(index))
        self.assertIn("all_outputs_verified.txt", status["records"])


if __name__ == "__main__":
    unittest.main()
