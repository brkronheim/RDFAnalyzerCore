"""Sealed configurations stage their full dependency closure without live aux."""
from pathlib import Path
import tempfile
import unittest
import sys
import yaml

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/"core/python"), str(ROOT/"core/python/law")]
from run_bundle import compile_config_bundle, verify_bundle
from analysis_tasks import _write_job_config, read_config, _staged_copy_is_current
import analysis_tasks
from dataset_manifest import DatasetEntry


class BundleStagingTests(unittest.TestCase):
    def test_preparation_reuses_archive_rewrites_with_identical_job_contents(self):
        from collections import Counter
        from unittest.mock import patch
        import tarfile

        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/"payload.json").write_text('{"calibration": 3}')
            (root/"trigger.yaml").write_text('- file: payload.json\n')
            (root/"model.txt").write_text('model file=payload.json\n')
            (root/"datasets.yaml").write_text(
                'lumi: 1\ndatasets:\n'
                '- {name: data, dtype: data, files: [input.root]}\n'
                '- {name: mc1, dtype: mc, files: [input.root]}\n'
                '- {name: mc2, dtype: mc, files: [input.root]}\n')
            (root/"submit.yaml").write_text(
                'sampleConfig: datasets.yaml\ntriggerConfig: trigger.yaml\n'
                'modelConfig: model.txt\nenableSkim: false\n')
            bundle = root/"bundle"
            compile_config_bundle(root/"submit.yaml", bundle,
                                  asset_keys=["sampleConfig", "triggerConfig", "modelConfig"])
            template = str(bundle/"submit_config.yaml")
            executable = root/"analysis"
            executable.write_text('#!/bin/sh\nexit 0\n')
            executable.chmod(0o755)
            with patch.object(analysis_tasks, "WORKSPACE", str(root)), \
                 patch.object(analysis_tasks, "_collect_skim_shared_libs", return_value={}), \
                 patch.object(analysis_tasks, "_rewrite_staged_analysis_config",
                              wraps=analysis_tasks._rewrite_staged_analysis_config) as rewrite:
                task = analysis_tasks.PrepareSkimJobs(name="cached", exe=str(executable),
                    submit_config=template, dataset_manifest=str(bundle/"dataset_manifest.yaml"),
                    make_test_job=False, prepare_workers=3)
                task._run_impl()
                # Every shared payload is rewritten once, before the parallel
                # data/MC jobs are created, rather than once per job.
                sources = [call.kwargs["source_config_path"] for call in rewrite.call_args_list]
                self.assertTrue(sources)
                self.assertTrue(all(count == 1 for count in Counter(sources).values()))
                with tarfile.open(root/"skimRun_cached/shared_inputs.tar.gz") as archive:
                    archived = {Path(member.name).name: archive.extractfile(member).read()
                                for member in archive.getmembers()
                                if member.isfile() and member.name.startswith("cfg/")}
                manifest = analysis_tasks.DatasetManifest.load(str(bundle/"dataset_manifest.yaml"))
                for dataset in manifest.datasets:
                    job = root/"skimRun_cached/jobs"/dataset.name
                    config = read_config(str(job/"submit_config.txt"))
                    before = {path.name: path.read_bytes() for path in (job/"cfg").iterdir()}
                    self.assertEqual(before, archived)
                    # Re-run the uncached writer at the same paths to compare
                    # complete config and payload bytes, including metadata.
                    config_bytes = (job/"submit_config.txt").read_bytes()
                    _write_job_config(str(job), template, dataset, config["saveDirectory"],
                                      extra_overrides=config, stage_shared_inputs_locally=False)
                    self.assertEqual(config_bytes, (job/"submit_config.txt").read_bytes())
                    self.assertEqual(before, {p.name: p.read_bytes() for p in (job/"cfg").iterdir()})
                    trigger = yaml.safe_load((job/config["triggerConfig"]).read_text())
                    self.assertTrue((job/trigger[0]["file"]).is_file())
                # A new preparation must build a fresh cache, even in the same process.
                rewrite.reset_mock()
                task._run_impl()
                self.assertEqual(len(sources), rewrite.call_count)
            verify_bundle(bundle)

    def test_changed_execution_cannot_refresh_an_existing_runs_workers(self):
        from unittest.mock import patch
        from run_bundle import BundleError
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            (root/"datasets.yaml").write_text('lumi: 1\ndatasets:\n- name: data\n  dtype: data\n  files: [input.root]\n')
            (root/"submit.yaml").write_text('sampleConfig: datasets.yaml\nenableSkim: false\n')
            bundle=root/"bundle"
            compile_config_bundle(root/"submit.yaml",bundle,asset_keys=["sampleConfig"])
            executable=root/"analysis"
            executable.write_text('#!/bin/sh\nexit 0\n')
            executable.chmod(0o755)
            with patch.object(analysis_tasks,"WORKSPACE",str(root)), \
                 patch.object(analysis_tasks,"_collect_skim_shared_libs",return_value={}):
                task=analysis_tasks.PrepareSkimJobs(name="sealed",exe=str(executable),
                    submit_config=str(bundle/"submit_config.yaml"),
                    dataset_manifest=str(bundle/"dataset_manifest.yaml"),make_test_job=False)
                task._run_impl()
                self.assertTrue((root/"skimRun_sealed/execution_plan.json").is_file())
                staged=root/"skimRun_sealed/shared_inputs/analysis"
                before=staged.read_bytes()
                executable.write_text('#!/bin/sh\nexit 1\n')
                with self.assertRaisesRegex(BundleError,"new run name"):
                    task._run_impl()
                self.assertEqual(staged.read_bytes(),before)

    def test_nested_assets_and_bundle_identity_reach_workers(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            (root/"payload.json").write_text('{"calibration": 3}')
            (root/"trigger.yaml").write_text('- file: payload.json\n')
            (root/"submit.yaml").write_text('triggerConfig: trigger.yaml\nenableSkim: false\n')
            bundle = root/"bundle"
            compile_config_bundle(root/"submit.yaml", bundle, asset_keys=["triggerConfig"])
            job = root/"job"
            _write_job_config(str(job), str(bundle/"submit_config.yaml"),
                DatasetEntry(name="sample", files=["input.root"], dtype="mc"), str(root/"output"))
            config = read_config(str(job/"submit_config.txt"))
            self.assertEqual(config["runBundleId"],verify_bundle(bundle)["bundle_id"])
            triggers = yaml.safe_load((job/config["triggerConfig"]).read_text())
            self.assertTrue((job/triggers[0]["file"]).is_file())
            self.assertFalse((job/"aux").exists())
            # The remote/local executor layout has a cfg/ prefix instead of
            # the direct-run flat layout; both must close nested references.
            second=root/"worker"
            _write_job_config(str(second), str(bundle/"submit_config.yaml"),
                DatasetEntry(name="sample", files=["input.root"], dtype="mc"), str(root/"output"),
                stage_shared_inputs_locally=False)
            worker_config=read_config(str(second/"submit_config.txt"))
            nested=yaml.safe_load((second/worker_config["triggerConfig"]).read_text())
            self.assertTrue((second/nested[0]["file"]).is_file())
            verify_bundle(bundle)

    def test_equal_size_and_mtime_cannot_hide_a_changed_executable(self):
        import os
        with tempfile.TemporaryDirectory() as tmp:
            first, second = Path(tmp)/"first", Path(tmp)/"second"
            first.write_text("aaaa")
            second.write_text("bbbb")
            stat=first.stat()
            os.utime(second, ns=(stat.st_atime_ns,stat.st_mtime_ns))
            self.assertFalse(_staged_copy_is_current(first, second))


if __name__ == "__main__":
    unittest.main()
