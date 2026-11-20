"""Local regression cases for merge completion and public cache resolution."""
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_merge_tasks import _LAW_AVAILABLE, _SKIP_MSG, _make_skim_manifest
from output_schema import (
    ArtifactResolutionStatus, OutputManifest, SkimSchema, check_cache_validity,
    write_cache_sidecar,
)


class CacheValidation(unittest.TestCase):
    def test_incompatible_envelope_and_damaged_sidecars(self):
        with tempfile.TemporaryDirectory() as directory:
            artifact = Path(directory, "skim.root")
            artifact.touch()
            manifest = OutputManifest(skim=SkimSchema(output_file=str(artifact)),
                                      manifest_version=999)
            sidecar = Path(write_cache_sidecar(str(artifact), manifest))
            for content in (sidecar.read_text(), "manifest: [", "{}", "manifest: null"):
                with self.subTest(content=content):
                    sidecar.write_text(content)
                    self.assertEqual(check_cache_validity(str(artifact)),
                                     ArtifactResolutionStatus.MUST_REGENERATE)


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class MergeValidation(unittest.TestCase):
    def test_metadata_from_skim_only_validates_once_and_completes(self):
        from merge_tasks import MergeMetadata
        with tempfile.TemporaryDirectory() as directory:
            inputs = Path(directory, "inputs")
            _make_skim_manifest(str(inputs / "job"))
            task = MergeMetadata(name="test", input_dir=str(inputs), output_dir=directory)
            with patch.object(OutputManifest, "validate", autospec=True,
                              side_effect=OutputManifest.validate) as validate:
                task.run()
                self.assertEqual(validate.call_count, 1)
            self.assertTrue(task.complete())
            summary = Path(task.output().path).with_name("provenance_summary.json")
            summary.write_text("{}")
            self.assertFalse(task.complete())
            summary.unlink()
            self.assertFalse(task.complete())

    def test_one_sided_pileup_variation_in_prediction(self):
        from merge_tasks import MergeHistograms
        from test_merge_tasks import _make_histogram_manifest
        with tempfile.TemporaryDirectory() as directory:
            inputs = Path(directory, "inputs")
            path = _make_histogram_manifest(str(inputs / "job"))
            manifest = OutputManifest.load_yaml(path)
            manifest.histograms.axis_labels = {"systematic": ["pileupDown"]}
            manifest.save_yaml(path)
            (Path(path).parent / manifest.histograms.output_file).touch()
            task = MergeHistograms(name="test", input_dir=str(inputs), output_dir=directory)
            with patch("merge_tasks.read_embedded_histogram_schema", return_value=None), \
                    patch("merge_tasks._find_hadd"), patch("merge_tasks.merge_root_files"), \
                    patch.object(task, "publish_message") as messages:
                task.run()
            messages.assert_any_call("Pre-merge coverage prediction: 1 systematic "
                                     "base(s) detected, 0 with both up+down variations.")

    def test_invalid_coverage_precedes_physical_merge(self):
        from merge_tasks import MergeSkims
        from output_schema import MergeInputValidationError
        with tempfile.TemporaryDirectory() as directory:
            inputs = Path(directory, "inputs")
            path = _make_skim_manifest(str(inputs / "job"))
            manifest = OutputManifest.load_yaml(path)
            manifest.coverage_report = {"selected": "invalid"}
            manifest.save_yaml(path)
            task = MergeSkims(name="test", input_dir=str(inputs), output_dir=directory)
            with patch("merge_tasks._run_hadd") as hadd:
                with self.assertRaisesRegex(MergeInputValidationError, "non-numeric"):
                    task.run()
                hadd.assert_not_called()
            self.assertFalse(Path(task.output().path).exists())

    def test_failed_second_skim_group_preserves_published_files(self):
        from merge_tasks import MergeSkims
        with tempfile.TemporaryDirectory() as directory:
            inputs = Path(directory, "inputs")
            output = Path(directory, "skims")
            output.mkdir()
            for name in ("a", "b"):
                filename = name + ".root"
                _make_skim_manifest(str(inputs / name), output_file=filename)
                (inputs / name / filename).touch()
                (output / filename).write_bytes(b"previous")
            task = MergeSkims(name="test", input_dir=str(inputs), output_dir=directory)

            def failing_hadd(destination, paths):
                Path(destination).write_bytes(b"replacement")
                if Path(destination).name == "b.root":
                    raise RuntimeError("injected failure")

            with patch("merge_tasks._run_hadd", side_effect=failing_hadd):
                with self.assertRaisesRegex(RuntimeError, "injected failure"):
                    task.run()
            self.assertEqual(sorted(p.name for p in output.iterdir()), ["a.root", "b.root"])
            self.assertTrue(all(p.read_bytes() == b"previous" for p in output.iterdir()))

    def test_group_index_must_list_primary(self):
        import uproot
        from merge_tasks import MergeSkims
        with tempfile.TemporaryDirectory() as directory:
            task = MergeSkims(name="test", output_dir=directory)
            output = Path(task.output().path).parent
            _make_skim_manifest(str(output))
            with uproot.recreate(output / "skim.root") as root:
                root.mktree("Events", {"x": "float64"})
            groups = output / "merged_skim_groups.json"
            for value, expected in (({"skim.root": "skim.root"}, True),
                                    ({}, False), ([], False)):
                groups.write_text(json.dumps(value))
                self.assertEqual(task.complete(), expected)
