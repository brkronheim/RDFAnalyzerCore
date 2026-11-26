"""Exercise the public skim paths across interrupted publication and retry."""
import json
import os
import shutil
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from test_merge_tasks import _LAW_AVAILABLE, _SKIP_MSG, _make_skim_manifest


@unittest.skipUnless(_LAW_AVAILABLE, _SKIP_MSG)
class SkimPublication(unittest.TestCase):
    def test_failure_and_retry_at_each_commit_boundary(self):
        import uproot
        from merge_tasks import MergeSkims, OutputManifest

        for boundary in ("stage manifest", "b.root", "merge_skims.perf.json", "merged_skim_groups.json",
                         "output_manifest.yaml"):
            with self.subTest(boundary=boundary), tempfile.TemporaryDirectory() as directory:
                inputs = Path(directory, "inputs")
                task = MergeSkims(name="publication", input_dir=str(inputs), output_dir=directory)
                manifest = Path(task.output().path)
                names = ("a.root", "b.root")
                for name in names:
                    _make_skim_manifest(str(inputs / name), output_file=name)

                def write_inputs(generation):
                    for name in names:
                        with uproot.recreate(inputs / name / name) as root:
                            root.mktree("Events", {"generation": "int32"})
                            root["Events"].extend({"generation": [generation]})

                def generations():
                    result = []
                    for name in names:
                        with uproot.open(manifest.parent / name,
                                         handler=uproot.source.file.MemmapSource) as root:
                            result.extend(root["Events"]["generation"].array(library="np"))
                    return result

                replace = os.replace

                def inject(source, destination):
                    if Path(destination).parent == manifest.parent:
                        self.assertFalse(task.complete())
                        if Path(destination).name == boundary:
                            raise OSError("injected publication failure")
                    return replace(source, destination)

                def copy_group(destination, sources):
                    shutil.copyfile(sources[0], destination)

                with patch("rdfproduction.manifest_merge._run_hadd", side_effect=copy_group):
                    write_inputs(1)
                    task.run()
                    self.assertTrue(task.complete())
                    write_inputs(2)
                    if boundary == "stage manifest":
                        with patch.object(OutputManifest, "save_yaml", side_effect=OSError("stage")):
                            with self.assertRaisesRegex(OSError, "stage"):
                                task.run()
                        self.assertTrue(task.complete())
                        self.assertEqual(generations(), [1, 1])
                    else:
                        with patch("rdfproduction.manifest_merge.os.replace", side_effect=inject):
                            with self.assertRaisesRegex(OSError, "injected publication"):
                                task.run()
                        self.assertFalse(manifest.exists())
                        self.assertFalse(task.complete())
                        expected = [2, 1] if boundary == "b.root" else [2, 2]
                        self.assertEqual(generations(), expected)
                    task.run()
                    self.assertTrue(task.complete())
                    self.assertEqual(generations(), [2, 2])
                    index = manifest.with_name("merged_skim_groups.json")
                    self.assertEqual(json.loads(index.read_text()),
                                     {name: str(manifest.parent / name) for name in names})
