import os
import sys
import tempfile
import unittest
from unittest.mock import patch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

import convert_config


class TestConvertConfig(unittest.TestCase):
    def test_text_to_yaml_conversion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input.txt")
            output_path = os.path.join(tmpdir, "output.yaml")
            with open(input_path, "w") as fh:
                fh.write("saveFile=test_output.root\nthreads=1\n")

            with patch.object(sys, "argv", ["convert_config.py", input_path, output_path]):
                convert_config.main()

            self.assertTrue(os.path.exists(output_path))
            with open(output_path) as fh:
                content = fh.read()
            self.assertIn("saveFile: test_output.root", content)
            self.assertIn("threads: '1'", content)

    def test_yaml_to_text_output_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input.yaml")
            output_path = os.path.join(tmpdir, "output.txt")
            with open(input_path, "w") as fh:
                fh.write("saveFile: test_output.root\nthreads: 1\n")

            with patch.object(sys, "argv", ["convert_config.py", input_path, output_path]):
                with self.assertRaises(SystemExit):
                    convert_config.main()
            self.assertFalse(os.path.exists(output_path))

    def test_missing_input_file_returns_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = os.path.join(tmpdir, "missing.txt")
            output_path = os.path.join(tmpdir, "output.yaml")
            with patch.object(sys, "argv", ["convert_config.py", missing_path, output_path]):
                with self.assertRaises(SystemExit) as exc:
                    convert_config.main()
                self.assertEqual(exc.exception.code, 1)

    def test_record_list_and_ambiguous_field(self):
        from pathlib import Path
        import yaml
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "input.txt", Path(directory) / "output.yaml"
            for kind, old, expected in [
                ("records", "name=mu bins=20", [{"name": "mu", "bins": "20"}]),
                ("list", "Muon_* # keep glob", ["Muon_*"]),
            ]:
                source.write_text(old)
                with patch.object(sys, "argv", ["convert_config.py", "--kind", kind, str(source), str(output)]):
                    convert_config.main()
                self.assertEqual(yaml.safe_load(output.read_text()), expected)
            previous = output.read_bytes()
            source.write_text("name=mu label=Muon momentum")
            with patch.object(sys, "argv", ["convert_config.py", "--kind", "records", str(source), str(output)]):
                with self.assertRaises(SystemExit):
                    convert_config.main()
            self.assertEqual(previous, output.read_bytes())

    def test_sample_manifest_preserves_base_dirs_and_selection(self):
        from pathlib import Path
        from sample_config import load_sample_config
        with tempfile.TemporaryDirectory() as directory:
            source, output = Path(directory) / "input.txt", Path(directory) / "datasets.yaml"
            source.write_text("prefix_cern=/sample/base\nlumi=3.5\nWL=T2_US\nrecids=123,789\nname=Muon dtype=data das=123 ptcut=25\n")
            with patch.object(sys, "argv", ["convert_config.py", "--kind", "samples", str(source), str(output)]):
                convert_config.main()
            sample = load_sample_config(str(output))
            self.assertEqual(sample.base_dirs, ["/sample/base"])
            self.assertEqual(sample.lumi, 3.5)
            self.assertEqual(sample.whitelist, ["T2_US"])
            self.assertEqual(list(sample.entries), ["Muon"])
            self.assertEqual(sample.entries["Muon"].extra["ptcut"], "25")
            from opendata_discovery import parse_opendata_config
            self.assertEqual(parse_opendata_config(str(output))[1], ["123", "789"])
