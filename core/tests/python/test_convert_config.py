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

    def test_yaml_to_text_conversion(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            input_path = os.path.join(tmpdir, "input.yaml")
            output_path = os.path.join(tmpdir, "output.txt")
            with open(input_path, "w") as fh:
                fh.write("saveFile: test_output.root\nthreads: 1\n")

            with patch.object(sys, "argv", ["convert_config.py", input_path, output_path]):
                convert_config.main()

            self.assertTrue(os.path.exists(output_path))
            with open(output_path) as fh:
                content = fh.read()
            self.assertIn("saveFile=test_output.root", content)
            self.assertIn("threads=1", content)

    def test_missing_input_file_returns_error(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            missing_path = os.path.join(tmpdir, "missing.txt")
            output_path = os.path.join(tmpdir, "output.yaml")
            with patch.object(sys, "argv", ["convert_config.py", missing_path, output_path]):
                with self.assertRaises(SystemExit) as exc:
                    convert_config.main()
                self.assertEqual(exc.exception.code, 1)
