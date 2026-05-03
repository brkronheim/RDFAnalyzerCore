import json
import os
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

import opendata_discovery


class TestOpenDataDiscovery(unittest.TestCase):
    def test_parse_opendata_config_collects_recids_and_files(self):
        manifest = {
            "lumi": 12.34,
            "datasets": [
                {"name": "sample_with_das", "das": "123, 456"},
                {"name": "sample_with_files", "files": ["root://eospublic.cern.ch//eos/opendata/cms/foo.root"]},
            ],
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as fh:
            json.dump(manifest, fh)
            config_path = fh.name
        try:
            samples, recids, lumi = opendata_discovery.parse_opendata_config(config_path)
            self.assertEqual(lumi, 12.34)
            self.assertIn("sample_with_das", samples)
            self.assertIn("sample_with_files", samples)
            self.assertEqual(recids, ["123", "456"])
        finally:
            os.remove(config_path)

    def test_parse_opendata_config_rejects_non_yaml_files(self):
        with self.assertRaises(ValueError):
            opendata_discovery.parse_opendata_config("config.txt")

    @patch("opendata_discovery.shutil.which", return_value=None)
    @patch("opendata_discovery.requests.get")
    def test_load_file_indices_uses_api_fallback(self, mock_get, mock_which):
        response = MagicMock()
        response.ok = True
        response.json.return_value = {"metadata": {"_file_indices": [{"files": []}]}}
        mock_get.return_value = response

        result = opendata_discovery.load_file_indices("12345")
        self.assertEqual(result, [{"files": []}])

    @patch.object(opendata_discovery, "load_file_indices", return_value=[
        {"files": [{"uri": "/eos/opendata/cms/foo.root", "key": "cms_dataset_file_index"}]}
    ])
    def test_process_metadata_maps_sample_names(self, mock_load_file_indices):
        sample_names = {"cms_dataset": "my_sample"}
        result = opendata_discovery.process_metadata("12345", sample_names, redirector="root://test/")
        self.assertEqual(result["my_sample"], ["root://test//eos/opendata/cms/foo.root"])
