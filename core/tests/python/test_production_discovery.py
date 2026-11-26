"""Behavior of scheduler-neutral input inventory materialization."""

import json
from pathlib import Path
import subprocess
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from production_discovery import discover_inputs


class TestProductionDiscovery(unittest.TestCase):
    def setUp(self):
        scratch = TemporaryDirectory()
        self.addCleanup(scratch.cleanup)
        self.root = Path(scratch.name)
        self.manifest = self.root / "datasets.yaml"

    def test_named_inventory_reuses_other_filename_and_preserves_other_datasets(self):
        self.manifest.write_text("datasets:\n  - name: selected\n")
        directory = self.root / "rucioFileList_prior"
        directory.mkdir()
        saved = directory / "arbitrary.json"
        saved.write_text('{"sample":"selected","files":["root://eos//selected.root"]}')
        extra = directory / "extra.json"
        extra.write_text('{"sample":"extra","files":["root://eos//extra.root"]}')
        with patch("production_discovery.load_extension_module") as load:
            result = discover_inputs(self.root, "current", "rucio", self.manifest, file_source_name="prior")
            self.assertEqual(result, directory)
            with self.assertRaisesRegex(ValueError, "named discovery.*missing dataset"):
                discover_inputs(self.root, "current", "rucio", self.manifest, file_source_name="absent")
            load.assert_not_called()
        self.assertEqual({path.name for path in directory.iterdir()}, {"arbitrary.json", "extra.json"})

    def test_explicit_files_require_no_catalogue_provider(self):
        self.manifest.write_text("datasets:\n  - name: local\n    files: [/tmp/local.root]\n")
        with patch("production_discovery.load_extension_module") as load:
            directory = discover_inputs(self.root, "test", "rucio", self.manifest)
            load.assert_not_called()
        self.assertEqual(json.loads((directory / "local.json").read_text()),
                         {"sample": "local", "files": ["/tmp/local.root"]})

    def test_rucio_queries_use_provider_and_preserve_groups_and_redirectors(self):
        self.manifest.write_text(
            "whitelist: [first-site]\n"
            "blacklist: [second-site]\n"
            "datasets:\n"
            "  - name: sample\n"
            "    site: preferred-site\n"
            "    das: /A/first/NANOAOD,/A/second/NANOAOD\n"
        )
        calls = {}
        url = "root://xrootd.example//store/"
        client = object()

        def query(path, size, allow, block, site, used_client, **options):
            calls[path] = (size, allow, block, site, used_client, options)
            groups = {0: url + "shared.root"}
            if path == "/A/second/NANOAOD":
                groups[1] = url + "other.root"
            return {"groups": groups,
                    "site_redirectors": {"/store/shared.root": ["root://preferred/"]}}

        provider = SimpleNamespace(get_rucio_client=lambda: client, query_rucio=query)
        with patch("production_discovery.load_extension_module", return_value=provider) as load:
            directory = discover_inputs(self.root, "test", "rucio", self.manifest, files_per_job=2,
                                        rucio_group_size_gb=1.5)
            load.assert_called_once_with("cms", "rucio_discovery", required=True)
        self.assertEqual(list(calls.values()), [
            (1.5, ["first-site"], ["second-site"], "preferred-site", client,
             {"max_files_per_group": 2, "raise_on_error": True}),
        ] * 2)
        self.assertEqual(json.loads((directory / "sample.json").read_text()), {
            "sample": "sample", "files": [url + "shared.root", url + "other.root"],
            "groups": [url + "shared.root", url + "other.root"],
            "site_redirectors": {"/store/shared.root": ["root://preferred/"]},
        })

    def test_xrdfs_fails_without_publishing_incomplete_tree(self):
        self.manifest.write_text("datasets:\n  - name: sample\n    xrdfs_path: /dataset\n")
        calls = []

        def list_dir(command, **options):
            directory = command[-1]
            calls.append(directory)
            if directory == "/dataset/child":
                raise subprocess.CalledProcessError(9, command, stderr="permission denied")
            return SimpleNamespace(stdout="- /dataset/parent.root\nd /dataset/child\n")

        with patch("production_discovery.subprocess.run", side_effect=list_dir):
            with self.assertRaisesRegex(RuntimeError, "/dataset/child.*permission denied"):
                discover_inputs(self.root, "test", "xrdfs", self.manifest,
                                xrdfs_server="root://redirector.example/")
        self.assertEqual(calls, ["/dataset", "/dataset/child"])
        self.assertFalse((self.root / "xrdfsFileList_test" / "sample.json").exists())

    def test_opendata_uses_record_ids_and_filters_shared_records_by_sample_key(self):
        self.manifest.write_text(
            "datasets:\n"
            "  - name: first\n"
            "    das: '42'\n"
            "  - name: second\n"
            "    das: '42'\n"
            "    opendata_key: alias\n"
            "  - name: independent\n"
            "    das: '43'\n"
        )
        records = {
            "42": [{"files": [{"key": "first_file_index", "uri": "/eos/first.root"},
                               {"key": "alias_file_index", "uri": "/eos/second.root"}]}],
            "43": [{"files": [{"key": "other_file_index", "uri": "/eos/other.root"}]}],
        }
        with patch("opendata_discovery.load_file_indices", side_effect=records.__getitem__) as load:
            directory = discover_inputs(self.root, "test", "opendata", self.manifest)
            self.assertEqual(load.call_count, 2)
        for sample, filename in {"first": "first", "second": "second", "independent": "other"}.items():
            payload = json.loads((directory / f"{sample}.json").read_text())
            self.assertEqual(payload["files"], [f"root://eospublic.cern.ch//eos/{filename}.root"])


if __name__ == "__main__":
    unittest.main()
