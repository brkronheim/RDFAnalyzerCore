from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import json
import subprocess

from xrdfs_tasks import GetXRDFSFileList, xrdfs_list_files


def test_xrdfs_list_files_returns_matching_roots(monkeypatch):
    class FakeCompletedProcess:
        def __init__(self, returncode, stdout):
            self.returncode = returncode
            self.stdout = stdout
            self.stderr = ""

    def fake_run(cmd, capture_output, text, timeout):
        return FakeCompletedProcess(
            returncode=0,
            stdout="""
            drwxr-xr-x  2 root root 4096 Jan 1 12:00 /eos/foo/bar
            -rw-r--r--  1 root root 123 Jan 1 12:01 /eos/foo/bar/event1.root
            -rw-r--r--  1 root root 456 Jan 1 12:02 /eos/foo/bar/event2.root
            """,
        )

    monkeypatch.setattr(subprocess, "run", fake_run)
    files = xrdfs_list_files(
        server="root://eosuser.cern.ch/",
        remote_path="/eos/foo/bar",
        pattern="*.root",
        recursive=False,
        timeout=10,
        max_workers=2,
    )

    assert files == ["root://eosuser.cern.ch/eos/foo/bar/event1.root", "root://eosuser.cern.ch/eos/foo/bar/event2.root"]


def test_get_xrdfs_file_list_branch_map(tmp_path):
    manifest = tmp_path / "datasets.yaml"
    manifest.write_text(
        "lumi: 1.0\ndatasets:\n  - name: datasetA\n"
    )
    task = GetXRDFSFileList(name="test", dataset_manifest=str(manifest))
    branch_map = task.create_branch_map()

    assert isinstance(branch_map, dict)
    assert 0 in branch_map
    assert branch_map[0].name == "datasetA"
