from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import version_info


def test_get_git_hash_returns_none_for_none():
    assert version_info.get_git_hash(None) is None


def test_get_git_root_and_hash(monkeypatch):
    class FakeCompletedProcess:
        def __init__(self, stdout):
            self.stdout = stdout

    def fake_run(cmd, capture_output, text, check):
        return FakeCompletedProcess(stdout="/repo/root\n")

    monkeypatch.setattr(subprocess, "run", fake_run)
    root = version_info.get_git_root("/repo/root/some/path")
    assert root == "/repo/root"
    git_hash = version_info.get_git_hash("/repo/root")
    assert git_hash == "/repo/root"


def test_get_config_mtime(tmp_path):
    config_file = tmp_path / "config.txt"
    config_file.write_text("content")
    mtime = version_info.get_config_mtime(str(config_file))
    assert mtime is not None
    assert "+00:00" in mtime


def test_write_version_info_json(tmp_path):
    output_path = tmp_path / "version_info.json"
    version_info.write_version_info_json(
        str(output_path),
        {"framework_hash": "abc", "user_repo_hash": "def", "config_mtime": "2026-01-01T00:00:00+00:00"},
    )
    assert output_path.exists()
    loaded = json.loads(output_path.read_text())
    assert loaded["framework_hash"] == "abc"
    assert loaded["user_repo_hash"] == "def"
