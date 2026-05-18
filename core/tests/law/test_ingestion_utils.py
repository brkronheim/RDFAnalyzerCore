from __future__ import annotations

import os
import sys
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from ingestion_utils import append_unique_lines, normalize_config_paths, rechunk_urls, read_failed_job_error


def test_append_unique_lines_deduplicates(tmp_path):
    path = tmp_path / "lines.txt"
    path.write_text("a\na\nb\n")

    append_unique_lines(str(path), ["b", "c", "c"])

    assert path.read_text().splitlines() == ["a", "b", "c"]


def test_normalize_config_paths_keeps_non_paths():
    config = {
        "config": "cfg/sub/config.yaml",
        "other": "nochange",
        "not_a_config": "value.txt",
    }
    normalized = normalize_config_paths(config)
    assert normalized["config"] == "config.yaml"
    assert normalized["other"] == "nochange"
    assert normalized["not_a_config"] == "value.txt"


def test_rechunk_urls_splits_into_groups():
    urls = [f"url{i}" for i in range(5)]
    groups = rechunk_urls(urls, max_files=2)
    assert groups[0] == "url0,url1"
    assert groups[1] == "url2,url3"
    assert groups[2] == "url4"


def test_read_failed_job_error_prefers_existing_stderr(tmp_path):
    condor_logs = tmp_path / "condor_logs"
    condor_logs.mkdir(parents=True)
    stderr_path = condor_logs / "log_1_2.stderr"
    stderr_path.write_text("line1\nline2\n")

    result = read_failed_job_error(str(tmp_path), "1", 2, 0)
    assert result == "line1\nline2"
