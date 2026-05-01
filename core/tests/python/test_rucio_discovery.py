from __future__ import annotations

import os
import sys
import textwrap
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import pytest
import rucio_discovery


def test_load_sample_config_yaml(tmp_path):
    manifest = tmp_path / "datasets.yaml"
    manifest.write_text(textwrap.dedent(
        """
        lumi: 12.3
        whitelist: [T1]
        blacklist: [T2]
        datasets:
          - name: sampleA
            year: 2023
        """
    ))

    data = rucio_discovery.load_sample_config(str(manifest))
    assert data.lumi == 12.3
    assert data.whitelist == ["T1"]
    assert data.blacklist == ["T2"]
    assert list(data.entries) == ["sampleA"]
    assert data.entries["sampleA"].name == "sampleA"


def test_load_legacy_sample_config(tmp_path):
    legacy = tmp_path / "legacy.txt"
    legacy.write_text(
        "name=legacy_sample prefix_cern=/store\n"
        "lumi=10\n"
        "WL=T1,T2\n"
        "BL=T3\n"
    )
    data = rucio_discovery.load_legacy_sample_config(str(legacy))
    assert data.lumi == 10.0
    assert data.entries["legacy_sample"].name == "legacy_sample"
    assert data.whitelist == ["T1", "T2"]
    assert data.blacklist == ["T3"]


def test_load_sample_config_txt_warns(tmp_path):
    legacy = tmp_path / "legacy.txt"
    legacy.write_text("name=legacy_sample prefix_cern=/store/lumi=1 WL=T1 BL=T2\n")
    with pytest.warns(DeprecationWarning):
        data = rucio_discovery.load_sample_config(str(legacy))
    assert data.entries["legacy_sample"].name == "legacy_sample"


def test_query_rucio_groups_and_retry(monkeypatch):
    class FakeClient:
        def __init__(self):
            self.calls = 0

        def list_replicas(self, dataset):
            self.calls += 1
            if self.calls == 1:
                raise rucio_discovery.RequestException("temporary failure")
            return [
                {"name": "root://cms-xrd-global.cern.ch//store/mc/part1.root", "bytes": 1e9},
                {"name": "root://cms-xrd-global.cern.ch//store/mc/part2.root", "bytes": 1e9},
            ]

    monkeypatch.setattr(rucio_discovery.time, "sleep", lambda *_: None)
    result = rucio_discovery.query_rucio(
        "/A/B/C",
        file_split_gb=1.1,
        client=FakeClient(),
        max_files_per_group=2,
    )

    assert result["groups"] == {
        0: "root://cms-xrd-global.cern.ch//store/mc/part1.root",
        1: "root://cms-xrd-global.cern.ch//store/mc/part2.root",
    }


def test_query_rucio_invalid_dataset_returns_empty():
    result = rucio_discovery.query_rucio("not_a_path", file_split_gb=1.0, client=object())
    assert result == {"groups": {}, "site_redirectors": {}}
