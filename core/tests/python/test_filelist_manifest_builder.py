from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from core.python.dataset_manifest import DatasetEntry, DatasetManifest


def test_build_manifest_from_filelist_dir(tmp_path):
    manifest = DatasetManifest(
        datasets=[
            DatasetEntry(
                name="sampleA",
                year=2022,
                dtype="mc",
                process="zh",
                group="signal",
                das="/A/B/C",
            )
        ],
        lumi=1.0,
    )
    base_manifest = tmp_path / "base.yaml"
    manifest.save_yaml(str(base_manifest))

    filelist_dir = tmp_path / "filelists"
    filelist_dir.mkdir()
    with (filelist_dir / "sampleA.json").open("w") as handle:
        json.dump(
            {"sample": "sampleA", "files": ["root://cmsxrootd.fnal.gov//store/foo.root"]},
            handle,
        )

    output_manifest = tmp_path / "out.yaml"
    repo_root = Path(__file__).resolve().parents[3]
    subprocess.run(
        [
            sys.executable,
            str(repo_root / "core/python/law/build_manifest_from_filelist_dir.py"),
            "--base-manifest",
            str(base_manifest),
            "--filelist-dir",
            str(filelist_dir),
            "--output",
            str(output_manifest),
        ],
        check=True,
    )

    out = DatasetManifest.load_yaml(str(output_manifest))
    entry = out.by_name("sampleA")
    assert entry is not None
    assert entry.files == ["root://cmsxrootd.fnal.gov//store/foo.root"]
    assert entry.das is None
