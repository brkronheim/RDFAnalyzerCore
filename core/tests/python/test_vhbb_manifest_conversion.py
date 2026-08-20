from __future__ import annotations

import json

from core.python.convert_vhbb_samples_to_manifest import convert
from core.python.dataset_manifest import DatasetManifest


def test_convert_vhbb_samples_to_manifest(tmp_path):
    payload = {
        "ZHto2L2B": {
            "sample": "ZHto2L2B",
            "files": [
                {
                    "das_names": ["/ZH/Run3/NANOAODSIM"],
                    "metadata": {"year": "2022_preEE", "isMC": True, "xsec": 0.12},
                }
            ],
        }
    }
    samples_path = tmp_path / "samples.json"
    with samples_path.open("w") as handle:
        json.dump(payload, handle)

    output = tmp_path / "datasets.yaml"
    convert(samples_path, output, {2022: 8.077})

    manifest = DatasetManifest.load_yaml(str(output))
    assert len(manifest.datasets) == 1
    entry = manifest.datasets[0]
    assert entry.name == "ZHto2L2B_2022_preEE"
    assert entry.das == "/ZH/Run3/NANOAODSIM"
    assert entry.dtype == "mc"
    assert entry.year == 2022
    assert entry.process == "zh"
