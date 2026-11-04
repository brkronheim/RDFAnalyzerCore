import json
from convert_samples_to_manifest import convert
from dataset_manifest import DatasetManifest


def test_manifest_format_and_analysis_supplied_policy(tmp_path):
    source = tmp_path / "samples.json"
    source.write_text(json.dumps({"arbitrary": {"sample": "NewProcess", "files": [
        {"das_names": ["/one", "/two"], "metadata": {"year": "2030_custom", "isMC": True, "xsec": 2.5}},
        {"das_names": ["/data"], "metadata": {"year": "2030B", "isMC": False}},
    ]}}))
    output = tmp_path / "manifest.yaml"
    convert(source, output, {2030: 123.0},
            process_classifier=lambda name: "custom_process",
            group_classifier=lambda process, is_mc: "custom_group" if is_mc else "observed")
    manifest = DatasetManifest.load_yaml(str(output))
    assert len(manifest.datasets) == 2
    assert manifest.datasets[0].process == "custom_process"
    assert manifest.datasets[0].group == "custom_group"
    assert manifest.datasets[0].das == "/one,/two"
    assert manifest.datasets[0].xsec == 2.5
    assert manifest.datasets[1].dtype == "data"
    assert manifest.datasets[1].era == "2030B"
    assert manifest.datasets[1].group == "observed"
