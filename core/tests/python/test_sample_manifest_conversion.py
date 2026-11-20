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


def test_integer_year_tags_only():
    from convert_samples_to_manifest import _parse_lumi_map

    import unittest

    with unittest.TestCase().assertRaisesRegex(ValueError, "annual year:lumi"):
        _parse_lumi_map("2022postEE:8.1")
    assert _parse_lumi_map("2024:10,2024B:3,2024.5:4") == {2024: 10.0}


def test_string_false_and_invalid_flag(tmp_path):
    import unittest

    source = tmp_path / "samples.json"
    output = tmp_path / "manifest.yaml"
    metadata = {"year": "2030B", "isMC": "False"}
    sample = {"data": {"files": [{"das_names": ["/data"], "metadata": metadata}]}}
    source.write_text(json.dumps(sample))
    convert(source, output, {})
    entry, = DatasetManifest.load_yaml(str(output)).datasets
    assert (entry.dtype, entry.group, entry.era) == ("data", "data", "2030B")

    original = output.read_bytes()
    metadata["isMC"] = "unknown"
    source.write_text(json.dumps(sample))
    with unittest.TestCase().assertRaisesRegex(ValueError, "invalid isMC"):
        convert(source, output, {})
    assert output.read_bytes() == original


def test_duplicate_dataset_preserves_existing_output(tmp_path):
    import unittest

    source = tmp_path / "samples.json"
    output = tmp_path / "manifest.yaml"
    block = {"das_names": ["/one"], "metadata": {"year": "2030"}}
    source.write_text(json.dumps({"sample": {"files": [block, block]}}))
    output.write_bytes(b"previous manifest")
    with unittest.TestCase().assertRaisesRegex(ValueError, "Duplicate dataset name"):
        convert(source, output, {})
    assert output.read_bytes() == b"previous manifest"


def test_atomic_replacement_through_relative_symlink(tmp_path):
    import stat

    source = tmp_path / "samples.json"
    source.write_text("{}")
    directory = tmp_path / "targets"
    directory.mkdir()
    target = directory / "current"
    target.write_bytes(b"original")
    target.chmod(0o640)
    output = tmp_path / "manifest.yaml"
    output.symlink_to("targets/current")

    with target.open("rb") as previous:
        convert(source, output, {})
        assert previous.read() == b"original"
    assert str(output.readlink()) == "targets/current"
    assert stat.S_IMODE(target.stat().st_mode) == 0o640
    assert DatasetManifest.load_yaml(str(output)).datasets == []
    assert list(directory.iterdir()) == [target]


def test_dangling_symlink_creates_target_with_normal_mode(tmp_path):
    import os
    import stat

    source = tmp_path / "samples.json"
    source.write_text("{}")
    output = tmp_path / "manifest.yaml"
    output.symlink_to("target")
    control = tmp_path / "ordinary-file"
    previous_mask = os.umask(0o027)
    try:
        control.touch()
        convert(source, output, {})
    finally:
        os.umask(previous_mask)
    assert str(output.readlink()) == "target"
    assert stat.S_IMODE(output.stat().st_mode) == stat.S_IMODE(control.stat().st_mode)


def test_replace_failure_preserves_destination_and_cleans_candidate(tmp_path):
    from pathlib import Path
    from unittest import TestCase, mock

    source = tmp_path / "samples.json"
    source.write_text("{}")
    target = tmp_path / "manifest.yaml"
    target.write_bytes(b"original")
    with mock.patch.object(Path, "replace", side_effect=PermissionError):
        with TestCase().assertRaises(PermissionError):
            convert(source, target, {})
    assert target.read_bytes() == b"original"
    assert set(tmp_path.iterdir()) == {target, source}
