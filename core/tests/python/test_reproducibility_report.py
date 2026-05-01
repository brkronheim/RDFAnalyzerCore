"""
Tests for core/python/reproducibility_report.py.

Covers:
- ReproducibilityReport construction (empty and with provenance)
- Structured section accessors: framework, analysis, environment, configuration,
  file_hashes, dataset_manifest, plugins, task_metadata, other
- Serialisation: to_dict, to_yaml, to_json, to_text
- Deserialisation: from_dict (full and bare-flat), load_yaml, load_json round-trips
- File I/O: save_yaml, save_json, save_text
- build_report_from_provenance convenience function
- load_provenance_from_root: missing file, missing uproot, missing directory,
  and happy-path using a mock uproot interface
- CLI main() behaviour
"""
from __future__ import annotations

import json
import os
import sys
import textwrap

import pytest
import yaml

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from reproducibility_report import (
    REPRODUCIBILITY_REPORT_VERSION,
    ReproducibilityReport,
    build_report_from_provenance,
    load_provenance_from_root,
    main,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

SAMPLE_PROVENANCE: dict = {
    "framework.git_hash": "abcdef1234567890",
    "framework.git_dirty": "false",
    "framework.build_timestamp": "2024-01-15T10:00:00Z",
    "framework.compiler": "GNU 13.2.0",
    "root.version": "6.30/02",
    "analysis.git_hash": "fedcba0987654321",
    "analysis.git_dirty": "true",
    "env.container_tag": "myimage:1.0",
    "executor.num_threads": "4",
    "config.hash": "deadbeef12345678",
    "filelist.hash": "cafebabe87654321",
    "file.hash.correctionFile": "aabbccdd11223344",
    "file.hash.modelFile": "11223344aabbccdd",
    "dataset_manifest.file_hash": "manifest_hash_value",
    "dataset_manifest.query_params": '{"sample": "ttbar"}',
    "dataset_manifest.resolved_entries": "ttbar_2022A,ttbar_2022B",
    "plugin.histogramManager.version": "3",
    "plugin.histogramManager.config_hash": "histo_cfg_hash",
    "plugin.weightManager.nominal_weight": "weight_nominal",
    "plugin.weightManager.config_hash": "weight_cfg_hash",
    "task.sample_name": "ttbar",
    "task.output_tag": "v2",
}


def _make_report() -> ReproducibilityReport:
    return ReproducibilityReport(provenance=SAMPLE_PROVENANCE)


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


def test_empty_report_has_empty_provenance():
    report = ReproducibilityReport()
    assert report.provenance == {}
    assert len(report.framework) == 0
    assert len(report.plugins) == 0
    assert report.report_version == REPRODUCIBILITY_REPORT_VERSION


def test_report_version_constant():
    assert REPRODUCIBILITY_REPORT_VERSION == 1


def test_construction_from_provenance_dict():
    report = _make_report()
    assert report.provenance["framework.git_hash"] == "abcdef1234567890"
    assert len(report.provenance) == len(SAMPLE_PROVENANCE)


def test_timestamp_defaults_to_current_time():
    import datetime
    before = datetime.datetime.now(tz=datetime.timezone.utc).isoformat(timespec="seconds")
    report = ReproducibilityReport()
    after = datetime.datetime.now(tz=datetime.timezone.utc).isoformat(timespec="seconds")
    assert before <= report.timestamp <= after


def test_explicit_timestamp_is_preserved():
    ts = "2024-06-01T12:00:00+00:00"
    report = ReproducibilityReport(timestamp=ts)
    assert report.timestamp == ts


# ---------------------------------------------------------------------------
# Section accessors
# ---------------------------------------------------------------------------


def test_framework_section():
    report = _make_report()
    fw = report.framework
    assert fw["git_hash"] == "abcdef1234567890"
    assert fw["git_dirty"] == "false"
    assert fw["build_timestamp"] == "2024-01-15T10:00:00Z"
    assert fw["compiler"] == "GNU 13.2.0"
    # root.version is folded into framework
    assert fw["version"] == "6.30/02"


def test_analysis_section():
    report = _make_report()
    an = report.analysis
    assert an["git_hash"] == "fedcba0987654321"
    assert an["git_dirty"] == "true"


def test_environment_section():
    report = _make_report()
    env = report.environment
    assert env["container_tag"] == "myimage:1.0"
    assert env["num_threads"] == "4"


def test_configuration_section():
    report = _make_report()
    cfg = report.configuration
    assert cfg["hash"] == "deadbeef12345678"
    # filelist.hash is folded in with a disambiguating prefix
    assert cfg["filelist_hash"] == "cafebabe87654321"


def test_file_hashes_section():
    report = _make_report()
    fh = report.file_hashes
    assert fh["correctionFile"] == "aabbccdd11223344"
    assert fh["modelFile"] == "11223344aabbccdd"


def test_dataset_manifest_section():
    report = _make_report()
    dm = report.dataset_manifest
    assert dm["file_hash"] == "manifest_hash_value"
    assert dm["query_params"] == '{"sample": "ttbar"}'
    assert dm["resolved_entries"] == "ttbar_2022A,ttbar_2022B"


def test_plugins_section_structure():
    report = _make_report()
    plugins = report.plugins
    assert "histogramManager" in plugins
    assert "weightManager" in plugins
    assert plugins["histogramManager"]["version"] == "3"
    assert plugins["histogramManager"]["config_hash"] == "histo_cfg_hash"
    assert plugins["weightManager"]["nominal_weight"] == "weight_nominal"
    assert plugins["weightManager"]["config_hash"] == "weight_cfg_hash"


def test_task_metadata_section():
    report = _make_report()
    tm = report.task_metadata
    assert tm["sample_name"] == "ttbar"
    assert tm["output_tag"] == "v2"


def test_other_section_empty_for_known_entries():
    report = _make_report()
    # All entries in SAMPLE_PROVENANCE have known prefixes
    assert report.other == {}


def test_other_section_captures_unknown_entries():
    prov = dict(SAMPLE_PROVENANCE)
    prov["unknown.custom.key"] = "some_value"
    report = ReproducibilityReport(provenance=prov)
    assert "unknown.custom.key" in report.other
    assert report.other["unknown.custom.key"] == "some_value"


def test_plugins_section_with_no_subkeys():
    """A plugin entry recorded as plugin.<role> only (no sub-keys)."""
    prov = {"plugin.myPlugin": "MyPluginType"}
    report = ReproducibilityReport(provenance=prov)
    # The entry has no dot after the role, so it lands in 'other' (not under plugin)
    # because the role is the whole suffix and sub_key is empty → no inner entry
    # The plugin dict will have an empty inner dict for 'myPlugin'
    assert "myPlugin" in report.plugins
    assert report.plugins["myPlugin"] == {}


# ---------------------------------------------------------------------------
# to_dict
# ---------------------------------------------------------------------------


def test_to_dict_has_required_keys():
    report = _make_report()
    d = report.to_dict()
    assert d["report_version"] == REPRODUCIBILITY_REPORT_VERSION
    assert "timestamp" in d
    assert "summary" in d
    assert "framework" in d
    assert "analysis" in d
    assert "environment" in d
    assert "configuration" in d
    assert "file_hashes" in d
    assert "dataset_manifest" in d
    assert "plugins" in d
    assert "task_metadata" in d
    assert "other" in d
    assert "provenance" in d


def test_to_dict_summary_counts():
    report = _make_report()
    s = report.to_dict()["summary"]
    assert s["n_total_entries"] == len(SAMPLE_PROVENANCE)
    assert s["n_plugin_roles"] == 2
    assert s["n_file_hash_entries"] == 2
    assert s["n_task_metadata_entries"] == 2


def test_to_dict_provenance_flat_map_preserved():
    report = _make_report()
    d = report.to_dict()
    for k, v in SAMPLE_PROVENANCE.items():
        assert d["provenance"][k] == v


def test_to_dict_plugins_nested():
    report = _make_report()
    d = report.to_dict()
    assert d["plugins"]["histogramManager"]["version"] == "3"
    assert d["plugins"]["weightManager"]["config_hash"] == "weight_cfg_hash"


# ---------------------------------------------------------------------------
# to_yaml
# ---------------------------------------------------------------------------


def test_to_yaml_is_valid_yaml():
    report = _make_report()
    text = report.to_yaml()
    parsed = yaml.safe_load(text)
    assert isinstance(parsed, dict)
    assert parsed["report_version"] == REPRODUCIBILITY_REPORT_VERSION


def test_to_yaml_round_trip_via_from_dict():
    report = _make_report()
    text = report.to_yaml()
    parsed = yaml.safe_load(text)
    report2 = ReproducibilityReport.from_dict(parsed)
    assert report2.provenance == report.provenance
    assert report2.timestamp == report.timestamp


# ---------------------------------------------------------------------------
# to_json
# ---------------------------------------------------------------------------


def test_to_json_is_valid_json():
    report = _make_report()
    text = report.to_json()
    parsed = json.loads(text)
    assert isinstance(parsed, dict)
    assert parsed["report_version"] == REPRODUCIBILITY_REPORT_VERSION


def test_to_json_round_trip():
    report = _make_report()
    text = report.to_json()
    parsed = json.loads(text)
    report2 = ReproducibilityReport.from_dict(parsed)
    assert report2.provenance == report.provenance


def test_to_json_custom_indent():
    report = _make_report()
    text4 = report.to_json(indent=4)
    assert "    " in text4
    parsed = json.loads(text4)
    assert parsed["report_version"] == REPRODUCIBILITY_REPORT_VERSION


# ---------------------------------------------------------------------------
# to_text
# ---------------------------------------------------------------------------


def test_to_text_contains_title():
    report = _make_report()
    text = report.to_text()
    assert "REPRODUCIBILITY REPORT" in text


def test_to_text_contains_framework_section():
    report = _make_report()
    text = report.to_text()
    assert "FRAMEWORK PROVENANCE" in text
    assert "abcdef1234567890" in text


def test_to_text_contains_plugin_section():
    report = _make_report()
    text = report.to_text()
    assert "PLUGIN PROVENANCE" in text
    assert "histogramManager" in text
    assert "weightManager" in text


def test_to_text_contains_task_section():
    report = _make_report()
    text = report.to_text()
    assert "TASK METADATA" in text
    assert "ttbar" in text


def test_to_text_contains_file_hashes():
    report = _make_report()
    text = report.to_text()
    assert "FILE HASHES" in text
    assert "correctionFile" in text


def test_to_text_contains_dataset_manifest():
    report = _make_report()
    text = report.to_text()
    assert "DATASET MANIFEST" in text
    assert "manifest_hash_value" in text


def test_to_text_empty_report_does_not_crash():
    report = ReproducibilityReport()
    text = report.to_text()
    assert "REPRODUCIBILITY REPORT" in text
    assert "END OF REPORT" in text


def test_to_text_ends_with_end_marker():
    report = _make_report()
    text = report.to_text()
    assert "END OF REPORT" in text


def test_to_text_no_optional_sections_when_empty():
    """Sections with no entries should not appear when provenance is empty."""
    report = ReproducibilityReport()
    text = report.to_text()
    assert "FILE HASHES" not in text
    assert "DATASET MANIFEST" not in text
    assert "PLUGIN PROVENANCE" not in text
    assert "TASK METADATA" not in text
    assert "OTHER ENTRIES" not in text


# ---------------------------------------------------------------------------
# from_dict
# ---------------------------------------------------------------------------


def test_from_dict_full_round_trip():
    report = _make_report()
    d = report.to_dict()
    report2 = ReproducibilityReport.from_dict(d)
    assert report2.provenance == report.provenance
    assert report2.timestamp == report.timestamp
    assert report2.report_version == report.report_version


def test_from_dict_bare_flat_map():
    """from_dict should accept a bare flat provenance dict directly."""
    flat = {"framework.git_hash": "abc", "config.hash": "def"}
    report = ReproducibilityReport.from_dict(flat)
    assert report.provenance["framework.git_hash"] == "abc"
    assert report.provenance["config.hash"] == "def"


def test_from_dict_ignores_none_values():
    d = {
        "report_version": 1,
        "timestamp": "2024-01-01T00:00:00+00:00",
        "provenance": {
            "framework.git_hash": "abc",
            "framework.git_dirty": None,
        },
    }
    report = ReproducibilityReport.from_dict(d)
    assert "framework.git_hash" in report.provenance
    # None values are dropped
    assert "framework.git_dirty" not in report.provenance


def test_from_dict_missing_provenance_key_falls_back():
    """from_dict with no 'provenance' key treats the whole dict as flat map."""
    d = {"framework.git_hash": "zzz"}
    report = ReproducibilityReport.from_dict(d)
    assert report.provenance["framework.git_hash"] == "zzz"


# ---------------------------------------------------------------------------
# File I/O – save_yaml / save_json / save_text / load_yaml / load_json
# ---------------------------------------------------------------------------


def test_save_and_load_yaml(tmp_path):
    report = _make_report()
    p = str(tmp_path / "repro.yaml")
    report.save_yaml(p)
    assert os.path.isfile(p)
    report2 = ReproducibilityReport.load_yaml(p)
    assert report2.provenance == report.provenance


def test_save_and_load_json(tmp_path):
    report = _make_report()
    p = str(tmp_path / "repro.json")
    report.save_json(p)
    assert os.path.isfile(p)
    report2 = ReproducibilityReport.load_json(p)
    assert report2.provenance == report.provenance


def test_save_text(tmp_path):
    report = _make_report()
    p = str(tmp_path / "repro.txt")
    report.save_text(p)
    assert os.path.isfile(p)
    with open(p, encoding="utf-8") as fh:
        content = fh.read()
    assert "REPRODUCIBILITY REPORT" in content


def test_save_yaml_creates_parent_dirs(tmp_path):
    report = _make_report()
    p = str(tmp_path / "nested" / "dir" / "repro.yaml")
    report.save_yaml(p)
    assert os.path.isfile(p)


def test_load_yaml_raises_on_invalid_yaml(tmp_path):
    p = str(tmp_path / "bad.yaml")
    with open(p, "w") as fh:
        fh.write("- not a mapping\n")
    with pytest.raises(ValueError, match="must be a YAML mapping"):
        ReproducibilityReport.load_yaml(p)


def test_load_json_raises_on_invalid_json(tmp_path):
    p = str(tmp_path / "bad.json")
    with open(p, "w") as fh:
        fh.write("[1, 2, 3]")
    with pytest.raises(ValueError, match="must be a JSON object"):
        ReproducibilityReport.load_json(p)


# ---------------------------------------------------------------------------
# build_report_from_provenance
# ---------------------------------------------------------------------------


def test_build_report_from_provenance_returns_report():
    prov = {"framework.git_hash": "abc", "config.hash": "xyz"}
    report = build_report_from_provenance(prov)
    assert isinstance(report, ReproducibilityReport)
    assert report.provenance == prov


def test_build_report_from_provenance_with_timestamp():
    ts = "2025-01-01T00:00:00+00:00"
    report = build_report_from_provenance({}, timestamp=ts)
    assert report.timestamp == ts


def test_build_report_from_empty_provenance():
    report = build_report_from_provenance({})
    assert report.provenance == {}
    text = report.to_text()
    assert "REPRODUCIBILITY REPORT" in text


# ---------------------------------------------------------------------------
# load_provenance_from_root
# ---------------------------------------------------------------------------


def test_load_provenance_from_root_missing_file():
    with pytest.raises(FileNotFoundError, match="not found"):
        load_provenance_from_root("/nonexistent/path/meta.root")


def test_load_provenance_from_root_raises_import_error_if_uproot_absent(
    tmp_path, monkeypatch
):
    """If uproot is not installed the function should raise ImportError."""
    import importlib
    import builtins

    real_import = builtins.__import__

    def mock_import(name, *args, **kwargs):
        if name == "uproot":
            raise ImportError("uproot not available")
        return real_import(name, *args, **kwargs)

    p = tmp_path / "fake.root"
    p.write_bytes(b"")
    monkeypatch.setattr(builtins, "__import__", mock_import)
    with pytest.raises(ImportError, match="uproot"):
        load_provenance_from_root(str(p))


def test_load_provenance_from_root_missing_provenance_dir(tmp_path, monkeypatch):
    """Returns empty dict when file has no 'provenance' directory."""
    p = tmp_path / "meta.root"
    p.write_bytes(b"")

    class FakeTNamed:
        def __init__(self, title):
            self.title = title

    class FakeRootFile:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def __contains__(self, name):
            return False  # no 'provenance' key
        def __getitem__(self, name):
            raise KeyError(name)
        def keys(self):
            return []

    class FakeUproot:
        @staticmethod
        def open(path):
            return FakeRootFile()

    import sys
    monkeypatch.setitem(sys.modules, "uproot", FakeUproot)
    result = load_provenance_from_root(str(p))
    assert result == {}


def test_load_provenance_from_root_happy_path(tmp_path, monkeypatch):
    """Happy-path: uproot returns TNamed objects from the provenance dir."""
    p = tmp_path / "meta.root"
    p.write_bytes(b"")

    class FakeTNamed:
        def __init__(self, title):
            self.title = title

    class FakeProvDir:
        def keys(self):
            return ["framework.git_hash;1", "config.hash;1", "plugin.h.version;1"]
        def __getitem__(self, key):
            data = {
                "framework.git_hash;1": FakeTNamed("abc123"),
                "config.hash;1": FakeTNamed("deadbeef"),
                "plugin.h.version;1": FakeTNamed("2"),
            }
            return data[key]

    class FakeRootFile:
        def __enter__(self):
            return self
        def __exit__(self, *_):
            pass
        def __contains__(self, name):
            return name == "provenance"
        def __getitem__(self, name):
            if name == "provenance":
                return FakeProvDir()
            raise KeyError(name)

    class FakeUproot:
        @staticmethod
        def open(path):
            return FakeRootFile()

    import sys
    monkeypatch.setitem(sys.modules, "uproot", FakeUproot)
    result = load_provenance_from_root(str(p))
    assert result["framework.git_hash"] == "abc123"
    assert result["config.hash"] == "deadbeef"
    assert result["plugin.h.version"] == "2"


# ---------------------------------------------------------------------------
# CLI main()
# ---------------------------------------------------------------------------


def test_main_no_args_returns_1():
    assert main([]) == 1


def test_main_missing_root_file_returns_1():
    assert main(["/nonexistent/file.root"]) == 1


def test_main_load_yaml_prints_report(tmp_path, capsys):
    report = _make_report()
    p = str(tmp_path / "repro.yaml")
    report.save_yaml(p)
    rc = main(["--load-yaml", p, "--quiet"])
    assert rc == 0


def test_main_load_yaml_saves_outputs(tmp_path, capsys):
    report = _make_report()
    src = str(tmp_path / "repro.yaml")
    report.save_yaml(src)

    out_yaml = str(tmp_path / "out.yaml")
    out_json = str(tmp_path / "out.json")
    out_text = str(tmp_path / "out.txt")

    rc = main(["--load-yaml", src, "--yaml", out_yaml, "--json", out_json,
               "--text", out_text, "--quiet"])
    assert rc == 0
    assert os.path.isfile(out_yaml)
    assert os.path.isfile(out_json)
    assert os.path.isfile(out_text)


def test_main_load_json(tmp_path, capsys):
    report = _make_report()
    src = str(tmp_path / "repro.json")
    report.save_json(src)
    rc = main(["--load-json", src, "--quiet"])
    assert rc == 0


def test_main_prints_report_to_stdout(tmp_path, capsys):
    report = _make_report()
    src = str(tmp_path / "repro.yaml")
    report.save_yaml(src)
    rc = main(["--load-yaml", src])
    assert rc == 0
    captured = capsys.readouterr()
    assert "REPRODUCIBILITY REPORT" in captured.out


def test_main_load_yaml_invalid_file_returns_1(tmp_path):
    p = str(tmp_path / "bad.yaml")
    with open(p, "w") as fh:
        fh.write("- list not mapping\n")
    assert main(["--load-yaml", p]) == 1


# ---------------------------------------------------------------------------
# Plugin accessor – edge cases
# ---------------------------------------------------------------------------


def test_plugins_empty_when_no_plugin_entries():
    report = ReproducibilityReport(provenance={"framework.git_hash": "x"})
    assert report.plugins == {}


def test_plugins_multiple_roles():
    prov = {
        "plugin.roleA.key1": "val1",
        "plugin.roleA.key2": "val2",
        "plugin.roleB.key1": "val3",
    }
    report = ReproducibilityReport(provenance=prov)
    assert set(report.plugins.keys()) == {"roleA", "roleB"}
    assert report.plugins["roleA"]["key1"] == "val1"
    assert report.plugins["roleA"]["key2"] == "val2"
    assert report.plugins["roleB"]["key1"] == "val3"


# ---------------------------------------------------------------------------
# Configuration section – filelist.hash folded in
# ---------------------------------------------------------------------------


def test_configuration_includes_filelist_hash():
    prov = {"config.hash": "abc", "filelist.hash": "xyz"}
    report = ReproducibilityReport(provenance=prov)
    cfg = report.configuration
    assert cfg["hash"] == "abc"
    assert cfg["filelist_hash"] == "xyz"


def test_configuration_without_filelist():
    prov = {"config.hash": "myhash"}
    report = ReproducibilityReport(provenance=prov)
    assert report.configuration["hash"] == "myhash"
