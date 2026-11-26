from pathlib import Path
"""Tests for YAML/text config read-write functions in submission_backend."""

import os
import tempfile
import pytest

from submission_backend import (
    read_config,
    write_config,
    get_config_extension,
    read_config_text,
    read_config_yaml,
)


def test_read_text_format():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
        f.write("# This is a comment\n")
        f.write("key1=value1\n")
        f.write("key2=value2 # inline comment\n")
        f.write("key3=value with spaces\n")
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1'
        assert result['key2'] == 'value2'
        assert result['key3'] == 'value with spaces'
    finally:
        os.unlink(fname)


def test_read_text_matches_cpp_pair_parser():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
        f.write("  sampleConfig = samples.txt  # inline comment\n")
        f.write("endpoint=root://host/path?token=a=b\n")
        f.write("empty=\n")
        f.write("not a config line\n")
    try:
        assert read_config_text(fname) == {
            'sampleConfig': 'samples.txt',
            'endpoint': 'root://host/path?token=a=b',
            'empty': '',
        }
    finally:
        os.unlink(fname)


def test_read_text_rejects_duplicate_keys():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
        f.write("repeat=first\nrepeat=second\n")
    try:
        with pytest.raises(ValueError, match="Duplicate key 'repeat'"):
            read_config_text(fname)
    finally:
        os.unlink(fname)


def test_read_yaml_format():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
        f.write("key1: value1\n")
        f.write("key2: value2\n")
        f.write("key3: value with spaces\n")
    try:
        result = read_config(fname)
        assert result['key1'] == 'value1'
        assert result['key2'] == 'value2'
        assert result['key3'] == 'value with spaces'
    finally:
        os.unlink(fname)


def test_yaml_collections_match_cpp_config_string_representation():
    fixture = os.path.join(
        os.path.dirname(__file__), "..", "cpp", "cfg", "yaml_conversion.yaml"
    )
    result = read_config_yaml(os.path.abspath(fixture))
    assert result["scalar"] == "quoted scalar"
    assert result["enabled"] == "true"
    assert result["labels"] == "[alpha, beta]"
    assert result["mapping"] == "{mode: fast, flags: [x, y]}"


def test_write_text_format_roundtrip():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname = f.name
    try:
        with pytest.raises(ValueError, match="must use .yaml"):
            write_config(test_data, fname)
        assert Path(fname).read_bytes() == b""
    finally:
        os.unlink(fname)


def test_write_yaml_format_roundtrip():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname = f.name
    try:
        write_config(test_data, fname)
        result = read_config(fname)
        assert result == test_data
    finally:
        os.unlink(fname)


def test_get_config_extension_txt():
    assert get_config_extension('config.txt') == '.txt'
    assert get_config_extension('/path/to/config.txt') == '.txt'


def test_get_config_extension_yaml():
    assert get_config_extension('config.yaml') == '.yaml'
    assert get_config_extension('/path/to/config.yml') == '.yaml'
    assert get_config_extension('config.yml') == '.yaml'


def test_text_yaml_consistency():
    test_data = {'key1': 'value1', 'key2': 'value2', 'key3': 'value with spaces'}
    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as f:
        fname_txt = f.name
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
        fname_yaml = f.name
    try:
        Path(fname_txt).write_text("key1=value1\nkey2=value2\nkey3=value with spaces\n")
        write_config(test_data, fname_yaml)
        assert read_config(fname_txt) == read_config(fname_yaml)
    finally:
        os.unlink(fname_txt)
        os.unlink(fname_yaml)


def test_native_scalar_spelling_and_yaml_errors(tmp_path):
    path = tmp_path / "settings.yaml"
    path.write_text("code: 00123\nflag: on\ndate: 2024-01-01\ncase: True\n")
    assert read_config(path) == {"code": "00123", "flag": "on", "date": "2024-01-01", "case": "True"}
    path.write_text("code: first\ncode: second\n")
    with pytest.raises(ValueError, match=r"settings.yaml:2: Duplicate key 'code'"):
        read_config(path)
    path.write_text("- threads\n")
    with pytest.raises(ValueError, match="expected a YAML mapping"):
        read_config(path)


def test_existing_job_config_selection_and_digest(tmp_path):
    from native_config_io import job_config_path, parse_config_bytes
    path = job_config_path(tmp_path)
    assert path.name == "submit_config.yaml"
    legacy = tmp_path / "submit_config.txt"
    legacy.write_text("sample=001\n")
    assert job_config_path(tmp_path) == legacy
    assert parse_config_bytes(legacy.read_bytes(), legacy) == {"sample": "001"}
    path.write_text("sample: 001\n")
    assert job_config_path(tmp_path) == path
    assert parse_config_bytes(path.read_bytes(), path) == {"sample": "001"}


def test_native_source_settings_accept_yaml_sequences():
    from pathlib import Path
    import yaml
    config = read_config(Path(__file__).parents[1] / "cpp" / "cfg" / "yaml_lists.yaml")
    assert config["unset"] == ""
    assert config["fileList"] == "/tmp/a.root,/tmp/b.root"
    assert config["treeList"] == "Events"
    assert config["antiglobs"] == ""
    assert yaml.safe_load(config["custom"]) == ["a,b", "b: c"]


if __name__ == '__main__':
    import sys
    sys.exit(pytest.main([__file__, '-v']))


@pytest.mark.parametrize("policy", ["adaptive", "legacy"])
def test_worker_yaml_and_archived_retries_preserve_policy_and_source_bytes(tmp_path, policy):
    import subprocess
    import yaml
    from native_config_io import materialize_runtime_config
    from rdfproduction.executors import _run_analysis_job
    from rdfproduction.jobs.job_config import _write_job_config
    from rdfproduction.jobs.condor_files import _prepare_split_skim_job
    from dataset_manifest import DatasetEntry
    from submission_backend import generate_condor_runscript, generate_condor_submit, materialize_retry_submit

    original = tmp_path / "job_0"
    original.mkdir()
    receipt = original / "submit_config.txt"
    saved = f"# old receipt\nfileList=a.root,b.root\nxrdReadPolicy={policy}\nsample=0012\nthreads=1\n"
    receipt.write_text(saved)
    execution = materialize_runtime_config(receipt)
    assert execution.name == "submit_config.runtime.yaml"
    assert yaml.safe_load(execution.read_bytes())["sample"] == "0012"
    assert materialize_runtime_config(execution) == execution
    fresh = tmp_path / "fresh"
    fresh.mkdir()
    _write_job_config(str(fresh), str(receipt), DatasetEntry(name="data", files=["a.root"], dtype="data"), str(tmp_path))
    assert yaml.safe_load((fresh / "submit_config.yaml").read_bytes())["xrdReadPolicy"] == policy
    assert not (fresh / "submit_config.txt").exists()
    local = tmp_path / "local"
    local.write_text('#!/bin/sh\ncp "$1" observed.yaml\n')
    local.chmod(0o755)
    _run_analysis_job(str(local), str(original))
    assert yaml.safe_load((original / "observed.yaml").read_bytes())["xrdReadPolicy"] == policy
    split = _prepare_split_skim_job(str(original), str(tmp_path), 0, 1, 0, ["a.root"])
    split_config = Path(split["dir"]) / "submit_config.yaml"
    sentinel = split_config.parent / "original_receipt.json"
    sentinel.write_bytes(b"historical result")
    assert _prepare_split_skim_job(str(original), str(tmp_path), 0, 1, 0, ["a.root"]) == split
    with pytest.raises(RuntimeError, match="different partition"):
        _prepare_split_skim_job(str(original), str(tmp_path), 0, 1, 0, ["b.root"])
    assert sentinel.read_bytes() == b"historical result"
    assert not split_config.with_suffix(".txt").exists()
    values = yaml.safe_load(split_config.read_bytes())
    assert values == {"fileList": "a.root", "xrdReadPolicy": policy, "sample": "0012", "threads": "1"}

    # Generated historical-style submit description accepted a text file in a
    # process-index directory. Preserve that description and both source files.
    exe = tmp_path / "condor_runscript.sh"
    exe.write_text(generate_condor_runscript("worker", False, False, "", config_file="submit_config.txt"))
    old_script = exe.read_bytes()
    template = generate_condor_submit(str(tmp_path), 1, "worker", config_file="submit_config.txt")
    migrated = materialize_retry_submit(template, tmp_path, split_config)
    assert "submit_config.txt" not in migrated
    assert "rdf_worker_yaml.zip" in migrated
    input_line = next(l for l in migrated.splitlines() if l.lower().startswith("transfer_input_files"))
    retry_config = next(Path(p.strip()) for p in input_line.split("=", 1)[1].split(",")
                        if p.strip().endswith("/submit_config.yaml"))
    worker = retry_config.parent / "worker"
    worker.write_text('#!/bin/sh\ncp "$1" observed.yaml\n')
    worker.chmod(0o755)
    subprocess.run(["bash", "condor_runscript.sh"], cwd=retry_config.parent, check=True, capture_output=True)
    assert yaml.safe_load((retry_config.parent / "observed.yaml").read_text()) == values
    assert exe.read_bytes() == old_script
    assert receipt.read_bytes() == saved.encode()


def test_bundle_converts_legacy_recipe_without_transcoding_model(tmp_path):
    from run_bundle import compile_config_bundle
    import yaml
    (tmp_path / "bdt.txt").write_bytes(b"trained model payload\n")
    (tmp_path / "models.txt").write_text("file=bdt.txt name=score inputVariables=Jet_pt runVar=ready\n")
    (tmp_path / "worker.txt").write_text("bdtConfig=models.txt\nxrdReadPolicy=adaptive\n")
    out = tmp_path / "sealed"
    compile_config_bundle(tmp_path / "worker.txt", out, asset_keys=["bdtConfig"])
    config = yaml.safe_load((out / "submit_config.yaml").read_bytes())
    recipe = out / config["bdtConfig"]
    assert recipe.suffix == ".yaml"
    assert config["xrdReadPolicy"] == "adaptive"
    payload = out / yaml.safe_load(recipe.read_bytes())[0]["file"]
    assert payload.suffix == ".txt"
    assert payload.read_bytes() == (tmp_path / "bdt.txt").read_bytes()
