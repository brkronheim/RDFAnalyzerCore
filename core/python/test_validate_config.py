"""
Tests for validate_config.py – focusing on the YAML manifest integration
added by the dataset-manifest adoption audit.

Covers:
- _validate_yaml_sample_config for NANO and OpenData modes (auto-detect,
  explicit, valid, and invalid inputs)
- validate_submit_config routing to YAML validation when sampleConfig is a
  YAML file
- _parse_opendata_config (in generateSubmissionFilesOpenData) for both legacy
  text format and YAML manifest format
"""
from __future__ import annotations

import os
import sys
import textwrap
import tempfile

import pytest

# Make core/python importable when running from repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from validate_config import _validate_yaml_sample_config, validate_submit_config
from generateSubmissionFilesOpenData import _parse_opendata_config


# ---------------------------------------------------------------------------
# Helper to write temp YAML / text files
# ---------------------------------------------------------------------------

def _write(tmp_path, filename, content):
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content))
    return str(p)


# ---------------------------------------------------------------------------
# _validate_yaml_sample_config tests
# ---------------------------------------------------------------------------

class TestValidateYamlSampleConfig:

    def test_valid_nano_manifest(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                sum_weights: 500000.0
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert errors == []

    def test_nano_auto_detect(self, tmp_path):
        """Auto-detect mode with DAS paths should resolve to nano."""
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                sum_weights: 500000.0
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="auto")
        assert errors == []

    def test_nano_mc_missing_xsec_warns(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                sum_weights: 500000.0
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert errors == []
        assert any("xsec" in w for w in warnings)

    def test_nano_missing_sum_weights_warns(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert errors == []
        assert any("sum_weights" in w for w in warnings)

    def test_nano_missing_file_source_warns(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                xsec: 98.34
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert any("das" in w or "files" in w for w in warnings)

    def test_nano_data_no_xsec_ok(self, tmp_path):
        """Real data entries don't need xsec – should not warn."""
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: data_2022C
                dtype: data
                das: /SingleMuon/Run2022C/NANOAOD
                sum_weights: ~
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert errors == []
        assert not any("xsec" in w for w in warnings)

    def test_duplicate_name_is_error(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: ttbar
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
              - name: ttbar
                das: /TTto2L2Nu/Run3/NANO2
                xsec: 98.34
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert any("Duplicate" in e for e in errors)

    def test_missing_parent_is_error(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: ttbar_skim
                das: /skims/ttbar.root
                xsec: 98.34
                parent: ttbar_powheg_nonexistent
        """)
        errors, _ = _validate_yaml_sample_config(path, mode="nano")
        assert any("parent" in e for e in errors)

    def test_empty_manifest_is_error(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", "datasets: []\n")
        errors, _ = _validate_yaml_sample_config(path, mode="nano")
        assert errors

    def test_invalid_yaml_syntax_is_error(self, tmp_path):
        path = _write(tmp_path, "broken.yaml", "datasets: [unterminated\n")
        errors, _ = _validate_yaml_sample_config(path, mode="nano")
        assert errors

    def test_opendata_mode_no_das_warns(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: mydata
                dtype: data
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="opendata")
        # Entries without das should warn (not hard error)
        assert any("das" in w or "record" in w.lower() for w in warnings)

    def test_opendata_auto_detect_numeric_recids(self, tmp_path):
        """Auto-detect should recognise numeric das fields as Open Data record IDs."""
        path = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: mydata
                dtype: data
                das: "12345"
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="auto")
        # Should not report "missing file source" as if it were a NANO sample
        assert not any("das" in e and "NANO" in e for e in errors)

    def test_lumi_by_year_unknown_year_is_warning(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            lumi_by_year:
              9999: 1.0
            datasets:
              - name: ttbar
                dtype: mc
                year: 2022
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                sum_weights: 500000.0
        """)
        errors, warnings = _validate_yaml_sample_config(path, mode="nano")
        assert errors == []
        assert any("9999" in w for w in warnings)


# ---------------------------------------------------------------------------
# validate_submit_config routing tests (YAML sample config path)
# ---------------------------------------------------------------------------

class TestValidateSubmitConfigYamlRouting:
    """Verify that validate_submit_config routes YAML sample configs correctly."""

    def _make_submit_config(self, tmp_path, sample_config_path):
        """Write a minimal submit_config.txt pointing at sample_config_path (absolute)."""
        cfg_path = tmp_path / "submit_config.txt"
        cfg_path.write_text(
            f"sampleConfig={sample_config_path}\n"
            "saveDirectory=/tmp/out\n"
            "saveTree=Events\n"
        )
        return str(cfg_path)

    def test_yaml_sample_config_no_errors(self, tmp_path):
        sample_yaml = _write(tmp_path, "samples.yaml", """\
            lumi: 59.7
            datasets:
              - name: ttbar
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                sum_weights: 500000.0
        """)
        cfg = self._make_submit_config(tmp_path, sample_yaml)
        errors, warnings = validate_submit_config(cfg, mode="nano")
        assert errors == []

    def test_yaml_duplicate_name_detected(self, tmp_path):
        sample_yaml = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: ttbar
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                sum_weights: 1.0
              - name: ttbar
                das: /TTto2L2Nu/Run3/NANO2
                xsec: 98.34
                sum_weights: 1.0
        """)
        cfg = self._make_submit_config(tmp_path, sample_yaml)
        errors, _ = validate_submit_config(cfg, mode="nano")
        assert any("Duplicate" in e for e in errors)


# ---------------------------------------------------------------------------
# _parse_opendata_config tests
# ---------------------------------------------------------------------------

class TestParseOpendataConfig:

    def test_legacy_text_format(self, tmp_path):
        path = _write(tmp_path, "samples.txt", """\
            lumi=59.7
            recids=12345,67890
            name=mydata das=CMS_Run2012B xsec=1.0 type=1
        """)
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert lumi == 59.7
        assert "12345" in recids
        assert "67890" in recids
        assert "mydata" in samples
        assert samples["mydata"]["das"] == "CMS_Run2012B"

    def test_yaml_manifest_format(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 41.5
            datasets:
              - name: mydata
                dtype: data
                das: "54321"
                xsec: ~
              - name: mymc
                dtype: mc
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
        """)
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert lumi == 41.5
        assert "mydata" in samples
        assert "mymc" in samples
        # The das field of mydata ("54321") should appear in recids
        assert "54321" in recids
        # The DAS path /TTto2L2Nu/Run3/NANO should also appear in recids
        assert "/TTto2L2Nu/Run3/NANO" in recids

    def test_yaml_multi_das_recids(self, tmp_path):
        """Comma-separated das fields should be split into multiple recids."""
        path = _write(tmp_path, "samples.yaml", """\
            lumi: 1.0
            datasets:
              - name: s1
                das: "111,222,333"
        """)
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert "111" in recids
        assert "222" in recids
        assert "333" in recids

    def test_yaml_empty_datasets(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", "lumi: 1.0\ndatasets: []\n")
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert samples == {}
        assert recids == []
        assert lumi == 1.0

    def test_legacy_no_recids(self, tmp_path):
        path = _write(tmp_path, "samples.txt", "name=s1 xsec=1.0 type=1\n")
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert recids == []
        assert "s1" in samples

    def test_yaml_lumi_default(self, tmp_path):
        path = _write(tmp_path, "samples.yaml", """\
            datasets:
              - name: s1
                das: /TTto2L2Nu
        """)
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert lumi == 1.0  # default from DatasetManifest

    def test_legacy_lumi_parsed(self, tmp_path):
        path = _write(tmp_path, "samples.txt", "lumi=77.5\nname=s xsec=1.0 type=1\n")
        samples, recids, lumi = _parse_opendata_config(str(path))
        assert lumi == 77.5
