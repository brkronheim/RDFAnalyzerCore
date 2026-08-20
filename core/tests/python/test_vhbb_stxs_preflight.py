"""
Tests for the VHbb STXS preflight readiness gate (vhbb_stxs_preflight.py).

Covers:
- PreflightResult construction and summary
- STXS bin config validation
- Datacard config validation
- Pass config validation
- Dataset manifest validation
"""
from __future__ import annotations

import os
import sys
import tempfile
import textwrap
from pathlib import Path

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
_CORE_PYTHON = os.path.normpath(os.path.join(_HERE, "..", "..", "python"))
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

from vhbb_stxs_preflight import (
    PreflightResult,
    VHbbStxsPreflight,
)


# ===========================================================================
# PreflightResult tests
# ===========================================================================

class TestPreflightResult:

    def test_empty_result_passes(self):
        r = PreflightResult()
        assert r.passed

    def test_all_pass(self):
        r = PreflightResult()
        r.ok("check_1", "detail")
        r.ok("check_2")
        assert r.passed
        assert r.summary().startswith("VHbb STXS Preflight: 2/2 passed")

    def test_one_fail(self):
        r = PreflightResult()
        r.ok("pass_check")
        r.fail("fail_check", "something broke")
        assert not r.passed
        assert "[FAIL] fail_check" in r.summary()

    def test_one_warn_is_still_pass(self):
        r = PreflightResult()
        r.ok("pass_check")
        r.warn("warn_check", "advisory")
        assert r.passed
        assert "[WARN] warn_check" in r.summary()

    def test_skip_is_pass(self):
        r = PreflightResult()
        r.ok("pass_check")
        r.skip("skip_check", "not applicable")
        assert r.passed
        assert "[SKIP] skip_check" in r.summary()


# ===========================================================================
# STXS bin config validation tests
# ===========================================================================

class TestStxsBinConfigValidation:

    def test_valid_stxs_bins(self, tmp_path):
        """45 bins, contiguous, properly formatted."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        bins_file = cfg_dir / "stxs_bins.txt"
        lines = []
        for i in range(1, 46):
            prefix = "wh" if i <= 15 else ("zh" if i <= 30 else "ggzh")
            lines.append(f"{i} {prefix}_{300+i} {300+i}")
        bins_file.write_text("\n".join(lines))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        assert result.passed

    def test_missing_file(self, tmp_path):
        """Missing stxs_bins.txt should fail."""
        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        assert not result.passed
        assert any("File not found" in c["detail"] for c in result.checks)

    def test_empty_bin_config(self, tmp_path):
        """Empty file should fail."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        (cfg_dir / "stxs_bins.txt").write_text("")

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        assert not result.passed
        assert any("No valid bin entries" in c["detail"] for c in result.checks)

    def test_malformed_line(self, tmp_path):
        """Line with wrong column count should warn."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        bins_file = cfg_dir / "stxs_bins.txt"
        bins_file.write_text("1 wh_301 301\nbad_line\n2 wh_302 302\n")

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        # The malformed line produces a "expected 3 tokens" error
        assert not result.passed
        assert any("expected" in c["detail"] for c in result.checks)


# ===========================================================================
# Datacard config validation tests
# ===========================================================================

class TestDatacardConfigValidation:

    def test_valid_datacard(self, tmp_path):
        """Valid datacard YAML with regions and systematics."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        dc_file = cfg_dir / "datacard_vhbb_stxs.yaml"
        dc_file.write_text(textwrap.dedent("""\
            output_dir: datacards_test
            manifest_defaults:
              region_names: [SR, CR_1B]
            control_regions:
              SR:
                observable: vhqq_dnn_stxs
                data_process: data_obs
                rebin: 1
              CR_1B:
                observable: dibjet_mass_stxs
                data_process: data_obs
                rebin: 1
            systematics:
              lumi:
                type: rate
                distribution: lnN
                value: 1.025
              pileup:
                type: shape
                distribution: shape
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_datacard_config("cfg/datacard_vhbb_stxs.yaml")
        assert result.passed

    def test_missing_region_observable(self, tmp_path):
        """Region missing 'observable' should fail."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        dc_file = cfg_dir / "datacard_vhbb_stxs.yaml"
        dc_file.write_text(textwrap.dedent("""\
            control_regions:
              SR:
                data_process: data_obs
            systematics: {}
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_datacard_config("cfg/datacard_vhbb_stxs.yaml")
        assert not result.passed
        assert any("observable" in c["detail"] for c in result.checks)

    def test_missing_systematic_type(self, tmp_path):
        """Systematic missing 'type' should fail."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        dc_file = cfg_dir / "datacard_vhbb_stxs.yaml"
        dc_file.write_text(textwrap.dedent("""\
            control_regions:
              SR:
                observable: vhqq_dnn_stxs
                data_process: data_obs
            systematics:
              lumi:
                distribution: lnN
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_datacard_config("cfg/datacard_vhbb_stxs.yaml")
        assert not result.passed
        assert any("type" in c["detail"] for c in result.checks)


# ===========================================================================
# Pass config validation tests
# ===========================================================================

class TestPassConfigValidation:

    def test_valid_pass_config(self, tmp_path):
        """Valid pass config with required keys."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        pass_file = cfg_dir / "pass_test.yaml"
        pass_file.write_text(textwrap.dedent("""\
            fileList: input.root
            treeList: Events
            saveDirectory: output/
            saveTree: Events
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_pass_config("cfg/pass_test.yaml", "pass_test")
        assert result.passed

    def test_missing_required_keys(self, tmp_path):
        """Pass config missing required keys should fail."""
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        pass_file = cfg_dir / "pass_test.yaml"
        pass_file.write_text(textwrap.dedent("""\
            fileList: input.root
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_pass_config("cfg/pass_test.yaml", "pass_test")
        assert not result.passed
        assert any("required keys" in c["detail"] for c in result.checks)


# ===========================================================================
# Dataset manifest validation tests
# ===========================================================================

class TestDatasetManifestValidation:

    def test_valid_manifest(self, tmp_path):
        """Valid dataset manifest with datasets."""
        manifest = tmp_path / "datasets.yaml"
        manifest.write_text(textwrap.dedent("""\
            datasets:
              - name: ttbar_2022
                year: 2022
                campaign: 2022_preEE
                process: ttbar
                dtype: mc
                xsec: 100.0
                das: /ttbar/some/das/path
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_dataset_manifest(str(manifest))
        assert result.passed

    def test_empty_datasets(self, tmp_path):
        """Manifest with empty datasets list should fail."""
        manifest = tmp_path / "datasets.yaml"
        manifest.write_text(textwrap.dedent("""\
            datasets: []
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_dataset_manifest(str(manifest))
        assert not result.passed
        assert any("empty" in c["detail"] for c in result.checks)

    def test_duplicate_names(self, tmp_path):
        """Duplicate dataset names should fail."""
        manifest = tmp_path / "datasets.yaml"
        manifest.write_text(textwrap.dedent("""\
            datasets:
              - name: dup_name
                year: 2022
              - name: dup_name
                year: 2023
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_dataset_manifest(str(manifest))
        assert not result.passed
        assert any("Duplicate" in c["detail"] for c in result.checks)


# ===========================================================================
# Integration: full preflight run on minimal setup
# ===========================================================================

class TestVHbbStxsPreflightIntegration:

    def test_minimal_dry_run(self, tmp_path):
        """Preflight run with minimal valid configs should pass."""
        # Create minimal analysis directory structure
        cfg_dir = tmp_path / "cfg"
        cfg_dir.mkdir()
        aux_dir = tmp_path / "aux"
        aux_dir.mkdir()
        dnn_dir = aux_dir / "dnn"
        dnn_dir.mkdir()
        (dnn_dir / "normDF_DNN.csv").write_text("dummy,norm\n")
        stxs_dir = aux_dir / "stxs"
        stxs_dir.mkdir()
        trigger_dir = aux_dir / "trigger_Sf"
        trigger_dir.mkdir()

        # STXS bin config
        bins_file = cfg_dir / "stxs_bins.txt"
        bins_file.write_text("\n".join(
            f"{i} bin_{i} {300+i}" for i in range(1, 46)
        ))

        # Datacard config
        dc_file = cfg_dir / "datacard_vhbb_stxs.yaml"
        dc_file.write_text(textwrap.dedent("""\
            output_dir: datacards_test
            manifest_defaults:
              region_names: [SR]
            control_regions:
              SR:
                observable: vhqq_dnn_stxs
                data_process: data_obs
            systematics:
              lumi:
                type: rate
                distribution: lnN
                value: 1.025
        """))

        # Fit metadata
        fm_file = cfg_dir / "fit_metadata.yaml"
        fm_file.write_text(textwrap.dedent("""\
            sample_metadata:
              dy_b:
                process: vjets
                tags: [background, b]
            processes:
              V_plus_b:
                select:
                  tags_all: [background, b]
        """))

        # Pass config
        pass_file = cfg_dir / "pass1_hist_derivations.yaml"
        pass_file.write_text(textwrap.dedent("""\
            fileList: input.root
            treeList: Events
            saveDirectory: output/
            saveTree: Events
        """))

        # Year settings
        ys_file = cfg_dir / "year_settings.yaml"
        ys_file.write_text(textwrap.dedent("""\
            years:
              2022_preEE:
                electronYearToken: 2022
                muonCorrection: NUM_MediumID_DEN_TrackerMuons
                electronCorrection: Electron-ID-SF
                rhoBranch: fixedGridRhoFastjetAll
                triggerSfRoot: aux/trigger_Sf
                btvBTagFile: test/btag.json.gz
                btvCTagFile: test/ctag.json.gz
                pileupFile: test/pu.json.gz
                muonFile: test/muon.json.gz
                electronFile: test/electron.json.gz
                jmeFile: test/jme.json.gz
                triggers:
                  doubleMuon: [Mu17_Mu8]
        """))

        # Histogram config
        hist_file = cfg_dir / "histograms_stxs.yaml"
        hist_file.write_text(textwrap.dedent("""\
            - name: vhbb_dnn_stxs
              variable: vhqq_dnn_score
              weight: weight_nominal
              bins: 20
              lowerBound: -1.0
              upperBound: 1.0
              channelVariable: vhbb_channel
              channelBins: 3
              channelLowerBound: 1.0
              channelUpperBound: 4.0
              channelRegions: 2L,1L,0L
        """))

        preflight = VHbbStxsPreflight(str(tmp_path))
        result = preflight.run(
            pass_configs=["cfg/pass1_hist_derivations.yaml"],
            stxs_bin_config="cfg/stxs_bins.txt",
            datacard_config="cfg/datacard_vhbb_stxs.yaml",
            fit_metadata="cfg/fit_metadata.yaml",
            histogram_configs=["cfg/histograms_stxs.yaml"],
        )
        # We expect some warnings (missing CVMFS payloads) but no failures
        assert result.passed, f"Preflight failed: {result.summary()}"
