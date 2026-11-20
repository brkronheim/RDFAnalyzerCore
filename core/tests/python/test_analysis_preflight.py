import textwrap

from analysis_preflight import AnalysisPreflight, PreflightResult


def test_analysis_controls_report_title_and_failure_status():
    report = PreflightResult(title="Custom readiness")
    report.warn("optional", "missing optional resource")
    report.skip("data", "not applicable")
    assert report.passed
    report.fail("required", "missing payload")
    assert not report.passed
    assert report.summary().startswith("Custom readiness: 0/3 passed, 1 failed")


def test_yaml_paths_and_parse_failures(tmp_path):
    cfg = tmp_path / "cfg"
    cfg.mkdir()
    (cfg / "valid.yaml").write_text("bins: [1, 2]\n")
    (cfg / "invalid.yaml").write_text("bins: [\n")
    preflight = AnalysisPreflight(str(tmp_path))
    preflight.check_yaml_syntax("valid.yaml", "plain")
    preflight.check_yaml_syntax("cfg/valid.yaml", "prefixed")
    assert preflight.result.passed
    assert preflight._load_yaml("cfg/valid.yaml")[0] == {"bins": [1, 2]}
    preflight.check_yaml_syntax(str(cfg / "invalid.yaml"), "invalid")
    preflight.check_yaml_syntax("missing.yaml", "missing")
    assert not preflight.result.passed
    assert [c["status"] for c in preflight.result.checks] == ["PASS", "PASS", "FAIL", "FAIL"]


class TestPreflightResult:

    def test_empty_result_passes(self):
        r = PreflightResult(title="Analysis Preflight")
        assert r.passed

    def test_all_pass(self):
        r = PreflightResult(title="Analysis Preflight")
        r.ok("check_1", "detail")
        r.ok("check_2")
        assert r.passed
        assert r.summary().startswith("Analysis Preflight: 2/2 passed")

    def test_one_fail(self):
        r = PreflightResult(title="Analysis Preflight")
        r.ok("pass_check")
        r.fail("fail_check", "something broke")
        assert not r.passed
        assert "[FAIL] fail_check" in r.summary()

    def test_one_warn_is_still_pass(self):
        r = PreflightResult(title="Analysis Preflight")
        r.ok("pass_check")
        r.warn("warn_check", "advisory")
        assert r.passed
        assert "[WARN] warn_check" in r.summary()

    def test_skip_is_pass(self):
        r = PreflightResult(title="Analysis Preflight")
        r.ok("pass_check")
        r.skip("skip_check", "not applicable")
        assert r.passed
        assert "[SKIP] skip_check" in r.summary()


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

        preflight = AnalysisPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        assert result.passed

    def test_missing_file(self, tmp_path):
        """Missing stxs_bins.txt should fail."""
        preflight = AnalysisPreflight(str(tmp_path))
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

        preflight = AnalysisPreflight(str(tmp_path))
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

        preflight = AnalysisPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_stxs_bin_config("cfg/stxs_bins.txt")
        # The malformed line produces a "expected 3 tokens" error
        assert not result.passed
        assert any("expected" in c["detail"] for c in result.checks)


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

        preflight = AnalysisPreflight(str(tmp_path))
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

        preflight = AnalysisPreflight(str(tmp_path))
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

        preflight = AnalysisPreflight(str(tmp_path))
        result = PreflightResult()
        preflight.result = result
        preflight.check_dataset_manifest(str(manifest))
        assert not result.passed
        assert any("Duplicate" in c["detail"] for c in result.checks)

