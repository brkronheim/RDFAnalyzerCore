"""
Tests for core/python/dataset_manifest.py.

Covers:
- DatasetEntry construction, defaults, and to_legacy_dict / to_dict / from_dict round-trips
- DatasetManifest construction and querying (query, by_name, get_groups, get_processes, get_years)
- DatasetManifest.load_yaml / save_yaml round-trip
- DatasetManifest.load() YAML extension handling
"""
from __future__ import annotations

import os
import sys
import tempfile
import textwrap

import pytest
import yaml

# Make core/python importable when running from repo root
_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from dataset_manifest import (
    DatasetEntry,
    DatasetManifest,
    FriendTreeConfig,
    select_generator_weight_denominator,
)


# ---------------------------------------------------------------------------
# DatasetEntry tests
# ---------------------------------------------------------------------------

class TestDatasetEntry:

    def test_minimal_construction(self):
        e = DatasetEntry(name="mysample")
        assert e.name == "mysample"
        assert e.dtype == "mc"
        assert e.filter_efficiency == 1.0
        assert e.kfac == 1.0
        assert e.extra_scale == 1.0
        assert e.files == []
        assert e.xsec is None
        assert e.year is None
        assert e.parent is None
        assert e.generator_weight_mode == "sign"

    def test_generator_weight_mode_round_trips_and_defaults_to_sign(self):
        default_entry = DatasetEntry.from_dict({"name": "default"})
        full_entry = DatasetEntry.from_dict(
            {"name": "full", "generator_weight_mode": "FULL"}
        )
        full_legacy = DatasetEntry.from_legacy_dict(
            {"name": "full", "genWeightMode": "full"}
        )

        assert default_entry.generator_weight_mode == "sign"
        assert full_entry.generator_weight_mode == "full"
        assert DatasetEntry.from_dict(full_entry.to_dict()).generator_weight_mode == "full"
        assert full_entry.to_legacy_dict()["genWeightMode"] == "full"
        assert full_legacy.generator_weight_mode == "full"

    @pytest.mark.parametrize("mode", ["typo", "", None, 1])
    def test_generator_weight_mode_rejects_invalid_values(self, mode):
        with pytest.raises(ValueError, match="generator_weight_mode must be 'sign' or 'full'"):
            DatasetEntry(name="bad", generator_weight_mode=mode)

    def test_full_construction(self):
        e = DatasetEntry(
            name="ttbar_2022",
            year=2022,
            era=None,
            campaign="Run3Summer22",
            process="ttbar",
            group="ttbar",
            stitch_id=3,
            sample_type=42,
            xsec=98.34,
            filter_efficiency=0.5,
            kfac=1.1,
            extra_scale=2.0,
            sum_weights=123456.0,
            dtype="mc",
            das="/TTto2L2Nu/Run3/NANO",
            files=[],
            parent="ttbar_gen",
            site="T2_US_MIT",
        )
        assert e.year == 2022
        assert e.campaign == "Run3Summer22"
        assert e.stitch_id == 3
        assert e.sample_type == 42
        assert e.xsec == 98.34
        assert e.site == "T2_US_MIT"

    def test_to_legacy_dict_mc(self):
        e = DatasetEntry(
            name="ttbar",
            year=2022,
            campaign="Run3Summer22",
            process="ttbar",
            group="ttbar",
            dtype="mc",
            xsec=98.34,
            kfac=1.2,
            extra_scale=0.5,
            filter_efficiency=0.9,
            sum_weights=500000.0,
            das="/TTto2L2Nu/Run3/NANO",
            stitch_id=1,
            sample_type=120,
            parent="ttbar_gen",
            site="T2_US_MIT",
        )
        d = e.to_legacy_dict()
        assert d["name"] == "ttbar"
        assert d["xsec"] == "98.34"
        assert d["kfac"] == "1.2"
        assert d["extraScale"] == "0.5"
        assert d["filterEfficiency"] == "0.9"
        assert d["norm"] == "500000.0"
        assert d["das"] == "/TTto2L2Nu/Run3/NANO"
        assert d["type"] == "mc"
        assert d["stitch_id"] == "1"
        assert d["sample_type"] == "120"
        assert d["year"] == "2022"
        assert d["campaign"] == "Run3Summer22"
        assert d["process"] == "ttbar"
        assert d["group"] == "ttbar"
        assert d["parent"] == "ttbar_gen"
        assert d["site"] == "T2_US_MIT"

    def test_from_legacy_dict_numeric_type(self):
        e = DatasetEntry.from_legacy_dict(
            {
                "name": "open_data_sample",
                "type": "7",
                "das": "12345",
                "site": "T2_CH_CERN",
            }
        )
        assert e.name == "open_data_sample"
        assert e.sample_type == 7
        assert e.dtype == "mc"
        assert e.site == "T2_CH_CERN"

    def test_to_legacy_dict_defaults_omitted(self):
        """Fields at their default values (kfac=1.0, extra_scale=1.0,
        filter_efficiency=1.0) must be absent from the legacy dict."""
        e = DatasetEntry(name="s", dtype="mc")
        d = e.to_legacy_dict()
        assert "kfac" not in d
        assert "extraScale" not in d
        assert "filterEfficiency" not in d
        assert "norm" not in d
        assert "xsec" not in d

    def test_to_legacy_dict_files(self):
        e = DatasetEntry(name="s", files=["file_a.root", "file_b.root"])
        d = e.to_legacy_dict()
        assert d["fileList"] == "file_a.root,file_b.root"

    def test_to_dict_round_trip(self):
        e = DatasetEntry(
            name="wjets_0",
            year=2022,
            group="wjets_ht",
            xsec=1000.0,
            dtype="mc",
            extra={"xrdfs_path": "/eos/user/a/alice/sample"},
        )
        d = e.to_dict()
        e2 = DatasetEntry.from_dict(d)
        assert e2.name == e.name
        assert e2.year == e.year
        assert e2.group == e.group
        assert e2.xsec == e.xsec
        assert e2.dtype == e.dtype
        assert e2.extra == e.extra

    def test_from_dict_promotes_unknown_keys_to_extra(self):
        d = {
            "name": "s",
            "dtype": "data",
            "unknown_future_field": "value",
        }
        e = DatasetEntry.from_dict(d)
        assert e.name == "s"
        assert e.dtype == "data"
        assert e.extra["unknown_future_field"] == "value"

    def test_from_dict_preserves_extra_metadata(self):
        d = {
            "name": "s",
            "dtype": "mc",
            "extra": {
                "xrdfs_server": "root://eosuser.cern.ch/",
                "xrdfs_path": "/eos/user/b/test/sample",
            },
        }
        e = DatasetEntry.from_dict(d)
        assert e.extra == d["extra"]


# ---------------------------------------------------------------------------
# DatasetManifest construction tests
# ---------------------------------------------------------------------------

class TestDatasetManifestConstruction:

    def test_empty_manifest(self):
        m = DatasetManifest()
        assert len(m) == 0
        assert m.lumi == 1.0
        assert m.whitelist == []
        assert m.blacklist == []

    def test_manifest_with_entries(self):
        entries = [DatasetEntry("a"), DatasetEntry("b")]
        m = DatasetManifest(datasets=entries, lumi=59.7)
        assert len(m) == 2
        assert m.lumi == 59.7

    def test_iteration(self):
        entries = [DatasetEntry("a"), DatasetEntry("b")]
        m = DatasetManifest(datasets=entries)
        names = [e.name for e in m]
        assert names == ["a", "b"]


# ---------------------------------------------------------------------------
# DatasetManifest query tests
# ---------------------------------------------------------------------------

class TestDatasetManifestQuery:

    def _make_manifest(self):
        return DatasetManifest(datasets=[
            DatasetEntry("ttbar_2022",     year=2022, dtype="mc",   process="ttbar",  group="ttbar"),
            DatasetEntry("ttbar_2023",     year=2023, dtype="mc",   process="ttbar",  group="ttbar"),
            DatasetEntry("wjets_0_2022",   year=2022, dtype="mc",   process="wjets",  group="wjets_ht", stitch_id=0),
            DatasetEntry("wjets_1_2022",   year=2022, dtype="mc",   process="wjets",  group="wjets_ht", stitch_id=1),
            DatasetEntry("data_2022C",     year=2022, dtype="data",  era="C",          group="data_2022"),
            DatasetEntry("data_2022D",     year=2022, dtype="data",  era="D",          group="data_2022"),
            DatasetEntry("ttbar_skim",     year=2022, dtype="mc",   process="ttbar",  parent="ttbar_2022"),
        ])


    def test_selects_denominator_for_each_mode_with_negative_events(self):
        # The synthetic generator weights are [-2, 1, 4], so the raw sum is 3
        # while the sum of signs is 1.
        assert select_generator_weight_denominator("sign", 3.0, 1.0) == 1.0
        assert select_generator_weight_denominator("full", 3.0, 1.0) == 3.0

    @pytest.mark.parametrize(
        ("mode", "sum_weights", "sum_sign_weights", "selected"),
        [
            ("sign", 5.0, 0.0, "sum_sign_weights"),
            ("sign", 5.0, -2.0, "sum_sign_weights"),
            ("sign", 5.0, float("nan"), "sum_sign_weights"),
            ("sign", 5.0, float("inf"), "sum_sign_weights"),
            ("full", 0.0, 5.0, "sum_weights"),
            ("full", -2.0, 5.0, "sum_weights"),
            ("full", float("nan"), 5.0, "sum_weights"),
            ("full", float("inf"), 5.0, "sum_weights"),
        ],
    )
    def test_rejects_invalid_selected_denominator_without_fallback(
        self, mode, sum_weights, sum_sign_weights, selected
    ):
        with pytest.raises(ValueError, match=selected) as exc:
            select_generator_weight_denominator(
                mode,
                sum_weights,
                sum_sign_weights,
                dataset_name="mc_negative_weights",
            )
        assert "must be finite and strictly positive" in str(exc.value)
        assert "No fallback" in str(exc.value)

    def test_skim_collector_uses_the_manifest_selected_denominator(self, tmp_path, monkeypatch):
        analysis_tasks = pytest.importorskip("analysis_tasks")
        manifest_path = tmp_path / "datasets.yaml"
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "lumi": 1.0,
                    "datasets": [
                        {
                            "name": "negative_sign",
                            "dtype": "mc",
                            "xsec": 0.003,
                            "files": ["sign.root"],
                        },
                        {
                            "name": "negative_full",
                            "dtype": "mc",
                            "xsec": 0.003,
                            "generator_weight_mode": "full",
                            "files": ["full.root"],
                        },
                    ],
                }
            )
        )
        job_info = {
            "negative_sign": {0: {"meta": "sign_meta.root"}},
            "negative_full": {0: {"meta": "full_meta.root"}},
        }
        state = {
            "datasets": {
                dataset: {"jobs": {"0": {"status": "done"}}}
                for dataset in job_info
            }
        }
        raw_sums = {"sign_meta.root": 3.0, "full_meta.root": 3.0}
        sign_sums = {"sign_meta.root": 1.0, "full_meta.root": 1.0}
        monkeypatch.setattr(
            analysis_tasks,
            "_shared_read_weight_sum",
            lambda path, sample: raw_sums[path],
        )
        monkeypatch.setattr(
            analysis_tasks,
            "_shared_read_weight_sign_sum",
            lambda path, sample: sign_sums[path],
        )

        result = analysis_tasks._collect_skim_normalization(
            str(manifest_path), job_info, state
        )

        sign_result = result["datasets"]["negative_sign"]
        full_result = result["datasets"]["negative_full"]
        assert sign_result["normalization_mode"] == "sign"
        assert sign_result["normalization_denominator_name"] == "sum_sign_weights"
        assert sign_result["normalization_denominator"] == 1.0
        assert sign_result["normalization"] == 3.0
        assert full_result["normalization_mode"] == "full"
        assert full_result["normalization_denominator_name"] == "sum_weights"
        assert full_result["normalization_denominator"] == 3.0
        assert full_result["normalization"] == 1.0

    @pytest.mark.parametrize(
        ("mode", "raw_total", "sign_total", "selected"),
        [
            ("sign", 3.0, 0.0, "sum_sign_weights"),
            ("full", 0.0, 1.0, "sum_weights"),
        ],
    )
    def test_skim_collector_rejects_invalid_selected_sum_without_fallback(
        self, tmp_path, monkeypatch, mode, raw_total, sign_total, selected
    ):
        analysis_tasks = pytest.importorskip("analysis_tasks")
        manifest_path = tmp_path / "datasets.yaml"
        manifest_path.write_text(
            yaml.safe_dump(
                {
                    "datasets": [
                        {
                            "name": "invalid_denominator",
                            "dtype": "mc",
                            "xsec": 1.0,
                            "generator_weight_mode": mode,
                            "files": ["input.root"],
                        }
                    ]
                }
            )
        )
        job_info = {"invalid_denominator": {0: {"meta": "meta.root"}}}
        state = {
            "datasets": {
                "invalid_denominator": {"jobs": {"0": {"status": "done"}}}
            }
        }
        monkeypatch.setattr(
            analysis_tasks,
            "_shared_read_weight_sum",
            lambda path, sample: raw_total,
        )
        monkeypatch.setattr(
            analysis_tasks,
            "_shared_read_weight_sign_sum",
            lambda path, sample: sign_total,
        )

        result = analysis_tasks._collect_skim_normalization(
            str(manifest_path), job_info, state
        )["datasets"]["invalid_denominator"]

        assert result["status"] == "error"
        assert result["normalization"] is None
        assert selected in result["errors"][0]
        assert "No fallback" in result["errors"][0]

    def test_job_config_receives_explicit_full_generator_weight_mode(self, tmp_path):
        analysis_tasks = pytest.importorskip("analysis_tasks")
        dataset = DatasetEntry(name="full_sample", generator_weight_mode="full")
        metadata = analysis_tasks._dataset_job_metadata(dataset)
        assert metadata["genWeightMode"] == "full"

        template = tmp_path / "submit_config.txt"
        template.write_text("enableCounters=true\n")
        job_dir = tmp_path / "job"
        output_dir = tmp_path / "output"
        analysis_tasks._write_job_config(
            str(job_dir), str(template), dataset, str(output_dir)
        )
        written_config = analysis_tasks.read_config(
            str(job_dir / "submit_config.txt")
        )
        assert written_config["genWeightMode"] == "full"

        data_dataset = DatasetEntry(name="data_sample", dtype="data")
        data_job_dir = tmp_path / "data_job"
        analysis_tasks._write_job_config(
            str(data_job_dir), str(template), data_dataset, str(output_dir)
        )
        data_config = analysis_tasks.read_config(
            str(data_job_dir / "submit_config.txt")
        )
        assert "genWeightMode" not in data_config

    def test_query_by_year(self):
        m = self._make_manifest()
        result = m.query(year=2022)
        assert all(e.year == 2022 for e in result)
        names = {e.name for e in result}
        assert "ttbar_2023" not in names

    def test_query_by_dtype(self):
        m = self._make_manifest()
        data = m.query(dtype="data")
        assert all(e.dtype == "data" for e in data)
        assert {e.name for e in data} == {"data_2022C", "data_2022D"}

    def test_query_by_process(self):
        m = self._make_manifest()
        result = m.query(process="wjets")
        assert all(e.process == "wjets" for e in result)
        assert len(result) == 2

    def test_query_by_group(self):
        m = self._make_manifest()
        result = m.query(group="wjets_ht")
        assert {e.name for e in result} == {"wjets_0_2022", "wjets_1_2022"}

    def test_query_combined(self):
        m = self._make_manifest()
        # Both ttbar_2022 and ttbar_skim are year=2022, mc, process=ttbar
        result = m.query(year=2022, dtype="mc", process="ttbar")
        assert len(result) == 2
        names = {e.name for e in result}
        assert names == {"ttbar_2022", "ttbar_skim"}
        # Further narrow to group="ttbar" to exclude the skim (no group set)
        result_grouped = m.query(year=2022, dtype="mc", process="ttbar", group="ttbar")
        assert len(result_grouped) == 1
        assert result_grouped[0].name == "ttbar_2022"

    def test_query_by_era(self):
        m = self._make_manifest()
        result = m.query(era="C")
        assert len(result) == 1
        assert result[0].name == "data_2022C"

    def test_query_by_parent(self):
        m = self._make_manifest()
        result = m.query(parent="ttbar_2022")
        assert len(result) == 1
        assert result[0].name == "ttbar_skim"

    def test_query_no_match(self):
        m = self._make_manifest()
        assert m.query(year=9999) == []

    def test_query_empty_args_returns_all(self):
        m = self._make_manifest()
        assert len(m.query()) == len(m.datasets)

    def test_by_name_found(self):
        m = self._make_manifest()
        e = m.by_name("ttbar_2022")
        assert e is not None
        assert e.name == "ttbar_2022"

    def test_by_name_not_found(self):
        m = self._make_manifest()
        assert m.by_name("nonexistent") is None

    def test_get_groups(self):
        m = self._make_manifest()
        groups = m.get_groups()
        assert "wjets_ht" in groups
        assert "ttbar" in groups
        assert groups == sorted(groups)

    def test_get_processes(self):
        m = self._make_manifest()
        procs = m.get_processes()
        assert "ttbar" in procs
        assert "wjets" in procs
        assert procs == sorted(procs)

    def test_get_years(self):
        m = self._make_manifest()
        years = m.get_years()
        assert years == [2022, 2023]


# ---------------------------------------------------------------------------
# DatasetManifest YAML I/O tests
# ---------------------------------------------------------------------------

class TestDatasetManifestYAML:

    def _make_yaml(self, tmp_path):
        content = textwrap.dedent("""\
            lumi: 59.7
            whitelist:
              - T2_US_MIT
            blacklist: []
            datasets:
              - name: ttbar_2022
                year: 2022
                campaign: Run3Summer22
                process: ttbar
                group: ttbar
                dtype: mc
                generator_weight_mode: full
                das: /TTto2L2Nu/Run3/NANO
                xsec: 98.34
                filter_efficiency: 1.0
                kfac: 1.0
                extra_scale: 1.0
              - name: data_2022C
                year: 2022
                era: C
                dtype: data
                group: data_2022
        """)
        p = tmp_path / "manifest.yaml"
        p.write_text(content)
        return str(p)

    def test_load_yaml(self, tmp_path):
        path = self._make_yaml(tmp_path)
        m = DatasetManifest.load_yaml(path)
        assert m.lumi == 59.7
        assert m.whitelist == ["T2_US_MIT"]
        assert len(m.datasets) == 2
        ttbar = m.by_name("ttbar_2022")
        assert ttbar is not None
        assert ttbar.year == 2022
        assert ttbar.xsec == 98.34
        assert ttbar.das == "/TTto2L2Nu/Run3/NANO"
        assert ttbar.generator_weight_mode == "full"

    def test_save_and_reload_yaml(self, tmp_path):
        original = DatasetManifest(
            datasets=[
                DatasetEntry(
                    name="wjets_0",
                    year=2022,
                    group="wjets_ht",
                    xsec=1000.0,
                    stitch_id=0,
                    dtype="mc",
                    generator_weight_mode="full",
                    das="/WtoLNu/Run3/NANO",
                )
            ],
            lumi=41.48,
            whitelist=["T2_DE_DESY"],
            blacklist=["T1_FR_CCIN2P3"],
        )
        path = str(tmp_path / "out.yaml")
        original.save_yaml(path)
        loaded = DatasetManifest.load_yaml(path)
        assert loaded.lumi == original.lumi
        assert loaded.whitelist == original.whitelist
        assert loaded.blacklist == original.blacklist
        assert len(loaded.datasets) == 1
        e = loaded.datasets[0]
        assert e.name == "wjets_0"
        assert e.stitch_id == 0
        assert e.xsec == 1000.0
        assert e.generator_weight_mode == "full"

    def test_load_auto_detect_yaml(self, tmp_path):
        path = self._make_yaml(tmp_path)
        m = DatasetManifest.load(path)
        assert len(m.datasets) == 2

    def test_load_invalid_yaml_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("- a\n- b\n")  # list, not mapping
        with pytest.raises(ValueError, match="mapping"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_rejects_unknown_top_level_keys(self, tmp_path):
        p = tmp_path / "bad_top.yaml"
        p.write_text(textwrap.dedent("""\
            lumi: 59.7
            unknown_key: true
            datasets:
              - name: ttbar_2022
                dtype: mc
        """))
        with pytest.raises(ValueError, match="unknown top-level keys"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_rejects_invalid_generator_weight_mode(self, tmp_path):
        p = tmp_path / "bad_weight_mode.yaml"
        p.write_text(textwrap.dedent("""\
            datasets:
              - name: ttbar
                dtype: mc
                generator_weight_mode: magnitude
        """))
        with pytest.raises(ValueError, match="ttbar.*generator_weight_mode"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_promotes_unknown_dataset_keys_to_extra(self, tmp_path):
        p = tmp_path / "dataset_extra.yaml"
        p.write_text(textwrap.dedent("""\
            datasets:
              - name: ttbar_2022
                dtype: mc
                unsupportedField: yes
        """))
        m = DatasetManifest.load_yaml(str(p))
        assert len(m.datasets) == 1
        assert m.datasets[0].extra["unsupportedField"] is True

    def test_load_yaml_rejects_invalid_dtype(self, tmp_path):
        p = tmp_path / "bad_dtype.yaml"
        p.write_text(textwrap.dedent("""\
            datasets:
              - name: ttbar_2022
                dtype: invalid
        """))
        with pytest.raises(ValueError, match="invalid dtype"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_rejects_duplicate_dataset_names(self, tmp_path):
        p = tmp_path / "duplicate_names.yaml"
        p.write_text(textwrap.dedent("""\
            datasets:
              - name: sample
              - name: sample
        """))
        with pytest.raises(ValueError, match="Duplicate dataset name"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_rejects_missing_parent(self, tmp_path):
        p = tmp_path / "missing_parent.yaml"
        p.write_text(textwrap.dedent("""\
            datasets:
              - name: child
                parent: missing
        """))
        with pytest.raises(ValueError, match="parent 'missing' not found"):
            DatasetManifest.load_yaml(str(p))

    def test_load_yaml_warns_for_unused_luminosity_year(self, tmp_path):
        p = tmp_path / "unused_lumi_year.yaml"
        p.write_text(textwrap.dedent("""\
            lumi_by_year:
              2099: 1.0
            datasets:
              - name: sample
                year: 2022
        """))
        with pytest.warns(UserWarning, match="lumi_by_year contains year 2099"):
            manifest = DatasetManifest.load_yaml(str(p))
        assert manifest.by_name("sample") is not None


# ---------------------------------------------------------------------------
# DatasetManifest legacy text format removal tests

class TestDatasetManifestRemovedLegacyText:

    def test_load_non_yaml_raises(self, tmp_path):
        path = tmp_path / "samples.txt"
        path.write_text("name=ttbar xsec=98.34 das=/TTto2L2Nu type=mc")
        with pytest.raises(ValueError, match="only supports YAML manifest files"):
            DatasetManifest.load(str(path))


# ---------------------------------------------------------------------------
# New feature: multi-value query
# ---------------------------------------------------------------------------
# New feature: multi-value query
# ---------------------------------------------------------------------------

class TestDatasetManifestMultiValueQuery:

    def _make_manifest(self):
        return DatasetManifest(datasets=[
            DatasetEntry("ttbar_2022",   year=2022, dtype="mc",   process="ttbar",  era=None),
            DatasetEntry("ttbar_2023",   year=2023, dtype="mc",   process="ttbar",  era=None),
            DatasetEntry("wjets_2022",   year=2022, dtype="mc",   process="wjets",  era=None),
            DatasetEntry("data_2022C",   year=2022, dtype="data", process="data",   era="C"),
            DatasetEntry("data_2022D",   year=2022, dtype="data", process="data",   era="D"),
            DatasetEntry("data_2023C",   year=2023, dtype="data", process="data",   era="C"),
        ])

    def test_query_multi_year_list(self):
        m = self._make_manifest()
        result = m.query(year=[2022, 2023])
        assert len(result) == len(m.datasets)  # all have year 2022 or 2023

    def test_query_multi_year_list_filtered(self):
        m = self._make_manifest()
        result = m.query(year=[2022, 2023], dtype="mc")
        assert len(result) == 3
        assert all(e.dtype == "mc" for e in result)
        assert {e.name for e in result} == {"ttbar_2022", "ttbar_2023", "wjets_2022"}

    def test_query_multi_era_list(self):
        m = self._make_manifest()
        result = m.query(era=["C", "D"])
        assert len(result) == 3  # data_2022C, data_2022D, data_2023C
        assert all(e.era in ("C", "D") for e in result)

    def test_query_multi_process_list(self):
        m = self._make_manifest()
        result = m.query(process=["ttbar", "wjets"])
        assert len(result) == 3
        assert all(e.process in ("ttbar", "wjets") for e in result)

    def test_query_multi_dtype_list(self):
        m = self._make_manifest()
        result = m.query(dtype=["mc", "data"])
        assert len(result) == len(m.datasets)

    def test_query_single_element_list_behaves_as_scalar(self):
        m = self._make_manifest()
        result_list = m.query(year=[2022])
        result_scalar = m.query(year=2022)
        assert {e.name for e in result_list} == {e.name for e in result_scalar}


# ---------------------------------------------------------------------------
# New feature: get_eras / get_eras_for_year
# ---------------------------------------------------------------------------

class TestDatasetManifestEras:

    def _make_manifest(self):
        return DatasetManifest(datasets=[
            DatasetEntry("d_2022C", year=2022, era="C", dtype="data"),
            DatasetEntry("d_2022D", year=2022, era="D", dtype="data"),
            DatasetEntry("d_2023B", year=2023, era="B", dtype="data"),
            DatasetEntry("mc_2022", year=2022, era=None, dtype="mc"),
        ])

    def test_get_eras(self):
        m = self._make_manifest()
        eras = m.get_eras()
        assert eras == ["B", "C", "D"]
        assert eras == sorted(eras)

    def test_get_eras_empty(self):
        m = DatasetManifest(datasets=[DatasetEntry("mc", era=None)])
        assert m.get_eras() == []

    def test_get_eras_for_year(self):
        m = self._make_manifest()
        assert m.get_eras_for_year(2022) == ["C", "D"]
        assert m.get_eras_for_year(2023) == ["B"]

    def test_get_eras_for_year_no_eras(self):
        m = self._make_manifest()
        # MC samples have era=None, so only data eras are returned
        assert m.get_eras_for_year(2022) == ["C", "D"]

    def test_get_eras_for_year_unknown_year(self):
        m = self._make_manifest()
        assert m.get_eras_for_year(9999) == []


# ---------------------------------------------------------------------------
# New feature: lumi_for / lumi_by_year
# ---------------------------------------------------------------------------

class TestDatasetManifestLumiByYear:

    def test_lumi_for_no_by_year(self):
        m = DatasetManifest(lumi=59.7)
        assert m.lumi_for() == 59.7
        assert m.lumi_for(year=2022) == 59.7

    def test_lumi_for_with_by_year(self):
        m = DatasetManifest(lumi=100.0, lumi_by_year={2022: 38.01, 2023: 27.01})
        assert m.lumi_for(year=2022) == 38.01
        assert m.lumi_for(year=2023) == 27.01

    def test_lumi_for_year_not_in_map_falls_back(self):
        m = DatasetManifest(lumi=100.0, lumi_by_year={2022: 38.01})
        assert m.lumi_for(year=2023) == 100.0

    def test_lumi_for_no_year_arg(self):
        m = DatasetManifest(lumi=100.0, lumi_by_year={2022: 38.01})
        assert m.lumi_for() == 100.0

    def test_lumi_for_era_arg_accepted(self):
        """era parameter is accepted for forward-compatibility (not used in lookup)."""
        m = DatasetManifest(lumi=100.0, lumi_by_year={2022: 38.01})
        assert m.lumi_for(year=2022, era="C") == 38.01

    def test_lumi_by_year_yaml_round_trip(self, tmp_path):
        original = DatasetManifest(
            datasets=[DatasetEntry("ttbar", year=2022)],
            lumi=100.0,
            lumi_by_year={2022: 38.01, 2023: 27.01},
        )
        path = str(tmp_path / "manifest.yaml")
        original.save_yaml(path)
        with pytest.warns(UserWarning, match="lumi_by_year contains year 2023"):
            loaded = DatasetManifest.load_yaml(path)
        assert loaded.lumi == 100.0
        assert loaded.lumi_by_year == {2022: 38.01, 2023: 27.01}
        assert loaded.lumi_for(year=2022) == 38.01
        assert loaded.lumi_for(year=2023) == 27.01

    def test_lumi_by_year_not_in_yaml_gives_empty_dict(self, tmp_path):
        content = "lumi: 59.7\ndatasets:\n  - name: s\n"
        p = tmp_path / "m.yaml"
        p.write_text(content)
        m = DatasetManifest.load_yaml(str(p))
        assert m.lumi_by_year == {}
        assert m.lumi_for(year=2022) == 59.7

    def test_save_yaml_omits_lumi_by_year_when_empty(self, tmp_path):
        import yaml as _yaml
        m = DatasetManifest(datasets=[DatasetEntry("s")], lumi=1.0)
        path = str(tmp_path / "out.yaml")
        m.save_yaml(path)
        with open(path) as fh:
            raw = _yaml.safe_load(fh)
        assert "lumi_by_year" not in raw


# ---------------------------------------------------------------------------
# New feature: validate()
# ---------------------------------------------------------------------------

class TestDatasetManifestValidate:

    def test_valid_manifest_returns_empty(self):
        m = DatasetManifest(datasets=[
            DatasetEntry("a", year=2022),
            DatasetEntry("b", year=2022, parent="a"),
        ])
        assert m.validate() == []

    def test_duplicate_name_detected(self):
        m = DatasetManifest(datasets=[
            DatasetEntry("a"),
            DatasetEntry("a"),
        ])
        errors = m.validate()
        assert any("Duplicate" in e and "'a'" in e for e in errors)

    def test_missing_parent_detected(self):
        m = DatasetManifest(datasets=[
            DatasetEntry("child", parent="nonexistent"),
        ])
        errors = m.validate()
        assert any("parent" in e and "'nonexistent'" in e for e in errors)

    def test_valid_parent_reference_ok(self):
        m = DatasetManifest(datasets=[
            DatasetEntry("parent_ds"),
            DatasetEntry("child_ds", parent="parent_ds"),
        ])
        assert m.validate() == []

    def test_lumi_by_year_unknown_year_warns(self):
        m = DatasetManifest(
            datasets=[DatasetEntry("s", year=2022)],
            lumi_by_year={2022: 38.01, 2099: 999.0},  # 2099 has no datasets
        )
        errors = m.validate()
        assert any("Warning" in e and "2099" in e for e in errors)
        # 2022 is valid and should not generate a warning
        assert not any("2022" in e for e in errors)

    def test_empty_manifest_valid(self):
        m = DatasetManifest()
        assert m.validate() == []


# ---------------------------------------------------------------------------
# New feature: file_hash()
# ---------------------------------------------------------------------------

class TestDatasetManifestFileHash:

    def test_file_hash_returns_hex_string(self, tmp_path):
        content = "lumi: 59.7\ndatasets:\n  - name: s\n"
        p = tmp_path / "m.yaml"
        p.write_text(content)
        h = DatasetManifest.file_hash(str(p))
        assert isinstance(h, str)
        # SHA-256 produces a 64-character hex digest
        assert len(h) == 64
        assert all(c in "0123456789abcdef" for c in h)

    def test_file_hash_stable_same_content(self, tmp_path):
        content = "lumi: 59.7\ndatasets:\n  - name: s\n"
        p1 = tmp_path / "m1.yaml"
        p2 = tmp_path / "m2.yaml"
        p1.write_text(content)
        p2.write_text(content)
        assert DatasetManifest.file_hash(str(p1)) == DatasetManifest.file_hash(str(p2))

    def test_file_hash_different_for_different_content(self, tmp_path):
        p1 = tmp_path / "m1.yaml"
        p2 = tmp_path / "m2.yaml"
        p1.write_text("lumi: 59.7\ndatasets: []\n")
        p2.write_text("lumi: 1.0\ndatasets: []\n")
        assert DatasetManifest.file_hash(str(p1)) != DatasetManifest.file_hash(str(p2))

    def test_file_hash_not_found_returns_placeholder(self, tmp_path):
        h = DatasetManifest.file_hash(str(tmp_path / "nonexistent.yaml"))
        assert h == "<not found>"


# ---------------------------------------------------------------------------
# FriendTreeConfig tests
# ---------------------------------------------------------------------------

class TestFriendTreeConfig:
    """Tests for the FriendTreeConfig dataclass."""

    def test_defaults(self):
        cfg = FriendTreeConfig(alias="calib")
        assert cfg.alias == "calib"
        assert cfg.tree_name == "Events"
        assert cfg.files == []
        assert cfg.directory is None
        assert cfg.globs == [".root"]
        assert cfg.antiglobs == []
        assert cfg.index_branches == []

    def test_explicit_files(self):
        cfg = FriendTreeConfig(
            alias="taggers",
            tree_name="BTagging",
            files=["/data/tag1.root", "root://server//tag2.root"],
        )
        assert cfg.alias == "taggers"
        assert cfg.tree_name == "BTagging"
        assert len(cfg.files) == 2
        assert cfg.files[1] == "root://server//tag2.root"

    def test_directory_based(self):
        cfg = FriendTreeConfig(
            alias="derived",
            directory="/path/to/derived",
            globs=[".root"],
            antiglobs=["output.root"],
        )
        assert cfg.directory == "/path/to/derived"
        assert cfg.globs == [".root"]
        assert cfg.antiglobs == ["output.root"]

    def test_index_branches(self):
        cfg = FriendTreeConfig(
            alias="calib",
            files=["/data/calib.root"],
            index_branches=["run", "luminosityBlock"],
        )
        assert cfg.index_branches == ["run", "luminosityBlock"]

    def test_to_dict_round_trip(self):
        cfg = FriendTreeConfig(
            alias="calib",
            tree_name="Calibrations",
            files=["/data/calib.root"],
            index_branches=["run", "event"],
        )
        d = cfg.to_dict()
        assert d["alias"] == "calib"
        assert d["tree_name"] == "Calibrations"
        assert d["files"] == ["/data/calib.root"]
        assert d["index_branches"] == ["run", "event"]
        # Round-trip via from_dict
        cfg2 = FriendTreeConfig.from_dict(d)
        assert cfg2 == cfg

    def test_from_dict_ignores_unknown_keys(self):
        d = {"alias": "x", "tree_name": "Events", "unknown_future_key": 42}
        cfg = FriendTreeConfig.from_dict(d)
        assert cfg.alias == "x"
        assert not hasattr(cfg, "unknown_future_key")


# ---------------------------------------------------------------------------
# DatasetEntry + FriendTreeConfig integration tests
# ---------------------------------------------------------------------------

class TestDatasetEntryFriendTrees:
    """Tests for the friend_trees field on DatasetEntry."""

    def test_default_is_empty(self):
        entry = DatasetEntry("ds")
        assert entry.friend_trees == []

    def test_set_friend_trees(self):
        ft = FriendTreeConfig(alias="calib", files=["/data/calib.root"])
        entry = DatasetEntry("ds", friend_trees=[ft])
        assert len(entry.friend_trees) == 1
        assert entry.friend_trees[0].alias == "calib"

    def test_to_dict_includes_friend_trees(self):
        ft = FriendTreeConfig(
            alias="calib",
            files=["/data/calib.root"],
            index_branches=["run", "luminosityBlock"],
        )
        entry = DatasetEntry("ds", friend_trees=[ft])
        d = entry.to_dict()
        assert "friend_trees" in d
        assert len(d["friend_trees"]) == 1
        assert d["friend_trees"][0]["alias"] == "calib"

    def test_from_dict_deserialises_friend_trees(self):
        d = {
            "name": "ds",
            "friend_trees": [
                {
                    "alias": "calib",
                    "tree_name": "Events",
                    "files": ["/data/calib.root"],
                    "directory": None,
                    "globs": [".root"],
                    "antiglobs": [],
                    "index_branches": ["run", "luminosityBlock"],
                }
            ],
        }
        entry = DatasetEntry.from_dict(d)
        assert len(entry.friend_trees) == 1
        ft = entry.friend_trees[0]
        assert isinstance(ft, FriendTreeConfig)
        assert ft.alias == "calib"
        assert ft.index_branches == ["run", "luminosityBlock"]

    def test_yaml_round_trip_with_friend_trees(self, tmp_path):
        ft = FriendTreeConfig(
            alias="calib",
            tree_name="Calibrations",
            files=["/data/calib_2022.root"],
            index_branches=["run", "luminosityBlock"],
        )
        entry = DatasetEntry(
            name="ttbar_2022",
            year=2022,
            dtype="mc",
            friend_trees=[ft],
        )
        manifest = DatasetManifest(datasets=[entry], lumi=38.01)

        yaml_path = tmp_path / "manifest.yaml"
        manifest.save_yaml(str(yaml_path))

        loaded = DatasetManifest.load_yaml(str(yaml_path))
        assert len(loaded.datasets) == 1
        loaded_entry = loaded.datasets[0]
        assert len(loaded_entry.friend_trees) == 1
        loaded_ft = loaded_entry.friend_trees[0]
        assert isinstance(loaded_ft, FriendTreeConfig)
        assert loaded_ft.alias == "calib"
        assert loaded_ft.tree_name == "Calibrations"
        assert loaded_ft.files == ["/data/calib_2022.root"]
        assert loaded_ft.index_branches == ["run", "luminosityBlock"]

    def test_validate_manifest_yaml_rejects_invalid_files_list(self, tmp_path):
        bad_manifest = textwrap.dedent(
            """
            lumi: 38.01
            datasets:
              - name: bad_files
                dtype: mc
                files: [123, 456]
            """
        )
        path = tmp_path / "bad_manifest.yaml"
        path.write_text(bad_manifest)

        with pytest.raises(ValueError, match="'files' must be a list of strings"):
            DatasetManifest.load_yaml(str(path))

    def test_validate_manifest_yaml_rejects_invalid_friend_tree_files(self, tmp_path):
        bad_manifest = textwrap.dedent(
            """
            lumi: 38.01
            datasets:
              - name: bad_friend_tree
                dtype: mc
                friend_trees:
                  - alias: calib
                    files: [123]
            """
        )
        path = tmp_path / "bad_friend_tree.yaml"
        path.write_text(bad_manifest)

        with pytest.raises(ValueError, match=r"friend_trees\[0\] 'files' must be a list of strings"):
            DatasetManifest.load_yaml(str(path))

    def test_entry_without_friend_trees_yaml_round_trip(self, tmp_path):
        """Entries without friend_trees should round-trip without errors."""
        entry = DatasetEntry("plain_ds", year=2022, dtype="mc")
        manifest = DatasetManifest(datasets=[entry])
        yaml_path = tmp_path / "m.yaml"
        manifest.save_yaml(str(yaml_path))
        loaded = DatasetManifest.load_yaml(str(yaml_path))
        assert loaded.datasets[0].friend_trees == []

    def test_legacy_dict_does_not_include_friend_trees(self):
        """to_legacy_dict() should not serialise friend_trees (unsupported format)."""
        ft = FriendTreeConfig(alias="calib", files=["/data/calib.root"])
        entry = DatasetEntry("ds", friend_trees=[ft])
        d = entry.to_legacy_dict()
        assert "friend_trees" not in d
