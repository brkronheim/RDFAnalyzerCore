"""
Tests for core/python/dataset_manifest.py.

Covers:
- DatasetEntry construction, defaults, and to_legacy_dict / to_dict / from_dict round-trips
- DatasetManifest construction and querying (query, by_name, get_groups, get_processes, get_years)
- DatasetManifest.load_yaml / save_yaml round-trip
- DatasetManifest.load_text (legacy key=value format)
- DatasetManifest.load auto-detection
- to_legacy_sample_dict compatibility with getSampleList return shape
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

from dataset_manifest import DatasetEntry, DatasetManifest


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

    def test_full_construction(self):
        e = DatasetEntry(
            name="ttbar_2022",
            year=2022,
            era=None,
            campaign="Run3Summer22",
            process="ttbar",
            group="ttbar",
            stitch_id=3,
            xsec=98.34,
            filter_efficiency=0.5,
            kfac=1.1,
            extra_scale=2.0,
            sum_weights=123456.0,
            dtype="mc",
            das="/TTto2L2Nu/Run3/NANO",
            files=[],
            parent="ttbar_gen",
        )
        assert e.year == 2022
        assert e.campaign == "Run3Summer22"
        assert e.stitch_id == 3
        assert e.xsec == 98.34

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
            parent="ttbar_gen",
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
        assert d["year"] == "2022"
        assert d["campaign"] == "Run3Summer22"
        assert d["process"] == "ttbar"
        assert d["group"] == "ttbar"
        assert d["parent"] == "ttbar_gen"

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
        )
        d = e.to_dict()
        e2 = DatasetEntry.from_dict(d)
        assert e2.name == e.name
        assert e2.year == e.year
        assert e2.group == e.group
        assert e2.xsec == e.xsec
        assert e2.dtype == e.dtype

    def test_from_dict_ignores_unknown_keys(self):
        d = {
            "name": "s",
            "dtype": "data",
            "unknown_future_field": "value",
        }
        e = DatasetEntry.from_dict(d)
        assert e.name == "s"
        assert e.dtype == "data"


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

    def test_to_legacy_sample_dict(self):
        m = self._make_manifest()
        d = m.to_legacy_sample_dict()
        assert isinstance(d, dict)
        assert "ttbar_2022" in d
        assert d["ttbar_2022"]["name"] == "ttbar_2022"


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

    def test_load_auto_detect_yaml(self, tmp_path):
        path = self._make_yaml(tmp_path)
        m = DatasetManifest.load(path)
        assert len(m.datasets) == 2

    def test_load_invalid_yaml_raises(self, tmp_path):
        p = tmp_path / "bad.yaml"
        p.write_text("- a\n- b\n")  # list, not mapping
        with pytest.raises(ValueError, match="mapping"):
            DatasetManifest.load_yaml(str(p))


# ---------------------------------------------------------------------------
# DatasetManifest legacy text format tests
# ---------------------------------------------------------------------------

class TestDatasetManifestLegacyText:

    def _make_text_config(self, tmp_path):
        content = textwrap.dedent("""\
            lumi=59.7
            WL=T2_US_MIT
            BL=T1_FR_CCIN2P3
            name=ttbar xsec=98.34 das=/TTto2L2Nu/Run3/NANO type=mc kfac=1.0
            name=data_2022C type=data das=/SingleMuon/Run2022C/NANOAOD
            # comment line
            name=wjets_ht_0 xsec=54000.0 type=mc norm=1234567 stitch_id=0 year=2022 group=wjets_ht process=wjets
        """)
        p = tmp_path / "samples.txt"
        p.write_text(content)
        return str(p)

    def test_load_text(self, tmp_path):
        path = self._make_text_config(tmp_path)
        m = DatasetManifest.load_text(path)
        assert m.lumi == 59.7
        assert "T2_US_MIT" in m.whitelist
        assert "T1_FR_CCIN2P3" in m.blacklist
        assert len(m.datasets) == 3

    def test_load_text_fields(self, tmp_path):
        path = self._make_text_config(tmp_path)
        m = DatasetManifest.load_text(path)
        ttbar = m.by_name("ttbar")
        assert ttbar is not None
        assert ttbar.xsec == 98.34
        assert ttbar.dtype == "mc"
        assert ttbar.das == "/TTto2L2Nu/Run3/NANO"
        wjets = m.by_name("wjets_ht_0")
        assert wjets is not None
        assert wjets.stitch_id == 0
        assert wjets.year == 2022
        assert wjets.group == "wjets_ht"
        assert wjets.sum_weights == 1234567.0

    def test_load_auto_detect_text(self, tmp_path):
        path = self._make_text_config(tmp_path)
        m = DatasetManifest.load(path)
        assert len(m.datasets) == 3

    def test_load_text_query(self, tmp_path):
        path = self._make_text_config(tmp_path)
        m = DatasetManifest.load_text(path)
        mc = m.query(dtype="mc")
        assert all(e.dtype == "mc" for e in mc)
        data = m.query(dtype="data")
        assert len(data) == 1
        assert data[0].name == "data_2022C"


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
