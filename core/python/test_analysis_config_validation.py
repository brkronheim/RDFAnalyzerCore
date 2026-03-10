"""
Tests for the analysis config schema validation added to validate_config.py.

Covers:
- validate_histogram_config_file  – histogram text-format validation
- validate_regions_config         – region definition list validation
- validate_nuisance_groups_config – nuisance group list validation
- validate_friend_config          – friend/sidecar tree definition validation
- validate_plugin_config          – plugin configuration block validation
- validate_analysis_config        – top-level analysis YAML validator
"""
from __future__ import annotations

import os
import sys
import textwrap

import pytest

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

from validate_config import (
    validate_histogram_config_file,
    validate_regions_config,
    validate_nuisance_groups_config,
    validate_friend_config,
    validate_plugin_config,
    validate_analysis_config,
    ValidationError,
    _KNOWN_PLUGINS,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path, filename, content):
    p = tmp_path / filename
    p.write_text(textwrap.dedent(content))
    return str(p)


# ===========================================================================
# validate_histogram_config_file
# ===========================================================================

class TestValidateHistogramConfigFile:

    def test_valid_simple_histogram(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=myhist variable=pt bins=50 lowerBound=0.0 upperBound=500.0\n")
        errors, warnings = validate_histogram_config_file(path)
        assert errors == []

    def test_valid_multiple_histograms(self, tmp_path):
        path = _write(tmp_path, "hists.txt", """\
            name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=500.0
            name=h2 variable=eta bins=60 lowerBound=-3.0 upperBound=3.0 weight=w
        """)
        errors, warnings = validate_histogram_config_file(path)
        assert errors == []

    def test_comment_and_blank_lines_ignored(self, tmp_path):
        path = _write(tmp_path, "hists.txt", """\
            # This is a comment
            
            name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=500.0
        """)
        errors, warnings = validate_histogram_config_file(path)
        assert errors == []

    def test_missing_required_key_name(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "variable=pt bins=50 lowerBound=0.0 upperBound=500.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("name" in e for e in errors)

    def test_missing_required_key_variable(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 bins=50 lowerBound=0.0 upperBound=500.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("variable" in e for e in errors)

    def test_missing_required_key_bins(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt lowerBound=0.0 upperBound=500.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("bins" in e for e in errors)

    def test_missing_required_key_lowerBound(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 upperBound=500.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("lowerBound" in e for e in errors)

    def test_missing_required_key_upperBound(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 lowerBound=0.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("upperBound" in e for e in errors)

    def test_non_integer_bins_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=5.5 lowerBound=0.0 upperBound=100.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("bins" in e for e in errors)

    def test_zero_bins_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=0 lowerBound=0.0 upperBound=100.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("bins" in e for e in errors)

    def test_negative_bins_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=-5 lowerBound=0.0 upperBound=100.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("bins" in e for e in errors)

    def test_non_float_lowerBound_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 lowerBound=abc upperBound=100.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("lowerBound" in e for e in errors)

    def test_lower_ge_upper_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 lowerBound=100.0 upperBound=0.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("lowerBound" in e or "upperBound" in e for e in errors)

    def test_equal_bounds_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 lowerBound=5.0 upperBound=5.0\n")
        errors, _ = validate_histogram_config_file(path)
        assert any("lowerBound" in e or "upperBound" in e for e in errors)

    def test_duplicate_name_is_error(self, tmp_path):
        path = _write(tmp_path, "hists.txt", """\
            name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=500.0
            name=h1 variable=eta bins=60 lowerBound=-3.0 upperBound=3.0
        """)
        errors, _ = validate_histogram_config_file(path)
        assert any("Duplicate" in e and "h1" in e for e in errors)

    def test_file_not_found(self, tmp_path):
        errors, _ = validate_histogram_config_file("/nonexistent/path/hists.txt")
        assert any("not found" in e for e in errors)

    def test_empty_file_is_warning(self, tmp_path):
        path = _write(tmp_path, "hists.txt", "# only comments\n")
        _, warnings = validate_histogram_config_file(path)
        assert any("no histogram entries" in w for w in warnings)

    def test_unknown_key_is_warning(self, tmp_path):
        path = _write(tmp_path, "hists.txt",
                      "name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=100.0 unknownKey=val\n")
        _, warnings = validate_histogram_config_file(path)
        assert any("unknownKey" in w for w in warnings)

    def test_channel_axis_validated(self, tmp_path):
        """Channel bins must be a positive integer; channel bounds must be ordered."""
        path = _write(tmp_path, "hists.txt", (
            "name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=500.0 "
            "channelVariable=ch channelBins=0 channelLowerBound=0.0 channelUpperBound=2.0\n"
        ))
        errors, _ = validate_histogram_config_file(path)
        assert any("channelBins" in e for e in errors)

    def test_line_number_context_in_errors(self, tmp_path):
        """Error messages should include a [line N] context prefix."""
        path = _write(tmp_path, "hists.txt", """\
            # comment line
            variable=pt bins=50 lowerBound=0.0 upperBound=500.0
        """)
        errors, _ = validate_histogram_config_file(path)
        assert any("[line " in e for e in errors)


# ===========================================================================
# validate_regions_config
# ===========================================================================

class TestValidateRegionsConfig:

    def test_valid_single_root_region(self):
        regions = [{"name": "signal", "filter_column": "isSR"}]
        errors, warnings = validate_regions_config(regions, "test.yaml")
        assert errors == []

    def test_valid_hierarchy(self):
        regions = [
            {"name": "parent", "filter_column": "isParent"},
            {"name": "child", "filter_column": "isChild", "parent": "parent"},
        ]
        errors, _ = validate_regions_config(regions)
        assert errors == []

    def test_not_a_list_is_error(self):
        errors, _ = validate_regions_config({"name": "signal"}, "test.yaml")
        assert errors

    def test_item_not_a_dict_is_error(self):
        errors, _ = validate_regions_config(["signal"], "test.yaml")
        assert errors

    def test_missing_name_is_error(self):
        errors, _ = validate_regions_config(
            [{"filter_column": "isSR"}], "test.yaml"
        )
        assert any("name" in e for e in errors)

    def test_missing_filter_column_is_error(self):
        errors, _ = validate_regions_config(
            [{"name": "signal"}], "test.yaml"
        )
        assert any("filter_column" in e for e in errors)

    def test_duplicate_name_is_error(self):
        regions = [
            {"name": "signal", "filter_column": "isSR"},
            {"name": "signal", "filter_column": "isSR2"},
        ]
        errors, _ = validate_regions_config(regions, "test.yaml")
        assert any("Duplicate" in e and "signal" in e for e in errors)

    def test_unknown_parent_is_error(self):
        regions = [
            {"name": "child", "filter_column": "isChild", "parent": "nonexistent"},
        ]
        errors, _ = validate_regions_config(regions, "test.yaml")
        assert any("parent" in e or "nonexistent" in e for e in errors)

    def test_cycle_is_error(self):
        regions = [
            {"name": "a", "filter_column": "isA", "parent": "b"},
            {"name": "b", "filter_column": "isB", "parent": "a"},
        ]
        errors, _ = validate_regions_config(regions, "test.yaml")
        assert any("Cycle" in e or "cycle" in e for e in errors)

    def test_source_context_in_errors(self):
        errors, _ = validate_regions_config(
            [{"filter_column": "isSR"}], source_context="myconfig.yaml"
        )
        assert any("myconfig.yaml" in e for e in errors)

    def test_empty_list_valid(self):
        errors, _ = validate_regions_config([], "test.yaml")
        assert errors == []


# ===========================================================================
# validate_nuisance_groups_config
# ===========================================================================

class TestValidateNuisanceGroupsConfig:

    def test_valid_shape_group(self):
        groups = [
            {
                "name": "JES",
                "group_type": "shape",
                "systematics": ["JES_Abs", "JES_Flav"],
            }
        ]
        errors, warnings = validate_nuisance_groups_config(groups, "test.yaml")
        assert errors == []

    def test_valid_rate_group(self):
        groups = [
            {
                "name": "lumi",
                "group_type": "rate",
                "systematics": ["lumi_2022"],
            }
        ]
        errors, _ = validate_nuisance_groups_config(groups)
        assert errors == []

    def test_not_a_list_is_error(self):
        errors, _ = validate_nuisance_groups_config({"name": "JES"}, "test.yaml")
        assert errors

    def test_item_not_dict_is_error(self):
        errors, _ = validate_nuisance_groups_config(["JES"], "test.yaml")
        assert errors

    def test_missing_name_is_error(self):
        errors, _ = validate_nuisance_groups_config(
            [{"group_type": "shape", "systematics": ["JES"]}], "test.yaml"
        )
        assert any("name" in e for e in errors)

    def test_invalid_group_type_is_error(self):
        groups = [{"name": "JES", "group_type": "invalid_type", "systematics": ["JES"]}]
        errors, _ = validate_nuisance_groups_config(groups, "test.yaml")
        assert any("group_type" in e or "invalid_type" in e for e in errors)

    def test_invalid_output_usage_is_error(self):
        groups = [
            {
                "name": "JES",
                "group_type": "shape",
                "systematics": ["JES"],
                "output_usage": ["invalid_tool"],
            }
        ]
        errors, _ = validate_nuisance_groups_config(groups, "test.yaml")
        assert any("output_usage" in e or "invalid_tool" in e for e in errors)

    def test_empty_systematics_is_warning(self):
        groups = [{"name": "JES", "group_type": "shape", "systematics": []}]
        _, warnings = validate_nuisance_groups_config(groups, "test.yaml")
        assert any("systematics" in w or "JES" in w for w in warnings)

    def test_duplicate_name_is_error(self):
        groups = [
            {"name": "JES", "group_type": "shape", "systematics": ["JES1"]},
            {"name": "JES", "group_type": "shape", "systematics": ["JES2"]},
        ]
        errors, _ = validate_nuisance_groups_config(groups, "test.yaml")
        assert any("Duplicate" in e and "JES" in e for e in errors)

    def test_source_context_in_errors(self):
        groups = [{"group_type": "shape", "systematics": ["JES"]}]
        errors, _ = validate_nuisance_groups_config(
            groups, source_context="myconfig.yaml"
        )
        assert any("myconfig.yaml" in e for e in errors)

    def test_valid_output_usages(self):
        groups = [
            {
                "name": "JES",
                "group_type": "shape",
                "systematics": ["JES"],
                "output_usage": ["histogram", "datacard"],
            }
        ]
        errors, _ = validate_nuisance_groups_config(groups, "test.yaml")
        assert errors == []

    def test_empty_list_valid(self):
        errors, _ = validate_nuisance_groups_config([], "test.yaml")
        assert errors == []


# ===========================================================================
# validate_friend_config
# ===========================================================================

class TestValidateFriendConfig:

    def test_valid_files_based_friend(self):
        friends = [
            {
                "alias": "calib",
                "tree_name": "Events",
                "files": ["/data/calib.root"],
            }
        ]
        errors, warnings = validate_friend_config(friends, "test.yaml")
        assert errors == []

    def test_valid_directory_based_friend(self):
        friends = [
            {
                "alias": "tagger",
                "tree_name": "BTagging",
                "directory": "/data/taggers",
                "globs": [".root"],
            }
        ]
        errors, warnings = validate_friend_config(friends, "test.yaml")
        assert errors == []

    def test_not_a_list_is_error(self):
        errors, _ = validate_friend_config({"alias": "x"}, "test.yaml")
        assert errors

    def test_missing_alias_is_error(self):
        friends = [{"tree_name": "Events", "files": ["/data/f.root"]}]
        errors, _ = validate_friend_config(friends, "test.yaml")
        assert any("alias" in e for e in errors)

    def test_no_file_source_is_warning(self):
        friends = [{"alias": "calib"}]
        _, warnings = validate_friend_config(friends, "test.yaml")
        assert any("file source" in w or "files" in w or "directory" in w for w in warnings)

    def test_duplicate_alias_is_error(self):
        friends = [
            {"alias": "calib", "files": ["/a.root"]},
            {"alias": "calib", "files": ["/b.root"]},
        ]
        errors, _ = validate_friend_config(friends, "test.yaml")
        assert any("Duplicate" in e and "calib" in e for e in errors)

    def test_files_not_list_is_error(self):
        friends = [{"alias": "calib", "files": "/single/path.root"}]
        errors, _ = validate_friend_config(friends, "test.yaml")
        assert any("files" in e for e in errors)

    def test_alias_not_string_is_error(self):
        friends = [{"alias": 123, "files": ["/a.root"]}]
        errors, _ = validate_friend_config(friends, "test.yaml")
        assert any("alias" in e for e in errors)

    def test_unknown_key_is_warning(self):
        friends = [
            {"alias": "calib", "files": ["/a.root"], "unknown_key": "val"}
        ]
        _, warnings = validate_friend_config(friends, "test.yaml")
        assert any("unknown_key" in w for w in warnings)

    def test_camelcase_treename_accepted(self):
        """Accepts camelCase treeName (variant used in test_friend_config.yaml)."""
        friends = [
            {
                "alias": "calib",
                "treeName": "Events",
                "fileList": ["/data/calib.root"],
            }
        ]
        errors, _ = validate_friend_config(friends, "test.yaml")
        assert errors == []

    def test_source_context_in_errors(self):
        friends = [{"tree_name": "Events", "files": []}]
        errors, _ = validate_friend_config(
            friends, source_context="myconfig.yaml"
        )
        assert any("myconfig.yaml" in e for e in errors)

    def test_empty_list_valid(self):
        errors, _ = validate_friend_config([], "test.yaml")
        assert errors == []

    def test_item_not_dict_is_error(self):
        errors, _ = validate_friend_config(["calib"], "test.yaml")
        assert errors


# ===========================================================================
# validate_plugin_config
# ===========================================================================

class TestValidatePluginConfig:

    def test_valid_known_plugin_no_config(self):
        """Known plugin with None config (flag-style) should be valid."""
        plugins = {"RegionManager": None}
        errors, warnings = validate_plugin_config(plugins, "test.yaml")
        assert errors == []

    def test_valid_known_plugin_with_config(self):
        plugins = {
            "WeightManager": {
                "nominal_weight": "weight_nom",
                "scale_factors": [],
            }
        }
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert errors == []

    def test_unknown_plugin_is_warning(self):
        plugins = {"NonExistentPlugin": {"key": "val"}}
        _, warnings = validate_plugin_config(plugins, "test.yaml")
        assert any("NonExistentPlugin" in w for w in warnings)

    def test_plugin_config_not_dict_is_error(self):
        plugins = {"WeightManager": "string_value"}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert any("WeightManager" in e for e in errors)

    def test_wrong_type_for_key_is_error(self):
        """nominal_weight must be a string, not an integer."""
        plugins = {"WeightManager": {"nominal_weight": 123}}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert any("nominal_weight" in e for e in errors)

    def test_wrong_type_bool_key_is_error(self):
        """enabled must be a bool."""
        plugins = {"RegionManager": {"enabled": "yes"}}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert any("enabled" in e for e in errors)

    def test_unknown_key_in_plugin_config_is_warning(self):
        plugins = {"RegionManager": {"enabled": True, "nonexistent_key": "val"}}
        _, warnings = validate_plugin_config(plugins, "test.yaml")
        assert any("nonexistent_key" in w for w in warnings)

    def test_not_a_dict_is_error(self):
        errors, _ = validate_plugin_config(["RegionManager"], "test.yaml")
        assert errors

    def test_source_context_in_errors(self):
        plugins = {"WeightManager": {"nominal_weight": 999}}
        errors, _ = validate_plugin_config(
            plugins, source_context="myconfig.yaml"
        )
        assert any("myconfig.yaml" in e for e in errors)

    def test_all_known_plugins_accepted_with_empty_config(self):
        """All registered plugins accept None/empty config without error."""
        plugins = {name: None for name in _KNOWN_PLUGINS}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert errors == []

    def test_bool_true_flag_accepted(self):
        """True (flag-style enable) is treated as an empty config dict."""
        plugins = {"CutflowManager": True}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert errors == []

    def test_valid_ndhistogram_config(self):
        plugins = {
            "NDHistogramManager": {
                "histogram_config": "histograms.txt",
                "output_file": "hists.root",
                "region_aware": True,
            }
        }
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert errors == []

    def test_wrong_type_region_aware_is_error(self):
        plugins = {"NDHistogramManager": {"region_aware": "yes"}}
        errors, _ = validate_plugin_config(plugins, "test.yaml")
        assert any("region_aware" in e for e in errors)

    def test_empty_plugins_dict_valid(self):
        errors, _ = validate_plugin_config({}, "test.yaml")
        assert errors == []


# ===========================================================================
# validate_analysis_config  (top-level YAML validator)
# ===========================================================================

class TestValidateAnalysisConfig:

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(ValidationError, match="not found"):
            validate_analysis_config(str(tmp_path / "nonexistent.yaml"))

    def test_invalid_yaml_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", "regions: [unterminated\n")
        errors, _ = validate_analysis_config(path)
        assert errors

    def test_top_level_not_mapping_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", "- item1\n- item2\n")
        errors, _ = validate_analysis_config(path)
        assert errors

    def test_empty_analysis_config_ok(self, tmp_path):
        """An empty mapping is technically valid (no sections to validate)."""
        path = _write(tmp_path, "config.yaml", "{}\n")
        errors, warnings = validate_analysis_config(path)
        assert errors == []

    def test_unknown_top_level_key_warns(self, tmp_path):
        path = _write(tmp_path, "config.yaml", "unknown_section: true\n")
        _, warnings = validate_analysis_config(path)
        assert any("unknown_section" in w for w in warnings)

    def test_valid_regions_section(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            regions:
              - name: signal
                filter_column: isSR
              - name: sideband
                filter_column: isSB
        """)
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_invalid_region_missing_filter_column(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            regions:
              - name: signal
        """)
        errors, _ = validate_analysis_config(path)
        assert any("filter_column" in e for e in errors)

    def test_region_cycle_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            regions:
              - name: a
                filter_column: isA
                parent: b
              - name: b
                filter_column: isB
                parent: a
        """)
        errors, _ = validate_analysis_config(path)
        assert any("Cycle" in e or "cycle" in e for e in errors)

    def test_valid_nuisance_groups_section(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            nuisance_groups:
              - name: JES
                group_type: shape
                systematics:
                  - JES_Absolute
                  - JES_FlavorQCD
        """)
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_invalid_nuisance_group_type(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            nuisance_groups:
              - name: JES
                group_type: INVALID
                systematics: [JES]
        """)
        errors, _ = validate_analysis_config(path)
        assert any("group_type" in e or "INVALID" in e for e in errors)

    def test_histogram_config_path_resolved_relative(self, tmp_path):
        """histogram_config path relative to config file should be resolved."""
        hist_path = _write(tmp_path, "histograms.txt",
                           "name=h1 variable=pt bins=50 lowerBound=0.0 upperBound=500.0\n")
        path = _write(tmp_path, "config.yaml", "histogram_config: histograms.txt\n")
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_histogram_config_not_found_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml",
                      "histogram_config: /nonexistent/histograms.txt\n")
        errors, _ = validate_analysis_config(path)
        assert any("not found" in e or "histogram_config" in e for e in errors)

    def test_histogram_config_empty_string_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", "histogram_config: \"\"\n")
        errors, _ = validate_analysis_config(path)
        assert any("histogram_config" in e for e in errors)

    def test_valid_friend_trees_section(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            friend_trees:
              - alias: calib
                tree_name: Events
                files:
                  - /data/calib.root
        """)
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_friend_tree_missing_alias_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            friend_trees:
              - tree_name: Events
                files:
                  - /data/calib.root
        """)
        errors, _ = validate_analysis_config(path)
        assert any("alias" in e for e in errors)

    def test_valid_plugins_section(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            plugins:
              RegionManager:
                enabled: true
              WeightManager:
                nominal_weight: weight_nom
        """)
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_unknown_plugin_warns(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            plugins:
              UnknownPlugin:
                key: value
        """)
        _, warnings = validate_analysis_config(path)
        assert any("UnknownPlugin" in w for w in warnings)

    def test_plugin_wrong_type_is_error(self, tmp_path):
        path = _write(tmp_path, "config.yaml", """\
            plugins:
              WeightManager:
                nominal_weight: 999
        """)
        errors, _ = validate_analysis_config(path)
        assert any("nominal_weight" in e for e in errors)

    def test_comprehensive_valid_config(self, tmp_path):
        """A comprehensive config with all sections valid should produce no errors."""
        hist_path = _write(tmp_path, "histograms.txt",
                           "name=ZMass variable=m_ll bins=60 lowerBound=70.0 upperBound=110.0\n")
        path = _write(tmp_path, "config.yaml", f"""\
            regions:
              - name: signal
                filter_column: isSR
                description: Signal region
              - name: control
                filter_column: isCR

            nuisance_groups:
              - name: JES
                group_type: shape
                systematics:
                  - JES_Absolute
                processes: []
                regions: []

            histogram_config: histograms.txt

            friend_trees:
              - alias: calib
                tree_name: Events
                files:
                  - /data/calib.root
                index_branches:
                  - run
                  - luminosityBlock

            plugins:
              RegionManager:
                enabled: true
              CutflowManager: true
              NDHistogramManager:
                histogram_config: histograms.txt
                region_aware: false
        """)
        errors, warnings = validate_analysis_config(path)
        assert errors == []

    def test_error_messages_include_file_context(self, tmp_path):
        """All error messages should reference the config file path."""
        path = _write(tmp_path, "myanalysis.yaml", """\
            regions:
              - name: signal
        """)
        errors, _ = validate_analysis_config(path)
        assert errors
        assert all(str(path) in e for e in errors), (
            f"Expected all errors to contain file path {path!r}; got: {errors}"
        )

    def test_friends_key_alias_accepted(self, tmp_path):
        """Both 'friend_trees' and 'friends' top-level keys are accepted."""
        path = _write(tmp_path, "config.yaml", """\
            friends:
              - alias: calib
                files:
                  - /data/calib.root
        """)
        errors, _ = validate_analysis_config(path)
        assert errors == []

    def test_both_friend_keys_warns(self, tmp_path):
        """Having both 'friend_trees' and 'friends' keys produces a warning."""
        path = _write(tmp_path, "config.yaml", """\
            friend_trees:
              - alias: calib
                files:
                  - /data/calib.root
            friends:
              - alias: tagger
                files:
                  - /data/tagger.root
        """)
        _, warnings = validate_analysis_config(path)
        assert any("friend_trees" in w and "friends" in w for w in warnings)
