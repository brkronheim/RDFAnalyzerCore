import argparse
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

from submission_backend import read_config
from dataset_manifest import DatasetManifest, FriendTreeConfig
from output_schema import RegionDefinition, validate_region_hierarchy, NuisanceGroupDefinition, NUISANCE_GROUP_TYPES, NUISANCE_GROUP_OUTPUT_USAGES


class ValidationError(Exception):
    pass


# ---------------------------------------------------------------------------
# Plugin schema registry
# ---------------------------------------------------------------------------

#: Schemas for known plugin configuration blocks.  Each entry maps a plugin
#: name to a dict describing its ``required_keys``, ``optional_keys``, and
#: ``key_types`` (a mapping of key → acceptable Python type(s)).
#:
#: An empty ``required_keys`` list means the plugin block is optional or all
#: keys are optional.  Keys not listed under ``optional_keys`` or
#: ``required_keys`` are flagged as unknown.
_KNOWN_PLUGINS: Dict[str, Dict[str, Any]] = {
    "RegionManager": {
        "required_keys": [],
        "optional_keys": ["enabled"],
        "key_types": {"enabled": bool},
    },
    "WeightManager": {
        "required_keys": [],
        "optional_keys": [
            "nominal_weight",
            "scale_factors",
            "normalizations",
            "weight_variations",
        ],
        "key_types": {
            "nominal_weight": str,
            "scale_factors": list,
            "normalizations": list,
            "weight_variations": list,
        },
    },
    "NDHistogramManager": {
        "required_keys": [],
        "optional_keys": ["histogram_config", "output_file", "region_aware"],
        "key_types": {
            "histogram_config": str,
            "output_file": str,
            "region_aware": bool,
        },
    },
    "CutflowManager": {
        "required_keys": [],
        "optional_keys": ["enabled"],
        "key_types": {"enabled": bool},
    },
    "CorrectionManager": {
        "required_keys": [],
        "optional_keys": ["corrections"],
        "key_types": {"corrections": list},
    },
    "BDTManager": {
        "required_keys": [],
        "optional_keys": ["models"],
        "key_types": {"models": list},
    },
    "OnnxManager": {
        "required_keys": [],
        "optional_keys": ["models"],
        "key_types": {"models": list},
    },
    "SofieManager": {
        "required_keys": [],
        "optional_keys": ["models"],
        "key_types": {"models": list},
    },
    "TriggerManager": {
        "required_keys": [],
        "optional_keys": ["triggers"],
        "key_types": {"triggers": list},
    },
    "GoldenJsonManager": {
        "required_keys": [],
        "optional_keys": ["json_files"],
        "key_types": {"json_files": list},
    },
    "KinematicFitManager": {
        "required_keys": [],
        "optional_keys": ["fits"],
        "key_types": {"fits": list},
    },
    "NamedObjectManager": {
        "required_keys": [],
        "optional_keys": ["objects"],
        "key_types": {"objects": list},
    },
}


# ---------------------------------------------------------------------------
# Histogram config text-format validator
# ---------------------------------------------------------------------------

#: Required keys in each histogram configuration line.
_HISTOGRAM_REQUIRED_KEYS = ["name", "variable", "bins", "lowerBound", "upperBound"]

#: All recognised keys in a histogram configuration line.
_HISTOGRAM_KNOWN_KEYS = {
    "name", "variable", "weight", "bins", "lowerBound", "upperBound", "label",
    "suffix",
    "channelVariable", "channelBins", "channelLowerBound", "channelUpperBound",
    "channelRegions",
    "controlRegionVariable", "controlRegionBins", "controlRegionLowerBound",
    "controlRegionUpperBound", "controlRegionRegions",
    "sampleCategoryVariable", "sampleCategoryBins",
    "sampleCategoryLowerBound", "sampleCategoryUpperBound",
    "sampleCategoryRegions",
}


def validate_histogram_config_file(
    path: str,
) -> Tuple[List[str], List[str]]:
    """Validate a histogram configuration text file.

    Each non-comment, non-blank line must be a ``key=value`` record
    containing at least the required keys (``name``, ``variable``, ``bins``,
    ``lowerBound``, ``upperBound``).  Numeric fields are type-checked and bin
    counts must be positive.  Duplicate histogram names are reported as
    errors.

    Parameters
    ----------
    path : str
        Absolute or relative path to the histogram config text file.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)`` lists, each item including a line-number
        context prefix such as ``"[line 3]"``.
    """
    errors: List[str] = []
    warnings: List[str] = []

    if not os.path.exists(path):
        errors.append(f"Histogram config file not found: {path}")
        return errors, warnings

    seen_names: Dict[str, int] = {}
    entries_found = 0

    with open(path) as fh:
        for lineno, raw in enumerate(fh, start=1):
            line = raw.split("#")[0].strip()
            if not line:
                continue

            parts = line.split()
            entry: Dict[str, str] = {}
            for part in parts:
                if "=" not in part:
                    warnings.append(
                        f"[line {lineno}] Token without '=' ignored: {part!r}"
                    )
                    continue
                k, v = part.split("=", 1)
                entry[k.strip()] = v.strip()

            if not entry:
                continue

            ctx = f"[line {lineno}]"
            entries_found += 1

            # Check required keys
            for req in _HISTOGRAM_REQUIRED_KEYS:
                if req not in entry:
                    errors.append(
                        f"{ctx} Missing required key '{req}'"
                    )

            # Check for unknown keys
            for k in entry:
                if k not in _HISTOGRAM_KNOWN_KEYS:
                    warnings.append(
                        f"{ctx} Unknown histogram config key '{k}'"
                    )

            # Validate numeric fields
            for num_key in ("bins", "lowerBound", "upperBound",
                            "channelBins", "channelLowerBound", "channelUpperBound",
                            "controlRegionBins", "controlRegionLowerBound",
                            "controlRegionUpperBound",
                            "sampleCategoryBins", "sampleCategoryLowerBound",
                            "sampleCategoryUpperBound"):
                if num_key not in entry:
                    continue
                if num_key in ("bins", "channelBins", "controlRegionBins",
                               "sampleCategoryBins"):
                    if not _is_int(entry[num_key]):
                        errors.append(
                            f"{ctx} Key '{num_key}' must be an integer, got {entry[num_key]!r}"
                        )
                    elif int(entry[num_key]) <= 0:
                        errors.append(
                            f"{ctx} Key '{num_key}' must be a positive integer, got {entry[num_key]!r}"
                        )
                else:
                    if not _is_float(entry[num_key]):
                        errors.append(
                            f"{ctx} Key '{num_key}' must be a float, got {entry[num_key]!r}"
                        )

            # Validate bound ordering: lowerBound < upperBound
            for lower_key, upper_key in (
                ("lowerBound", "upperBound"),
                ("channelLowerBound", "channelUpperBound"),
                ("controlRegionLowerBound", "controlRegionUpperBound"),
                ("sampleCategoryLowerBound", "sampleCategoryUpperBound"),
            ):
                if lower_key in entry and upper_key in entry:
                    if _is_float(entry[lower_key]) and _is_float(entry[upper_key]):
                        if float(entry[lower_key]) >= float(entry[upper_key]):
                            errors.append(
                                f"{ctx} '{lower_key}' ({entry[lower_key]}) must be "
                                f"less than '{upper_key}' ({entry[upper_key]})"
                            )

            # Duplicate name detection
            name = entry.get("name", "")
            if name:
                if name in seen_names:
                    errors.append(
                        f"{ctx} Duplicate histogram name '{name}' "
                        f"(first seen at line {seen_names[name]})"
                    )
                else:
                    seen_names[name] = lineno

    if entries_found == 0:
        warnings.append("Histogram config file contains no histogram entries")

    return errors, warnings


# ---------------------------------------------------------------------------
# Region config validator
# ---------------------------------------------------------------------------


def validate_regions_config(
    regions_data: Any,
    source_context: str = "",
) -> Tuple[List[str], List[str]]:
    """Validate a list of region definition dicts.

    Parameters
    ----------
    regions_data : any
        The raw value from the ``regions`` key in an analysis config.  Must
        be a list of dicts.
    source_context : str
        Human-readable label for the source (e.g. the config file path)
        prepended to error messages so the user can pinpoint the problem.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.
    """
    errors: List[str] = []
    warnings: List[str] = []
    ctx = f"[{source_context}] " if source_context else ""

    if not isinstance(regions_data, list):
        errors.append(f"{ctx}regions must be a list, got {type(regions_data).__name__}")
        return errors, warnings

    region_objects: List[RegionDefinition] = []
    for i, item in enumerate(regions_data):
        entry_ctx = f"{ctx}regions[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{entry_ctx}: entry must be a dict, got {type(item).__name__}")
            continue
        if "name" not in item:
            errors.append(f"{entry_ctx}: missing required key 'name'")
        if "filter_column" not in item:
            errors.append(f"{entry_ctx}: missing required key 'filter_column'")
        try:
            region_objects.append(RegionDefinition.from_dict(item))
        except Exception as exc:
            errors.append(f"{entry_ctx}: failed to parse region definition: {exc}")

    # Hierarchy validation (cycles, unknown parents, duplicates) via output_schema
    hierarchy_errors = validate_region_hierarchy(region_objects)
    errors.extend(f"{ctx}{e}" for e in hierarchy_errors)

    return errors, warnings


# ---------------------------------------------------------------------------
# Nuisance group config validator
# ---------------------------------------------------------------------------


def validate_nuisance_groups_config(
    nuisance_data: Any,
    source_context: str = "",
) -> Tuple[List[str], List[str]]:
    """Validate a list of nuisance group definition dicts.

    Parameters
    ----------
    nuisance_data : any
        The raw value from the ``nuisance_groups`` key.  Must be a list of
        dicts.
    source_context : str
        Human-readable label for the source config file.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.
    """
    errors: List[str] = []
    warnings: List[str] = []
    ctx = f"[{source_context}] " if source_context else ""

    if not isinstance(nuisance_data, list):
        errors.append(
            f"{ctx}nuisance_groups must be a list, got {type(nuisance_data).__name__}"
        )
        return errors, warnings

    seen_names: Dict[str, int] = {}
    for i, item in enumerate(nuisance_data):
        entry_ctx = f"{ctx}nuisance_groups[{i}]"
        if not isinstance(item, dict):
            errors.append(
                f"{entry_ctx}: entry must be a dict, got {type(item).__name__}"
            )
            continue

        # Required keys
        if "name" not in item:
            errors.append(f"{entry_ctx}: missing required key 'name'")

        # Validate via the schema class
        try:
            ng = NuisanceGroupDefinition.from_dict(item)
        except Exception as exc:
            errors.append(
                f"{entry_ctx}: failed to parse nuisance group definition: {exc}"
            )
            continue

        for e in ng.validate():
            errors.append(f"{entry_ctx} ('{ng.name}'): {e}")

        # Duplicate name detection
        if ng.name:
            if ng.name in seen_names:
                errors.append(
                    f"{entry_ctx}: Duplicate nuisance group name '{ng.name}' "
                    f"(first seen at index {seen_names[ng.name]})"
                )
            else:
                seen_names[ng.name] = i

        # Warn if no systematics declared
        if not ng.systematics:
            warnings.append(
                f"{entry_ctx} ('{ng.name}'): nuisance group has no systematics declared"
            )

        # Validate output_usage values
        for usage in item.get("output_usage", []):
            if usage not in NUISANCE_GROUP_OUTPUT_USAGES:
                errors.append(
                    f"{entry_ctx} ('{ng.name}'): unsupported output_usage value "
                    f"'{usage}'; must be one of {NUISANCE_GROUP_OUTPUT_USAGES}"
                )

    return errors, warnings


# ---------------------------------------------------------------------------
# Friend/sidecar tree config validator
# ---------------------------------------------------------------------------

#: Required keys in each friend tree definition dict.
_FRIEND_REQUIRED_KEYS = ["alias"]

#: All recognised keys in a friend tree definition.
_FRIEND_KNOWN_KEYS = {
    "alias", "tree_name",
    # YAML variants (camelCase) used in test_friend_config.yaml
    "treeName",
    "files", "fileList",
    "directory",
    "globs", "antiglobs",
    "index_branches", "indexBranches",
}


def validate_friend_config(
    friends_data: Any,
    source_context: str = "",
) -> Tuple[List[str], List[str]]:
    """Validate a list of friend/sidecar tree configuration dicts.

    Parameters
    ----------
    friends_data : any
        Raw value from the ``friend_trees`` key (or ``friends`` key).  Must
        be a list of dicts.
    source_context : str
        Human-readable label for the source config file.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.
    """
    errors: List[str] = []
    warnings: List[str] = []
    ctx = f"[{source_context}] " if source_context else ""

    if not isinstance(friends_data, list):
        errors.append(
            f"{ctx}friend_trees must be a list, got {type(friends_data).__name__}"
        )
        return errors, warnings

    seen_aliases: Dict[str, int] = {}
    for i, item in enumerate(friends_data):
        entry_ctx = f"{ctx}friend_trees[{i}]"
        if not isinstance(item, dict):
            errors.append(
                f"{entry_ctx}: entry must be a dict, got {type(item).__name__}"
            )
            continue

        # Required keys
        for req in _FRIEND_REQUIRED_KEYS:
            if req not in item:
                errors.append(f"{entry_ctx}: missing required key '{req}'")

        alias = item.get("alias", "")

        # Duplicate alias detection
        if alias:
            if alias in seen_aliases:
                errors.append(
                    f"{entry_ctx}: Duplicate friend tree alias '{alias}' "
                    f"(first seen at index {seen_aliases[alias]})"
                )
            else:
                seen_aliases[alias] = i

        # Must have at least one file source (files/fileList or directory)
        has_files = bool(item.get("files") or item.get("fileList"))
        has_directory = bool(item.get("directory"))
        if not has_files and not has_directory:
            warnings.append(
                f"{entry_ctx} (alias '{alias}'): no file source specified; "
                "provide 'files' or 'directory'"
            )

        # Validate list fields
        for list_key in ("files", "fileList", "globs", "antiglobs",
                         "index_branches", "indexBranches"):
            val = item.get(list_key)
            if val is not None and not isinstance(val, list):
                errors.append(
                    f"{entry_ctx} (alias '{alias}'): '{list_key}' must be a list"
                )

        # Validate string fields
        for str_key in ("alias", "tree_name", "treeName", "directory"):
            val = item.get(str_key)
            if val is not None and not isinstance(val, str):
                errors.append(
                    f"{entry_ctx}: '{str_key}' must be a string"
                )

        # Unknown key warnings
        for k in item:
            if k not in _FRIEND_KNOWN_KEYS:
                warnings.append(
                    f"{entry_ctx} (alias '{alias}'): unknown key '{k}'"
                )

    return errors, warnings


# ---------------------------------------------------------------------------
# Plugin config validator
# ---------------------------------------------------------------------------


def validate_plugin_config(
    plugins_data: Any,
    source_context: str = "",
) -> Tuple[List[str], List[str]]:
    """Validate a mapping of plugin name → configuration dict.

    Each key in *plugins_data* must be a recognised plugin name
    (see :data:`_KNOWN_PLUGINS`).  For each plugin the required keys are
    checked to be present and the declared key types are enforced.

    Parameters
    ----------
    plugins_data : any
        Raw value from the ``plugins`` key in an analysis config.  Must be a
        dict mapping plugin names to their config dicts (or ``None`` /
        ``true`` for plugins with no configuration).
    source_context : str
        Human-readable label for the source config file.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)``.
    """
    errors: List[str] = []
    warnings: List[str] = []
    ctx = f"[{source_context}] " if source_context else ""

    if not isinstance(plugins_data, dict):
        errors.append(
            f"{ctx}plugins must be a mapping (dict), got {type(plugins_data).__name__}"
        )
        return errors, warnings

    for plugin_name, plugin_cfg in plugins_data.items():
        entry_ctx = f"{ctx}plugins.{plugin_name}"

        if plugin_name not in _KNOWN_PLUGINS:
            warnings.append(
                f"{entry_ctx}: unknown plugin name '{plugin_name}'; "
                f"known plugins are {sorted(_KNOWN_PLUGINS)}"
            )
            continue

        schema = _KNOWN_PLUGINS[plugin_name]
        required = schema.get("required_keys", [])
        optional = schema.get("optional_keys", [])
        key_types = schema.get("key_types", {})

        # plugin_cfg may be None/True (flag-style) or a dict
        if plugin_cfg is None or plugin_cfg is True or plugin_cfg is False:
            # Treat as an empty config dict – only valid if no required keys
            plugin_cfg_dict: Dict[str, Any] = {}
        elif not isinstance(plugin_cfg, dict):
            errors.append(
                f"{entry_ctx}: plugin config must be a mapping, got "
                f"{type(plugin_cfg).__name__}"
            )
            continue
        else:
            plugin_cfg_dict = plugin_cfg

        # Check required keys
        for req in required:
            if req not in plugin_cfg_dict:
                errors.append(
                    f"{entry_ctx}: missing required key '{req}'"
                )

        # Check key types
        all_known = set(required) | set(optional)
        for k, v in plugin_cfg_dict.items():
            if k not in all_known:
                warnings.append(
                    f"{entry_ctx}: unknown config key '{k}'"
                )
                continue
            expected_type = key_types.get(k)
            if expected_type is not None and not isinstance(v, expected_type):
                errors.append(
                    f"{entry_ctx}: key '{k}' must be of type "
                    f"{expected_type.__name__}, got {type(v).__name__}"
                )

    return errors, warnings


# ---------------------------------------------------------------------------
# Top-level analysis config validator
# ---------------------------------------------------------------------------

#: Recognised top-level keys in an analysis config YAML.
_ANALYSIS_CONFIG_KNOWN_KEYS = {
    "regions",
    "nuisance_groups",
    "histogram_config",
    "friend_trees",
    "plugins",
}


def validate_analysis_config(
    config_path: str,
) -> Tuple[List[str], List[str]]:
    """Validate a comprehensive analysis configuration YAML file.

    The file may contain any of the following top-level keys:

    ``regions``
        List of region definition dicts validated against
        :class:`~output_schema.RegionDefinition` schema and hierarchy rules.

    ``nuisance_groups``
        List of nuisance group definition dicts validated against
        :class:`~output_schema.NuisanceGroupDefinition` schema.

    ``histogram_config``
        Path to a histogram text-format configuration file.  The path is
        resolved relative to the directory containing the analysis config.
        The file is parsed and each entry is validated.

    ``friend_trees``
        List of friend/sidecar tree configuration dicts.

    ``plugins``
        Mapping of plugin name → configuration dict.  Plugin names are
        validated against the known plugin registry and required/optional
        keys are checked.

    Parameters
    ----------
    config_path : str
        Path to the analysis configuration YAML file.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)`` lists.  Errors must be fixed before
        execution; warnings are advisory.

    Raises
    ------
    ValidationError
        If *config_path* does not exist.
    """
    errors: List[str] = []
    warnings: List[str] = []

    config_path = os.path.abspath(config_path)
    if not os.path.exists(config_path):
        raise ValidationError(f"Analysis config file not found: {config_path}")

    try:
        with open(config_path) as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        errors.append(f"Failed to parse analysis config YAML: {exc}")
        return errors, warnings

    if not isinstance(data, dict):
        errors.append(
            "Analysis config must be a YAML mapping at the top level, "
            f"got {type(data).__name__ if data is not None else 'null'}"
        )
        return errors, warnings

    config_dir = os.path.dirname(config_path)
    source_context = config_path

    # Warn about unrecognised top-level keys
    for k in data:
        if k not in _ANALYSIS_CONFIG_KNOWN_KEYS:
            warnings.append(
                f"[{source_context}] Unknown top-level key '{k}'"
            )

    # ------------------------------------------------------------------ regions
    if "regions" in data:
        errs, warns = validate_regions_config(data["regions"], source_context)
        errors.extend(errs)
        warnings.extend(warns)

    # --------------------------------------------------------- nuisance_groups
    if "nuisance_groups" in data:
        errs, warns = validate_nuisance_groups_config(
            data["nuisance_groups"], source_context
        )
        errors.extend(errs)
        warnings.extend(warns)

    # -------------------------------------------------------- histogram_config
    if "histogram_config" in data:
        hist_cfg_val = data["histogram_config"]
        if not isinstance(hist_cfg_val, str) or not hist_cfg_val.strip():
            errors.append(
                f"[{source_context}] 'histogram_config' must be a non-empty string path"
            )
        else:
            hist_path = (
                hist_cfg_val
                if os.path.isabs(hist_cfg_val)
                else os.path.join(config_dir, hist_cfg_val)
            )
            errs, warns = validate_histogram_config_file(hist_path)
            errors.extend(
                f"[{source_context}] histogram_config '{hist_path}': {e}"
                for e in errs
            )
            warnings.extend(
                f"[{source_context}] histogram_config '{hist_path}': {w}"
                for w in warns
            )

    # ----------------------------------------------------------- friend_trees
    if "friend_trees" in data and "friends" in data:
        warnings.append(
            f"[{source_context}] Both 'friend_trees' and 'friends' keys are present; "
            "only 'friend_trees' will be validated – remove the duplicate key"
        )
    for key in ("friend_trees", "friends"):
        if key in data:
            errs, warns = validate_friend_config(data[key], source_context)
            errors.extend(errs)
            warnings.extend(warns)
            break

    # --------------------------------------------------------------- plugins
    if "plugins" in data:
        errs, warns = validate_plugin_config(data["plugins"], source_context)
        errors.extend(errs)
        warnings.extend(warns)

    return errors, warnings


def _resolve_path(base_config_path: str, value: str) -> str:
    if not value:
        return value
    if os.path.isabs(value):
        return value
    config_dir = os.path.dirname(os.path.abspath(base_config_path))
    base_dir = os.path.abspath(os.path.join(config_dir, ".."))
    return os.path.abspath(os.path.join(base_dir, value))


def _parse_sample_config(sample_config_path: str):
    entries = []
    with open(sample_config_path) as file:
        for idx, line in enumerate(file, start=1):
            raw = line
            line = line.split("#")[0].strip()
            if not line:
                continue
            parts = line.split()
            entry = {}
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    entry[k.strip()] = v.strip()
            if entry:
                entry["__line__"] = idx
                entry["__raw__"] = raw.strip()
                entries.append(entry)
    return entries


def _is_float(val: str) -> bool:
    try:
        float(val)
        return True
    except Exception:
        return False


def _is_int(val: str) -> bool:
    try:
        int(val)
        return True
    except Exception:
        return False


def _validate_yaml_sample_config(sample_config_path: str, mode: str = "auto"):
    """Validate a YAML dataset manifest used as a sample config.

    Uses :class:`~dataset_manifest.DatasetManifest` to load and intrinsically
    validate the manifest (duplicate names, missing parent references, etc.),
    then applies mode-specific field checks.

    Parameters
    ----------
    sample_config_path : str
        Absolute path to the ``.yaml`` / ``.yml`` manifest file.
    mode : str
        ``"nano"``, ``"opendata"``, or ``"auto"`` (default).  In ``"auto"``
        mode the function infers the mode from the manifest content: if any
        entry carries a ``das`` field that looks like a CERN Open Data record
        ID (purely numeric) it is treated as ``"opendata"``; otherwise
        ``"nano"``.

    Returns
    -------
    tuple[list[str], list[str]]
        ``(errors, warnings)`` lists.
    """
    errors = []
    warnings = []

    try:
        manifest = DatasetManifest.load_yaml(sample_config_path)
    except Exception as exc:
        errors.append(f"Failed to parse YAML sample config: {exc}")
        return errors, warnings

    # Run the manifest's own internal consistency checks
    for msg in manifest.validate():
        if msg.startswith("Warning:"):
            warnings.append(msg)
        else:
            errors.append(msg)

    if not manifest.datasets:
        errors.append("Sample config (YAML manifest) contains no dataset entries")
        return errors, warnings

    # Auto-detect mode: if any entry's das field looks like a numeric record ID
    # (CERN Open Data) rather than a DAS path (starts with "/"), use opendata.
    mode_local = mode.lower()
    if mode_local == "auto":
        def _looks_like_recid(das_str):
            if not das_str:
                return False
            return all(part.strip().isdigit() for part in das_str.split(",") if part.strip())
        has_recids = any(
            _looks_like_recid(e.das) for e in manifest.datasets if e.das
        )
        mode_local = "opendata" if has_recids else "nano"

    if mode_local == "opendata":
        # For Open Data, each entry's das field should contain record IDs
        entries_with_das = [e for e in manifest.datasets if e.das]
        if not entries_with_das:
            errors.append("OpenData mode: no dataset entries have a 'das' (record ID) field")
        for entry in manifest.datasets:
            if not entry.das:
                warnings.append(
                    f"OpenData dataset {entry.name!r} has no 'das' (record ID) field"
                )
    else:
        # NANO mode: each entry should have a file source (das or files) and xsec (for MC)
        for entry in manifest.datasets:
            if not entry.das and not entry.files:
                warnings.append(
                    f"NANO dataset {entry.name!r} has neither 'das' nor 'files'"
                )
            if entry.dtype == "mc" and entry.xsec is None:
                warnings.append(
                    f"NANO MC dataset {entry.name!r} is missing 'xsec'"
                )
            if entry.sum_weights is None:
                warnings.append(
                    f"NANO dataset {entry.name!r} has no 'sum_weights': "
                    "normalization will be calculated on the fly"
                )

    return errors, warnings


def validate_submit_config(config_path: str, mode: str = "auto"):
    errors = []
    warnings = []
    config_path = os.path.abspath(config_path)
    if not os.path.exists(config_path):
        raise ValidationError(f"Config file not found: {config_path}")

    cfg = read_config(config_path)

    # 'saveConfig' is optional: when missing, we will warn and snapshot will save the full dataframe.
    required_keys = ["sampleConfig", "saveDirectory", "saveTree"]
    for key in required_keys:
        if key not in cfg or not cfg[key].strip():
            errors.append(f"Missing required key '{key}' in submit config")

    # File existence checks
    for key in ["sampleConfig", "floatConfig", "intConfig"]:
        if key in cfg and cfg[key].strip():
            p = _resolve_path(config_path, cfg[key])
            if not os.path.exists(p):
                errors.append(f"File not found for '{key}': {p}")

    # saveConfig is optional; if provided, warn if the referenced file doesn't exist
    if "saveConfig" in cfg and cfg["saveConfig"].strip():
        p = _resolve_path(config_path, cfg["saveConfig"])
        if not os.path.exists(p):
            warnings.append(f"File not found for 'saveConfig': {p}")

    # threads validation (optional)
    if "threads" in cfg and cfg["threads"].strip():
        threads_val = cfg["threads"].strip().lower()
        if threads_val not in {"auto", "max"} and not _is_int(threads_val):
            errors.append("Invalid 'threads' value (expected int, 'auto', or 'max')")

    sample_config = cfg.get("sampleConfig", "")
    if sample_config:
        sample_config_path = _resolve_path(config_path, sample_config)
        if os.path.exists(sample_config_path):
            ext = os.path.splitext(sample_config_path)[1].lower()
            if ext in (".yaml", ".yml"):
                # Validate as a DatasetManifest YAML file
                errs, warns = _validate_yaml_sample_config(sample_config_path, mode)
                errors.extend(errs)
                warnings.extend(warns)
            else:
                entries = _parse_sample_config(sample_config_path)
                has_recids = any("recids" in e for e in entries)
                mode_local = mode.lower()
                if mode_local == "auto":
                    mode_local = "opendata" if has_recids else "nano"

                sample_entries = [e for e in entries if "name" in e]
                if not sample_entries:
                    errors.append("Sample config has no entries with 'name='")

                if mode_local == "opendata":
                    recid_entries = [e for e in entries if "recids" in e]
                    if not recid_entries:
                        errors.append("OpenData mode: missing 'recids' entry in sample config")
                    for e in sample_entries:
                        if "das" not in e:
                            errors.append(
                                f"OpenData sample missing 'das' (line {e['__line__']})"
                            )
                        for key in ["xsec", "norm", "kfac", "extraScale", "lumi"]:
                            if key in e and not _is_float(e[key]):
                                errors.append(
                                    f"Invalid float for '{key}' in sample config (line {e['__line__']})"
                                )
                        if "type" in e and not _is_int(e["type"]):
                            errors.append(
                                f"Invalid int for 'type' in sample config (line {e['__line__']})"
                            )
                else:
                    for e in sample_entries:
                        missing = [k for k in ["das", "xsec", "type"] if k not in e]
                        if missing:
                            errors.append(
                                f"NANO sample missing {', '.join(missing)} (line {e['__line__']})"
                            )
                        if "xsec" in e and not _is_float(e["xsec"]):
                            errors.append(
                                f"Invalid float for 'xsec' in sample config (line {e['__line__']})"
                            )
                        if "norm" not in e:
                            warnings.append(
                                f"Missing 'norm' in sample config (line {e['__line__']}): 'norm' will be calculated on the fly and is deprecated"
                            )
                        elif not _is_float(e["norm"]):
                            errors.append(
                                f"Invalid float for 'norm' in sample config (line {e['__line__']})"
                            )
                        if "type" in e and not _is_int(e["type"]):
                            errors.append(
                                f"Invalid int for 'type' in sample config (line {e['__line__']})"
                            )
        else:
            errors.append(f"Sample config file not found: {sample_config_path}")

    return errors, warnings


def main():
    parser = argparse.ArgumentParser(
        "Validate submission, sample, and analysis configs"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--config", help="Path to submit config (key=value text file)")
    group.add_argument(
        "--analysis-config",
        help="Path to analysis configuration YAML file (regions, nuisances, "
             "histograms, friend trees, plugins)",
    )
    parser.add_argument(
        "--mode",
        choices=["auto", "nano", "opendata"],
        default="auto",
        help="Validation mode for submit config (auto detects from sample config)",
    )
    args = parser.parse_args()

    try:
        if args.analysis_config:
            errors, warnings = validate_analysis_config(args.analysis_config)
        else:
            errors, warnings = validate_submit_config(args.config, args.mode)
    except ValidationError as exc:
        print(str(exc))
        raise SystemExit(1)

    if warnings:
        print("Config validation warnings:")
        for w in warnings:
            print(f"- {w}")

    if errors:
        print("Config validation failed:")
        for err in errors:
            print(f"- {err}")
        raise SystemExit(1)

    print("Config validation OK")


if __name__ == "__main__":
    main()
