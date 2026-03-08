import argparse
import os
from pathlib import Path

from submission_backend import read_config
from dataset_manifest import DatasetManifest


class ValidationError(Exception):
    pass


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
    parser = argparse.ArgumentParser("Validate submission and sample configs")
    parser.add_argument("--config", required=True, help="Path to submit config")
    parser.add_argument(
        "--mode",
        choices=["auto", "nano", "opendata"],
        default="auto",
        help="Validation mode (auto detects from sample config)",
    )
    args = parser.parse_args()

    try:
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
