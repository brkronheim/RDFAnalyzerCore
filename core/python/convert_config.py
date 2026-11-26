#!/usr/bin/env python3
"""Migrate a native analysis mapping, record list, or string list to YAML."""
import argparse
from pathlib import Path

from native_config_io import get_config_extension, load_yaml_document, read_config, read_legacy_aux, read_stxs_table, write_config_yaml


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--kind", choices=("mapping", "records", "list", "samples", "stxs_bins", "stxs_migration"), default="mapping",
                        help="Use records for plugin definitions, list for skim columns.")
    args = parser.parse_args()
    try:
        if args.input.resolve() == args.output.resolve():
            raise ValueError("Supply a different output path to keep the original file.")
        if get_config_extension(args.input) != ".yaml":
            value = read_legacy_aux(args.input, args.kind)
        elif args.kind in ("stxs_bins", "stxs_migration"):
            value = read_stxs_table(args.input, args.kind)
        elif args.kind == "samples":
            from dataset_manifest import DatasetManifest
            manifest = DatasetManifest.load_yaml(str(args.input))
            errors = [message for message in manifest.validate() if not message.startswith("Warning:")]
            if errors:
                raise ValueError("; ".join(errors))
            value = manifest.as_yaml_document()
        elif args.kind == "mapping":
            value = read_config(args.input)
        else:
            value = load_yaml_document(args.input)
            item_type = dict if args.kind == "records" else str
            if not isinstance(value, list) or not all(isinstance(item, item_type) for item in value):
                raise ValueError(f"{args.input}: expected a sequence of {item_type.__name__} entries")
        write_config_yaml(value, args.output)
    except (OSError, ValueError) as exc:
        parser.exit(1, f"Conversion failed: {exc}\n")


if __name__ == "__main__":
    main()
