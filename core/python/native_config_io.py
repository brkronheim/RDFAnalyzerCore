"""Native analysis configuration I/O shared by the controller and generated workers.

The C++ configuration ABI exposes strings. Preserve scalar spelling (including
leading zeros and values like "on") when lowering native YAML into that model.
"""
from pathlib import Path


def get_config_extension(config_file):
    return ".yaml" if Path(config_file).suffix in (".yaml", ".yml") else ".txt"


def parse_config_text(lines, source, *, strict=False):
    """Compatibility decoder for historical key=value configuration."""
    config = {}
    for lineno, raw in enumerate(lines, 1):
        line = raw.partition("#")[0]
        key, separator, value = line.partition("=")
        key = key.strip()
        if strict and line.strip() and not (key and separator):
            raise ValueError(f"{source}:{lineno}: expected key=value, got {line.strip()!r}")
        if key and separator:
            if key in config:
                raise ValueError(f"Duplicate key '{key}' in text config '{source}' at line {lineno}")
            config[key] = value.strip()
    return config


def read_config_text(config_file):
    with open(config_file, encoding="utf-8") as stream:
        return parse_config_text(stream, config_file)


def _load_yaml(stream, config_file, *, typed=False):
    """Load one YAML document, rejecting ambiguous or duplicate mapping keys."""
    import yaml

    class Loader(yaml.SafeLoader if typed else yaml.BaseLoader):
        def construct_mapping(self, node, deep=False):
            seen = set()
            for key, _ in node.value:
                if not isinstance(key, yaml.ScalarNode) or not key.value or key.tag == "tag:yaml.org,2002:null":
                    raise ValueError(f"{config_file}:{key.start_mark.line + 1}: expected a nonempty scalar key")
                identity = self.construct_object(key, deep=deep) if typed and key.tag != "tag:yaml.org,2002:merge" else key.value
                if identity in seen:
                    raise ValueError(f"{config_file}:{key.start_mark.line + 1}: Duplicate key '{key.value}'")
                seen.add(identity)
            return super().construct_mapping(node, deep)

    if not typed:
        # Preserve every scalar spelling except YAML null, which represents an unset option.
        Loader.yaml_implicit_resolvers = {
            initial: [rule for rule in rules if rule[0] == "tag:yaml.org,2002:null"]
            for initial, rules in yaml.SafeLoader.yaml_implicit_resolvers.items()
        }
        Loader.add_constructor("tag:yaml.org,2002:null", lambda loader, node: None)

    try:
        return yaml.load(stream, Loader=Loader)
    except yaml.YAMLError as exc:
        raise ValueError(f"{config_file}: {exc}") from exc


def load_yaml_document(config_file, *, typed=False):
    with open(config_file, encoding="utf-8") as stream:
        return _load_yaml(stream, config_file, typed=typed)


def _config_string(value, key, source):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    if isinstance(value, list) and key in {"fileList", "treeList", "globs", "antiglobs"}:
        if not all(isinstance(item, str) and "," not in item for item in value):
            raise ValueError(f"{source}: {key} requires scalar items without embedded commas")
        return ",".join(item for item in value if item)
    import yaml
    return yaml.safe_dump(value, default_flow_style=True, sort_keys=False, width=float("inf")).strip()


def _config_mapping(document, config_file):
    if not isinstance(document, dict):
        raise ValueError(f"{config_file}: expected a YAML mapping of analysis settings")
    return {key: _config_string(value, key, config_file) for key, value in document.items()}


def read_config_yaml(config_file):
    return _config_mapping(load_yaml_document(config_file), config_file)


def parse_config_bytes(data, source):
    """Parse exactly the bytes hashed by production provenance checks."""
    if get_config_extension(source) == ".yaml":
        return _config_mapping(_load_yaml(data, source), source)
    return parse_config_text(data.decode("utf-8").splitlines(), source)


def read_config(config_file):
    if get_config_extension(config_file) == ".yaml":
        return read_config_yaml(config_file)
    return read_config_text(config_file)


def read_config_records(config_file, *, strict_legacy=False):
    """Read plugin records. Refuse lossy legacy conversion on request."""
    if get_config_extension(config_file) == ".yaml":
        document = load_yaml_document(config_file)
        if not isinstance(document, list):
            raise ValueError(f"{config_file}: expected a sequence of mappings")
        return [_config_mapping(row, f"{config_file}: entry {n}")
                for n, row in enumerate(document, 1)]
    result = []
    with open(config_file, encoding="utf-8") as stream:
        for lineno, raw in enumerate(stream, 1):
            line = raw.partition("#")[0].strip()
            if not line:
                continue
            entry = {}
            for field in line.split(" "):
                if not field:
                    continue
                key, eq, value = field.partition("=")
                if not (key and eq and value):
                    if strict_legacy:
                        raise ValueError(f"{config_file}:{lineno}: expected key=value, got {field!r}. "
                                         "Convert this record manually to preserve the intended value.")
                    continue
                if key in entry:
                    raise ValueError(f"{config_file}:{lineno}: Duplicate key '{key}'")
                entry[key] = value
            result.append(entry)
    return result


# Native auxiliary document kinds. Raw model and calibration payloads retain their
# domain format. Extension-owned config keys can supply native YAML directly.
_LEGACY_AUX_KINDS = {
    **dict.fromkeys(("floatConfig", "intConfig", "correctedJetCollectionConfig",
                     "correctedFatJetCollectionConfig", "correctedMuonCollectionConfig",
                     "correctedElectronCollectionConfig", "correctedPhotonCollectionConfig",
                     "correctedTauCollectionConfig"), "mapping"),
    **dict.fromkeys(("saveConfig", "goldenJsonConfig", "jetVetoMapConfig"), "list"),
    **dict.fromkeys(("bdtConfig", "onnxConfig", "sofieConfig", "kinematicFitConfig", "aliasConfig",
                     "optionalBranchesConfig", "histogramConfig", "triggerConfig", "taggerConfig",
                     "correctionConfig", "correctionlibConfig", "workflowConfig"), "records"),
    "sampleConfig": "samples",
    "stxsBinConfig": "stxs_bins",
    "stxsMigrationBinConfig": "stxs_migration",
}


def legacy_aux_kind(key, source):
    if Path(source).suffix in {".txt", ".cfg"}:
        return _LEGACY_AUX_KINDS.get(key)
    return None


def read_stxs_table(source, kind):
    """Read the native STXS bin/migration schema or its positional compatibility form."""
    path = Path(source)
    is_bins = kind == "stxs_bins"
    if not is_bins and kind != "stxs_migration":
        raise ValueError(f"{path}: unknown STXS table {kind!r}")
    fields = (("analysis_bin", "label", "fine_categories") if is_bins else
              ("fine_category", "pt_bin", "jet_pt_bin", "scale_bin", "acceptance_fallback"))
    yaml_input = get_config_extension(path) == ".yaml"
    if yaml_input:
        document = load_yaml_document(path)
        if not isinstance(document, list):
            raise ValueError(f"{path}: expected a sequence of STXS bin mappings")
        rows = enumerate(document, 1)
    else:
        tables = []
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            parts = line.partition("#")[0].split()
            if not parts:
                continue
            if len(parts) != len(fields):
                raise ValueError(f"{path}:{number}: expected {' '.join(fields)}")
            if is_bins:
                parts[-1] = parts[-1].split(",")
            elif parts[-1] not in ("0", "1"):
                raise ValueError(f"{path}:{number}: expected acceptance_fallback 0 or 1")
            tables.append((number, dict(zip(fields, parts))))
        rows = tables
    result = []
    seen_categories = set()
    for number, row in rows:
        try:
            if not isinstance(row, dict):
                raise ValueError("expected a bin mapping")
            if is_bins:
                categories = row["fine_categories"]
                label = row["label"]
                if not isinstance(label, str) or not label:
                    raise ValueError("label must be a nonempty string")
                if not isinstance(categories, list) or not categories:
                    raise ValueError("fine_categories must be a nonempty sequence of integers")
                values = [int(row["analysis_bin"]), label, [int(c) for c in categories]]
            else:
                indices = [int(row[f]) for f in fields[:-1]]
                if min(indices) <= 0:
                    raise ValueError("migration category and bin indices must be positive")
                if indices[0] in seen_categories:
                    raise ValueError(f"duplicate migration category {indices[0]}")
                seen_categories.add(indices[0])
                fallback = row["acceptance_fallback"]
                accepted = ("true", "false", "yes", "no", "on", "off", "y", "n") if yaml_input else ("1", "0")
                if str(fallback).lower() not in accepted:
                    raise ValueError("acceptance_fallback must be a boolean")
                values = [*indices, str(fallback).lower() in ("true", "yes", "on", "y", "1")]
            result.append(dict(zip(fields, values)))
        except (TypeError, KeyError, ValueError) as exc:
            raise ValueError(f"{path}: STXS entry {number}: {exc}") from exc
    if not result:
        raise ValueError(f"{path}: No valid bin entries found")
    return result


def read_legacy_aux(source, kind):
    path = Path(source)
    if kind == "mapping":
        with path.open(encoding="utf-8") as stream:
            return parse_config_text(stream, path, strict=True)
    if kind == "list":
        return [line.partition("#")[0].strip() for line in path.read_text().splitlines()
                if line.partition("#")[0].strip()]
    if kind == "records":
        return read_config_records(path, strict_legacy=True)
    if kind in ("stxs_bins", "stxs_migration"):
        return read_stxs_table(path, kind)
    if kind == "samples":
        from sample_config import convert_legacy_manifest
        return convert_legacy_manifest(path).as_yaml_document()
    raise ValueError(f"{source}: unsupported legacy document kind {kind!r}")


def dump_config(config_dict, stream=None):
    """Serialize a native document with the same format as file publication."""
    import yaml
    return yaml.safe_dump(config_dict, stream, default_flow_style=False, sort_keys=False)


def write_config_yaml(config_dict, output_file):
    """Publish one YAML document atomically, retaining permissions of an existing target."""
    import os
    import stat
    import uuid
    target = Path(output_file)
    if get_config_extension(target) != ".yaml":
        raise ValueError(f"{target}: configuration output must use .yaml or .yml. Read legacy text and write a new YAML file.")
    temporary = target.with_name(f".{target.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as stream:
            dump_config(config_dict, stream)
            if target.exists():
                os.fchmod(stream.fileno(), stat.S_IMODE(target.stat().st_mode))
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()


write_config = write_config_yaml


def runtime_yaml_path(source):
    """Keep historical text immutable, even when used for another execution."""
    path = Path(source)
    return path if get_config_extension(path) == ".yaml" else path.with_suffix(".runtime.yaml")


def materialize_runtime_config(source):
    """Use an existing native YAML, or materialize its legacy equivalent beside the input."""
    path = Path(source)
    target = runtime_yaml_path(path)
    if target != path:
        write_config(read_config(path), target)
    return target


def job_config_path(directory, *, include_manager=False):
    """Select the prepared YAML worker config, falling back to archived text receipts."""
    directory = Path(directory)
    stems = ("submit_config", "job_config") if include_manager else ("submit_config",)
    for stem in stems:
        for suffix in (".yaml", ".txt"):
            candidate = directory / (stem + suffix)
            if candidate.is_file():
                return candidate
    return directory / "submit_config.yaml"
