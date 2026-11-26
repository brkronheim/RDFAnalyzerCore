"""Versioned job metadata, lossless provenance bundles and ROOT reduction.

Descriptive objects never pass through hadd. Bundle sidecars are immutable and
content addressed; the ROOT reference and completion record are published last.
The readers accept legacy TNamed metadata as well as compact job documents.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import re
from pathlib import Path
import subprocess
import tempfile

JOB_KEY = "__rdf_job_metadata_v1"
REFERENCE_KEY = "__rdf_metadata_reference_v1"
SCHEMA_KEY = "__rdf_histogram_schema_v1"
FORMAT = "rdf-metadata-bundle"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False)


def digest(value):
    return hashlib.sha256(canonical(value).encode()).hexdigest()


def _read_bytes(path):
    if str(path).startswith("root://"):
        with tempfile.TemporaryDirectory(prefix="rdf-metadata-read-") as tmp:
            local = os.path.join(tmp, "metadata.gz")
            subprocess.run(["xrdcp", "-f", str(path), local], check=True,
                           capture_output=True, text=True)
            return Path(local).read_bytes()
    return Path(path).read_bytes()


def _open_root(path):
    import uproot
    # A local mmap avoids reopening a file for every small legacy TNamed key.
    # Remote sources retain uproot's protocol-specific reader.
    if not str(path).startswith("root://") and hasattr(uproot, "source"):
        return uproot.open(path, handler=uproot.source.file.MemmapSource)
    return uproot.open(path)


def _validate_audit(audit):
    if not isinstance(audit, dict):
        raise ValueError("Invalid weight audit record")
    for key in ("n", "n_negative", "n_zero"):
        value = audit.get(key)
        if value is not None and (type(value) is not int or value < 0):
            raise ValueError(f"Invalid audit count {key}: {value!r}")
    for key in ("sum_w", "min_w", "max_w"):
        value = audit.get(key)
        if value is not None and (not isinstance(value, (int, float)) or not math.isfinite(value)):
            raise ValueError(f"Invalid audit statistic {key}: {value!r}")
    if audit.get("sum_w") is None:
        raise ValueError("Missing audit sum_w")
    n = audit.get("n")
    if n is not None:
        if any(audit.get(k) is None or audit[k] > n for k in ("n_negative", "n_zero")):
            raise ValueError("Audit counts inconsistent with n")
        if n and (audit.get("min_w") is None or audit.get("max_w") is None
                  or audit["min_w"] > audit["max_w"]):
            raise ValueError("Missing/inconsistent audit extrema")


def reduce_audits(records):
    grouped = {}
    for record in records:
        for key, audit in record.get("weight_audits", {}).items():
            _validate_audit(audit)
            # Audits are scoped by dataset, not just column name.
            scope = record.get("sample", "unknown")
            grouped.setdefault(scope, {}).setdefault(key, []).append(audit)
    result = {}
    for scope, columns in grouped.items():
        result[scope] = {}
        for key, audits in columns.items():
            counts_known = all(a.get("n") is not None for a in audits)
            n = sum(a["n"] for a in audits) if counts_known else None
            active = [a for a in audits if a.get("n") != 0]
            merged = {"n": n, "sum_w": math.fsum(a["sum_w"] for a in audits),
                      "min_w": min(a["min_w"] for a in active) if counts_known and active else None,
                      "max_w": max(a["max_w"] for a in active) if counts_known and active else None}
            for field in ("n_negative", "n_zero"):
                merged[field] = (sum(a[field] for a in audits)
                                 if all(a.get(field) is not None for a in audits) else None)
            merged["mean"] = merged["sum_w"] / n if n else None
            result[scope][key] = merged
    return result


def _normalize_record(document, source):
    if document.get("format") != "rdf-job-metadata" or document.get("schema_version") != 1:
        raise ValueError(f"Unsupported job metadata format in {source}")
    if "provenance" not in document:
        raise ValueError(f"Missing provenance map in compact metadata: {source}")
    record = {key: document.get(key, {}) for key in
              ("provenance", "regions", "normalizations", "weight_audits", "objects")}
    prov = record["provenance"]
    if not isinstance(prov, dict) or not all(isinstance(k, str) and isinstance(v, str)
                                              for k, v in prov.items()):
        raise ValueError(f"Invalid provenance map in {source}")
    for field in ("regions", "normalizations", "weight_audits", "objects"):
        if not isinstance(record[field], dict):
            raise ValueError(f"Invalid metadata field {field} in {source}")
    for value in record["weight_audits"].values():
        _validate_audit(value)
    record["sample"] = prov.get("service.counter.sample", prov.get("dataset.name", "unknown"))
    # Effective legacy config/file-list hashes distinguish jobs without relying
    # on ROOT cycles. With no identity evidence, source path is an explicit fallback.
    identity = prov.get("job.logical_id") or prov.get("task.job_id")
    if identity is None:
        identity = digest({k: prov[k] for k in ("config.hash", "filelist.hash") if k in prov}) if (
            "config.hash" in prov or "filelist.hash" in prov) else str(source)
    record["id"] = digest({"sample": record["sample"], "job": identity})
    record["source"] = str(source)
    return record


def _root_text(obj):
    return str(obj) if obj.classname == "TObjString" else str(obj.member("fTitle"))


def _unique_texts(root_file, path):
    """An invariant may have identical old cycles; differing cycles are ambiguous."""
    if not hasattr(root_file, "_rdf_metadata_cycles"):
        cycles = {}
        for key in root_file.keys(recursive=True, cycle=True):
            cycles.setdefault(key.rsplit(";", 1)[0], []).append(key)
        root_file._rdf_metadata_cycles = cycles
    values = {_root_text(root_file[k]) for k in root_file._rdf_metadata_cycles.get(path, [])}
    if len(values) > 1:
        raise ValueError(f"Conflicting legacy key cycles for {path!r}")
    return next(iter(values)) if values else None


def _legacy_record(root_file, source):
    document = {"format": "rdf-job-metadata", "schema_version": 1,
                "provenance": {}, "regions": {}, "normalizations": {},
                "weight_audits": {}, "objects": {}}
    classes = root_file.classnames(recursive=True, cycle=False)
    for path, cls in classes.items():
        if cls not in ("TNamed", "TObjString") or path == SCHEMA_KEY:
            continue
        if path.startswith("provenance/"):
            document["provenance"][path.split("/", 1)[1]] = _unique_texts(root_file, path)
        elif path.startswith("regions/"):
            document["regions"][path.split("/", 1)[1]] = _unique_texts(root_file, path)
        elif path.startswith("weight_norm_"):
            document["normalizations"][path] = float(_unique_texts(root_file, path))
        elif path.startswith("weight_audit_") and path.endswith(("_negCount", "_zeroCount")):
            continue
        else:
            # Unknown descriptive objects are preserved with job identity.
            document["objects"][path] = {"class": cls, "value": _unique_texts(root_file, path)}
    for path, cls in classes.items():
        if path.startswith("weight_audit_") and cls.startswith("TH1"):
            hist = root_file[path]
            values = hist.values()
            if len(values) != 4:
                raise ValueError(f"Unsupported legacy weight audit {path}")
            audit = {"n": None, "sum_w": float(values[0]), "min_w": None, "max_w": None,
                     "legacy_mean": float(values[1]), "legacy_min_w": float(values[2]),
                     "legacy_max_w": float(values[3])}
            for suffix, field in (("_negCount", "n_negative"), ("_zeroCount", "n_zero")):
                text = _unique_texts(root_file, path + suffix)
                audit[field] = int(text) if text is not None else None
            document["weight_audits"][path] = audit
    return _normalize_record(document, source)


def expand_bundle(bundle):
    if bundle.get("format") != FORMAT or bundle.get("schema_version") != 1:
        raise ValueError("Unsupported metadata bundle version")
    definitions = bundle["definitions"]
    for key, definition in definitions.items():
        if digest(definition) != key:
            raise ValueError("Metadata definition digest mismatch")
    records = []
    for job in bundle["jobs"]:
        run, dataset = definitions[job["run"]], definitions[job["dataset"]]
        record = {k: v for k, v in job.items() if k not in ("run", "dataset")}
        record["provenance"] = {**run["provenance"], **record["provenance"]}
        record.update(dataset)
        records.append(record)
    if len(records) != bundle["n_inputs"]:
        raise ValueError("Metadata bundle job count mismatch")
    return records


def load_metadata_records(path):
    """Read complete per-job metadata; merged references are digest checked."""
    if str(path).endswith((".json", ".json.gz")):
        payload = _read_bytes(path)
        data = json.loads(gzip.decompress(payload) if str(path).endswith(".gz") else payload)
        if "metadata_bundle" in data:
            return expand_bundle(data["metadata_bundle"]) if data["metadata_bundle"] is not None else []
        return expand_bundle(data)
    import uproot
    with _open_root(path) as f:
        if REFERENCE_KEY in f:
            ref = json.loads(_unique_texts(f, REFERENCE_KEY))
            if ref.get("schema_version") != 1:
                raise ValueError("Unsupported metadata reference version")
            name = ref["file"]
            if not isinstance(name, str) or Path(name).name != name or name in ("", ".", ".."):
                raise ValueError("Metadata sidecar must be a sibling basename")
            sidecar = str(path).rsplit("/", 1)[0] + "/" + name if "/" in str(path) else name
            payload = _read_bytes(sidecar)
            if hashlib.sha256(payload).hexdigest() != ref["sha256"]:
                raise ValueError("Metadata sidecar digest mismatch")
            bundle = json.loads(gzip.decompress(payload))
            records = expand_bundle(bundle)
            if len(records) != ref["n_inputs"]:
                raise ValueError("Metadata reference job count mismatch")
            return records
        if JOB_KEY in f:
            return [_normalize_record(json.loads(_unique_texts(f, JOB_KEY)), path)]
        return [_legacy_record(f, path)]


def common_provenance(records):
    if not records:
        return {}
    first = records[0]["provenance"]
    return {k: v for k, v in first.items()
            if all(r["provenance"].get(k) == v for r in records[1:])}


def validate_semantic_config_compatibility(policy):
    """Validate an explicit, sample-scoped exception to semantic hash equality."""
    if policy is None:
        return
    if (not isinstance(policy, dict) or policy.get("schema_version") != 1
        or not isinstance(policy.get("reason"), str) or not policy["reason"].strip()
        or not isinstance(policy.get("samples"), dict)):
        raise ValueError("Invalid semantic config compatibility policy")
    for sample, hashes in policy["samples"].items():
        if (not isinstance(sample, str) or not sample
            or not isinstance(hashes, list) or len(hashes) < 2
            or any(not isinstance(h, str) or not h for h in hashes)
            or len(set(hashes)) != len(hashes)):
            raise ValueError(f"Invalid semantic config compatibility hashes for {sample!r}")
    replays = policy.get("executable_replays", {})
    if not isinstance(replays, dict):
        raise ValueError("Invalid executable replay policy")
    for job_id, replay in replays.items():
        if (not isinstance(job_id, str) or not job_id or not isinstance(replay, dict)
            or set(replay) != {"sample", "original", "replay"}
            or not isinstance(replay["sample"], str) or not replay["sample"]
            or any(not isinstance(replay[k], str) or not re.fullmatch("[0-9a-f]{64}", replay[k])
                   for k in ("original", "replay"))
            or replay["original"] == replay["replay"]):
            raise ValueError(f"Invalid executable replay policy for {job_id!r}")


def load_semantic_config_compatibility(path):
    policy = json.loads(Path(path).read_text())
    validate_semantic_config_compatibility(policy)
    return policy


def build_bundle(records, *, semantic_config_compatibility=None):
    if not records:
        raise ValueError("No metadata records")
    validate_semantic_config_compatibility(semantic_config_compatibility)
    allowed = semantic_config_compatibility["samples"] if semantic_config_compatibility is not None else {}
    replays = semantic_config_compatibility.get("executable_replays", {}) if semantic_config_compatibility else {}
    used_exceptions, used_replays = {}, {}
    definitions, jobs, seen, scopes = {}, [], set(), {}
    build_reference = None
    semantic_configs = {}
    for record in sorted(records, key=lambda r: r["id"]):
        if record["id"] in seen:
            raise ValueError(f"Duplicate logical job contribution: {record['source']}")
        seen.add(record["id"])
        prov = record["provenance"]
        # Configure timestamps are retained as evidence, not compatibility IDs.
        # Registry cardinalities describe each event graph, not the ROOT build.
        # Retain them in leaf provenance; the registry implementation mode and
        # every other framework/ROOT/extension identity remain strict.
        # Legacy files have no deployed-binary digest; do not pretend their Git
        # labels prove identical dirty sources. New jobs also identify the
        # expected staged executable by its content hash.
        build = {k: v for k, v in prov.items()
                 if (k.startswith(("framework.", "root.", "extensions."))
                     and k not in {"framework.build_timestamp",
                                   "root.registry_definitions",
                                   "root.registry_input_bindings_before",
                                   "root.registry_input_bindings_after"})
                 or k == "run.expected_executable_sha256"}
        scope = record["sample"]
        replay = replays.get(record["id"])
        executable_key = "run.expected_executable_sha256"
        if replay is not None and scope == replay["sample"] and build.get(executable_key) == replay["replay"]:
            # Compare against the original only for this exact logical job.
            # The actual replay digest remains in the leaf provenance.
            build[executable_key] = replay["original"]
            used_replays[record["id"]] = replay
        if build:
            if build_reference is not None and build_reference != build:
                raise ValueError("Incompatible framework/extension builds in merge group")
            build_reference = build
        config = {k: v for k, v in prov.items() if k.endswith(".config_hash")}
        invariant = {"build": build, "plugins": config, "regions": record["regions"],
                     "normalizations": record["normalizations"]}
        if prov.get("config.semantic_hash"):
            previous = semantic_configs.setdefault(scope, prov["config.semantic_hash"])
            if previous != prov["config.semantic_hash"]:
                hashes = allowed.get(scope, [])
                if previous not in hashes or prov["config.semantic_hash"] not in hashes:
                    raise ValueError(f"Incompatible semantic analysis configurations for {scope}: "
                                     f"{previous} != {prov['config.semantic_hash']}")
                used_exceptions[scope] = hashes
        if scope in scopes and scopes[scope] != invariant:
            raise ValueError(f"Incompatible build/plugin/region/normalization definitions for {scope}")
        scopes[scope] = invariant
        run_prov = {k: v for k, v in prov.items() if k.startswith(
            ("framework.", "root.", "extensions.", "analysis.", "env.", "plugin.", "file.hash.",
             "file.reference_fingerprint.", "run."))
            or k == "config.semantic_hash"}
        for key in list(run_prov):
            if key.startswith(("file.hash.__orig_", "file.reference_fingerprint.__orig_")):
                del run_prov[key]
        run = {"provenance": run_prov}
        dataset = {k: record[k] for k in ("sample", "regions", "normalizations")}
        run_id, dataset_id = digest(run), digest(dataset)
        definitions[run_id], definitions[dataset_id] = run, dataset
        job = {k: v for k, v in record.items() if k not in dataset and k != "provenance"}
        job.update(run=run_id, dataset=dataset_id,
                   provenance={k: v for k, v in prov.items() if k not in run_prov})
        jobs.append(job)
    result = {"format": FORMAT, "schema_version": 1, "n_inputs": len(jobs),
              "definitions": definitions, "jobs": jobs,
              "weight_audits": reduce_audits(records)}
    if used_exceptions or used_replays:
        # Subsequent merges must independently supply the policy again.
        result["semantic_config_compatibility"] = {
            "schema_version": 1, "reason": semantic_config_compatibility["reason"],
            "samples": used_exceptions}
        if used_replays:
            result["semantic_config_compatibility"]["executable_replays"] = used_replays
    # Enforce an exactly reversible representation, including missing fields.
    assert sorted(expand_bundle(result), key=lambda r: r["id"]) == sorted(records, key=lambda r: r["id"])
    return result


def _axis_metadata(axis):
    labels = [""] * axis.member("fNbins")
    for label in axis.member("fLabels") or []:
        index = label.member("@fUniqueID") - 1
        if 0 <= index < len(labels):
            labels[index] = str(label)
    return {"edges": axis.edges().tolist(), "labels": labels}


def _sparse_axes(path, names, cache):
    """Read ROOT's physical axes in compiled code, avoiding Python sparse I/O.

    Uproot's general THn streamer decodes the complete sparse payload just to
    inspect its axes. ROOT's native reader and one JSON conversion are much
    cheaper for the many mostly empty histograms in worker files.
    """
    if not names:
        return {}
    import ROOT
    if not hasattr(ROOT, "rdfMetadataSparseAxesV1"):
        ROOT.gInterpreter.Declare(r'''
        #include <TFile.h>
        #include <THnBase.h>
        #include <iomanip>
        #include <memory>
        #include <sstream>
        #include <stdexcept>
        #include <vector>
        std::string rdfMetadataQuoteV1(const std::string& s) {
            std::ostringstream out; out << '"';
            for (unsigned char c : s) {
                if (c == '"' || c == '\\') out << '\\' << c;
                else if (c < 32) out << "\\u" << std::hex << std::setw(4)
                                    << std::setfill('0') << unsigned(c) << std::dec;
                else out << c;
            }
            out << '"'; return out.str();
        }
        std::string rdfMetadataSparseAxesV1(const char* path,
                                           const std::vector<std::string>& names) {
            TFile f(path, "READ");
            if (f.IsZombie()) throw std::runtime_error("Cannot inspect sparse axes");
            std::ostringstream out; out.imbue(std::locale::classic());
            out << std::setprecision(17) << '{'; bool first = true;
            for (const auto& name : names) {
                std::unique_ptr<TObject> object(f.Get(name.c_str()));
                auto* h = dynamic_cast<THnBase*>(object.get());
                if (!h) throw std::runtime_error("Missing/invalid sparse histogram: " + name);
                if (!first) out << ','; first = false;
                out << rdfMetadataQuoteV1(name) << ":[";
                for (int d = 0; d < h->GetNdimensions(); ++d) {
                    if (d) out << ',';
                    auto* axis = h->GetAxis(d);
                    out << "{\"edges\":[";
                    for (int i = 1; i <= axis->GetNbins()+1; ++i) {
                        if (i > 1) out << ',';
                        out << axis->GetBinLowEdge(i);
                    }
                    out << "],\"labels\":[";
                    for (int i = 1; i <= axis->GetNbins(); ++i) {
                        if (i > 1) out << ',';
                        out << rdfMetadataQuoteV1(axis->GetBinLabel(i));
                    }
                    out << "]}";
                }
                out << ']';
            }
            out << '}'; return out.str();
        }
        ''')
    text = str(ROOT.rdfMetadataSparseAxesV1(str(path), sorted(names)))
    if text not in cache:
        cache[text] = json.loads(text)
    return cache[text]


def _inspect_inputs(paths, role, *, semantic_config_compatibility=None, sparse_validator=None):
    import uproot
    from extract_nd_histograms import validate_compatible_histogram_schemas
    records, schemas, exclusions = [], [], {JOB_KEY, REFERENCE_KEY, SCHEMA_KEY, "provenance", "regions"}
    axis_reference = {}
    class_reference, numeric_names, descriptive_names = {}, set(), set()
    physical_cache, schema_cache, validated_axes = {}, {}, set()
    sparse_inventories = []
    for path in paths:
        records.extend(load_metadata_records(path))
        with _open_root(path) as f:
            schema_text = _unique_texts(f, SCHEMA_KEY) if SCHEMA_KEY in f else None
            if schema_text is not None and schema_text not in schema_cache:
                schema_cache[schema_text] = json.loads(schema_text)
            schema = schema_cache.get(schema_text)
            schemas.append((str(path), schema))
            sparse_names = set()
            classes = f.classnames(recursive=True, cycle=False)
            sparse_keys = [key for key, cls in classes.items() if cls.startswith("THn")]
            physical_sparse_axes = ((sparse_validator(path, sparse_keys) if sparse_validator is not None
                                     else _sparse_axes(path, sparse_keys, physical_cache))
                                    if role != "cutflow" else {})
            if role != "cutflow" and set(physical_sparse_axes) != set(sparse_keys):
                raise ValueError("Sparse validator did not cover the complete physical inventory")
            for key, cls in classes.items():
                name = key.rsplit("/", 1)[-1]
                if cls.startswith("TDirectory"):
                    continue
                descriptive = cls in ("TNamed", "TObjString")
                audit = name.startswith("weight_audit_")
                cutflow = name.startswith(("cutflow", "counter_"))
                if descriptive or audit:
                    if descriptive and key.startswith(("provenance/", "regions/")):
                        # Their parent directory is excluded as a whole. Child
                        # names may contain whitespace or match histogram names.
                        continue
                    if name in numeric_names:
                        raise ValueError(f"Conflicting numerical/metadata object name: {name}")
                    descriptive_names.add(name)
                    exclusions.add(name)
                    continue
                if role == "cutflow" and not cutflow:
                    exclusions.add(name)
                    continue
                if not cls.startswith(("TH1", "TH2", "TH3", "THn", "TProfile")):
                    raise ValueError(f"No reduction policy for {key} ({cls}) in {path}")
                if name in descriptive_names:
                    raise ValueError(f"Conflicting numerical/metadata object name: {name}")
                numeric_names.add(name)
                if key in class_reference and class_reference[key] != cls:
                    raise ValueError(f"Incompatible numerical ROOT classes: {key}")
                class_reference[key] = cls
                if cls.startswith("THn"):
                    sparse_names.add(key)
                    axes = physical_sparse_axes[key]
                    token = (id(axes), id(schema), key)
                    if sparse_validator is None:
                        if token in validated_axes:
                            continue
                        validated_axes.add(token)
                    if sparse_validator is None and schema is None:
                        raise ValueError(f"Sparse histogram {key} requires its embedded schema")
                    if sparse_validator is None:
                        declared = next((entry["axes"] for entry in schema["histograms"]
                                         if schema["directory"] + "/" + entry["key"] == key), None)
                        if declared is None or len(axes) != len(declared):
                            raise ValueError(f"Physical sparse dimensions differ from schema: {key}")
                        for actual, expected in zip(axes, declared):
                            edges = expected["edges"]
                            labels = expected.get("labels", [])
                            if (len(actual["edges"]) != len(edges)
                                or len(actual["edges"]) != expected["bins"] + 1
                                or any(not math.isclose(a, b, rel_tol=1e-13, abs_tol=1e-13)
                                       for a, b in zip(actual["edges"], edges))
                                or actual["labels"][:len(labels)] != labels):
                                raise ValueError(f"Physical sparse axes differ from schema: {key}")
                else:
                    hist = f[key]
                    axes = [_axis_metadata(a) for a in hist.axes]
                signature = canonical(axes)
                if key in axis_reference and axis_reference[key] != signature:
                    raise ValueError(f"Incompatible histogram/cutflow axes: {key}")
                axis_reference[key] = signature
            sparse_inventories.append(sparse_names)
    present = [(p, s) for p, s in schemas if s is not None]
    if sparse_validator is None and present and len(present) != len(schemas):
        raise ValueError("Cannot merge mixed legacy and sparse-first histogram schemas")
    # Identical serialized declarations need parsing/validation only once.
    schema = (validate_compatible_histogram_schemas(
        [("unique schema " + str(i), value) for i, value in enumerate(schema_cache.values())]
    ) if sparse_validator is None else None)
    if schema is not None and role != "cutflow":
        expected = {schema["directory"] + "/" + h["key"] for h in schema["histograms"]}
        if any(names != expected for names in sparse_inventories):
            raise ValueError("Physical sparse histogram inventory differs from embedded schema")
    return (build_bundle(records, semantic_config_compatibility=semantic_config_compatibility),
            schema if role != "cutflow" else None, exclusions)


def merge_root_files(output, inputs, *, role="all", hadd="hadd", jobs=1,
                     max_open_files=10000, postprocess=None, log=None,
                     semantic_config_compatibility=None, sparse_validator=None):
    """Validate and merge numerical objects; publish a portable metadata bundle.

    Unknown descriptive strings are archived per job. Unknown non-histogram
    objects are rejected. A caller-supplied sparse_validator must validate every
    sparse object against its owned schema and return exact axis/schema signatures.
    The default requires the framework ND schema. Main histograms use ROOT's reducer; audit records use
    sufficient statistics. Existing outputs survive validation/merge failures.
    """
    paths = list(map(str, inputs))
    if not paths:
        raise ValueError("No input ROOT files")
    if role not in ("all", "histograms", "cutflow") or jobs < 1:
        raise ValueError("Invalid merge role or worker count")
    output = Path(output).absolute()
    if str(output) in paths or any(not p.startswith("root://") and Path(p).resolve() == output.resolve() for p in paths):
        raise ValueError("Merge output cannot overwrite an input")
    validate_semantic_config_compatibility(semantic_config_compatibility)
    bundle, schema, exclusions = _inspect_inputs(
        paths, role, semantic_config_compatibility=semantic_config_compatibility, sparse_validator=sparse_validator)
    payload = gzip.compress(canonical(bundle).encode(), mtime=0)
    sha = hashlib.sha256(payload).hexdigest()
    sidecar = output.with_name(output.name + ".metadata." + sha + ".json.gz")
    ref = {"schema_version": 1, "file": sidecar.name, "sha256": sha, "n_inputs": bundle["n_inputs"]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".rdf-merge-", dir=output.parent) as tmp:
        merged = Path(tmp) / "merged.root"
        filter_path = Path(tmp) / "exclude.txt"
        if any("/" in key or any(c.isspace() for c in key) for key in exclusions):
            raise ValueError("ROOT hadd cannot filter a metadata object with whitespace or slash in its name")
        filter_path.write_text("\n".join(sorted(exclusions)) + "\n")
        command = [hadd, "-f", "-n", str(max_open_files), "-L", str(filter_path), "-Ltype", "SkipListed"]
        if jobs > 1:
            command += ["-j", str(jobs), "-d", tmp]
        command += [str(merged), *paths]
        result = subprocess.run(command, stdout=log if log is not None else subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        if result.returncode:
            raise RuntimeError(f"hadd failed (exit {result.returncode}): {result.stdout or 'see merge log'}")
        import ROOT
        f = ROOT.TFile.Open(str(merged), "UPDATE")
        if not f or f.IsZombie() or not f.IsWritable():
            raise RuntimeError("Cannot open merged ROOT output")
        try:
            if schema is not None:
                if ROOT.TObjString(canonical(schema)).Write(SCHEMA_KEY, ROOT.TObject.kOverwrite) <= 0:
                    raise OSError("Failed to write merged histogram schema")
            if ROOT.TObjString(canonical(ref)).Write(REFERENCE_KEY, ROOT.TObject.kOverwrite) <= 0:
                raise OSError("Failed to write merged metadata reference")
            f.Flush()
            if f.TestBit(ROOT.TFile.kWriteError):
                raise OSError("Failed to flush merged ROOT output")
        finally:
            f.Close()
        if postprocess is not None:
            postprocess(str(merged))
        # Content-addressed siblings make replacement safe for an existing ROOT
        # file that still references its previous sidecar. Completion is last.
        staged_sidecar = Path(tmp) / "metadata.gz"
        staged_sidecar.write_bytes(payload)
        os.replace(staged_sidecar, sidecar)
        os.replace(merged, output)
        completion = {"format": FORMAT, "schema_version": 1,
                      "root_bytes": output.stat().st_size, "metadata": ref}
        staged_complete = Path(tmp) / "complete.json"
        staged_complete.write_text(canonical(completion) + "\n")
        os.replace(staged_complete, str(output) + ".merge.json")
    return ref


def validate_metadata_bundle(path):
    """Check companion availability/digest, and completion when one exists."""
    records = load_metadata_records(path)
    import uproot
    with _open_root(path) as root_file:
        reference = json.loads(_unique_texts(root_file, REFERENCE_KEY)) if REFERENCE_KEY in root_file else None
    complete = Path(str(path) + ".merge.json")
    if reference is not None and not complete.exists():
        raise FileNotFoundError(f"Missing metadata merge completion record: {complete}")
    if complete.exists():
        marker = json.loads(complete.read_text())
        if (marker.get("schema_version") != 1 or marker["root_bytes"] != Path(path).stat().st_size
            or marker.get("metadata") != reference):
            raise ValueError("Stale metadata merge completion record")
        if len(records) != marker["metadata"]["n_inputs"]:
            raise ValueError("Stale metadata completion input count")
    return records


def metadata_artifact_paths(path):
    """Return every sibling needed to archive a validated ROOT artifact."""
    validate_metadata_bundle(path)
    paths = [Path(path)]
    with _open_root(path) as root_file:
        if REFERENCE_KEY in root_file:
            reference = json.loads(_unique_texts(root_file, REFERENCE_KEY))
            paths.extend([Path(path).parent/reference["file"], Path(str(path)+".merge.json")])
    return paths


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output")
    parser.add_argument("inputs", nargs="+")
    parser.add_argument("--jobs", type=int, default=1)
    parser.add_argument("--max-open-files", type=int, default=10000)
    parser.add_argument("--extract-histograms", action="store_true")
    parser.add_argument("--nominal-only", action="store_true")
    parser.add_argument("--semantic-config-compatibility", type=Path,
                        help="JSON policy allowing reviewed semantic hashes and exact executable replays")
    args = parser.parse_args()
    def extract(path):
        from extract_nd_histograms import extract_nd_histograms
        extract_nd_histograms(path, path, nominal_only=args.nominal_only)
    merge_root_files(args.output, args.inputs, jobs=args.jobs, max_open_files=args.max_open_files,
                     postprocess=extract if args.extract_histograms else None,
                     semantic_config_compatibility=(load_semantic_config_compatibility(
                         args.semantic_config_compatibility) if args.semantic_config_compatibility else None))


if __name__ == "__main__":
    main()
