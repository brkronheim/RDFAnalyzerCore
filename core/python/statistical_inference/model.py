"""Strict template-card reader and a portable model consumed by native exporters.

Only the supported binned subset is accepted. In particular, RooWorkspace
objects, arbitrary formula rateParams and unknown directives cannot disappear
silently during a backend switch. TensorFlow is never imported here. ROOT is
loaded lazily for the default template reader; Uproot is an explicit alternative.
"""
from __future__ import annotations

import fnmatch
import hashlib
import json
import os
from pathlib import Path
import tempfile

import numpy as np

from .errors import FitError

PREPARATION_VERSION = 1


def _file_digest(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def cached_model(card_paths, cache_path, *, poi_map=None, template_reader="root"):
    """Reuse expensive template loading only after verifying every source hash."""
    if template_reader not in ("uproot", "root"):
        raise FitError("template reader must be uproot or root")
    cache_path = Path(cache_path)
    inputs = dict(cards=[str(Path(p).resolve()) for p in card_paths],
                  poi_map=str(Path(poi_map).resolve()) if poi_map else None)
    try:
        cached = json.loads(cache_path.read_text())
        model = cached["model"]
        if (model.get("preparation_version") == PREPARATION_VERSION and cached["inputs"] == inputs and
                all(_file_digest(p) == sha for p, sha in model["sources"].items())):
            identity = model.pop("identity")
            actual = hashlib.sha256(json.dumps(model, sort_keys=True).encode()).hexdigest()
            model["identity"] = identity
            if identity == actual:
                return model
    except (OSError, ValueError, KeyError, TypeError):
        pass
    model = read_model(card_paths, poi_map=poi_map, template_reader=template_reader)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=cache_path.parent, delete=False) as stream:
        temporary = stream.name
        json.dump(dict(inputs=inputs, model=model), stream, allow_nan=False)
    os.replace(temporary, cache_path)
    return model


def _number(token, context):
    try:
        value = float(token)
    except ValueError as exc:
        raise FitError(f"invalid number {token!r}: {context}") from exc
    if not np.isfinite(value):
        raise FitError(f"non-finite number: {context}")
    return value


def _parameter(name, initial=1., limits=None, poi=False, constant=False):
    lo, hi = limits if limits is not None else (0., None)
    if not np.isfinite(initial) or any(b is not None and not np.isfinite(b) for b in (lo, hi)):
        raise FitError(f"parameter {name}: initial value and bounds must be finite")
    if (lo is not None and initial <= lo) or (hi is not None and initial >= hi):
        raise FitError(f"parameter {name}: initial value must be strictly inside its bounds")
    return dict(parameter=name, initial=float(initial), min=lo, max=hi,
                is_poi=poi, constant=constant)


class _RootTemplateFile:
    """C++ deserialization with owned NumPy copies and no bin loops."""

    def __init__(self, path):
        try:
            import ROOT
        except ImportError as exc:
            raise FitError("the root template reader requires PyROOT; use the analysis environment "
                           "or select template_reader='uproot'") from exc
        self.root = ROOT
        self.file = ROOT.TFile.Open(str(path), "READ")
        if not self.file or self.file.IsZombie():
            raise FitError(f"cannot open ROOT template file {path}")
        self.directories = {}

    def read(self, template):
        parent, _, leaf = template.rpartition("/")
        if parent not in self.directories:
            self.directories[parent] = self.file.GetDirectory(parent) if parent else self.file
        directory = self.directories[parent]
        if not directory:
            raise KeyError(template)
        h = directory.Get(leaf)
        if not h:
            raise KeyError(template)
        if not h.InheritsFrom("TH1") or h.GetDimension() != 1 or not hasattr(h, "GetArray"):
            raise ValueError(f"not a one-dimensional TH1: {template}")
        h.SetDirectory(0)
        self.root.SetOwnership(h, True)
        size = h.GetNbinsX()
        data = h.GetArray()
        data.reshape((size+2,))
        values = np.array(data, dtype=np.float64, copy=True)[1:-1]
        if h.GetSumw2N():
            data = h.GetSumw2().GetArray()
            data.reshape((size+2,))
            variances = np.array(data, dtype=np.float64, copy=True)[1:-1]
        else:
            variances = np.maximum(values, 0.)
        axis = h.GetXaxis()
        if axis.GetXbins().GetSize():
            data = axis.GetXbins().GetArray()
            data.reshape((size+1,))
            edges = np.array(data, dtype=np.float64, copy=True)
        else:
            edges = np.linspace(axis.GetXmin(), axis.GetXmax(), size+1)
        return values, variances, edges

    def close(self):
        self.file.Close()


def read_model(card_paths, *, poi_map=None, shared_signal=True, template_reader="root"):
    """Read one simultaneous card or independent-region cards into one model.

    A repeated channel is an error; callers choose simultaneous versus
    independent fits explicitly. Shared nuisance names retain correlations.
    """
    import uproot

    if template_reader not in ("uproot", "root"):
        raise FitError("template reader must be uproot or root")

    paths = [Path(p).expanduser().resolve() for p in card_paths]
    if not paths:
        raise FitError("no datacards supplied")
    metadata = json.loads(Path(poi_map).read_text()) if poi_map else {}
    polynomial = metadata.get("polynomial")
    ttbar = metadata.get("ttbar_polynomial")
    if polynomial and polynomial.get("schema") not in ("vjet-njetpt-polynomial-v1", "vjet-njetpt-polynomial-v2"):
        raise FitError("unsupported structured polynomial schema")
    if ttbar and ttbar.get("schema") != "vjet-ttbar-polynomial-v1":
        raise FitError("unsupported structured ttbar schema")
    model = dict(schema="rdf-statistical-model-v1", preparation_version=PREPARATION_VERSION, channels=[], parameters=[],
                 effects=[], scales=[], sources={}, metadata=metadata,
                 mc_stat=False, groups={})
    parameters = {}
    all_columns = []
    numeric_rates = []
    formula_rates = []
    flat_params = set()
    ext_args = {}
    channel_names = set()
    observed_sources = {}

    def fingerprint(path):
        path = Path(path).resolve()
        if str(path) not in model["sources"]:
            model["sources"][str(path)] = _file_digest(path)

    if poi_map:
        fingerprint(poi_map)

    for path in paths:
        fingerprint(path)
        lines = [line.split("#", 1)[0].split() for line in path.read_text().splitlines()]
        lines = [t for t in lines if t and not set(t[0]) == {"-"}]
        try:
            rate_index = next(i for i, t in enumerate(lines) if t[0] == "rate")
            before, after = lines[:rate_index], lines[rate_index + 1:]
            processes = [t[1:] for t in before if t[0] == "process"]
            names, ids = processes
            ids = list(map(int, ids))
            columns = [t[1:] for t in before if t[0] == "bin"][-1]
            rates = lines[rate_index][1:]
            observations = next(t[1:] for t in before if t[0] == "observation")
            obs_channels = [t[1:] for t in before if t[0] == "bin"][0]
            if len(observations) != len(obs_channels):
                raise ValueError("observation/channel lengths differ")
            obs = dict(zip(obs_channels, observations))
        except (StopIteration, ValueError, IndexError) as exc:
            raise FitError(f"invalid binned datacard: {path}: {exc}") from exc
        if not (len(names) == len(ids) == len(columns) == len(rates)):
            raise FitError(f"inconsistent process columns in {path}")
        allowed_header = {"imax", "jmax", "kmax", "shapes", "bin", "observation", "process"}
        for tokens in before:
            if tokens[0] not in allowed_header:
                raise FitError(f"unsupported card header in {path}: {' '.join(tokens)}")
        shapes = [t for t in before if t[0] == "shapes"]
        files = {}
        directories = {}
        resolved_files = {}

        def histogram(process, channel, variation=None):
            matches = [t for t in shapes if len(t) >= 5 and
                       fnmatch.fnmatchcase(process, t[1]) and fnmatch.fnmatchcase(channel, t[2])]
            if not matches:
                return None
            matches.sort(key=lambda t: (t[1] == process) + (t[2] == channel))
            directive = matches[-1]
            if variation and len(directive) < 6:
                raise FitError(f"no systematic shape pattern for {channel}/{process}")
            template = directive[5 if variation else 4]
            filename = directive[3]
            for key, val in (("$PROCESS", process), ("$CHANNEL", channel),
                             ("$SYSTEMATIC", variation or ""), ("$MASS", "125")):
                template = template.replace(key, val)
                filename = filename.replace(key, val)
            if ":" in template or "://" in filename:
                raise FitError("Rabbit requires local TH1 templates; RooWorkspaces/remote inputs are unsupported")
            if filename not in resolved_files:
                resolved_files[filename] = (path.parent / filename).resolve()
            root_path = resolved_files[filename]
            if root_path not in files:
                fingerprint(root_path)
                files[root_path] = (uproot.open(root_path) if template_reader == "uproot"
                                    else _RootTemplateFile(root_path))
            try:
                if template_reader == "root":
                    values, variances, edges = files[root_path].read(template)
                else:
                    parent, _, leaf = template.rpartition("/")
                    key = (root_path, parent)
                    if key not in directories:
                        directories[key] = files[root_path][parent] if parent else files[root_path]
                    h = directories[key][leaf]
                    values, edges = h.to_numpy(flow=False)
                    variances = h.variances(flow=False)
            except (KeyError, AttributeError, ValueError) as exc:
                raise FitError(f"missing or non-TH1 template {root_path}:{template}") from exc
            values = np.asarray(values, dtype=float)
            if values.ndim != 1 or np.any(~np.isfinite(values)) or np.any(values < 0):
                raise FitError(f"non-finite, negative, or non-1D template: {root_path}:{template}")
            variances = np.asarray(variances if variances is not None else values, dtype=float)
            if variances.shape != values.shape or np.any(~np.isfinite(variances)) or np.any(variances < 0):
                raise FitError(f"invalid sumw2: {root_path}:{template}")
            return dict(values=values.tolist(), variances=variances.tolist(), edges=edges.tolist())

        local_columns = []
        rate_scales = {}
        try:
            for channel in dict.fromkeys(columns):
                if channel in channel_names:
                    raise FitError(f"duplicate channel {channel}; do not mix combined and constituent cards")
                channel_names.add(channel)
                data = histogram("data_obs", channel)
                if data is None:
                    count = _number(obs[channel], "observation")
                    if count < 0:
                        raise FitError(f"missing data_obs for channel {channel}")
                    data = dict(values=[count], variances=[count], edges=[0., 1.])
                observed_sources[channel] = data
                model["channels"].append(dict(name=channel, data=data, processes=[]))
            channel_lookup = {c["name"]: c for c in model["channels"]}
            for channel, process, process_id, rate in zip(columns, names, ids, rates):
                nominal = histogram(process, channel)
                factor = 1.
                rate = _number(rate, "rate")
                if nominal is None:
                    if rate < 0:
                        raise FitError(f"missing nominal {channel}/{process}")
                    nominal = dict(values=[rate], variances=[0.], edges=[0., 1.])
                elif rate >= 0:
                    total = sum(nominal["values"])
                    if total == 0 and rate > 0:
                        raise FitError(f"positive rate with empty template {channel}/{process}")
                    factor = rate / total if total else 1.
                    nominal["values"] = [v * factor for v in nominal["values"]]
                    nominal["variances"] = [v * factor ** 2 for v in nominal["variances"]]
                elif rate != -1:
                    raise FitError(f"unsupported negative rate {rate}")
                if nominal["edges"] != observed_sources[channel]["edges"]:
                    raise FitError(f"data/process binning mismatch for {channel}/{process}")
                target = channel_lookup[channel]["processes"]
                if any(p["name"] == process for p in target):
                    raise FitError(f"duplicate process {channel}/{process}")
                target.append(dict(name=process, signal=process_id <= 0, **nominal))
                rate_scales[(channel, process)] = factor
                local_columns.append((channel, process))
                all_columns.append((channel, process, process_id <= 0))

            for tokens in after:
                if len(tokens) < 2:
                    raise FitError(f"unsupported card directive: {' '.join(tokens)}")
                name, kind, *args = tokens
                if kind in ("lnN", "shape"):
                    if len(args) != len(local_columns):
                        raise FitError(f"nuisance column count mismatch: {name}")
                    for (channel, process), effect in zip(local_columns, args):
                        if effect in ("-", "0"):
                            continue
                        record = dict(name=name, kind=kind, channel=channel, process=process)
                        if kind == "lnN":
                            factors = [_number(v, name) for v in effect.split("/")]
                            if len(factors) not in (1, 2) or any(v <= 0 for v in factors):
                                raise FitError(f"invalid lnN effect: {name} {effect}")
                            record["factors"] = factors
                        else:
                            record["factor"] = _number(effect, name)
                            for direction in ("Up", "Down"):
                                shifted = histogram(process, channel, name + direction)
                                if shifted is None or shifted["edges"] != observed_sources[channel]["edges"]:
                                    raise FitError(f"missing or mismatched shape {name}{direction}: {channel}/{process}")
                                scale = rate_scales[(channel, process)]
                                shifted["values"] = [v * scale for v in shifted["values"]]
                                shifted["variances"] = [v * scale ** 2 for v in shifted["variances"]]
                                record[direction.lower()] = shifted
                        model["effects"].append(record)
                elif kind == "rateParam":
                    if len(args) < 3:
                        raise FitError(f"invalid rateParam: {tokens}")
                    if args[2].startswith("("):
                        formula_rates.append((name, args))
                    else:
                        if len(args) != 4:
                            raise FitError(f"numeric rateParam needs explicit bounds: {tokens}")
                        initial = _number(args[2], name)
                        limits = None
                        if len(args) > 3:
                            limits = [_number(v, name) for v in args[3].strip("[]").split(",")]
                            if len(limits) != 2:
                                raise FitError(f"invalid bounds for {name}")
                        record = _parameter(name, initial, limits,
                                            poi=name in {r["parameter"] for r in metadata.get("active", [])})
                        if name in parameters and parameters[name] != record:
                            raise FitError(f"inconsistent shared parameter {name}")
                        parameters[name] = record
                        numeric_rates.append((name, args[0], args[1]))
                elif kind == "autoMCStats":
                    if args not in (["0", "1"], ["0", "1", "1"]):
                        raise FitError("supported autoMCStats policy is threshold 0, include signal, hist-mode 1")
                    if name != "*":
                        raise FitError("per-channel autoMCStats is not supported")
                    model["mc_stat"] = True
                elif kind == "group":
                    if args[:1] != ["="]:
                        raise FitError("only group = definitions are supported")
                    model["groups"].setdefault(name, []).extend(args[1:])
                elif kind == "flatParam":
                    if args:
                        raise FitError(f"unsupported flatParam syntax: {tokens}")
                    flat_params.add(name)
                elif kind == "extArg":
                    if len(args) != 2:
                        raise FitError(f"unsupported external argument: {tokens}")
                    ext_args[name] = args
                else:
                    raise FitError(f"unsupported card directive: {' '.join(tokens)}")
        finally:
            for file in files.values():
                file.close()

    # Analysis adapters supply structured formulas, never executable expressions.
    aliases = metadata.get("process_aliases", {})
    structured_names = set()
    for source in (polynomial, ttbar):
        if not source:
            continue
        for record in source["parameters"]:
            parsed = _parameter(record["parameter"], record["initial"],
                                (record["min"], record["max"]),
                                record.get("is_poi", False) if source is polynomial else False,
                                record.get("constant", False))
            parameters[parsed["parameter"]] = parsed
            structured_names.add(parsed["parameter"])
        source_names = {p["parameter"] for p in source["parameters"]}
        if len(source_names) != len(source["parameters"]):
            raise FitError("duplicate structured model parameters")
        for process, record in source["processes"].items():
            if record.get("normalization_only"):
                if record.get("normalization") not in source_names:
                    raise FitError(f"unknown normalization parameter for {process}")
                continue
            names, weights = record["parameters"], np.asarray(record["weights"], dtype=float)
            if (not names or len(names) != len(weights) or set(names) - source_names or
                    np.any(~np.isfinite(weights)) or np.any(weights < 0) or not np.isclose(weights.sum(), 1.)):
                raise FitError(f"invalid structured basis for {process}")
        for normalization in source.get("normalizations", {}).values():
            terms = normalization["reference_terms"]
            total = normalization["reference_total"]
            if (not terms or not np.isfinite(total) or total <= 0 or
                    any(t["process"] not in source["processes"] or not np.isfinite(t["yield"]) or t["yield"] < 0 for t in terms)):
                raise FitError("invalid structured normalization reference")
    unresolved = (flat_params | set(ext_args)) - structured_names
    if unresolved:
        raise FitError(f"unresolved flatParam/extArg: {sorted(unresolved)}")
    for name, args in ext_args.items():
        record = parameters[name]
        limits = [_number(t, name) for t in args[1].strip("[]").split(",")]
        if _number(args[0], name) != record["initial"] or limits != [record["min"], record["max"]]:
            raise FitError(f"extArg {name} differs from the staged structured model")
    for name, args in formula_rates:
        # Match the staged ttbar basis exactly before replacing its formula.
        process = aliases.get(args[1], args[1])
        rec = (ttbar or {}).get("processes", {}).get(process)
        expected = ("(" + "+".join(f"{w:.17g}*@{i}" for i, w in enumerate(rec["weights"])) + ")") if rec else None
        if not rec or args[0] != "*" or args[2] != expected or args[3:] != [",".join(rec["parameters"])]:
            raise FitError(f"unsupported formula rateParam {name}; a verified structured adapter is required")
    for channel, process, signal in all_columns:
        canonical = aliases.get(process, process)
        poly_canonical = (polynomial or {}).get("process_aliases", {}).get(process, canonical)
        if polynomial and poly_canonical in polynomial["processes"]:
            model["scales"].append(dict(channel=channel, process=process, kind="polynomial",
                                         record=polynomial["processes"][poly_canonical]))
        elif ttbar and canonical in ttbar["processes"]:
            model["scales"].append(dict(channel=channel, process=process, kind="linear",
                                         record=ttbar["processes"][canonical]))
        matched = False
        for name, ch_pattern, proc_pattern in numeric_rates:
            if fnmatch.fnmatchcase(channel, ch_pattern) and fnmatch.fnmatchcase(process, proc_pattern):
                model["scales"].append(dict(channel=channel, process=process, kind="parameter", parameter=name))
                matched = True
        if signal and not matched and not polynomial:
            name = "r" if shared_signal else process
            # Match the standard Combine default model's r[1,0,20].
            parameters.setdefault(name, _parameter(name, limits=(0., 20.), poi=True))
            model["scales"].append(dict(channel=channel, process=process, kind="parameter", parameter=name))
        elif signal and polynomial and poly_canonical not in polynomial["processes"]:
            raise FitError(f"signal {process} is absent from the structured polynomial model")
    if not metadata:
        for p in parameters.values():
            p["is_poi"] = True
    model["parameters"] = sorted(parameters.values(), key=lambda p: not p["is_poi"])
    internal_names = {f"rdf_internal_{i}" for i, p in enumerate(p for p in model["parameters"] if not p.get("constant"))}
    if internal_names & {e["name"] for e in model["effects"]}:
        raise FitError("nuisance name collides with Rabbit's internal model coordinates")
    if not model["parameters"] or not any(p["is_poi"] for p in model["parameters"]):
        raise FitError("model has no explicit parameters of interest")
    model["identity"] = hashlib.sha256(json.dumps(model, sort_keys=True).encode()).hexdigest()
    return model
