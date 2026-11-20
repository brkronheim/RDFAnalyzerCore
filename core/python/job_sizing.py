"""Workload profiles, automatic learning and entry caches for job planning.

Run ``python core/python/job_sizing.py --help`` to learn conservative event
budgets from completed worker logs plus an exported HTCondor history JSON.
No scheduler or production state is changed by this tool.
"""
from __future__ import annotations

import argparse
import collections
import concurrent.futures
import fnmatch
import fcntl
import glob
import hashlib
import json
import math
import os
from pathlib import Path
import re
import statistics
import socket
import tempfile
import time
from urllib.parse import urlsplit


def file_key(url: str) -> str:
    """CMS public LFNs are redirector independent; other URLs retain authority."""
    parsed = urlsplit(url)
    if parsed.scheme == "root" and parsed.path.lstrip("/").startswith("store/"):
        return "/" + parsed.path.lstrip("/")
    return url


def atomic_json(path: str | Path, value: object) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=path.name + ".", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as stream:
            json.dump(value, stream, indent=2, sort_keys=True, allow_nan=False)
            stream.write("\n")
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def positive_int(value, name: str, *, zero=False) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < (0 if zero else 1):
        raise ValueError(f"{name} must be {'nonnegative' if zero else 'positive'} integer")
    return value


class EntryCache:
    """Tree-scoped cache; local files also require a size/mtime fingerprint.

    A preparation owns its writable cache. Seed a new preparation from an
    offline snapshot instead of sharing a writable cache between preparations.
    """
    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else None
        self.data = {"schema_version": 1, "trees": {}}
        if self.path and self.path.exists():
            self.data = json.loads(self.path.read_text())
            if self.data.get("schema_version") != 1 or not isinstance(self.data.get("trees"), dict):
                raise ValueError("Invalid input entry-cache schema")
        self.dirty = False

    @staticmethod
    def fingerprint(url):
        if "://" in url or url.startswith("/store/"):
            return None
        stat = Path(url).stat()
        return [stat.st_size, stat.st_mtime_ns]

    def get(self, url, tree="Events"):
        record = self.data["trees"].get(tree, {}).get(file_key(url))
        if record is None:
            return None
        if record.get("fingerprint") != self.fingerprint(url):
            return None
        return positive_int(record["entries"], "cached entries", zero=True)

    def put(self, url, entries, tree="Events"):
        record = {"entries": positive_int(entries, "entries", zero=True)}
        fingerprint = self.fingerprint(url)
        if fingerprint is not None:
            record["fingerprint"] = fingerprint
        records = self.data["trees"].setdefault(tree, {})
        if records.get(file_key(url)) != record:
            records[file_key(url)] = record
            self.dirty = True

    def populate(self, urls, query, tree="Events", workers=4):
        # A public LFN can appear through several redirectors; query it once.
        missing = {file_key(url): url for url in sorted(set(urls)) if self.get(url, tree) is None}
        failures = []
        # Queries are bounded; save successful discoveries even if another fails.
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
                futures = {pool.submit(query, url, tree): url for url in missing.values()}
                for future in concurrent.futures.as_completed(futures):
                    try:
                        self.put(futures[future], future.result(), tree)
                    except Exception as exc:
                        failures.append((futures[future], exc))
        finally:
            self.save()
        if failures:
            url, error = failures[0]
            raise RuntimeError(f"Entry metadata query failed for {url}: {error}") from error

    def save(self):
        if self.path and self.dirty:
            atomic_json(self.path, self.data)
            self.dirty = False


def load_profile(path: str | Path | None) -> dict:
    if not path:
        return {}
    with open(path) as stream:
        if str(path).endswith((".yaml", ".yml")):
            import yaml
            profile = yaml.safe_load(stream)
        else:
            profile = json.load(stream)
    return _validate_profile(profile)


def _validate_profile(profile):
    if not isinstance(profile, dict) or profile.get("schema_version") != 1:
        raise ValueError("Partition profile requires schema_version: 1")
    if not isinstance(profile.get("datasets", {}), dict) or not isinstance(profile.get("rules", []), list):
        raise ValueError("Partition profile datasets/rules must be a mapping/list")
    sizing = {"entries_per_job", "cpu_seconds_per_event", "target_runtime_seconds", "samples",
              "wall_seconds_per_event", "setup_seconds", "measurement"}
    for layer in [profile.get("defaults", {}), *profile.get("datasets", {}).values()]:
        if not isinstance(layer, dict) or set(layer) - sizing:
            raise ValueError("Unknown partition sizing fields")
    for layer in profile.get("rules", []):
        if not isinstance(layer, dict) or set(layer) - sizing - {"dataset", "process", "year", "era"}:
            raise ValueError("Unknown partition rule fields")
    return profile


def event_budget(profile: dict, dataset, fallback: int) -> int:
    """Last matching rule wins, then an exact dataset override wins.

    A rate or a fixed event limit replaces the inherited sizing method.
    Rules can match dataset, process, year and era using case-sensitive globs.
    """
    policy = {"entries_per_job": fallback,
              "target_runtime_seconds": profile.get("target_runtime_seconds", 3600)}
    layers = [profile.get("defaults", {})]
    for rule in profile.get("rules", []):
        if not isinstance(rule, dict):
            raise ValueError("Each partition rule must be a mapping")
        if all(fnmatch.fnmatchcase(str(getattr(dataset, key if key != "dataset" else "name", "") or ""), str(rule[key]))
               for key in ("dataset", "process", "year", "era") if key in rule):
            layers.append(rule)
    layers.append(profile.get("datasets", {}).get(dataset.name, {}))
    for layer in layers:
        if not isinstance(layer, dict):
            raise ValueError("Partition sizing must be a mapping")
        if "entries_per_job" in layer and "cpu_seconds_per_event" in layer:
            raise ValueError("Choose entries_per_job or cpu_seconds_per_event, not both")
        if "entries_per_job" in layer:
            policy.pop("cpu_seconds_per_event", None)
        if "cpu_seconds_per_event" in layer:
            policy.pop("entries_per_job", None)
        policy.update(layer)
    if "entries_per_job" in policy:
        return positive_int(policy["entries_per_job"], "entries_per_job")
    rate, target = policy["cpu_seconds_per_event"], policy["target_runtime_seconds"]
    if any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or v <= 0 for v in (rate, target)):
        raise ValueError("CPU/event and target runtime must be finite and positive")
    wall_rate, setup = policy.get("wall_seconds_per_event", 0), policy.get("setup_seconds", 0)
    if any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or v < 0 for v in (wall_rate, setup)):
        raise ValueError("Wall rate and setup seconds must be finite and nonnegative")
    return max(1, math.floor(max(0, target - setup) / max(rate, wall_rate)))


def quantile(values, fraction):
    values = sorted(values)
    return values[min(len(values)-1, math.ceil(fraction * len(values))-1)]


def learn_rates(records, minimum=3, fraction=.9):
    """Normalize matched datasets across hosts, then project to slowest host.

    Fast-only completed samples are projected using matched-workload host
    factors rather than assuming every worker has the fast host's throughput.
    """
    groups = collections.defaultdict(list)
    for r in records:
        groups[r["dataset"], r["host"]].append(r["cpu_s"] / r["events"])
    groups = {k: v for k, v in groups.items() if len(v) >= minimum}
    hosts = sorted({h for _, h in groups})
    if not hosts:
        return {}, {}
    reference = max(hosts, key=lambda h: sum(len(v) for (d, node), v in groups.items() if node == h))
    factors = {reference: 1.}
    while len(factors) < len(hosts):
        additions = {}
        for host in hosts:
            if host in factors:
                continue
            ratios = [statistics.median(v) / statistics.median(groups[d, other]) * factors[other]
                      for (d, node), v in groups.items() if node == host
                      for other in factors if (d, other) in groups]
            if ratios:
                additions[host] = statistics.median(ratios)
        if not additions:
            break
        factors.update(additions)
    # Disconnected worker populations cannot be normalized; retain observed rates
    # there, without inventing a speed relationship to the reference population.
    maximum = max(factors.values())
    rates = {}
    for dataset in sorted({d for d, _ in groups}):
        estimates = [quantile(v, fraction) / factors[h] * maximum if h in factors else quantile(v, fraction)
                     for (d, h), v in groups.items() if d == dataset]
        rates[dataset] = {"cpu_seconds_per_event": max(estimates),
                          "samples": sum(len(v) for (d, h), v in groups.items() if d == dataset)}
    return rates, factors


def collect_log(path, ads):
    m = re.search(r"_(\d+)_(\d+)\.(?:stdout|out)$", path.name)
    text = path.read_text(errors="replace")
    metadata = []
    for match in re.finditer(r"\[InputCache\] reading statistics .*? tree=(\S+) .*? file=(\S+).*? tree_entries=(\d+)", text):
        tree, url, n = match.groups()
        # Historical local-file statistics lack a content fingerprint.
        if url.startswith("root://"):
            metadata.append((url, int(n), tree))
    record = None
    if 'RDF_JOB_SIZING_PILOT=' in text:
        return metadata, None  # Paired jobs need their own per-process fit.
    if m:
        ad = ads.get((int(m[1]), int(m[2])), {})
        count = re.search(r"CounterService: sample=(\S+) entries=(\d+)", text)
        analysis = re.search(r"^Analysis time:\s*(\d+)", text, re.M)
        if ad.get("JobStatus") == 4 and ad.get("ExitCode") == 0 and not ad.get("ExitBySignal") and "Done!" in text and count and int(count[2]) >= 100000:
            wait = sum(float(v) for v in re.findall(r"prefetch_wait_since_report_s=([\d.eE+-]+)", text))
            if analysis and wait / max(1, int(analysis[1])) < .1:
                host = re.search(r"\[XrdAdaptor\] context .*?execution_host=(\S+)", text)
                host = host[1] if host else ad.get("LastRemoteHost", "unknown").split("@")[-1]
                cpu = ad.get("RemoteUserCpu", 0) + ad.get("RemoteSysCpu", 0)
                if cpu > 0:
                    record = {"dataset": count[1], "events": int(count[2]), "cpu_s": cpu, "host": host}
    return metadata, record


def sizing_cache_root():
    configured = os.environ.get("RDF_JOB_SIZING_CACHE")
    if configured == "none":
        return None
    if configured:
        return Path(configured).expanduser()
    return Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache") / "rdfanalyzer/job_sizing"


def sizing_context(config, executable, *, payloads=(), runtime="", environment=""):
    """Match workload contents, excluding input membership and output locations.

    Executable/runtime and declared calibration/config contents are deliberately
    strict: changed systematics or code require fresh measurements.
    """
    def digest(path):
        h = hashlib.sha256()
        with Path(path).open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                h.update(block)
        return h.hexdigest()
    ignored = {"fileList", "sampleConfig", "sample", "saveFile", "saveDirectory", "metaFile",
               "outputDirectory", "firstEntry", "lastEntry", "plannedInputEntries"}
    assets = {Path(p).name: digest(p) for p in payloads}
    settings = {k: ("@payload:" + Path(v).name if Path(v).name in assets else v)
                for k, v in config.items() if k not in ignored and not k.startswith("metadata")}
    identity = dict(schema_version=1, config=settings, assets=assets,
                    executable=digest(executable), runtime=runtime, environment=environment)
    key = hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return dict(workload=key, origin=os.environ.get("RDF_JOB_SIZING_ORIGIN") or socket.getfqdn())


def _sizing_cache_path(context):
    root = sizing_cache_root()
    if root is None:
        return None
    key = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()
    return root / (key + ".json")


def cached_sizing(context):
    """Read one atomic generation; callers copy it into their preparation."""
    path = _sizing_cache_path(context)
    if path is None or not path.is_file():
        return None
    state = json.loads(path.read_text())
    if state.get("schema_version") != 1 or state.get("context") != context:
        raise ValueError("Invalid automatic job-sizing cache")
    if state.get("updated", 0) < time.time() - 30 * 86400:
        return None
    _validate_profile(state.get("profile"))
    if not isinstance(state.get("runs"), dict) or state.get("metadata", {}).get("schema_version") != 1:
        raise ValueError("Invalid automatic job-sizing observations")
    return state


def select_sizing(run_dir, context, *, profile="", metadata_seed="", target_seconds=3600,
                  pilot_target_seconds=1800, allow_refresh=False):
    """Freeze one selection per run; later cache changes cannot invalidate jobs."""
    positive_int(target_seconds, 'sizing target seconds')
    positive_int(pilot_target_seconds, 'pilot sizing target seconds')
    run = Path(run_dir)
    run.mkdir(parents=True, exist_ok=True)
    receipt = run / "job_sizing_selection.json"
    requested = {"profile": profile, "metadata_seed": metadata_seed}
    for name, source in list(requested.items()):
        if source and source != "none":
            requested[name + "_sha256"] = hashlib.sha256(Path(source).read_bytes()).hexdigest()
    with (run / ".job_sizing_selection.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if receipt.is_file():
            saved = json.loads(receipt.read_text())
            changed_workload = saved.get("context") != context
            changed_inputs = saved.get("requested") != requested
            if not changed_workload and not changed_inputs:
                return saved
            if not allow_refresh:
                kind = "workload" if changed_workload else "profile inputs"
                raise ValueError(f"Job-sizing {kind} changed; use a new run name")
        selected = dict(schema_version=1, context=context, requested=requested,
                        profile="", metadata_seed="", source="default")
        state = None
        if profile != "none":
            try:
                state = cached_sizing(context)
            except (ValueError, OSError, KeyError, TypeError) as exc:
                selected["warning"] = f"Ignoring unusable automatic sizing cache: {exc}"
        if profile and profile != "none":
            chosen = load_profile(profile)
            selected["source"] = "explicit"
        elif state and state.get("profile", {}).get("datasets"):
            chosen = dict(state["profile"], target_runtime_seconds=target_seconds)
            # Cached pilot priors and supported production observations can
            # coexist. Apply this submission's policy to each copied dataset,
            # without changing shared rates or an already frozen selection.
            chosen['datasets'] = {
                name: dict(sizing, target_runtime_seconds=(
                    pilot_target_seconds if 'prefix' in sizing.get('measurement', {})
                    and 'full' in sizing.get('measurement', {}) else target_seconds))
                for name, sizing in chosen['datasets'].items()}
            selected["source"] = "learned"
        else:
            chosen = None
        if chosen:
            target = run / "partition_profile.selected.json"
            atomic_json(target, chosen)
            selected["profile"] = str(target.resolve())
        if metadata_seed:
            metadata = EntryCache(metadata_seed).data
        else:
            metadata = state.get("metadata") if state else None
        if metadata:
            target = run / "input_metadata.seed.json"
            atomic_json(target, metadata)
            selected["metadata_seed"] = str(target.resolve())
        atomic_json(receipt, selected)
        return selected


def profile_matches(profile, dataset):
    """Auto mode keeps ordinary grouping for datasets without sizing evidence."""
    sizing = {"cpu_seconds_per_event", "entries_per_job"}
    if sizing.intersection(profile.get("defaults", {})) or sizing.intersection(profile.get("datasets", {}).get(dataset.name, {})):
        return True
    return any(sizing.intersection(rule) and all(
        fnmatch.fnmatchcase(str(getattr(dataset, key if key != "dataset" else "name", "") or ""), str(rule[key]))
        for key in ("dataset", "process", "year", "era") if key in rule)
        for rule in profile.get("rules", []))


def fit_pilot(measurement, safety_factor=1.5, transfer_seconds=0):
    """Fit T(N)=setup+rate*N from paired processes reading the same file.

    CPU and wall differences remove repeatable graph/open setup. If a file is
    too short or the timing slope is nonpositive, the full-run average is a
    conservative fallback; no measured time is silently subtracted twice.
    """
    if not math.isfinite(safety_factor) or safety_factor < 1:
        raise ValueError("Pilot safety factor must be finite and at least one")
    short, full = measurement['prefix'], measurement['full']
    n, n0 = positive_int(full['events'], 'pilot full events'), positive_int(short['events'], 'pilot prefix events', zero=True)
    if n0 > n:
        raise ValueError('Pilot prefix has more events than full input')
    rates, fixed, methods = {}, {}, {}
    for dimension in ('cpu', 'wall'):
        a, b = short[dimension+'_s'], full[dimension+'_s']
        if any(isinstance(v, bool) or not isinstance(v, (float, int)) or not math.isfinite(v) or v <= 0 for v in (a, b)):
            raise ValueError('Pilot timings must be finite and positive')
        if n > n0 and b > a:
            rates[dimension] = (b-a)/(n-n0)
            fixed[dimension] = max(0, a-rates[dimension]*n0)
            methods[dimension] = 'paired_slope'
        else:
            rates[dimension] = b/n
            methods[dimension] = 'full_average_fallback'
    return dict(cpu_seconds_per_event=rates['cpu']*safety_factor,
                wall_seconds_per_event=rates['wall']*safety_factor,
                setup_seconds=(max(fixed.values() or [0])+max(0, transfer_seconds))*safety_factor,
                samples=1, measurement=dict(measurement, methods=methods, safety_factor=safety_factor,
                                            fixed_cpu_seconds=fixed.get('cpu'), fixed_wall_seconds=fixed.get('wall'),
                                            raw_cpu_seconds_per_event=rates['cpu'], raw_wall_seconds_per_event=rates['wall'],
                                            transfer_seconds=transfer_seconds))


def pilot_inputs(manifest, directory, profile):
    """Choose one deterministic file for each unprofiled manifest dataset."""
    discovered = {}
    for path in sorted(Path(directory).glob('*.json')):
        if path.name.endswith('.perf.json'):
            continue
        payload = json.loads(path.read_text())
        name = str(payload.get('sample', path.stem))
        if manifest.by_name(name) is None:
            continue
        if name in discovered:
            raise ValueError(f'Duplicate discovery payload for {name}')
        discovered[name] = payload
    selected = []
    for entry in manifest.datasets:
        if profile_matches(profile, entry):
            continue
        payload = discovered.get(entry.name)
        if payload is None:
            raise ValueError(f'No discovered input for pilot dataset {entry.name}')
        groups = [u.strip() for g in payload.get('groups', []) for u in str(g).split(',') if u.strip()]
        files = sorted({u.strip() for u in (payload.get('files') or groups) if u.strip()})
        if not files or (groups and (set(files) != set(groups) or len(groups) != len(set(groups)))):
            raise ValueError(f'Empty or inconsistent pilot input inventory for {entry.name}')
        copied = entry.__class__.from_dict(entry.to_dict())
        copied.files = files[:1]
        copied.das = None
        selected.append(copied)
    return selected


def seed_pilot_sizing(context, profile, metadata):
    """Publish provisional one-file priors; regular supported rates supersede them."""
    if not profile.get('datasets') and not any(metadata.get('trees', {}).values()):
        return {'status': 'skipped', 'reason': 'no pilot measurements'}
    path = _sizing_cache_path(context)
    if path is None or os.environ.get('RDF_JOB_SIZING_AUTO_UPDATE', '1').lower() in {'0', 'false', 'off', 'no'}:
        return {'status': 'disabled'}
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.with_suffix('.lock').open('a') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = cached_sizing(context) or dict(schema_version=1, context=context, runs={},
                    profile={'schema_version': 1, 'datasets': {}}, metadata={'schema_version': 1, 'trees': {}})
        now = time.time()
        priors = state.setdefault('pilot_priors', {})
        for name, sizing in profile.get('datasets', {}).items():
            if name not in state['profile']['datasets']:
                priors[name] = dict(observed=now, sizing=sizing)
                state['profile']['datasets'][name] = sizing
        cache = EntryCache();cache.data = state['metadata']
        for tree, files in metadata.get('trees', {}).items():
            for url, row in files.items():
                known = cache.get(url, tree)
                if known is not None and known != row['entries']:
                    raise ValueError(f'Conflicting entry metadata for {url}')
                cache.put(url, row['entries'], tree)
        state['updated'] = now
        atomic_json(path, state)
        return dict(status='updated', cache=str(path), datasets=len(priors))


def condor_event_ads(paths):
    """Read final successful execution CPU from returned events, without a query.

    Run Remote Usage belongs to the final execution; Total Remote Usage can
    include evicted attempts whose events are absent from the final stdout.
    """
    ads = {}
    hosts = {}
    for path in sorted(paths, key=lambda p: (p.stat().st_mtime_ns, str(p))):
        for block in path.read_text(errors="replace").split("\n..."):
            match = re.search(r"(?m)^(\d{3}) \((\d+)\.(\d+)\.\d+\)", block.strip())
            if not match:
                continue
            event, cluster, proc = match.groups()
            key = int(cluster), int(proc)
            if event == "001":
                host = re.search(r"SlotName:\s*\S+@([^\s]+)", block)
                if host:
                    hosts[key] = host[1]
            if event in {"001", "004", "009"}:
                ads.pop(key, None)
            if event != "005":
                continue
            ads.pop(key, None)
            if not re.search(r"Normal termination \(return value 0\)", block):
                continue
            usage = re.search(r"Usr (\d+) (\d+):(\d+):(\d+), Sys (\d+) (\d+):(\d+):(\d+)\s+-\s+Run Remote Usage", block)
            if not usage:
                continue
            values = list(map(int, usage.groups()))
            seconds = lambda v: v[0]*86400 + v[1]*3600 + v[2]*60 + v[3]
            ads[key] = dict(JobStatus=4, ExitCode=0, RemoteUserCpu=seconds(values[:4]),
                            RemoteSysCpu=seconds(values[4:]), LastRemoteHost=hosts.get(key, "unknown"))
    return ads


def update_sizing_after_run(run_dir, context, *, target_seconds=3600):
    """Called after verified completion only; serialize/idempotently learn runs.

    Keep raw successful cost observations for 32 runs/30 days, so repeated
    completions do not compound an already summarized percentile. Metadata can
    also be learned from failed attempts, but rates require a successful exit.
    """
    if os.environ.get("RDF_JOB_SIZING_AUTO_UPDATE", "1").lower() in {"0", "false", "no", "off"}:
        return dict(status="disabled")
    cache_path = _sizing_cache_path(context)
    if cache_path is None:
        return dict(status="disabled")
    run = Path(run_dir).resolve()
    logs = sorted(run.glob("condor_submissions/*/condor_logs/*out"))
    events = sorted(run.glob("condor_submissions/*/condor_logs/*.log"))
    if not logs:
        return dict(status="skipped", reason="no returned worker logs")
    fingerprint = hashlib.sha256(json.dumps([
        [str(p), p.stat().st_size, p.stat().st_mtime_ns] for p in logs + events
    ]).encode()).hexdigest()
    ads = condor_event_ads(events)
    metadata, records = [], {}
    for path in logs:
        entries, record = collect_log(path, ads)
        metadata.extend(entries)
        match = re.search(r"_(\d+)_(\d+)\.(?:stdout|out)$", path.name)
        if record and match:
            records[".".join(match.groups())] = record
    if not metadata and not records:
        return dict(status="skipped", reason="no usable event counts or successful CPU measurements")
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with cache_path.with_suffix(".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        state = cached_sizing(context) or dict(schema_version=1, context=context, runs={},
                                              metadata={"schema_version": 1, "trees": {}})
        if state["runs"].get(str(run), {}).get("fingerprint") == fingerprint:
            return dict(status="unchanged", cache=str(cache_path))
        now = time.time()
        state["runs"][str(run)] = dict(fingerprint=fingerprint, completed=now, records=records)
        state["runs"] = dict(sorted(((k, v) for k, v in state["runs"].items()
                                     if v["completed"] >= now - 30*86400),
                                    key=lambda kv: kv[1]["completed"], reverse=True)[:32])
        unique = {(run_id, key): record for run_id, value in state["runs"].items()
                  for key, record in value["records"].items()}
        rates, factors = learn_rates(unique.values())
        state['pilot_priors'] = {k: v for k, v in state.get('pilot_priors', {}).items()
                                 if v['observed'] >= now - 30*86400}
        rates = {**{k: v['sizing'] for k, v in state['pilot_priors'].items()}, **rates}
        entry_cache = EntryCache()
        entry_cache.data = state["metadata"]
        for url, n, tree in metadata:
            known = entry_cache.get(url, tree)
            if known is not None and known != n:
                raise ValueError(f"Conflicting entry metadata for {url}")
            entry_cache.put(url, n, tree)
        state.update(updated=now, profile={"schema_version": 1, "target_runtime_seconds": target_seconds,
                     "datasets": rates, "host_cost_factors": factors,
                     "provenance": {"completed_runs": len(state["runs"]), "cost_samples": len(unique)}})
        atomic_json(cache_path, state)
        return dict(status="updated", cache=str(cache_path), datasets=len(rates),
                    cost_samples=len(unique), completed_runs=len(state["runs"]),
                    cached_files=sum(len(v) for v in entry_cache.data["trees"].values()))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", help="Worker stdout paths, globs or skimRun directories")
    parser.add_argument("--history", help="condor_history -json export for verified successful CPU times")
    parser.add_argument("--profile", help="Output partition-profile JSON")
    parser.add_argument("--metadata-cache", required=True, help="Output/input entry-cache JSON snapshot")
    parser.add_argument("--target-seconds", type=float, default=3600)
    parser.add_argument("--min-samples", type=int, default=3)
    parser.add_argument("--quantile", type=float, default=.9)
    args = parser.parse_args(argv)
    if args.profile and not args.history:
        parser.error("--profile requires --history; wall time is not a substitute for CPU time")
    if args.min_samples < 1 or not 0 < args.quantile <= 1 or not math.isfinite(args.target_seconds) or args.target_seconds <= 0:
        parser.error("invalid sample count, quantile or runtime target")
    ads = {}
    if args.history:
        ads = {(r["ClusterId"], r["ProcId"]): r for r in json.loads(Path(args.history).read_text())}
    paths = set()
    for pattern in args.logs:
        for p in map(Path, glob.glob(pattern)):
            if p.is_dir():
                paths.update(p.glob("condor_submissions/*/condor_logs/*out"))
                paths.update(p.glob("*out"))
            else:
                paths.add(p)
    if not paths:
        parser.error("no worker logs matched")
    cache = EntryCache(args.metadata_cache)
    records = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        for metadata, record in pool.map(lambda p: collect_log(p, ads), sorted(paths)):
            for url, n, tree in metadata:
                known = cache.get(url, tree)
                if known is not None and known != n:
                    raise ValueError(f"Conflicting entry metadata for {url}")
                cache.put(url, n, tree)
            if record:
                records.append(record)
    cache.save()
    if args.profile:
        rates, factors = learn_rates(records, args.min_samples, args.quantile)
        if not rates:
            parser.error("no datasets have sufficient verified CPU-dominated samples")
        atomic_json(args.profile, {"schema_version": 1, "target_runtime_seconds": args.target_seconds,
                                 "datasets": rates, "host_cost_factors": factors,
                                 "provenance": {"logs": len(paths), "successful_cost_samples": len(records),
                                                "quantile": args.quantile, "min_samples": args.min_samples}})
    print(json.dumps({"logs": len(paths), "cost_samples": len(records),
                      "cached_files": sum(len(v) for v in cache.data["trees"].values())}))


if __name__ == "__main__":
    main()
