"""Build origin-specific XrdAdaptor ranking snapshots from returned job logs.

Usage: python3 core/python/xrd_site_rankings.py RUN_DIRECTORY --output sites.tsv
       --base extensions/cms/data/xrd_site_rankings.tsv

Only observed measurements are aggregated; loaded priors and process blacklists
are never converted to outage evidence. No network requests are performed.
"""
from __future__ import annotations

import argparse
import collections
import datetime as dt
import fcntl
import glob
import hashlib
import json
import math
import os
import re
import statistics
import tempfile
import time
from urllib.parse import urlsplit
from pathlib import Path

MAGIC = "# rdf-xrd-site-rankings-v1"
SUMMARY = re.compile(r"\[XrdAdaptor\]\s+\d+\s+(\S+)\s+(n/a|[\d.]+)\s+"
                     r"(yes|no)\s+(yes|no)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)")


def fields(line):
    return dict(re.findall(r"(\w+)=([^\s]+)", line))


def token(value):
    value = str(value).lower()
    if not re.fullmatch(r"[a-z0-9_.:\[\]-]+", value):
        raise ValueError(f"Invalid ranking identifier: {value!r}")
    return value


def read_snapshot(path):
    """Validate a snapshot before staging it; retain per-origin evidence."""
    if Path(path).stat().st_size > 4 * 1024 * 1024:
        raise ValueError("Ranking snapshot exceeds 4 MiB")
    lines = Path(path).read_text().splitlines()
    if not lines or lines[0] != MAGIC or len(lines) > 50001:
        raise ValueError(f"Unsupported or oversized ranking snapshot: {path}")
    sites, aliases, endpoints = {}, {}, {}
    for line in lines[1:]:
        if not line or line.startswith("#"):
            continue
        if len(line) > 4096:
            raise ValueError("Ranking record exceeds size limit")
        parts = line.split()
        kind = parts[0]
        if kind in ("alias", "endpoint") and len(parts) == 3:
            table = aliases if kind == "alias" else endpoints
            key, value = map(token, parts[1:])
            if key in table:
                raise ValueError(f"Duplicate {kind}: {key}")
            table[key] = value
        elif kind == "site" and len(parts) == 6:
            origin, site = map(token, parts[1:3])
            quality, jobs, updated = float(parts[3]), int(parts[4]), int(parts[5])
            if not math.isfinite(quality) or not 1 <= quality <= 1e9 or not 1 <= jobs <= 10**9 or updated <= 0:
                raise ValueError("Invalid ranking measurements")
            if (origin, site) in sites:
                raise ValueError(f"Duplicate site: {origin}/{site}")
            sites[origin, site] = dict(quality_ms=quality, jobs=jobs, updated=updated)
        else:
            raise ValueError(f"Invalid ranking record: {line!r}")
    return sites, aliases, endpoints


def ranking_cache_path():
    """Shared across analyses, with separate origin profiles inside the file."""
    configured = os.environ.get("RDF_XRD_SITE_RANKING_CACHE")
    if configured == "none":
        return None
    if configured:
        return Path(configured).expanduser()
    root = Path(os.environ.get("XDG_CACHE_HOME") or Path.home() / ".cache")
    return root / "rdfanalyzer" / "xrd_site_rankings.tsv"


def submission_ranking_source(seed=None):
    """Explicit pins win; otherwise use learned history, then the provider seed."""
    explicit = os.environ.get("RDF_XRD_SITE_RANKING_FILE")
    if explicit is not None:
        return explicit
    cache = ranking_cache_path()
    if cache and cache.is_file():
        read_snapshot(cache)
        return cache
    return seed


def _atomic_json(path, value):
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=path.name + ".", delete=False) as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.replace(path)


def update_after_run(run_dir, *, seed=None, min_jobs=5):
    """Learn only when the caller has verified the entire LAW production run.

    Keep a bounded registry of completed runs, re-aggregate their available logs
    for exact job-level medians, and retain previous cache rows when old logs
    were cleaned up. The snapshot's own observation times still govern expiry.
    A lock serializes different monitors sharing a cache; an unchanged run is
    idempotent. This never updates the bundled seed or an explicitly pinned file.
    """
    if os.environ.get("RDF_XRD_SITE_RANKING_AUTO_UPDATE", "1").lower() in {"0", "false", "no", "off"}:
        return dict(status="disabled")
    if os.environ.get("RDF_XRD_SITE_RANKING_FILE") == "none":
        return dict(status="disabled")
    cache = ranking_cache_path()
    if cache is None:
        return dict(status="disabled")
    run = Path(run_dir).resolve()
    logs = sorted(run.glob("condor_submissions/*/condor_logs/*out"))
    # No persistent cache side effects for local/non-Xrd jobs, empty campaigns,
    # or monitors whose returned logs contain no usable storage measurements.
    usable = False
    for log in logs:
        try:
            rows, opens, _ = observations(log)
            usable = any(r["site"] != "unknown-site" and r["samples"] for r in rows) or any(
                f.get("endpoint_role") == "storage" and f.get("site") != "unknown-site" for f in opens)
        except (OSError, ValueError, KeyError):
            continue
        if usable:
            break
    if not usable:
        return dict(status="skipped", reason="no site measurements in returned logs")
    fingerprint = hashlib.sha256(json.dumps([
        [str(p), p.stat().st_size, p.stat().st_mtime_ns] for p in logs
    ], separators=(",", ":")).encode()).hexdigest()
    cache.parent.mkdir(parents=True, exist_ok=True)
    registry_path = cache.with_suffix(cache.suffix + ".runs.json")
    with cache.with_suffix(cache.suffix + ".lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        registry = json.loads(registry_path.read_text()) if registry_path.is_file() else {"schema": 1, "runs": {}}
        if registry.get("schema") != 1 or not isinstance(registry.get("runs"), dict):
            raise ValueError("Invalid XRootD completed-run ranking registry")
        record = registry["runs"].get(str(run), {})
        if record.get("fingerprint") == fingerprint and cache.is_file():
            return dict(status="unchanged", cache=str(cache), fingerprint=fingerprint)
        now = time.time()
        registry["runs"][str(run)] = dict(fingerprint=fingerprint, completed=now, logs=[str(p) for p in logs])
        recent = sorted((item for item in registry["runs"].items()
                         if item[1]["completed"] >= now - 30 * 86400),
                        key=lambda item: item[1]["completed"], reverse=True)[:32]
        registry["runs"] = dict(recent)
        available = [Path(p) for _, record in recent for p in record["logs"] if Path(p).is_file()]
        base = cache if cache.is_file() else submission_ranking_source(seed)
        # Cache write and registry update are ordered under the same lock. If
        # interrupted between them, replay re-aggregates jobs without adding
        # historical pseudo-samples or counting a run twice.
        report = build(available, cache, base=base, min_jobs=min_jobs)
        _atomic_json(registry_path, registry)
        return dict(status="updated", cache=str(cache), fingerprint=fingerprint,
                    input_logs=report["input_logs"], completed_runs=len(recent),
                    site_groups=len(report["sites"]), rejected_logs=report["rejected_logs"])


def condor_context(path):
    """Read the matching execution event, never infer geography from an IP."""
    match = re.search(r"_(\d+)_(\d+)\.(?:stdout|out)$", path.name)
    if not match:
        return {}
    cluster, proc = map(int, match.groups())
    result = {}
    for event in sorted(path.parent.glob("*.log")):
        for block in event.read_text(errors="replace").split("\n..."):
            m = re.search(r"(\d{3}) \((\d+)\.(\d+)\.\d+\)", block)
            if not m or (int(m[2]), int(m[3])) != (cluster, proc):
                continue
            alias = re.search(r"alias=([^&>\s]+)", block)
            if m[1] == "000" and alias:
                result["origin"] = alias[1]
            elif m[1] == "001":
                host = re.search(r"SlotName:\s*\S+@([^\s]+)", block)
                if host or alias:
                    result["host"] = host[1] if host else alias[1]
    return result


def observations(path, origin_override=None):
    text = path.read_text(errors="replace")
    context = condor_context(path)
    context.update(origin=origin_override or context.get("origin"))
    structured, legacy, opens, endpoints = {}, {}, [], {}
    updated = int(path.stat().st_mtime)
    for line in text.splitlines():
        if "[XrdAdaptor]" in line and " server=" in line and " site=" in line and "open-" not in line:
            f = fields(line)
            if f.get("site") not in (None, "unknown-site"):
                host = urlsplit("root://" + f["server"]).hostname
                if host:
                    endpoints[host] = f["site"]
        if "[XrdAdaptor] context " in line:
            context.update(fields(line))
        elif "[XrdAdaptor] site-stat " in line:
            f = fields(line)
            if not {"site", "quality_ms", "samples", "read_errors", "successful_bytes", "epoch"} <= f.keys():
                continue  # A killed worker may leave an unfinished final record.
            structured[f["site"]] = dict(site=f["site"], quality_ms=float(f["quality_ms"]),
                origin=f.get("origin", context.get("origin")),
                samples=int(f["samples"]), read_errors=int(f["read_errors"]),
                successful_bytes=int(f["successful_bytes"]), updated=int(f["epoch"]))
        elif (m := SUMMARY.search(line)) and m[2] != "n/a":
            site, quality, _, _, errors, _, samples, size = m.groups()
            legacy[site] = dict(site=site, quality_ms=float(quality), samples=int(samples),
                read_errors=int(errors), successful_bytes=int(size), updated=updated)
        elif "[XrdAdaptor] open-result " in line or "[XrdAdaptor] stat-result " in line:
            f = fields(line)
            if not {"site", "server", "endpoint_role", "status", "epoch"} <= f.keys():
                continue
            f["operation"] = "stat" if "stat-result " in line else "open"
            opens.append(f)
    origin = context.get("origin") or next((r.get("origin") for r in structured.values() if r.get("origin")), None)
    origin = origin or next((r.get("origin") for r in opens if r.get("origin")), None)
    if not origin and (structured or legacy or opens):
        raise ValueError(f"No execution origin in {path}; supply --origin for untagged logs")
    rows = list((structured or legacy).values())
    for row in rows:
        row.update(origin=token(row.get("origin") or origin), host=context.get("execution_host", context.get("host", "")),
                   dataset=path.parent.parent.name, path=str(path))
    for opened in opens:
        host = urlsplit("root://" + opened["server"]).hostname
        opened.update(origin=token(opened.get("origin", origin)), path=str(path),
                      site=endpoints.get(host, opened.get("site", "unknown-site")))
    return rows, opens, endpoints


def build(paths, output, *, base=None, origin=None, min_jobs=5):
    sites, aliases, endpoints = read_snapshot(base) if base else ({}, {}, {})
    groups, open_groups = collections.defaultdict(list), collections.defaultdict(list)
    errors, hosts = [], collections.defaultdict(set)
    def update_site(key, value):
        previous = sites.get(key)
        if previous is None or (value["updated"], value["jobs"]) >= (previous["updated"], previous["jobs"]):
            sites[key] = value
    # Resolve glob overlaps and copies of the same Condor job before aggregation.
    unique = {}
    parsed = {}
    for path in sorted({Path(p).resolve() for p in paths}):
        try:
            parsed[path] = observations(path, origin)
        except (ValueError, KeyError) as exc:
            errors.append(str(exc))
            continue
        rows, opens, _ = parsed[path]
        source = rows[0]["origin"] if rows else opens[0]["origin"] if opens else str(path)
        match = re.search(r"_(\d+)_(\d+)\.(?:stdout|out)$", path.name)
        key = (source, path.parent.parent.name, *match.groups()) if match else (str(path),)
        if key not in unique or path.stat().st_mtime > unique[key].stat().st_mtime:
            unique[key] = path
    for path in unique.values():
        rows, opens, mapped = parsed[path]
        for host, site in mapped.items():
            endpoints[token(host)] = token(site)
        for row in rows:
            if row["site"] == "unknown-site":
                continue
            groups[row["origin"], token(row["site"])].append(row)
            if row["host"]:
                hosts[token(row["host"])].add(row["origin"])
        for f in opens:
            # The last host in a redirect chain is only a candidate: do not
            # convert an ambiguous redirector failure into a storage outage.
            if f.get("endpoint_role") == "storage" and f.get("site") != "unknown-site":
                open_groups[token(f["origin"]), token(f["site"])].append(f)
                host = urlsplit("root://" + f["server"]).hostname
                if host:
                    endpoints[token(host)] = token(f["site"])
    evidence, measured_sites = [], set()
    for (source, site), rows in sorted(groups.items()):
        adequate = [r for r in rows if r["samples"] - r["read_errors"] >= 3
                    and r["successful_bytes"] >= 3 * 256 * 1024
                    and math.isfinite(r["quality_ms"]) and r["quality_ms"] >= 1]
        failed = sum(r["read_errors"] for r in rows)
        count = sum(r["samples"] for r in rows)
        quality = statistics.median(r["quality_ms"] for r in adequate) if adequate else None
        evidence.append(dict(origin=source, site=site, adequate_jobs=len(adequate), jobs=len(rows),
            datasets=len({r["dataset"] for r in rows}), read_errors=failed, samples=count,
            read_error_fraction=failed / count if count else None, quality_ms=quality,
            open_attempts=len(open_groups[source, site]),
            open_failures=sum(f.get("status") == "failed" for f in open_groups[source, site]),
            paths=[r["path"] for r in rows]))
        # Open availability is separate from read latency, but repeated storage
        # open failures may weaken the initial prior. Discovery exhaustion at a
        # redirector never receives this penalty.
        opened = open_groups[source, site]
        failure_fraction = sum(f.get("status") == "failed" for f in opened) / len(opened) if opened else 0
        if len(adequate) >= min_jobs:
            measured_sites.add((source, site))
            update_site((source, site), dict(quality_ms=max(quality, 5130 * failure_fraction), jobs=len(adequate),
                                      updated=max(r["updated"] for r in adequate)))
    for (source, site), opened in sorted(open_groups.items()):
        if (source, site) in measured_sites:
            continue
        support = len({f["path"] for f in opened})
        errors_count = sum(f.get("status") == "failed" for f in opened)
        if (source, site) not in groups:
            evidence.append(dict(origin=source, site=site, adequate_jobs=0, jobs=support,
                open_attempts=len(opened), open_failures=errors_count, quality_ms=None))
        if support >= min_jobs and errors_count:
            update_site((source, site), dict(quality_ms=max(260, 5130 * errors_count / len(opened)), jobs=support,
                updated=max(int(f["epoch"]) for f in opened)))
    for host, origins in hosts.items():
        # A hostname reused across execution sites must not silently merge them.
        if len(origins) == 1 and (host not in aliases or aliases[host] in origins):
            aliases[host] = next(iter(origins))
        else:
            aliases.pop(host, None)
    if not evidence:
        raise ValueError("No usable site summaries found")
    lines = [MAGIC, "# site ORIGIN SITE QUALITY_MS ADEQUATE_JOBS OBSERVED_UNIX",
             "# Quality is a median of final rolling scores; not RTT or lifetime latency."]
    lines += [f"alias\t{host}\t{source}" for host, source in sorted(aliases.items())]
    lines += [f"endpoint\t{host}\t{site}" for host, site in sorted(endpoints.items())]
    lines += [f"site\t{source}\t{site}\t{r['quality_ms']:.3f}\t{r['jobs']}\t{r['updated']}"
              for (source, site), r in sorted(sites.items())]
    target = Path(output)
    target.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", dir=target.parent, prefix=target.name + ".", delete=False) as stream:
        stream.write("\n".join(lines) + "\n")
        temporary = Path(stream.name)
    read_snapshot(temporary)
    temporary.replace(target)
    report = dict(schema=1, generated_utc=dt.datetime.now(dt.timezone.utc).isoformat(),
                  input_logs=len(unique), rejected_logs=errors, sites=evidence,
                  note="Priors are origin scoped and exclude inherited synthetic measurements")
    target.with_suffix(target.suffix + ".json").write_text(json.dumps(report, indent=2) + "\n")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("logs", nargs="+", help="Log files, directories, or quoted glob patterns")
    parser.add_argument("--output", required=True)
    parser.add_argument("--base", help="Preserve profiles from an existing snapshot; replace observed site rows")
    parser.add_argument("--origin", help="Origin for legacy logs; explicit worker context takes precedence")
    parser.add_argument("--min-jobs", type=int, default=5)
    args = parser.parse_args()
    if args.min_jobs < 1:
        parser.error("--min-jobs must be positive")
    paths = []
    for pattern in args.logs:
        for value in glob.glob(pattern, recursive=True):
            path = Path(value)
            paths.extend(p for p in path.rglob("*") if p.suffix in (".out", ".stdout")) if path.is_dir() else paths.append(path)
    if not paths:
        parser.error("No logs matched")
    try:
        report = build(paths, args.output, base=args.base, origin=args.origin, min_jobs=args.min_jobs)
    except ValueError as exc:
        parser.error(str(exc))
    print(f"Read {report['input_logs']} logs; {len(report['sites'])} origin/site groups; "
          f"{len(report['rejected_logs'])} rejected logs. Wrote {args.output}")


if __name__ == "__main__":
    main()
