#!/usr/bin/env python3
"""Replay staged single-slot workers concurrently under legacy and adaptive XRootD policies."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

from benchmark_worker import load_worker_config, load_yaml_document, write_config_yaml
from compare_worker_products import compare


def link_bytes():
    receipt = os.environ.get("RDF_BENCHMARK_XRD_RECEIPT")
    if not receipt:
        return {}
    receipt = Path(receipt)
    links = {site: (receipt / f"{site}.ns", "uplink") for site in ("a", "b")}
    if int(os.environ.get("RDF_BENCHMARK_SHARED_MBIT", 0)):
        links["shared_client"] = receipt / "controller.pid", "r-client"
    result = {}
    for key, (pidfile, device) in links.items():
        data = subprocess.check_output(["nsenter", "-t", pidfile.read_text().strip(), "-n",
                                        "ip", "-s", "-j", "link", "show", "dev", device])
        result[key] = json.loads(data)[0]["stats64"]["tx"]["bytes"]
    return result


def run_case(row, path, policy, folder):
    name = row["name"]
    src = (path / row["config"]).resolve(strict=True)
    worker = (path / row["worker"]).resolve(strict=True)
    cwd = (path / row.get("cwd", src.parent)).resolve(strict=True)
    folder.mkdir()
    uri = row["input"]
    if "://" not in uri:
        uri = os.environ["RDF_BENCHMARK_XRD_URL"] + "/" + uri.lstrip("/")
    if not uri.startswith(("root://", "roots://")):
        raise ValueError(f"{name}: use an XRootD URL or a file in the test federation")
    start, entries = int(row.get("first_entry", 0)), int(row["entries"])
    if start < 0 or entries < 1:
        raise ValueError(f"{name}: invalid entry range")
    config = load_worker_config(src, cwd)
    config.update(fileList=uri, firstEntry=str(start), lastEntry=str(start + entries),
                  threads="1", rootInputBackend="xrd-adaptor", xrdReadPolicy=policy,
                  xrdReadDiagnostics="1")
    cfg = folder / "worker.yaml"
    write_config_yaml(config, cfg)
    command = [sys.executable, str(Path(__file__).with_name("benchmark_worker.py")), str(cfg),
               "--variant", f"{policy}={worker}", "--events", str(entries),
               "--repeats", "1", "--output", str(folder / "timing"), "--cwd", str(cwd)]
    if "cpu" in row:
        command = ["taskset", "-c", str(int(row["cpu"])), *command]
    subprocess.run(command, check=True)
    manifest = json.loads((folder / "timing/results.json").read_text())
    trial, = manifest["trials"]
    log = Path(trial["output"]) / "worker.log"
    counters = None
    with log.open(errors="replace") as stream:
        for line in stream:
            if "[XrdAdaptor] read-traffic process=1 " in line:
                counters = {k: int(v) for k, v in re.findall(r"(\w+)=(\d+)", line)}
    if not counters or not counters.get("winning_bytes"):
        raise RuntimeError(f"{name}: missing XrdAdaptor payload counters in {log}")
    return dict(name=name, traffic=counters, **trial)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="YAML sequence of workload mappings")
    parser.add_argument("output", type=Path, help="New output directory")
    parser.add_argument("--repeats", type=int, default=2, help="Number of legacy/adaptive pairs (two gives ABBA)")
    args = parser.parse_args()
    path = args.manifest.resolve(strict=True)
    rows = load_yaml_document(path, typed=True)
    if not isinstance(rows, list) or not rows or args.repeats < 1:
        parser.error("provide at least one workload and a positive repeat count")
    names = [r["name"] for r in rows]
    if len(set(names)) != len(names) or any(not re.fullmatch(r"[\w-]+", n) for n in names):
        parser.error("workload names must be unique simple identifiers")
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    result = dict(manifest=str(path), waves=[], physics_checks=[])
    resultfile = output / "comparison.json"
    with ThreadPoolExecutor(max_workers=len(rows)) as pool:
        for i in range(2 * args.repeats):
            policy = ("legacy", "adaptive", "adaptive", "legacy")[i % 4]
            before = link_bytes()
            start = time.monotonic()
            futures = [pool.submit(run_case, row, path.parent, policy,
                                   output / f"{i:02}-{policy}-{row['name']}") for row in rows]
            trials = [f.result() for f in futures]
            wall = time.monotonic() - start
            after = link_bytes()
            result["waves"].append(dict(policy=policy, wall_s=wall, trials=trials,
                                        link_tx_bytes={k: after[k] - v for k, v in before.items()}))
            resultfile.write_text(json.dumps(result, indent=2) + "\n")
    for index, name in enumerate(names):
        reference = Path(result["waves"][0]["trials"][index]["output"])
        for wave in result["waves"][1:]:
            compare(reference, Path(wave["trials"][index]["output"]))
        result["physics_checks"].append(name)
        resultfile.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
