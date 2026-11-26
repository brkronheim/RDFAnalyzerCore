#!/usr/bin/env python3
"""Time native Analyzer executables on the same staged YAML worker config."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "core/python"))
from native_config_io import load_yaml_document, write_config_yaml


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def phases(log, start, end, cpu):
    markers = []
    with log.open(errors="replace") as stream:
        for line in stream:
            if line.startswith("RDF_BENCHMARK_LOOP "):
                _, event, wall, used = line.split()
                markers.append((event, float(wall), float(used)))
    if [m[0] for m in markers] != ["start", "finish"]:
        raise ValueError(f"expected exactly one ROOT loop for phase accounting in {log}")
    (_, first, first_cpu), (_, last, last_cpu) = markers
    return dict(before_loop_wall_s=first - start, before_loop_cpu_s=first_cpu,
                loop_wall_s=last - first, loop_cpu_s=last_cpu - first_cpu,
                after_loop_wall_s=end - last, after_loop_cpu_s=cpu - last_cpu)


def load_worker_config(source, cwd):
    config = load_yaml_document(source)
    if not isinstance(config, dict):
        raise ValueError(f"{source}: expected a YAML mapping")
    for key, value in config.items():
        if key.endswith("Config") and isinstance(value, str) and not Path(value).is_absolute():
            for base in (cwd, source.parent):
                candidate = base / value
                if candidate.is_file():
                    config[key] = str(candidate)
                    break
    return config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--variant", action="append", required=True,
                        metavar="LABEL=EXECUTABLE", help="Repeat for each build")
    parser.add_argument("--events", type=int, required=True,
                        help="Verified input entries in the configured range, before selection")
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--output", type=Path, required=True, help="New result directory")
    parser.add_argument("--check-products", action="store_true",
                        help="Compare exact single-slot histograms and skims after all timing runs")
    parser.add_argument("--cwd", type=Path, help="Asset bundle directory (default: config parent)")
    parser.add_argument("--phase-probe", type=Path,
                        help="Linux libbenchmarkWorkerPhases.so built with the worker's ROOT")
    args = parser.parse_args()
    if args.events <= 0 or args.repeats <= 0:
        parser.error("--events and --repeats must be positive")
    source = args.config.resolve(strict=True)
    env = os.environ.copy()
    if args.phase_probe:
        probe = args.phase_probe.resolve(strict=True)
        if sys.platform != "linux":
            parser.error("--phase-probe requires Linux")
        env["RDF_BENCHMARK_CHILD_LD_PRELOAD"] = env.get("LD_PRELOAD", "")
        env["LD_PRELOAD"] = os.pathsep.join(filter(None, [str(probe), env.get("LD_PRELOAD")]))
    cwd = args.cwd.resolve(strict=True) if args.cwd else source.parent
    config = load_worker_config(source, cwd)
    variants = {}
    for spec in args.variant:
        label, sep, filename = spec.partition("=")
        if not sep or not re.fullmatch(r"[\w-]+", label) or label in variants:
            parser.error(f"expected a unique LABEL=EXECUTABLE: {spec}")
        path = Path(filename).resolve(strict=True)
        if not path.is_file() or not os.access(path, os.X_OK):
            parser.error(f"worker is not executable: {path}")
        variants[label] = path
    if args.check_products and args.repeats * len(variants) < 2:
        parser.error("--check-products requires at least two trials")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    manifest = dict(config=str(source), config_sha256=digest(source), cwd=str(cwd),
                    events=args.events, executables={
                        k: dict(path=str(v), sha256=digest(v)) for k, v in variants.items()
                    }, trials=[])
    results = output / "results.json"
    for repeat in range(args.repeats):
        order = list(variants)
        if repeat % 2:
            order.reverse()
        for label in order:
            run = output / f"{repeat:02}-{label}"
            run.mkdir()
            settings = dict(config, saveDirectory=str(run), saveFile=str(run / "skim.root"),
                            metaFile=str(run / "meta.root"))
            trial_config = run / "config.yaml"
            write_config_yaml(settings, trial_config)
            with (run / "worker.log").open("wb") as log:
                start = time.monotonic()
                process = subprocess.Popen([str(variants[label]), str(trial_config)],
                                           cwd=cwd, stdout=log, stderr=subprocess.STDOUT, env=env)
                _, status, usage = os.wait4(process.pid, 0)
                end = time.monotonic()
                elapsed = end - start
                process.returncode = os.waitstatus_to_exitcode(status)
            # Linux reports ru_maxrss in KiB; Darwin reports bytes.
            rss = usage.ru_maxrss / (1024 if sys.platform == "darwin" else 1)
            trial = dict(variant=label, repeat=repeat, exit_code=process.returncode,
                         wall_s=elapsed, user_s=usage.ru_utime, system_s=usage.ru_stime,
                         peak_rss_kib=rss, output=str(run))
            if not process.returncode:
                trial["input_events_per_s"] = args.events / elapsed
            manifest["trials"].append(trial)
            results.write_text(json.dumps(manifest, indent=2) + "\n")
            if process.returncode:
                raise SystemExit(f"worker exited {process.returncode}; see {run / 'worker.log'}")
            if args.phase_probe:
                trial.update(phases(run / "worker.log", start, end,
                                    usage.ru_utime + usage.ru_stime))
                results.write_text(json.dumps(manifest, indent=2) + "\n")
    if args.check_products:
        from compare_worker_products import compare
        reference = Path(manifest["trials"][0]["output"])
        for trial in manifest["trials"][1:]:
            compare(reference, Path(trial["output"]))
        manifest["verified_against"] = str(reference)
        results.write_text(json.dumps(manifest, indent=2) + "\n")


if __name__ == "__main__":
    main()
