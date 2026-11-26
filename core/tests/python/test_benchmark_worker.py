import json
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
import subprocess
import sys

import pytest
import yaml


HARNESS = Path(__file__).resolve().parents[3] / "scripts" / "benchmark_worker.py"


def load_script(path):
    spec = spec_from_file_location(path.stem, path)
    module = module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_worker_replay_isolates_trials_and_preserves_asset_lookup(tmp_path):
    bundle = tmp_path / "bundle"
    bundle.mkdir()
    worker = tmp_path / "worker"
    worker.write_text("#!/bin/sh\nprintf 'worker %s\\n' \"$1\"\n")
    worker.chmod(0o755)
    config = tmp_path / "run.yaml"
    config.write_text("saveConfig: columns.yaml\nsaveFile: original.root\nlabel: 0016\nstream: on\n")
    for path in (tmp_path, bundle):
        (path / "columns.yaml").write_text("- event\n")
    output = tmp_path / "results"
    subprocess.run([sys.executable, str(HARNESS), str(config), "--cwd", str(bundle),
                    "--events", "10", "--repeats", "2", "--output", str(output),
                    "--variant", f"old={worker}", "--variant", f"new={worker}"], check=True)
    trials = json.loads((output / "results.json").read_text())["trials"]
    assert [t["variant"] for t in trials] == ["old", "new", "new", "old"]
    for trial in trials:
        run = Path(trial["output"])
        settings = yaml.safe_load((run / "config.yaml").read_text())
        assert settings["saveConfig"] == str(bundle / "columns.yaml")
        assert settings["saveFile"] == str(run / "skim.root")
        assert settings["label"] == "0016"
        assert settings["stream"] == "on"
        assert trial["input_events_per_s"] == pytest.approx(10 / trial["wall_s"])
        assert trial["peak_rss_kib"] > 0
    assert yaml.safe_load(config.read_text())["saveFile"] == "original.root"


def test_worker_failure_keeps_metrics_and_log(tmp_path):
    source = tmp_path / "run.yaml"
    source.write_text("{}\n")
    worker = tmp_path / "worker"
    worker.write_text("#!/bin/sh\necho failed-run\nexit 3\n")
    worker.chmod(0o755)
    out = tmp_path / "results"
    result = subprocess.run([sys.executable, str(HARNESS), str(source), "--variant",
                             f"bad={worker}", "--events", "1", "--output", str(out)])
    assert result.returncode != 0
    trial, = json.loads((out / "results.json").read_text())["trials"]
    assert trial["exit_code"] == 3
    assert "input_events_per_s" not in trial
    assert "failed-run" in (out / "00-bad" / "worker.log").read_text()


def test_phase_summary_requires_one_measured_loop(tmp_path):
    module = load_script(HARNESS)
    log = tmp_path / "worker.log"
    log.write_text("RDF_BENCHMARK_LOOP start 101.0 0.8\n"
                   "RDF_BENCHMARK_LOOP finish 104.5 4.0\n")
    summary = module.phases(log, 100.0, 105.0, 4.2)
    assert summary["loop_wall_s"] == 3.5
    assert summary["loop_cpu_s"] == pytest.approx(3.2)
    assert summary["after_loop_cpu_s"] == pytest.approx(0.2)
    log.write_text(log.read_text() * 2)
    with pytest.raises(ValueError, match="exactly one ROOT loop"):
        module.phases(log, 100.0, 105.0, 4.2)


def test_skim_parity_detects_entry_reordering(tmp_path):
    uproot = pytest.importorskip("uproot")
    np = pytest.importorskip("numpy")
    compare = load_script(HARNESS.with_name("compare_worker_products.py")).compare
    reference, candidate = tmp_path / "base", tmp_path / "new"
    for folder, entries in ((reference, [1, 2]), (candidate, [2, 1])):
        folder.mkdir()
        with uproot.recreate(folder / "skim.root") as f:
            tree = f.mktree("Events", {"event": "int64"})
            tree.extend({"event": np.array(entries, dtype=np.int64)})
    with pytest.raises(ValueError, match="skim schema, entry order or values"):
        compare(reference, candidate)


def test_sparse_histogram_parity_handles_wide_uniform_axis(tmp_path):
    root = pytest.importorskip("ROOT")
    from array import array
    compare = load_script(HARNESS.with_name("compare_worker_products.py")).compare
    baseline, candidate = tmp_path / "old", tmp_path / "new"
    for folder in (baseline, candidate):
        folder.mkdir()
        with root.TFile(str(folder / "meta.root"), "RECREATE"):
            hist = root.THnSparseF("h", "packed", 1, array("i", [100_000_000]),
                                   array("d", [0]), array("d", [100_000_000]), 1024)
            hist.Fill(array("d", [12.5]), 1.)
            hist.Write()
    compare(baseline, candidate)


def test_xrd_matrix_writes_isolated_policy_and_requires_traffic(tmp_path, monkeypatch):
    monkeypatch.syspath_prepend(str(HARNESS.parent))
    module = load_script(HARNESS.with_name("benchmark_xrd_policies.py"))
    worker = tmp_path / "worker"
    worker.write_text("#!/bin/sh\necho '[XrdAdaptor] read-traffic process=1 winning_bytes=42'\n")
    worker.chmod(0o755)
    source = tmp_path / "run.yaml"
    source.write_text("threads: 8\n")
    row = dict(name="skim", config="run.yaml", worker="worker", first_entry=4,
               entries=7, input="root://fixture//events/dy.root")
    result = module.run_case(row, tmp_path, "adaptive", tmp_path / "first")
    assert result["traffic"]["winning_bytes"] == 42
    cfg = yaml.safe_load((tmp_path / "first/worker.yaml").read_text())
    assert {key: cfg[key] for key in ("threads", "firstEntry", "lastEntry", "xrdReadPolicy")} == {
        "threads": "1", "firstEntry": "4", "lastEntry": "11", "xrdReadPolicy": "adaptive"}
    assert source.read_text() == "threads: 8\n"
    worker.write_text("#!/bin/sh\nexit 0\n")
    with pytest.raises(RuntimeError, match="missing XrdAdaptor payload counters"):
        module.run_case(row, tmp_path, "legacy", tmp_path / "second")
