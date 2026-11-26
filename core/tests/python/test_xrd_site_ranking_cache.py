from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path

from core.python.xrd_site_rankings import (
    MAGIC, read_snapshot, submission_ranking_source, update_after_run,
)
from core.python.submission_backend import generate_condor_runscript


def returned_logs(run, cluster, qualities, origin="cluster-a"):
    directory = run / "condor_submissions/sample/condor_logs"
    directory.mkdir(parents=True)
    for index, quality in enumerate(qualities):
        (directory / f"log_{cluster}_{index}.stdout").write_text(
            f"[XrdAdaptor] context origin={origin} execution_host=worker-{origin} epoch=100000\n"
            f"[XrdAdaptor] site-stat origin={origin} site=storage quality_ms={quality} "
            "samples=20 read_errors=0 successful_bytes=2000000 epoch=100000\n")


def setup_cache(tmp_path, monkeypatch):
    cache = tmp_path / "cache/sites.tsv"
    monkeypatch.setenv("RDF_XRD_SITE_RANKING_CACHE", str(cache))
    monkeypatch.delenv("RDF_XRD_SITE_RANKING_FILE", raising=False)
    monkeypatch.delenv("RDF_XRD_SITE_RANKING_AUTO_UPDATE", raising=False)
    return cache


def test_small_completed_runs_accumulate_and_replay_is_idempotent(tmp_path, monkeypatch):
    cache = setup_cache(tmp_path, monkeypatch)
    a, b = tmp_path / "run-a", tmp_path / "run-b"
    returned_logs(a, 100, [40, 50])
    returned_logs(b, 200, [60, 70, 80])
    first = update_after_run(a)
    assert first["status"] == "updated"
    assert first["site_groups"] == 1
    assert first["site_rows_changed"] == 0
    assert first["minimum_jobs_per_site"] == 5
    assert read_snapshot(cache)[0] == {}  # Two jobs do not meet five-job support.
    result = update_after_run(b)
    sites, _, _ = read_snapshot(cache)
    assert result["completed_runs"] == 2
    assert result["site_rows_changed"] == 1
    assert sites["cluster-a", "storage"]["quality_ms"] == 60
    assert sites["cluster-a", "storage"]["jobs"] == 5
    before = cache.read_bytes(), cache.stat().st_mtime_ns
    assert update_after_run(b)["status"] == "unchanged"
    assert before == (cache.read_bytes(), cache.stat().st_mtime_ns)
    assert len(json.loads(cache.with_suffix(".tsv.runs.json").read_text())["runs"]) == 2


def test_completed_origins_merge_and_future_workers_ship_learned_cache(tmp_path, monkeypatch):
    cache = setup_cache(tmp_path, monkeypatch)
    seed = tmp_path / "seed.tsv"
    seed.write_text(MAGIC + "\nsite cluster-c old-storage 100 5 100000\n")
    a, b = tmp_path / "run-a", tmp_path / "run-b"
    returned_logs(a, 100, [40] * 5)
    returned_logs(b, 200, [80] * 5, "cluster-b")
    # Separate monitors must not lose each other's origin profiles.
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda run: update_after_run(run, seed=seed), [a, b]))
    sites, _, _ = read_snapshot(cache)
    assert all(result["status"] == "updated" for result in results)
    assert set(sites) == {("cluster-a", "storage"), ("cluster-b", "storage"), ("cluster-c", "old-storage")}
    assert submission_ranking_source(seed) == cache
    # Decode the generated worker's immutable payload, rather than only testing
    # source precedence. It must include newly learned remote origins.
    script = generate_condor_runscript("analysis", False, False, "")
    import ast
    import base64
    import re
    encoded = ast.literal_eval(re.search(r"payload = base64.b64decode\((.*)\)", script)[1])
    assert base64.b64decode(encoded) == cache.read_bytes()
    monkeypatch.setenv("RDF_XRD_SITE_RANKING_FILE", str(seed))
    assert submission_ranking_source(cache) == str(seed)  # Explicit pin remains authoritative.


def test_non_xrd_runs_and_opt_out_do_not_create_cache(tmp_path, monkeypatch):
    cache = setup_cache(tmp_path, monkeypatch)
    run = tmp_path / "run"
    run.mkdir()
    assert update_after_run(run)["status"] == "skipped"
    assert not cache.parent.exists()
    returned_logs(run, 100, [40] * 5)
    monkeypatch.setenv("RDF_XRD_SITE_RANKING_AUTO_UPDATE", "0")
    assert update_after_run(run)["status"] == "disabled"
    assert not cache.parent.exists()


def test_explicit_pin_at_cache_path_is_immutable(tmp_path, monkeypatch):
    cache = setup_cache(tmp_path, monkeypatch)
    cache.parent.mkdir()
    original = MAGIC + "\nsite cluster-a storage 80 5 100000\n"
    cache.write_text(original)
    alias = tmp_path / "pinned.tsv"
    alias.symlink_to(cache)
    monkeypatch.setenv("RDF_XRD_SITE_RANKING_FILE", str(alias))
    run = tmp_path / "run"
    returned_logs(run, 100, [40] * 5)
    assert submission_ranking_source() == str(alias)
    assert update_after_run(run)["status"] == "disabled"
    assert cache.read_text() == original
    assert not cache.with_suffix(".tsv.runs.json").exists()
