from pathlib import Path
import os
import subprocess
import sys

import pytest

from core.python.xrd_site_rankings import MAGIC, build, read_snapshot
from core.python.submission_backend import generate_condor_runscript


def log(path, *, origin="cluster-a", quality=40, samples=20, errors=0, opened=""):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(f"[XrdAdaptor] context origin={origin} execution_host=worker-a epoch=100000\n"
        f"[XrdAdaptor] site-stat origin={origin} site=t2_example quality_ms={quality} "
        f"samples={samples} read_errors={errors} successful_bytes=2000000 epoch=100000\n"
        # Old and new summaries in the same job are one observation.
        f"[XrdAdaptor] 1 t2_example {quality} yes no {errors} 0 {samples} 2000000\n" + opened)
    return path


def test_ranker_keeps_origins_separate_and_merges_new_profiles(tmp_path):
    a = log(tmp_path / "a/Dataset/condor_logs/log_1_0.stdout")
    # Same job ID at another origin must not be deduplicated.
    b = log(tmp_path / "b/Dataset/condor_logs/log_1_0.stdout", origin="cluster-b", quality=200)
    c = log(tmp_path / "a/Dataset/condor_logs/log_2_0.stdout", quality=60)
    output = tmp_path / "sites.tsv"
    report = build([a, a, c], output, min_jobs=1)
    sites, _, _ = read_snapshot(output)
    assert sites["cluster-a", "t2_example"]["jobs"] == 2
    assert sites["cluster-a", "t2_example"]["quality_ms"] == 50
    build([b], tmp_path / "merged.tsv", base=output, min_jobs=1)
    merged, aliases, _ = read_snapshot(tmp_path / "merged.tsv")
    assert merged["cluster-a", "t2_example"] == sites["cluster-a", "t2_example"]
    assert merged["cluster-b", "t2_example"]["quality_ms"] == 200
    assert report["sites"][0]["samples"] == 40
    assert "worker-a" not in aliases


def test_ranker_ignores_unmeasured_sites_and_loaded_prior_messages(tmp_path):
    a = log(tmp_path / "log.stdout", samples=0)
    a.write_text(a.read_text() + "[XrdAdaptor] site-prior origin=cluster-a site=fake quality_ms=1 weight=1\n")
    output = tmp_path / "sites.tsv"
    build([a], output, min_jobs=1)
    assert read_snapshot(output)[0] == {}


@pytest.mark.parametrize("partial_read_summary", [False, True])
def test_failed_open_uses_storage_endpoint_and_preserves_crashed_jobs(tmp_path, partial_read_summary):
    p = tmp_path / "failed.stdout"
    p.write_text("[XrdAdaptor] context origin=cluster-a execution_host=worker-a epoch=100000\n"
        "[XrdAdaptor] open-result origin=cluster-a site=t2_bad server=bad.example:1094 "
        "endpoint_role=storage status=failed epoch=100000\n"
        "[XrdAdaptor] open-result origin=cluster-a site=dns:redirector server=redirector:1094 "
        "endpoint_role=redirector status=failed epoch=100000\n")
    if partial_read_summary:
        p.write_text(p.read_text() + "[XrdAdaptor] site-stat origin=cluster-a site=t2_bad "
            "quality_ms=50 samples=1 read_errors=0 successful_bytes=32 epoch=100000\n")
    out = tmp_path / "sites.tsv"
    build([p], out, min_jobs=1)
    sites, _, endpoints = read_snapshot(out)
    assert sites["cluster-a", "t2_bad"]["quality_ms"] == 5130
    assert not any("redirector" in key[1] for key in sites)
    assert endpoints["bad.example"] == "t2_bad"


def test_truncated_final_record_does_not_discard_completed_observations(tmp_path):
    source = log(tmp_path / "partial.stdout")
    source.write_text(source.read_text() + "[XrdAdaptor] site-stat site=t2_other quality_ms=")
    output = tmp_path / "sites.tsv"
    report = build([source], output, min_jobs=1)
    assert report["rejected_logs"] == []
    assert read_snapshot(output)[0]["cluster-a", "t2_example"]["quality_ms"] == 40


@pytest.mark.parametrize("site_tag", ["T2_REMOTE_WORKER", ""])
def test_worker_ships_immutable_snapshot_and_uses_worker_origin(tmp_path, monkeypatch, site_tag):
    source = tmp_path / "source.tsv"
    payload = MAGIC + "\nsite cluster-a t2_example 40 5 100000\n"
    source.write_text(payload)
    script = generate_condor_runscript("myexe", False, False, "", site_ranking_file=source, config_file="submit_config.yaml")
    source.unlink()  # The worker cannot depend on submit-host paths.
    fake = tmp_path / "myexe"
    fake.write_text("#!" + sys.executable + "\nimport os, pathlib\n"
        "pathlib.Path('consumed.tsv').write_bytes(pathlib.Path(os.environ['RDF_XRD_SITE_RANKING_FILE']).read_bytes())\n"
        "pathlib.Path('origin.txt').write_text(os.environ['RDF_XRD_SITE_ORIGIN'])\n")
    from submission_backend import _package_worker_yaml
    _package_worker_yaml(tmp_path)
    (tmp_path / "submit_config.yaml").write_text("fileList: local.root\nthreads: 1\n")
    ad = tmp_path / "machine.ad"
    ad.write_text('Machine = "worker-name.privnet"\nName = "slot1@worker-name.privnet"\n')
    env = dict(os.environ, GLIDEIN_CMSSite=site_tag, _CONDOR_MACHINE_AD=str(ad))
    env.pop("RDF_XRD_SITE_ORIGIN", None)
    env.pop("CMS_LOCAL_SITE", None)
    env.pop("OSG_SITE_NAME", None)
    env.pop("SITE_NAME", None)
    subprocess.run(["bash"], input=script, cwd=tmp_path, env=env, text=True, check=True, capture_output=True)
    assert (tmp_path / "consumed.tsv").read_text() == payload
    assert (tmp_path / "origin.txt").read_text() == (site_tag or "worker-name.privnet")


def test_snapshot_validation_rejects_invalid_and_duplicate_rows(tmp_path):
    source = tmp_path / "sites.tsv"
    for body in ["site a b nan 1 100", "site a b 40 -1 100", "site a b 40 1 100\nsite a b 50 1 100"]:
        source.write_text(MAGIC + "\n" + body + "\n")
        with pytest.raises(ValueError):
            read_snapshot(source)
