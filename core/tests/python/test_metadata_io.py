"""Real ROOT round trips, merge arithmetic, lineage and bundle integrity."""
import gzip
import json
import shutil
from pathlib import Path

import pytest

ROOT = pytest.importorskip("ROOT")
pytest.importorskip("uproot")
import uproot
from metadata_io import (JOB_KEY, REFERENCE_KEY, canonical, build_bundle,
                         expand_bundle, load_metadata_records, merge_root_files,
                         reduce_audits, validate_metadata_bundle, common_provenance)


def job(path, identity, *, sample="sample", norm=2., regions=None, audit=None,
        legacy=False, cut_label="selected", revision="abc", semantic_hash=None):
    path = Path(path)
    f = ROOT.TFile.Open(str(path), "RECREATE")
    h = ROOT.TH1D("observable", "observable", 2, 0., 2.)
    h.Sumw2()
    h.Fill(.5, float(identity))
    h.Fill(1.5, -2.)
    h.Write()
    counter = ROOT.TH1D("counter_weightSum_sample", "counter", 1, 0., 1.)
    counter.SetBinContent(1, float(identity))
    counter.Write()
    cutflow = ROOT.TH1D("cutflow", "cutflow", 1, 0., 1.)
    cutflow.GetXaxis().SetBinLabel(1, cut_label)
    cutflow.SetBinContent(1, float(identity))
    cutflow.Write()
    prov = {"job.logical_id": str(identity), "service.counter.sample": sample,
            "framework.git_hash": revision, "config.hash": str(identity)}
    if semantic_hash is not None:
        prov["config.semantic_hash"] = semantic_hash
    document = {"schema_version": 1, "format": "rdf-job-metadata", "provenance": prov,
                "regions": regions or {}, "normalizations": {"weight_norm_total": norm},
                "weight_audits": {"weight_audit_w": audit} if audit is not None else {}}
    if legacy:
        f.mkdir("provenance").cd()
        for key, value in prov.items():
            ROOT.TNamed(key, value).Write()
        f.cd()
        ROOT.TNamed("weight_norm_total", str(norm)).Write()
        if audit is not None:
            h = ROOT.TH1D("weight_audit_w", "audit", 4, -.5, 3.5)
            for i, value in enumerate((audit["sum_w"], 2., 1., 5.), 1):
                h.SetBinContent(i, value)
            h.Write()
            ROOT.TNamed("weight_audit_w_negCount", str(audit["n_negative"])).Write()
            ROOT.TNamed("weight_audit_w_zeroCount", str(audit["n_zero"])).Write()
    else:
        ROOT.TObjString(canonical(document)).Write(JOB_KEY)
    f.Close()
    return path


def audit(n, total, low, high):
    return {"n": n, "sum_w": total, "min_w": low, "max_w": high,
            "n_negative": 0, "n_zero": 0}


def test_new_and_legacy_roundtrip_and_correct_audits(tmp_path):
    paths = [job(tmp_path/"a.root", 1, audit=audit(2, 2., 1., 1.)),
             job(tmp_path/"b.root", 2, audit=audit(3, 12., 3., 5.))]
    original = [r for p in paths for r in load_metadata_records(p)]
    out = tmp_path/"merged.root"
    merge_root_files(out, paths)
    records = validate_metadata_bundle(out)
    assert sorted(records, key=lambda r: r["id"]) == sorted(original, key=lambda r: r["id"])
    stats = reduce_audits(records)["sample"]["weight_audit_w"]
    assert stats == {"n": 5, "sum_w": 14., "min_w": 1., "max_w": 5.,
                     "mean": 2.8, "n_negative": 0, "n_zero": 0}
    assert common_provenance(records) == {"framework.git_hash": "abc", "service.counter.sample": "sample"}
    with uproot.open(out) as f:
        assert f["observable"].values().tolist() == [3., -4.]
        assert f["observable"].variances().tolist() == [5., 8.]
        assert f["counter_weightSum_sample"].values()[0] == 3.
        assert not any(k.startswith(("weight_audit", "provenance")) for k in f.keys())
        assert len([k for k in f.keys() if k.startswith(REFERENCE_KEY)]) == 1


def test_legacy_unknown_mean_is_unavailable(tmp_path):
    paths = [job(tmp_path/f"{i}.root", i, audit=audit(2, 2., 1., 1.), legacy=True)
             for i in (1, 2)]
    merge_root_files(tmp_path/"out.root", paths)
    stats = reduce_audits(load_metadata_records(tmp_path/"out.root"))["sample"]["weight_audit_w"]
    assert stats["sum_w"] == 4.
    assert stats["n"] is stats["mean"] is stats["min_w"] is None
    assert stats["n_negative"] == 0


def test_empty_and_unequal_jobs(tmp_path):
    paths = [job(tmp_path/"empty.root", 1, audit=audit(0, 0., None, None)),
             job(tmp_path/"full.root", 2, audit=audit(3, 12., 3., 5.))]
    records = [r for p in paths for r in load_metadata_records(p)]
    stats = reduce_audits(records)["sample"]["weight_audit_w"]
    assert (stats["n"], stats["mean"], stats["min_w"], stats["max_w"]) == (3, 4., 3., 5.)


@pytest.mark.parametrize("change", [{"norm": 4.}, {"regions": {"A": "pass"}},
                                    {"revision": "other"}, {"cut_label": "different"}])
def test_incompatible_inputs_preserve_old_output(tmp_path, change):
    first = job(tmp_path/"a.root", 1)
    second = job(tmp_path/"b.root", 2, **change)
    out = tmp_path/"out.root"
    out.write_bytes(b"existing artifact")
    with pytest.raises(ValueError, match="Incompatible"):
        merge_root_files(out, [first, second])
    assert out.read_bytes() == b"existing artifact"


def test_sample_scoped_normalization(tmp_path):
    paths = [job(tmp_path/"a.root", 1, sample="a", norm=2.),
             job(tmp_path/"b.root", 2, sample="b", norm=3.)]
    merge_root_files(tmp_path/"out.root", paths)
    records = load_metadata_records(tmp_path/"out.root")
    assert {r["normalizations"]["weight_norm_total"] for r in records} == {2., 3.}


def test_reviewed_semantic_hash_exception_is_scoped_and_lossless(tmp_path):
    paths = [job(tmp_path/"a.root", 1, semantic_hash="original"),
             job(tmp_path/"b.root", 2, semantic_hash="cache32")]
    out = tmp_path/"merged.root"
    policy = {"schema_version": 1, "reason": "Verified cache size change only",
              "samples": {"sample": ["original", "cache32"]}}
    with pytest.raises(ValueError, match="semantic analysis"):
        merge_root_files(out, paths)
    for samples in ({"other": ["original", "cache32"]},
                    {"sample": ["original", "unreviewed"]}):
        with pytest.raises(ValueError, match="semantic analysis"):
            merge_root_files(out, paths, semantic_config_compatibility={**policy, "samples": samples})
    merge_root_files(out, paths, semantic_config_compatibility=policy)
    assert validate_metadata_bundle(out) == expand_bundle(build_bundle(
        [r for p in paths for r in load_metadata_records(p)], semantic_config_compatibility=policy))
    with uproot.open(out) as f:
        ref = json.loads(str(f[REFERENCE_KEY]))
        assert f["observable"].values().tolist() == [3., -4.]
    bundle = json.loads(gzip.decompress((tmp_path/ref["file"]).read_bytes()))
    assert bundle["semantic_config_compatibility"] == policy
    assert {r["provenance"]["config.semantic_hash"] for r in expand_bundle(bundle)} == {"original", "cache32"}
    # Reading an archived approval does not silently authorize a later merge.
    with pytest.raises(ValueError, match="semantic analysis"):
        merge_root_files(tmp_path/"later.root", [out])


@pytest.mark.parametrize("change", [{"norm": 4.}, {"regions": {"A": "pass"}},
                                    {"revision": "other"}, {"cut_label": "different"}])
def test_semantic_exception_retains_other_merge_guards(tmp_path, change):
    paths = [job(tmp_path/"a.root", 1, semantic_hash="original"),
             job(tmp_path/"b.root", 2, semantic_hash="cache32", **change)]
    policy = {"schema_version": 1, "reason": "Cache-only exception",
              "samples": {"sample": ["original", "cache32"]}}
    with pytest.raises(ValueError, match="Incompatible"):
        merge_root_files(tmp_path/"out.root", paths, semantic_config_compatibility=policy)


@pytest.mark.parametrize("policy", [{}, {"schema_version": 2},
    {"schema_version": 1, "reason": "", "samples": {}},
    {"schema_version": 1, "reason": "reviewed", "samples": {"sample": ["a", "a"]}}])
def test_bad_semantic_exception_policy_fails_before_io(tmp_path, policy):
    with pytest.raises(ValueError, match="Invalid semantic config compatibility"):
        merge_root_files(tmp_path/"out.root", [tmp_path/"missing.root"],
                         semantic_config_compatibility=policy)


def test_reviewed_executable_replay_preserves_leaf_and_requires_exact_job(tmp_path):
    inputs = [job(tmp_path / "a.root", 1), job(tmp_path / "b.root", 2)]
    records = [load_metadata_records(path)[0] for path in inputs]
    original, replay = "a" * 64, "b" * 64
    key = "run.expected_executable_sha256"
    records[0]["provenance"][key] = original
    records[1]["provenance"][key] = replay
    rule = {"sample": "sample", "original": original, "replay": replay}
    policy = {"schema_version": 1, "reason": "Validated replacement",
              "samples": {}, "executable_replays": {records[1]["id"]: rule}}
    with pytest.raises(ValueError, match="Incompatible framework"):
        build_bundle(records)
    for replays in ({"different_job": rule}, {records[1]["id"]: {**rule, "sample": "other"}}):
        with pytest.raises(ValueError, match="Incompatible framework"):
            build_bundle(records, semantic_config_compatibility={**policy, "executable_replays": replays})
    merged = build_bundle(records, semantic_config_compatibility=policy)
    assert merged["semantic_config_compatibility"] == policy
    assert {r["provenance"][key] for r in expand_bundle(merged)} == {original, replay}
    records[1]["provenance"]["framework.git_hash"] = "different-build"
    with pytest.raises(ValueError, match="Incompatible framework"):
        build_bundle(records, semantic_config_compatibility=policy)


def test_duplicate_jobs_and_retries_rejected(tmp_path):
    first = job(tmp_path/"a.root", 1)
    retry = job(tmp_path/"retry.root", 1)
    with pytest.raises(ValueError, match="Duplicate logical job"):
        merge_root_files(tmp_path/"out.root", [first, retry])


def test_hierarchical_merging_and_duplicate_leaf(tmp_path):
    paths = [job(tmp_path/f"{i}.root", i, audit=audit(i, i*2., 2., 2.)) for i in (1, 2, 3)]
    group = tmp_path/"group.root"
    merge_root_files(group, paths[:2])
    direct, tree = tmp_path/"direct.root", tmp_path/"tree.root"
    merge_root_files(direct, paths)
    merge_root_files(tree, [group, paths[2]])
    assert build_bundle(load_metadata_records(tree)) == build_bundle(load_metadata_records(direct))
    with uproot.open(tree) as f, uproot.open(direct) as g:
        assert f["observable"].values().tolist() == g["observable"].values().tolist()
    with pytest.raises(ValueError, match="Duplicate logical job"):
        merge_root_files(tmp_path/"duplicate.root", [group, paths[0]])


def test_portable_bundle_and_digest_verification(tmp_path):
    source = tmp_path/"source"; source.mkdir()
    p = job(source/"input.root", 1)
    out = source/"out.root"
    merge_root_files(out, [p])
    target = tmp_path/"moved"
    shutil.copytree(source, target)
    assert len(validate_metadata_bundle(target/"out.root")) == 1
    sidecar = next(target.glob("*.gz"))
    payload = sidecar.read_bytes()
    sidecar.write_bytes(payload+b"bad")
    with pytest.raises(ValueError, match="digest"):
        validate_metadata_bundle(target/"out.root")
    sidecar.unlink()
    with pytest.raises(FileNotFoundError):
        validate_metadata_bundle(target/"out.root")


def test_cutflow_merge_selects_counters_without_main_histograms(tmp_path):
    paths = [job(tmp_path/f"{i}.root", i) for i in (1, 2)]
    merge_root_files(tmp_path/"cutflow.root", paths, role="cutflow")
    with uproot.open(tmp_path/"cutflow.root") as f:
        assert "observable" not in f
        assert f["cutflow"].values().tolist() == [3.]
        assert f["counter_weightSum_sample"].values().tolist() == [3.]


def test_unknown_description_is_archived_and_ambiguous_cycles_fail(tmp_path):
    p = job(tmp_path/"a.root", 1, legacy=True)
    f = ROOT.TFile.Open(str(p), "UPDATE")
    ROOT.TNamed("custom_description", "value").Write()
    f.Close()
    merge_root_files(tmp_path/"out.root", [p])
    r = load_metadata_records(tmp_path/"out.root")[0]
    assert r["objects"]["custom_description"]["value"] == "value"
    f = ROOT.TFile.Open(str(p), "UPDATE")
    ROOT.TNamed("custom_description", "conflict").Write()
    f.Close()
    with pytest.raises(ValueError, match="Conflicting legacy key cycles"):
        merge_root_files(tmp_path/"bad.root", [p])


def test_failed_postprocess_preserves_previous_bundle(tmp_path):
    p = job(tmp_path/"a.root", 1)
    out = tmp_path/"out.root"
    merge_root_files(out, [p])
    original = out.read_bytes()
    def fail(_):
        raise RuntimeError("postprocessing failed")
    with pytest.raises(RuntimeError):
        merge_root_files(out, [p], postprocess=fail)
    assert out.read_bytes() == original
    assert len(validate_metadata_bundle(out)) == 1


def test_bad_versions_and_audit_counts_fail(tmp_path):
    p = job(tmp_path/"a.root", 1, audit=audit(2, 2., 1., 1.))
    f = ROOT.TFile.Open(str(p), "UPDATE")
    ROOT.TObjString('{"schema_version":999,"format":"rdf-job-metadata"}').Write(JOB_KEY, ROOT.TObject.kOverwrite)
    f.Close()
    with pytest.raises(ValueError, match="Unsupported"):
        load_metadata_records(p)
    with pytest.raises(ValueError, match="count"):
        job(tmp_path/"b.root", 2, audit={**audit(2, 2., 1., 1.), "n_negative": -1})
        load_metadata_records(tmp_path/"b.root")


def test_worker_validator_rejects_bad_compact_metadata(tmp_path):
    import analysis_tasks
    p = job(tmp_path/"job.root", 1)
    assert analysis_tasks._root_file_tree_error(str(p), None) is None
    f = ROOT.TFile.Open(str(p), "UPDATE")
    ROOT.TObjString('{"schema_version":999}').Write(JOB_KEY, ROOT.TObject.kOverwrite)
    f.Close()
    assert "invalid compact metadata" in analysis_tasks._root_file_tree_error(str(p), None)


def test_missing_completion_and_stale_reference_invalidate_bundle(tmp_path):
    p = job(tmp_path/"job.root", 1)
    out = tmp_path/"out.root"
    merge_root_files(out, [p])
    marker = Path(str(out)+".merge.json")
    saved = marker.read_text()
    marker.unlink()
    with pytest.raises(FileNotFoundError, match="completion"):
        validate_metadata_bundle(out)
    data = json.loads(saved)
    data["metadata"]["sha256"] = "bad"
    marker.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="Stale"):
        validate_metadata_bundle(out)


def test_reproducibility_cli_keeps_all_job_records(tmp_path):
    from reproducibility_report import main, ReproducibilityReport
    paths = [job(tmp_path/f"{i}.root", i) for i in (1, 2)]
    out, report = tmp_path/"out.root", tmp_path/"report.json"
    merge_root_files(out, paths)
    assert main([str(out), "--json", str(report), "--quiet"]) == 0
    restored = ReproducibilityReport.load_json(str(report))
    assert len(restored.job_records) == 2
    assert "config.hash" not in restored.provenance
    assert "2 (shared provenance" in restored.to_text()


def sparse_job(path, identity, physical_bins=2):
    from array import array
    p = job(path, identity)
    f = ROOT.TFile.Open(str(p), "UPDATE")
    roles = ["channel", "control", "sample", "systematic", "observable"]
    labels = ["signal", "default", "sample", "Nominal"]
    h = ROOT.THnSparseF("sparse", "sparse", 5, array('i', [1,1,1,1,physical_bins]),
                       array('d', [0.]*5), array('d', [1.,1.,1.,1.,float(physical_bins)]), 16 * 1024)
    h.Sumw2()
    for i, label in enumerate(labels):
        h.GetAxis(i).SetBinLabel(1, label)
    h.Fill(array('d', [.5]*5), float(identity))
    f.mkdir("nd_histograms_v1").cd()
    h.Write()
    schema = {"schema_version": 1, "directory": "nd_histograms_v1", "histograms": [
        {"key": "sparse", "name": "sparse", "variable": "x", "label": "x", "axes": [
            {"role": role, "bins": 1 if i < 4 else 2,
             "labels": [labels[i]] if i < 4 else [],
             "edges": [0.,1.] if i < 4 else [0.,1.,2.]} for i,role in enumerate(roles)]}]}
    f.cd()
    ROOT.TObjString(canonical(schema)).Write("__rdf_histogram_schema_v1")
    f.Close()
    return p


def test_sparse_schema_kept_once_and_physical_axis_conflict_rejected(tmp_path):
    from extract_nd_histograms import extract_nd_histograms
    first, second = sparse_job(tmp_path/"a.root", 1), sparse_job(tmp_path/"b.root", 2)
    out = tmp_path/"out.root"
    merge_root_files(out, [first, second], postprocess=lambda p: extract_nd_histograms(p, p))
    validate_metadata_bundle(out)
    with uproot.open(out) as f:
        assert len([k for k in f.keys() if k.startswith("__rdf_histogram_schema_v1;")]) == 1
        exported = [f[k] for k, cls in f.classnames(cycle=False).items() if cls == "TH1F"]
        assert len(exported) == 1
        assert exported[0].values().sum() == 3.
        assert exported[0].variances().sum() == pytest.approx(5., rel=1e-14)
    incompatible = sparse_job(tmp_path/"c.root", 3, physical_bins=3)
    with pytest.raises(ValueError, match="Physical sparse axes"):
        merge_root_files(tmp_path/"bad.root", [first, incompatible])


def test_law_real_merge_checks_sidecars_and_selects_physical_roles(tmp_path):
    pytest.importorskip("law")
    from output_schema import OutputManifest, HistogramSchema, CutflowSchema, MetadataSchema
    from merge_tasks import MergeHistograms, MergeCutflows
    for i in (1, 2):
        directory = tmp_path/f"job{i}"
        directory.mkdir()
        job(directory/"meta.root", i)
        OutputManifest(histograms=HistogramSchema(output_file="meta.root", histogram_names=["observable"]),
                       cutflow=CutflowSchema(output_file="meta.root"),
                       metadata=MetadataSchema(output_file="meta.root")).save_yaml(str(directory/"output_manifest.yaml"))
    task = MergeHistograms(name="metadata_integration_hist", input_dir=str(tmp_path),
                           output_dir=str(tmp_path/"merged"))
    task.run()
    assert task.complete()
    cf = MergeCutflows(name="metadata_integration_cf", input_dir=str(tmp_path),
                      output_dir=str(tmp_path/"cutflows"))
    # Restrict discovery to the original jobs rather than the histogram output
    # manifest that was deliberately written inside the temporary input tree.
    cf.manifest_glob = "job*/output_manifest.yaml"
    cf.run()
    manifest = OutputManifest.load_yaml(cf.output().path)
    with uproot.open(manifest.cutflow.output_file) as f:
        assert "observable" not in f
        assert f["counter_weightSum_sample"].values().tolist() == [3.]
    output_manifest = OutputManifest.load_yaml(task.output().path)
    root = Path(output_manifest.histograms.output_file)
    with uproot.open(root) as f:
        ref = json.loads(str(f[REFERENCE_KEY]))
    (root.parent/ref["file"]).unlink()
    assert not task.complete()
