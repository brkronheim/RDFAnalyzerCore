import os
from core.python.submission_backend import generate_condor_submit, generate_condor_runscript, stage_outputs_blocks


def test_generate_condor_submit_exports_condor_proc_env(tmp_path):
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=2,
        exe_relpath="bin/fakeexe",
        use_shared_inputs=False,
        shared_dir_name=None,
        config_file="submit_config.txt",
    )
    assert 'environment = "CONDOR_PROC=$(Process)' in sub
    assert "CONDOR_CLUSTER=$(Cluster)" in sub


def test_generate_condor_submit_sets_singularity_image(tmp_path):
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=1,
        exe_relpath="bin/fakeexe",
        config_file="submit_config.txt",
        container_image="/cvmfs/cms.cern.ch/common/cmssw-el9",
    )
    assert 'MY.SingularityImage = "/cvmfs/cms.cern.ch/common/cmssw-el9"' in sub


def test_generate_condor_submit_sets_want_os(tmp_path):
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=1,
        exe_relpath="bin/fakeexe",
        config_file="submit_config.txt",
        want_os="el8",
    )
    assert 'MY.WantOS = "el8"' in sub


def test_generate_condor_submit_sets_x86_v2_requirements(tmp_path):
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=1,
        exe_relpath="bin/fakeexe",
        config_file="submit_config.txt",
    )
    assert 'requirements = (TARGET.Arch =?= "X86_64")' in sub
    assert 'TARGET.Microarch =!= UNDEFINED' in sub
    assert 'TARGET.Has_sse4_1 =?= True' in sub


def test_stage_outputs_preblock_contains_condor_proc_handling():
    pre, post = stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt")
    # ensure pre-block reads CONDOR_PROC and will append process id to filenames
    assert "CONDOR_PROC" in pre
    assert "_proc" not in pre  # we don't hardcode a literal suffix, but proc-aware logic exists
    assert "check_output_endpoint(cfg.get(\"__orig_saveFile\", \"\"))" in pre
    assert "check_output_endpoint(cfg.get(\"__orig_metaFile\", \"\"))" in pre
    assert "ensure_output_endpoint_ready" in post
    assert "verify_remote_file" in post
    assert 'xrdfs_ok(host_url, "stat", eos_dir)' in post
    assert 'xrdfs_ok(host_url, "stat", remote_path)' in post
    assert 'XRDCP_TIMEOUT_FLOOR' in post


def test_generate_condor_submit_shared_dir_uses_directory_entry(tmp_path):
    """When shared_dir_name is set the whole directory should appear in
    transfer_input_files, not individual file paths inside it."""
    shared = tmp_path / "shared_inputs"
    shared.mkdir()
    (shared / "myexe").write_text("fake exe")

    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=3,
        exe_relpath="myexe",
        shared_dir_name="shared_inputs",
        config_file="submit_config.txt",
    )
    expected_dir = str(tmp_path / "shared_inputs")
    assert expected_dir in sub
    # Individual file paths inside shared_inputs must NOT be listed separately
    assert str(tmp_path / "shared_inputs" / "myexe") not in sub


def test_generate_condor_runscript_shared_block_copies_config_files(tmp_path):
    """The generated runscript shared_block must include a loop that copies
    regular files from shared_inputs/ to the working directory."""
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
        shared_dir_name="shared_inputs",
    )
    # Generic copy loop must be present
    assert "for _shared_f in shared_inputs/*" in script
    assert "cp -n" in script


def test_generate_condor_runscript_shared_archive_materializes_cfg_runtime():
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
        x509loc="x509",
        shared_archive_name="shared_inputs.tar.gz",
        runtime_config_relpath="cfg/submit_config.txt",
    )
    assert 'tar -xzf "shared_inputs.tar.gz"' in script
    assert 'cp -f submit_config.txt cfg/submit_config.txt' in script
    assert 'chmod +x "myexe"' in script
    assert 'export LD_LIBRARY_PATH="$PWD:${LD_LIBRARY_PATH:-}"' in script
    assert './myexe cfg/submit_config.txt' in script
    assert 'export X509_USER_PROXY=x509' in script


def test_runscript_uses_preserved_host_python_for_helpers():
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
    )
    assert 'RDF_HELPER_PYTHON="$(command -v python3 || command -v python || true)"' in script
    assert 'run_helper_python() {' in script
    assert 'env -u PYTHONHOME LD_LIBRARY_PATH="${RDF_ORIG_LD_LIBRARY_PATH:-}" "$helper" "$@"' in script
    assert "run_helper_python - << 'XRDPY'" in script


def test_runscript_relaxes_ulimit_commands_in_root_setup():
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="ulimit -n 10000\nexport FOO=bar\n",
    )
    assert "(ulimit -n 10000) || true" in script
    assert "export FOO=bar" in script


def test_runscript_includes_xrootd_optimize_when_no_stage_in():
    """When stage_inputs=False, the runscript must include the XRootD
    optimisation block that probes and selects the fastest redirector."""
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
    )
    assert "Optimizing XRootD redirectors" in script
    assert "xrd-opt" in script
    assert "SITE_REDIRECTOR_MARKER" in script
    assert "detect_local_site" in script
    # Must use ROOT macro, not pyxrootd
    assert "probe_via_root_macro" in script
    assert "probe_via_pyxrootd" not in script
    assert "probe_via_subprocess" not in script


def test_runscript_excludes_xrootd_optimize_when_stage_in():
    """When stage_inputs=True, files are staged locally so no XRootD
    optimisation is needed and the block must be absent."""
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=True,
        stage_outputs=False,
        root_setup="",
    )
    assert "Optimizing XRootD redirectors" not in script
    # Stage-in block must be present instead
    assert "Staging input files" in script


def test_stage_in_uses_site_selection():
    """The stage-in block must probe redirectors in parallel and pick the best."""
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "rank_redirectors_parallel" in block
    assert "filter_site_redirectors" in block
    assert "ThreadPoolExecutor" in block


def test_stage_in_probe_bytes_is_1mb():
    """Stage-in probing should use 1 MiB (not tiny 32 KiB)."""
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "1 * 1024 * 1024" in block


def test_stage_in_uses_60s_timeout():
    """Each xrdcp copy in stage-in must use a 60-second timeout."""
    from core.python.submission_backend import stage_inputs_block
    import re
    block = stage_inputs_block()
    m = re.search(r"STAGE_COPY_TIMEOUT\s*=\s*(\d+)", block)
    assert m is not None, "STAGE_COPY_TIMEOUT not found in stage_inputs_block"
    assert int(m.group(1)) == 60


def test_stage_in_caps_retries_at_3():
    """Stage-in must cap total attempts at 3 per file."""
    from core.python.submission_backend import stage_inputs_block
    import re
    block = stage_inputs_block()
    m = re.search(r"MAX_ATTEMPTS\s*=\s*(\d+)", block)
    assert m is not None, "MAX_ATTEMPTS not found in stage_inputs_block"
    assert int(m.group(1)) == 3


def test_stage_in_uses_only_site_specific_redirectors():
    """Stage-in must rank and retry only site-specific redirectors."""
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "SITE_REDIRECTOR_MARKER" in block
    assert "GLOBAL_REDIRECTOR" not in block
    assert "build_url(extract_lfn(url), GLOBAL_REDIRECTOR)" not in block


def test_stage_in_uses_root_macro():
    """Stage-in probing must use the ROOT macro (not pyxrootd)."""
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "probe_via_root_macro" in block
    assert "probe_via_pyxrootd" not in block


def test_runscript_xrootd_optimize_uses_correct_config_file():
    """The XRootD optimisation block must reference the custom config file."""
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
        config_file="cfg/my_config.txt",
    )
    assert "cfg/my_config.txt" in script


def test_runscript_xrootd_optimize_uses_runtime_config_relpath():
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="",
        config_file="submit_config.txt",
        runtime_config_relpath="cfg/submit_config.txt",
    )
    assert 'cfg_path = "cfg/submit_config.txt"' in script
    assert './myexe cfg/submit_config.txt' in script


def test_xrootd_optimize_block_includes_blacklist():
    """Blacklisted sites passed to xrootd_optimize_block must appear in the
    generated script so the worker node can skip them."""
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block(blacklisted_sites=["T2_Bad_Site", "bad-host.cern.ch"])
    assert "T2_Bad_Site" in block
    assert "bad-host.cern.ch" in block


def test_xrootd_optimize_block_uses_short_timeout():
    """The probe timeout in the worker-side script must be <= 10 s."""
    from core.python.submission_backend import xrootd_optimize_block
    import re
    block = xrootd_optimize_block()
    m = re.search(r"PROBE_TIMEOUT\s*=\s*(\d+(?:\.\d+)?)", block)
    assert m is not None, "PROBE_TIMEOUT not found in generated block"
    assert float(m.group(1)) <= 10.0, f"PROBE_TIMEOUT too large: {m.group(1)}"


def test_xrootd_optimize_block_reads_runtime_blacklist():
    """The generated script must read the xrdBlacklist key from the config."""
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block()
    assert "xrdBlacklist" in block


def test_xrootd_optimize_block_filters_site_specific_redirectors():
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block()
    assert "filter_site_redirectors" in block
    assert "SITE_REDIRECTOR_MARKER" in block


def test_stage_in_block_filters_site_specific_redirectors():
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "filter_site_redirectors" in block
    assert "SITE_REDIRECTOR_MARKER" in block


def test_stage_in_block_accepts_direct_pfn_candidates():
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert "is_direct_xrootd_candidate" in block
    assert "GENERIC_REDIRECTOR_HOSTS" in block
    assert "candidate_url" in block


def test_stage_in_block_fails_job_when_any_redirector_selection_fails():
    from core.python.submission_backend import stage_inputs_block
    block = stage_inputs_block()
    assert 'redirector_failures = []' in block
    assert 'sys.exit(42)' in block
    assert 'raise RuntimeError(f"all probes failed for {lfn}")' in block
    assert 'using original URL' not in block


def test_xrootd_optimize_block_uses_root_probe_only():
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block()
    assert "probe_via_subprocess" not in block
    assert "probe_via_root_macro" in block


def test_xrootd_optimize_block_accepts_direct_pfn_candidates():
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block()
    assert "is_direct_xrootd_candidate" in block
    assert "GENERIC_REDIRECTOR_HOSTS" in block
    assert "candidate_url" in block


def test_xrootd_optimize_block_fails_job_when_any_redirector_selection_fails():
    from core.python.submission_backend import xrootd_optimize_block
    block = xrootd_optimize_block()
    assert 'redirector_failures = []' in block
    assert 'sys.exit(42)' in block
    assert 'raise RuntimeError(f"all probes failed for {lfn}")' in block
