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


def test_stage_outputs_preblock_contains_condor_proc_handling():
    pre, post = stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt")
    # ensure pre-block reads CONDOR_PROC and will append process id to filenames
    assert "CONDOR_PROC" in pre
    assert "_proc" not in pre  # we don't hardcode a literal suffix, but proc-aware logic exists


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
    assert "CMS_REDIRECTORS" in script
    assert "detect_local_site" in script


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
