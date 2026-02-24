import os
from core.python.submission_backend import generate_condor_submit, stage_outputs_blocks


def test_generate_condor_submit_exports_condor_proc_env(tmp_path):
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=2,
        exe_relpath="bin/fakeexe",
        use_shared_inputs=False,
        shared_dir_name=None,
        config_file="submit_config.txt",
    )
    assert "environment = CONDOR_PROC=$(Process)" in sub
    assert "CONDOR_CLUSTER=$(Cluster)" in sub


def test_stage_outputs_preblock_contains_condor_proc_handling():
    pre, post = stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt")
    # ensure pre-block reads CONDOR_PROC and will append process id to filenames
    assert "CONDOR_PROC" in pre
    assert "_proc" not in pre  # we don't hardcode a literal suffix, but proc-aware logic exists
