import os

import pytest
from core.python.submission_backend import ensure_xrootd_redirector, generate_condor_submit, generate_condor_runscript, get_copy_file_list, stage_outputs_blocks


def test_get_copy_file_list_includes_onnx_payloads():
    assert get_copy_file_list({"model": "cfg/model_even_train.onnx"}) == [
        "cfg/model_even_train.onnx"
    ]


def test_ensure_xrootd_redirector_preserves_absolute_local_paths():
    local_path = "/tmp/rdfanalyzer/local_input.root"
    assert ensure_xrootd_redirector(local_path) == local_path


def test_ensure_xrootd_redirector_rewrites_eos_style_paths():
    eos_path = "/store/mc/Run3/file.root"
    assert ensure_xrootd_redirector(eos_path) == "root://eospublic.cern.ch//store/mc/Run3/file.root"


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


def test_generate_condor_submit_sets_x86_v2_requirements(tmp_path, monkeypatch):
    monkeypatch.delenv("RDF_CONDOR_EXTRA_BLOCKED_HOSTS", raising=False)
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=1,
        exe_relpath="bin/fakeexe",
        config_file="submit_config.txt",
    )
    assert 'requirements =' in sub
    assert '(TARGET.Arch =?= "X86_64")' in sub
    assert 'TARGET.Microarch =!= UNDEFINED' in sub
    assert 'TARGET.Has_sse4_1 =?= True' in sub
    assert 'TARGET.Machine != "hepcms-henrietta.privnet"' in sub
    assert 'TARGET.Machine != "hepcms-namenode.privnet"' in sub

    monkeypatch.setenv("RDF_CONDOR_EXTRA_BLOCKED_HOSTS", "")
    sub = generate_condor_submit(
        main_dir=str(tmp_path),
        jobs=1,
        exe_relpath="bin/fakeexe",
        config_file="submit_config.txt",
    )
    assert 'TARGET.Machine != "hepcms-henrietta.privnet"' in sub
    assert 'TARGET.Machine != "hepcms-namenode.privnet"' not in sub


def test_stage_outputs_preblock_contains_condor_proc_handling():
    pre, post = stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt")
    # ensure pre-block reads CONDOR_PROC and will append process id to filenames
    assert "CONDOR_PROC" in pre
    assert "_proc" not in pre  # we don't hardcode a literal suffix, but proc-aware logic exists
    # Remote EOS checks are deferred until stage-out so a slow endpoint does
    # not prevent the analysis executable from running.
    assert "check_output_endpoint" not in pre
    assert "xrdfs" not in pre
    assert "ensure_output_endpoint_ready" in post
    assert "verify_remote_file" in post
    assert 'xrdfs_ok(host_url, "mkdir", "-p", eos_dir)' in post
    assert 'xrdfs_ok(host_url, "stat", eos_dir)' not in post
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
    assert "run_helper_python - << 'RDFINPUT'" in script


def test_runscript_relaxes_ulimit_commands_in_root_setup():
    script = generate_condor_runscript(
        exe_relpath="myexe",
        stage_inputs=False,
        stage_outputs=False,
        root_setup="ulimit -n 10000\nexport FOO=bar\n",
    )
    assert "(ulimit -n 10000) || true" in script
    assert "export FOO=bar" in script


def _run_input_block(tmp_path, monkeypatch, stage_in=False, yaml_config=False, failures=0):
    import subprocess
    import yaml
    from core.python import submission_backend as backend
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(backend._io_policy, "GLOBAL_REDIRECTOR", "root://cms-xrd-global.cern.ch")
    monkeypatch.setattr(backend._io_policy, "PUBLIC_STORAGE_PREFIXES", ("/store/mc/", "/store/data/"))
    monkeypatch.setattr(backend._io_policy, "SITE_REDIRECTOR_MARKER", "/store/test/xrootd/")
    path = tmp_path / ("input.yaml" if yaml_config else "input.txt")
    values = ["root://site//store/test/xrootd/T2_Site/store/mc/a.root?tried=bad&token=keep",
              "root://old//store/data/b.root", "root://private//store/user/me/c.root",
              "/store/mc/d.root", "/tmp/local.root", "https://other/file.root"]
    cfg = dict(fileList=values if yaml_config else ','.join(values),
               enableSystematics=True if yaml_config else 'true', saveFile='keep.root')
    if yaml_config:
        path.write_text(yaml.safe_dump(cfg))
    else:
        path.write_text(''.join('{}={}\n'.format(k, v) for k, v in cfg.items()))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        assert command[0] == 'xrdcp'
        if len(calls) <= failures:
            raise subprocess.CalledProcessError(1, command)
        return subprocess.CompletedProcess(command, 0)
    monkeypatch.setattr(subprocess, 'run', run)
    block = backend.stage_inputs_block(config_file=str(path)) if stage_in else backend.xrootd_optimize_block(str(path))
    code = block.split("<< 'RDFINPUT'\n", 1)[1].rsplit('\nRDFINPUT', 1)[0]
    exec(compile(code, '<worker-input>', 'exec'), {})
    actual = backend.read_config(str(path))
    return actual, calls


@pytest.mark.parametrize('yaml_config', [False, True])
def test_worker_routes_public_inputs_globally_without_probes(tmp_path, monkeypatch, yaml_config):
    actual, calls = _run_input_block(tmp_path, monkeypatch, yaml_config=yaml_config)
    assert calls == []
    assert actual['fileList'].split(',') == [
        'root://cms-xrd-global.cern.ch//store/mc/a.root?token=keep',
        'root://cms-xrd-global.cern.ch//store/data/b.root',
        'root://private//store/user/me/c.root',
        'root://cms-xrd-global.cern.ch//store/mc/d.root',
        '/tmp/local.root', 'https://other/file.root']
    assert actual['enableSystematics'] == 'true'
    assert actual['saveFile'] == 'keep.root'


def test_stage_in_retries_global_endpoint_without_probing(tmp_path, monkeypatch):
    actual, calls = _run_input_block(tmp_path, monkeypatch, stage_in=True, failures=2)
    assert actual['fileList'] == ','.join('input_{}.root'.format(i) for i in range(6))
    assert len(calls) == 8
    assert all(cmd[-2] == 'root://cms-xrd-global.cern.ch//store/mc/a.root?token=keep' for cmd in calls[:3])
    assert actual['saveFile'] == 'keep.root'


def test_failed_stage_in_preserves_original_input_config(tmp_path, monkeypatch):
    import subprocess
    with pytest.raises(subprocess.CalledProcessError):
        _run_input_block(tmp_path, monkeypatch, stage_in=True, failures=100)
    assert 'input_0.root' not in (tmp_path / 'input.txt').read_text()


@pytest.mark.parametrize('stage_in', [False, True])
def test_generated_workers_have_no_redirector_probe_processes(stage_in):
    import subprocess
    script = generate_condor_runscript('myexe', stage_in, False, '', runtime_config_relpath='cfg/custom.txt')
    assert 'probe_via_root_macro' not in script
    assert 'ThreadPoolExecutor' not in script
    assert 'site_redirectors.json' not in script
    assert './myexe cfg/custom.txt' in script
    subprocess.run(['bash', '-n'], input=script, text=True, check=True)


@pytest.mark.parametrize('cpus,site_cpus', [(1, None), (1, 1), (2, None), (2, 2), (2, 1)])
def test_worker_preserves_inherited_affinity_and_limits_thread_pools(tmp_path, cpus, site_cpus):
    import json
    import subprocess
    import sys
    if len(os.sched_getaffinity(0)) < cpus:
        pytest.skip('Insufficient CPUs in the test allocation')
    exe = tmp_path / 'myexe'
    exe.write_text(
        '#!' + sys.executable + '\n'
        'import json, os, pathlib, subprocess, sys, threading\n'
        'seen = []\n'
        't = threading.Thread(target=lambda: seen.extend(os.sched_getaffinity(0)))\n'
        't.start(); t.join()\n'
        'child = subprocess.check_output([sys.executable, "-c", '
        '"import os, json; print(json.dumps(sorted(os.sched_getaffinity(0))))"], text=True)\n'
        'pathlib.Path("cpu.json").write_text(json.dumps(dict('
        'affinity=sorted(os.sched_getaffinity(0)), thread=sorted(seen), child=json.loads(child), '
        'omp=os.environ["OMP_NUM_THREADS"], blas=os.environ["OPENBLAS_NUM_THREADS"])))\n')
    (tmp_path / 'submit_config.txt').write_text('fileList=local.root\nthreads=1\n')
    script = generate_condor_runscript('myexe', False, False, '', request_cpus=cpus)
    # Setup/staging must retain the same inherited placement as the analysis.
    script = script.replace('stage_in_start=$(date +%s)',
        "run_helper_python -c 'import os; print(len(os.sched_getaffinity(0)))' > setup_cpus.txt\n"
        'stage_in_start=$(date +%s)')
    env = dict(os.environ, OPENBLAS_NUM_THREADS='64', OMP_NUM_THREADS='64')
    site_affinity = set(sorted(os.sched_getaffinity(0))[:site_cpus])
    subprocess.run(['bash'], input=script, cwd=tmp_path, env=env,
                   text=True, check=True, capture_output=True,
                   preexec_fn=(lambda: os.sched_setaffinity(0, site_affinity)) if site_cpus else None)
    result = json.loads((tmp_path / 'cpu.json').read_text())
    expected = sorted(site_affinity if site_cpus else os.sched_getaffinity(0))
    assert result['affinity'] == expected
    assert result['thread'] == result['affinity']
    assert result['child'] == expected
    assert result['omp'] == result['blas'] == str(cpus)
    assert int((tmp_path / 'setup_cpus.txt').read_text()) == len(expected)


def test_worker_cpu_launcher_preserves_analysis_failure(tmp_path):
    import subprocess
    (tmp_path / 'myexe').write_text('#!/bin/sh\nexit 23\n')
    (tmp_path / 'submit_config.txt').write_text('fileList=local.root\n')
    script = generate_condor_runscript('myexe', False, False, '')
    result = subprocess.run(['bash'], input=script, cwd=tmp_path,
                            text=True, capture_output=True)
    assert result.returncode == 23


@pytest.mark.parametrize('stage_in', [False, True])
def test_worker_executes_with_materialized_runtime_config(tmp_path, monkeypatch, stage_in):
    import subprocess
    import sys
    from pathlib import Path
    monkeypatch.chdir(tmp_path)
    # The fake analysis consumes the actual config passed by the complete worker.
    fake = tmp_path / 'myexe'
    fake.write_text('#!' + sys.executable + '\nimport pathlib, sys\n'
                    'pathlib.Path("consumed.txt").write_text(pathlib.Path(sys.argv[1]).read_text())\n')
    (tmp_path / 'local.root').write_bytes(b'local input fixture')
    (tmp_path / 'submit_config.txt').write_text('fileList=' + str(tmp_path / 'local.root') + '\nthreads=1\n')
    script = generate_condor_runscript('myexe', stage_in, False, '',
                                      runtime_config_relpath='cfg/submit_config.txt')
    subprocess.run(['bash'], input=script, text=True, check=True, capture_output=True)
    consumed = (tmp_path / 'consumed.txt').read_text()
    assert 'threads=1' in consumed
    assert ('fileList=input_0.root' if stage_in else str(tmp_path / 'local.root')) in consumed
    if stage_in:
        assert (tmp_path / 'input_0.root').read_bytes() == b'local input fixture'
