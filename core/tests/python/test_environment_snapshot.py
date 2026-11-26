"""Relocated LAW setup scripts must not discover extensions on workers."""
import os
from pathlib import Path
import subprocess
import sys

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT/'core/python'))
sys.path.insert(0, str(ROOT/'core/python/law'))
import optional_extensions


SETUP = '''# RDF_EXTENSION_ENV_BEGIN
echo "unexpected worker-side extension discovery" >&2
return 99
# RDF_EXTENSION_ENV_END
export SETUP_FINISHED=yes
'''


def test_relocated_snapshot_preserves_exports_and_hook_early_return(tmp_path, monkeypatch):
    hook = tmp_path/'extension.sh'
    hook.write_text('export SNAPSHOT_PRESET="preserved with spaces"\nreturn 0\nexit 99\n')
    monkeypatch.setattr(optional_extensions, 'environment_scripts', lambda: iter([hook]))
    text = optional_extensions.render_environment_setup(SETUP)
    hook.unlink()  # Neither the original hook nor its checkout is on the worker.
    worker = tmp_path/'worker with spaces'
    worker.mkdir()
    setup = worker/'temporary_root_setup.sh'
    setup.write_text(text)
    result = subprocess.run(['bash', '-c',
        'source "$1" && test "$SNAPSHOT_PRESET" = "preserved with spaces" && test "$SETUP_FINISHED" = yes',
        'test', str(setup)], cwd=worker, capture_output=True, text=True)
    assert result.returncode == 0, result.stderr
    assert 'unexpected' not in result.stderr


def test_hook_failure_aborts_setup(tmp_path, monkeypatch):
    hook = tmp_path/'extension.sh'
    hook.write_text('return 42\n')
    monkeypatch.setattr(optional_extensions, 'environment_scripts', lambda: iter([hook]))
    setup = tmp_path/'setup.sh'
    setup.write_text(optional_extensions.render_environment_setup(SETUP))
    result = subprocess.run(['bash', '-c', 'source "$1"', 'test', str(setup)])
    assert result.returncode != 0


def test_disabled_extensions_need_no_worker_discovery(tmp_path, monkeypatch):
    monkeypatch.setenv('RDF_BUILD_EXTENSIONS', 'OFF')
    setup = tmp_path/'setup.sh'
    setup.write_text(optional_extensions.render_environment_setup(SETUP))
    result = subprocess.run(['bash', '-c', 'source "$1" && test "$SETUP_FINISHED" = yes',
                             'test', str(setup)], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_unmarked_user_setup_is_unchanged(monkeypatch):
    def unexpected():
        raise AssertionError('unmarked setup should not discover extensions')
    monkeypatch.setattr(optional_extensions, 'environment_scripts', unexpected)
    text = 'export USER_PRESET=yes\n'
    assert optional_extensions.render_environment_setup(text) == text


@pytest.mark.parametrize('text', ['# RDF_EXTENSION_ENV_BEGIN', '# RDF_EXTENSION_ENV_END',
    '# RDF_EXTENSION_ENV_END\n# RDF_EXTENSION_ENV_BEGIN', SETUP+SETUP])
def test_invalid_snapshot_markers_fail(text):
    with pytest.raises(ValueError):
        optional_extensions.render_environment_setup(text)


def test_task_snapshot_runs_through_local_executor_and_condor_wrapper(tmp_path, monkeypatch):
    import analysis_tasks
    from workflow_executors import _run_analysis_job
    from submission_backend import generate_condor_runscript, _package_worker_yaml
    hook = tmp_path/'extension.sh'
    hook.write_text('export SNAPSHOT_PRESET=yes\n')
    monkeypatch.setattr(optional_extensions, 'environment_scripts', lambda: iter([hook]))
    original = tmp_path/'original_setup.sh'
    original.write_text(SETUP)
    task = analysis_tasks.RunSkimTestJob(exe='/bin/true', root_setup=str(original),
        submit_config=str(tmp_path/'submit_config.yaml'), dataset_manifest='', name='snapshot')
    snapshot = task._root_setup_content
    hook.unlink()
    worker = tmp_path/'worker'
    worker.mkdir()
    exe = worker/'check.sh'
    exe.write_text('#!/bin/bash\ntest "$SNAPSHOT_PRESET" = yes && test "$SETUP_FINISHED" = yes\n')
    exe.chmod(0o755)
    (worker/'submit_config.yaml').write_text('batch: false\n')
    _package_worker_yaml(worker)
    monkeypatch.setenv('XRD_OPTIMIZE_SITES', '0')
    assert _run_analysis_job(exe_path=str(exe), job_dir=str(worker), root_setup=snapshot).startswith('done:')
    wrapper = worker/'run.sh'
    wrapper.write_text(generate_condor_runscript(exe_relpath=exe.name,
        stage_inputs=False, stage_outputs=False, root_setup=snapshot, config_file="submit_config.yaml"))
    result = subprocess.run(['bash', str(wrapper)], cwd=worker,
        env=dict(os.environ, XRD_OPTIMIZE_SITES='0'), capture_output=True, text=True)
    assert result.returncode == 0, result.stdout+result.stderr
