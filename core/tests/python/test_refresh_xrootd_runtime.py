import io
import json
import tarfile

import pytest

from core.python.refresh_xrootd_runtime import (
    archive_hashes, patch_probe_functions, refresh_run,
)
from core.python.submission_backend import generate_condor_runscript


@pytest.mark.parametrize("stage_in", [False, True])
def test_refresh_patches_collector_after_other_heredocs(stage_in):
    script = generate_condor_runscript("analysis", stage_in, True, "export KEEP_THIS=1")
    old = script.replace("as_completed(futures):", "as_completed(futures, timeout=12):", 1)
    patched = patch_probe_functions(old)
    assert "as_completed(futures, timeout=12):" not in patched
    assert "export KEEP_THIS=1" in patched
    assert patched == script
    assert patch_probe_functions(patched) == patched


def test_refresh_preserves_payloads_and_keeps_rollback_copy(tmp_path):
    run = tmp_path / "run"
    shared = run / "shared_inputs"
    shared.mkdir(parents=True)
    old_library = b"\x7fELF_old_library"
    (shared / "libRDFXrdAdaptor.so").write_bytes(old_library)
    library = tmp_path / "patched.so"
    library.write_bytes(b"\x7fELF_patched_library")
    archive = run / "shared_inputs.tar.gz"
    with tarfile.open(archive, "w:gz") as output:
        for name, contents in {"libRDFXrdAdaptor.so": old_library,
                               "analysis": b"original analysis executable",
                               "cfg/config.txt": b"selection=unchanged"}.items():
            info = tarfile.TarInfo(name)
            info.size = len(contents)
            output.addfile(info, io.BytesIO(contents))
    script = run / "condor_submissions/dataset/condor_runscript_inner.sh"
    script.parent.mkdir(parents=True)
    script.write_text(generate_condor_runscript("analysis", False, False, "").replace(
        "as_completed(futures):", "as_completed(futures, timeout=12):"))
    script.chmod(0o755)
    state = run / "monitor_state.json"
    state.write_text('{"existing": "monitor state"}')
    before = archive_hashes(archive)
    original_script = script.read_bytes()
    preview = refresh_run(run, library)
    assert preview["scripts_changed"] == 1
    assert archive_hashes(archive) == before
    assert script.read_bytes() == original_script
    result = refresh_run(run, library, apply=True)
    after = archive_hashes(archive)
    assert after["analysis"] == before["analysis"]
    assert after["cfg/config.txt"] == before["cfg/config.txt"]
    assert after["libRDFXrdAdaptor.so"] != before["libRDFXrdAdaptor.so"]
    assert (shared / "libRDFXrdAdaptor.so").read_bytes() == library.read_bytes()
    assert "timeout=12" not in script.read_text()
    assert script.stat().st_mode & 0o111
    assert state.read_text() == '{"existing": "monitor state"}'
    from pathlib import Path
    backup = Path(result["backup_dir"])
    assert archive_hashes(backup / "shared_inputs.tar.gz") == before
    assert (backup / script.relative_to(run)).read_bytes() == original_script
    assert json.loads((backup / "manifest.json").read_text())["applied"] is True
