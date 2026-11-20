#!/usr/bin/env python3
"""Refresh prepared skim retry artifacts while their LAW monitors are stopped.

Defaults to a preview. With --apply, backs up originals and atomically replaces
the worker input helpers and adaptor library. No jobs are submitted or removed.
"""

import argparse
import ast
import copy
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tarfile
import tempfile
from datetime import datetime, timezone

try:
    from .submission_backend import stage_inputs_block, xrootd_optimize_block
except ImportError:
    from submission_backend import stage_inputs_block, xrootd_optimize_block


def patch_probe_functions(script):
    """Replace legacy worker probes with global routing, preserving config paths.

    The public name is retained for existing callers of the recovery utility.
    Current workers are also refreshed idempotently. Unrelated output helpers
    and container/environment setup are left intact.
    """
    pattern = r"run_helper_python - << '(XRDPY|PY|RDFINPUT)'\n(.*?)\n\1$"
    for match in reversed(list(re.finditer(pattern, script, re.S | re.M))):
        tree = ast.parse(match[2])
        functions = {node.name for node in tree.body if isinstance(node, ast.FunctionDef)}
        assignments = {target.id: node.value for node in tree.body
                       if isinstance(node, ast.Assign) for target in node.targets
                       if isinstance(target, ast.Name)}
        if 'select_best_url' in functions:
            stage_in = False
        elif 'rank_redirectors_parallel' in functions:
            stage_in = True
        elif 'global_url' in functions and 'STAGE_INPUTS' in assignments:
            stage_in = ast.literal_eval(assignments['STAGE_INPUTS'])
        else:
            continue
        config = 'submit_config.txt'
        if 'cfg_path' in assignments:
            config = ast.literal_eval(assignments['cfg_path'])
        else:
            for node in ast.walk(tree):
                if (isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
                        and node.func.id == 'read_config' and node.args
                        and isinstance(node.args[0], ast.Constant)):
                    config = node.args[0].value
                    break
        generator = stage_inputs_block if stage_in else xrootd_optimize_block
        replacement = generator(config_file=config).strip('\n')
        script = script[:match.start()] + replacement + script[match.end():]
    return script.replace('echo "Optimizing XRootD redirectors for fastest site"\n', '')

def archive_hashes(path):
    """Hash each regular member to verify unrelated payloads are unchanged."""
    hashes = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if member.isfile():
                stream = archive.extractfile(member)
                digest = hashlib.sha256()
                for block in iter(lambda: stream.read(1024 * 1024), b""):
                    digest.update(block)
                hashes[member.name] = digest.hexdigest()
    return hashes


def patched_archive(source, destination, library):
    """Preserve all archive entries and metadata except the library contents."""
    found = 0
    with tarfile.open(source, "r:gz") as old, tarfile.open(destination, "w:gz") as new:
        for member in old:
            stream = old.extractfile(member) if member.isfile() else None
            if member.name.lstrip("./") == "libRDFXrdAdaptor.so":
                if not member.isfile():
                    raise ValueError("Adaptor archive entry is not a regular file")
                member = copy.copy(member)
                member.size = len(library)
                stream = io.BytesIO(library)
                found += 1
            new.addfile(member, stream)
    if found != 1:
        raise ValueError(f"Expected one adaptor library in {source}, found {found}")
    before, after = archive_hashes(source), archive_hashes(destination)
    library_name = next(n for n in before if n.lstrip("./") == "libRDFXrdAdaptor.so")
    before[library_name] = hashlib.sha256(library).hexdigest()
    if before != after:
        raise ValueError("Archive verification failed: unrelated file contents changed")


def refresh_run(run_dir, library_path, apply=False):
    run_dir, library_path = Path(run_dir).resolve(), Path(library_path).resolve()
    archive = run_dir / "shared_inputs.tar.gz"
    loose_library = run_dir / "shared_inputs/libRDFXrdAdaptor.so"
    if not archive.is_file() or not loose_library.is_file():
        raise ValueError(f"Missing prepared XRootD runtime in {run_dir}")
    library = library_path.read_bytes()
    if not library.startswith(b"\x7fELF"):
        raise ValueError(f"Not an ELF shared library: {library_path}")
    scripts = {}
    for path in sorted(run_dir.glob("condor_submissions/*/condor_runscript*.sh")):
        original = path.read_text()
        updated = patch_probe_functions(original)
        if updated != original:
            scripts[path] = updated.encode()
    result = {"run_dir": str(run_dir), "scripts_changed": len(scripts),
              "library_sha256": hashlib.sha256(library).hexdigest(), "applied": apply}
    if not apply:
        # Validate the archive layout even during a preview.
        hashes = archive_hashes(archive)
        if sum(n.lstrip("./") == "libRDFXrdAdaptor.so" for n in hashes) != 1:
            raise ValueError(f"Missing or duplicate adaptor in {archive}")
        return result

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    backup = run_dir / "runtime_patches" / ("xrootd_" + stamp)
    backup.mkdir(parents=True)
    # Prepare and validate every replacement before modifying an existing file.
    with tempfile.TemporaryDirectory(prefix=".xrootd_refresh_", dir=run_dir) as temporary:
        staged_archive = Path(temporary) / archive.name
        patched_archive(archive, staged_archive, library)
        replacements = {**scripts, loose_library: library}
        staged_files = {}
        for index, (path, contents) in enumerate(replacements.items()):
            staged = Path(temporary) / str(index)
            staged.write_bytes(contents)
            shutil.copymode(path, staged)
            if path.suffix == ".sh":
                subprocess.run(["bash", "-n", str(staged)], check=True)
            staged_files[path] = staged
        shutil.copymode(archive, staged_archive)
        staged_files[archive] = staged_archive
        originals = {}
        for path in staged_files:
            saved = backup / path.relative_to(run_dir)
            saved.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, saved)
            originals[str(path.relative_to(run_dir))] = hashlib.sha256(path.read_bytes()).hexdigest()
        result.update(backup_dir=str(backup), originals_sha256=originals)
        # Preserve rollback information even if a later filesystem operation fails.
        (backup / "manifest.json").write_text(json.dumps(result, indent=2) + "\n")
        for path, staged in staged_files.items():
            os.replace(staged, path)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--library", required=True, type=Path)
    parser.add_argument("--apply", action="store_true", help="Apply after stopping LAW monitors")
    parser.add_argument("run_dirs", nargs="+", type=Path)
    args = parser.parse_args()
    for run_dir in args.run_dirs:
        print(json.dumps(refresh_run(run_dir, args.library, args.apply)), flush=True)


if __name__ == "__main__":
    main()
