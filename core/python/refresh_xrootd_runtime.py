#!/usr/bin/env python3
"""Refresh prepared skim retry artifacts while their LAW monitors are stopped.

Defaults to a preview. With --apply, backs up originals and atomically replaces
the probe functions and adaptor library. No jobs are submitted or removed.
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
    """Replace just the embedded probe collectors, preserving the run setup."""
    for delimiter, generator, function in (
        ("XRDPY", xrootd_optimize_block, "select_best_url"),
        ("PY", stage_inputs_block, "rank_redirectors_parallel"),
    ):
        marker = "run_helper_python - << '" + delimiter + "'\n"
        if marker not in script:
            continue
        generated = generator().split(marker, 1)[1].split("\n" + delimiter, 1)[0]
        replacement = next(n for n in ast.parse(generated).body
                           if isinstance(n, ast.FunctionDef) and n.name == function)
        pattern = re.escape(marker) + r"(.*?)\n" + delimiter + r"$"
        for match in reversed(list(re.finditer(pattern, script, re.S | re.M))):
            code = match[1]
            nodes = [n for n in ast.parse(code).body
                     if isinstance(n, ast.FunctionDef) and n.name == function]
            if not nodes:
                continue
            node = nodes[0]
            lines = code.splitlines(keepends=True)
            new_lines = generated.splitlines(keepends=True)
            code = ("".join(lines[:node.lineno - 1]) +
                    "".join(new_lines[replacement.lineno - 1:replacement.end_lineno]) +
                    "".join(lines[node.end_lineno:]))
            ast.parse(code)
            script = script[:match.start(1)] + code + script[match.end(1):]
    return script


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
