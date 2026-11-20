#!/usr/bin/env python3
"""Preview or update only idle, never-started Condor jobs to an immutable adaptor archive.

Original archives remain unchanged unless --refresh-retries is requested. That
option atomically replaces only their adaptor library, preserving worker helpers,
executable, credentials and configuration. Active LAW monitors can remain running.
"""
from __future__ import annotations
import argparse
import collections
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
import time

if __package__:
    from .refresh_xrootd_runtime import patched_archive, archive_hashes
else:
    from refresh_xrootd_runtime import patched_archive, archive_hashes


def condor(args):
    result = subprocess.run(args, text=True, capture_output=True, timeout=60, check=True)
    return result.stdout


def proc_expression(value, proc):
    """Represent only this job's numbered directory through ProcId.

    Equality with this expression is also a server-side precondition, ensuring
    that no uninspected transfer list can accidentally be rewritten.
    """
    marker = f"/job_{proc}/"
    if marker not in value:
        return json.dumps(value)
    parts = value.split(marker)
    args = [json.dumps(parts[0])]
    for part in parts[1:]:
        args += [json.dumps('/job_'), 'string(ProcId)', json.dumps('/' + part)]
    return 'strcat(' + ','.join(args) + ')'


IDLE = '(JobStatus == 1 && (JobCurrentStartDate =?= UNDEFINED || JobCurrentStartDate == 0) && TransferringInput =!= true)'


def transfer_input_value(value, proc):
    """Evaluate only the strcat/string(ProcId) expressions emitted by this tool.

    Condor JSON encodes an unevaluated expression as /Expr(...)/. Never execute
    arbitrary expressions, or mistake commas inside their strings for file
    separators. Unknown expressions are left ineligible for editing.
    """
    if not isinstance(value, str):
        return None
    if not value.startswith('/Expr('):
        return value
    match = re.fullmatch(r'/Expr\(strcat\((.*)\)\)/', value, re.S)
    if not match:
        return None
    terms, position, result = match[1], 0, []
    decoder = json.JSONDecoder()
    while position < len(terms):
        position += len(terms[position:]) - len(terms[position:].lstrip())
        if terms.startswith('string(ProcId)', position):
            result.append(str(proc))
            position += len('string(ProcId)')
        else:
            try:
                term, end = decoder.raw_decode(terms, position)
            except ValueError:
                return None
            if not isinstance(term, str):
                return None
            result.append(term)
            position = end
        position += len(terms[position:]) - len(terms[position:].lstrip())
        if position == len(terms):
            return ''.join(result)
        if terms[position] != ',':
            return None
        position += 1
    return None


def owned_archive(path, run):
    candidate = Path(path)
    return (candidate == run/'shared_inputs.tar.gz' or
            (candidate.name == 'shared_inputs.tar.gz' and
             candidate.parent.parent == run/'runtime_patches' and
             re.fullmatch(r'located_[0-9a-f]{16}', candidate.parent.name) is not None))


def plan_edits(jobs, run, archive, sha256):
    run = Path(run).resolve()
    archive = Path(archive).resolve()
    edits = collections.defaultdict(list)
    for job in jobs:
        if job.get('JobStatus') != 1 or job.get('JobCurrentStartDate', 0) or job.get('TransferringInput'):
            continue
        value = transfer_input_value(job.get('TransferInput', ''), job['ProcId'])
        if value is None:
            continue
        parts = value.split(',')
        originals = [p for p in parts if owned_archive(p, run)]
        if len(originals) != 1 or originals[0] == str(archive):
            continue  # Spool copies, up-to-date jobs and unrelated inputs.
        replacement = ','.join(str(archive) if p == originals[0] else p for p in parts)
        old_expr = proc_expression(value, job['ProcId'])
        new_expr = proc_expression(replacement, job['ProcId'])
        edits[job['ClusterId'], old_expr, new_expr].append(f"{job['ClusterId']}.{job['ProcId']}")
    plans = []
    for (cluster, old, new), ids in edits.items():
        constraint = f'({IDLE} && ClusterId == {cluster} && TransferInput == {old})'
        plans.append(dict(jobs=ids, constraint=constraint, command=['condor_qedit', '-constraint', constraint,
            'TransferInput', new, 'RDFXrdRuntimeSHA256', json.dumps(sha256),
            'RDFXrdRuntimeArchive', json.dumps(str(archive))]))
    return plans


def refresh_retry_archive(run, archive, library, *, apply=False):
    """Library-only atomic replacement; no changes to monitor-owned state/scripts.

    Each transfer observes a complete old or new archive. Existing workers retain
    their copied library. Backups and a receipt precede replacing any original.
    """
    original = run/'shared_inputs.tar.gz'
    loose = run/'shared_inputs/libRDFXrdAdaptor.so'
    if not original.is_file() or not loose.is_file():
        raise ValueError(f'Missing prepared retry runtime in {run}')
    digest = hashlib.sha256(library).hexdigest()
    expected = archive_hashes(original)
    names = [n for n in expected if n.lstrip('./') == 'libRDFXrdAdaptor.so']
    if len(names) != 1:
        raise ValueError(f'Missing or duplicate adaptor in {original}')
    changed = expected[names[0]] != digest or hashlib.sha256(loose.read_bytes()).hexdigest() != digest
    result = dict(archive=str(original), changed=changed, applied=apply)
    if not changed or not apply:
        return result
    expected[names[0]] = digest
    if archive_hashes(archive) != expected:
        raise ValueError(f'Retry archive would change unrelated payloads in {run}')
    backup = archive.parent/('retry_originals_' + str(time.time_ns()))
    backup.mkdir()
    manifest = dict(library_sha256=digest, files={})
    with tempfile.TemporaryDirectory(prefix='.retry_runtime_', dir=run) as temporary:
        staged = {}
        for index, (path, source) in enumerate(((original, archive), (loose, None))):
            saved = backup/path.name
            shutil.copy2(path, saved)
            manifest['files'][str(path)] = dict(backup=str(saved), sha256=hashlib.sha256(saved.read_bytes()).hexdigest())
            target = Path(temporary)/str(index)
            if source:
                shutil.copyfile(source, target)
            else:
                target.write_bytes(library)
            shutil.copymode(path, target)
            staged[path] = target
        receipt = backup/'manifest.json'
        receipt.write_text(json.dumps(manifest, indent=2)+'\n')
        # Detect another writer rather than overwriting an uninspected archive.
        for path in staged:
            if hashlib.sha256(path.read_bytes()).hexdigest() != manifest['files'][str(path)]['sha256']:
                raise RuntimeError(f'Retry runtime changed during staging: {path}')
        for path, target in staged.items():
            os.replace(target, path)
        if archive_hashes(original) != expected or loose.read_bytes() != library:
            raise RuntimeError(f'Retry runtime verification failed: {run}')
        manifest['verified'] = True
        receipt.write_text(json.dumps(manifest, indent=2)+'\n')
    result.update(backup_dir=str(backup), receipt=str(receipt), verified=True)
    return result


def update_queued(run_dirs, library_path, *, apply=False, refresh_retries=False, query=condor):
    library = Path(library_path).resolve().read_bytes()
    if not library.startswith(b'\x7fELF'):
        raise ValueError('Not an ELF adaptor library')
    digest = hashlib.sha256(library).hexdigest()
    runs, clusters = [], set()
    for directory in run_dirs:
        run = Path(directory).resolve()
        state = json.loads((run/'monitor_state.json').read_text())
        owned = {int(j['cluster']) for d in state['datasets'].values() for j in d['jobs'].values() if j.get('cluster')}
        runs.append((run, owned)); clusters.update(owned)
    if not clusters:
        return dict(applied=apply, runs=[], reason='No submitted clusters')
    scope = '(' + ' || '.join(f'ClusterId == {c}' for c in sorted(clusters)) + ')'
    attributes = 'ClusterId,ProcId,JobStatus,JobCurrentStartDate,TransferringInput,TransferInput,RDFXrdRuntimeSHA256,RDFXrdRuntimeArchive'
    command = ['condor_q', '-constraint', scope, '-json', '-attributes', attributes]
    before = json.loads(query(command) or '[]')
    report = dict(applied=apply, refresh_retries=refresh_retries, library_sha256=digest, queried_unix=time.time(), runs=[])
    for run, owned in runs:
        archive = run/'runtime_patches'/('located_' + digest[:16])/'shared_inputs.tar.gz'
        plans = plan_edits([j for j in before if j['ClusterId'] in owned], run, archive, digest)
        row = dict(run=str(run), archive=str(archive), eligible_jobs=sum(len(p['jobs']) for p in plans), plans=plans)
        report['runs'].append(row)
        if refresh_retries:
            row['retry_refresh'] = refresh_retry_archive(run, archive, library, apply=False)
        if (not plans and not refresh_retries) or not apply:
            continue
        archive.parent.mkdir(parents=True, exist_ok=True)
        original_hashes = archive_hashes(run/'shared_inputs.tar.gz')
        if archive.is_file():
            expected = dict(original_hashes)
            name = next(k for k in expected if k.lstrip('./') == 'libRDFXrdAdaptor.so')
            expected[name] = digest
            if archive_hashes(archive) != expected:
                raise ValueError(f'Existing immutable archive differs: {archive}')
        else:
            temporary = archive.with_suffix('.tmp')
            patched_archive(run/'shared_inputs.tar.gz', temporary, library)
            temporary.replace(archive)
            archive.chmod(0o444)
        if refresh_retries:
            row['retry_refresh'] = refresh_retry_archive(run, archive, library, apply=True)
        # Persist rollback/evidence before modifying the scheduler.
        receipt = archive.parent/f'queue_update_{time.time_ns()}.json'
        receipt.write_text(json.dumps(dict(library_sha256=digest, before=before, plan=row), indent=2)+'\n')
        for plan in plans:
            try:
                plan['result'] = query(plan['command'])
            except subprocess.CalledProcessError:
                # condor_qedit treats zero matches as an error. Jobs may have
                # started (or completed) since the initial preview; skip them,
                # but never hide a rejection while eligible matches remain.
                remaining = json.loads(query(['condor_q', '-constraint', plan['constraint'],
                    '-json', '-attributes', 'ClusterId,ProcId,JobStatus']) or '[]')
                if remaining:
                    raise
                plan['result'] = 'Skipped: no jobs still match the idle/input preconditions'
            finally:
                receipt.write_text(json.dumps(dict(library_sha256=digest, before=before, plan=row), indent=2)+'\n')
        row['receipt'] = str(receipt)
        receipt.write_text(json.dumps(dict(library_sha256=digest, before=before, plan=row), indent=2)+'\n')
    if apply:
        after = json.loads(query(command) or '[]')
        by_id = {(j['ClusterId'], j['ProcId']): j for j in after}
        for job in before:
            current = by_id.get((job['ClusterId'], job['ProcId']))
            if current and job['JobStatus'] == 2 and current.get('TransferInput') != job.get('TransferInput'):
                raise RuntimeError('Running job transfer inputs changed during deployment')
        report['patched_jobs_in_queue'] = sum(j.get('RDFXrdRuntimeSHA256') == digest for j in after)
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--library', required=True, type=Path)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--refresh-retries', action='store_true',
                        help='Also atomically refresh the original library/archive used by future LAW retries')
    parser.add_argument('run_dirs', nargs='+', type=Path)
    args = parser.parse_args()
    print(json.dumps(update_queued(args.run_dirs, args.library, apply=args.apply,
                                  refresh_retries=args.refresh_retries), indent=2))

if __name__ == '__main__':
    main()
