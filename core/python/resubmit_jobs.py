#!/usr/bin/env python3
"""
Resubmit specific Condor job processes from a submission directory.

Usage examples:
  resubmit_jobs.py /path/to/submit_dir --jobs 3,7-9        # resubmit job_3, job_7, job_8, job_9
  resubmit_jobs.py . --jobs 5 --dry-run                  # show what would be submitted

Behavior:
- Reads <submit_dir>/condor_submit.sub and produces a temporary submit file for
  each requested job index by replacing occurrences of "job_$(Process)/" with
  "job_<N>/" and changing the queue line to "queue 1".
- When generating the per-job submit file, the script will **double the
  +MaxRuntime value** from the original submit file (if present) so the
  resubmitted job requests twice the original runtime. Use the
  `--no-double-runtime` flag to opt out of this behavior.
- Calls `condor_submit` on the per-job submit file so only that single process
  is queued.
- Validates that job_<N>/submit_config.txt exists before submitting.

This is non-destructive and intended for interactive use.
"""

from __future__ import annotations
import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from typing import List


def parse_job_list(s: str) -> List[int]:
    out = []
    for part in s.split(','):
        part = part.strip()
        if not part:
            continue
        if '-' in part:
            a, b = part.split('-', 1)
            out.extend(range(int(a), int(b) + 1))
        else:
            out.append(int(part))
    return sorted(set(out))


def load_submit_template(submit_path: str) -> str:
    with open(submit_path, 'r') as f:
        return f.read()


def make_per_job_submit(template: str, job_index: int, double_maxruntime: bool = True) -> str:
    # replace job_$(Process)/ with job_<N>/ for any occurrence
    s = template
    s = s.replace('job_$(Process)/', f'job_{job_index}/')

    # if transfer_input_files contains $(Process) elsewhere, also replace
    s = s.replace('$(Process)', str(job_index))

    # change the queue line to submit only 1 job
    s = re.sub(r'(?m)^[ \t]*queue\b.*$', 'queue 1', s)

    # optionally double +MaxRuntime if present so resubmitted jobs request more time
    if double_maxruntime:
        def _double(m):
            val = int(m.group(1))
            return f'+MaxRuntime={val * 2}'

        s, replaced = re.subn(r'(?m)^[ \t]*\+MaxRuntime\s*=\s*(\d+)\s*$', _double, s)
        if replaced:
            # keep behavior explicit: only modify when a +MaxRuntime line exists
            pass
    return s


def ensure_job_dir_has_submit_config(main_dir: str, job_index: int) -> bool:
    p = os.path.join(main_dir, f'job_{job_index}', 'submit_config.txt')
    return os.path.exists(p)


def submit_temp_file(contents: str, dry_run: bool = False) -> int:
    fd, path = tempfile.mkstemp(prefix='condor_resubmit_', suffix='.sub')
    os.close(fd)
    try:
        with open(path, 'w') as f:
            f.write(contents)
        if dry_run:
            print(f"[dry-run] would run: condor_submit {path}")
            return 0
        print(f"Submitting: {path}")
        out = subprocess.run(['condor_submit', path], check=False, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        print(out.stdout)
        return out.returncode
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description='Resubmit selected job_N entries from a condor submission directory')
    parser.add_argument('submit_dir', nargs='?', default='.', help='Submission directory (where condor_submit.sub lives)')
    parser.add_argument('--jobs', '-j', required=True, help='Comma-separated job indices or ranges (e.g. 0,3-5)')
    parser.add_argument('--dry-run', action='store_true', help='Show actions without calling condor_submit')
    parser.add_argument('--no-double-runtime', action='store_true', help='Do not double +MaxRuntime when resubmitting')
    parser.add_argument('--yes', '-y', action='store_true', help='Assume yes to prompts')
    args = parser.parse_args(argv)

    submit_dir = os.path.abspath(args.submit_dir)
    submit_path = os.path.join(submit_dir, 'condor_submit.sub')
    if not os.path.exists(submit_path):
        print(f"ERROR: submit file not found: {submit_path}")
        return 2

    job_indices = parse_job_list(args.jobs)
    if not job_indices:
        print("No job indices parsed from --jobs")
        return 1

    template = load_submit_template(submit_path)

    missing = [i for i in job_indices if not ensure_job_dir_has_submit_config(submit_dir, i)]
    if missing:
        print(f"Warning: the following job dirs are missing submit_config.txt and will be skipped: {missing}")
        job_indices = [i for i in job_indices if i not in missing]
    if not job_indices:
        print("No valid jobs to resubmit")
        return 0

    print(f"Will resubmit {len(job_indices)} job(s): {job_indices}")
    if not args.yes and not args.dry_run:
        ans = input('Continue? [y/N] ').strip().lower()
        if ans not in ('y', 'yes'):
            print('Aborted by user')
            return 0

    rc_sum = 0
    for idx in job_indices:
        per_job = make_per_job_submit(template, idx, double_maxruntime=(not args.no_double_runtime))
        rc = submit_temp_file(per_job, dry_run=args.dry_run)
        rc_sum += (rc != 0)

    return 0 if rc_sum == 0 else 3


if __name__ == '__main__':
    sys.exit(main())
