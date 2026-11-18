#!/usr/bin/env python3
"""Steer an already compiled analysis; the Python parent imports no ROOT runtime."""
import argparse
import importlib
import json
from pathlib import Path
import subprocess
import sys


def load_receiver(directory):
    directory = Path(directory).resolve(strict=True)
    expected = (directory / 'rdfresults.py').resolve(strict=True)
    if 'rdfresults' in sys.modules and Path(sys.modules['rdfresults'].__file__).resolve() != expected:
        raise ValueError('a different rdfresults receiver is already loaded')
    sys.path.insert(0, str(directory))
    module = importlib.import_module('rdfresults')
    if Path(module.__file__).resolve() != expected:
        raise ValueError('receiver must come from the explicitly selected installation')
    return module.RunResult


def run_native(executable, receiver_dir):
    executable = Path(executable)
    if not executable.is_absolute():
        raise ValueError('use an absolute compiled executable path')
    receiver = load_receiver(receiver_dir)
    completed = subprocess.run([str(executable), '--run'], capture_output=True, text=True, timeout=30)
    if completed.returncode:
        raise RuntimeError('compiled analysis failed: ' + (completed.stderr or completed.stdout)[:2000])
    result = receiver.from_dict(json.loads(completed.stdout))
    if not result.success:
        raise ValueError('compiled analysis did not report success')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--executable', type=Path)
    parser.add_argument('--receiver-dir', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if not args.run:
        document = dict(schema='native-results-steering-plan-v1', native_launched=False,
                        executable=None if args.executable is None else str(args.executable),
                        arguments=['--run'], receiver='installed rdfresults.RunResult.from_dict',
                        scope='Python orchestrates; physics and all ROOT graph work are compiled in the linked C++ library')
    else:
        if args.executable is None or args.receiver_dir is None:
            parser.error('explicit --executable and --receiver-dir required with --run')
        document = run_native(args.executable, args.receiver_dir).as_dict()
        if args.output is not None:
            with args.output.open('x') as stream:
                json.dump(document, stream, indent=2, allow_nan=False)
                stream.write('\n')
    print(json.dumps(document, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
