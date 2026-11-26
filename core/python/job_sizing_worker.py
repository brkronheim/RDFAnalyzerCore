"""Standalone paired pilot driver, embedded in Condor workers (standard library)."""
from native_config_io import read_config, write_config
import json
import os
from pathlib import Path
import re
import resource
import subprocess
import sys
import time


def run_measurement(executable, config_path, base, label, entries=None):
    config = dict(base)
    for key in ("firstEntry", "lastEntry", "entryRangeMode"):
        config.pop(key, None)
    config['enableCounters'] = 'true'
    if entries is not None:
        config.update(firstEntry='0', lastEntry=str(entries), entryRangeMode='filter')
        for key in ('saveFile', 'metaFile'):
            if config.get(key):
                p = Path(config[key])
                config[key] = str(p.with_name('pilot_prefix_' + p.name))
    path = config_path.with_name('pilot_' + label + '.yaml')
    write_config(config, path)
    out = Path('pilot_' + label + '.stdout')
    err = Path('pilot_' + label + '.stderr')
    before = resource.getrusage(resource.RUSAGE_CHILDREN)
    start = time.monotonic()
    env = dict(os.environ, RDF_ANALYZER_SUMMARY='1')
    with out.open('w') as stdout, err.open('w') as stderr:
        result = subprocess.run([executable, str(path)], env=env, stdout=stdout, stderr=stderr)
    wall = time.monotonic() - start
    after = resource.getrusage(resource.RUSAGE_CHILDREN)
    text = out.read_text(errors='replace')
    print(text, end='', flush=True)
    print(err.read_text(errors='replace'), file=sys.stderr, end='', flush=True)
    if result.returncode:
        if result.returncode < 0:
            # Preserve signal exit semantics of the normal analysis launcher.
            import signal
            signal.signal(-result.returncode, signal.SIG_DFL)
            os.kill(os.getpid(), -result.returncode)
        raise SystemExit(result.returncode)
    count = re.search(r'CounterService: sample=\S+ entries=(\d+)', text)
    if not count:
        count = re.search(r'RDF_ANALYZER_EVENTS=(\d+)', text)
    if not count:
        raise RuntimeError('Pilot requires CounterService raw input-event counts')
    return dict(events=int(count[1]), wall_s=wall,
                cpu_s=after.ru_utime + after.ru_stime - before.ru_utime - before.ru_stime)


def main():
    executable, source, prefix = sys.argv[1:]
    path = Path(source)
    config = read_config(path)
    if len([v for v in config.get('fileList', '').split(',') if v.strip()]) != 1:
        raise RuntimeError('Sizing pilots require exactly one file')
    short = run_measurement(executable, path, config, 'prefix', int(prefix))
    full = run_measurement(executable, path, config, 'full')
    print('RDF_JOB_SIZING_PILOT=' + json.dumps(dict(schema_version=1, prefix=short, full=full)), flush=True)


if __name__ == '__main__':
    main()
