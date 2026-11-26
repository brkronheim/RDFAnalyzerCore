#!/usr/bin/env python3
"""Explicit finite integration check of an installed native-results example.

Default: metadata only. --run launches four finite native invocations (two
zero-loop inspections and two eight-event runs), no compiler or framework build.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys


def oracle():
    values = [-2., -1., 0., .5, 1., 2., 3., 5.]
    weights = [1., -.5, 2., -1., .5, 1.5, -2., .25]
    selected = [(2 * x - .5, w) for x, w in zip(values, weights) if x >= 0 and 2 * x - .5 < 7]
    contents, variances = [0.] * 5, [0.] * 5
    for value, weight in selected:
        index = 0 if value < 0 else 4 if value >= 3 else int(value) + 1
        contents[index] += weight
        variances[index] += weight * weight
    return dict(counts=dict(input=8, selected=len(selected)),
                sums=dict(input_weight=sum(weights), selected_weight=sum(w for _, w in selected),
                          selected_observable=sum(x for x, _ in selected)),
                histograms=dict(observable=dict(edges=[0., 1., 2., 3.], sumw=contents, sumw2=variances, flow=True)))


def check_physics(document):
    truth = oracle()
    if (document.get('schema') != 'rdf-run-result-v1' or document.get('status') != 'succeeded'
            or document.get('success') is not True or document.get('event_loops') != 1):
        raise ValueError('successful result with supplied one-loop count required')
    for name, expected in truth.items():
        if document.get(name) != expected:
            raise ValueError('independent ' + name + ' oracle differs')
    details = document.get('details', {})
    if details.get('before_event_loops') != 0 or details.get('actions_booked') != 6 or details.get('histogram_entries') != 5:
        raise ValueError('booked-action, preflight or histogram-entry evidence differs')


def physics_document(document):
    # Only variable timing values are excluded; all other envelope fields match.
    return {name: value for name, value in document.items() if name != 'timings'}


def sha(path):
    value = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1048576), b''):
            value.update(block)
    return value.hexdigest()


def validate(executable, receiver_dir, output, forbid_source_root=None):
    executable = Path(executable).resolve(strict=True)
    receiver_dir = Path(receiver_dir).resolve(strict=True)
    steer_path = (Path(__file__).resolve().parents[1] / 'steer.py').resolve(strict=True)
    output = Path(output).absolute()
    if forbid_source_root is not None:
        excluded = Path(forbid_source_root).resolve()
        for path in (executable, receiver_dir / 'rdfresults.py', steer_path, Path(__file__).resolve()):
            path = path.resolve(strict=True)
            if path == excluded or excluded in path.parents:
                raise ValueError('validation must use installed files outside the checkout')
    output.mkdir(parents=True, exist_ok=False)
    commands = []
    def call(command):
        commands.append(command)
        # A fresh working directory and -S/explicit receiver selection below
        # avoid Python imports from the checkout. ROOT runtime environment stays.
        environment = dict(os.environ)
        environment.pop('PYTHONPATH', None)
        result = subprocess.run(command, cwd=output, env=environment, capture_output=True, text=True, timeout=30, check=True)
        return json.loads(result.stdout)
    descriptions = [call([str(executable), mode]) for mode in ('--describe', '--preflight')]
    for value in descriptions:
        if (value.get('schema') != 'native-results-description-v1' or value.get('event_loops') != 0
                or value.get('fixture_entries') != 8 or value.get('actions_booked') != 6
                or value.get('execution_requested') is not False):
            raise ValueError('description/preflight must leave the ROOT graph unexecuted')
    direct = call([str(executable), '--run'])
    spec = importlib.util.spec_from_file_location('native_results_example_steering', steer_path)
    steering = importlib.util.module_from_spec(spec); spec.loader.exec_module(steering)
    receiver = steering.load_receiver(receiver_dir)
    if receiver.from_dict(direct).as_dict() != direct:
        raise ValueError('native document changed through detached receiver')
    steered = call([sys.executable, '-S', str(steer_path), '--executable', str(executable),
                    '--receiver-dir', str(receiver_dir), '--run'])
    check_physics(direct); check_physics(steered)
    if physics_document(direct) != physics_document(steered):
        raise ValueError('direct and Python-steered compiled results differ')
    for name, document in (('direct.json', direct), ('steered.json', steered), ('inspections.json', descriptions)):
        with (output / name).open('x') as stream:
            json.dump(document, stream, indent=2, allow_nan=False); stream.write('\n')
    receipt = dict(schema='native-results-example-validation-v1', accepted=True, actual_processing_runs=2,
                   native_invocations=4, supplied_event_loops_per_run=1, descriptions_event_loops=[0, 0],
                   exact_physics_parity=True, excluded_parity_fields=['timings'], commands=commands,
                   selected_files_sha256={str(path): sha(path) for path in
                       (executable, (receiver_dir / 'rdfresults.py').resolve(strict=True), steer_path, Path(__file__).resolve())},
                   accepted_for_performance=False,
                   scope='finite installed-example correctness; selected paths/import isolation, not complete filesystem or DSO attestation')
    with (output / 'ACCEPTANCE.json').open('x') as stream:
        json.dump(receipt, stream, indent=2, allow_nan=False); stream.write('\n')
    return receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument('--executable', type=Path)
    parser.add_argument('--receiver-dir', type=Path)
    parser.add_argument('--output', type=Path)
    parser.add_argument('--forbid-source-root', type=Path)
    parser.add_argument('--run', action='store_true')
    args = parser.parse_args()
    if args.run:
        if any(value is None for value in (args.executable, args.receiver_dir, args.output)):
            parser.error('explicit executable, installed receiver directory and fresh output required')
        result = validate(args.executable, args.receiver_dir, args.output, args.forbid_source_root)
    else:
        result = dict(schema='native-results-example-validation-plan-v1', native_launched=False,
                      required=['installed executable', 'installed rdfresults receiver', 'fresh output', 'explicit --run'],
                      expected_counts=oracle()['counts'], future_processing_runs=2, accepted=False)
    print(json.dumps(result, indent=2, allow_nan=False))


if __name__ == '__main__':
    main()
