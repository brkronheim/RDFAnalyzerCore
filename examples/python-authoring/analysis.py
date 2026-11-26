"""Small Python/Numba analysis; default description opens no input or native runtime."""
import argparse
import json
from pathlib import Path
from rdfanalysis import AnalysisConfig, InputSettings


def has_jets(pt):
    return len(pt) > 0


def selected_ht(pt, eta):
    total = 0.
    for index in range(len(pt)):
        if pt[index] > 30. and abs(eta[index]) < 2.:
            total += pt[index]
    return total


def twice(value):
    return 2. * value


def build(analysis):
    # Keep expensive definitions downstream of cuts; preserve declaration order.
    analysis.count('input')
    analysis.define_numba('has_jets', has_jets, ['Jet_pt'], return_type='bool')
    analysis.filter('nonempty', 'has_jets')
    analysis.define_numba('jet_ht', selected_ht, ['Jet_pt', 'Jet_eta'], return_type='double')
    analysis.filter('ht_cut', 'jet_ht > 60.')
    analysis.define_numba('twice_ht', twice, ['jet_ht'], return_type='double')
    analysis.count('selected').sum('total_ht', 'jet_ht')
    analysis.histogram('ht', 'jet_ht', 10, 0., 200.)
    analysis.histogram('double_ht', 'twice_ht', 10, 0., 200.)
    return analysis


def make_input(path):
    # Optional tutorial data creation is explicit; never needed for description.
    import awkward as ak
    import numpy as np
    import uproot
    with uproot.create(path) as output:
        output.mktree('Events', {
            'Jet_pt': ak.values_astype(ak.Array([[50., 40.], [10.], []]), np.float32),
            'Jet_eta': ak.values_astype(ak.Array([[.2, -.2], [.1], []]), np.float32)})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', type=Path, default=Path('tutorial.root'))
    parser.add_argument('--output', type=Path, default=Path('tutorial-output.root'))
    parser.add_argument('--mode', choices=('describe', 'schema', 'make-input', 'run'), default='describe')
    args = parser.parse_args(argv)
    try:
        config = AnalysisConfig(files=[str(args.input)], output=str(args.output),
            skim=False, systematics=False, persist_histograms=True,
            input_settings=InputSettings(cache_enabled=False))
        if args.mode == 'schema':
            document = config.schema()
        elif args.mode == 'describe':
            document = config.describe().as_dict()
        elif args.mode == 'make-input':
            make_input(args.input)
            document = {'input': str(args.input), 'expected': {'input': 3, 'selected': 1, 'total_ht': 90.}}
        else:
            if not args.input.is_file():
                raise FileNotFoundError(f'Input does not exist: {args.input}; use --mode make-input or supply your file')
            metadata = Path(config.native_config()['metaFile'])
            if metadata.exists():
                raise FileExistsError(f'Use a fresh --output; metadata already exists: {metadata}')
            from rdfanalysis import Analysis
            analysis = build(Analysis(config))
            analysis.run()
            document = analysis.result().as_dict()
    except (ValueError, TypeError, OSError, ImportError, RuntimeError) as error:
        print(json.dumps({'success': False, 'error': str(error)}, indent=2))
        return 1
    print(json.dumps(document, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
