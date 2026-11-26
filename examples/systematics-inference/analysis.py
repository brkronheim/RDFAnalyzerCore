"""Synthetic shape/weight variations and real native BDT inference in an event graph."""
import argparse
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
from rdfanalysis import AnalysisConfig

LABELS = ('Nominal', 'shapeUp', 'shapeDown', 'weightUp', 'weightDown')


def shift_up(x): return 1.2*x
def shift_down(x): return .8*x
def feature(x): return x


def build(analysis):
    analysis.count('input')
    analysis.define_numba('x_up', shift_up, ['x'], return_type='double', nominal=True)
    analysis.define_numba('x_down', shift_down, ['x'], return_type='double', nominal=True)
    analysis.register_variation_pair('x', 'shape', 'x_up', 'x_down')
    analysis.define_nominal('weight_up', '2.f * genWeight')
    analysis.define_nominal('weight_down', '0.5f * genWeight')
    analysis.weights({'generator': 'genWeight'}, variations={
        'weight': ('generator', 'weight_up', 'weight_down')})
    # Union selection retains nominal-fail/shift-pass events. Later definitions
    # stay downstream; each histogram uses its own variation's acceptance mask.
    analysis.filter('selected', 'x > 0.5')
    analysis.define_numba('ml_feature', feature, ['x'], return_type='float')
    analysis.define_nominal('run_model', 'true')
    analysis.native.AddPlugin('bdt', 'BDTManager')
    analysis.native.applyAllBDTs()
    analysis.count('selected_union')
    for label in LABELS:
        analysis.histogram(label+'/score', 'score', 2, 0., 1., weight='event_weight', variation=label)
        analysis.histogram(label+'/acceptance', 'score', 2, 0., 1., variation=label)
    return analysis


def make_input(path):
    import numpy as np
    import uproot
    with uproot.create(path) as output:
        output.mktree('Events', {'x': np.array([.45, .55, .85], dtype='float64'),
            'genWeight': np.array([1., -2., 3.], dtype='float32')})


def contract():
    return {'scope': 'synthetic tutorial; no CMS calibration or trained physics classifier',
        'shape': 'x multiplied by1.2/0.8; varied acceptance x>0.5 and BDT input',
        'weight': 'generator factor multiplied by2/0.5; one-at-a-time replacement, no shape/weight cross product',
        'model': 'one split at feature0.6, raw margins -1/+1, base margin0; native BDTManager sigmoid',
        'labels': list(LABELS), 'expected_counts': {'input': 3, 'selected_union': 3},
        'per_label_selected': [2, 3, 1, 2, 2],
        'score_sumw_inner': [[-2., 3.], [1., 1.], [0., 3.], [-4., 6.], [-1., 1.5]],
        'score_sumw2_inner': [[4., 9.], [1., 13.], [0., 9.], [16., 36.], [1., 2.25]],
        'flow_bins': 'both empty; prepend/append zero to inner-bin arrays',
        'execution_acceptance': 'queued; planning/kernel/builder tests do not certify native scores'}


def config(input_path, output_path, models=None):
    return AnalysisConfig(files=[str(input_path)], output=str(output_path), skim=False,
        systematics=True, persist_histograms=True,
        options={} if models is None else {'bdtConfig': str(models)})


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', choices=('describe', 'make-input', 'run'), default='describe')
    parser.add_argument('--input', type=Path, default=Path('systematics.root'))
    parser.add_argument('--output', type=Path, default=Path('systematics-output.root'))
    args = parser.parse_args(argv)
    try:
        settings = config(args.input, args.output)
        if args.mode == 'describe':
            document = {'configuration': settings.describe().as_dict(), 'contract': contract(),
                'model_setup': 'explicit run stages the adjacent toy model and creates a temporary native model config'}
        elif args.mode == 'make-input':
            make_input(args.input); document = {'input': str(args.input), 'contract': contract()}
        else:
            if not args.input.is_file():
                raise FileNotFoundError(f'Missing input {args.input}; use --mode make-input or supply your file')
            if Path(settings.native_config()['metaFile']).exists():
                raise FileExistsError('Use a fresh --output; metadata already exists')
            with TemporaryDirectory(prefix='rdf_inference_') as temporary:
                work = Path(temporary)
                if any(char.isspace() for char in str(work)):
                    raise ValueError('Native BDT config grammar requires a temporary directory without whitespace; set TMPDIR')
                model = work/'toy_bdt.txt'
                shutil.copyfile(Path(__file__).with_name('toy_bdt.txt'), model)
                models = work/'models.yaml'
                import yaml
                models.write_text(yaml.safe_dump([dict(file=str(model), name='score',
                    inputVariables='ml_feature', runVar='run_model')]))
                from rdfanalysis import Analysis
                analysis = build(Analysis(config(args.input, args.output, models)))
                analysis.run(); document = analysis.result().as_dict()
    except (ValueError, TypeError, OSError, ImportError, RuntimeError) as error:
        print(json.dumps({'success': False, 'error': str(error)}, indent=2)); return 1
    print(json.dumps(document, indent=2)); return 0


if __name__ == '__main__':
    raise SystemExit(main())
