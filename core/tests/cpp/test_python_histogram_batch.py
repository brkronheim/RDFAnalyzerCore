"""Explicit batch slices preserve migrations, counts, weights and booking nodes."""
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

import numpy as np
import uproot

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO/'core/python'))
if os.environ.get('RDF_HISTOGRAM_BATCH_MODULE_DIR'):
    sys.path.insert(0, os.environ['RDF_HISTOGRAM_BATCH_MODULE_DIR'])
from rdfanalysis import Analysis, AnalysisConfig, Stage, HistogramGroup, Observable, RegularAxis


class HistogramBatchTests(unittest.TestCase):
    def test_signed_weights_migrations_regions_and_multiple_booking_nodes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); source = root/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events', {'pt': np.array([24., 26., 35.]), 'shift': np.array([28., 24., 37.]),
                    'weight': np.array([-2., 3., -4.]), 'weight_up': np.array([-2.2, 3.3, -4.4])})
            results, graphs = [], []
            for batch in (False, True):
                analysis = Analysis(AnalysisConfig(files=[str(source)], output=str(root/f'{batch}.root'), skim=False))
                analysis.register_variation('pt', 'shape', 'shift')
                analysis.register_variation('weight', 'weight_up', 'weight_up')
                analysis.define('accepted', 'pt > 25.', ['pt'])
                analysis.region('jets', 'accepted')
                analysis.define('observable', 'pt*2.', ['pt'])
                first = [dict(name=f'before/{label}/{variable}', variable=variable, bins=10, low=0., high=100.,
                              weight='weight', region=region, variation=label)
                         for label in ('Nominal', 'shape', 'weight_up')
                         for variable, region in (('pt', None), ('observable', 'jets'))]
                if batch: analysis.histogram_batch(first)
                else:
                    for item in first: analysis.histogram(**item)
                analysis.filter_column('cut', 'accepted')
                analysis.define('later', 'pt+100.', ['pt']).count('after')
                second = [dict(name='after/'+label, variable='later', bins=10, low=100., high=200.,
                               weight='weight', variation=label) for label in ('Nominal', 'shape', 'weight_up')]
                if batch: analysis.histogram_batch(second)
                else:
                    for item in second: analysis.histogram(**item)
                graphs.append(analysis.describe().as_dict()['details']['graph'])
                self.assertIn('histogram-batch', analysis.describe().capabilities)
                self.assertEqual(analysis.native.event_loops, 0)
                analysis.run(); self.assertEqual(analysis.native.event_loops, 1)
                results.append((analysis.counts, analysis.histograms))
                with self.assertRaisesRegex(RuntimeError, 'before run'):
                    analysis.histogram_batch([dict(name='late', variable='later', bins=10, low=0, high=200)])
            self.assertEqual(results[0], results[1])
            self.assertEqual(graphs[0], graphs[1])
            counts, histograms = results[1]
            self.assertEqual(counts['before/Nominal/pt'], 3)
            self.assertEqual(counts['before/Nominal/observable'], 2)
            self.assertEqual(counts['after/Nominal'], 2)
            self.assertAlmostEqual(sum(histograms['after/Nominal']['sumw']), -1.)
            self.assertAlmostEqual(sum(histograms['after/Nominal']['sumw2']), 25.)
            self.assertAlmostEqual(sum(histograms['after/shape']['sumw']), -6.)
            self.assertAlmostEqual(sum(histograms['after/shape']['sumw2']), 20.)

    def test_structural_failures_book_nothing_and_old_modules_fail_clearly(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); source = root/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events', {'pt': np.array([24., 26.])})
            analysis = Analysis(AnalysisConfig(files=[str(source)], output=str(root/'output.root'), skim=False))
            good = dict(name='pt', variable='pt', bins=10, low=0, high=100)
            before = analysis.inspect()
            bad_items = [dict(good, bins=True), dict(good, high=float('nan')), dict(good, region='missing'),
                         dict(good, invalid=True), dict(good, variable='bad\nname')]
            for bad in bad_items:
                with self.subTest(bad=bad), self.assertRaises((ValueError, TypeError)):
                    analysis.histogram_batch([dict(good, name='first'), bad])
            with self.assertRaisesRegex(ValueError, 'Duplicate'):
                analysis.histogram_batch([good, good])
            with self.assertRaisesRegex(ValueError, 'max_histograms'):
                analysis.histogram_batch([good, dict(good, name='other')], max_histograms=1)
            self.assertEqual(analysis.inspect(), before)
            self.assertEqual(analysis.native.event_loops, 0)
        old = object.__new__(Analysis); old.native = SimpleNamespace()
        with self.assertRaisesRegex(RuntimeError, 'rebuild rdfanalyzer'):
            old.histogram_batch([good])

    def test_stage_group_can_explicitly_batch_without_moving_across_filters(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'pt':np.array([24.,26.,35.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'output.root'),skim=False))
            group=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(10,0,100)),
                                          Observable('same_pt','pt',RegularAxis(5,0,50))))
            analysis.apply(Stage('before').histograms(group,batch=True))
            analysis.filter('cut','pt>25.',['pt'])
            after=HistogramGroup('after',(Observable('pt','pt',RegularAxis(10,0,100)),))
            analysis.apply(Stage('after').histograms(after,batch=True))
            self.assertEqual(analysis._stages[0]['histograms'],2)
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run();self.assertEqual(analysis.native.event_loops,1)
            self.assertEqual(analysis.counts['plots/inclusive/Nominal/pt'],3)
            self.assertEqual(analysis.counts['plots/inclusive/Nominal/same_pt'],3)
            self.assertEqual(analysis.counts['after/inclusive/Nominal/pt'],2)


if __name__ == '__main__':
    unittest.main()
