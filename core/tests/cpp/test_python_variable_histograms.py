"""Variable axes preserve ROOT flows, native types, migrations and booking nodes."""
import json
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
if os.environ.get('RDF_VARIABLE_AXES_MODULE_DIR'):
    sys.path.insert(0, os.environ['RDF_VARIABLE_AXES_MODULE_DIR'])
from rdfanalysis import Analysis, AnalysisConfig, Stage, HistogramGroup, Observable, RegularAxis, VariableAxis


class VariableHistogramTests(unittest.TestCase):
    def make_input(self, root):
        source = root/'input.root'
        pt = np.array([-1., 0., .25, 1., 2., 3., 8., 10., 11.])
        weight = np.array([1., -2., 3., -4., 5., -6., 7., -8., 9.])
        with uproot.recreate(source) as output:
            output.mktree('Events', {'pt': pt, 'shift': pt+2., 'weight': weight, 'weight_up':weight*1.5,
                'pt_float': pt.astype('float32'), 'weight_float': weight.astype('float32'),
                'pt_int': pt.astype('int32')})
        return source

    def test_exact_edges_flows_signed_weights_native_types_and_mixed_batch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); source=self.make_input(root)
            results=[]; graphs=[]
            for batch in (False, True):
                analysis=Analysis(AnalysisConfig(files=[str(source)], output=str(root/f'output{batch}.root'),
                                                skim=False, persist_histograms=True))
                requests=[]
                for variable in ('pt','pt_float','pt_int'):
                    for weight in ('','weight','weight_float'):
                        requests.append(dict(name=variable+'/'+(weight or 'unweighted'),variable=variable,
                                             edges=[0.,1.,3.,10.],weight=weight))
                requests.append(dict(name='regular',variable='pt',bins=10,low=0.,high=10.,weight='weight'))
                if batch: analysis.histogram_batch(requests)
                else:
                    for request in requests:
                        getattr(analysis,'histogram_variable' if 'edges' in request else 'histogram')(**request)
                self.assertEqual(analysis.native.event_loops,0)
                capabilities=analysis.describe().capabilities
                self.assertIn('variable-histograms',capabilities)
                self.assertIn('mixed-histogram-batch',capabilities)
                graphs.append(analysis.describe().as_dict()['details']['graph'])
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                results.append((analysis.counts,analysis.histograms))
                for name,histogram in analysis.histograms.items():
                    self.assertEqual(analysis.counts[name],9)
                    with uproot.open(analysis.config.native_config()['metaFile']) as output:
                        saved=output['python_histograms/'+name]
                        np.testing.assert_array_equal(saved.axis().edges(),histogram['edges'])
                        np.testing.assert_array_equal(saved.values(flow=True),histogram['sumw'])
                        np.testing.assert_allclose(saved.variances(flow=True),histogram['sumw2'],rtol=1e-15)
                weighted=analysis.histograms['pt/weight']
                self.assertEqual(weighted['edges'],[0.,1.,3.,10.])
                self.assertEqual(weighted['sumw'],[1.,1.,1.,1.,1.])
                np.testing.assert_allclose(weighted['sumw2'],[1.,13.,41.,85.,145.],rtol=1e-15)
                with self.assertRaisesRegex(RuntimeError,'before run'):
                    analysis.histogram_variable('late','pt',[0,1])
            self.assertEqual(results[0],results[1])
            self.assertEqual(graphs[0],graphs[1])

    def test_staged_mixed_groups_selection_migrations_and_separate_filter_nodes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); source=self.make_input(root)
            results=[];graphs=[]
            for batch in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/f'{batch}.root'),skim=False))
                analysis.register_variation('pt','shape','shift')
                analysis.register_variation('weight','weightScale','weight_up')
                analysis.define('accepted','pt>1.',['pt'])
                analysis.define('selected','pt>=0. && pt<10.',['pt'])
                analysis.region('inside','selected')
                observables=(Observable('variable','pt',VariableAxis([0,1,3,10])),
                             Observable('regular','pt',RegularAxis(10,0,10)))
                before=HistogramGroup('before',observables,regions=(None,'inside'),variations='all',weight='weight')
                analysis.apply(Stage('before').histograms(before,batch=batch))
                analysis.filter_column('cut','accepted')
                analysis.define('later','pt+20.',['pt'])
                after=HistogramGroup('after',observables,regions=(None,'inside'),variations='all',weight='weight')
                stage=Stage('after').histograms(after,batch=batch)
                stage.histogram_variable('later','later',[20,21,23,30],weight='weight')
                json.dumps(stage.preview(analysis).as_dict())
                analysis.apply(stage)
                self.assertEqual(analysis._stages[0]['histograms'],12)
                self.assertEqual(analysis._stages[1]['histograms'],13)
                self.assertEqual(analysis.native.event_loops,0)
                graphs.append(analysis.describe().as_dict()['details']['graph'])
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                results.append((analysis.counts,analysis.histograms))
                self.assertEqual(analysis.counts['before/inclusive/Nominal/variable'],9)
                self.assertEqual(analysis.counts['before/inside/Nominal/variable'],6)
                self.assertEqual(analysis.counts['after/inclusive/Nominal/variable'],5)
                self.assertEqual(analysis.counts['after/inclusive/shape/variable'],8)
                self.assertEqual(analysis.counts['after/inside/Nominal/variable'],3)
                self.assertEqual(analysis.counts['after/inside/shape/variable'],5)
                nominal=analysis.histograms['after/inclusive/Nominal/variable']
                varied=analysis.histograms['after/inclusive/weightScale/variable']
                np.testing.assert_array_equal(varied['sumw'],np.array(nominal['sumw'])*1.5)
                np.testing.assert_allclose(varied['sumw2'],np.array(nominal['sumw2'])*2.25,rtol=1e-15)
            self.assertEqual(results[0],results[1]);self.assertEqual(graphs[0],graphs[1])

    def test_invalid_edges_prevalidate_entire_batch_and_native_defense(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary); source=self.make_input(root)
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'output.root'),skim=False))
            good=dict(name='good',variable='pt',bins=5,low=0,high=10)
            before=analysis.inspect()
            for edges in ([],[0],[0,0],[1,0],[0,float('nan')],[0,float('inf')],[False,1],['0',1]):
                with self.subTest(edges=edges):
                    with self.assertRaises(ValueError):analysis.histogram_variable('bad','pt',edges)
                    with self.assertRaises(ValueError):analysis.histogram_batch([good,dict(name='bad',variable='pt',edges=edges)])
                    if all(type(value) in (int,float) for value in edges):
                        with self.assertRaises(ValueError):analysis.native.book_histogram_variable('bad','pt',edges)
                        with self.assertRaises(ValueError):
                            analysis.native.book_histograms_mixed([
                                ('first','pt',5,0.,10.,[],'','','Nominal'),
                                ('bad','pt',0,0.,0.,edges,'','','Nominal')])
            with self.assertRaisesRegex(ValueError,'requires'):
                analysis.histogram_batch([dict(good,edges=[0,1])])
            self.assertEqual(analysis.inspect(),before)
            self.assertEqual(analysis.native.event_loops,0)
            # Existing native 8-tuple batches remain a supported entry point.
            analysis.native.book_histograms([('legacy','pt',5,0.,10.,'','','Nominal')])
            analysis.run();self.assertEqual(analysis.counts['legacy'],9)

    def test_separate_mixed_calls_after_definition_never_reuse_an_old_node(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=self.make_input(root)
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/'out.root'),skim=False))
            analysis.histogram_batch([dict(name='before',variable='pt',edges=[0,1,3,10]),
                                      dict(name='regular_before',variable='pt',bins=10,low=0,high=10)])
            analysis.define('later','pt+20.',['pt'])
            analysis.histogram_batch([dict(name='after',variable='later',edges=[20,21,23,30]),
                                      dict(name='regular_after',variable='later',bins=10,low=20,high=30)])
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run();self.assertEqual(analysis.native.event_loops,1)
            self.assertEqual(analysis.histograms['before']['sumw'],analysis.histograms['after']['sumw'])
            self.assertEqual(analysis.counts['before'],analysis.counts['after'])

    def test_old_native_modules_raise_clear_feature_errors(self):
        analysis=object.__new__(Analysis);analysis.native=SimpleNamespace(book_histograms=lambda items:None)
        with self.assertRaisesRegex(RuntimeError,'rebuild rdfanalyzer'):
            analysis.histogram_variable('x','pt',[0,1])
        with self.assertRaisesRegex(RuntimeError,'mixed histogram batch.*rebuild rdfanalyzer'):
            analysis.histogram_batch([dict(name='x',variable='pt',edges=[0,1])])


if __name__=='__main__':unittest.main()
