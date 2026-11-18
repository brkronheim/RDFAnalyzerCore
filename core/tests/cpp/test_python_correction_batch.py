"""Native correction batches preserve shapes, signed weights and vector types."""
import json
import os
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest

import awkward as ak
import numpy as np
import uproot

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
if os.environ.get('RDF_CORRECTION_BATCH_MODULE_DIR'):
    sys.path.insert(0,os.environ['RDF_CORRECTION_BATCH_MODULE_DIR'])
from rdfanalysis import Analysis,AnalysisConfig


def payload(path):
    content=[]
    for key,factor in [('nominal',1.),('up',1.1),('down',.9)]:
        content.append({'key':key,'value':{'nodetype':'formula','expression':f'{factor}*(x+y)',
            'parser':'TFormula','variables':['pt','offset']}})
    path.write_text(json.dumps({'schema_version':2,'corrections':[{'name':'sf','version':1,
        'inputs':[{'name':'variation','type':'string'},{'name':'pt','type':'real'},{'name':'offset','type':'real'}],
        'output':{'name':'weight','type':'real'},'data':{'nodetype':'category','input':'variation','content':content}}]}))


def configured(root,source,name,features,*,systematics=True):
    correction=root/'corrections.json'
    if not correction.exists():payload(correction)
    analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(root/(name+'.root')),skim=False,systematics=systematics,
        options={'correctionScalarOutputType':'double'}))
    analysis.native.AddPlugin('corr','CorrectionManager')
    analysis.native.registerCorrection('corr','sf',str(correction),'sf',features)
    return analysis


class CorrectionBatchTests(unittest.TestCase):
    def test_scalar_batch_matches_independent_calls_with_signed_shape_migrations(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'pt':np.array([24.,26.,35.]),
                'shift':np.array([28.,24.,37.]),'gen':np.array([-2.,3.,-4.])})
            results=[]
            for batch in (False,True):
                analysis=configured(root,source,str(batch),['pt','offset'])
                analysis.define_nominal('offset','0.').register_variation('pt','shape','shift')
                if batch:
                    self.assertIs(analysis.correction_batch('corr','sf',[['nominal'],['up'],['down']],
                        outputs=['sf','sf_up','sf_down']),analysis)
                else:
                    for label,column in [('nominal','sf'),('up','sf_up'),('down','sf_down')]:
                        analysis.native.applyCorrection('corr','sf',[label],[],column)
                self.assertIn('correction-batch',analysis.describe().capabilities)
                self.assertIn(analysis.native.column_type('sf'),('double','Double_t'))
                analysis.define('accepted','pt>25.',['pt']).filter_column('cut','accepted')
                for column in ('sf','sf_up','sf_down'):
                    analysis.define('weighted_'+column,'gen*'+column,['gen',column])
                    for label in ('Nominal','shape'):
                        analysis.histogram(column+'/'+label,'pt',10,0,100,weight='weighted_'+column,variation=label)
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                results.append((analysis.counts,analysis.histograms))
                with self.assertRaisesRegex(RuntimeError,'before run'):
                    analysis.correction_batch('corr','sf',[['up']],outputs=['late'])
            self.assertEqual(results[0],results[1])
            histograms=results[1][1]
            self.assertAlmostEqual(sum(histograms['sf/Nominal']['sumw']),-62.)
            self.assertAlmostEqual(sum(histograms['sf/Nominal']['sumw2']),25684.)
            self.assertAlmostEqual(sum(histograms['sf/shape']['sumw']),-204.)
            self.assertAlmostEqual(sum(histograms['sf/shape']['sumw2']),25040.)

    def test_per_output_nominal_policy_and_structural_errors_do_not_mutate_graph(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'pt':np.array([24.,26.,35.]),
                'shift':np.array([28.,24.,37.]),'gen':np.array([-2.,3.,-4.])})
            analysis=configured(root,source,'policy',['pt','offset'])
            analysis.define_nominal('offset','0.').register_variation('pt','shape','shift')
            before=analysis.inspect()
            for kwargs in ({'outputs':['one']},{'outputs':['same','same']},
                           {'outputs':['up','down'],'propagate_systematics':[True]},
                           {'outputs':['up','down'],'propagate_systematics':['true',False]},
                           {'outputs':['up','down'],'vector':'false'}):
                with self.subTest(kwargs=kwargs),self.assertRaises((ValueError,TypeError)):
                    analysis.correction_batch('corr','sf',[['up'],['down']],**kwargs)
            self.assertEqual(analysis.inspect(),before)
            analysis.correction_batch('corr','sf',[['up'],['down']],outputs=['up','down'],
                                      propagate_systematics=[True,False])
            self.assertTrue(analysis.has_column('up_shape'));self.assertFalse(analysis.has_column('down_shape'))
            with self.assertRaisesRegex(ValueError,'output already exists'):
                analysis.correction_batch('corr','sf',[['down']],outputs=['down'])
            analysis.correction_batch('corr','sf',[['down']],inputs=['shift','offset'],
                                      outputs=['other_down'],propagate_systematics=[False])
            analysis.define('accepted','pt>25.',['pt']).filter_column('cut','accepted')
            analysis.define('weighted','gen*down',['gen','down'])
            analysis.define('other_weighted','gen*other_down',['gen','other_down'])
            analysis.histogram('down_shape','pt',10,0,100,weight='weighted',variation='shape')
            analysis.histogram('other_down_shape','pt',10,0,100,weight='other_weighted',variation='shape')
            analysis.run();self.assertEqual(analysis.native.event_loops,1)
            self.assertAlmostEqual(sum(analysis.histograms['down_shape']['sumw']),-169.2)
            self.assertAlmostEqual(sum(analysis.histograms['other_down_shape']['sumw']),-183.6)
        old=object.__new__(Analysis);old.native=SimpleNamespace()
        with self.assertRaisesRegex(RuntimeError,'rebuild rdfanalyzer'):
            old.correction_batch('corr','sf',[['up']],outputs=['up'])

    def test_vector_batch_preserves_broadcasting_empty_collections_and_float_output(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            jets=ak.values_astype(ak.Array([[24.,25.],[26.],[]]),np.float32)
            with uproot.recreate(source) as output:output.mktree('Events',{'Jet_pt':jets,'shift':jets*2.})
            results=[]
            for batch in (False,True):
                analysis=configured(root,source,'vector'+str(batch),['Jet_pt','offset'])
                analysis.define_nominal('offset','.5').register_variation('Jet_pt','shape','shift')
                if batch:analysis.correction_batch('corr','sf',[['up'],['down']],outputs=['up','down'],vector=True)
                else:
                    for label in ('up','down'):analysis.native.applyCorrectionVec('corr','sf',[label],[],label)
                dtype=analysis.native.column_type('up')
                self.assertIn('RVec<',dtype);self.assertNotIn('double',dtype)
                analysis.define('total','double(ROOT::VecOps::Sum(up))',['up']).sum('total','total')
                for label in ('Nominal','shape'):analysis.histogram(label,'total',10,0,100,variation=label)
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                results.append((analysis.counts,analysis.histograms,analysis.sums))
            # Legacy single-vector calls intentionally consume explicitly
            # resolved inputs; only explicit batches now expand input shifts.
            self.assertEqual(results[0][0],results[1][0])
            self.assertEqual(results[0][1]['Nominal'],results[1][1]['Nominal'])
            self.assertEqual(results[0][2]['total'],results[1][2]['total'])
            self.assertEqual(results[0][1]['shape'],results[0][1]['Nominal'])
            self.assertNotEqual(results[1][1]['shape'],results[1][1]['Nominal'])
            self.assertAlmostEqual(results[1][2]['total'],84.15,places=4)

    def test_expanded_destination_collisions_reject_before_mutation(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':np.array([2.,3.]),'shift':np.array([4.,6.])})
            cases=[(['sf','sf_shape'],None),(['sf_shape','sf'],None),
                   (['sf'],'existing'),(['sf','alias'],'alias'),(['sf'],'sf')]
            for vector in (False,True):
                for index,(outputs,mapping) in enumerate(cases):
                    analysis=configured(root,source,f'collision{vector}{index}',['pt','offset'])
                    analysis.define_nominal('offset','.5').register_variation('pt','shape','shift')
                    if mapping=='existing':analysis.define_nominal('sf_shape','99.')
                    elif mapping is not None:analysis.register_variation('sf','shape',mapping)
                    before=analysis.inspect();columns=set(analysis.columns)
                    with self.subTest(vector=vector,outputs=outputs,mapping=mapping),self.assertRaisesRegex(
                            ValueError,'already exists|destination collision|aliases nominal'):
                        analysis.correction_batch('corr','sf',[['up'] for _ in outputs],
                                                  outputs=outputs,vector=vector)
                    self.assertEqual(analysis.inspect(),before)
                    self.assertEqual(set(analysis.columns),columns)
                    self.assertEqual(analysis.native.event_loops,0)
            # Even an unchanged paired direction is a materialized destination.
            analysis=configured(root,source,'paired_collision',['pt','offset'])
            analysis.define_nominal('offset','.5').register_variation_pair('pt','scale','shift','pt')
            before=analysis.inspect()
            with self.assertRaisesRegex(ValueError,'destination collision'):
                analysis.correction_batch('corr','sf',[['up'],['down']],outputs=['sf','sf_scaleDown'])
            self.assertEqual(analysis.inspect(),before)

    def test_vector_batch_shift_values_mixed_flags_custom_inputs_and_disabled_systematics(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            jets=ak.values_astype(ak.Array([[1.,2.],[3.],[]]),np.float32)
            with uproot.recreate(source) as output:
                output.mktree('Events',{'Jet_pt':jets,'shift':jets*2.})
            for flags in ([False,True],[True,False]):
                analysis=configured(root,source,'flags'+str(flags[0]),['Jet_pt','offset'])
                analysis.define_nominal('offset','.5')
                analysis.register_variation('Jet_pt','shape','shift')
                analysis.register_variation_pair('Jet_pt','scale','shift','Jet_pt')
                analysis.correction_batch('corr','sf',[['up'],['down']],outputs=['up','down'],
                                          propagate_systematics=flags,vector=True)
                for name,factor,expand in zip(('up','down'),(1.1,.9),flags):
                    for suffix,multiplier in ([('',1.),('_shape',2.),('_scaleUp',2.),('_scaleDown',1.)] if expand else [('',1.)]):
                        column=name+suffix
                        self.assertTrue(analysis.has_column(column))
                        analysis.define_nominal('sum_'+column,'double(ROOT::VecOps::Sum('+column+'))',[column])
                        analysis.sum(column,'sum_'+column)
                    if not expand:
                        self.assertFalse(analysis.has_column(name+'_shape'))
                # Explicit shifted inputs remain shifted with propagation false.
                analysis.correction_batch('corr','sf',[['up']],outputs=['terminal'],
                    inputs=['shift','offset'],propagate_systematics=[False],vector=True)
                analysis.define_nominal('terminal_sum','double(ROOT::VecOps::Sum(terminal))',['terminal']).sum('terminal','terminal_sum')
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                for name,factor,expand in zip(('up','down'),(1.1,.9),flags):
                    for suffix,multiplier in ([('',1.),('_shape',2.),('_scaleUp',2.),('_scaleDown',1.)] if expand else [('',1.)]):
                        expected=sum(factor*(value*multiplier+.5) for value in (1.,2.,3.))
                        self.assertAlmostEqual(analysis.sums[name+suffix],expected,places=5)
                self.assertAlmostEqual(analysis.sums['terminal'],14.85,places=5)
            analysis=configured(root,source,'disabled',['Jet_pt','offset'],systematics=False)
            analysis.define_nominal('offset','.5').register_variation('Jet_pt','shape','shift')
            analysis.correction_batch('corr','sf',[['up']],outputs=['up'],vector=True)
            self.assertFalse(analysis.has_column('up_shape'))
            analysis.define_nominal('total','double(ROOT::VecOps::Sum(up))',['up']).sum('total','total')
            analysis.run();self.assertAlmostEqual(analysis.sums['total'],8.25,places=5)
            analysis=configured(root,source,'custom_output',['Jet_pt','offset'])
            analysis.define_nominal('offset','.5').register_variation('Jet_pt','shape','shift')
            analysis.register_variation('up','shape','custom_up')
            analysis.correction_batch('corr','sf',[['up']],outputs=['up'],vector=True)
            self.assertTrue(analysis.has_column('custom_up'))
            self.assertFalse(analysis.has_column('up_shape'))
            analysis.define_nominal('custom_total','double(ROOT::VecOps::Sum(custom_up))',['custom_up'])
            analysis.sum('custom','custom_total').run()
            self.assertAlmostEqual(analysis.sums['custom'],14.85,places=5)

    def test_false_propagation_outputs_internal_names_and_raw_idempotence(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);source=root/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':np.array([2.,3.]),'shift':np.array([4.,6.])})
            analysis=configured(root,source,'names',['pt','offset'])
            analysis.define_nominal('offset','.5').register_variation('pt','shape','shift')
            analysis.correction_batch('corr','sf',[['up'],['down']],outputs=['sf','sf_shape'],
                                      propagate_systematics=[False,False])
            analysis.correction_batch('corr','sf',[['up']],outputs=['__rdf_correction_input_1'],
                                      propagate_systematics=[False])
            # Also protect a generated pack name from becoming a user output.
            analysis.correction_batch('corr','sf',[['down']],outputs=['__rdf_correction_input_3_shape'],
                                      propagate_systematics=[False])
            columns=set(analysis.columns)
            analysis.native.applyCorrections('corr','sf',[['up'],['down']],[],['sf','sf_shape'],[False,False])
            self.assertEqual(set(analysis.columns),columns)
            for column in ('sf','sf_shape','__rdf_correction_input_1','__rdf_correction_input_3_shape'):
                analysis.sum(column,column)
            analysis.run();self.assertEqual(analysis.native.event_loops,1)
            self.assertAlmostEqual(analysis.sums['sf'],6.6)
            self.assertAlmostEqual(analysis.sums['sf_shape'],5.4)
            self.assertAlmostEqual(analysis.sums['__rdf_correction_input_1'],6.6)
            self.assertAlmostEqual(analysis.sums['__rdf_correction_input_3_shape'],5.4)


if __name__=='__main__':unittest.main()
