"""Exercise optional contracts through the real native graph and lifecycle."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
sys.path.insert(0,os.environ.get('RDFANALYZER_PYTHON_MODULE_DIR',str(REPO/'build/python')))
from rdfanalysis import Analysis,AnalysisConfig,InputSettings,ActionClaim,AnalysisModule,ModuleSpec,ModuleSequence,Stage


def fixture(directory):
    import numpy as np
    import uproot
    path=directory/'input.root'
    with uproot.recreate(path) as output:output.mktree('Events',{'value':np.array([1.,2.])})
    return Analysis(AnalysisConfig(files=[str(path)],output=str(directory/'output.root'),skim=False,
                                   input_settings=InputSettings(cache_statistics=False)))


class NativeModuleTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec('numba'),'optional Numba unavailable')
    def test_external_numba_modules_preserve_graph_gating_types_shapes_and_signed_outputs(self):
        import awkward as ak
        import numpy as np
        import uproot
        from optional_extensions import load_extension_module
        with patch.dict(os.environ,{'RDF_EXTENSION_PATHS':str(REPO/'examples/authoring-extension'),
                                     'RDF_BUILD_EXTENSIONS':'ON','RDF_ENABLE_EXAMPLE_AUTHORING':'ON'}):
            provider=load_extension_module('example_authoring','modules',required=True)
        def guarded_score(ht,flag):
            if flag<0:raise ValueError('a module moved this kernel before preselection')
            return ht+flag
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            pt=ak.Array([[],[10.,40.],[20.,35.],[50.,20.],[15.,25.,45.],[90.]])
            weight=np.array([-1.,2.,1.,-2.,3.,1.])
            with uproot.recreate(source) as output:
                output.mktree('Events',dict(Jet_pt=pt,shift_up=pt+10.,shift_down=pt-20.,replica=pt*1.1,
                    flag=np.array([0,-1,1,2,3,4],dtype=np.int32),jet_pt_min=np.full(6,30.),
                    weight=weight,wup=weight*1.05,wdown=weight*.95))
            expected_graph=None;expected_physics=None;expected_types=None
            for use_modules in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{use_modules}.root'),skim=False,
                                                 input_settings=InputSettings(cache_statistics=False)))
                analysis.count('input').register_variation_pair('Jet_pt','shape','shift_up','shift_down')
                analysis.register_variation('Jet_pt','replica','replica')
                analysis.weights({'generator':'weight'},variations={'norm':('generator','wup','wdown')})
                analysis.filter('preselection','flag>=0')
                labels=analysis.variation_labels
                def selection(analysis,settings):
                    stage=Stage('selection').filter('ht_cut','jet_ht>30.')
                    stage.define_numba('score',guarded_score,['jet_ht','flag'],return_type='double')
                    stage.define('score_squared','score*score').filter('score_cut','score<80.')
                    stage.count('selected').sum('score_sum','score')
                    for label in labels:stage.histogram('score/'+label,'score_squared',10,0,5000,weight='event_weight',variation=label)
                    analysis.apply(stage)
                ht=provider.make_ht_module()
                selected=AnalysisModule(ModuleSpec('analysis.selection',requires=('jet_ht','flag'),
                    produces=('score','score_squared'),dependencies=('example.ht',),resources=('selection.acceptance',),
                    actions=(ActionClaim('count','selected'),ActionClaim('sum','score_sum'),
                             *(ActionClaim('histogram','score/'+label) for label in labels))),selection)
                if use_modules:
                    sequence=ModuleSequence((ht,selected));before=analysis.native._graph_description()
                    sequence.preview(analysis);json.dumps(sequence.describe().as_dict())
                    self.assertEqual(before,analysis.native._graph_description())
                    self.assertEqual(analysis.native.event_loops,0)
                    analysis.use(ht,selected)
                    self.assertEqual(analysis.compilation_stats['numba_kernels_compiled'],0)
                    self.assertEqual(analysis.compilation_stats['numba_cache_hits'],2)
                    inventory=analysis.inspect()['modules']
                    self.assertEqual(list(inventory),['example.ht','analysis.selection'])
                    inventory['example.ht']['settings']['output']='changed'
                    self.assertEqual(analysis.inspect()['modules']['example.ht']['settings']['output'],'jet_ht')
                else:
                    analysis.define_numba('jet_ht',provider.sum_above_threshold,['Jet_pt','jet_pt_min'],return_type='double')
                    selection(analysis,{})
                graph=analysis.native._graph_description()
                types={column:analysis.native.column_type(column) for column in analysis.columns}
                if expected_graph is None:expected_graph=graph;expected_types=types
                else:self.assertEqual(graph,expected_graph);self.assertEqual(types,expected_types)
                self.assertEqual(types['jet_ht'],'double')
                self.assertTrue(analysis.has_column('score_shapeUp'));self.assertTrue(analysis.has_column('score_replica'))
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                physics=dict(counts=analysis.counts,histograms=analysis.histograms,sums=analysis.sums)
                if expected_physics is None:expected_physics=physics
                else:self.assertEqual(physics,expected_physics)
                self.assertEqual(analysis.counts['input'],6)
                self.assertEqual(analysis.counts['selected'],4)
                self.assertEqual(analysis.sums['score_sum'],136.)

    def test_preflight_rejects_batch_conflicts_without_touching_a_real_graph(self):
        with tempfile.TemporaryDirectory() as temporary:
            analysis=fixture(Path(temporary));analysis.count('input')
            before=analysis.native._graph_description();columns=list(analysis.columns);calls=[]
            def build(analysis,settings):calls.append('called');analysis.define('work','value*2.')
            first=AnalysisModule(ModuleSpec('first',requires=('value',),produces=('work',),resources=('correction.jet',)),build)
            second=AnalysisModule(ModuleSpec('second',resources=('correction.jet',)),build)
            with self.assertRaisesRegex(ValueError,'resource'):analysis.use(first,second)
            self.assertFalse(calls);self.assertIsNone(analysis._module_failure)
            self.assertEqual(before,analysis.native._graph_description());self.assertEqual(columns,list(analysis.columns))
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run();self.assertEqual(analysis.counts['input'],2)

    def test_failed_builder_and_attempted_frontend_run_cannot_execute_partial_graphs(self):
        def broken(analysis,settings):
            analysis.define('partial','value*2.')
            raise ValueError('deliberate partial failure')
        for builder in (broken,lambda analysis,settings:analysis.run()):
            with self.subTest(builder=builder),tempfile.TemporaryDirectory() as temporary:
                analysis=fixture(Path(temporary));analysis.count('input')
                with self.assertRaisesRegex(RuntimeError,'fresh Analysis'):
                    analysis.use(AnalysisModule(ModuleSpec('bad'),builder))
                with self.assertRaisesRegex(RuntimeError,'module failed'):analysis.run()
                self.assertEqual(analysis.native.event_loops,0)
                self.assertIsNone(analysis._module_active)

    def test_native_plugins_can_have_optional_metadata_or_remain_unannotated(self):
        with tempfile.TemporaryDirectory() as temporary:
            analysis=fixture(Path(temporary));analysis.count('input')
            config=Path(temporary)/'bdt.cfg';config.write_text('')
            analysis.native.setConfig('bdtConfig',str(config))
            analysis.native.AddPlugin('opaque_bdt','BDTManager')
            def add_native(analysis,settings):analysis.native.AddPlugin('described_bdt','BDTManager')
            module=AnalysisModule(ModuleSpec('external.native_bdt',implementation='native-manager',
                outputs_at='native-lifecycle',resources=('plugin.described_bdt',),capabilities=('bdt',)),add_native)
            analysis.use(module)
            with self.assertRaisesRegex(ValueError,'duplicate module'):analysis.use(module)
            self.assertEqual(analysis.native.getBDTNames('described_bdt'),[])
            self.assertEqual(analysis.native.getBDTNames('opaque_bdt'),[])
            description=analysis.describe().as_dict()['details']
            self.assertEqual(list(description['inventory']['modules']),['external.native_bdt'])
            self.assertIn('opaque',description['graph_completeness'])
            analysis.run();self.assertEqual(analysis.counts['input'],2)


if __name__=='__main__':
    import rdfanalyzer
    import rdfanalysis
    print('Native:',rdfanalyzer.__file__,'Frontend:',rdfanalysis.__file__,flush=True)
    unittest.main()
