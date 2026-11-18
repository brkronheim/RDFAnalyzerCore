"""Compare declaration lowering with the immediate native-backed authoring path."""
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
sys.path.insert(0,os.environ.get('RDFANALYZER_PYTHON_MODULE_DIR',str(REPO/'build/python')))
from rdfanalysis import Analysis,AnalysisConfig,InputSettings,SkimSettings,Stage,HistogramGroup,Observable,RegularAxis
from rdfanalysis import CollectionRef,KernelOutputs


class DeclarationTests(unittest.TestCase):
    def test_optional_declaration_sources_preserve_native_graph_and_outputs(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',dict(value=np.array([.5,1.5,-.5]),accepted=np.array([True,True,False])))
            previous_graph=None;previous_histograms=None
            for enabled in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{enabled}.root'),skim=False))
                stage=Stage('located',record_sources=enabled).filter_column('cut','accepted').count('selected').histogram('value','value',2,0,2)
                stage.validate(analysis,strict=True).raise_if_invalid()
                self.assertEqual(analysis.native.event_loops,0)
                self.assertFalse(stage.describe().as_dict()['details']['sealed'])
                analysis.apply(stage)
                graph=analysis.native._graph_description()
                if previous_graph is None:previous_graph=graph
                else:self.assertEqual(graph,previous_graph)
                self.assertEqual('declaration_sources' in analysis.inspect()['stages'][0],enabled)
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();self.assertEqual(analysis.native.event_loops,1)
                self.assertEqual(analysis.counts['selected'],2)
                if previous_histograms is None:previous_histograms=analysis.histograms
                else:self.assertEqual(analysis.histograms,previous_histograms)
            broken=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/'broken.root'),skim=False))
            stage=Stage('broken',record_sources=True).project('unknown','missing',0)
            declared=stage.describe().as_dict()['details']['steps'][0]['source']
            with self.assertRaisesRegex(RuntimeError,'declared at') as caught:broken.apply(stage)
            self.assertIn(f"{declared['file']}:{declared['line']}",str(caught.exception))
            self.assertEqual(broken.native.event_loops,0)
            with self.assertRaisesRegex(RuntimeError,'fresh Analysis'):broken.run()

    @unittest.skipUnless(importlib.util.find_spec('numba'),'optional Numba unavailable')
    def test_named_outputs_preserve_types_graph_cache_and_variation_dependencies(self):
        import numpy as np
        import uproot
        def single(value):return np.array([value,value*2.],dtype=np.float32)
        def double(value):return np.array([value,value*2.],dtype=np.float64)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:
                values=np.array([-2.,-1.,0.,1.,2.,3.])
                output.mktree('Events',dict(value=values,up=values+1.,down=values-1.,replica=values*1.1))
            for kernel,return_type,scalar in ((single,'RVec<float>','float'),(double,'RVec<double>','double')):
                expected_graph=None;expected_output=None
                outputs=KernelOutputs('summary',('accepted','observable'))
                nominal=KernelOutputs('nominal_summary',('value',),nominal=True)
                jets=CollectionRef('objects',{'pt':'value'},alignment='input-order')
                for mode in ('raw','immediate','staged','split'):
                    analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{scalar}-{mode}.root'),skim=False))
                    analysis.register_variation_pair(jets['pt'],'shape','up','down').register_variation(jets['pt'],'replica','replica')
                    target=Stage('selected') if mode in ('staged','split') else analysis
                    if mode=='raw':target.define_numba('summary',kernel,[jets['pt']],return_type=return_type)
                    else:target.define_numba_outputs(outputs,kernel,[jets['pt']],return_type=return_type)
                    if mode=='raw':target.project('accepted','summary',0)
                    else:target.project('accepted',outputs['accepted'])
                    target.filter('accepted','accepted>0.')
                    if mode=='split':
                        analysis.apply(target)
                        target=Stage('observables')
                    if mode=='raw':
                        target.project('observable','summary',1)
                        target.define_numba('nominal_summary',kernel,[jets['pt']],return_type=return_type,nominal=True)
                        target.project('nominal_value','nominal_summary',0,nominal=True)
                    else:
                        target.project('observable',outputs['observable'])
                        target.define_numba_outputs(nominal,kernel,[jets['pt']],return_type=return_type)
                        target.project('nominal_value',nominal['value'])
                    # Sum<double> requires an explicit conversion for float fields.
                    target.define_nominal('sum_double','double(observable)')
                    target.count('selected').sum('observable_sum','sum_double').histogram('nominal','nominal_value',8,-4,4)
                    for label in analysis.variation_labels:
                        target.histogram('obs/'+label,'observable',12,-4,8,variation=label)
                    if mode in ('staged','split'):analysis.apply(target)
                    self.assertEqual(analysis.native.column_type('observable'),scalar)
                    self.assertFalse(analysis.has_column('nominal_summary_shapeUp'))
                    for label in ('shapeUp','shapeDown','replica'):
                        self.assertTrue(analysis.has_column('observable_'+label))
                    graph=analysis.native._graph_description()
                    if expected_graph is None:expected_graph=graph
                    else:self.assertEqual(graph,expected_graph)
                    if mode!='raw':
                        self.assertEqual(analysis.compilation_stats['numba_kernels_compiled'],0)
                        self.assertEqual(analysis.compilation_stats['numba_cache_hits'],2)
                        self.assertEqual(analysis.inspect()['kernel_outputs']['summary']['fields'],{'accepted':0,'observable':1})
                    self.assertEqual(analysis.native.event_loops,0)
                    analysis.run()
                    actual=dict(counts=analysis.counts,sums=analysis.sums,histograms=analysis.histograms)
                    self.assertEqual(analysis.native.event_loops,1)
                    if expected_output is None:expected_output=actual
                    else:self.assertEqual(actual,expected_output)
                    # Count sees the union retained by the shifted selections.
                    self.assertEqual(analysis.counts['selected'],4)
                    self.assertEqual(analysis.sums['observable_sum'],12.)

    @unittest.skipUnless(importlib.util.find_spec('numba'),'optional Numba unavailable')
    def test_named_projection_checks_empty_vectors_at_execution(self):
        import numpy as np
        import uproot
        def empty(value):return np.empty(0,dtype=np.float64)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/'output.root'),skim=False))
            outputs=KernelOutputs('empty',('first',))
            analysis.define_numba_outputs(outputs,empty,['value'],return_type='RVec<double>')
            analysis.project('first',outputs['first']).sum('value','first')
            with self.assertRaisesRegex(Exception,'out of range|out of bounds|projection|Projection'):
                analysis.run()

    @unittest.skipUnless(importlib.util.find_spec('numba'),'optional Numba unavailable')
    def test_interleaved_filters_and_definitions_retain_order_and_gating(self):
        import numpy as np
        import uproot
        def guarded(value):
            if value<0:raise ValueError('a filtered negative row reached the kernel')
            return value+1.
        recipe=Stage('interleaved').define('original','value*1.').filter('positive','original>0.')
        recipe.define_numba('guarded',guarded,['original'],return_type='double')
        recipe.filter('second','guarded<4.').define('squared','guarded*guarded').count('selected').sum('total','squared')
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([-2.,-1.,0.,1.,2.,3.])})
            graphs=[];outputs=[]
            for staged in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{staged}.root'),skim=False))
                analysis.count('input')
                if staged:analysis.apply(recipe)
                else:
                    analysis.define('original','value*1.').filter('positive','original>0.')
                    analysis.define_numba('guarded',guarded,['original'],return_type='double')
                    analysis.filter('second','guarded<4.').define('squared','guarded*guarded').count('selected').sum('total','squared')
                graph=analysis.native._graph_description();graphs.append(graph)
                by_name={item['name']:item for item in graph['operations']}
                self.assertEqual([by_name[name]['stage'] for name in ('original','guarded','squared')],[0,1,2])
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run();outputs.append((analysis.counts,analysis.sums,analysis.native.event_loops))
                self.assertEqual(analysis.counts,{'input':6,'selected':2})
                self.assertEqual(analysis.sums['total'],13.)
            self.assertEqual(graphs[0],graphs[1]);self.assertEqual(outputs[0],outputs[1])

    def test_typed_paths_constants_aliases_and_skim_settings_execute(self):
        import numpy as np
        import uproot
        import ROOT
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([20.,30.])})
            (directory/'floats.txt').write_text('threshold=25.0\n')
            (directory/'aliases.txt').write_text('existingName=value newName=aliased\n')
            (directory/'analysis.yaml').write_text('files: [input.root]\noutput: selected.root\n'
                'save_columns: [value, threshold]\n'
                'input_settings: {float_constants: floats.txt, aliases: aliases.txt, cache_statistics: false}\n'
                'skim_settings: {tree: Selected, metadata_file: custom.root, compression_algorithm: zlib, compression_level: 1}\n')
            config=AnalysisConfig.from_file(directory/'analysis.yaml')
            analysis=Analysis(config)
            self.assertTrue(analysis.has_column('value'))
            self.assertFalse(analysis.has_column('not_a_column'))
            self.assertEqual(analysis.native.column_type('threshold'),'float')
            analysis.define('shifted','aliased+double(threshold)').sum('total','shifted').run()
            self.assertEqual(analysis.sums['total'],100.)
            self.assertTrue((directory/'custom.root').is_file())
            with uproot.open(directory/'selected.root',handler=uproot.source.file.MultithreadedFileSource,num_workers=1,
                             decompression_executor=uproot.source.futures.TrivialExecutor(),
                             interpretation_executor=uproot.source.futures.TrivialExecutor()) as output:
                np.testing.assert_array_equal(output['Selected']['value'].array(library='np'),[20.,30.])
                np.testing.assert_array_equal(output['Selected']['threshold'].array(library='np'),[25.,25.])
            output=ROOT.TFile.Open(str(directory/'selected.root'))
            self.assertEqual(output.GetCompressionSettings(),101);output.Close()
            self.assertEqual(analysis.native.event_loops,1)

    @unittest.skipUnless(importlib.util.find_spec('numba'),'optional Numba unavailable')
    def test_stages_match_immediate_graph_migrations_weights_flows_and_persistence(self):
        import numpy as np
        import uproot
        def pack(pt):return np.array([pt,2.*pt],dtype=np.float64)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            arrays={key:np.array(value,dtype='float64') for key,value in {
                'pt':[19,24,26,35,90],'up':[21,26,28,37,110],'down':[17,22,24,33,80],
                'weight':[-2,3,-1,2,1.5],'wup':[-1,4,0,3,2.5],'wdown':[-3,2,-2,1,.5]}.items()}
            with uproot.recreate(source) as output:output.mktree('Events',arrays)
            group=HistogramGroup('kinematics',(Observable('twice_pt','obs',RegularAxis(5,50,150)),),
                                 regions=('base','signal'),variations='all',weight='event_weight')
            recipe=Stage('selected').define_numba('packed',pack,['pt'],return_type='RVec<double>')
            recipe.project('obs','packed',1).define('pass_base','obs>40.').define('pass_signal','obs>60.')
            recipe.filter('global','obs>50.').region('signal','pass_signal',parent='base').region('base','pass_base')
            recipe.count('union').histograms(group)
            payloads=[];graphs=[]
            for staged in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{staged}.root'),
                                                skim=False,persist_histograms=True))
                if staged:
                    analysis.apply(Stage('input').register_variation_pair('pt','jes','up','down').count('input'))
                else:analysis.register_variation_pair('pt','jes','up','down').count('input')
                analysis.weights({'generator':'weight'},variations={'norm':('generator','wup','wdown')})
                if staged:
                    before=analysis.columns;stats=analysis.compilation_stats
                    preview=recipe.preview(analysis).as_dict()['details']
                    self.assertEqual(preview['region_order'],['base','signal'])
                    self.assertEqual(len(preview['actions']),11)
                    self.assertEqual(analysis.columns,before)
                    self.assertEqual(analysis.compilation_stats,stats)
                    analysis.apply(recipe)
                    with self.assertRaisesRegex(ValueError,'duplicate stage'):analysis.apply(recipe)
                    with self.assertRaisesRegex(RuntimeError,'sealed'):recipe.count('late')
                    self.assertEqual(analysis.inspect()['stages'][-1]['histograms'],10)
                else:
                    analysis.define_numba('packed',pack,['pt'],return_type='RVec<double>')
                    analysis.project('obs','packed',1).define('pass_base','obs>40.').define('pass_signal','obs>60.')
                    analysis.filter('global','obs>50.').region('base','pass_base').region('signal','pass_signal',parent='base')
                    analysis.count('union')
                    for region in ('base','signal'):
                        for label in analysis.variation_labels:
                            analysis.histogram(f'kinematics/{region}/{label}/twice_pt','obs',5,50,150,
                                               region=region,variation=label,weight='event_weight')
                self.assertEqual(analysis.native.event_loops,0)
                graphs.append((sorted(analysis.columns),analysis.native._graph_description()))
                analysis.run()
                payloads.append((analysis.counts,analysis.histograms,analysis.native.event_loops))
                self.assertEqual(analysis.counts['input'],5)
                self.assertEqual(analysis.counts['union'],4)
                self.assertEqual(analysis.counts['kinematics/base/Nominal/twice_pt'],3)
                self.assertEqual(analysis.counts['kinematics/base/jesUp/twice_pt'],4)
                self.assertEqual(analysis.histograms['kinematics/base/Nominal/twice_pt']['sumw'][-1],1.5)
                self.assertEqual(analysis.histograms['kinematics/base/Nominal/twice_pt']['sumw'][1],-1.)
                with uproot.open(analysis.config.native_config()['metaFile']) as output:
                    for name,histogram in analysis.histograms.items():
                        np.testing.assert_allclose(output['python_histograms/'+name].values(flow=True),histogram['sumw'])
                        np.testing.assert_allclose(output['python_histograms/'+name].variances(flow=True),histogram['sumw2'])
            self.assertEqual(graphs[0],graphs[1])
            self.assertEqual(payloads[0],payloads[1])

    def test_preflight_rejects_expansion_hierarchy_duplicates_and_stage_reordering(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.,2.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(temporary)/'out.root'),skim=False))
            group=HistogramGroup('group',(Observable('value','value',RegularAxis(2,0,3)),),variations='all')
            excessive=Stage('huge',max_histograms=2).register_variation_pair('value','shape','value','value').define('new','value')
            excessive.histograms(group)
            with self.assertRaisesRegex(ValueError,'budget'):analysis.apply(excessive)
            self.assertNotIn('new',analysis.columns)
            self.assertEqual(analysis.variation_labels,['Nominal'])
            cyclic=Stage('cycle').define('yes','true').region('a','yes',parent='b').region('b','yes',parent='a')
            with self.assertRaisesRegex(ValueError,'cyclic'):analysis.apply(cyclic)
            self.assertNotIn('yes',analysis.columns)
            duplicate=Stage('duplicates').histograms(group).histograms(group)
            with self.assertRaisesRegex(ValueError,'duplicate'):analysis.apply(duplicate)
            with self.assertRaisesRegex(ValueError,'phases'):Stage('order').count('before').filter('after','true')
            self.assertFalse(analysis._stage_failure)
            analysis.count('still_valid').run();self.assertEqual(analysis.counts['still_valid'],2)

    def test_native_lowering_failure_blocks_accidental_partial_execution(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.,2.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(temporary)/'out.root'),skim=False))
            group=HistogramGroup('group',(Observable('value','value',RegularAxis(2,0,3)),),variations=('unknown',))
            bad=Stage('bad').define('new','value+1.').histograms(group)
            with self.assertRaisesRegex(RuntimeError,'unknown variations') as caught:analysis.apply(bad)
            self.assertIsInstance(caught.exception.__cause__,ValueError)
            self.assertIn('new',analysis.columns)
            self.assertEqual(analysis._action_names['histograms'],set())
            self.assertEqual(analysis.native.event_loops,0)
            with self.assertRaisesRegex(RuntimeError,'fresh Analysis'):analysis.run()

    def test_native_discovered_labels_recheck_expansion_before_booking(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{name:np.array([1.,2.]) for name in ('pt','pt_shapeUp','pt_shapeDown')})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(temporary)/'out.root'),skim=False))
            group=HistogramGroup('group',(Observable('pt','derived',RegularAxis(2,0,3)),),variations='all')
            stage=Stage('discovery',max_histograms=2).define('derived','pt').histograms(group)
            self.assertEqual(len(stage.preview(analysis).as_dict()['details']['actions']),1)
            with self.assertRaisesRegex(RuntimeError,'budget'):analysis.apply(stage)
            self.assertIn('shapeUp',analysis.variation_labels)
            self.assertEqual(analysis._action_names['histograms'],set())
            self.assertEqual(analysis.native.event_loops,0)

    def test_region_base_capture_limitation_is_reported_before_new_definitions(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.,2.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(temporary)/'out.root'),skim=False))
            analysis.define('base','value>0.').region('base','base')
            late=Stage('late').define('signal','value>1.').region('signal','signal',parent='base')
            with self.assertRaisesRegex(ValueError,'first native region'):analysis.apply(late)
            self.assertNotIn('signal',analysis.columns)
            analysis.count('valid').run();self.assertEqual(analysis.counts['valid'],2)


if __name__=='__main__':unittest.main()
