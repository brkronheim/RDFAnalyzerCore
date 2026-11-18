"""Execute inferred/explicit Python graphs and check variation migrations."""
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
import importlib.util

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / "core/python"))
sys.path.insert(0, os.environ.get("RDFANALYZER_PYTHON_MODULE_DIR", str(REPO / "build/python")))
from rdfanalysis import Analysis, AnalysisConfig


class ConfigurationTests(unittest.TestCase):
    def test_typed_entry_ranges_validate_before_input_access(self):
        base={'files':['nonexistent.root'],'output':'unused.root'}
        config=AnalysisConfig(**base,entry_range=[2**32,2**32+3],range_mode='source')
        self.assertEqual(config.entry_range,(2**32,2**32+3))
        self.assertEqual(config.native_config()['firstEntry'],str(2**32))
        self.assertEqual(config.native_config()['lastEntry'],str(2**32+3))
        self.assertEqual(config.native_config()['entryRangeMode'],'source')
        # Legacy native options remain usable when the typed range is absent.
        legacy=AnalysisConfig(**base,options={'firstEntry':'3','lastEntry':'6','entryRangeMode':'source'})
        self.assertEqual(legacy.native_config()['entryRangeMode'],'source')
        for kwargs in ({'entry_range':[-1,2]},{'entry_range':[3,3]},{'entry_range':[True,3]},
            {'entry_range':[0,2**63]},{'entry_range':[0,2**32]},
            {'entry_range':[0,2],'threads':2},{'range_mode':'source'},
            {'entry_range':[0,2],'options':{'firstEntry':'1'}}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):AnalysisConfig(**base,**kwargs)

    def test_stale_native_module_fails_before_input_access(self):
        from types import SimpleNamespace
        from unittest.mock import patch
        for old in (SimpleNamespace(),SimpleNamespace(python_authoring_api_version=3)):
            with patch.dict(sys.modules,{'rdfanalyzer':old}):
                with self.assertRaisesRegex(RuntimeError,'older than this Python frontend'):
                    Analysis(AnalysisConfig(files=['nonexistent.root'],output='unused.root',
                        entry_range=(2**32,2**32+3),range_mode='source'))

    def test_invalid_settings_fail_before_native_import(self):
        for change in ({"files": "input.root"}, {"threads": True}, {"threads": 0},
                       {"systematics": "false"}, {"save_columns": "pt"},
                       {"persist_histograms":"true"},
                       {"options": {"enableSystematics": "false"}}):
            with self.subTest(change=change), self.assertRaises(ValueError):
                AnalysisConfig(**{**{"files": ["input.root"], "output": "out.root"}, **change})

    def test_file_relative_paths_and_unknown_fields(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "analysis.yaml"
            path.write_text("files: [input.root]\noutput: out.root\n")
            config = AnalysisConfig.from_file(path)
            self.assertEqual(config.files, (str(path.parent / "input.root"),))
            path.write_text("files: [input.root]\noutput: out.root\nthreds: 4\n")
            with self.assertRaisesRegex(ValueError, "threds"):
                AnalysisConfig.from_file(path)


class AuthoringTests(unittest.TestCase):
    @unittest.skipUnless(importlib.util.find_spec('numba'), 'optional Numba is not installed')
    def test_systematic_inference_tutorial_signed_shapes_and_native_bdt(self):
        # Queued native gate; the lightweight provider suite never invokes it.
        import runpy
        import shutil
        import uproot
        import numpy as np
        tutorial_dir = REPO/'examples/systematics-inference'
        tutorial = runpy.run_path(str(tutorial_dir/'analysis.py'))
        oracle = tutorial['contract']()
        with tempfile.TemporaryDirectory(prefix='rdf_inference_test_') as temporary:
            directory = Path(temporary); source = directory/'input.root'
            tutorial['make_input'](source)
            model = directory/'model.txt'; shutil.copyfile(tutorial_dir/'toy_bdt.txt', model)
            models = directory/'models.txt'
            models.write_text(f'file={model} name=score inputVariables=ml_feature runVar=run_model\n')
            config = tutorial['config'](source, directory/'output.root', models)
            analysis = tutorial['build'](Analysis(config))
            self.assertEqual(analysis.native.event_loops, 0)
            self.assertEqual(set(analysis.variation_labels), set(tutorial['LABELS']))
            analysis.run(); result = analysis.result().as_dict()
            self.assertEqual(result['counts']['input'], 3)
            self.assertEqual(result['counts']['selected_union'], 3)
            self.assertEqual(result['event_loops'], 1)
            self.assertEqual(len(result['histograms']), 10)
            with uproot.open(config.native_config()['metaFile'],
                    handler=uproot.source.file.MultithreadedFileSource, num_workers=1) as output:
                for label, count, inner, variance in zip(tutorial['LABELS'],
                        oracle['per_label_selected'], oracle['score_sumw_inner'], oracle['score_sumw2_inner']):
                    values, variances = [0.]+inner+[0.], [0.]+variance+[0.]
                    self.assertEqual(result['histograms'][label+'/score']['sumw'], values)
                    # The binding reports GetBinError() squared, which can
                    # round by a few ulps relative to stored ROOT Sumw2.
                    np.testing.assert_allclose(
                        result['histograms'][label+'/score']['sumw2'], variances,
                        rtol=1e-14, atol=0.)
                    self.assertEqual(sum(result['histograms'][label+'/acceptance']['sumw']), count)
                    persisted = output['python_histograms/'+label+'/score']
                    self.assertEqual(persisted.values(flow=True).tolist(), values)
                    self.assertEqual(persisted.variances(flow=True).tolist(), variances)
            self.assertEqual(analysis.native.event_loops, 1)

    @unittest.skipUnless(importlib.util.find_spec('numba'), 'optional Numba is not installed')
    def test_python_tutorial_preserves_expected_outputs_and_single_loop(self):
        # Queued native acceptance; lightweight typing tests never invoke this.
        import runpy
        import uproot
        from rdfanalysis import InputSettings
        tutorial = runpy.run_path(str(REPO/'examples/python-authoring/analysis.py'))
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary); source = directory/'input.root'
            tutorial['make_input'](source)
            config = AnalysisConfig(files=[str(source)], output=str(directory/'output.root'),
                skim=False, systematics=False, persist_histograms=True,
                input_settings=InputSettings(cache_enabled=False))
            analysis = tutorial['build'](Analysis(config))
            self.assertEqual(analysis.native.event_loops, 0)
            analysis.run(); result = analysis.result()
            self.assertEqual(result.counts['input'], 3)
            self.assertEqual(result.counts['selected'], 1)
            self.assertEqual(result.sums['total_ht'], 90.)
            self.assertEqual(result.event_loops, 1)
            with uproot.open(config.native_config()['metaFile'],
                    handler=uproot.source.file.MultithreadedFileSource, num_workers=1) as output:
                for name, index in (('ht', 5), ('double_ht', 10)):
                    expected = [0.]*12; expected[index] = 1.
                    self.assertEqual(result.as_dict()['histograms'][name]['sumw'], expected)
                    self.assertEqual(result.as_dict()['histograms'][name]['sumw2'], expected)
                    histogram = output['python_histograms/'+name]
                    self.assertEqual(histogram.values(flow=True).tolist(), expected)
                    self.assertEqual(histogram.variances(flow=True).tolist(), expected)
            self.assertEqual(analysis.native.event_loops, 1)

    def test_projection_accepts_root_vector_typedefs_without_changing_precision(self):
        import awkward as ak
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            values=ak.Array([[1.,2.],[3.,4.]])
            with uproot.recreate(source) as output:
                output.mktree('Events',{'doubles':values,'floats':ak.values_astype(values,np.float32),
                                       'integers':ak.values_astype(values,np.int32)})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/'out.root'),skim=False))
            self.assertIn('Double_t',analysis.native.column_type('doubles'))
            analysis.project('double_element','doubles',1).project('float_element','floats',0)
            self.assertEqual(analysis.native.column_type('double_element'),'double')
            self.assertEqual(analysis.native.column_type('float_element'),'float')
            with self.assertRaisesRegex(ValueError,'requires RVec'):analysis.project('bad','integers',0)
            analysis.sum('total','double_element').histogram('float','float_element',4,0,4).run()
            self.assertEqual(analysis.sums['total'],6.)
            self.assertEqual(analysis.counts['float'],2)
            snapshot = analysis.result()
            self.assertEqual(dict(snapshot.sums), dict(analysis.sums))
            self.assertEqual(dict(snapshot.counts), dict(analysis.counts))
            self.assertEqual(snapshot.as_dict()['histograms'], analysis.histograms)
            self.assertEqual(snapshot.event_loops, 1)
            self.assertEqual(analysis.native.event_loops, 1)
            with self.assertRaises(TypeError):
                snapshot.counts['float'] = 999

    def test_recording_opt_out_preserves_outputs_and_action_inventory(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':np.array([24.,26.,35.]),'up':np.array([26.,28.,37.])})
            results=[]
            for recording in (False,True):
                analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/f'{recording}.root'),skim=False),
                                  record_graph=recording)
                analysis.register_variation('pt','shape','up').define('value','pt*2.')
                analysis.count('input').filter('cut','value>50.')
                analysis.histogram('nominal','value',4,40,80).histogram('shifted','value',4,40,80,variation='shape')
                description=analysis.describe().as_dict()
                graph=description['details']['graph']
                self.assertEqual(graph['enabled'],recording)
                if recording:
                    self.assertTrue(graph['operations'])
                    self.assertIn('recompute value_shape',analysis.explain('shifted'))
                else:
                    self.assertEqual(graph['operations'],[])
                    self.assertIn('value',description['details']['inventory']['columns'])
                    with self.assertRaisesRegex(RuntimeError,'recording is disabled'):analysis.explain()
                self.assertEqual(analysis.native.event_loops,0)
                analysis.run()
                results.append((analysis.counts,analysis.histograms,analysis.native.event_loops))
            self.assertEqual(results[0],results[1])

    def test_common_description_does_not_execute_booked_graph(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'value':np.array([-1.,1.,2.],dtype='float64')})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/'out.root'),skim=False))
            analysis.define('shifted','value+1.').filter('positive','shifted>1.')
            analysis.count('selected').histogram('shifted','shifted',4,0,4)
            self.assertEqual(analysis.native.event_loops,0)
            description=analysis.describe(include_types=True).as_dict()
            self.assertEqual(description['schema'],'rdf-workflow-description-v1')
            inventory=description['details']['inventory']
            self.assertIn('shifted',inventory['columns'])
            self.assertEqual(inventory['column_types']['shifted'],'double')
            self.assertIn('partial',description['details']['graph_completeness'])
            operations=description['details']['graph']['operations']
            self.assertEqual([(op['kind'],op['stage']) for op in operations],
                [('expression',0),('expression',0),('filter',1),('count',1),('histogram',1)])
            self.assertEqual(operations[0]['inputs'],['value'])
            self.assertIn('stage 1 histogram shifted',analysis.explain('shifted'))
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run()
            self.assertEqual(analysis.counts['selected'],2)
            self.assertEqual(analysis.native.event_loops,1)

    def test_explanation_preserves_stages_late_registration_and_opaque_boundaries(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':np.array([24.,26.,35.]),'shift':np.array([26.,28.,37.])})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(directory/'out.root'),skim=False))
            analysis.define('early','pt*2.').count('input')
            analysis.register_variation('pt','shape','shift')
            analysis.define('late','pt*2.').define_nominal('fixed','pt*2.')
            analysis.filter('cut','late>50.').count('union')
            analysis.histogram('nominal','late',4,40,80)
            analysis.histogram('shifted','late',4,40,80,variation='shape')
            analysis.filter('second','pt<30.').sum('after_second','late')
            graph=analysis.describe().as_dict()['details']['graph']
            by_name={op['name']:op for op in graph['operations']}
            self.assertEqual(by_name['early']['variations'],{'Nominal':'early'})
            self.assertEqual(by_name['late']['variations']['shape'],'late_shape')
            self.assertEqual(by_name['fixed']['variations']['shape'],'fixed')
            self.assertEqual([by_name[name]['stage'] for name in ('input','union','shifted','after_second')],[0,1,1,2])
            text=analysis.explain('early',variation='shape')
            self.assertIn('not registered at construction',text)
            self.assertIn('propagation explicitly disabled',analysis.explain('fixed',variation='shape'))
            text=analysis.explain('nominal',variation='shape')
            self.assertIn('nominal late',text)
            self.assertNotIn('recompute late_shape',text)
            self.assertIn('does not change this booked action',text)
            text=analysis.explain('shifted')
            self.assertIn('recompute late_shape',text)
            self.assertIn('inputs: shift',text)
            self.assertIn('recompute late_shape',analysis.explain('late_shape'))
            self.assertIn('stage 1 filter cut',text)
            self.assertNotIn('filter second',text)
            self.assertIn('no nominal mask',analysis.explain('union'))
            with self.assertRaisesRegex(ValueError,'Unknown variation'):analysis.explain('late',variation='typo')
            with self.assertRaisesRegex(ValueError,'No tracked operation'):analysis.explain('shift')
            # A detached description must not corrupt future descriptions.
            graph['operations'].clear()
            self.assertTrue(analysis.describe().as_dict()['details']['graph']['operations'])
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run()
            self.assertEqual(analysis.counts,{'input':3,'union':3,'nominal':2,'shifted':3})
            self.assertEqual(analysis.sums['after_second'],52.)
            self.assertEqual(analysis.native.event_loops,1)
            self.assertIn('opaque',analysis.describe().as_dict()['details']['graph']['stage_scope'])

    def test_typed_ranges_preserve_cross_file_and_64bit_entry_ids(self):
        import ROOT
        import numpy as np
        import uproot
        source_supported=64000<=ROOT.gROOT.GetVersionInt()<64100
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            files=[]
            for index in range(2):
                path=directory/f'input{index}.root';files.append(str(path))
                with uproot.recreate(path) as output:
                    output.mktree('Events',{'value':np.arange(index*4,index*4+4,dtype='float64')})
            for mode in (('filter','source') if source_supported else ('filter',)):
                with self.subTest(mode=mode):
                    analysis=Analysis(AnalysisConfig(files=files,output=str(directory/f'{mode}.root'),
                        skim=False,entry_range=(3,6),range_mode=mode))
                    analysis.define('global_entry','double(rdfentry_)')
                    analysis.count('entries').sum('entry_ids','global_entry').sum('values','value').run()
                    self.assertEqual(analysis.counts['entries'],3)
                    self.assertEqual(analysis.sums,{'entry_ids':12.,'values':12.})
                    self.assertEqual(analysis.native.event_loops,1)
            if source_supported:
                # A branchless virtual tree tests the Python/native 64-bit
                # boundary without storing or traversing billions of entries.
                begin=2**32;path=directory/'virtual64.root'
                output=ROOT.TFile(str(path),'RECREATE')
                tree=ROOT.TTree('Events','');tree.SetEntries(begin+10);tree.Write();output.Close()
                analysis=Analysis(AnalysisConfig(files=[str(path)],output=str(directory/'range64.root'),
                    skim=False,entry_range=(begin,begin+3),range_mode='source'))
                analysis.define('global_entry','double(rdfentry_)').count('entries').sum('entry_ids','global_entry').run()
                self.assertEqual(analysis.counts['entries'],3)
                self.assertEqual(analysis.sums['entry_ids'],float(3*begin+3))
                self.assertEqual(analysis.native.event_loops,1)

    @unittest.skipUnless(importlib.util.find_spec("numba"), "optional Numba is not installed")
    def test_captured_numba_parameters_invalidate_cached_registration(self):
        import numpy as np
        import uproot
        from rdfanalysis import AnalysisSession
        parameters=np.array([1.],dtype='float64')
        def scale(value):
            if value < 0:raise ValueError('negative value in captured-parameter kernel')
            return value*parameters[0]
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.,2.,3.])})
            def build(analysis):
                analysis.define_numba('scaled',scale,['value'],return_type='double')
                analysis.sum('total','scaled')
            with AnalysisSession(max_jobs=4,max_numba_signatures=10000) as session:
                first=session.run(AnalysisConfig(files=[str(source)],output=str(directory/'first.root'),skim=False),build)
                unchanged=session.run(AnalysisConfig(files=[str(source)],output=str(directory/'unchanged.root'),skim=False),build)
                parameters[0]=10.
                second=session.run(AnalysisConfig(files=[str(source)],output=str(directory/'second.root'),skim=False),build)
                third=session.run(AnalysisConfig(files=[str(source)],output=str(directory/'third.root'),skim=False),build)
            self.assertEqual([result.sums['total'] for result in (first,unchanged,second,third)],[6.,6.,60.,60.])
            self.assertEqual([result.compilation['numba_kernels_compiled'] for result in (first,unchanged,second,third)],[1,0,1,0])

    @unittest.skipUnless(importlib.util.find_spec("numba"), "optional Numba is not installed")
    def test_native_event_loop_allows_python_heartbeat_threads(self):
        import threading
        import time
        import numpy as np
        import uproot
        def busy(value):
            for _ in range(100000):value=value*1.000000001+0.00000001
            return value
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory)/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.ones(1000,dtype='float64')})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(directory)/'out.root'),skim=False))
            analysis.define_numba('busy',busy,['value'],return_type='double').sum('total','busy')
            active=threading.Event();stop=threading.Event();ready=threading.Event();ticks=[];rejections=[]
            def heartbeat():
                ready.set()
                while not stop.is_set():
                    if active.is_set():
                        ticks.append(time.monotonic())
                        try:analysis.describe()
                        except RuntimeError as error:
                            if 'while it is running' in str(error):rejections.append(str(error))
                            else:raise
                    time.sleep(.001)
            thread=threading.Thread(target=heartbeat);thread.start();ready.wait()
            try:
                active.set();analysis.run();active.clear()
            finally:
                active.clear();stop.set();thread.join()
            self.assertGreater(len(ticks),0,'native loop held the GIL and blocked Python heartbeat threads')
            self.assertTrue(rejections,'description should reject inspection during native execution')
            self.assertEqual(analysis.native.event_loops,1)
            with self.assertRaises(RuntimeError):analysis.run()

    def test_native_weight_replacement_regions_and_shape_propagation(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory);source=directory/'input.root'
            arrays={name:np.asarray(values,dtype='float64') for name,values in {
                'pt':[19,25,35,45],'pt_shift':[21,27,37,47],
                'gen':[-1,0,2,3],'sf':[0,2,.5,1],'sf_shift':[0,2,.6,1.1],
                'sf_up':[1,3,1,2],'sf_down':[-1,1,0,.5]}.items()}
            with uproot.recreate(source) as output:output.mktree('Events',arrays)
            config=AnalysisConfig(files=[str(source)],output=str(directory/'out.root'),skim=False,persist_histograms=True)
            analysis=Analysis(config)
            analysis.register_variation('pt','jetShape','pt_shift')
            analysis.register_variation('sf','jetShape','sf_shift')
            weights=analysis.weights({'generator':'gen','lepton':'sf'},normalizations={'lumi':2.},
                variations={'leptonSF':('lepton','sf_up','sf_down')})
            self.assertEqual(set(weights),{'Nominal','jetShape','leptonSFUp','leptonSFDown'})
            # All region booleans precede the native manager's base capture.
            analysis.define('pass_base','pt > 20.').define('pass_signal','pt > 30.')
            analysis.region('base','pass_base').region('signal','pass_signal',parent='base')
            for label in ('Nominal','jetShape','leptonSFUp','leptonSFDown'):
                analysis.histogram(label,'pt',5,0,50,weight=weights['Nominal'],region='base',variation=label)
                analysis.sum('sum_'+label,weights[label])
            analysis.histogram('signal','pt',5,0,50,weight=weights['Nominal'],region='signal')
            for invalid in ({'region':'missing'},{'region':'base','selection':'pass_base'}):
                with self.assertRaises(ValueError):analysis.histogram('bad','pt',5,0,50,**invalid)
            with self.assertRaisesRegex(ValueError,'duplicate'):analysis.region('base','pass_base')
            with self.assertRaisesRegex(ValueError,'parent'):analysis.region('orphan','pass_base',parent='absent')
            inspected=analysis.inspect()
            self.assertEqual(inspected['regions']['signal']['chain'],['pass_base','pass_signal'])
            self.assertEqual(inspected['weights']['event_weight']['components']['lepton'],'sf')
            graph=analysis.describe().as_dict()['details']['graph']
            product=next(op for op in graph['operations'] if op['kind']=='weights')
            self.assertIn({'output':'event_weight_jetShape','inputs':['gen','sf_shift']},product['materializations'])
            self.assertIn({'output':'event_weight_leptonSFUp','inputs':['gen','sf_up']},product['materializations'])
            text=analysis.explain('leptonSFUp')
            self.assertIn('inputs: gen, sf_up',text)
            self.assertIn('reuse __rdf_python_region_base',text)
            self.assertEqual(analysis.native.event_loops,0)
            analysis.run()
            self.assertEqual(analysis.native.event_loops,1)
            self.assertEqual(analysis.counts['Nominal'],3)
            self.assertEqual(analysis.counts['jetShape'],4)
            self.assertEqual(analysis.counts['signal'],2)
            for label,expected in [('Nominal',8.),('jetShape',9.),('leptonSFUp',14.),('leptonSFDown',5.)]:
                self.assertAlmostEqual(analysis.sums['sum_'+label],expected)
            # Native replacement multiplies remaining components directly,
            # including a zero nominal SF; no ratio/division is used.
            with uproot.open(config.native_config()['metaFile'],handler=uproot.source.file.MultithreadedFileSource,num_workers=1) as output:
                metadata=json.loads(str(output['__rdf_job_metadata_v1']))
                self.assertEqual(metadata['regions']['signal'],'pass_signal:base')
                audit=metadata['weight_audits']['weight_audit_event_weight']
                self.assertEqual(audit['n'],4)
                self.assertEqual(audit['n_zero'],2)
                self.assertAlmostEqual(audit['sum_w'],8.)

    def test_weight_input_validation_does_not_create_outputs(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as directory:
            source=Path(directory)/'input.root'
            with uproot.recreate(source) as output:output.mktree('Events',{'value':np.array([1.],dtype='float64')})
            analysis=Analysis(AnalysisConfig(files=[str(source)],output=str(Path(directory)/'out.root'),skim=False))
            for kwargs in ({'components':{'factor':'missing'}},{'components':{'factor':'value'},'normalizations':{'x':float('nan')}},
                {'components':{'factor':'value'},'variations':{'bad':('other','value','value')}}):
                with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):analysis.weights(**kwargs)
                self.assertNotIn('event_weight',analysis.columns)
            analysis.register_variation_pair('value','joint','value','value')
            with self.assertRaisesRegex(ValueError,'overlaps'):
                analysis.weights({'factor':'value'},variations={'joint':('factor','value','value')})
            self.assertNotIn('event_weight',analysis.columns)
            analysis.weights({'factor':'value'})
            with self.assertRaisesRegex(ValueError,'one event-weight'):
                analysis.weights({'factor':'value'},output='second_weight')
            analysis.sum('total','event_weight').run()
            self.assertEqual(analysis.sums['total'],1.)

    def test_persistent_histograms_share_metadata_provenance_and_one_loop(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'pt':'float64','up':'float64','weight':'float64'})
                output['Events'].extend({'pt':np.array([24.,35.,45.]),'up':np.array([26.,37.,47.]),'weight':np.array([-2.,3.,-4.])})
            config=AnalysisConfig(files=[str(source)],output=str(directory/'out.root'),skim=False,persist_histograms=True)
            analysis=Analysis(config)
            analysis.register_variation('pt','shape','up').filter('cut','pt > 25.')
            analysis.histogram('Nominal/value','pt',2,30,40,weight='weight')
            analysis.histogram('shape/value','pt',2,30,40,weight='weight',variation='shape')
            for name in ('','/absolute','a//b','a/../b','cycle;1','Nominal'):
                with self.subTest(name=name),self.assertRaises(ValueError):analysis.histogram(name,'pt',2,30,40)
            analysis.run()
            self.assertEqual(analysis.native.event_loops,1)
            metadata=Path(config.native_config()['metaFile'])
            with uproot.open(metadata,handler=uproot.source.file.MultithreadedFileSource,num_workers=1) as output:
                for name,histogram in analysis.histograms.items():
                    saved=output['python_histograms/'+name]
                    self.assertEqual(saved.classname,'TH1D')
                    np.testing.assert_allclose(saved.values(flow=True),histogram['sumw'])
                    np.testing.assert_allclose(saved.variances(flow=True),histogram['sumw2'])
                self.assertEqual(output['python_histograms/shape/value'].values(flow=True)[0],-2.)
                document=json.loads(str(output['__rdf_job_metadata_v1']))
                self.assertIn('metadata:python_histograms (TH1D)',document['provenance'].values())
            # Rerun only replaces the owned histogram namespace, avoiding stale
            # slices when the booking inventory shrinks.
            second=Analysis(config)
            second.histogram('only','pt',2,30,40).run()
            with uproot.open(metadata,handler=uproot.source.file.MultithreadedFileSource,num_workers=1) as output:
                self.assertNotIn('python_histograms/Nominal/value',output)
                self.assertIn('python_histograms/only',output)
                self.assertIn('__rdf_job_metadata_v1',output)

    @unittest.skipUnless(importlib.util.find_spec("numba"), "optional Numba is not installed")
    def test_session_disjoint_inputs_reuse_kernels_and_detach_graphs(self):
        import gc
        import weakref
        import numpy as np
        import uproot
        from rdfanalysis import AnalysisSession, WorkerRecycleRequired
        def twice(value):
            return value*2.
        owners=[]
        def build(analysis):
            owners.append(weakref.ref(analysis))
            analysis.count("input")
            analysis.define_numba("twice",twice,["value"],return_type="double",nominal=True)
            analysis.sum("total","twice")
            analysis.histogram("shape","twice",10,0,1000)
            self.assertIn("twice",analysis.inspect()["columns"])
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            configs=[]
            for i,scale in enumerate((1.,10.,100.)):
                source=directory/f"input{i}.root"
                with uproot.recreate(source) as out:
                    out.mktree("Events",{"value":"float64"})
                    out["Events"].extend({"value":np.array([scale,2*scale])})
                configs.append(AnalysisConfig(files=[str(source)],output=str(directory/f"job{i}.root"),skim=False,systematics=False))
            with AnalysisSession(max_jobs=3,max_numba_signatures=10000) as session:
                results=[session.run(config,build) for config in configs]
                self.assertEqual([r.sums["total"] for r in results],[6.,60.,600.])
                self.assertEqual([r.counts["input"] for r in results],[2,2,2])
                self.assertEqual([r.event_loops for r in results],[1,1,1])
                snapshots = [r.to_run_result() for r in results]
                for original, snapshot in zip(results, snapshots):
                    self.assertEqual(snapshot.as_dict()['histograms'], original.histograms)
                    self.assertEqual(dict(snapshot.counts), original.counts)
                    self.assertEqual(dict(snapshot.sums), original.sums)
                    self.assertEqual(snapshot.event_loops, 1)
                self.assertEqual(results[0].compilation["numba_kernels_compiled"],1)
                for result in results[1:]:
                    self.assertEqual(result.compilation["numba_kernels_compiled"],0)
                    self.assertEqual(result.compilation["numba_cache_hits"],1)
                self.assertEqual(len({r.metadata for r in results}),3)
                self.assertTrue(all(Path(r.metadata).exists() for r in results))
                self.assertTrue(all(r.output is None for r in results))
                gc.collect()
                self.assertTrue(all(ref() is None for ref in owners))
                with self.assertRaises(WorkerRecycleRequired):session.run(configs[0],build)
            with self.assertRaisesRegex(RuntimeError,"closed"):session.run(configs[0],build)

    def test_session_validates_budget_and_output_isolation(self):
        from rdfanalysis import AnalysisSession
        for kwargs in ({"max_jobs":0},{"max_jobs":True},{"max_numba_signatures":-1}):
            with self.assertRaises(ValueError):AnalysisSession(**kwargs)
        session=AnalysisSession()
        config=AnalysisConfig(files=["unused.root"],output="job.root",options={"metaFile":"job.root"})
        with self.assertRaisesRegex(ValueError,"distinct"):session.run(config,lambda a:None)
        session.close()

    @unittest.skipUnless(importlib.util.find_spec("numba"), "optional Numba is not installed")
    def test_session_compilation_budget_allows_cached_kernel_only(self):
        import numpy as np
        import uproot
        import rdfanalysis as frontend
        from rdfanalysis import AnalysisSession, WorkerRecycleRequired
        def original(value):return value+1.
        def unseen(value):return value+2.
        with tempfile.TemporaryDirectory() as directory:
            directory=Path(directory)
            source=directory/'input.root'
            with uproot.recreate(source) as output:
                output.mktree('Events',{'value':'float64'})
                output['Events'].extend({'value':np.array([1.,2.])})
            def config(index):return AnalysisConfig(files=[str(source)],output=str(directory/f'job{index}.root'),skim=False,systematics=False)
            def build(analysis,function=original):
                analysis.define_numba('result',function,['value'],return_type='double',nominal=True)
                analysis.sum('total','result')
            budget=frontend._numba_compilation_attempts+1
            with AnalysisSession(max_jobs=5,max_numba_signatures=budget) as session:
                result=session.run(config(0),build)
                self.assertEqual(result.sums['total'],5.)
                self.assertEqual(session.cache_info['compilation_attempts'],budget)
                self.assertEqual(session.run(config(1),build).compilation['numba_cache_hits'],1)
                with self.assertRaisesRegex(ValueError,'distinct'):
                    session.run(config(1),build)
                with self.assertRaisesRegex(WorkerRecycleRequired,'compilation budget'):
                    session.run(config(2),build,unseen)
                # A rejected compile does not poison the session or consume a
                # new registration. Existing kernels remain usable afterward.
                self.assertEqual(session.cache_info['compilation_attempts'],budget)
                self.assertEqual(session.run(config(3),build).sums['total'],5.)

    def test_lazy_sum_without_histograms_or_skim(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "input.root"
            with uproot.recreate(source) as output:
                output.mktree("Events", {"weight": "float64", "shift": "float64", "index": "int32"})
                output["Events"].extend({"weight": np.array([-2., 3., 4.]), "shift": np.array([1., 4., 5.]),
                                        "index": np.arange(3, dtype="int32")})
            analysis = Analysis(AnalysisConfig(files=[str(source)], output=str(directory/"unused.root"),
                                               skim=False, systematics=True))
            analysis.register_variation("weight", "shape", "shift")
            analysis.sum("all", "weight")
            with self.assertRaisesRegex(RuntimeError, "run"):
                _ = analysis.sums
            with self.assertRaisesRegex(ValueError, "Duplicate sum"):
                analysis.sum("all", "weight")
            with self.assertRaisesRegex(ValueError, "scalar double"):
                analysis.sum("invalid", "index")
            analysis.filter("positive", "weight > 0")
            analysis.sum("selected", "weight")
            analysis.run()
            self.assertEqual(analysis.sums, {"all": 5., "selected": 7.})
            self.assertEqual(analysis.native.event_loops, 1)
            with self.assertRaisesRegex(RuntimeError, "before run"):
                analysis.sum("late", "weight")

    @unittest.skipUnless(importlib.util.find_spec("numba"), "optional Numba is not installed")
    def test_numba_vector_projection_and_lifecycle(self):
        import numpy as np
        import uproot
        import gc
        def pack(value):
            return np.array([value, 2 * value], dtype=np.float64)
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "input.root"
            arrays = {"pt": np.array([24., 35.]), "pt_jesUp": np.array([26., 37.]),
                      "pt_jesDown": np.array([22., 33.])}
            with uproot.recreate(source) as output:
                output.mktree("Events", {k: v.dtype for k, v in arrays.items()})
                output["Events"].extend(arrays)
            analysis = Analysis(AnalysisConfig(files=[str(source)], output=str(directory / "out.root"), skim=False))
            analysis.count("input")
            analysis.define_numba("packed", pack, ["pt"], return_type="RVec<double>")
            analysis.define_numba("second_pack", pack, ["pt"], return_type="RVec<double>")
            analysis.project("twice", "packed", 1)
            analysis.filter("cut", "twice > 50")
            analysis.histogram("nominal", "twice", 4, 40, 80)
            analysis.histogram("up", "twice", 4, 40, 80, variation="jesUp")
            before=analysis.compilation_stats
            graph=analysis.describe().as_dict()['details']['graph']
            by_name={op['name']:op for op in graph['operations']}
            self.assertEqual(by_name['packed']['kind'],'compiled')
            self.assertEqual(by_name['twice']['kind'],'projection')
            self.assertIn({'output':'twice_jesUp','inputs':['packed_jesUp']},by_name['twice']['materializations'])
            self.assertIn('inputs: pt_jesUp',analysis.explain('up'))
            self.assertEqual(analysis.compilation_stats,before)
            self.assertEqual(analysis.native.event_loops,0)
            gc.collect()
            analysis.run()
            self.assertEqual(analysis.native.event_loops, 1)
            self.assertEqual(analysis.counts, {"input": 2, "nominal": 1, "up": 2})
            self.assertEqual(analysis.histograms["up"]["sumw"], [0, 0, 1, 0, 1, 0])
            self.assertEqual(analysis.compilation_stats["numba_kernels_compiled"], 1)
            self.assertEqual(analysis.compilation_stats["numba_cache_hits"], 1)

    def test_inferred_and_explicit_dependencies_match(self):
        import numpy as np
        import uproot
        import rdfanalyzer
        print("Using native module:", rdfanalyzer.__file__)
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "input.root"
            arrays = {"pt": np.array([24., 26., 35., 0.]),
                      "up": np.array([26., 28., 37., 0.]),
                      "down": np.array([22., 24., 33., 0.]),
                      "replica": np.array([27., 29., 38., 0.]),
                      "first": np.ones(4)}
            with uproot.recreate(source) as output:
                output.mktree("Events", {k: v.dtype for k, v in arrays.items()})
                output["Events"].extend(arrays)
            payloads = []
            for explicit in (False, True):
                path = directory / f"output_{explicit}.root"
                analysis = Analysis(AnalysisConfig(
                    files=[str(source)], output=str(path),
                    options={"skimSystematics": "true"}))
                analysis.count("input")
                analysis.register_variation_pair("pt", "jes", "up", "down")
                analysis.register_variation("pt", "pdf_0", "replica")
                analysis.define("scaled", "pt * 2", ["pt"] if explicit else [])
                analysis.define("observable", "scaled + 1", ["scaled"] if explicit else [])
                analysis.define_nominal("nominal_probe", "pt + 10")
                analysis.define("literal_probe", 'pt + double(std::string(R"tag(pt first)tag").size())')
                analysis.define("member_probe", "std::make_pair(pt, first).first")
                analysis.filter("kin", "observable > 50", ["observable"] if explicit else [])
                analysis.filter("second", "rdfentry_ != 1")
                analysis.define_nominal("signed_weight", "pt == 24. ? -2. : 3.")
                analysis.histogram("nominal", "observable", 4, 40, 80, weight="signed_weight")
                analysis.histogram("jes_up", "observable", 4, 40, 80,
                                   weight="signed_weight", variation="jesUp")
                with self.assertRaisesRegex(RuntimeError, "run"):
                    _ = analysis.histograms
                self.assertIn("observable_pdf_0", analysis.columns)
                self.assertNotIn("nominal_probe_jesUp", analysis.columns)
                masks = analysis.selection_masks
                self.assertEqual(set(masks), {"Nominal", "jesUp", "jesDown", "pdf_0"})
                with self.assertRaisesRegex(ValueError, "already exists"):
                    analysis.define("observable", "pt")
                with self.assertRaisesRegex(ValueError, "undeclared input"):
                    analysis.define("bad_dependencies", "pt + first", ["pt"])
                if explicit:
                    graph=analysis.describe().as_dict()['details']['graph']
                    by_name={op['name']:op for op in graph['operations']}
                    self.assertNotIn('bad_dependencies',by_name)
                    self.assertEqual(by_name['literal_probe']['inputs'],['pt'])
                    self.assertEqual(by_name['member_probe']['inputs'],['pt','first'])
                    self.assertEqual(by_name['observable']['variations']['pdf_0'],'observable_pdf_0')
                    self.assertIn('inputs: replica',analysis.explain('observable',variation='pdf_0'))
                    self.assertIn('reuse nominal_probe',analysis.explain('nominal_probe',variation='jesUp'))
                    self.assertEqual(analysis.native.event_loops,0)
                analysis.run()
                self.assertEqual(analysis.native.event_loops, 1)
                self.assertEqual(analysis.counts, {"input": 4, "nominal": 1, "jes_up": 2})
                self.assertEqual(analysis.histograms["nominal"]["sumw"], [0, 0, 0, 0, 3, 0])
                self.assertEqual(analysis.histograms["jes_up"]["sumw"], [0, 0, -2, 0, 3, 0])
                np.testing.assert_allclose(analysis.histograms["jes_up"]["sumw2"], [0, 0, 4, 0, 9, 0])
                with uproot.open(path, handler=uproot.source.file.MultithreadedFileSource,
                                 num_workers=1,
                                 decompression_executor=uproot.source.futures.TrivialExecutor(),
                                 interpretation_executor=uproot.source.futures.TrivialExecutor()) as output:
                    # Compare the scalar contract; internal packed skim masks
                    # are ROOT RVec<bool> objects, not flat NumPy columns.
                    columns = ["pt", "observable", "observable_jesUp", "observable_jesDown",
                               "observable_pdf_0", "literal_probe", "literal_probe_jesUp",
                               "member_probe", "member_probe_jesUp", "nominal_probe",
                               *masks.values()]
                    values = output["Events"].arrays(columns, library="np")
                np.testing.assert_array_equal(values["pt"], [24., 35.])
                np.testing.assert_array_equal(values["observable"], [49., 71.])
                np.testing.assert_array_equal(values["observable_jesUp"], [53., 75.])
                np.testing.assert_array_equal(values["observable_jesDown"], [45., 67.])
                np.testing.assert_array_equal(values["observable_pdf_0"], [55., 77.])
                np.testing.assert_array_equal(values[masks["Nominal"]], [False, True])
                np.testing.assert_array_equal(values[masks["jesUp"]], [True, True])
                np.testing.assert_array_equal(values[masks["jesDown"]], [False, True])
                payloads.append({name: value.tolist() for name, value in values.items()})
            self.assertEqual(payloads[0], payloads[1])

    def test_conventional_pairs_and_disabled_systematics(self):
        import numpy as np
        import uproot
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            source = directory / "input.root"
            arrays = {"pt": np.array([24., 35.]), "pt_jesUp": np.array([26., 37.]),
                      "pt_jesDown": np.array([22., 33.])}
            with uproot.recreate(source) as output:
                output.mktree("Events", {k: v.dtype for k, v in arrays.items()})
                output["Events"].extend(arrays)
            for enabled in (True, False):
                path = directory / f"out_{enabled}.root"
                analysis = Analysis(AnalysisConfig(files=[str(source)], output=str(path),
                                                   systematics=enabled))
                analysis.define("twice", "pt * 2").filter("cut", "twice > 50")
                self.assertEqual("twice_jesUp" in analysis.columns, enabled)
                analysis.run()
                with uproot.open(path) as output:
                    self.assertEqual(output["Events"].num_entries, 2 if enabled else 1)


if __name__ == "__main__":
    unittest.main()
