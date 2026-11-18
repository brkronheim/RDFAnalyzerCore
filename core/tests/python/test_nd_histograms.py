"""Packed ND declarations and structural lowering, without an event runtime."""
import copy
from dataclasses import FrozenInstanceError
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock

from rdfanalysis import Analysis, AnalysisResult, NDCategoryAxis, NDHistogramGroup, NDHistogramSettings, NDHistogramSpec, RegularAxis, VariableAxis, Stage


def group(name='plots', *, variations='all'):
    return NDHistogramGroup(name, [NDHistogramSpec('pt', 'pt', VariableAxis([0., 1., 3.]), 'weight')],
        channel=NDCategoryAxis('channel', RegularAxis(2, -.5, 1.5), ['a', 'b']), variations=variations)


class NDHistogramDeclarationTests(unittest.TestCase):
    def test_snapshots_names_axes_bounds_and_labels_without_runtime(self):
        histograms=list(group().histograms); labels=['a','b']
        category=NDCategoryAxis('channel',RegularAxis(2,-.5,1.5),labels)
        declaration=NDHistogramGroup('plots',histograms,channel=category)
        histograms.clear();labels.reverse()
        self.assertEqual(category.labels,('a','b'))
        description=declaration.describe();json.dumps(description)
        self.assertEqual(description['axis_roles'],['channel','control','sample','systematic','observable'])
        self.assertEqual(description['histograms'][0]['native_key'],'pt_')
        self.assertEqual(description['histograms'][0]['edges'],[0.,1.,3.])
        self.assertEqual(description['categories'][1]['column'],'zero__')
        description['histograms'][0]['edges'][0]=-9
        self.assertEqual(declaration.describe()['histograms'][0]['edges'][0],0.)
        with self.assertRaises(FrozenInstanceError):declaration.name='other'

    def test_bad_declarations_are_rejected_before_lowering(self):
        bad=[lambda:NDHistogramSpec('dir/pt','pt',RegularAxis(2,0,1),'weight'),
             lambda:NDHistogramSpec('pt','pt',RegularAxis(2,0,1),''),
             lambda:NDHistogramSpec('pt','pt',RegularAxis(2,0,1),'weight',force_sparse='yes'),
             lambda:NDHistogramSpec('pt','pt',RegularAxis(2,1e40,2e40),'weight'),
             lambda:NDHistogramSpec('pt','pt',RegularAxis(2,1.,1.+1e-12),'weight'),
             lambda:NDCategoryAxis('channel',VariableAxis([0,1]),['a']),
             lambda:NDCategoryAxis('channel',RegularAxis(1,1e40,2e40),['a']),
             lambda:NDCategoryAxis('channel',RegularAxis(2,0,2),['a','a']),
             lambda:NDCategoryAxis('channel',RegularAxis(2,0,2),['a']),
             lambda:NDHistogramGroup('empty',[]),
             lambda:NDHistogramGroup('duplicate',[group().histograms[0]]*2),
             lambda:NDHistogramGroup('subset',group().histograms,variations=['shapeUp'])]
        for make in bad:
            with self.subTest(make=make),self.assertRaises((ValueError,TypeError)):make()
        for budget in (0,True):
            with self.assertRaises(ValueError):group().native_descriptors(max_histograms=budget)
        two=NDHistogramGroup('two',[NDHistogramSpec('a','pt',RegularAxis(2,0,1),'weight'),
                                    NDHistogramSpec('b','pt',RegularAxis(2,0,1),'weight')])
        with self.assertRaisesRegex(ValueError,'budget'):two.native_descriptors(max_histograms=1)

    def test_storage_options_keep_native_overrides_and_are_detached(self):
        settings=NDHistogramSettings(backend='root',precision='double',sparse_backend='flat',dense_limit_mib=64)
        native=settings.native_options();native['histogramBackend']='boost'
        self.assertEqual(settings.native_options()['histogramBackend'],'root')
        self.assertEqual(settings.native_options()['histogramDenseLimitMiB'],'64')
        self.assertEqual(settings.native_options()['histogramCollectionLayout'],'fixed')
        self.assertEqual(NDHistogramSettings(collection_layout='per-slot').native_options()['histogramCollectionLayout'],'per-slot')
        for kwargs in ({'backend':'gpu'},{'precision':'half'},{'sparse_backend':'other'},
                       {'dense_limit_mib':-1},{'dense_limit_mib':True},{'dense_limit_mib':2**31},
                       {'collection_layout':'auto'}):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):NDHistogramSettings(**kwargs)

    def test_huge_sparse_logical_grid_is_descriptive_not_an_allocation(self):
        category=NDCategoryAxis('category',RegularAxis(1000,0,1000),[str(i) for i in range(1000)])
        declaration=NDHistogramGroup('huge',[NDHistogramSpec('a','pt',RegularAxis(10**6,0,1),'weight')],
            sample=category,channel=category,control=category)
        self.assertGreater(declaration.describe()['logical_cells_per_systematic_bin'],10**12)

    def test_frontend_lowers_once_preserves_metadata_and_rejects_duplicates(self):
        analysis=object.__new__(Analysis)
        analysis.config=SimpleNamespace(options={})
        operation=Mock(return_value={'systematic_labels':['Nominal','shapeUp']})
        analysis.native=SimpleNamespace(_book_nd_histograms=operation,_has_column=lambda name:True)
        analysis._run_status=None;analysis._nd_failure=None;analysis._nd_groups={}
        self.assertIs(analysis.nd_histograms(group()),analysis)
        requests,categories,nominal=operation.call_args.args
        self.assertFalse(nominal);self.assertEqual(requests[0][0],'pt');self.assertEqual(len(categories),3)
        self.assertEqual(analysis._nd_groups['plots']['native']['systematic_labels'],['Nominal','shapeUp'])
        with self.assertRaisesRegex(ValueError,'duplicate ND group'):analysis.nd_histograms(group())
        with self.assertRaisesRegex(ValueError,'destination'):analysis.nd_histograms(group('other'))
        self.assertEqual(operation.call_count,1)

    def test_absent_capability_and_missing_inputs_do_not_book(self):
        analysis=object.__new__(Analysis);analysis.native=SimpleNamespace()
        with self.assertRaisesRegex(RuntimeError,'rebuild'):analysis.nd_histograms(group())
        operation=Mock()
        analysis.native=SimpleNamespace(_book_nd_histograms=operation,_has_column=lambda name:name!='weight')
        analysis._run_status=None;analysis._nd_failure=None;analysis._nd_groups={}
        with self.assertRaisesRegex(ValueError,'unknown ND input'):analysis.nd_histograms(group())
        operation.assert_not_called()
        analysis._run_status='succeeded'
        with self.assertRaisesRegex(RuntimeError,'before run'):analysis.nd_histograms(group())

    def test_native_failure_prevents_running_partial_graph(self):
        analysis=object.__new__(Analysis)
        analysis.native=SimpleNamespace(_book_nd_histograms=Mock(side_effect=RuntimeError('payload failure')),
                                        _has_column=lambda name:True,run=Mock())
        analysis._run_status=None;analysis._nd_failure=None;analysis._nd_groups={}
        with self.assertRaisesRegex(RuntimeError,'payload failure'):analysis.nd_histograms(group())
        with self.assertRaisesRegex(RuntimeError,'fresh Analysis'):analysis.run()
        analysis.native.run.assert_not_called()

    def test_per_slot_requires_explicit_native_capability_before_booking(self):
        analysis=object.__new__(Analysis)
        analysis.config=SimpleNamespace(options={'histogramCollectionLayout':'per-slot'})
        operation=Mock(return_value={'systematic_labels':['Nominal']})
        analysis.native=SimpleNamespace(_book_nd_histograms=operation,_has_column=lambda name:True)
        analysis._run_status=None;analysis._nd_failure=None;analysis._nd_groups={}
        with self.assertRaisesRegex(RuntimeError,'matching native build'):analysis.nd_histograms(group())
        operation.assert_not_called()
        self.assertIsNone(analysis._nd_failure)
        analysis.native._nd_collection_layouts=lambda:['fixed','per-slot']
        analysis.nd_histograms(group())
        self.assertEqual(operation.call_count,1)
        self.assertEqual(analysis._nd_groups['plots']['configured_storage_options']['histogramCollectionLayout'],'per-slot')

    def test_stages_preview_bounds_collisions_and_interleaved_dependencies(self):
        inventory=SimpleNamespace(columns=['pt','weight','channel'],variation_labels=['Nominal'],
            _regions={},_region_base_columns=None,_filter_names=set(),_stages=[],
            _action_names={'counts':set(),'histograms':set(),'sums':set()},_nd_groups={})
        stage=Stage('plots',max_histograms=2).define('accepted','pt>0.',['pt']).filter_column('cut','accepted').nd_histograms(group()).histogram('pt','pt',2,0,3)
        description=stage.preview(inventory).as_dict();json.dumps(description);json.dumps(stage.describe().as_dict())
        self.assertEqual([a['kind'] for a in description['details']['actions']],['nd_histograms','histogram'])
        with self.assertRaisesRegex(ValueError,'phases'):stage.define('late','pt')
        with self.assertRaisesRegex(ValueError,'budget'):
            Stage('budget',max_histograms=1).nd_histograms(group()).histogram('pt','pt',2,0,3).preview(inventory)
        with self.assertRaisesRegex(ValueError,'duplicate ND'):
            Stage('duplicate').nd_histograms(group()).nd_histograms(group('other')).preview(inventory)
        inventory.columns=['pt','weight']
        with self.assertRaisesRegex(ValueError,'unknown ND input'):Stage('missing').nd_histograms(group()).preview(inventory)

    def test_clean_import_and_description_perform_no_io_or_runtime_imports(self):
        script='''
import json,sys
from unittest.mock import patch
from rdfanalysis import NDHistogramGroup,NDHistogramSpec,RegularAxis,NDHistogramSettings
with patch('builtins.open',side_effect=AssertionError('unexpected I/O')):
 g=NDHistogramGroup('plots',[NDHistogramSpec('pt','pt',RegularAxis(2,0,1),'weight')])
 json.dumps(g.describe());NDHistogramSettings().native_options()
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''
        result=subprocess.run([sys.executable,'-S','-c',script],env=dict(os.environ,
            PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python')),capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)

    def test_detached_session_metadata_serializes_without_loading_outputs(self):
        declaration=group().describe()
        result=AnalysisResult({}, {}, {}, {}, {}, 1, None, 'not-opened.root', nd_histograms={'plots':declaration})
        detached=result.as_dict();detached['nd_histograms']['plots']['name']='changed'
        self.assertEqual(result.nd_histograms['plots']['name'],'plots')
        envelope=result.to_run_result();json.dumps(envelope.as_dict())
        self.assertEqual(envelope.details['nd_histograms']['plots']['name'],'plots')


if __name__=='__main__':unittest.main()
