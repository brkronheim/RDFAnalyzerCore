"""Structural declaration checks need no native modules or input data."""
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import Mock, patch
import weakref

from rdfdeclarations import Stage,HistogramGroup,Observable,RegularAxis


def inventory(**changes):
    return SimpleNamespace(**{**dict(columns=['pt','pass_base'],variation_labels=['Nominal','jesUp','jesDown','pdf_0'],
        _stage_failure=None,_stages=[],_regions={},_region_base_columns=None,_filter_names=set(),
        _action_names={'counts':set(),'sums':set(),'histograms':set()}),**changes})


class DeclarationConfigurationTests(unittest.TestCase):
    def test_optional_source_sites_are_detached_and_default_does_not_inspect_frames(self):
        with patch('sys._getframe',side_effect=AssertionError('default must not inspect frames')):
            ordinary=Stage('ordinary').define('twice','pt*2.').count('events')
        self.assertTrue(all('source' not in step for step in ordinary.describe().as_dict()['details']['steps']))
        namespace={'Stage':Stage}
        exec(compile("stage=Stage('located',record_sources=True)\nstage.define('twice','pt*2.')\nstage.filter('cut','twice>0.')\n",'/virtual/my analysis.py','exec'),namespace)
        description=namespace['stage'].describe().as_dict()['details']
        self.assertEqual(description['steps'][0]['source'],
            {'file':'/virtual/my analysis.py','line':2,'function':'<module>'})
        self.assertEqual(description['steps'][1]['source']['line'],3)
        description['steps'][0]['source']['line']=999
        self.assertEqual(namespace['stage'].describe().as_dict()['details']['steps'][0]['source']['line'],2)
        with self.assertRaises(TypeError):Stage('invalid_sources',record_sources='yes')

    def test_source_record_does_not_keep_calling_frame_or_locals_alive(self):
        class Sentinel:pass
        def build():
            owner=Sentinel();reference=weakref.ref(owner)
            stage=Stage('located',record_sources=True).count('events')
            return stage,reference
        stage,reference=build()
        self.assertIsNone(reference())
        self.assertEqual(stage.describe().as_dict()['details']['steps'][0]['source']['function'],'build')

    def test_lowering_failure_reports_declaration_location_and_keeps_original_cause(self):
        namespace={'Stage':Stage}
        exec(compile("stage=Stage('located',record_sources=True)\nstage.define('twice','invalid kernel')\n",'/virtual/analysis.py','exec'),namespace)
        analysis=inventory();cause=ValueError('bad kernel');analysis.define=Mock(side_effect=cause)
        with self.assertRaisesRegex(RuntimeError,r'declared at /virtual/analysis.py:2') as caught:
            namespace['stage'].apply(analysis)
        self.assertIs(caught.exception.__cause__,cause)
        self.assertIn('analysis.py:2',analysis._stage_failure)

    def test_expanded_group_failures_report_group_declaration_for_both_booking_paths(self):
        for batch in (False,True):
            with self.subTest(batch=batch):
                group=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(5,0,100)),),variations='all')
                namespace={'Stage':Stage,'group':group,'batch':batch}
                exec(compile("stage=Stage('located',record_sources=True)\nstage.histograms(group,batch=batch)\n",'/virtual/groups.py','exec'),namespace)
                analysis=inventory();analysis.histogram=Mock(side_effect=ValueError('booking failed'))
                analysis.histogram_batch=Mock(side_effect=ValueError('booking failed'))
                with self.assertRaisesRegex(RuntimeError,r'declared at /virtual/groups.py:2'):
                    namespace['stage'].apply(analysis)

    def test_successful_stage_retains_only_explicit_source_metadata(self):
        analysis=inventory();analysis.count=Mock()
        stage=Stage('located',record_sources=True).count('events')
        self.assertIs(stage.apply(analysis),analysis)
        self.assertEqual(analysis.count.call_args.args,('events',))
        self.assertEqual(analysis._stages[0]['declaration_sources']['0']['function'],
            'test_successful_stage_retains_only_explicit_source_metadata')
        self.assertNotIn('declaration_sources',Stage('ordinary').describe().as_dict()['details'])

    def test_structural_validation_returns_common_report_without_lowering_or_sealing(self):
        analysis=inventory();analysis.define=Mock(side_effect=AssertionError('must not lower'))
        stage=Stage('preflight').define('derived','invalid expression')
        # Expressions are intentionally opaque at this validation level.
        self.assertTrue(stage.validate(analysis).passed)
        self.assertFalse(stage.describe().as_dict()['details']['sealed'])
        self.assertEqual(analysis._stages,[]);analysis.define.assert_not_called()
        stage.filter_column('later','missing')
        report=stage.validate(analysis)
        self.assertFalse(report.passed)
        self.assertEqual(report.level,'declarations')
        self.assertEqual(report.issues[0].location,'preflight')
        self.assertIn('unknown predicate',report.issues[0].message)
        with self.assertRaisesRegex(ValueError,'unknown predicate'):report.raise_if_invalid()
        self.assertIsNone(analysis._stage_failure)
        self.assertFalse(stage.describe().as_dict()['details']['sealed'])

    def test_structural_validation_distinguishes_provisional_and_strict_labels(self):
        group=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(5,0,100)),),variations=('not_yet_registered',))
        stage=Stage('preflight').histograms(group)
        self.assertTrue(stage.validate(inventory()).passed)
        self.assertFalse(stage.validate(inventory(),strict=True).passed)
        with self.assertRaises(TypeError):stage.validate(inventory(),strict='yes')
        failed=inventory(_stage_failure='earlier native setup failed')
        self.assertFalse(stage.validate(failed).passed)

    def test_point_queries_avoid_exporting_the_entire_native_inventory(self):
        calls=[]
        class Native:
            def _has_column(self,name):
                calls.append(name);return name in ('pt','pass_base')
        analysis=inventory(native=Native(),columns=None)
        recipe=Stage('small').define('new','pt*2.').filter('cut','new>0.')
        recipe.region('base','pass_base').histogram('pt','new',5,0,100,region='base')
        self.assertEqual(recipe.preview(analysis).as_dict()['details']['region_order'],['base'])
        self.assertEqual(calls,['new','pass_base','__rdf_python_region_base'])

    def test_interleaved_recipe_description_keeps_exact_declaration_order(self):
        recipe=Stage('interleaved').define('first','pt').filter('first_cut','first>0.')
        recipe.define('second','first*2.').filter('second_cut','second>50.').project('last','packed',0)
        steps=recipe.describe().as_dict()['details']['steps']
        self.assertEqual([item['method'] for item in steps],['define','filter','define','filter','project'])

    def test_bool_column_filters_keep_positions_and_check_available_predicates(self):
        recipe=Stage('interleaved').define('first','pt>0.').filter_column('cut','first')
        recipe.define('second','pt*2.').filter('second_cut','second>50.')
        self.assertEqual([step['method'] for step in recipe.describe().as_dict()['details']['steps']],
                         ['define','filter_column','define','filter'])
        recipe.preview(inventory())
        with self.assertRaisesRegex(ValueError,'declaration position'):
            Stage('future').filter_column('cut','later').define('later','pt>0.').preview(inventory())
        with self.assertRaisesRegex(ValueError,'duplicate filter'):
            Stage('duplicate').filter('same','pt>0.').filter_column('same','pass_base').preview(inventory())

    def test_variation_axis_defaults_families_labels_and_budget(self):
        observables=(Observable('pt','pt',RegularAxis(5,0,100)),)
        labels=['Nominal','jesUp','jesDown','pdf_0']
        default=HistogramGroup('group',observables)
        self.assertEqual([item['variation'] for item in default.resolve(labels,budget=10)],['Nominal'])
        selected=HistogramGroup('group',observables,variations=('Nominal','jesUp','pdf_0'),families=('jes',))
        self.assertEqual([item['variation'] for item in selected.resolve(labels,budget=10)],
                         ['Nominal','jesUp','pdf_0','jesDown'])
        with self.assertRaisesRegex(ValueError,'unknown variations'):
            HistogramGroup('group',observables,families=('absent',)).resolve(labels,budget=10)
        with self.assertRaisesRegex(ValueError,'budget'):
            HistogramGroup('group',observables,regions=('a','b'),variations='all').resolve(labels,budget=7)

    def test_group_batch_is_explicit_bounded_and_keeps_action_boundaries(self):
        group=HistogramGroup('group',(Observable('pt','pt',RegularAxis(5,0,100)),),variations='all')
        default=Stage('default').histograms(group).preview(inventory()).as_dict()['details']['actions']
        self.assertTrue(all(item['kind']=='histogram' for item in default))
        stage=Stage('batch').count('before').histograms(group,batch=True).count('after')
        actions=stage.preview(inventory()).as_dict()['details']['actions']
        self.assertEqual([item['kind'] for item in actions],['count','histogram_batch','count'])
        self.assertEqual(len(actions[1]['arguments']),4)
        with self.assertRaisesRegex(ValueError,'budget'):
            Stage('bounded',max_histograms=3).histograms(group,batch=True).preview(inventory())
        with self.assertRaises(TypeError):Stage('invalid').histograms(group,batch='yes')

    def test_default_groups_use_available_native_batch_without_crossing_actions(self):
        group=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(5,0,100)),),variations='all')
        analysis=inventory(native=SimpleNamespace(book_histograms=lambda requests:None))
        stage=Stage('automatic').count('before').histograms(group).sum('weights','pt')
        actions=stage.preview(analysis).as_dict()['details']['actions']
        self.assertEqual([x['kind']for x in actions],['count','histogram_batch','sum'])
        self.assertEqual(len(actions[1]['arguments']),4)
        analysis.count=Mock();analysis.sum=Mock();analysis.histogram_batch=Mock()
        stage.apply(analysis)
        analysis.histogram_batch.assert_called_once()
        self.assertEqual(analysis.histogram_batch.call_args.kwargs,{'max_histograms':1000})
        self.assertEqual([x['kind']for x in analysis._stages[0]['actions']],['count','histogram_batch','sum'])
        ordinary=Stage('compatibility').histograms(group,batch=False).preview(analysis).as_dict()['details']['actions']
        self.assertTrue(all(x['kind']=='histogram'for x in ordinary))

    def test_automatic_mixed_group_requires_matching_native_capability(self):
        from rdfdeclarations import VariableAxis
        group=HistogramGroup('mixed',(Observable('fixed','pt',RegularAxis(5,0,100)),Observable('edges','pt',VariableAxis((0,30,100)))))
        regular_only=inventory(native=SimpleNamespace(book_histograms=lambda requests:None))
        old=Stage('old').histograms(group).preview(regular_only).as_dict()['details']['actions']
        self.assertEqual([x['kind']for x in old],['histogram','histogram_variable'])
        mixed=inventory(native=SimpleNamespace(book_histograms_mixed=lambda requests:None))
        actions=Stage('new').histograms(group).preview(mixed).as_dict()['details']['actions']
        self.assertEqual([x['kind']for x in actions],['histogram_batch'])
        self.assertEqual([x['name']for x in actions[0]['arguments']],['mixed/inclusive/Nominal/fixed','mixed/inclusive/Nominal/edges'])

    def test_single_histogram_group_keeps_immediate_booking(self):
        group=HistogramGroup('single',(Observable('pt','pt',RegularAxis(5,0,100)),))
        analysis=inventory(native=SimpleNamespace(book_histograms=lambda requests:None))
        actions=Stage('one').histograms(group).preview(analysis).as_dict()['details']['actions']
        self.assertEqual([x['kind']for x in actions],['histogram'])

    def test_automatic_group_failure_keeps_original_declaration_site(self):
        group=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(5,0,100)),),variations='all')
        namespace={'Stage':Stage,'group':group}
        exec(compile("stage=Stage('located',record_sources=True)\nstage.histograms(group)\n",'/virtual/automatic.py','exec'),namespace)
        analysis=inventory(native=SimpleNamespace(book_histograms=lambda requests:None));cause=ValueError('native invalid column')
        analysis.histogram_batch=Mock(side_effect=cause)
        with self.assertRaisesRegex(RuntimeError,r'declared at /virtual/automatic.py:2')as caught:namespace['stage'].apply(analysis)
        self.assertIs(caught.exception.__cause__,cause)
        self.assertIn('histogram_batch',analysis._stage_failure)

    def test_nested_groups_keep_their_own_declaration_site_in_both_paths(self):
        first=HistogramGroup('plots',(Observable('pt','pt',RegularAxis(5,0,100)),Observable('again','pt',RegularAxis(5,0,100))))
        nested=HistogramGroup('plots/sub',(Observable('pt','pt',RegularAxis(5,0,100)),Observable('again','pt',RegularAxis(5,0,100))))
        for batch in (None,False):
            with self.subTest(batch=batch):
                namespace={'Stage':Stage,'first':first,'nested':nested,'batch':batch}
                exec(compile("stage=Stage('nested',record_sources=True)\nstage.histograms(first,batch=batch)\nstage.histograms(nested,batch=batch)\n",'/virtual/nested.py','exec'),namespace)
                analysis=inventory(native=SimpleNamespace(book_histograms=lambda requests:None))
                cause=ValueError('nested invalid column')
                analysis.histogram_batch=Mock(side_effect=[None,cause])
                analysis.histogram=Mock(side_effect=[None,None,cause])
                with self.assertRaisesRegex(RuntimeError,r'declared at /virtual/nested.py:3')as caught:
                    namespace['stage'].apply(analysis)
                self.assertIs(caught.exception.__cause__,cause)

    def test_preview_checks_directory_collisions_and_existing_actions(self):
        analysis=inventory(_action_names={'counts':{'group'},'sums':set(),'histograms':{'group'}})
        stage=Stage('collision').histogram('group/pt','pt',5,0,100)
        with self.assertRaisesRegex(ValueError,'path collision'):stage.preview(analysis)
        analysis=inventory(_action_names={'counts':{'group/pt'},'sums':set(),'histograms':{'group/pt'}})
        with self.assertRaisesRegex(ValueError,'path collision'):
            Stage('collision').histogram('group','pt',5,0,100).preview(analysis)
        with self.assertRaisesRegex(ValueError,'duplicate count'):
            Stage('duplicate').count('group/pt').preview(analysis)
        # Counts and sums keep the native independent name spaces.
        self.assertEqual(len(Stage('sum').sum('group/pt','pt').preview(analysis).as_dict()['details']['actions']),1)

    def test_phase_order_and_region_parent_planning(self):
        analysis=inventory()
        stage=Stage('regions').region('child','pass_base',parent='base').region('base','pass_base')
        self.assertEqual(stage.preview(analysis).as_dict()['details']['region_order'],['base','child'])
        self.assertFalse(analysis._regions)
        with self.assertRaisesRegex(ValueError,'phases'):Stage('bad').count('before').define('late','pt')
        with self.assertRaisesRegex(ValueError,'phases'):Stage('bad').filter('4j1b','pt>0.').register_variation('pt','shape','pt')
        with self.assertRaises(ValueError):RegularAxis(True,0,1)
        with self.assertRaises(ValueError):RegularAxis(1,0,float('inf'))
        with self.assertRaises(ValueError):Stage('bad').histogram('a/../b','pt',1,0,1)

    def test_recipe_description_imports_no_optional_stack(self):
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        code="""
import sys
from rdfanalysis import Stage,HistogramGroup,Observable,RegularAxis
def kernel(pt):return pt*2.
stage=Stage('example').define_numba('twice',kernel,['pt'],return_type='double')
stage.histograms(HistogramGroup('plots',(Observable('pt','twice',RegularAxis(10,0,100)),)))
assert stage.describe().as_dict()['details']['steps'][0]['args'][1]['callable']=='kernel'
assert not {'ROOT','rdfanalyzer','numba','numpy','yaml','rdfanalyzer_cms'}.intersection(sys.modules)
print('clean')
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,text=True,capture_output=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')


if __name__=='__main__':unittest.main()
