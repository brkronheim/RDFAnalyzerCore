"""Names and declared alignment resolve without importing the event runtime."""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

from rdfanalysis import CollectionRef, FieldMetadata, KernelOutputs, Stage
from rdfcolumns import KernelField


class ColumnReferenceTests(unittest.TestCase):
    def test_delayed_fields_keep_interleaving_and_are_detached_descriptions(self):
        outputs=KernelOutputs('summary',['accepted','ht'])
        def kernel(pt):return pt
        stage=Stage('selected').define_numba_outputs(outputs,kernel,['pt'],return_type='RVec<double>')
        stage.project('accepted',outputs['accepted']).filter('accept','accepted>0.')
        stage.project('ht',outputs['ht']).histogram('ht','ht',20,0,100)
        steps=stage.describe().as_dict()['details']['steps']
        self.assertEqual([step['method'] for step in steps],
                         ['define_numba_outputs','project','filter','project','histogram'])
        self.assertEqual(steps[1]['args'],['accepted','summary',0])
        self.assertEqual(steps[3]['args'],['ht','summary',1])
        steps[0]['args'][0]['fields']['ht']=999
        self.assertEqual(outputs['ht'].index,1)
        self.assertEqual(outputs.fields,('accepted','ht'))

    def test_names_projection_policy_and_supported_types_fail_before_lowering(self):
        for fields in ([],['x','x'],['bad name']):
            with self.assertRaises(ValueError):KernelOutputs('summary',fields)
        outputs=KernelOutputs('summary',('weight',),nominal=True)
        stage=Stage('weights').project('weight',outputs['weight'])
        self.assertTrue(stage.describe().as_dict()['details']['steps'][0]['kwargs']['nominal'])
        with self.assertRaisesRegex(KeyError,'unknown field'):outputs['absent']
        with self.assertRaisesRegex(ValueError,'already specifies'):Stage('bad').project('weight',outputs['weight'],0)
        with self.assertRaisesRegex(ValueError,'conflicts'):Stage('bad').project('weight',outputs['weight'],nominal=False)
        with self.assertRaisesRegex(ValueError,'boolean'):Stage('bad').project('weight',outputs['weight'],nominal=1)
        with self.assertRaisesRegex(ValueError,'explicit RVec'):
            Stage('bad').define_numba_outputs(outputs,lambda pt:pt,['pt'],return_type='RVec<int>')
        for index in (-1,True,0.5):
            with self.assertRaises(ValueError):KernelField('summary',index)

    def test_collection_fields_and_index_mapping_never_claim_automatic_alignment(self):
        fields={'pt':'Jet_pt','eta':'Jet_eta'}
        raw=CollectionRef('raw',fields,alignment='input-jet-order')
        fields['pt']='changed'
        shifted=CollectionRef('shifted',{'pt':'jet_pt_shifted','eta':'Jet_eta'},alignment='input-jet-order')
        selected=CollectionRef('selected',{'pt':'selected_pt'},alignment='selected-jet-order',
                               index_column='selected_original_index',index_target='raw')
        self.assertEqual(raw['pt'],'Jet_pt')
        self.assertIs(raw.require_aligned(shifted),raw)
        with self.assertRaisesRegex(ValueError,'differs'):raw.require_aligned(selected)
        with self.assertRaisesRegex(ValueError,'unknown'):raw.require_aligned(CollectionRef('other',{'pt':'pt'}))
        with self.assertRaisesRegex(ValueError,'together'):CollectionRef('bad',{'pt':'pt'},index_column='index')
        with self.assertRaises(TypeError):raw.fields['pt']='mutated'
        with self.assertRaises(KeyError):raw['mass']
        description=selected.describe();description['fields']['pt']='mutated'
        self.assertEqual(selected['pt'],'selected_pt')
        self.assertEqual(description['index_target'],'raw')
        json.dumps(description)

    def test_preview_checks_named_result_collisions_without_calling_kernel(self):
        from test_rdfdeclarations import inventory
        outputs=KernelOutputs('pt',('first',))
        def forbidden(pt):raise AssertionError('a preview called the event kernel')
        stage=Stage('bad').define_numba_outputs(outputs,forbidden,['pt'],return_type='RVec<float>')
        with self.assertRaisesRegex(ValueError,'duplicate definition'):stage.preview(inventory())
        good=Stage('good').define_numba_outputs(KernelOutputs('result',('value',)),forbidden,['pt'],return_type='RVec<float>')
        good.project('value',KernelOutputs('result',('value',))['value']).preview(inventory())
        self.assertFalse(good.describe().as_dict()['details']['sealed'])

    def test_field_metadata_is_owned_optional_and_never_changes_consumer_columns(self):
        from test_rdfdeclarations import inventory
        metadata = {'pt': FieldMetadata(dtype='RVec<float>', unit='GeV',
                                       description='Corrected jet momentum\nOriginal jet order.')}
        fields = {'pt': 'pt'}
        raw = CollectionRef('raw', fields, alignment='input')
        annotated = CollectionRef('annotated', fields, alignment='input', metadata=metadata)
        self.assertIs(raw.require_aligned(annotated), raw)
        metadata.clear()
        fields['pt'] = 'changed'
        self.assertEqual(annotated['pt'], raw['pt'])
        self.assertEqual(annotated.metadata['pt'].unit, 'GeV')
        with self.assertRaises(TypeError):
            annotated.metadata['pt'] = FieldMetadata(unit='MeV')
        description = annotated.describe()
        description['metadata']['pt']['unit'] = 'changed'
        self.assertEqual(annotated.metadata['pt'].unit, 'GeV')
        self.assertNotIn('metadata', raw.describe())
        json.dumps(description)
        def forbidden(pt):
            raise AssertionError('metadata must not evaluate a kernel')
        def plan(collection):
            return Stage('same').define_numba('sum_pt', forbidden, [collection['pt']],
                                              return_type='double').preview(inventory()).as_dict()
        self.assertEqual(plan(raw), plan(annotated))
        with self.assertRaisesRegex(KeyError, 'annotated: unknown field.*available'):
            annotated['mass']

    def test_metadata_typos_and_unsupported_values_fail_without_native_setup(self):
        for value in ('', ' ', 'GeV\n', 1, object()):
            for name in ('unit', 'dtype'):
                with self.subTest(name=name, value=repr(value)), self.assertRaises(ValueError):
                    FieldMetadata(**{name: value})
        for value in (None, 1, 'bad\0text'):
            with self.assertRaises(ValueError):
                FieldMetadata(description=value)
        for metadata, error in ((None, TypeError), ([], TypeError),
                                ({'absent': FieldMetadata()}, ValueError),
                                ({1: FieldMetadata()}, ValueError),
                                ({'pt': {'unit': 'GeV'}}, TypeError)):
            with self.subTest(metadata=repr(metadata)), self.assertRaises(error):
                CollectionRef('jets', {'pt': 'Jet_pt'}, metadata=metadata)

    def test_explicit_schema_requirements_keep_order_exact_types_and_alias_locations(self):
        from dataclasses import FrozenInstanceError
        from rdfschema import ColumnRequirement, compare_schema
        jets = CollectionRef('jets', {
            'untyped_pt': 'Jet_pt', 'eta': 'Jet_eta', 'pt': 'Jet_pt',
            'same_pt': 'Jet_pt', 'raw_eta': 'Jet_eta', 'event_count': 'nJet'},
            alignment='declared-only', index_column='selected_indices', index_target='raw',
            metadata={'pt': FieldMetadata(dtype='ROOT::VecOps::RVec<float>', unit='GeV'),
                      'same_pt': FieldMetadata(dtype='ROOT::VecOps::RVec<float>'),
                      'eta': FieldMetadata(dtype='RVec<float>'),
                      'event_count': FieldMetadata(dtype='unsigned int')})
        before = jets.describe()
        requirements = jets.schema_requirements()
        self.assertIsInstance(requirements, tuple)
        self.assertTrue(all(isinstance(item, ColumnRequirement) for item in requirements))
        self.assertEqual([(item.name, item.type) for item in requirements], [
            ('Jet_pt', 'ROOT::VecOps::RVec<float>'), ('Jet_eta', 'RVec<float>'),
            ('nJet', 'unsigned int')])
        self.assertEqual(requirements[0].location,
            'CollectionRef(jets).fields.untyped_pt, CollectionRef(jets).fields.pt, '
            'CollectionRef(jets).fields.same_pt')
        self.assertEqual(requirements[1].location,
            'CollectionRef(jets).fields.eta, CollectionRef(jets).fields.raw_eta')
        with self.assertRaises(FrozenInstanceError):
            requirements[0].type = 'double'
        self.assertTrue(compare_schema(requirements, {
            'Jet_pt': 'ROOT::VecOps::RVec<float>', 'Jet_eta': 'RVec<float>',
            'nJet': 'unsigned int'}).passed)
        self.assertEqual(jets.describe(), before)
        self.assertEqual(jets['pt'], 'Jet_pt')

    def test_explicit_schema_reports_missing_wrong_and_unknown_types_contextually(self):
        from rdfschema import compare_schema
        jets = CollectionRef('corrected', {
            'pt': 'corrected_pt', 'eta': 'Jet_eta', 'unknown': 'Jet_unknown', 'presence': 'present'},
            metadata={name: FieldMetadata(dtype='RVec<float>')
                      for name in ('pt', 'eta', 'unknown')})
        requirements = jets.schema_requirements()
        report = compare_schema(requirements, {
            'Jet_eta': 'ROOT::VecOps::RVec<float>', 'Jet_unknown': '', 'present': ''})
        self.assertFalse(report.passed)
        self.assertEqual([issue.code for issue in report.issues],
                         ['schema.missing', 'schema.type', 'schema.type'])
        self.assertEqual([issue.location for issue in report.issues], [
            'CollectionRef(corrected).fields.pt', 'CollectionRef(corrected).fields.eta',
            'CollectionRef(corrected).fields.unknown'])
        self.assertIn('unknown type', report.issues[-1].message)
        self.assertIn('ROOT::VecOps::RVec<float>', report.issues[1].message)

    def test_alias_type_conflict_is_explicit_and_names_both_typed_fields(self):
        jets = CollectionRef('jets', {'presence': 'Jet_pt', 'pt': 'Jet_pt', 'scaled': 'Jet_pt'},
            metadata={'pt': FieldMetadata(dtype='RVec<float>'),
                      'scaled': FieldMetadata(dtype='ROOT::VecOps::RVec<float>')})
        # Ordinary authoring remains available before the explicit schema request.
        self.assertEqual(jets['scaled'], 'Jet_pt')
        json.dumps(jets.describe())
        with self.assertRaises(ValueError) as caught:
            jets.schema_requirements()
        message = str(caught.exception)
        self.assertIn('CollectionRef(jets).fields.pt expects', message)
        self.assertIn('CollectionRef(jets).fields.scaled expects', message)
        self.assertNotIn('fields.presence expects', message)
        self.assertIn('Jet_pt', message)

    def test_presence_only_requirements_and_custom_per_file_reader_remain_explicit(self):
        from rdfschema import InputSchema, check_input_schemas, compare_schema
        collection = CollectionRef('objects', {'first': 'objects', 'alias': 'objects'},
            index_column='not_requested', index_target='not_inspected',
            metadata={'first': FieldMetadata(unit='arbitrary')})
        requirements = collection.schema_requirements()
        self.assertEqual([(item.name, item.type) for item in requirements], [('objects', None)])
        self.assertTrue(compare_schema(requirements, {'objects': ''}).passed)
        calls = []
        def reader(source, tree, columns):
            calls.append((source, tree, columns))
            return InputSchema(source, tree, {'objects': ''} if source == 'first' else {})
        result = check_input_schemas(['first', 'second'], requirements, reader=reader)
        self.assertEqual(calls, [('first', 'Events', ('objects',)),
                                 ('second', 'Events', ('objects',))])
        self.assertFalse(result.passed)
        self.assertIn('second', result.report.issues[0].location)
        self.assertIn('CollectionRef(objects).fields.first', result.report.issues[0].location)
        self.assertIn('CollectionRef(objects).fields.alias', result.report.issues[0].location)

    def test_schema_bridge_is_lazy_and_stdlib_only(self):
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        code="""
import json,sys
from rdfcolumns import CollectionRef,FieldMetadata
jets=CollectionRef('jets',{'pt':'Jet_pt'},metadata={'pt':FieldMetadata(dtype='RVec<float>')})
before=jets.describe()
assert jets['pt']=='Jet_pt'
assert 'rdfschema' not in sys.modules
requirements=jets.schema_requirements()
assert 'rdfschema' in sys.modules
assert requirements[0].type=='RVec<float>'
assert jets.describe()==before
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
print('clean')
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,text=True,capture_output=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')

    def test_stdlib_only_authoring_and_serialization(self):
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        code="""
import json,sys
from rdfanalysis import KernelOutputs,CollectionRef,FieldMetadata,Stage
jets=CollectionRef('jets',{'pt':'Jet_pt'},alignment='input-order',
 metadata={'pt':FieldMetadata(dtype='RVec<float>',unit='GeV')})
outputs=KernelOutputs('summary',('accepted','ht'))
def kernel(pt):return pt
stage=Stage('selected').define_numba_outputs(outputs,kernel,[jets['pt']],return_type='RVec<double>')
stage.project('ht',outputs['ht'])
json.dumps(stage.describe().as_dict())
json.dumps(jets.describe())
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
print('clean')
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,text=True,capture_output=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')


if __name__=='__main__':unittest.main()
