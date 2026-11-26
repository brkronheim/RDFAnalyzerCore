"""Per-file coverage, exact types and optional zero-loop metadata acquisition."""
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from _stub_contracts import assert_stub_contract
from rdfschema import ColumnRequirement, InputSchema, InputSchemaCheck, check_input_schemas, compare_schema, read_root_schema

REPO = Path(__file__).resolve().parents[3]


class SchemaTests(unittest.TestCase):
    def test_exact_types_presence_unknown_types_and_source_locations(self):
        requirements = [ColumnRequirement('Jet_pt', 'ROOT::VecOps::RVec<float>', 'selection.jets'),
                        ColumnRequirement('event'), ColumnRequirement('unknown', 'float')]
        report = compare_schema(requirements, {'Jet_pt': 'RVec<float>', 'unknown': '', 'extra': 'int'})
        self.assertEqual([issue.code for issue in report.issues], ['schema.type', 'schema.missing', 'schema.type'])
        self.assertEqual(report.issues[0].location, 'selection.jets')
        self.assertIn('unknown type', report.issues[2].message)
        self.assertEqual(report.supported_levels, ('schema',))
        self.assertTrue(compare_schema([ColumnRequirement('event')], {'event': ''}).passed)

    def test_later_files_are_checked_and_errors_aggregate_in_input_order(self):
        calls = []
        def reader(source, tree, names):
            calls.append((source, tree, names))
            if source == 'inaccessible.root':
                raise OSError('storage is unavailable')
            return InputSchema(source, tree, {'event': 'ULong64_t'} if source == 'first.root' else {})
        requirements = [ColumnRequirement('event', 'ULong64_t', 'analysis.event')]
        result = check_input_schemas(['first.root', 'inaccessible.root', 'later.root', 'first.root'],
                                    requirements, reader=reader)
        self.assertFalse(result.passed)
        self.assertEqual([call[0] for call in calls], list(result.files))
        self.assertEqual([item.code for item in result.report.issues], ['schema.input', 'schema.missing'])
        self.assertIn('files[2] (later.root):Events:analysis.event', result.report.issues[1].location)
        self.assertEqual(len(result.schemas), 3)
        detached = result.as_dict(); detached['schemas'][0]['columns']['event'] = 'mutated'
        self.assertEqual(result.schemas[0].columns['event'], 'ULong64_t')
        with self.assertRaisesRegex(ValueError, 'later.root'):
            result.raise_if_invalid()
        json.dumps(result.as_dict())

    def test_bad_requests_fail_before_reader_runs_and_bugs_are_not_input_errors(self):
        reader = unittest.mock.Mock()
        for files, requirements in [([], []), ('one.root', []), (['one.root'], ['event']),
                (['one.root'], [ColumnRequirement('event'), ColumnRequirement('event')])]:
            with self.subTest(files=files, requirements=requirements), self.assertRaises((ValueError, TypeError)):
                check_input_schemas(files, requirements, reader=reader)
        reader.assert_not_called()
        for returned in [None, InputSchema('different.root', 'Events', {})]:
            with self.subTest(returned=returned), self.assertRaises((ValueError, TypeError)):
                check_input_schemas(['one.root'], [], reader=lambda *args: returned)
        def broken(*args):
            raise RuntimeError('reader programming error')
        with self.assertRaisesRegex(RuntimeError, 'programming error'):
            check_input_schemas(['one.root'], [], reader=broken)

    def test_snapshot_is_detached_and_validated(self):
        columns = {'event': 'ULong64_t'}
        snapshot = InputSchema('input.root', 'Events', columns)
        columns['event'] = 'double'
        self.assertEqual(snapshot.columns['event'], 'ULong64_t')
        with self.assertRaises(TypeError):
            snapshot.columns['event'] = 'double'
        for name in ('', 'bad\0name', 'bad\nname'):
            with self.subTest(name=name), self.assertRaises(ValueError):
                ColumnRequirement(name)

    def test_analysis_config_adapter_uses_all_files_and_its_selected_tree(self):
        from rdfanalysis import AnalysisConfig
        calls = []
        def reader(source, tree, names):
            calls.append((source, tree, names))
            return InputSchema(source, tree, {'event': 'ULong64_t'})
        config = AnalysisConfig(files=['one.root', 'two.root'], tree='Selected', output='unused.root')
        self.assertTrue(config.check_schema([ColumnRequirement('event')], reader=reader).passed)
        self.assertEqual(calls, [('one.root', 'Selected', ('event',)), ('two.root', 'Selected', ('event',))])
        self.assertTrue(config.validate().passed)
        unsupported=config.validate('schema')
        self.assertFalse(unsupported.passed)
        self.assertIn('check_schema(requirements)',unsupported.issues[0].message)

    def test_passing_result_cannot_claim_first_file_coverage_for_a_chain(self):
        from rdfworkflow import ValidationReport
        report = ValidationReport('schema', ('schema',))
        with self.assertRaisesRegex(ValueError, 'every supplied file'):
            InputSchemaCheck(report, ('first.root', 'later.root'), (InputSchema('first.root', 'Events', {}),))

    def test_cli_bad_requirements_are_structured_and_help_needs_no_root(self):
        import tempfile
        with tempfile.TemporaryDirectory() as temporary:
            requirements = Path(temporary)/'requirements.json'
            requirements.write_text('{invalid')
            env = dict(os.environ, PYTHONPATH=str(REPO/'core/python'))
            command = [sys.executable, '-S', '-m', 'rdfschema']
            result = subprocess.run(command+[str(requirements), 'unused.root'], env=env,
                                    text=True, capture_output=True)
            self.assertEqual(result.returncode, 2)
            self.assertEqual(json.loads(result.stdout)['issues'][0]['code'], 'schema.setup')
            subprocess.run(command+['--help'], env=env, check=True, capture_output=True, text=True)

    def test_no_heavy_imports_or_file_open_for_supplied_metadata(self):
        program = '''
import builtins,sys
def forbidden(*args,**kwargs):raise AssertionError('unexpected input opening')
import rdfschema
builtins.open=forbidden
r=rdfschema.compare_schema([rdfschema.ColumnRequirement('event','ULong64_t')],{'event':'ULong64_t'})
assert r.passed
r=rdfschema.check_input_schemas(['root://example.invalid/input.root'],[],
 reader=lambda source,tree,names:rdfschema.InputSchema(source,tree,{}))
assert r.passed
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''
        subprocess.run([sys.executable, '-S', '-c', program], check=True, capture_output=True, text=True,
            env=dict(os.environ, PYTHONPATH=str(REPO/'core/python')))

    def test_reader_closes_root_file_on_metadata_failure_and_never_books_actions(self):
        for fail in (None, 'metadata', 'execution'):
            calls = []
            class Dataframe:
                def GetColumnNames(self): return ['event']
                def GetColumnType(self, name):
                    if fail == 'metadata': raise RuntimeError('cannot read branch metadata')
                    return 'ULong64_t'
                def GetNRuns(self): return int(fail == 'execution')
                def __del__(self): calls.append('dataframe released')
            handle = SimpleNamespace(IsZombie=lambda: False, Get=lambda tree:
                SimpleNamespace(InheritsFrom=lambda kind: kind == 'TTree'), Close=lambda: calls.append('closed'))
            root = SimpleNamespace(TFile=SimpleNamespace(Open=lambda *args: handle),
                                   RDataFrame=lambda tree: Dataframe())
            with self.subTest(fail=fail), patch.dict(sys.modules, {'ROOT': root}):
                if fail == 'execution':
                    with self.assertRaisesRegex(AssertionError, 'event loop'):
                        read_root_schema('input.root', 'Events', ['event'])
                elif fail:
                    with self.assertRaisesRegex(OSError, 'branch metadata'):
                        read_root_schema('input.root', 'Events', ['event'])
                else:
                    self.assertEqual(read_root_schema('input.root', 'Events', ['event']).columns['event'], 'ULong64_t')
            self.assertEqual(calls.count('closed'), 1)
            self.assertEqual(calls.count('dataframe released'), 1)
            if not fail:
                self.assertEqual(calls, ['dataframe released', 'closed'])

    def test_stub_matches_public_runtime_signatures(self):
        assert_stub_contract(self, 'rdfschema')


if __name__ == '__main__':
    unittest.main()
