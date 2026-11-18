"""Detached results, failure scopes and lazy lifecycle behavior without ROOT."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import ANY,patch

from rdfanalysis import Analysis, AnalysisConfig, AnalysisResult
from rdfresults import RunResult
from rdfworkflow import CppAnalysisSpec, CommandResult, ValidationIssue, _execute


class RunResultTests(unittest.TestCase):
    def test_supplied_native_envelope_round_trip_and_unknown_values_are_preserved(self):
        original=RunResult('compiled-analysis','succeeded','caller-reported native lifecycle',
            counts={'input':4,'selected':2},sums={'signed':-2.5},
            histograms={'pt':{'edges':[0.,1.],'sumw':[-1.,2.,0.],'sumw2':[1.,4.,0.]}},
            timings={'execute_s':.125},outputs={'metadata':'unavailable.root'},
            provenance={'build_identity':'caller-owned-id'},details={'optional':None})
        document=json.loads(json.dumps(original.as_dict()))
        with patch.object(Path,'open',side_effect=AssertionError('must not open paths')):
            restored=RunResult.from_dict(document)
        self.assertEqual(restored.as_dict(),original.as_dict())
        self.assertIsNone(restored.event_loops)
        document['histograms']['pt']['sumw'][0]=99.
        self.assertEqual(restored.histograms['pt']['sumw'][0],-1.)
        self.assertEqual(RunResult.from_dict(RunResult('opaque','failed','process only').as_dict()).counts,{})

    def test_envelope_receiver_rejects_unknown_incomplete_or_contradictory_claims(self):
        base=RunResult('native','succeeded','explicit scope').as_dict()
        bad=[None,[],{**base,'schema':'rdf-run-result-v2'},{**base,'schema':ANY},{**base,'status':ANY},
             {**base,'unexpected':1},{**base,'success':False},{**base,'success':1},
             {**base,'counts':{'input':True}},{**base,'timings':{'execute_s':float('inf')}},
             {**base,'event_loops':-1},{**base,'histograms':{'native':object()}},
             {**base,1:'non-string key'},{key:value for key,value in base.items() if key!='event_loops'}]
        for document in bad:
            with self.subTest(document=document),self.assertRaises((TypeError,ValueError)):
                RunResult.from_dict(document)
        for status in ('failed','timed_out','cancelled'):
            with self.assertRaisesRegex(ValueError,'disagrees'):
                RunResult.from_dict({**base,'status':status})

    def test_imports_and_serialization_do_not_load_event_runtime_or_read_outputs(self):
        code = '''
import json,sys
from rdfresults import RunResult
from rdfanalysis import AnalysisResult
from rdfworkflow import CommandResult
result=RunResult('custom','succeeded','custom operation returned', outputs={'skim':'missing.root'})
assert json.loads(json.dumps(result.as_dict()))['schema']=='rdf-run-result-v1'
assert RunResult.from_dict(result.as_dict()).as_dict()==result.as_dict()
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
'''
        subprocess.run([sys.executable, '-S', '-c', code], check=True,
            env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python')))

    def test_snapshot_is_deeply_detached_and_serialization_returns_mutable_copy(self):
        histogram = {'ht': {'sumw': [-2., 0., 4.], 'sumw2': [4., 0., 16.]}}
        result = RunResult('custom', 'succeeded', 'declared test scope',
            counts={'selected': 0}, histograms=histogram, provenance={'custom': ['id']})
        histogram['ht']['sumw'][0] = 99.
        self.assertEqual(result.histograms['ht']['sumw'][0], -2.)
        with self.assertRaises(TypeError):
            result.histograms['ht']['sumw'][0] = 12.
        document = result.as_dict(); document['histograms']['ht']['sumw'][0] = 33.
        self.assertEqual(result.histograms['ht']['sumw'][0], -2.)
        self.assertEqual(result.counts['selected'], 0)
        self.assertIsNone(result.event_loops)
        self.assertEqual(dict(RunResult('opaque', 'failed', 'test').counts), {})

    def test_invalid_values_do_not_silently_convert_types_or_native_objects(self):
        for values in ({'counts': {'input': True}}, {'counts': {'input': -1}},
                {'timings': {'execute_s': -1}}, {'timings': {'execute_s': float('nan')}},
                {'details': {'native': object()}}, {'event_loops': True},
                {'outputs': {'result': None}}, {'status': 'running'}, {'status': ANY},
                {'counts': {1: 2}}, {'issues': [object()]}):
            with self.subTest(values=values), self.assertRaises((TypeError, ValueError)):
                RunResult(**dict({'backend': 'custom', 'status': 'succeeded', 'completion_scope': 'test'}, **values))

    def analysis(self, error=None):
        analysis = Analysis.__new__(Analysis)
        analysis.config = AnalysisConfig(files=['missing.root'], output='missing-skim.root', skim=False)
        analysis._module_active = analysis._module_failure = analysis._stage_failure = None
        analysis._run_status = analysis._run_wall_s = None
        analysis._compilation_stats = {'numba_kernels_compiled': 0}
        analysis._directory = SimpleNamespace(cleanup=lambda: None)
        class Native:
            event_loops = 0
            reads = 0
            def run(self):
                self.event_loops += 1
                if error is not None:
                    raise error
            @property
            def counts(self):
                self.reads += 1
                return {'before': 10, 'after': 3}
            @property
            def histograms(self):
                self.reads += 1
                return {'ht': {'sumw': [-1., 2.], 'sumw2': [1., 4.]}}
            @property
            def sums(self):
                self.reads += 1
                return {'signed': -4.}
        analysis.native = Native()
        return analysis

    def test_result_requires_success_and_run_does_not_collect_actions_eagerly(self):
        analysis = self.analysis()
        with self.assertRaisesRegex(RuntimeError, 'successful'):
            analysis.result()
        self.assertEqual(analysis.native.reads, 0)
        self.assertEqual(analysis.native.event_loops, 0)
        self.assertIs(analysis.run(), analysis)
        self.assertEqual(analysis.native.reads, 0)
        with patch.object(Path, 'open', side_effect=AssertionError('must not read output')):
            result = analysis.result()
        self.assertEqual(result.counts, {'before': 10, 'after': 3})
        self.assertEqual(result.sums['signed'], -4.)
        self.assertEqual(result.event_loops, 1)
        self.assertEqual(set(result.outputs), {'metadata'})
        self.assertEqual(result.provenance, {})
        self.assertGreaterEqual(result.timings['execute_s'], 0)
        with self.assertRaisesRegex(RuntimeError, 'already been attempted'):
            analysis.run()
        self.assertTrue(analysis.result().success)
        del analysis
        self.assertEqual(result.counts['after'], 3)

    def test_native_failure_preserves_cause_and_never_exposes_success(self):
        error = ValueError('native setup/finalization failure')
        analysis = self.analysis(error)
        with self.assertRaises(ValueError) as caught:
            analysis.run()
        self.assertIs(caught.exception, error)
        with self.assertRaisesRegex(RuntimeError, 'successful'):
            analysis.result()
        with self.assertRaisesRegex(RuntimeError, 'already been attempted'):
            analysis.run()
        self.assertEqual(analysis.native.reads, 0)

    def test_disabled_metadata_is_unavailable_instead_of_a_false_output_reference(self):
        analysis = self.analysis()
        analysis.config = AnalysisConfig(files=['missing.root'], output='unused.root',
            skim=False, options={'metaFile': ''})
        analysis.run()
        self.assertEqual(analysis.result().outputs, {})

    def test_legacy_session_result_remains_compatible_and_common_copy_is_detached(self):
        old = AnalysisResult({'input': 10}, {'ht': {'sumw': [2.]}}, {'signed': -2.},
            {'numba_kernels_compiled': 1}, {'setup_s': .1, 'execute_s': .3, 'total_s': .4},
            1, None, 'missing-meta.root')
        before = old.as_dict(); result = old.to_run_result()
        self.assertEqual(old.as_dict(), before)
        old.histograms['ht']['sumw'][0] = 99.
        self.assertEqual(result.histograms['ht']['sumw'][0], 2.)
        self.assertEqual(result.backend, 'python-numba-session')
        self.assertEqual(result.timings['total_s'], .4)

    def test_command_failure_timeout_and_cancel_status_stay_distinct(self):
        for status in ('failed', 'timed_out', 'cancelled'):
            command = CommandResult(status, -15, ('missing',), .2, 'logs',
                {'summary': 'missing-output'}, (ValidationIssue(status, 'stopped', 'execution'),))
            result = command.to_run_result()
            self.assertFalse(result.success)
            self.assertEqual(result.status, status)
            self.assertEqual(result.issues[0]['code'], status)
            self.assertEqual(dict(result.counts), {})
            self.assertIn('separate', result.completion_scope)

    def test_real_stdlib_child_preserves_run_json_and_captured_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            code = "from pathlib import Path;Path('summary.txt').write_text('opaque native output')"
            spec = CppAnalysisSpec(sys.executable, ('-S', '-c', code),
                working_directory=directory, outputs={'summary': 'summary.txt'})
            command = spec.run(directory/'logs')
            record = json.loads((directory/'logs/run.json').read_text())
            with patch.object(Path, 'open', side_effect=AssertionError('no rereading run/output files')):
                result = command.to_run_result()
            self.assertEqual(command.as_dict()['schema'], 'rdf-command-result-v1')
            self.assertEqual(result.provenance['artifact_identity'], record['artifacts']['artifact_identity'])
            self.assertTrue(result.success)
            self.assertEqual(result.histograms, {})
            self.assertEqual(result.backend, 'compiled-executable')
            self.assertEqual(record['operation'], 'analysis')
            self.assertEqual(record['provenance'], dict(result.provenance))

    def test_build_and_analysis_command_results_have_distinct_roles(self):
        command = CommandResult('succeeded', 0, ('cmake', '--build', 'build'), .1, 'logs', operation='build')
        result = command.to_run_result()
        self.assertEqual(result.backend, 'cmake-build')
        self.assertEqual(result.details['operation'], 'build')
        self.assertEqual(result.counts, {})

    def test_finalized_build_manifest_identity_survives_without_reopening_manifest(self):
        # Exercise build-finalization recording with a stdlib child, no CMake
        # invocation or compilation during the user's benchmark hold.
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            command = _execute([sys.executable, '-S', '-c', 'pass'], directory/'logs',
                cwd=directory, environment=dict(os.environ), timeout_s=10, grace_s=.1,
                operation='build', finalize_artifacts=lambda: {
                    'build_manifest': {'artifact_id': 'recorded-build-id', 'path': 'not-opened.json'}})
            result = command.to_run_result()
            self.assertEqual(result.backend, 'cmake-build')
            self.assertEqual(result.provenance['build_identity'], 'recorded-build-id')

    def test_missing_declared_output_cannot_be_adapted_into_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            command = CppAnalysisSpec(sys.executable, ('-S', '-c', 'pass'),
                working_directory=directory, outputs={'summary': 'absent.json'}).run(directory/'logs', check=False)
            result = command.to_run_result()
            self.assertEqual(result.status, 'failed')
            self.assertEqual(result.details['returncode'], 0)
            self.assertEqual(result.issues[0]['code'], 'missing_output')


if __name__ == '__main__':
    unittest.main()
