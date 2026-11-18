"""Source/mock contracts only; no compiler, subprocess, ROOT import or event loop."""
from contextlib import redirect_stdout, redirect_stderr
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
EXAMPLE = HERE.parent
RECEIVER = HERE.parents[2] / 'core/python'


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module


steer = load('native_example_steer_unit', EXAMPLE / 'steer.py')
validate = load('native_example_validate_unit', HERE / 'validate.py')


def payload():
    return dict(schema='rdf-run-result-v1', backend='linked-native-cpp', status='succeeded', success=True,
                completion_scope='synthetic unit payload only', **validate.oracle(), timings=dict(execute_and_materialize_s=.1),
                outputs={}, provenance=dict(fixture='generated-eight-events-v1'), issues=[], event_loops=1,
                details=dict(before_event_loops=0, actions_booked=6, histogram_entries=5))


class ExampleContracts(unittest.TestCase):
    def test_default_python_entrypoints_have_no_io_or_native_launch(self):
        for module in (steer, validate):
            with self.subTest(module=module.__name__), patch('sys.argv', [module.__name__]), \
                    patch.object(Path, 'open', side_effect=AssertionError('no file I/O')), \
                    patch.object(subprocess, 'run', side_effect=AssertionError('no child')), redirect_stdout(io.StringIO()) as output:
                module.main()
            self.assertFalse(json.loads(output.getvalue())['native_launched'])

    def test_explicit_run_needs_executable_and_installed_receiver(self):
        for module in (steer, validate):
            with self.subTest(module=module.__name__), patch('sys.argv', [module.__name__, '--run']), \
                    patch.object(subprocess, 'run', side_effect=AssertionError('no child')), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    module.main()

    def test_independent_signed_flow_and_variance_oracle(self):
        result = validate.oracle()
        self.assertEqual(result['counts'], dict(input=8, selected=5))
        self.assertEqual(result['sums'], dict(input_weight=1.75, selected_weight=1., selected_observable=10.5))
        self.assertEqual(result['histograms']['observable']['sumw'], [2., -1., .5, 0., -.5])
        self.assertEqual(result['histograms']['observable']['sumw2'], [4., 1., .25, 0., 6.25])
        validate.check_physics(payload())

    def test_steering_receives_detached_native_envelope_and_keeps_exact_argv(self):
        completed = subprocess.CompletedProcess(['/mock/native', '--run'], 0, json.dumps(payload()), '')
        with patch.object(subprocess, 'run', return_value=completed) as run:
            result = steer.run_native('/mock/native', RECEIVER)
        self.assertEqual(result.as_dict(), payload())
        run.assert_called_once_with(['/mock/native', '--run'], capture_output=True, text=True, timeout=30)
        changed = result.as_dict(); changed['counts']['input'] = 99
        self.assertEqual(result.counts['input'], 8)

    def test_native_exit_and_receiver_schema_failures_do_not_become_success(self):
        for completed, error in ((subprocess.CompletedProcess([], 1, '', 'mock native error'), RuntimeError),
                                  (subprocess.CompletedProcess([], 0, '{"schema":"unknown"}', ''), ValueError)):
            with patch.object(subprocess, 'run', return_value=completed):
                with self.assertRaises(error):
                    steer.run_native('/mock/native', RECEIVER)

    def test_bad_flow_variance_or_supplied_loop_count_fails_oracle(self):
        for kind in ('variance', 'loop', 'preflight'):
            value = payload()
            if kind == 'variance': value['histograms']['observable']['sumw2'][4] = 0.
            elif kind == 'loop': value['event_loops'] = 2
            else: value['details']['before_event_loops'] = 1
            with self.assertRaises(ValueError): validate.check_physics(value)

    def mocked_validation(self, root, mismatch=False):
        executable = root / 'mock-native'; executable.write_text('unit placeholder, never executable\n')
        def run(command, **kwargs):
            self.assertEqual(kwargs['cwd'], root / 'output')
            self.assertNotIn('PYTHONPATH', kwargs['env'])
            if command[1] in ('--describe', '--preflight'):
                value = dict(schema='native-results-description-v1', event_loops=0, fixture_entries=8,
                             actions_booked=6, execution_requested=False)
            else:
                value = payload()
                if '--receiver-dir' in command:
                    self.assertIn('-S', command)
                    value['timings']['execute_and_materialize_s'] = .2
                    if mismatch: value['provenance']['fixture'] = 'changed'
            return subprocess.CompletedProcess(command, 0, json.dumps(value), '')
        with patch.object(subprocess, 'run', side_effect=run) as child:
            result = validate.validate(executable, RECEIVER, root / 'output')
        self.assertEqual(child.call_count, 4)
        return result

    def test_mocked_full_parity_excludes_only_timings_and_uses_fresh_cwd(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = self.mocked_validation(root)
            self.assertTrue(result['accepted'])
            self.assertFalse(result['accepted_for_performance'])
            self.assertEqual(result['excluded_parity_fields'], ['timings'])
            self.assertTrue((root / 'output/ACCEPTANCE.json').is_file())
            with self.assertRaises(FileExistsError): validate.validate(root / 'mock-native', RECEIVER, root / 'output')

    def test_non_timing_envelope_mismatch_still_fails_parity(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(ValueError, 'compiled results differ'):
                self.mocked_validation(Path(directory), mismatch=True)

    def test_source_checkout_selection_rejected_before_child_or_output(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); executable = root / 'mock-native'; executable.write_text('never executed')
            with patch.object(subprocess, 'run', side_effect=AssertionError('no child')):
                with self.assertRaisesRegex(ValueError, 'outside the checkout'):
                    validate.validate(executable, RECEIVER, root / 'output', HERE.parents[2])
            self.assertFalse((root / 'output').exists())

    def test_installed_steering_symlink_into_checkout_rejected_before_child(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            installed = root / 'installed'; tests = installed / 'tests'; tests.mkdir(parents=True)
            validator_file = tests / 'validate.py'; validator_file.write_bytes((HERE / 'validate.py').read_bytes())
            (installed / 'steer.py').symlink_to(EXAMPLE / 'steer.py')
            receiver_dir = root / 'python'; receiver_dir.mkdir(); (receiver_dir / 'rdfresults.py').write_text('# never imported\n')
            executable = root / 'mock-native'; executable.write_text('never executed')
            installed_validator = load('installed_symlink_guard_unit', validator_file)
            with patch.object(subprocess, 'run', side_effect=AssertionError('no child')):
                with self.assertRaisesRegex(ValueError, 'outside the checkout'):
                    installed_validator.validate(executable, receiver_dir, root / 'output', HERE.parents[2])
            self.assertFalse((root / 'output').exists())


if __name__ == '__main__':
    unittest.main()
