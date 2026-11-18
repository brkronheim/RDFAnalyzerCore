"""Controller observations and completion semantics with stdlib-only children."""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import rdfworkflow
from rdfworkflow import CMakeBuildSpec, CppAnalysisSpec, read_command_status


class CommandProgressTests(unittest.TestCase):
    def spec(self, directory, code='pass', **kwargs):
        return CppAnalysisSpec(sys.executable, ('-S', '-c', code), working_directory=directory, **kwargs)

    def wait_for(self, path):
        deadline = time.monotonic()+5
        while not path.exists() and time.monotonic()<deadline:
            time.sleep(.01)
        self.assertTrue(path.exists(), str(path))

    def test_default_does_not_construct_observer_or_write_progress(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with patch('rdfworkflow._CommandProgress', side_effect=AssertionError('observer is opt-in')):
                result = self.spec(directory).run(directory/'logs')
            self.assertTrue(result.success)
            self.assertEqual(result.progress, {})
            self.assertFalse((directory/'logs/progress.json').exists())
            self.assertTrue(read_command_status(directory/'logs')['terminal'])

    def test_invalid_interval_fails_before_preparation_or_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            spec = self.spec(directory)
            prepared = spec.prepare()
            build = CMakeBuildSpec(str(directory/'not-configured'), 'analysis')
            for value in (True, 0, -.5, .1, 3601, float('nan'), '5'):
                for operation in (spec.run, prepared.run, build.build):
                    with self.subTest(value=value, operation=operation), self.assertRaisesRegex(ValueError, 'progress_interval_s'):
                        operation(directory/'logs', progress_interval_s=value)
            self.assertFalse((directory/'logs').exists())

    def test_periodic_snapshots_are_throttled_and_fixed_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            observer = rdfworkflow._CommandProgress(Path(temporary), 1., 'analysis', 0.)
            writes = []
            clock = [0.]
            with patch('rdfworkflow.time.perf_counter', side_effect=lambda: clock[0]), \
                    patch('rdfworkflow.write_json', side_effect=lambda path, record: writes.append(dict(record))):
                observer.emit('launching')
                observer.emit('running', process=SimpleNamespace(pid=12, returncode=None))
                for clock[0] in (.1, .5, .999): observer.emit('running', periodic=True)
                clock[0] = 1.; observer.emit('running', periodic=True)
                clock[0] = 1.1; observer.emit('finalizing')
                clock[0] = 1.2; observer.emit('finished', status='succeeded')
            self.assertEqual([record['sequence'] for record in writes], [1, 2, 3, 4, 5])
            self.assertEqual([record['phase'] for record in writes], ['launching','running','running','finalizing','finished'])
            for record in writes:
                self.assertFalse({'events', 'percent', 'command', 'environment'} & record.keys())

    def test_live_status_is_observational_and_final_record_takes_priority(self):
        with tempfile.TemporaryDirectory() as temporary, ThreadPoolExecutor(max_workers=1) as pool:
            directory = Path(temporary); ready = directory/'ready'; release = directory/'release'
            code = ("from pathlib import Path;import time;Path('ready').touch();"
                "\nwhile not Path('release').exists():time.sleep(.02)\n")
            future = pool.submit(self.spec(directory, code).run, directory/'logs', progress_interval_s=.5)
            try:
                self.wait_for(ready)
                status = read_command_status(directory/'logs')
                deadline = time.monotonic()+5
                while status['phase']!='running' and time.monotonic()<deadline:
                    time.sleep(.01)
                    status = read_command_status(directory/'logs')
                self.assertFalse(status['terminal'])
                self.assertEqual(status['phase'], 'running')
                self.assertEqual(status['operation'], 'analysis')
            finally:
                release.touch()
            result = future.result(timeout=5)
            self.assertTrue(result.success)
            snapshot = json.loads((directory/'logs/progress.json').read_text())
            self.assertEqual(snapshot['phase'], 'finished')
            self.assertEqual(snapshot['status'], 'succeeded')
            (directory/'logs/progress.json').write_text('invalid observer JSON')
            with patch('rdfworkflow.subprocess.Popen', side_effect=AssertionError('status must not launch')), \
                    patch('rdfworkflow.os.kill', side_effect=AssertionError('status must not probe PID')):
                final = read_command_status(directory/'logs')
            self.assertTrue(final['terminal'])
            self.assertEqual(final['status'], 'succeeded')

    def test_zero_exit_missing_output_remains_failed_in_progress_and_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            result = self.spec(directory, outputs={'result': 'missing.txt'}).run(
                directory/'logs', check=False, progress_interval_s=.5)
            self.assertFalse(result.success)
            self.assertEqual(result.returncode, 0)
            self.assertEqual(read_command_status(directory/'logs')['status'], 'failed')
            self.assertEqual(json.loads((directory/'logs/progress.json').read_text())['status'], 'failed')

    def test_reporting_write_errors_never_change_child_result(self):
        original = rdfworkflow.write_json
        for fail_at, exitcode in (('first', 0), ('finished', 0), ('first', 7), ('finished', 7)):
            with self.subTest(fail_at=fail_at, exitcode=exitcode), tempfile.TemporaryDirectory() as temporary:
                directory = Path(temporary); attempts = []
                def write(path, record):
                    if Path(path).name == 'progress.json':
                        attempts.append(record['phase'])
                        if fail_at=='first' or record['phase']=='finished':
                            raise PermissionError('observer write refused')
                    return original(path, record)
                with patch('rdfworkflow.write_json', side_effect=write):
                    result = self.spec(directory, 'import sys;sys.exit('+str(exitcode)+')').run(
                        directory/'logs', progress_interval_s=.5, check=False)
                self.assertEqual(result.success, exitcode==0)
                self.assertEqual(result.returncode, exitcode)
                self.assertEqual(tuple(issue.code for issue in result.issues), () if exitcode==0 else ('failed',))
                self.assertIn('observer write refused', result.progress['write_error'])
                self.assertIn('observer write refused', json.loads((directory/'logs/run.json').read_text())['progress']['write_error'])
                self.assertTrue(read_command_status(directory/'logs')['terminal'])
                if fail_at=='first': self.assertEqual(attempts, ['launching'])

    def test_failed_diagnostic_refresh_preserves_committed_completion(self):
        original = rdfworkflow.write_json
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary); run_writes = []
            def write(path, record):
                if Path(path).name == 'progress.json' and record['phase']=='finished':
                    raise PermissionError('last snapshot refused')
                if Path(path).name == 'run.json':
                    run_writes.append(record)
                    if len(run_writes)>1: raise PermissionError('diagnostic refresh refused')
                return original(path, record)
            with patch('rdfworkflow.write_json', side_effect=write):
                result = self.spec(directory).run(directory/'logs', progress_interval_s=.5)
            self.assertTrue(result.success)
            self.assertEqual(read_command_status(directory/'logs')['status'], 'succeeded')
            self.assertIn('last snapshot refused', result.progress['write_error'])

    def test_no_terminal_success_before_completion_record_commits(self):
        original = rdfworkflow.write_json
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            def write(path, record):
                if Path(path).name == 'run.json': raise PermissionError('completion write refused')
                return original(path, record)
            with patch('rdfworkflow.write_json', side_effect=write), self.assertRaises(PermissionError):
                self.spec(directory).run(directory/'logs', progress_interval_s=.5)
            status = read_command_status(directory/'logs')
            self.assertFalse(status['terminal'])
            self.assertEqual(status['phase'], 'finalizing')
            self.assertEqual(status['status'], 'running')

    def test_timeout_and_cancellation_keep_distinct_final_statuses(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            result = self.spec(directory, 'import time;time.sleep(30)', timeout_s=.2,
                terminate_grace_s=.1).run(directory/'timeout', check=False, progress_interval_s=.5)
            self.assertEqual(result.status, 'timed_out')
            self.assertEqual(read_command_status(directory/'timeout')['status'], 'timed_out')
            cancelled = threading.Event(); cancelled.set()
            with patch('rdfworkflow.subprocess.Popen', side_effect=AssertionError('cancelled before launch')):
                result = self.spec(directory).run(directory/'cancel', check=False,
                    cancel_event=cancelled, progress_interval_s=.5)
            self.assertEqual(result.status, 'cancelled')
            self.assertEqual(read_command_status(directory/'cancel')['status'], 'cancelled')
            self.assertIsNone(result.returncode)

    def test_active_cancellation_reports_stopping_and_reaps_owned_child(self):
        with tempfile.TemporaryDirectory() as temporary, ThreadPoolExecutor(max_workers=1) as pool:
            directory = Path(temporary); cancel = threading.Event()
            code = "from pathlib import Path;import time;Path('ready').touch();time.sleep(30)"
            with patch('rdfworkflow.write_json', wraps=rdfworkflow.write_json) as writes:
                future = pool.submit(self.spec(directory, code, terminate_grace_s=.1).run,
                    directory/'logs', check=False, progress_interval_s=.5, cancel_event=cancel)
                try:
                    self.wait_for(directory/'ready')
                finally:
                    cancel.set()
                result = future.result(timeout=5)
            self.assertEqual(result.status, 'cancelled')
            self.assertIsNotNone(result.returncode)
            snapshots = [call.args[1] for call in writes.call_args_list if Path(call.args[0]).name=='progress.json']
            self.assertTrue(any(record['phase']=='stopping' and record['requested_status']=='cancelled' for record in snapshots))
            self.assertEqual(read_command_status(directory/'logs')['status'], 'cancelled')

    def test_stale_or_invalid_records_are_not_inferred_as_completed(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            with self.assertRaises(FileNotFoundError): read_command_status(directory)
            progress = directory/'progress.json'
            progress.write_text(json.dumps({'schema':'rdf-command-progress-v1','phase':'finished','status':'succeeded'}))
            self.assertEqual(read_command_status(directory)['status'], 'unconfirmed')
            self.assertFalse(read_command_status(directory)['terminal'])
            progress.write_text('[]')
            with self.assertRaises(ValueError): read_command_status(directory)
            (directory/'run.json').write_text(json.dumps({'schema':'rdf-command-result-v1','status':'failed','success':True}))
            with self.assertRaises(ValueError): read_command_status(directory)

    def test_cli_status_reads_failed_commands_without_executing_them(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary); spec = directory/'spec.json'
            spec.write_text(json.dumps({'executable':sys.executable,
                'arguments':['-S','-c','import sys;sys.exit(7)']}))
            env = dict(os.environ, PYTHONPATH=str(Path(rdfworkflow.__file__).parent))
            result = subprocess.run([sys.executable,'-S','-m','rdfworkflow','run',str(spec),
                '--directory',str(directory/'logs'),'--progress-interval','.5'], env=env,
                capture_output=True, text=True)
            self.assertEqual(result.returncode, 1, result.stderr)
            status = subprocess.run([sys.executable,'-S','-m','rdfworkflow','status',str(directory/'logs')],
                env=env, capture_output=True, text=True, check=True)
            self.assertEqual(json.loads(status.stdout)['status'], 'failed')
            self.assertEqual(json.loads(status.stdout)['returncode'], 7)
            invalid = subprocess.run([sys.executable,'-S','-m','rdfworkflow','status',str(directory/'absent')],
                env=env, capture_output=True, text=True)
            self.assertEqual(invalid.returncode, 1, invalid.stderr)
            self.assertFalse(json.loads(invalid.stdout)['passed'])


if __name__ == '__main__': unittest.main()
