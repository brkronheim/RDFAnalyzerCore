"""Observable contracts of lightweight compiled-analysis orchestration."""
import concurrent.futures
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest

from rdfworkflow import CMakeBuildSpec, CppAnalysisSpec, ExecutionError


class CompiledWorkflowTests(unittest.TestCase):
    def test_planning_imports_no_native_or_numeric_stack(self):
        code = """
import json,sys
from rdfworkflow import CppAnalysisSpec
from rdfanalysis import AnalysisConfig
cpp=CppAnalysisSpec('does-not-exist',arguments=('literal',))
assert cpp.validate().passed
assert not cpp.validate('assets').passed
assert not cpp.validate('graph').passed
assert cpp.describe().as_dict()['backend']=='compiled-executable'
py=AnalysisConfig(files=['missing.root'],output='unused.root')
assert py.describe().as_dict()['details']['graph'].startswith('not constructed')
assert py.validate().passed and not py.validate('graph').passed
assert not {'ROOT','rdfanalyzer','numba','numpy','yaml'}.intersection(sys.modules)
print('clean')
"""
        env=dict(os.environ, PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,capture_output=True,text=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')

    def test_bad_settings_and_references_fail_without_execution(self):
        for settings in ({'arguments':'--arg'}, {'arguments':['{asset:missing}']},
                {'timeout_s':True}, {'timeout_s':float('nan')}, {'terminate_grace_s':61},
                {'environment':{'a=b':'x'}}, {'outputs':{'bad name':'x'}}):
            with self.subTest(settings=settings),self.assertRaises(ValueError):
                CppAnalysisSpec('missing',**settings)
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            with self.assertRaisesRegex(ValueError,'overwrite'):
                CppAnalysisSpec(sys.executable,working_directory=temporary,
                    assets={'config':'input.txt'},outputs={'alias':'sub/../input.txt'})

    def test_spec_file_paths_and_literal_arguments(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);path=directory/'spec.json'
            path.write_text(json.dumps({'executable':'analysis','working_directory':'work',
                'assets':{'config':'input.txt'},'outputs':{'summary':'result.json'},
                'arguments':['{asset:config}','{output:summary}','{"literal": 3}']}))
            spec=CppAnalysisSpec.from_file(path)
            self.assertEqual(spec.executable,str(directory/'analysis'))
            self.assertEqual(spec.command()[1],str(directory/'work/input.txt'))
            self.assertEqual(spec.command()[3],'{"literal": 3}')
            path.write_text('{"exectuable":"typo"}')
            with self.assertRaisesRegex(ValueError,'unknown fields.*exectuable'):
                CppAnalysisSpec.from_file(path)

    def test_concurrent_children_do_not_mutate_parent_cwd_or_environment(self):
        before_cwd=os.getcwd();before_environment=dict(os.environ)
        with tempfile.TemporaryDirectory(prefix='rdf workflow ') as temporary:
            directory=Path(temporary)
            def launch(index):
                work=directory/str(index);work.mkdir()
                code="import json,os,sys;open(sys.argv[1],'w').write(json.dumps({'cwd':os.getcwd(),'label':os.environ['RDF_WORKFLOW_TEST_LABEL'],'literal':sys.argv[2]}));print('x'*65536)"
                spec=CppAnalysisSpec(sys.executable,arguments=('-c',code,'{output:summary}','$(literal); space'),
                    working_directory=str(work),outputs={'summary':'result.json'},
                    environment={'RDF_WORKFLOW_TEST_LABEL':str(index)})
                result=spec.prepare().run(work/'logs')
                return result,json.loads((work/'result.json').read_text())
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                results=list(pool.map(launch,range(2)))
            for index,(result,data) in enumerate(results):
                self.assertTrue(result.success)
                self.assertEqual(data,{'cwd':str(directory/str(index)),'label':str(index),'literal':'$(literal); space'})
                self.assertGreater((Path(result.log_directory)/'stdout.log').stat().st_size,65536)
                record=json.loads((Path(result.log_directory)/'run.json').read_text())
                self.assertNotIn('RDF_WORKFLOW_TEST_LABEL',record.get('environment',{}))
                self.assertTrue(record['artifacts']['files']['executable']['sha256'])
        self.assertEqual(os.getcwd(),before_cwd)
        self.assertEqual(dict(os.environ),before_environment)

    def test_changed_prepared_asset_rejected_before_launch(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);asset=directory/'config.txt';asset.write_text('first')
            spec=CppAnalysisSpec(sys.executable,('-c','pass'),working_directory=temporary,assets={'config':str(asset)})
            prepared=spec.prepare();asset.write_text('changed')
            with self.assertRaisesRegex(ValueError,'artifact changed'):
                prepared.run(directory/'logs')
            self.assertFalse((directory/'logs').exists())
            with self.assertRaises(TypeError):prepared.artifacts['executable']['sha256']='changed'

    def test_one_shot_run_uses_current_declared_asset_and_records_policy(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            (directory/'input.txt').write_text('current settings')
            code="import pathlib,sys;pathlib.Path(sys.argv[2]).write_text(pathlib.Path(sys.argv[1]).read_text())"
            spec=CppAnalysisSpec(sys.executable,('-c',code,'{asset:config}','{output:result}'),
                working_directory=temporary,assets={'config':'input.txt'},outputs={'result':'result.txt'})
            result=spec.run(directory/'one-shot')
            self.assertTrue(result.success)
            self.assertEqual((directory/'result.txt').read_text(),'current settings')
            record=json.loads((directory/'one-shot/run.json').read_text())
            self.assertEqual(record['artifacts']['verification_policy'],'prepare-immediately-before-launch')
            self.assertEqual(record['artifacts']['verification_s'],0)

    def test_executable_permission_is_not_changed(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'analysis';path.write_text('not executable');path.chmod(0o600)
            spec=CppAnalysisSpec(str(path))
            self.assertIn('not_executable',[issue.code for issue in spec.validate('assets').issues])
            with self.assertRaisesRegex(ValueError,'execute permission'):spec.prepare()
            self.assertEqual(path.stat().st_mode & 0o777,0o600)

    def test_existing_outputs_are_never_accepted_as_new_success(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'result.json';path.write_text('old')
            spec=CppAnalysisSpec(sys.executable,('-c','pass'),working_directory=temporary,outputs={'summary':str(path)})
            with self.assertRaisesRegex(ValueError,'Refusing to overwrite'):spec.prepare()
            self.assertEqual(path.read_text(),'old')

    def test_zero_exit_with_missing_declared_output_fails(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            spec=CppAnalysisSpec(sys.executable,('-c','pass'),working_directory=temporary,outputs={'summary':'missing.json'})
            with self.assertRaises(ExecutionError) as caught:spec.prepare().run(directory/'logs')
            result=caught.exception.result
            self.assertFalse(result.success);self.assertEqual(result.returncode,0)
            self.assertEqual(result.issues[0].code,'missing_output')
            self.assertFalse(json.loads((directory/'logs/run.json').read_text())['success'])

    def test_nonzero_exit_keeps_logs_and_result(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary)
            spec=CppAnalysisSpec(sys.executable,('-c',"import sys;print('reason',file=sys.stderr);sys.exit(7)"))
            result=spec.prepare().run(directory/'logs',check=False)
            self.assertEqual(result.returncode,7);self.assertFalse(result.success)
            self.assertIn('reason',(directory/'logs/stderr.log').read_text())
            with self.assertRaisesRegex(ExecutionError,'returncode=7'):result.raise_for_status()

    @unittest.skipUnless(os.name=='posix' and Path('/proc').is_dir(),'Linux process-group validation')
    def test_timeout_kills_descendant_even_if_leader_exits_on_term(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);pidfile=directory/'descendant.pid'
            child="import signal,time;signal.signal(signal.SIGTERM,signal.SIG_IGN);time.sleep(120)"
            code="import pathlib,signal,subprocess,sys,time;child=subprocess.Popen([sys.executable,'-c',sys.argv[1]]);pathlib.Path(sys.argv[2]).write_text(str(child.pid));signal.signal(signal.SIGTERM,lambda *args:sys.exit(0));time.sleep(120)"
            spec=CppAnalysisSpec(sys.executable,('-c',code,child,str(pidfile)),timeout_s=1,terminate_grace_s=.1)
            result=spec.prepare().run(directory/'logs',check=False)
            self.assertEqual(result.status,'timed_out')
            self.assertTrue(pidfile.exists())
            state=Path('/proc')/pidfile.read_text()/'stat'
            def alive():
                try:
                    return state.read_text().split()[2]!='Z'
                except (FileNotFoundError,ProcessLookupError):
                    # A reaped descendant may disappear between /proc lookup
                    # and read; that is the successful cleanup outcome.
                    return False
            deadline=time.monotonic()+1
            while alive() and time.monotonic()<deadline:
                time.sleep(.01)
            self.assertFalse(alive())

    def test_explicit_cancellation_records_failure_and_reaps_child(self):
        with tempfile.TemporaryDirectory() as temporary:
            cancel=threading.Event();timer=threading.Timer(.2,cancel.set);timer.start()
            try:
                spec=CppAnalysisSpec(sys.executable,('-c','import time;time.sleep(120)'),terminate_grace_s=.1)
                result=spec.prepare().run(Path(temporary)/'logs',check=False,cancel_event=cancel)
                self.assertEqual(result.status,'cancelled');self.assertIsNotNone(result.returncode)
            finally:timer.join()

    def test_log_directory_is_not_reused(self):
        with tempfile.TemporaryDirectory() as temporary:
            prepared=CppAnalysisSpec(sys.executable,('-c','pass')).prepare()
            prepared.run(Path(temporary)/'logs')
            with self.assertRaises(FileExistsError):prepared.run(Path(temporary)/'logs')

    @unittest.skipUnless(os.name=='posix','POSIX CLI termination')
    def test_cli_term_cleans_up_child_and_records_cancellation(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);ready=directory/'ready'
            spec=directory/'spec.json'
            spec.write_text(json.dumps({'executable':sys.executable,
                'arguments':['-c',f"import pathlib,time;pathlib.Path({str(ready)!r}).touch();time.sleep(120)"],
                'terminate_grace_s':.1}))
            process=subprocess.Popen([sys.executable,'-m','rdfworkflow','run',str(spec),'--directory',str(directory/'logs')],
                stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            try:
                deadline=time.monotonic()+5
                while not ready.exists() and time.monotonic()<deadline and process.poll() is None:time.sleep(.01)
                self.assertTrue(ready.exists())
                process.send_signal(signal.SIGTERM)
                _,stderr=process.communicate(timeout=5)
                self.assertEqual(process.returncode,128+signal.SIGTERM,stderr)
                self.assertEqual(json.loads((directory/'logs/run.json').read_text())['status'],'cancelled')
            finally:
                if process.poll() is None:process.kill();process.wait()

    @unittest.skipUnless(shutil.which('cmake') and shutil.which('c++'),'CMake and C++ compiler required')
    def test_build_and_run_linked_cpp_translation_units(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);source=directory/'source';source.mkdir();build=directory/'build'
            (source/'CMakeLists.txt').write_text('cmake_minimum_required(VERSION 3.16)\nproject(HybridFixture LANGUAGES CXX)\nadd_library(physics STATIC physics.cc)\nadd_executable(analysis main.cc)\ntarget_link_libraries(analysis PRIVATE physics)\n')
            (source/'physics.cc').write_text('int answer(){return 42;}\n')
            (source/'main.cc').write_text('#include <iostream>\nint answer();int main(){std::cout<<answer();}\n')
            subprocess.run(['cmake','-S',str(source),'-B',str(build)],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
            builder=CMakeBuildSpec(str(build),'analysis',parallel=1)
            self.assertTrue(builder.build(directory/'build-logs').success)
            spec=CppAnalysisSpec(str(build/'analysis'),working_directory=temporary,build_spec=builder)
            prepared=spec.prepare()
            first=prepared.run(directory/'first-run')
            self.assertEqual((Path(first.log_directory)/'stdout.log').read_text(),'42')
            (source/'physics.cc').write_text('int answer(){return 43;}\n')
            builder.build(directory/'rebuild-logs')
            with self.assertRaisesRegex(ValueError,'artifact changed'):
                prepared.run(directory/'stale-run')
            second=spec.prepare().run(directory/'second-run')
            self.assertEqual((Path(second.log_directory)/'stdout.log').read_text(),'43')
            (source/'physics.cc').write_text('invalid C++ source')
            with self.assertRaises(ExecutionError):builder.build(directory/'failed-build')


if __name__=='__main__':
    unittest.main()
