"""Recover only recorded terminal command claims, without workers or output I/O."""
import json
import contextlib
import io
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import ANY, patch

from rdfworkflow import CommandResult, CppAnalysisSpec, ValidationIssue, read_command_result, _recorded_command_result, main

REPO = Path(__file__).resolve().parents[3]


def record(status='succeeded', code=0, operation='analysis'):
    return CommandResult(status, code, ('unused-executable', '--arg', ''), .125,
        'original/missing-logs', outputs={'summary':'never-opened.root'},
        provenance={'artifact_identity':'recorded-artifact','build_identity':'recorded-build'},
        operation=operation, progress={'enabled':True,'path':'never-opened-progress.json',
        'interval_s':.5,'write_error':None}).as_dict()


class RecordedCommandTests(unittest.TestCase):
    def test_terminal_statuses_operations_scope_and_unknown_summaries(self):
        for status,code in [('succeeded',0),('failed',0),('failed',7),('failed',None),
                            ('timed_out',None),('timed_out',-15),('cancelled',None),('cancelled',-15)]:
            for operation,backend in [('analysis','compiled-executable'),('build','cmake-build'),
                                      ('command','command'),('analysis-owned-task','command')]:
                document=record(status,code,operation)
                document['issues']=[ValidationIssue('reported','caller message','execution').as_dict()] if status!='succeeded' else []
                result=_recorded_command_result(document)
                self.assertEqual(result.backend,backend)
                self.assertEqual(result.status,status);self.assertEqual(result.success,status=='succeeded')
                self.assertEqual(result.details['operation'],operation)
                self.assertEqual(result.details['returncode'],code)
                self.assertEqual(result.details['command'],('unused-executable','--arg',''))
                self.assertEqual(result.details['log_directory'],'original/missing-logs')
                self.assertEqual(result.provenance['build_identity'],'recorded-build')
                self.assertIn('validation is separate',result.completion_scope)
                self.assertEqual((dict(result.counts),dict(result.sums),dict(result.histograms)),({},{},{}))
                self.assertIsNone(result.event_loops)

    def test_legacy_optional_omissions_remain_unknown_and_artifacts_do_not_infer_identity(self):
        document=record()
        for name in ('operation','progress','provenance'):document.pop(name)
        document['artifacts']={'build_manifest':{'artifact_id':'not-provenance','path':'not-opened.json'},
                               'declared_inputs':{'payload':{'path':'unavailable.so','sha256':'opaque'}}}
        result=_recorded_command_result(document)
        self.assertEqual(result.backend,'command');self.assertEqual(result.details['operation'],'command')
        self.assertEqual(result.provenance,{})
        self.assertEqual(result.details['progress'],{})
        self.assertEqual(result.details['recorded_artifacts']['build_manifest']['artifact_id'],'not-provenance')
        self.assertIsNone(result.event_loops)
        self.assertNotIn('recorded_artifacts',_recorded_command_result(record()).details)

    def test_malformed_incomplete_and_non_json_values_fail(self):
        base=record()
        for key in base:
            if key in ('operation','progress','provenance'):continue
            with self.subTest(missing=key),self.assertRaises((TypeError,ValueError)):
                _recorded_command_result({name:value for name,value in base.items() if name!=key})
        bad=[None,[],{**base,1:'bad key'},{**base,'schema':ANY},{**base,'schema':'rdf-run-result-v1'},
             {**base,'status':ANY},{**base,'status':'running'},{**base,'success':1},{**base,'success':False},
             {**base,'returncode':None},{**base,'returncode':2},{**base,'returncode':True},
             {**base,'command':[]},{**base,'command':'command string'},{**base,'command':['exe',object()]},
             {**base,'command':['exe','bad\0arg']},{**base,'log_directory':None},
             {**base,'completion_scope':'physics validated'},{**base,'completion_scope':ANY},
             {**base,'operation':None},{**base,'operation':ANY},{**base,'operation':''},
             {**base,'wall_s':float('nan')},{**base,'wall_s':float('inf')},{**base,'wall_s':-1.},
             {**base,'wall_s':True},{**base,'wall_s':'1.0'},{**base,'outputs':{'x':None}},
             {**base,'progress':None},{**base,'provenance':{'native':object()}},
             {**base,'artifacts':{'native':object()}},{**base,'artifacts':[]},
             {**base,'artifacts':{'duration':float('inf')}},{**base,'issues':[object()]},
             {**base,'issues':[{'code':'bad','message':'missing location'}]},
             {**base,'issues':[{'code':'bad','message':object(),'location':''}]},
             {**base,'counts':{'input':1}}]
        for document in bad:
            with self.subTest(document=document),self.assertRaises((TypeError,ValueError)):
                _recorded_command_result(document)

    def test_detached_result_copies_nested_source_and_remains_immutable(self):
        document=record();document['provenance']['extra']=['recorded']
        document['artifacts']={'nested':{'values':[1,2]}}
        result=_recorded_command_result(document)
        document['command'][0]='mutated';document['provenance']['extra'][0]='mutated'
        document['artifacts']['nested']['values'][0]=99
        self.assertEqual(result.details['command'][0],'unused-executable')
        self.assertEqual(result.provenance['extra'],('recorded',))
        self.assertEqual(result.details['recorded_artifacts']['nested']['values'],(1,2))
        with self.assertRaises(TypeError):result.details['recorded_artifacts']['nested']['values'][0]=10
        copy=result.as_dict();copy['details']['recorded_artifacts']['nested']['values'][0]=100
        self.assertEqual(result.details['recorded_artifacts']['nested']['values'],(1,2))

    def test_read_opens_only_run_json_and_never_uses_output_existence_or_live_processes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);target=root/'run.json';document=record();target.write_text(json.dumps(document))
            (root/'progress.json').write_text('not valid JSON; must not read')
            original=Path.open;opened=[]
            def only_record(path,*args,**kwargs):
                opened.append(path)
                if path!=target:raise AssertionError('unexpected path read: '+str(path))
                return original(path,*args,**kwargs)
            with patch.object(Path,'open',only_record),patch.object(Path,'is_file',side_effect=AssertionError('no file checks')),\
                 patch.object(subprocess,'Popen',side_effect=AssertionError('no execution')),\
                 patch.object(os,'kill',side_effect=AssertionError('no PID probing')):
                result=read_command_result(root)
            self.assertEqual(opened,[target]);self.assertTrue(result.success)
            self.assertEqual(result.outputs['summary'],'never-opened.root')
            target.unlink()
            with self.assertRaises(FileNotFoundError):read_command_result(root)
            target.write_text('{"schema":')
            with self.assertRaises(json.JSONDecodeError):read_command_result(root)
            text=json.dumps(document)
            target.write_text(text[:-1]+',"success":false}')
            with self.assertRaisesRegex(ValueError,'duplicate'):read_command_result(root)
            document['provenance']={'nested':{'x':1}}
            target.write_text(json.dumps(document).replace('"x": 1','"x": 1, "x": 2'))
            with self.assertRaisesRegex(ValueError,'duplicate'):read_command_result(root)

    def test_persisted_stdlib_child_and_missing_output_failure_match_live_scope(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            command=CppAnalysisSpec(sys.executable,('-S','-c','pass'),working_directory=root).run(root/'logs')
            saved=json.loads((root/'logs/run.json').read_text())
            result=read_command_result(root/'logs')
            expected=command.to_run_result().as_dict()
            expected['details']['recorded_artifacts']=saved['artifacts']
            self.assertEqual(result.as_dict(),expected)
            self.assertEqual(result.provenance['artifact_identity'],saved['provenance']['artifact_identity'])
            failed=CppAnalysisSpec(sys.executable,('-S','-c','pass'),working_directory=root,
                outputs={'summary':'missing.json'}).run(root/'failed',check=False)
            result=read_command_result(root/'failed')
            self.assertEqual(result.status,'failed');self.assertEqual(result.details['returncode'],0)
            self.assertEqual(result.issues[0]['code'],'missing_output')
            self.assertEqual(result.provenance,failed.to_run_result().provenance)
            nonzero=CppAnalysisSpec(sys.executable,('-S','-c','raise SystemExit(7)'),working_directory=root).run(root/'nonzero',check=False)
            result=read_command_result(root/'nonzero')
            self.assertEqual(result.status,'failed');self.assertEqual(result.details['returncode'],7)
            self.assertEqual(result.provenance,nonzero.to_run_result().provenance)

    def test_cli_reports_read_success_separately_from_recorded_worker_status(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary)
            with patch.object(CppAnalysisSpec,'from_file',side_effect=AssertionError('no specification')),\
                 patch.object(subprocess,'Popen',side_effect=AssertionError('no execution')),\
                 patch.object(os,'kill',side_effect=AssertionError('no PID probe')):
                for status,code in [('succeeded',0),('failed',0),('timed_out',None),('cancelled',-15)]:
                    (root/'run.json').write_text(json.dumps(record(status,code)))
                    output=io.StringIO()
                    with contextlib.redirect_stdout(output):exitcode=main(['result',str(root)])
                    self.assertEqual(exitcode,0)
                    self.assertEqual(json.loads(output.getvalue())['status'],status)
                for content in (None,'{"schema":',json.dumps(record('succeeded',2))):
                    if content is None:(root/'run.json').unlink()
                    else:(root/'run.json').write_text(content)
                    output=io.StringIO()
                    with contextlib.redirect_stdout(output):exitcode=main(['result',str(root)])
                    self.assertEqual(exitcode,1)
                    self.assertFalse(json.loads(output.getvalue())['passed'])


@unittest.skipUnless(shutil.which('cmake'),'language-free controller install')
class RecordedInstalledTests(unittest.TestCase):
    def test_installed_receiver_reads_persisted_stdlib_child_without_native_runtime(self):
        with tempfile.TemporaryDirectory(prefix='rdf recorded result install ') as temporary:
            root=Path(temporary);build=root/'build';prefix=root/'installed'
            environment=dict(os.environ,CC='/unavailable-compiler',CXX='/unavailable-compiler')
            subprocess.run(['cmake','-S',str(REPO/'core/python/controller'),'-B',str(build)],
                env=environment,check=True,capture_output=True,text=True)
            subprocess.run(['cmake','--install',str(build),'--prefix',str(prefix),'--component','PythonController'],
                env=environment,check=True,capture_output=True,text=True)
            code='''
from pathlib import Path
import json,sys
from rdfworkflow import CppAnalysisSpec,read_command_result
import rdfworkflow
assert Path(rdfworkflow.__file__).parent.resolve()==Path(sys.path[1]).resolve()
command=CppAnalysisSpec(sys.executable,('-S','-c','pass'),working_directory=Path.cwd()).run('logs')
result=read_command_result('logs')
assert result.success and result.backend=='compiled-executable'
assert result.provenance['artifact_identity']==command.provenance['artifact_identity']
assert result.counts=={} and result.event_loops is None
assert not {'ROOT','rdfanalyzer','rdfanalysis','numpy','numba','yaml'}.intersection(sys.modules)
print(json.dumps(result.as_dict()))
'''
            child=subprocess.run([sys.executable,'-S','-c',code],cwd=root,
                env=dict(os.environ,PYTHONPATH=str(prefix/'python')),check=True,capture_output=True,text=True)
            self.assertEqual(json.loads(child.stdout)['schema'],'rdf-run-result-v1')
            for status in ('succeeded','failed'):
                logs=root/('logs' if status=='succeeded' else 'failed-record')
                if status=='failed':
                    logs.mkdir();(logs/'run.json').write_text(json.dumps(record('failed',0)))
                cli=subprocess.run([sys.executable,'-S','-m','rdfworkflow','result',str(logs)],cwd=root,
                    env=dict(os.environ,PYTHONPATH=str(prefix/'python')),check=True,capture_output=True,text=True)
                document=json.loads(cli.stdout)
                self.assertEqual(document['schema'],'rdf-run-result-v1');self.assertEqual(document['status'],status)
            self.assertFalse(list(prefix.rglob('*.so')))


if __name__=='__main__':unittest.main()
