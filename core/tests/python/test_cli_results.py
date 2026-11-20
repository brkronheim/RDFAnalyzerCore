"""Opt-in stdout envelopes preserve command scope, status and execution options."""
import contextlib
import io
import json
from types import SimpleNamespace
import unittest
from unittest.mock import Mock,patch

from rdfresults import RunResult
from rdfworkflow import CommandResult,main


class CliResultTests(unittest.TestCase):
    def test_default_and_common_envelopes_preserve_all_terminal_statuses(self):
        for command in ('run','build'):
            for status in ('succeeded','failed','timed_out','cancelled'):
                for common in (False,True):
                    with self.subTest(command=command,status=status,common=common):
                        result=CommandResult(status,0 if status=='succeeded' else -15,
                            ('native',),.25,'not-opened',operation='analysis' if command=='run' else 'build')
                        execute=Mock(return_value=result)
                        spec=SimpleNamespace(run=execute,build_spec=SimpleNamespace(build=execute))
                        arguments=[command,'not-opened.json','--directory','fresh-logs','--progress-interval','2']
                        if common:arguments+=['--result-format','run-result']
                        output=io.StringIO()
                        with patch('rdfworkflow.CppAnalysisSpec.from_file',return_value=spec),contextlib.redirect_stdout(output):
                            code=main(arguments)
                        execute.assert_called_once_with('fresh-logs',check=False,progress_interval_s=2.)
                        self.assertEqual(code,0 if status=='succeeded' else 1)
                        document=json.loads(output.getvalue())
                        self.assertEqual(document['status'],status)
                        self.assertEqual(document,result.to_run_result().as_dict() if common else result.as_dict())
                        if common:
                            restored=RunResult.from_dict(document)
                            self.assertEqual(restored.counts,{})
                            self.assertIsNone(restored.event_loops)
                            self.assertIn('process exit and declared files',restored.completion_scope)


if __name__=='__main__':unittest.main()
