"""Read native summaries through the unchanged stdlib Python RunResult receiver.

The native fixture is built by CMake, never by this test. No ROOT/framework
imports, event processing or output-reference I/O are needed.
"""
import copy
import json
import math
import os
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'python'))
from rdfresults import RunResult


RECEIVER_MODES = ('succeeded', 'failed', 'timed_out', 'cancelled', 'known-loops',
                  'empty', 'max-loops', 'reset-loops')
REJECTED_MODES = ('negative-count', 'boolean-count', 'negative-time', 'boolean-time',
                  'negative-loops', 'empty-path', 'nan', 'infinity', 'bad-status',
                  'blank-backend', 'blank-scope', 'bad-utf8', 'surrogate',
                  'truncated-utf8', 'above-unicode', 'overlong-utf8',
                  'overflow-loops', 'null-string', 'bad-utf8-key')


def expected_result():
    # Known caller values, interpreted by the existing Python contract rather
    # than reimplementing the C++ validation or JSON writer.
    return RunResult('compiled-cpp', 'succeeded',
        'caller-supplied summaries; paths are references only',
        counts={'input': 2**64-1, 'selected': 42},
        timings={'execute_s': 1.25, 'setup_s': 0},
        outputs={'metadata': '/missing path/metadata.root'},
        sums={'weight': -2.5, 'negative_zero': -0.0},
        histograms={'example': {'contents': [2., -2., 0.], 'sumw2': [4., 4., 2.]}},
        details={'escape': 'quote"\\\n\t\0', 'unicode': 'μ jets', 'unknown': None,
                 'nested': [True, False, {'child': 7}],
                 'controls': ''.join(map(chr, range(32))),
                 'owned_string': 'owned', 'owned_object': {'child': 7}},
        provenance={'scope': 'analysis owned'},
        issues=[{'code': 'example', 'message': 'not a certification'}]).as_dict()


class NativeRunResultInterop(unittest.TestCase):
    def fixture(self, mode):
        executable = os.environ.get('RDF_RUN_RESULT_FIXTURE')
        if not executable:
            self.fail('RDF_RUN_RESULT_FIXTURE must name the CMake-built testRunResult executable')
        result = subprocess.run([executable, mode], check=True, capture_output=True,
                                text=True, timeout=30)
        self.assertEqual(result.stderr, '')
        return result.stdout

    def test_all_eight_native_envelopes_match_python_receiver(self):
        baseline = expected_result()
        self.assertEqual(len(RECEIVER_MODES), 8)
        for mode in RECEIVER_MODES:
            with self.subTest(mode=mode):
                document = json.loads(self.fixture(mode))
                if mode == 'empty':
                    expected = RunResult('compiled-cpp', 'succeeded', 'description only',
                                         event_loops=0).as_dict()
                else:
                    expected = copy.deepcopy(baseline)
                    if mode in ('failed', 'timed_out', 'cancelled'):
                        expected.update(status=mode, success=False)
                    elif mode == 'known-loops':
                        expected['event_loops'] = 1
                    elif mode == 'max-loops':
                        expected['event_loops'] = 2**63-1
                with patch('builtins.open', side_effect=AssertionError('result paths are references only')):
                    received = RunResult.from_dict(document)
                    self.assertEqual(received.as_dict(), document)
                self.assertEqual(document, expected)
                if mode != 'empty':
                    self.assertIs(type(document['counts']['input']), int)
                    self.assertIs(type(document['counts']['selected']), int)
                    self.assertIs(type(document['timings']['setup_s']), int)
                    self.assertIs(type(document['sums']['negative_zero']), float)
                    self.assertEqual(math.copysign(1, document['sums']['negative_zero']), -1)
                    self.assertTrue(all(type(value) is float for value in
                                        document['histograms']['example']['contents']))
                    self.assertIsNone(document['details']['unknown'])
                    self.assertEqual(document['details']['owned_string'], 'owned')
                    self.assertEqual(document['details']['owned_object'], {'child': 7})

    def test_formatted_ostream_flags_and_locale_do_not_change_json_values(self):
        raw = self.fixture('formatted-writer')
        document = json.loads(raw)
        self.assertEqual(document, {'integer': 1234567, 'unsigned': 2**64-1,
            'negative': -42, 'float': 1.25, 'bool': True, 'null': None, 'control': '\n'})
        self.assertIs(type(document['integer']), int)
        self.assertIs(type(document['unsigned']), int)
        self.assertIs(type(document['negative']), int)
        self.assertIs(type(document['float']), float)
        self.assertIs(type(document['bool']), bool)
        self.assertTrue(raw.startswith('{'))
        self.assertNotIn('*', raw)

    def test_all_nineteen_invalid_native_values_are_rejected(self):
        self.assertEqual(len(REJECTED_MODES), 19)
        for mode in REJECTED_MODES:
            with self.subTest(mode=mode):
                self.assertEqual(self.fixture(mode), 'rejected')

    def test_fixture_and_receiver_do_not_import_native_analysis_packages(self):
        # This process only consumes caller-owned JSON. Required native package
        # imports would make a detached result unavailable without the runtime.
        self.assertFalse({'ROOT', 'rdfanalyzer', 'numpy', 'numba', 'yaml'} & sys.modules.keys())


if __name__ == '__main__':
    unittest.main()
