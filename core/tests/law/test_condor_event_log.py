"""Expired-history recovery requires exact, complete scheduler evidence."""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path[:0] = [str(Path(__file__).resolve().parents[2] / 'python/law'),
               str(Path(__file__).resolve().parents[2] / 'python')]
from ingestion_utils import condor_event_log_exit


def event(cluster=123, proc=0, code='005', body='\t(1) Normal termination (return value 0)\n', complete=True):
    return f'{code} ({cluster}.{proc:03d}.000) 2026-11-16 10:00:00 event\n{body}' + ('...\n' if complete else '')


class TestCondorEventLog(unittest.TestCase):
    def test_exact_identity_latest_complete_normal_termination(self):
        cases = [
            (event(), 0),
            (event(proc=1), None),
            (event(cluster=124), None),
            (event(body='\t(1) Normal termination (return value 2)\n'), 2),
            (event(body='\t(0) Abnormal termination (signal 9)\n'), None),
            (event(complete=False), None),
            (event() + event(code='001', body='running\n'), None),
            (event() + event(code='005', body='incomplete', complete=False), None),
            (event() + event(cluster=124, code='001'), 0),
        ]
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'condor_logs/log_123.log';path.parent.mkdir()
            for text, expected in cases:
                with self.subTest(text=text):
                    path.write_text(text)
                    result=condor_event_log_exit(d,'123',0,0)
                    self.assertEqual(None if result is None else result[0],expected)

    def test_split_retry_uses_persisted_source_not_reassigned_index(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'condor_logs/resub_31_retry1_part1.log';path.parent.mkdir();path.write_text(event())
            self.assertEqual(condor_event_log_exit(d,'123',0,83,str(Path(d)/'resubmissions/job_31_retry1_part1')),(0,str(path)))
            self.assertIsNone(condor_event_log_exit(d,'124',0,83,str(Path(d)/'resubmissions/job_31_retry1_part1')))

    def test_shared_log_cache_invalidates_after_append(self):
        with tempfile.TemporaryDirectory() as d:
            path=Path(d)/'condor_logs/log_123.log';path.parent.mkdir();path.write_text(event());cache={}
            self.assertEqual(condor_event_log_exit(d,'123',0,0,log_cache=cache)[0],0)
            self.assertEqual(len(cache),1)
            with path.open('a') as f:f.write(event(code='001',body='restarted\n'))
            self.assertIsNone(condor_event_log_exit(d,'123',0,0,log_cache=cache))

    def test_conflicting_generated_logs_fail_closed(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)/'condor_logs';p.mkdir();(p/'log_123.log').write_text(event());(p/'resub_0.log').write_text(event(body='\t(1) Normal termination (return value 1)\n'))
            self.assertIsNone(condor_event_log_exit(d,'123',0,0))


if __name__=='__main__':unittest.main()
