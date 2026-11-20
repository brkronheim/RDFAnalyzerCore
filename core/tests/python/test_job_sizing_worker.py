"""Exercise the shipped worker script with a local executable, no remote jobs."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'python'))
from submission_backend import generate_condor_runscript


class PilotWorkerTest(unittest.TestCase):
    def test_embedded_worker_runs_prefix_then_full_and_keeps_full_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            exe = root/'analysis'
            exe.write_text('#!' + sys.executable + '\n' + r'''
from pathlib import Path
import sys
cfg=dict(l.split('=',1) for l in Path(sys.argv[1]).read_text().splitlines() if '=' in l)
n=min(1234,int(cfg.get('lastEntry',1234)))
print('CounterService: sample=sample entries='+str(n))
print('Done!')
Path(cfg['metaFile']).write_text(str(n))
''')
            exe.chmod(0o755)
            (root/'submit_config.txt').write_text('fileList=input.root\nsample=sample\nmetaFile=meta.root\nthreads=1\n')
            # Verify the exact embedded launcher, including argument rewriting
            # and the inherited CPU environment, rather than only the driver API.
            with patch.dict(os.environ, {'RDF_XRD_SITE_RANKING_FILE': 'none'}):
                script = generate_condor_runscript('analysis', False, False, '', sizing_pilot_entries=100,
                                                   site_ranking_file='none')
                ordinary = generate_condor_runscript('analysis', False, False, '', site_ranking_file='none')
                self.assertNotIn('RDF_JOB_SIZING_PILOT=', ordinary)
            (root/'worker.sh').write_text(script)
            result = subprocess.run(['bash', str(root/'worker.sh')], cwd=tmp,
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stdout+result.stderr)
            marker = next(l for l in result.stdout.splitlines() if l.startswith('RDF_JOB_SIZING_PILOT='))
            measurement = json.loads(marker.split('=',1)[1])
            self.assertEqual(measurement['prefix']['events'], 100)
            self.assertEqual(measurement['full']['events'], 1234)
            self.assertEqual((root/'meta.root').read_text(), '1234')
            self.assertEqual((root/'pilot_prefix_meta.root').read_text(), '100')
            self.assertIn('policy=inherited', result.stdout)
            self.assertIn('Worker setup time:', result.stdout)
            full_stamp = (root/'pilot_full.stdout').stat().st_mtime_ns
            exe.write_text('#!/bin/sh\nexit 7\n')
            failed = subprocess.run(['bash', str(root/'worker.sh')], cwd=tmp,
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(failed.returncode, 7, failed.stdout+failed.stderr)
            self.assertNotIn('RDF_JOB_SIZING_PILOT=', failed.stdout)
            self.assertEqual((root/'pilot_full.stdout').stat().st_mtime_ns, full_stamp)


if __name__ == '__main__':
    unittest.main()
