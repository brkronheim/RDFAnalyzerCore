"""Merge arbitration and reuse without ROOT or a scheduler."""
from pathlib import Path
import tempfile,unittest,json,fcntl,threading
from unittest.mock import patch
from types import SimpleNamespace
from partial_merge import _acquire_merge_lock,merge_completed_outputs

class TestMergeControl(unittest.TestCase):
 def test_busy_lock_fail_fast_or_wait_without_unlink(self):
  with tempfile.TemporaryDirectory() as d:
   path=Path(d)/'lock';owner=_acquire_merge_lock(path);inode=path.stat().st_ino;acquired=threading.Event()
   with self.assertRaisesRegex(RuntimeError,'already running'):_acquire_merge_lock(path)
   def wait():
    with _acquire_merge_lock(path,True):acquired.set()
   t=threading.Thread(target=wait);t.start();self.assertFalse(acquired.wait(.05));owner.close();t.join(2);self.assertFalse(t.is_alive());self.assertTrue(acquired.is_set());self.assertEqual(inode,path.stat().st_ino)
 def test_verified_snapshot_reuses_without_merger_and_invalidates_on_change(self):
  with tempfile.TemporaryDirectory() as d:
   root=Path(d);source=root/'source.root';source.write_bytes(b'input');state=root/'state.json';state.write_text(json.dumps(dict(datasets=dict(DY=dict(jobs={'0':dict(status='done',meta=str(source))})))));merger=root/'merger.py';merger.write_text('# test merger');destination=root/'out';calls=[]
   def artifacts(p):return [p,Path(str(p)+'.metadata'),Path(str(p)+'.merge.json')]
   def run(command,**kwargs):
    calls.append(command)
    for p in artifacts(Path(command[2])):p.write_bytes(b'published')
    return SimpleNamespace(returncode=0)
   with patch('subprocess.run',side_effect=run),patch('metadata_io.metadata_artifact_paths',side_effect=artifacts):
    opts=dict(hadd_jobs=1,xrdcp_jobs=1,sparse_only=True,metadata_merger=merger)
    merge_completed_outputs(state,destination,**opts);self.assertEqual(len(calls),1)
    merge_completed_outputs(state,destination,**opts);self.assertEqual(len(calls),1)
    (destination/'DY_partial.root').write_bytes(b'changed')
    merge_completed_outputs(state,destination,**opts);self.assertEqual(len(calls),2)

if __name__=='__main__':unittest.main()
