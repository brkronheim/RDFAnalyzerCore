"""Requested versus observed resource reporting; no ROOT import/event work."""
from pathlib import Path
import sys,types,unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'python'))
from rdfanalysis import Analysis,AnalysisConfig

def analysis(native,threads=2):
    a=object.__new__(Analysis);a.native=native;a.config=AnalysisConfig(files=['missing.root'],output='unused.root',threads=threads,skim=False);return a
class RuntimeThreading(unittest.TestCase):
    def test_affinity_capped_pool_is_not_reported_as_requested(self):
        native=types.SimpleNamespace(_threading_snapshot=lambda:dict(implicit_mt_enabled=True,pool_size=1,dataframe_slots=1))
        actual=analysis(native).runtime_threading
        self.assertEqual(actual,dict(requested_threads=2,observed=True,implicit_mt_enabled=True,pool_size=1,dataframe_slots=1))
    def test_older_module_has_explicit_unknown_observations(self):
        actual=analysis(types.SimpleNamespace(),threads=4).runtime_threading
        self.assertEqual(actual,dict(requested_threads=4,observed=False,implicit_mt_enabled=None,pool_size=None,dataframe_slots=None))
    def test_serial_pool_zero_and_one_dataframe_slot_are_distinct(self):
        a=analysis(types.SimpleNamespace(_threading_snapshot=lambda:dict(implicit_mt_enabled=False,pool_size=0,dataframe_slots=1)),threads=1)
        self.assertEqual(a.runtime_threading,dict(requested_threads=1,observed=True,implicit_mt_enabled=False,pool_size=0,dataframe_slots=1))
    def test_global_pool_change_is_visible_separately_from_booked_slots(self):
        a=analysis(types.SimpleNamespace(_threading_snapshot=lambda:dict(implicit_mt_enabled=True,pool_size=4,dataframe_slots=2)))
        self.assertEqual((a.runtime_threading['pool_size'],a.runtime_threading['dataframe_slots']),(4,2))
    def test_returned_snapshot_is_detached_and_unknown_extra_fields_are_ignored(self):
        data=dict(implicit_mt_enabled=True,pool_size=2,dataframe_slots=2,private_owner=object())
        a=analysis(types.SimpleNamespace(_threading_snapshot=lambda:data));before=a.runtime_threading;before['pool_size']=99
        self.assertEqual(a.runtime_threading['pool_size'],2);self.assertNotIn('private_owner',before)
    def test_malformed_contract_and_running_guard_are_visible(self):
        for data in ({},dict(implicit_mt_enabled=True,pool_size=True,dataframe_slots=1),dict(implicit_mt_enabled=True,pool_size=1,dataframe_slots=0)):
            with self.subTest(data=data),self.assertRaisesRegex(RuntimeError,'invalid contract'):
                analysis(types.SimpleNamespace(_threading_snapshot=lambda:data)).runtime_threading
        def running():raise RuntimeError('Inspect threading before or after run()')
        with self.assertRaisesRegex(RuntimeError,'before or after'):
            analysis(types.SimpleNamespace(_threading_snapshot=running)).runtime_threading
if __name__=='__main__':unittest.main()
