"""Graph diagnostics must remain evidence without becoming build identities."""
import copy,sys,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[2]/'python'))
from metadata_io import build_bundle,expand_bundle

def record(identity):
 return dict(id=identity,source=identity+'.root',sample='DY',regions={},normalizations={},objects={},weight_audits={},provenance={
  'framework.git_hash':'frozen','framework.git_dirty':'true','root.version':'6.40.04',
  'root.registry_optimization':'enabled','root.registry_definitions':'10',
  'root.registry_input_bindings_before':'100','root.registry_input_bindings_after':'20',
  'run.expected_executable_sha256':'planned','config.semantic_hash':'physics-config',
  'plugin.weights.config_hash':'weights','job.logical_id':identity})
class MetadataRuntimeCounterTest(unittest.TestCase):
 def test_different_graph_sizes_merge_losslessly(self):
  a,b=record('a'),record('b');b['provenance'].update({'root.registry_definitions':'12','root.registry_input_bindings_before':'120','root.registry_input_bindings_after':'24','task.runtime.actual_executable_sha256':'reviewed-override'})
  merged=build_bundle([a,b]);leaves=expand_bundle(merged)
  self.assertEqual(len(leaves),2)
  for original in [a,b]:self.assertEqual(next(r for r in leaves if r['id']==original['id'])['provenance'],original['provenance'])
 def test_build_config_and_implementation_guards_remain_strict(self):
  for key in ['framework.git_hash','root.version','root.registry_optimization','run.expected_executable_sha256','plugin.weights.config_hash','config.semantic_hash']:
   with self.subTest(key=key):
    a,b=record('a'),record('b');b['provenance'][key]='changed'
    with self.assertRaises(ValueError):build_bundle([a,b])
if __name__=='__main__':unittest.main()
