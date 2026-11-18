"""Public task identity and real source-task sizing propagation contracts."""
import unittest
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(REPO / 'core/python'))
sys.path.insert(0, str(REPO / 'core/python/law'))
try:
    import analysis_tasks as tasks
except ImportError:
    tasks = None


@unittest.skipIf(tasks is None, 'LAW/Luigi dependencies unavailable')
class SizingParameterContract(unittest.TestCase):
    def task(self, **options):
        return tasks.SkimTask(exe='/fake/exe', submit_config='submit.txt',
                              dataset_manifest='datasets.yaml', name='sizing-contract', **options)

    def test_independent_targets_and_runtime_survive_source_roundtrip(self):
        original=self.task(sizing_pilots=True, sizing_target_seconds=2700,
            sizing_pilot_target_seconds=720, sizing_pilot_parallel=3,
            sizing_pilot_prefix_entries=1234, sizing_pilot_safety_factor=1.75,
            max_runtime=10800, sizing_pilot_child=True)
        derived=tasks._make_stitching_derivation_task(original,'stitch.yaml','derived','counts','local')
        counter=derived.requires()
        for name in ('sizing_pilots','sizing_target_seconds','sizing_pilot_target_seconds',
                     'sizing_pilot_parallel','sizing_pilot_prefix_entries','sizing_pilot_safety_factor','max_runtime'):
            self.assertEqual(getattr(counter,name),getattr(original,name))
        # This is a source counter pass, not recursion into an internal pilot child.
        self.assertFalse(counter.sizing_pilot_child)

    def test_parameter_descriptors_remain_distinct_and_significant(self):
        ordinary=dict(tasks.SkimTask.get_params());source=dict(tasks.StitchingDerivationTask.get_params())
        for name in ('sizing_pilots','sizing_target_seconds','sizing_pilot_target_seconds',
                     'sizing_pilot_parallel','sizing_pilot_prefix_entries','sizing_pilot_safety_factor'):
            self.assertIsNot(ordinary[name],source['source_'+name])
            self.assertTrue(ordinary[name].significant)
            self.assertTrue(source['source_'+name].significant)

    def test_serialized_derivation_preserves_requirement_identity(self):
        original=self.task(sizing_target_seconds=2700,sizing_pilot_target_seconds=720)
        derived=tasks._make_stitching_derivation_task(original,'stitch.yaml','derived','counts','local')
        restored=tasks.StitchingDerivationTask.from_str_params(derived.to_str_params())
        self.assertEqual(restored.task_id,derived.task_id)
        self.assertEqual(restored.requires().task_id,derived.requires().task_id)

    def test_overriding_production_target_does_not_change_pilot_or_runtime(self):
        task=self.task(sizing_target_seconds=2400)
        self.assertEqual(task.sizing_pilot_target_seconds,1800)
        self.assertEqual(task.max_runtime,7200)


if __name__=='__main__':unittest.main()
