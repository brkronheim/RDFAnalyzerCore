"""Compare native settings/reports against the actual stdlib Python contracts."""
import json
import os
from pathlib import Path
import subprocess
import sys
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/'python'))
from rdfsettings import InputSettings, SkimSettings
from rdfworkflow import ValidationIssue, ValidationReport


class NativeInterop(unittest.TestCase):
    def fixture(self, mode, *args):
        result = subprocess.run([os.environ['RDF_COMMON_SETTINGS_FIXTURE'], mode, *args],
                                check=True, capture_output=True, text=True)
        return json.loads(result.stdout)

    def test_full_native_mapping_matches_python_settings(self):
        base = '/nonexistent analysis directory'
        expected = InputSettings(cache_enabled=True, cache_size_mb=32,
            cache_statistics=False, cache_verbosity=2,
            float_constants=base+'/cuts.txt', integer_constants=base+'/categories.txt',
            aliases=base+'/aliases.txt', optional_branches=base+'/optional.txt',
            friends=base+'/friends.txt').native_options()
        expected.update(SkimSettings(tree='Selected', metadata_file=base+'/metadata.root',
            systematics=False, compression_algorithm='zstd', compression_level=7).native_options())
        expected['extension.custom'] = 'keep | this: value'
        self.assertEqual(self.fixture('options',base), expected)

    def test_unset_settings_preserve_advanced_native_options(self):
        self.assertEqual(self.fixture('empty'), {'treeCacheStats':'0','extension.custom':'keep'})

    def test_report_schema_round_trip_and_escaped_text(self):
        expected = ValidationReport('assets', ('configuration','assets'), (
            ValidationIssue('asset.missing', 'bad "file"\\path\n\t\0 μ', 'input_settings.aliases'),))
        self.assertEqual(self.fixture('report'), expected.as_dict())
        self.assertEqual(self.fixture('passed'), ValidationReport('configuration',('configuration',)).as_dict())

    def test_native_configuration_issues_are_actionable(self):
        report = self.fixture('invalid')
        self.assertFalse(report['passed'])
        self.assertEqual(report['supported_levels'], ['configuration'])
        self.assertEqual({issue['code'] for issue in report['issues']}, {'settings.range','settings.dependency'})
        self.assertEqual({issue['location'] for issue in report['issues']}, {'input_settings.cache_size_mb'})
        with self.assertRaises(ValueError): InputSettings(cache_enabled=True, cache_size_mb=513)

    def test_native_schema_matches_python_keys_types_bounds_and_omission(self):
        report=self.fixture('describe')
        self.assertEqual(report['schema'],'rdf-workflow-description-v1')
        self.assertEqual(report['capabilities'],['configuration-validation'])
        self.assertEqual(report['details']['supported_levels'],['configuration'])
        expected={prefix+'.'+name:spec for prefix,cls in (('input_settings',InputSettings),('skim_settings',SkimSettings))
                  for name,spec in cls.schema().items()}
        observed=report['details']['schema']
        self.assertEqual(set(observed),set(expected))
        for name,spec in expected.items():
            for key in ('native_key','type','minimum','maximum','path','asset','choices','default','nullable','unset_behavior'):
                self.assertEqual(observed[name].get(key),json.loads(json.dumps(spec.get(key))),(name,key))
            self.assertTrue(observed[name]['description'])
        result=subprocess.run([os.environ['RDF_COMMON_SETTINGS_FIXTURE'],'help'],check=True,capture_output=True,text=True)
        for spec in observed.values():self.assertIn(spec['native_key'],result.stdout)

    def test_native_schema_diagnostics_round_trip_to_shared_python_envelope(self):
        result=subprocess.run([os.environ['RDF_NATIVE_PREFLIGHT_FIXTURE'],'schema-report'],check=True,capture_output=True,text=True)
        expected=ValidationReport('schema',('schema',),(
            ValidationIssue('schema.type','Expected ROOT::VecOps::RVec<float>, observed double: Jet_pt','inputs.jets'),
            ValidationIssue('schema.missing','Required column is missing: weight','inputs.weight')))
        self.assertEqual(json.loads(result.stdout),expected.as_dict())


if __name__ == '__main__':
    unittest.main()
