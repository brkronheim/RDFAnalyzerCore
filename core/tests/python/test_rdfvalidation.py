"""Shared report semantics and explicit provider discovery/help without native imports."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from unittest.mock import patch
import unittest

from rdfmodules import AnalysisModule,ModuleSpec
from rdfvalidation import SettingsSpec,validate_provider_settings,discover_settings_specs
from rdfworkflow import ValidationIssue,ValidationReport

REPO=Path(__file__).resolve().parents[3]


class ProviderValidationTests(unittest.TestCase):
    def test_returned_failed_reports_are_enforced_before_a_module_can_build(self):
        calls=[]
        def validate(settings):return ValidationReport('configuration',('configuration',),
            (ValidationIssue('choice','Unknown recipe','mode'),ValidationIssue('type','Expected bool','enabled')))
        report=validate_provider_settings(validate,{},location='external.settings')
        self.assertFalse(report.passed)
        self.assertEqual([issue.location for issue in report.issues],['external.settings.mode','external.settings.enabled'])
        with self.assertRaisesRegex(ValueError,'external.settings.mode: Unknown recipe'):
            AnalysisModule(ModuleSpec('external'),lambda a,s:calls.append('built'),validate_settings=validate)
        self.assertFalse(calls)

    def test_legacy_void_and_exception_validators_have_reports_and_invalid_return_values_fail(self):
        self.assertTrue(validate_provider_settings(lambda settings:None,{}).passed)
        def invalid(settings):raise ValueError('bad key')
        report=validate_provider_settings(invalid,{},location='recipe')
        self.assertEqual(report.issues[0].location,'recipe');self.assertIn('bad key',report.issues[0].message)
        for value in (False,True,{},'valid'):
            with self.subTest(value=value),self.assertRaisesRegex(TypeError,'None or ValidationReport'):
                validate_provider_settings(lambda settings:value,{})
        def broken(settings):raise RuntimeError('provider bug')
        with self.assertRaisesRegex(RuntimeError,'provider bug'):validate_provider_settings(broken,{})
        with self.assertRaisesRegex(TypeError,'expected'):
            validate_provider_settings(lambda settings:ValidationReport('assets',('assets',)),{})

    def test_module_validation_is_cached_detached_and_unavailable_is_not_success(self):
        calls=[]
        def validate(settings):
            calls.append(settings);settings['values'].append(2)
            return ValidationReport('configuration',('configuration',))
        module=AnalysisModule(ModuleSpec('checked'),lambda a,s:None,{'values':[1]},validate)
        self.assertTrue(module.validate().passed);self.assertTrue(module.validate().passed)
        described=module.describe().as_dict()['details']
        self.assertTrue(described['validation']['passed']);described['validation']['issues'].append('changed')
        self.assertEqual(module.validate().issues,());self.assertEqual(len(calls),1)
        self.assertEqual(module.settings['values'],(1,))
        unchecked=AnalysisModule(ModuleSpec('opaque'),lambda a,s:None)
        self.assertFalse(unchecked.validate().passed)
        self.assertIsNone(unchecked.describe().as_dict()['details']['validation'])

    def test_asset_level_is_explicit_and_runs_only_after_configuration_passes(self):
        calls=[]
        def config(settings):
            calls.append('configuration')
            return ValidationReport('configuration',('configuration',),
                () if settings.get('valid') else (ValidationIssue('missing','Need valid','valid'),))
        def assets(settings):calls.append('assets')
        spec=SettingsSpec('external.settings',{'type':'object'},config,asset_validator=assets)
        self.assertTrue(spec.validate({'valid':True}).passed);self.assertEqual(calls,['configuration'])
        failed=spec.validate({'valid':False},level='assets')
        self.assertFalse(failed.passed);self.assertEqual(failed.level,'assets');self.assertNotIn('assets',calls)
        self.assertTrue(spec.validate({'valid':True},level='assets').passed)
        self.assertEqual(calls[-2:],['configuration','assets'])
        calls.clear();self.assertFalse(spec.validate({},level='compile').passed);self.assertFalse(calls)

    def test_descriptive_schema_is_not_silently_an_enforcement_engine(self):
        schema={'type':'object','required':['example']}
        spec=SettingsSpec('external',schema,lambda settings:None)
        schema['required'].append('changed')
        self.assertTrue(spec.validate({}).passed)
        self.assertEqual(spec.describe().as_dict()['details']['schema']['required'],['example'])
        self.assertFalse(spec.validate([],location='source').passed)
        self.assertFalse(spec.validate({'nan':float('nan')}).passed)

    def test_discovery_preserves_legacy_services_and_rejects_duplicate_descriptions(self):
        from types import SimpleNamespace
        legacy=SimpleNamespace(PLUGIN_SCHEMAS={})
        spec=SettingsSpec('external',{},lambda settings:None)
        provider=SimpleNamespace(settings_specs=lambda:(spec,))
        with patch('optional_extensions.load_extension_services',return_value=[legacy,provider]):
            self.assertEqual(discover_settings_specs(),(spec,))
        with patch('optional_extensions.load_extension_services',return_value=[provider,provider]):
            with self.assertRaisesRegex(ValueError,'duplicate settings'):discover_settings_specs()

    def test_json_cli_uses_canonical_provider_types_without_an_experiment_checkout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary);package=root/'python/fixture_settings';package.mkdir(parents=True)
            (root/'rdf-extension.json').write_text(json.dumps({'name':'fixture_settings','core_api_version':1,
                'python_package':'fixture_settings','python_services':{'config_schemas':'provider'}}))
            (package/'__init__.py').write_text('')
            (package/'provider.py').write_text("from rdfvalidation import SettingsSpec\nPLUGIN_SCHEMAS={}\ndef settings_specs():\n    return (SettingsSpec('fixture.settings',{},lambda settings:None),)\n")
            env=dict(os.environ,PYTHONPATH=str(REPO/'core/python'),RDF_EXTENSION_PATHS=str(root),RDF_BUILD_EXTENSIONS='ON',RDF_ENABLE_CMS='OFF')
            result=subprocess.run([sys.executable,'-S','-m','rdfvalidation','schema','fixture.settings'],
                                  env=env,capture_output=True,text=True,check=True)
            self.assertEqual(json.loads(result.stdout)['details']['name'],'fixture.settings')

    @unittest.skipUnless((REPO/'extensions/cms/rdf-extension.json').is_file(),'optional CMS checkout unavailable')
    def test_cms_discovery_and_json_cli_have_source_locations_and_no_heavy_imports(self):
        env=dict(os.environ,PYTHONPATH=str(REPO/'core/python'),RDF_EXTENSION_PATHS=str(REPO/'extensions/cms'),
                 RDF_BUILD_EXTENSIONS='ON',RDF_ENABLE_CMS='ON')
        code="""
import json,sys
from rdfanalysis import discover_settings_specs
assert 'rdfanalyzer_cms' not in sys.modules
specs=discover_settings_specs()
assert [spec.name for spec in specs]==['cms.corrections']
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml'}.intersection(sys.modules)
print(json.dumps(specs[0].describe().as_dict()))
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,capture_output=True,text=True,check=True)
        schema=json.loads(result.stdout)['details']['schema']
        self.assertEqual(schema['properties']['jes']['enum'],['none','total','reduced','full'])
        with tempfile.TemporaryDirectory() as temporary:
            source=Path(temporary)/'recipe.json';source.write_text(json.dumps({'era':'22pre','jets':'false','jes':'typo'}))
            command=[sys.executable,'-S','-m','rdfvalidation','validate','cms.corrections',str(source)]
            invalid=subprocess.run(command,env=env,capture_output=True,text=True)
            report=json.loads(invalid.stdout);self.assertEqual(invalid.returncode,1);self.assertFalse(report['passed'])
            self.assertTrue(all(item['location'].startswith(str(source)+':settings.') for item in report['issues']))
            source.write_text('{\n invalid')
            parse=subprocess.run(command,env=env,capture_output=True,text=True)
            self.assertEqual(parse.returncode,1);self.assertIn(str(source)+':2:',json.loads(parse.stdout)['issues'][0]['location'])
            source.write_text('{"era":"22pre"}')
            valid=subprocess.run(command,env=env,capture_output=True,text=True,check=True)
            self.assertTrue(json.loads(valid.stdout)['passed'])


if __name__=='__main__':unittest.main()
