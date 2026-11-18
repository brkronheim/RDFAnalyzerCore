"""Typed settings preserve native options, path ownership and lazy imports."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

from rdfanalysis import Analysis, AnalysisConfig, InputSettings, SkimSettings


class TypedSettingsTests(unittest.TestCase):
    def test_default_and_explicit_typed_options_match_legacy(self):
        base=dict(files=['input.root'],output='out.root')
        self.assertNotIn('treeCacheEnabled',AnalysisConfig(**base).native_config())
        self.assertNotIn('skimCompressionLevel',AnalysisConfig(**base).native_config())
        with tempfile.TemporaryDirectory() as temporary:
            constants=str(Path(temporary)/'constants.txt')
            typed=AnalysisConfig(**base,input_settings=InputSettings(cache_enabled=True,cache_size_mb=64,
                cache_statistics=False,float_constants=constants),
                skim_settings=SkimSettings(systematics=True,metadata_file=str(Path(temporary)/'meta.root'),
                                           compression_algorithm='zstd',compression_level=7))
            legacy=AnalysisConfig(**base,options={
                'treeCacheEnabled':'true','treeCacheSizeMB':'64','treeCacheStats':'false',
                'floatConfig':constants,'metaFile':str(Path(temporary)/'meta.root'),
                'skimSystematics':'true','skimCompressionAlgorithm':'zstd','skimCompressionLevel':'7'})
            self.assertEqual(typed.native_config(),legacy.native_config())
            details=typed.describe().as_dict()['details']
            self.assertEqual(details['setting_origins']['floatConfig'],'typed')
            self.assertEqual(legacy.describe().as_dict()['details']['setting_origins']['floatConfig'],'legacy')
            separate=AnalysisConfig(**base,skim_settings=SkimSettings(tree='Selected'))
            self.assertEqual(separate.native_config()['saveTree'],'Selected')
            self.assertEqual(separate.native_config()['treeList'],'Events')

    def test_bad_typed_values_conflicts_and_opaque_extensions(self):
        for kind,kwargs in ((InputSettings,{'cache_enabled':'true'}),(InputSettings,{'cache_size_mb':32}),
                           (InputSettings,{'cache_enabled':True,'cache_size_mb':513}),
                           (InputSettings,{'cache_enabled':True,'cache_size_mb':True}),
                           (SkimSettings,{'compression_level':10}),
                           (SkimSettings,{'compression_algorithm':'zstdd'})):
            with self.subTest(kwargs=kwargs),self.assertRaises(ValueError):kind(**kwargs)
        base=dict(files=['missing.root'],output='out.root')
        with self.assertRaisesRegex(ValueError,'treeCacheEnabled'):
            AnalysisConfig(**base,input_settings={'cache_enabled':True},options={'treeCacheEnabled':'false'})
        with self.assertRaisesRegex(ValueError,'rootTTreeCacheSize'):
            AnalysisConfig(**base,input_settings={'cache_enabled':True,'cache_size_mb':64},options={'rootTTreeCacheSize':'1'})
        with self.assertRaisesRegex(ValueError,'unknown fields.*cache_szie'):
            AnalysisConfig(**base,input_settings={'cache_szie':64})
        # Unset typed keys and third-party namespaces retain the explicit escape hatch.
        config=AnalysisConfig(**base,options={'thirdPartyRecipe':'unchanged','skimCompressionLevel':'3'})
        self.assertEqual(config.native_config()['thirdPartyRecipe'],'unchanged')
        self.assertEqual(config.native_config()['skimCompressionLevel'],'3')

    def test_assets_are_explicit_local_config_checks_before_native_import(self):
        with tempfile.TemporaryDirectory() as temporary:
            path=Path(temporary)/'cuts.txt'
            config=AnalysisConfig(files=['root://not-probed/file.root'],output='out.root',
                                  input_settings=InputSettings(float_constants=str(path)))
            self.assertTrue(config.validate().passed)
            report=config.validate('assets')
            self.assertFalse(report.passed)
            self.assertEqual(report.issues[0].location,'input_settings.float_constants')
            with patch.dict(sys.modules,{'rdfanalyzer':None}),self.assertRaisesRegex(ValueError,'cuts.txt'):
                Analysis(config)
            path.write_text('threshold=30\n')
            self.assertTrue(config.validate('assets').passed)
            self.assertFalse(config.validate('schema').passed)

    def test_yaml_nested_paths_and_diagnostics_include_source(self):
        with tempfile.TemporaryDirectory() as temporary:
            directory=Path(temporary);path=directory/'analysis.yaml'
            path.write_text('files: [root://remote/file.root]\noutput: out.root\n'
                            'input_settings: {float_constants: cuts.txt}\n'
                            'skim_settings: {metadata_file: meta.root, tree: Selected}\n')
            config=AnalysisConfig.from_file(path)
            self.assertEqual(config.input_settings.float_constants,str(directory/'cuts.txt'))
            self.assertEqual(config.skim_settings.metadata_file,str(directory/'meta.root'))
            self.assertEqual(config.files,('root://remote/file.root',))
            path.write_text('files: [input.root]\noutput: out.root\nskim_settings: {compresion_level: 7}\n')
            with self.assertRaisesRegex(ValueError,str(path)+'.*compresion_level'):AnalysisConfig.from_file(path)

    def test_schema_and_descriptions_import_no_optional_stack(self):
        env=dict(os.environ,PYTHONPATH=str(Path(__file__).resolve().parents[2]/'python'))
        code="""
import sys,json
from rdfanalysis import AnalysisConfig,InputSettings,SkimSettings
schema=AnalysisConfig.schema()
assert schema['skim_settings']['compression_level']['maximum']==9
assert schema['input_settings']['cache_size_mb']['native_key']=='treeCacheSizeMB'
assert AnalysisConfig(files=['missing.root'],output='unused.root',input_settings={'cache_enabled':False}).validate('assets').passed
assert not {'ROOT','rdfanalyzer','numpy','numba','yaml','rdfanalyzer_cms'}.intersection(sys.modules)
print('clean')
"""
        result=subprocess.run([sys.executable,'-S','-c',code],env=env,text=True,capture_output=True,check=True)
        self.assertEqual(result.stdout.strip(),'clean')
        schema=subprocess.run([sys.executable,'-S','-m','rdfanalysis','schema'],env=env,text=True,capture_output=True,check=True)
        self.assertEqual(json.loads(schema.stdout)['schema'],'rdf-settings-schema-v1')


if __name__=='__main__':unittest.main()
