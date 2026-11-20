"""Legacy no-op requests warn locally without changing prepared workers."""
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import warnings

from core.python import submission_backend as backend


class LegacyOptions(unittest.TestCase):
    def compare(self, function, kwargs, option, value):
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter('always')
            expected=function(**kwargs)
        self.assertEqual(records, [])
        with warnings.catch_warnings(record=True) as records:
            warnings.simplefilter('always')
            actual=function(**kwargs, **{option:value})
        self.assertEqual(actual,expected)
        self.assertEqual(len(records),1)
        self.assertIs(records[0].category,FutureWarning)
        self.assertIn(option,str(records[0].message))
        self.assertIn('has no effect; remove this argument',str(records[0].message))
        self.assertEqual(Path(records[0].filename).resolve(),Path(__file__).resolve())

    def test_stage_in_legacy_request(self):
        self.compare(backend.stage_inputs_block,dict(config_file='config.yaml'),'eos_sched',True)

    def test_stage_out_legacy_request(self):
        self.compare(backend.stage_outputs_blocks,dict(config_file='config.yaml'),'eos_sched',True)

    def test_blacklist_request_and_empty_default(self):
        self.compare(backend.xrootd_optimize_block,dict(config_file='config.yaml'),'blacklisted_sites',['T2_example'])
        with warnings.catch_warnings(record=True)as records:
            warnings.simplefilter('always')
            backend.xrootd_optimize_block(blacklisted_sites=[])
        self.assertEqual(records,[])

    def test_runscript_warns_once_with_both_staging_modes(self):
        for staged in (False,True):
            self.compare(backend.generate_condor_runscript,
                dict(exe_relpath='bin/test',stage_inputs=staged,stage_outputs=staged,root_setup='',site_ranking_file='none'),
                'eos_sched',True)

    def test_submit_description_legacy_request(self):
        with tempfile.TemporaryDirectory()as directory:
            self.compare(backend.generate_condor_submit,
                dict(main_dir=directory,jobs=2,exe_relpath='bin/test'),'eos_sched',True)

    def test_file_preparation_warns_once_and_preserves_all_bytes(self):
        with tempfile.TemporaryDirectory()as directory,patch.object(backend,'_site_ranking_block',return_value=''):
            kwargs=dict(main_dir=directory,jobs=2,exe_relpath='bin/test',stage_inputs=True,stage_outputs=True,root_setup='')
            with warnings.catch_warnings(record=True)as records:
                warnings.simplefilter('always');backend.write_submit_files(**kwargs)
            self.assertEqual(records,[])
            before={str(p.relative_to(directory)):p.read_bytes()for p in Path(directory).rglob('*')if p.is_file()}
            with warnings.catch_warnings(record=True)as records:
                warnings.simplefilter('always');backend.write_submit_files(**kwargs,eos_sched=True)
            after={str(p.relative_to(directory)):p.read_bytes()for p in Path(directory).rglob('*')if p.is_file()}
            self.assertEqual(before,after)
            self.assertEqual(len(records),1)
            self.assertIn('eos_sched',str(records[0].message))


if __name__=='__main__':unittest.main()
