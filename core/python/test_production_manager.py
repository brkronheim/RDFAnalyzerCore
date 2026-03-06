#!/usr/bin/env python3
"""
Tests for ProductionManager.

Tests cover:
- Job generation
- Job submission (mocked)
- State persistence
- Output validation
- Progress monitoring
- Failure recovery
"""

import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import sys
sys.path.insert(0, str(Path(__file__).parent))

from production_manager import (
    ProductionManager,
    ProductionConfig,
    Job,
    JobStatus
)


class TestJob(unittest.TestCase):
    """Test Job dataclass"""
    
    def test_job_creation(self):
        """Test job creation"""
        job = Job(
            job_id=0,
            config_path="/path/to/config.txt",
            output_path="/path/to/output.root",
            meta_output_path="/path/to/output_meta.root"
        )
        
        self.assertEqual(job.job_id, 0)
        self.assertEqual(job.status, JobStatus.CREATED)
        self.assertEqual(job.attempts, 0)
        
    def test_job_serialization(self):
        """Test job serialization and deserialization"""
        job = Job(
            job_id=42,
            config_path="/path/to/config.txt",
            output_path="/path/to/output.root",
            meta_output_path="/path/to/output_meta.root",
            status=JobStatus.COMPLETED,
            condor_job_id="12345.0",
            attempts=1
        )
        
        # Serialize
        data = job.to_dict()
        self.assertEqual(data['job_id'], 42)
        self.assertEqual(data['status'], 'completed')
        
        # Deserialize
        job2 = Job.from_dict(data)
        self.assertEqual(job2.job_id, job.job_id)
        self.assertEqual(job2.status, job.status)
        self.assertEqual(job2.condor_job_id, job.condor_job_id)


class TestProductionConfig(unittest.TestCase):
    """Test ProductionConfig"""
    
    def test_config_creation(self):
        """Test config creation with defaults"""
        with tempfile.TemporaryDirectory() as tmpdir:
            config = ProductionConfig(
                name="test_prod",
                work_dir=Path(tmpdir) / "work",
                exe_path=Path("/path/to/exe"),
                base_config="config.txt",
                output_dir=Path(tmpdir) / "outputs"
            )
            
            self.assertEqual(config.name, "test_prod")
            self.assertEqual(config.backend, "htcondor")
            self.assertEqual(config.max_retries, 3)
            self.assertEqual(config.max_runtime, 3600)
            self.assertTrue(config.validate_outputs)
            self.assertIsNone(config.x509)


class TestProductionManager(unittest.TestCase):
    """Test ProductionManager"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        self.work_dir = Path(self.tmpdir) / "work"
        self.output_dir = Path(self.tmpdir) / "outputs"
        
        self.config = ProductionConfig(
            name="test_production",
            work_dir=self.work_dir,
            exe_path=Path(self.tmpdir) / "test_exe",
            base_config=str(Path(self.tmpdir) / "config.txt"),
            output_dir=self.output_dir
        )
        
        # Create dummy base config
        with open(self.config.base_config, 'w') as f:
            f.write("threads=1\n")
            f.write("batch=False\n")

        # Ensure the executable path exists (tests expect submission to be possible)
        exe_path = Path(self.config.exe_path)
        exe_path.write_text('#!/bin/sh\necho test')
        exe_path.chmod(0o755)
            
    def tearDown(self):
        """Clean up test fixtures"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        
    def test_manager_creation(self):
        """Test manager creation"""
        manager = ProductionManager(self.config)
        
        self.assertEqual(manager.config.name, "test_production")
        self.assertEqual(len(manager.jobs), 0)
        self.assertTrue(self.work_dir.exists())
        
    def test_job_generation(self):
        """Test job generation"""
        manager = ProductionManager(self.config)
        
        file_lists = [
            "file1.root,file2.root",
            "file3.root,file4.root",
            "file5.root"
        ]
        
        count = manager.generate_jobs(file_lists)
        
        self.assertEqual(count, 3)
        self.assertEqual(len(manager.jobs), 3)
        
        # Check that job directories were created
        for job_id in range(3):
            job_dir = self.work_dir / f"job_{job_id}"
            self.assertTrue(job_dir.exists())
            self.assertTrue((job_dir / "job_config.txt").exists())

    def test_generated_remote_output_paths_include_process(self):
        """Remote (__orig_saveFile/__orig_metaFile) and manager job paths include process id suffix"""
        manager = ProductionManager(self.config)
        manager.generate_jobs(["a.root"])
        job = manager.jobs[0]
        # manager stored output paths should include the job/process id suffix
        self.assertTrue(job.output_path.endswith("output_0_0.root"))
        self.assertTrue(job.meta_output_path.endswith("output_0_meta_0.root"))
        # job_config on disk should contain the __orig_saveFile with the same suffix
        cfg_text = (self.work_dir / 'job_0' / 'job_config.txt').read_text()
        self.assertIn('__orig_saveFile=', cfg_text)
        self.assertIn('output_0_0.root', cfg_text)
        self.assertIn('__orig_metaFile=', cfg_text)
        self.assertIn('output_0_meta_0.root', cfg_text)

    def test_generate_jobs_writes_float_int_files(self):
        """When extra_config contains inline float/int content, files are created."""
        manager = ProductionManager(self.config)

        file_lists = ["fileA.root"]
        extra_cfg = {
            'floatConfig': 'normScale=3.14\nsampleNorm=2.0',
            'intConfig': 'type=42',
            'type': '42'
        }

        count = manager.generate_jobs(file_lists, [extra_cfg])
        self.assertEqual(count, 1)

        job_dir = self.work_dir / 'job_0'
        # floats.txt and ints.txt should exist and contain the provided lines
        float_path = job_dir / 'floats.txt'
        int_path = job_dir / 'ints.txt'
        self.assertTrue(float_path.exists(), 'floats.txt missing')
        self.assertTrue(int_path.exists(), 'ints.txt missing')

        float_text = float_path.read_text().splitlines()
        self.assertIn('normScale=3.14', float_text)
        self.assertIn('sampleNorm=2.0', float_text)

        int_text = int_path.read_text().splitlines()
        self.assertIn('type=42', int_text)

        # job_config.txt must reference the filenames (not embed the content)
        cfg_text = (job_dir / 'job_config.txt').read_text()
        self.assertIn('floatConfig=floats.txt', cfg_text)
        self.assertIn('intConfig=ints.txt', cfg_text)

    def test_generate_jobs_copies_base_float_and_int_files(self):
        """If the base config points to existing float/int files, they are copied into jobs."""
        # prepare a cfg/ directory next to the base config and place floats/ints there
        base_cfg_dir = Path(self.config.base_config).parent
        cfg_dir = base_cfg_dir / 'cfg'
        cfg_dir.mkdir(parents=True, exist_ok=True)

        # create floats.txt and ints.txt referenced by the base config
        floats_src = cfg_dir / 'floats.txt'
        floats_src.write_text('from_base=1.23\nsampleNorm=7.0\n')
        ints_src = cfg_dir / 'ints.txt'
        ints_src.write_text('type=99\n')

        # rewrite base config to reference the cfg files
        with open(self.config.base_config, 'w') as f:
            f.write('threads=1\n')
            f.write('batch=False\n')
            f.write('floatConfig=cfg/floats.txt\n')
            f.write('intConfig=cfg/ints.txt\n')

        manager = ProductionManager(self.config)
        count = manager.generate_jobs(["fileX.root"])
        self.assertEqual(count, 1)

        job_dir = self.work_dir / 'job_0'
        float_path = job_dir / 'floats.txt'
        int_path = job_dir / 'ints.txt'
        self.assertTrue(float_path.exists(), 'floats.txt missing from job dir')
        self.assertTrue(int_path.exists(), 'ints.txt missing from job dir')

        float_lines = float_path.read_text().splitlines()
        self.assertIn('from_base=1.23', float_lines)
        self.assertIn('sampleNorm=7.0', float_lines)

        int_lines = int_path.read_text().splitlines()
        self.assertIn('type=99', int_lines)

        # job_config.txt should reference the basename only
        cfg_text = (job_dir / 'job_config.txt').read_text()
        self.assertIn('floatConfig=floats.txt', cfg_text)
        self.assertIn('intConfig=ints.txt', cfg_text)
            
    def test_state_persistence(self):
        """Test that state is saved and loaded correctly"""
        manager1 = ProductionManager(self.config)
        
        # Generate jobs
        file_lists = ["file1.root", "file2.root"]
        manager1.generate_jobs(file_lists)
        
        # Update a job status
        manager1.jobs[0].status = JobStatus.COMPLETED
        manager1._save_state()
        
        # Create new manager with same config
        manager2 = ProductionManager(self.config)
        
        # Check state was restored
        self.assertEqual(len(manager2.jobs), 2)
        self.assertEqual(manager2.jobs[0].status, JobStatus.COMPLETED)
        self.assertEqual(manager2.jobs[1].status, JobStatus.CREATED)
        
    def test_progress_tracking(self):
        """Test progress calculation"""
        manager = ProductionManager(self.config)
        
        # Generate jobs with various statuses
        file_lists = ["file1.root"] * 10
        manager.generate_jobs(file_lists)
        
        # Set various statuses
        manager.jobs[0].status = JobStatus.COMPLETED
        manager.jobs[1].status = JobStatus.COMPLETED
        manager.jobs[2].status = JobStatus.VALIDATED
        manager.jobs[3].status = JobStatus.VALIDATED
        manager.jobs[4].status = JobStatus.FAILED
        # Rest stay as CREATED
        
        progress = manager.get_progress()
        
        self.assertEqual(progress['total_jobs'], 10)
        self.assertEqual(progress['status_counts']['completed'], 2)
        self.assertEqual(progress['status_counts']['validated'], 2)
        self.assertEqual(progress['status_counts']['failed'], 1)
        self.assertEqual(progress['status_counts']['created'], 5)
        self.assertAlmostEqual(progress['completion_rate'], 0.2)
        self.assertAlmostEqual(progress['validation_rate'], 0.2)
        self.assertAlmostEqual(progress['failure_rate'], 0.1)
        
    @patch('production_manager.subprocess.run')
    def test_htcondor_submission(self, mock_run):
        """Test HTCondor job submission"""
        manager = ProductionManager(self.config)
        
        # Generate jobs
        file_lists = ["file1.root", "file2.root"]
        manager.generate_jobs(file_lists)
        
        # Mock successful submission
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "1 job(s) submitted to cluster 12345.\n"
        mock_run.return_value = mock_result
        
        # Submit jobs
        count = manager.submit_jobs(dry_run=False)
        
        self.assertEqual(count, 2)
        self.assertEqual(manager.jobs[0].status, JobStatus.SUBMITTED)
        self.assertEqual(manager.jobs[1].status, JobStatus.SUBMITTED)
        self.assertIsNotNone(manager.jobs[0].submit_time)
        
    def test_output_validation_missing_files(self):
        """Test output validation with missing files"""
        manager = ProductionManager(self.config)
        
        # Generate and "complete" a job
        file_lists = ["file1.root"]
        manager.generate_jobs(file_lists)
        manager.jobs[0].status = JobStatus.COMPLETED
        
        # Validate (files don't exist)
        results = manager.validate_outputs()
        
        self.assertEqual(len(results), 1)
        self.assertFalse(results[0])
        self.assertEqual(manager.jobs[0].status, JobStatus.MISSING_OUTPUT)
        
    def test_output_validation_with_files(self):
        """Test output validation with actual files"""
        manager = ProductionManager(self.config)
        
        # Generate and "complete" a job
        file_lists = ["file1.root"]
        manager.generate_jobs(file_lists)
        manager.jobs[0].status = JobStatus.COMPLETED
        
        # Create output files
        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_file = Path(manager.jobs[0].output_path)
        meta_file = Path(manager.jobs[0].meta_output_path)
        
        # Create non-empty files
        with open(output_file, 'w') as f:
            f.write("dummy content")
        with open(meta_file, 'w') as f:
            f.write("dummy content")
            
        # Validate
        results = manager.validate_outputs()
        
        # Note: Without ROOT, validation will skip file integrity check
        # but should at least confirm files exist and are non-empty
        self.assertEqual(len(results), 1)

    def test_create_test_job(self):
        """ProductionManager can create a local test job directory mirroring a job."""
        manager = ProductionManager(self.config)

        # Generate a job with inline float/int content so files are created
        file_lists = ["fileA.root,fileB.root"]
        extra_cfg = {
            'floatConfig': 'normScale=3.14\nsampleNorm=2.0',
            'intConfig': 'type=42',
            'type': '42'
        }
        manager.generate_jobs(file_lists, [extra_cfg])

        # Ensure the exe file exists so it can be copied into the test dir
        exe_path = self.config.exe_path
        exe_path.write_text('#!/bin/sh\necho test')
        exe_path.chmod(0o755)

        test_dir = manager.create_test_job(0)
        self.assertTrue(test_dir.exists())
        self.assertTrue((test_dir / 'submit_config.txt').exists())

        # Read the generated submit_config and assert it's a test config
        from submission_backend import read_config
        cfg = read_config(str(test_dir / 'submit_config.txt'))
        self.assertEqual(cfg.get('batch'), 'False')
        self.assertEqual(cfg.get('threads'), '1')
        self.assertEqual(cfg.get('saveFile'), 'test_output.root')
        # fileList should be reduced to the first input
        self.assertTrue(cfg.get('fileList', '').startswith('fileA.root'))
        # exe should be copied
        self.assertTrue((test_dir / exe_path.name).exists())        

    def test_create_test_job_includes_x509(self):
        """create_test_job copies configured x509 proxy into the test dir"""
        manager = ProductionManager(self.config)

        # create a dummy x509 proxy and set it in the config
        x509_src = Path(self.tmpdir) / 'proxy'
        x509_src.write_text('dummy')
        manager.config.x509 = str(x509_src)

        # generate a job and create test job
        file_lists = ["file1.root"]
        manager.generate_jobs(file_lists)
        test_dir = manager.create_test_job(0)

        # x509 should be copied into the test directory
        self.assertTrue((test_dir / x509_src.name).exists())

    @patch('submission_backend.write_submit_files')
    @patch('production_manager.subprocess.run')
    def test_htcondor_submission_with_x509(self, mock_run, mock_write_submit):
        """HTCondor submission forwards x509 location to submit-file generator"""
        manager = ProductionManager(self.config)

        # create x509 file and attach to manager config
        x509_src = Path(self.tmpdir) / 'proxy'
        x509_src.write_text('proxy')
        manager.config.x509 = str(x509_src)

        # Generate jobs
        file_lists = ["file1.root", "file2.root"]
        manager.generate_jobs(file_lists)

        # Mock write_submit_files and condor submit
        mock_write_submit.return_value = str(self.work_dir / 'submit.sub')
        mock_result = MagicMock()
        mock_result.returncode = 0
        mock_result.stdout = "1 job(s) submitted to cluster 12345.\n"
        mock_run.return_value = mock_result

        count = manager.submit_jobs(dry_run=False)
        self.assertEqual(count, 2)

        # Ensure write_submit_files received the x509 path
        _, kwargs = mock_write_submit.call_args
        self.assertEqual(kwargs.get('x509loc'), manager.config.x509)
        # Ensure write_submit_files is told to use shared_inputs
        self.assertEqual(kwargs.get('shared_dir_name'), 'shared_inputs')

        # The proxy should have been copied into work_dir/shared_inputs
        x509_name = Path(manager.config.x509).name
        self.assertTrue((manager.config.work_dir / 'shared_inputs' / x509_name).exists())

    def test_resubmit_failed(self):
        """Test resubmitting failed jobs"""
        manager = ProductionManager(self.config)
        
        # Generate jobs
        file_lists = ["file1.root", "file2.root", "file3.root"]
        manager.generate_jobs(file_lists)
        
        # Mark some as failed
        manager.jobs[0].status = JobStatus.FAILED
        manager.jobs[0].attempts = 1
        manager.jobs[1].status = JobStatus.MISSING_OUTPUT
        manager.jobs[1].attempts = 2
        manager.jobs[2].status = JobStatus.VALIDATED  # Don't resubmit
        
        # Mock submission
        with patch('production_manager.subprocess.run') as mock_run:
            mock_result = MagicMock()
            mock_result.returncode = 0
            mock_result.stdout = "jobs submitted to cluster 12345.\n"
            mock_run.return_value = mock_result
            
            # Resubmit with max_attempts=3
            count = manager.resubmit_failed(max_attempts=3)
            
            # Should resubmit jobs 0 and 1 (both under 3 attempts)
            self.assertEqual(count, 2)
            
    def test_resubmit_respects_max_attempts(self):
        """Test that resubmit respects max attempts limit"""
        manager = ProductionManager(self.config)
        
        # Generate job
        file_lists = ["file1.root"]
        manager.generate_jobs(file_lists)
        
        # Mark as failed with too many attempts
        manager.jobs[0].status = JobStatus.FAILED
        manager.jobs[0].attempts = 5
        
        # Try to resubmit with max_attempts=3
        with patch('production_manager.subprocess.run'):
            count = manager.resubmit_failed(max_attempts=3)
            
            # Should not resubmit (already exceeded max)
            self.assertEqual(count, 0)


class TestProductionManagerCLI(unittest.TestCase):
    """Test command-line interface"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.tmpdir = tempfile.mkdtemp()
        
    def tearDown(self):
        """Clean up"""
        import shutil
        shutil.rmtree(self.tmpdir, ignore_errors=True)
        
    @patch('production_manager.sys.argv')
    def test_cli_status_command(self, mock_argv):
        """Test status command"""
        # This is a basic test - more comprehensive CLI testing would use
        # integration tests or mock the manager more thoroughly
        pass


def run_tests():
    """Run all tests"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add all test classes
    suite.addTests(loader.loadTestsFromTestCase(TestJob))
    suite.addTests(loader.loadTestsFromTestCase(TestProductionConfig))
    suite.addTests(loader.loadTestsFromTestCase(TestProductionManager))
    suite.addTests(loader.loadTestsFromTestCase(TestProductionManagerCLI))
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == '__main__':
    success = run_tests()
    sys.exit(0 if success else 1)
