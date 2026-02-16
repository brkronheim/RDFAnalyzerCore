#!/usr/bin/env python3
"""
Production Manager for RDFAnalyzerCore

A unified, cohesive manager for generating, testing, monitoring, and validating
batch analysis jobs. Designed to be resilient to connection failures and work
in AFS/EOS areas.

Features:
- Job generation with automatic splitting
- Job testing and validation
- Output verification
- Progress monitoring and reporting
- State persistence (can stop/start without breaking)
- HTCondor submission with optional DASK backend
- Support for AFS/EOS storage areas
"""

import argparse
import json
import logging
import os
import pickle
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Any
import shutil

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class JobStatus(Enum):
    """Status of a production job"""
    CREATED = "created"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    VALIDATED = "validated"
    MISSING_OUTPUT = "missing_output"


@dataclass
class Job:
    """Represents a single production job"""
    job_id: int
    config_path: str
    output_path: str
    meta_output_path: str
    status: JobStatus = JobStatus.CREATED
    condor_job_id: Optional[str] = None
    submit_time: Optional[float] = None
    completion_time: Optional[float] = None
    attempts: int = 0
    last_error: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Serialize job to dictionary"""
        return {
            'job_id': self.job_id,
            'config_path': self.config_path,
            'output_path': self.output_path,
            'meta_output_path': self.meta_output_path,
            'status': self.status.value,
            'condor_job_id': self.condor_job_id,
            'submit_time': self.submit_time,
            'completion_time': self.completion_time,
            'attempts': self.attempts,
            'last_error': self.last_error,
        }
    
    @classmethod
    def from_dict(cls, data: dict) -> 'Job':
        """Deserialize job from dictionary"""
        data = data.copy()
        data['status'] = JobStatus(data['status'])
        return cls(**data)


@dataclass
class ProductionConfig:
    """Configuration for a production run"""
    name: str
    work_dir: Path
    exe_path: Path
    base_config: str
    output_dir: Path
    backend: str = "htcondor"  # htcondor or dask
    stage_inputs: bool = False
    stage_outputs: bool = False
    max_retries: int = 3
    validate_outputs: bool = True
    state_file: Optional[Path] = None
    
    def __post_init__(self):
        """Initialize derived attributes"""
        if self.state_file is None:
            self.state_file = self.work_dir / "production_state.json"


class ProductionManager:
    """
    Main production manager class.
    
    Manages the full lifecycle of a batch production:
    - Job generation
    - Job submission (HTCondor or DASK)
    - Progress monitoring
    - Output validation
    - State persistence for resilience
    """
    
    def __init__(self, config: ProductionConfig):
        self.config = config
        self.jobs: Dict[int, Job] = {}
        self.state_file = config.state_file
        
        # Create work directory
        self.config.work_dir.mkdir(parents=True, exist_ok=True)
        
        # Try to restore previous state
        self._load_state()
        
    def _load_state(self) -> None:
        """Load production state from disk"""
        if self.state_file and self.state_file.exists():
            try:
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                self.jobs = {
                    int(job_id): Job.from_dict(job_data)
                    for job_id, job_data in state.get('jobs', {}).items()
                }
                logger.info(f"Loaded state with {len(self.jobs)} jobs from {self.state_file}")
            except Exception as e:
                logger.warning(f"Failed to load state from {self.state_file}: {e}")
                
    def _save_state(self) -> None:
        """Save production state to disk"""
        try:
            state = {
                'production_name': self.config.name,
                'timestamp': time.time(),
                'jobs': {
                    job_id: job.to_dict()
                    for job_id, job in self.jobs.items()
                }
            }
            # Write atomically using temp file
            temp_file = self.state_file.with_suffix('.tmp')
            with open(temp_file, 'w') as f:
                json.dump(state, f, indent=2)
            temp_file.rename(self.state_file)
            logger.debug(f"Saved state with {len(self.jobs)} jobs to {self.state_file}")
        except Exception as e:
            logger.error(f"Failed to save state to {self.state_file}: {e}")
            
    def generate_jobs(
        self,
        file_lists: List[str],
        job_configs: Optional[List[Dict[str, Any]]] = None
    ) -> int:
        """
        Generate job configurations.
        
        Args:
            file_lists: List of comma-separated file lists, one per job
            job_configs: Optional list of additional config parameters per job
            
        Returns:
            Number of jobs created
        """
        logger.info(f"Generating {len(file_lists)} jobs")
        
        if job_configs is None:
            job_configs = [{}] * len(file_lists)
            
        for i, (file_list, extra_config) in enumerate(zip(file_lists, job_configs)):
            job_id = len(self.jobs)
            
            # Create job directory
            job_dir = self.config.work_dir / f"job_{job_id}"
            job_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate job config
            config_path = job_dir / "job_config.txt"
            output_path = self.config.output_dir / f"output_{job_id}.root"
            meta_output_path = self.config.output_dir / f"output_{job_id}_meta.root"
            
            # Read base config and update with job-specific values
            from submission_backend import read_config, write_config
            base_config = read_config(self.config.base_config)
            base_config['fileList'] = file_list
            base_config['saveFile'] = str(output_path)
            base_config['metaFile'] = str(meta_output_path)
            base_config['batch'] = 'True'
            base_config.update(extra_config)
            
            write_config(base_config, str(config_path))
            
            # Create job object
            job = Job(
                job_id=job_id,
                config_path=str(config_path),
                output_path=str(output_path),
                meta_output_path=str(meta_output_path),
            )
            self.jobs[job_id] = job
            
        self._save_state()
        logger.info(f"Created {len(file_lists)} jobs")
        return len(file_lists)
        
    def test_job(self, job_id: int, local: bool = True) -> bool:
        """
        Test a single job locally or on batch system.
        
        Args:
            job_id: ID of job to test
            local: If True, run locally. Otherwise submit to batch.
            
        Returns:
            True if test succeeded
        """
        if job_id not in self.jobs:
            logger.error(f"Job {job_id} not found")
            return False
            
        job = self.jobs[job_id]
        logger.info(f"Testing job {job_id}")
        
        if local:
            # Run locally
            try:
                cmd = [str(self.config.exe_path), job.config_path]
                result = subprocess.run(
                    cmd,
                    capture_output=True,
                    text=True,
                    timeout=300  # 5 minute timeout for test
                )
                
                if result.returncode != 0:
                    logger.error(f"Job {job_id} test failed: {result.stderr}")
                    job.last_error = result.stderr
                    self._save_state()
                    return False
                    
                logger.info(f"Job {job_id} test succeeded")
                return True
                
            except subprocess.TimeoutExpired:
                logger.error(f"Job {job_id} test timed out")
                job.last_error = "Test timeout"
                self._save_state()
                return False
            except Exception as e:
                logger.error(f"Job {job_id} test failed with exception: {e}")
                job.last_error = str(e)
                self._save_state()
                return False
        else:
            # Submit as batch test job (not yet implemented)
            logger.warning("Batch testing not yet implemented")
            return False
            
    def submit_jobs(
        self,
        job_ids: Optional[List[int]] = None,
        dry_run: bool = False
    ) -> int:
        """
        Submit jobs to batch system.
        
        Args:
            job_ids: List of job IDs to submit. If None, submit all created jobs.
            dry_run: If True, don't actually submit
            
        Returns:
            Number of jobs submitted
        """
        if job_ids is None:
            job_ids = [
                job_id for job_id, job in self.jobs.items()
                if job.status == JobStatus.CREATED
            ]
            
        if not job_ids:
            logger.info("No jobs to submit")
            return 0
            
        logger.info(f"Submitting {len(job_ids)} jobs")
        
        if self.config.backend == "htcondor":
            return self._submit_htcondor(job_ids, dry_run)
        elif self.config.backend == "dask":
            return self._submit_dask(job_ids, dry_run)
        else:
            logger.error(f"Unknown backend: {self.config.backend}")
            return 0
            
    def _submit_htcondor(self, job_ids: List[int], dry_run: bool) -> int:
        """Submit jobs via HTCondor"""
        from submission_backend import write_submit_files
        
        # Generate condor submission files
        submit_path = write_submit_files(
            str(self.config.work_dir),
            len(job_ids),
            os.path.basename(self.config.exe_path),
            self.config.stage_inputs,
            self.config.stage_outputs,
            "",  # root_setup
            x509loc=None,
            config_file="job_config.txt",
        )
        
        if dry_run:
            logger.info(f"Dry run: would submit {submit_path}")
            return len(job_ids)
            
        # Submit to condor
        try:
            result = subprocess.run(
                ["condor_submit", submit_path],
                capture_output=True,
                text=True,
                check=True
            )
            
            # Parse cluster ID from output using regex
            cluster_id = None
            import re
            match = re.search(r'submitted to cluster (\d+)', result.stdout, re.IGNORECASE)
            if match:
                cluster_id = match.group(1)
                            
            # Update job statuses
            submit_time = time.time()
            for job_id in job_ids:
                job = self.jobs[job_id]
                job.status = JobStatus.SUBMITTED
                job.submit_time = submit_time
                job.attempts += 1
                if cluster_id:
                    job.condor_job_id = f"{cluster_id}.{job_id}"
                    
            self._save_state()
            logger.info(f"Submitted {len(job_ids)} jobs (cluster {cluster_id})")
            return len(job_ids)
            
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed to submit jobs: {e.stderr}")
            return 0
            
    def _submit_dask(self, job_ids: List[int], dry_run: bool) -> int:
        """Submit jobs via DASK"""
        try:
            from dask.distributed import Client, LocalCluster
            from dask_jobqueue import HTCondorCluster
        except ImportError:
            logger.error("DASK not installed. Install with: pip install dask distributed dask-jobqueue")
            return 0
            
        if dry_run:
            logger.info(f"Dry run: would submit {len(job_ids)} jobs via DASK")
            return len(job_ids)
            
        try:
            # Create DASK cluster
            cluster = HTCondorCluster(
                cores=1,
                memory="2GB",
                disk="2GB",
                job_extra={
                    "+MaxRuntime": "3600",
                }
            )
            cluster.scale(jobs=len(job_ids))
            
            client = Client(cluster)
            
            # Submit jobs
            def run_job(job_id: int, exe_path: str, config_path: str) -> dict:
                """Run a single job"""
                import subprocess
                result = subprocess.run(
                    [exe_path, config_path],
                    capture_output=True,
                    text=True
                )
                return {
                    'job_id': job_id,
                    'returncode': result.returncode,
                    'stdout': result.stdout,
                    'stderr': result.stderr,
                }
                
            # Submit all jobs
            futures = []
            for job_id in job_ids:
                job = self.jobs[job_id]
                future = client.submit(
                    run_job,
                    job_id,
                    str(self.config.exe_path),
                    job.config_path
                )
                futures.append(future)
                job.status = JobStatus.SUBMITTED
                job.submit_time = time.time()
                job.attempts += 1
                
            self._save_state()
            logger.info(f"Submitted {len(job_ids)} jobs via DASK")
            
            # Note: In production, you'd want to monitor these futures asynchronously
            # For now, we just return the count
            return len(job_ids)
            
        except Exception as e:
            logger.error(f"Failed to submit jobs via DASK: {e}")
            return 0
            
    def update_status(self) -> Dict[JobStatus, int]:
        """
        Update status of all submitted jobs.
        
        Returns:
            Dictionary mapping status to count
        """
        logger.info("Updating job statuses")
        
        if self.config.backend == "htcondor":
            self._update_htcondor_status()
        elif self.config.backend == "dask":
            # DASK status updates would go here
            pass
            
        # Count statuses
        status_counts = {}
        for status in JobStatus:
            status_counts[status] = sum(
                1 for job in self.jobs.values() if job.status == status
            )
            
        self._save_state()
        return status_counts
        
    def _update_htcondor_status(self) -> None:
        """Update job statuses from HTCondor"""
        # Get condor_q output
        try:
            result = subprocess.run(
                ["condor_q", "-json"],
                capture_output=True,
                text=True,
                timeout=30
            )
            
            if result.returncode != 0:
                logger.warning("Failed to query condor_q")
                return
                
            queue_data = json.loads(result.stdout)
            
            # Build mapping of condor job ID to status
            condor_statuses = {}
            for entry in queue_data:
                job_id = entry.get('ClusterId')
                proc_id = entry.get('ProcId')
                if job_id is not None and proc_id is not None:
                    full_id = f"{job_id}.{proc_id}"
                    status = entry.get('JobStatus')
                    condor_statuses[full_id] = status
                    
            # Update our jobs
            for job in self.jobs.values():
                if job.condor_job_id and job.condor_job_id in condor_statuses:
                    condor_status = condor_statuses[job.condor_job_id]
                    # HTCondor status: 1=Idle, 2=Running, 3=Removed, 4=Completed, 5=Held
                    if condor_status == 2:
                        job.status = JobStatus.RUNNING
                    elif condor_status == 4:
                        job.status = JobStatus.COMPLETED
                        job.completion_time = time.time()
                    elif condor_status == 5:
                        job.status = JobStatus.FAILED
                        
        except Exception as e:
            logger.warning(f"Failed to update HTCondor status: {e}")
            
    def validate_outputs(self, job_ids: Optional[List[int]] = None) -> Dict[int, bool]:
        """
        Validate that job outputs were created correctly.
        
        Args:
            job_ids: List of job IDs to validate. If None, validate all completed jobs.
            
        Returns:
            Dictionary mapping job ID to validation result
        """
        if job_ids is None:
            job_ids = [
                job_id for job_id, job in self.jobs.items()
                if job.status == JobStatus.COMPLETED
            ]
            
        logger.info(f"Validating outputs for {len(job_ids)} jobs")
        
        results = {}
        for job_id in job_ids:
            if job_id not in self.jobs:
                continue
                
            job = self.jobs[job_id]
            
            # Check if output files exist and are non-empty
            valid = True
            
            if not os.path.exists(job.output_path):
                logger.warning(f"Job {job_id} missing output file: {job.output_path}")
                job.status = JobStatus.MISSING_OUTPUT
                valid = False
            elif os.path.getsize(job.output_path) == 0:
                logger.warning(f"Job {job_id} output file is empty: {job.output_path}")
                job.status = JobStatus.MISSING_OUTPUT
                valid = False
                
            if valid and not os.path.exists(job.meta_output_path):
                logger.warning(f"Job {job_id} missing meta output: {job.meta_output_path}")
                job.status = JobStatus.MISSING_OUTPUT
                valid = False
            elif valid and os.path.getsize(job.meta_output_path) == 0:
                logger.warning(f"Job {job_id} meta output is empty: {job.meta_output_path}")
                job.status = JobStatus.MISSING_OUTPUT
                valid = False
                
            # If files exist, try to open them with ROOT to verify they're valid
            if valid:
                try:
                    import ROOT
                    f = ROOT.TFile.Open(job.output_path)
                    if not f or f.IsZombie():
                        logger.warning(f"Job {job_id} output file is corrupted: {job.output_path}")
                        job.status = JobStatus.FAILED
                        valid = False
                    else:
                        f.Close()
                        
                    if valid:
                        f = ROOT.TFile.Open(job.meta_output_path)
                        if not f or f.IsZombie():
                            logger.warning(f"Job {job_id} meta output is corrupted: {job.meta_output_path}")
                            job.status = JobStatus.FAILED
                            valid = False
                        else:
                            f.Close()
                            
                except ImportError:
                    logger.warning("ROOT not available, skipping file validation")
                except Exception as e:
                    logger.warning(f"Job {job_id} validation failed: {e}")
                    job.status = JobStatus.FAILED
                    valid = False
                    
            if valid:
                job.status = JobStatus.VALIDATED
                
            results[job_id] = valid
            
        self._save_state()
        return results
        
    def get_progress(self) -> dict:
        """
        Get current production progress.
        
        Returns:
            Dictionary with progress information
        """
        status_counts = {}
        for status in JobStatus:
            status_counts[status.value] = sum(
                1 for job in self.jobs.values() if job.status == status
            )
            
        total = len(self.jobs)
        completed = status_counts[JobStatus.COMPLETED.value]
        validated = status_counts[JobStatus.VALIDATED.value]
        failed = status_counts[JobStatus.FAILED.value]
        missing = status_counts[JobStatus.MISSING_OUTPUT.value]
        
        progress = {
            'total_jobs': total,
            'status_counts': status_counts,
            'completion_rate': completed / total if total > 0 else 0.0,
            'validation_rate': validated / total if total > 0 else 0.0,
            'failure_rate': (failed + missing) / total if total > 0 else 0.0,
        }
        
        return progress
        
    def print_progress(self) -> None:
        """Print progress summary to console"""
        progress = self.get_progress()
        
        print(f"\n{'='*60}")
        print(f"Production: {self.config.name}")
        print(f"{'='*60}")
        print(f"Total jobs: {progress['total_jobs']}")
        print(f"\nStatus breakdown:")
        for status, count in progress['status_counts'].items():
            if count > 0:
                pct = 100.0 * count / progress['total_jobs']
                print(f"  {status:20s}: {count:5d} ({pct:5.1f}%)")
        print(f"\nCompletion rate: {100.0 * progress['completion_rate']:.1f}%")
        print(f"Validation rate: {100.0 * progress['validation_rate']:.1f}%")
        print(f"Failure rate:    {100.0 * progress['failure_rate']:.1f}%")
        print(f"{'='*60}\n")
        
    def resubmit_failed(self, max_attempts: Optional[int] = None) -> int:
        """
        Resubmit failed jobs.
        
        Args:
            max_attempts: Maximum number of attempts per job. If None, use config value.
            
        Returns:
            Number of jobs resubmitted
        """
        if max_attempts is None:
            max_attempts = self.config.max_retries
            
        failed_job_ids = [
            job_id for job_id, job in self.jobs.items()
            if job.status in (JobStatus.FAILED, JobStatus.MISSING_OUTPUT)
            and job.attempts < max_attempts
        ]
        
        if not failed_job_ids:
            logger.info("No failed jobs to resubmit")
            return 0
            
        logger.info(f"Resubmitting {len(failed_job_ids)} failed jobs")
        
        # Reset status to created so they'll be submitted
        for job_id in failed_job_ids:
            self.jobs[job_id].status = JobStatus.CREATED
            
        return self.submit_jobs(failed_job_ids)
        
    def cleanup(self, keep_outputs: bool = True) -> None:
        """
        Clean up production work directory.
        
        Args:
            keep_outputs: If True, keep output files
        """
        logger.info("Cleaning up production work directory")
        
        if not keep_outputs:
            # Remove output files
            for job in self.jobs.values():
                for path in [job.output_path, job.meta_output_path]:
                    if os.path.exists(path):
                        os.remove(path)
                        logger.debug(f"Removed {path}")
                        
        # Save final state
        self._save_state()


def main():
    """Command-line interface for ProductionManager"""
    parser = argparse.ArgumentParser(
        description="Production Manager for RDFAnalyzerCore batch analyses"
    )
    parser.add_argument(
        "command",
        choices=["create", "submit", "status", "validate", "resubmit", "test"],
        help="Command to execute"
    )
    parser.add_argument(
        "--name", "-n",
        required=True,
        help="Production name"
    )
    parser.add_argument(
        "--work-dir", "-w",
        help="Working directory for production"
    )
    parser.add_argument(
        "--exe",
        help="Path to analysis executable"
    )
    parser.add_argument(
        "--config", "-c",
        help="Base configuration file"
    )
    parser.add_argument(
        "--output-dir", "-o",
        help="Output directory for results"
    )
    parser.add_argument(
        "--backend",
        choices=["htcondor", "dask"],
        default="htcondor",
        help="Batch submission backend"
    )
    parser.add_argument(
        "--stage-inputs",
        action="store_true",
        help="Stage input files to worker nodes"
    )
    parser.add_argument(
        "--stage-outputs",
        action="store_true",
        help="Stage output files from worker nodes"
    )
    parser.add_argument(
        "--job-id",
        type=int,
        help="Specific job ID for test command"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without doing it"
    )
    
    args = parser.parse_args()
    
    # Build production config
    work_dir = Path(args.work_dir) if args.work_dir else Path(f"production_{args.name}")
    
    prod_config = ProductionConfig(
        name=args.name,
        work_dir=work_dir,
        exe_path=Path(args.exe) if args.exe else Path("./analyzer"),
        base_config=args.config if args.config else "config.txt",
        output_dir=Path(args.output_dir) if args.output_dir else work_dir / "outputs",
        backend=args.backend,
        stage_inputs=args.stage_inputs,
        stage_outputs=args.stage_outputs,
    )
    
    # Create manager
    manager = ProductionManager(prod_config)
    
    # Execute command
    if args.command == "create":
        logger.error("Create command requires custom implementation")
        logger.error("Use generateSubmissionFilesNANO.py or generateSubmissionFilesOpenData.py")
        sys.exit(1)
        
    elif args.command == "submit":
        count = manager.submit_jobs(dry_run=args.dry_run)
        print(f"Submitted {count} jobs")
        
    elif args.command == "status":
        manager.update_status()
        manager.print_progress()
        
    elif args.command == "validate":
        results = manager.validate_outputs()
        valid_count = sum(1 for v in results.values() if v)
        print(f"Validated {valid_count}/{len(results)} jobs")
        manager.print_progress()
        
    elif args.command == "resubmit":
        count = manager.resubmit_failed()
        print(f"Resubmitted {count} failed jobs")
        
    elif args.command == "test":
        if args.job_id is None:
            logger.error("--job-id required for test command")
            sys.exit(1)
        success = manager.test_job(args.job_id)
        sys.exit(0 if success else 1)
        
    else:
        logger.error(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
