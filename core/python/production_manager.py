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
import re
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
    base_config: str
    output_dir: Path
    exe_path: Optional[Path] = None
    backend: str = "htcondor"  # htcondor or dask
    stage_inputs: bool = False
    stage_outputs: bool = False
    max_retries: int = 3
    max_runtime: int = 3600
    validate_outputs: bool = True
    eos_sched: bool = False
    root_setup: str = ""  # command to setup ROOT on remote workers
    x509: Optional[str] = None  # path to x509 proxy (optional)
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

    def _resolve_existing_path(self, path_str: str) -> Path:
        p = Path(path_str)
        if p.is_absolute():
            return p

        repo_root = Path(__file__).resolve().parents[2]
        candidates = [
            repo_root / p,
            self.config.work_dir.parent / p,
        ]
        try:
            candidates.insert(0, Path.cwd() / p)
        except FileNotFoundError:
            pass
        for c in candidates:
            if c.exists():
                return c.resolve()
        return p
            
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
            
        from submission_backend import read_config, write_config, get_copy_file_list

        base_config_path = Path(self.config.base_config).resolve()
        base_config_dir = base_config_path.parent
        repo_root = Path(__file__).resolve().parents[2]
        shared_inputs_dir = self.config.work_dir / "shared_inputs"
        shared_aux_dir = shared_inputs_dir / "aux"
        shared_inputs_dir.mkdir(parents=True, exist_ok=True)

        def _resolve_path(path_value: str) -> Optional[Path]:
            if not path_value:
                return None
            p = Path(path_value)
            if p.is_absolute() and p.exists():
                return p
            candidates = [
                (base_config_dir / path_value),
                (base_config_dir.parent / path_value),
                (repo_root / path_value),
                Path(path_value),
            ]
            for candidate in candidates:
                if candidate.exists():
                    return candidate.resolve()
            return None

        def _copy_file_to_job(src: Path, job_dir: Path) -> Path:
            dst = job_dir / src.name
            dst.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(str(src), str(dst))
            return dst

        def _copy_aux_payload(job_dir: Path) -> None:
            analysis_aux_dir = base_config_dir.parent / "aux"
            if analysis_aux_dir.exists() and analysis_aux_dir.is_dir():
                try:
                    rel_aux = analysis_aux_dir.relative_to(repo_root)
                    aux_dst = job_dir / "aux" / rel_aux
                    shared_aux_dst = shared_aux_dir / rel_aux
                except ValueError:
                    aux_dst = job_dir / "aux" / analysis_aux_dir.name
                    shared_aux_dst = shared_aux_dir / analysis_aux_dir.name
                aux_dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(str(analysis_aux_dir), str(aux_dst), dirs_exist_ok=True)
                shared_aux_dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.copytree(str(analysis_aux_dir), str(shared_aux_dst), dirs_exist_ok=True)

            root_aux_dir = repo_root / "aux"
            if root_aux_dir.exists() and root_aux_dir.is_dir():
                shutil.copytree(str(root_aux_dir), str(shared_aux_dir), dirs_exist_ok=True)

            corr_cfg_rel = read_config(self.config.base_config).get('correctionConfig', '')
            corr_cfg_src = _resolve_path(corr_cfg_rel)
            if not corr_cfg_src or not corr_cfg_src.exists():
                return

            try:
                with open(corr_cfg_src, 'r') as corr_file:
                    for line in corr_file:
                        line = line.split('#', 1)[0].strip()
                        if not line:
                            continue
                        match = re.search(r'(^|\s)file=([^\s]+)', line)
                        if not match:
                            continue
                        rel_data_path = match.group(2).strip()
                        src_data = _resolve_path(rel_data_path)
                        if src_data is None and rel_data_path.startswith('aux/'):
                            src_data = _resolve_path(rel_data_path[len('aux/'):])
                        if src_data and src_data.exists() and src_data.is_file():
                            # Keep correction payloads available as top-level files
                            # in job directories / worker scratch.
                            dst_data = job_dir / src_data.name
                            shutil.copy2(str(src_data), str(dst_data))

                            # Also stage in shared aux using basename for transfer.
                            shared_data = shared_aux_dir / src_data.name
                            shared_data.parent.mkdir(parents=True, exist_ok=True)
                            shutil.copy2(str(src_data), str(shared_data))
            except Exception:
                pass

        def _rewrite_correction_config_to_top_level(job_dir: Path, cfg_name: str) -> None:
            if not cfg_name:
                return
            corr_cfg_path = job_dir / os.path.basename(cfg_name)
            if not corr_cfg_path.exists() or not corr_cfg_path.is_file():
                return

            try:
                lines_out = []
                with open(corr_cfg_path, 'r') as corr_file:
                    for raw_line in corr_file:
                        line = raw_line.rstrip('\n')
                        if not line.strip() or line.lstrip().startswith('#'):
                            lines_out.append(raw_line)
                            continue

                        def _replace_file_ref(match: re.Match) -> str:
                            prefix = match.group(1)
                            value = match.group(2).strip()
                            src_data = _resolve_path(value)
                            if src_data is None and value.startswith('aux/'):
                                src_data = _resolve_path(value[len('aux/'):])
                            if src_data and src_data.exists() and src_data.is_file():
                                dst = job_dir / src_data.name
                                try:
                                    shutil.copy2(str(src_data), str(dst))
                                except Exception:
                                    pass
                                return f"{prefix}file={src_data.name}"
                            return f"{prefix}file={os.path.basename(value)}"

                        rewritten = re.sub(r'(^|\s)file=([^\s#]+)', _replace_file_ref, line)
                        lines_out.append(rewritten + '\n')

                with open(corr_cfg_path, 'w') as corr_file:
                    corr_file.writelines(lines_out)
            except Exception:
                pass

        for i, (file_list, extra_config) in enumerate(zip(file_lists, job_configs)):
            job_id = len(self.jobs)
            
            # Create job directory
            job_dir = self.config.work_dir / f"job_{job_id}"
            job_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate job config
            config_path = job_dir / "job_config.txt"
            # include the Condor process id (which equals job_id) in the remote filenames
            # so uploaded EOS paths contain the process for easy tracing.
            output_path = self.config.output_dir / f"output_{job_id}_{job_id}.root"
            meta_output_path = self.config.output_dir / f"output_{job_id}_meta_{job_id}.root"
            
            base_config = read_config(self.config.base_config)
            base_config['fileList'] = file_list
            base_config['__orig_fileList'] = file_list
            base_config['saveFile'] = str(output_path)
            base_config['metaFile'] = str(meta_output_path)
            base_config['__orig_saveFile'] = str(output_path)
            base_config['__orig_metaFile'] = str(meta_output_path)
            base_config['batch'] = 'True'

            # Handle special inline float/int config content (keep backward compatibility
            # with generateSubmissionFilesNANO.py which appends lines into per-job files).
            # If `extra_config` provides `floatConfig`/`intConfig` as content (contains '\n'
            # or an '=' sign and is not a filename), write that content into a file in the
            # job directory and set the job config to reference the filename.
            if extra_config is None:
                extra_config = {}

            def _is_inline_content(val: Any) -> bool:
                if not isinstance(val, str):
                    return False
                # treat as content if it contains a newline or looks like key=value
                if '\n' in val:
                    return True
                if '=' in val and not any(val.strip().endswith(ext) for ext in ('.txt', '.yaml', '.yml')) and os.path.sep not in val:
                    return True
                return False

            # If base config already names the float/int file, reuse that basename
            # (strip any directory components so per-job files live inside the job dir)
            float_filename = os.path.basename(base_config.get('floatConfig', 'floats.txt'))
            int_filename = os.path.basename(base_config.get('intConfig', 'ints.txt'))

            # If the base config references an existing float/int file (absolute or
            # relative to the base-config directory), copy its contents into the
            # per-job file so job folders always contain concrete `floats.txt` and
            # `ints.txt` entries. This preserves lines and will be merged with any
            # inline extra_config content later.
            # Copy general referenced config files into job folder and relink to basename
            for cfg_key, cfg_value in list(base_config.items()):
                if cfg_key in ('floatConfig', 'intConfig'):
                    continue
                if not isinstance(cfg_value, str):
                    continue
                if not ('.txt' in cfg_value or '.yaml' in cfg_value or '.yml' in cfg_value):
                    continue
                src_cfg = _resolve_path(cfg_value)
                if src_cfg and src_cfg.exists() and src_cfg.is_file():
                    copied_cfg = _copy_file_to_job(src_cfg, job_dir)
                    base_config[cfg_key] = copied_cfg.name

            # Pre-populate per-job files from base config sources (if present)
            base_float_src = _resolve_path(base_config.get('floatConfig', ''))
            if base_float_src:
                float_path = job_dir / float_filename
                float_path.parent.mkdir(parents=True, exist_ok=True)
                # copy contents preserving existing ordering/deduplication behavior
                existing = []
                if float_path.exists():
                    with open(float_path, 'r') as _f:
                        existing = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                with open(base_float_src, 'r') as _f:
                    base_lines = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                seen = set(existing)
                merged = list(existing)
                for ln in base_lines:
                    if ln in seen:
                        continue
                    seen.add(ln)
                    merged.append(ln)
                with open(float_path, 'w') as _f:
                    for ln in merged:
                        _f.write(ln + '\n')
                base_config['floatConfig'] = os.path.basename(str(float_path))

            base_int_src = _resolve_path(base_config.get('intConfig', ''))
            if base_int_src:
                int_path = job_dir / int_filename
                int_path.parent.mkdir(parents=True, exist_ok=True)
                existing = []
                if int_path.exists():
                    with open(int_path, 'r') as _f:
                        existing = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                with open(base_int_src, 'r') as _f:
                    base_lines = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                seen = set(existing)
                merged = list(existing)
                for ln in base_lines:
                    if ln in seen:
                        continue
                    seen.add(ln)
                    merged.append(ln)
                with open(int_path, 'w') as _f:
                    for ln in merged:
                        _f.write(ln + '\n')
                base_config['intConfig'] = os.path.basename(str(int_path))

            # Process floatConfig
            if 'floatConfig' in extra_config and _is_inline_content(extra_config['floatConfig']):
                float_lines = [ln for ln in str(extra_config['floatConfig']).splitlines() if ln.strip()]
                float_path = job_dir / float_filename
                # create parent directory if the configured filename contains subdirs
                float_path.parent.mkdir(parents=True, exist_ok=True)
                # Deduplicate while preserving order if file already exists
                existing = []
                if float_path.exists():
                    with open(float_path, 'r') as _f:
                        existing = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                seen = set(existing)
                merged = list(existing)
                for ln in float_lines:
                    if ln in seen:
                        continue
                    seen.add(ln)
                    merged.append(ln)
                with open(float_path, 'w') as _f:
                    for ln in merged:
                        _f.write(ln + '\n')
                base_config['floatConfig'] = os.path.basename(str(float_path))
            elif 'floatConfig' in extra_config:
                float_src = _resolve_path(str(extra_config['floatConfig']))
                if float_src and float_src.exists() and float_src.is_file():
                    copied_float = _copy_file_to_job(float_src, job_dir)
                    base_config['floatConfig'] = copied_float.name
                else:
                    base_config['floatConfig'] = str(extra_config['floatConfig'])

            # Process intConfig
            if 'intConfig' in extra_config and _is_inline_content(extra_config['intConfig']):
                int_lines = [ln for ln in str(extra_config['intConfig']).splitlines() if ln.strip()]
                int_path = job_dir / int_filename
                # create parent directory if the configured filename contains subdirs
                int_path.parent.mkdir(parents=True, exist_ok=True)
                existing = []
                if int_path.exists():
                    with open(int_path, 'r') as _f:
                        existing = [l.rstrip('\n') for l in _f if l.rstrip('\n')]
                seen = set(existing)
                merged = list(existing)
                for ln in int_lines:
                    if ln in seen:
                        continue
                    seen.add(ln)
                    merged.append(ln)
                with open(int_path, 'w') as _f:
                    for ln in merged:
                        _f.write(ln + '\n')
                base_config['intConfig'] = os.path.basename(str(int_path))
            elif 'intConfig' in extra_config:
                int_src = _resolve_path(str(extra_config['intConfig']))
                if int_src and int_src.exists() and int_src.is_file():
                    copied_int = _copy_file_to_job(int_src, job_dir)
                    base_config['intConfig'] = copied_int.name
                else:
                    base_config['intConfig'] = str(extra_config['intConfig'])

            # Merge any remaining keys from extra_config (e.g. 'type')
            for k, v in extra_config.items():
                if k in ('floatConfig', 'intConfig'):
                    continue
                base_config[k] = str(v)

            _copy_aux_payload(job_dir)
            _rewrite_correction_config_to_top_level(job_dir, base_config.get('correctionConfig', ''))

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

    def create_test_job(self, job_id: int) -> Path:
        """Create a local test job directory for an existing job.

        The created test job mirrors the job's config but runs only the first
        input file and uses `batch=False`, `threads=1`. Files referenced by
        `floatConfig`/`intConfig` are copied if present.

        Returns the Path to the created test directory.
        """
        from submission_backend import read_config, write_config, get_copy_file_list

        if job_id not in self.jobs:
            raise KeyError(f"Job {job_id} not found")

        job = self.jobs[job_id]
        job_config_path = self._resolve_existing_path(job.config_path)
        job_cfg = read_config(str(job_config_path))

        # Determine first input file
        file_list = job_cfg.get('fileList', '')
        first_file = ''
        if file_list:
            first_file = [p.strip() for p in file_list.split(',') if p.strip()]
            first_file = first_file[0] if first_file else ''

        # Prepare test directory
        test_dir = self.config.work_dir / 'test_job'
        if test_dir.exists():
            in_test_dir = False
            try:
                current_dir = Path.cwd().resolve()
                in_test_dir = (current_dir == test_dir.resolve() or test_dir.resolve() in current_dir.parents)
            except FileNotFoundError:
                in_test_dir = False

            if in_test_dir:
                for child in test_dir.iterdir():
                    if child.is_dir():
                        shutil.rmtree(child)
                    else:
                        child.unlink()
            else:
                shutil.rmtree(test_dir)
        test_dir.mkdir(parents=True, exist_ok=True)

        # Copy executable and staged libs (if present)
        if self.config.exe_path and Path(self.config.exe_path).is_file():
            exe_src = Path(self.config.exe_path)
            exe_dst = test_dir / exe_src.name
            shutil.copy2(str(exe_src), str(exe_dst))
            exe_dst.chmod(0o755)
        else:
            logger.warning("Executable not configured or not found — test job will not include an executable")

        lib_dir = self.config.work_dir / 'lib'
        if lib_dir.exists() and lib_dir.is_dir():
            # copy only files (no directories)
            target_lib_dir = test_dir / 'lib'
            target_lib_dir.mkdir(exist_ok=True)
            for p in sorted(lib_dir.iterdir()):
                if p.is_file():
                    shutil.copy2(str(p), str(target_lib_dir / p.name))
        # Copy any auxiliary files referenced in the base config
        try:
            base_cfg = read_config(self.config.base_config)
            copy_list = get_copy_file_list(base_cfg)
            base_dir = os.path.dirname(self.config.base_config)
            for fname in copy_list:
                # resolve relative to base_config dir
                src = Path(fname) if os.path.isabs(fname) else Path(base_dir) / fname
                if src.exists():
                    shutil.copy2(str(src), str(test_dir / src.name))
        except Exception:
            # Not critical; continue without copy
            pass

        # Copy configured x509 proxy into the test directory (if provided)
        if self.config.x509:
            try:
                x509_src = Path(self.config.x509)
                if x509_src.exists() and x509_src.is_file():
                    shutil.copy2(str(x509_src), str(test_dir / x509_src.name))
            except Exception:
                logger.warning("Configured x509 proxy not found or could not be copied")

        # Ensure referenced config files are available in test dir
        job_dir = job_config_path.parent

        for cfg_name in get_copy_file_list(job_cfg):
            cfg_base = os.path.basename(cfg_name)
            try:
                src_cfg = job_dir / cfg_base
                if src_cfg.exists() and src_cfg.is_file():
                    shutil.copy2(str(src_cfg), str(test_dir / cfg_base))
            except Exception:
                continue

        # Ensure float/int config files are available in test dir
        float_cfg = job_cfg.get('floatConfig', 'floats.txt')
        int_cfg = job_cfg.get('intConfig', 'ints.txt')
        # job_cfg entries are usually basenames placed in job directory
        for cfg_name in (float_cfg, int_cfg):
            try:
                src = job_dir / cfg_name
                if src.exists():
                    shutil.copy2(str(src), str(test_dir / src.name))
            except Exception:
                continue

        # Copy aux payload if present in job/shared inputs
        try:
            src_aux = job_dir / 'aux'
            if src_aux.exists() and src_aux.is_dir():
                shutil.copytree(str(src_aux), str(test_dir / 'aux'), dirs_exist_ok=True)
            else:
                shared_aux = self.config.work_dir / 'shared_inputs' / 'aux'
                if shared_aux.exists() and shared_aux.is_dir():
                    shutil.copytree(str(shared_aux), str(test_dir / 'aux'), dirs_exist_ok=True)
        except Exception:
            pass

        # Build test submit_config (based on job_cfg)
        test_cfg = dict(job_cfg)

        # Copy all remaining local config files for robustness
        for p in job_dir.iterdir():
            if p.is_file() and p.suffix.lower() in ('.txt', '.yaml', '.yml'):
                shutil.copy2(str(p), str(test_dir / p.name))

        test_cfg['fileList'] = first_file or job_cfg.get('fileList', '')
        test_cfg['batch'] = 'False'
        test_cfg.setdefault('threads', '1')
        test_cfg['saveFile'] = 'test_output.root'
        test_cfg['metaFile'] = 'test_output_meta.root'

        # Ensure float/int references are basenames in test dir
        test_cfg['floatConfig'] = os.path.basename(float_cfg)
        test_cfg['intConfig'] = os.path.basename(int_cfg)

        # Normalize all config references to local top-level files in test_job
        for cfg_key in ('sampleConfig', 'saveConfig', 'correctionConfig', 'floatConfig', 'intConfig'):
            if cfg_key in test_cfg and test_cfg[cfg_key]:
                test_cfg[cfg_key] = os.path.basename(test_cfg[cfg_key])

        write_config(test_cfg, str(test_dir / 'submit_config.txt'))

        logger.info(f"Created local test job at: {test_dir}")
        return test_dir
            
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
            
    def _prepare_shared_libraries(self) -> None:
        """
        Discover and stage shared libraries required by the executable.
        Stages libraries into shared_inputs/lib.
        Only libraries physically located under the RDFAnalyzerCore repository
        are staged.
        """
        # Only attempt to stage shared libraries when an executable path is configured
        if not self.config.exe_path:
            logger.debug("No exe_path configured — skipping shared-library staging")
            return

        exe_path = Path(self.config.exe_path)
        if not exe_path.exists() or not exe_path.is_file():
            logger.debug(f"Executable not found at {exe_path} — skipping shared-library staging")
            return

        try:
            from cpp_wrapper import find_shared_libraries

            repo_root = Path(__file__).resolve().parents[2].resolve()
            shared_lib_dir = Path(self.config.work_dir) / "shared_inputs" / "lib"
            shared_lib_dir.mkdir(parents=True, exist_ok=True)

            logger.info("Discovering shared libraries for executable")
            so_files = find_shared_libraries(str(exe_path))

            staged_count = 0
            for so_path in so_files:
                try:
                    src = Path(so_path).resolve()
                except OSError:
                    continue
                if not src.exists() or not src.is_file():
                    continue

                # Only stage libraries from inside this repository
                try:
                    src.relative_to(repo_root)
                except ValueError:
                    continue

                dst = shared_lib_dir / src.name
                try:
                    if not dst.exists() or dst.stat().st_mtime != src.stat().st_mtime or dst.stat().st_size != src.stat().st_size:
                        shutil.copy2(str(src), str(dst))
                    staged_count += 1
                except OSError:
                    continue

            if staged_count:
                logger.info(f"Staged {staged_count} shared libraries in {shared_lib_dir}")
            else:
                logger.info("No in-repository shared libraries needed staging")

        except ImportError:
            logger.warning("cpp_wrapper module not available, skipping .so staging")
        except Exception as e:
            logger.warning(f"Failed to stage shared libraries: {e}")

    def _prepare_shared_inputs(self) -> None:
        """
        Prepare `shared_inputs/` in the work directory with the executable and
        optional x509 proxy so `generate_condor_submit` can include them in
        `transfer_input_files`.
        """
        shared_dir = Path(self.config.work_dir) / "shared_inputs"
        try:
            shared_dir.mkdir(parents=True, exist_ok=True)

            # Copy executable into shared_inputs if available
            if self.config.exe_path and Path(self.config.exe_path).is_file():
                exe_src = Path(self.config.exe_path)
                exe_dst = shared_dir / exe_src.name
                # only copy if missing or different
                if not exe_dst.exists() or exe_dst.stat().st_mtime != exe_src.stat().st_mtime or exe_dst.stat().st_size != exe_src.stat().st_size:
                    shutil.copy2(str(exe_src), str(exe_dst))
                    exe_dst.chmod(0o755)

            # Copy x509 proxy into shared_inputs if configured
            if self.config.x509:
                x509_src = Path(self.config.x509)
                if x509_src.exists() and x509_src.is_file():
                    x509_dst = shared_dir / x509_src.name
                    if not x509_dst.exists() or x509_dst.stat().st_mtime != x509_src.stat().st_mtime:
                        shutil.copy2(str(x509_src), str(x509_dst))

            # Copy aux payload from generated jobs, if present
            shared_aux = shared_dir / "aux"
            for job_dir in sorted(Path(self.config.work_dir).glob("job_*")):
                job_aux = job_dir / "aux"
                if job_aux.exists() and job_aux.is_dir():
                    shared_aux.mkdir(parents=True, exist_ok=True)
                    shutil.copytree(str(job_aux), str(shared_aux), dirs_exist_ok=True)
        except Exception as e:
            logger.warning(f"Failed to prepare shared_inputs: {e}")
            
    def _submit_htcondor(self, job_ids: List[int], dry_run: bool) -> int:
        """Submit jobs via HTCondor"""
        from submission_backend import write_submit_files, read_config
        
        # Prepare shared libraries and shared inputs (exe, x509)
        self._prepare_shared_libraries()
        self._prepare_shared_inputs()
        
        # Require executable to be configured and present for HTCondor submission
        if not self.config.exe_path or not Path(self.config.exe_path).is_file():
            logger.error("Executable not configured or not found — cannot submit to HTCondor")
            return 0

        # Ensure each job directory contains top-level inputs expected remotely
        # (configs are local via initialdir; executable/x509 may be copied here)
        exe_name = os.path.basename(str(self.config.exe_path))
        repo_root = Path(__file__).resolve().parents[2]
        shared_dir = self.config.work_dir / 'shared_inputs'
        shared_dir.mkdir(parents=True, exist_ok=True)

        def _normalize_job_correction_config(job_dir: Path) -> None:
            job_cfg_path = job_dir / "job_config.txt"
            if not job_cfg_path.exists() or not job_cfg_path.is_file():
                return
            try:
                job_cfg = read_config(str(job_cfg_path))
            except Exception:
                return

            corr_cfg_name = os.path.basename(job_cfg.get('correctionConfig', ''))
            if not corr_cfg_name:
                return
            corr_cfg_path = job_dir / corr_cfg_name
            if not corr_cfg_path.exists() or not corr_cfg_path.is_file():
                return

            def _resolve_corr_payload(value: str) -> Optional[Path]:
                candidates = [
                    job_dir / value,
                    job_dir / os.path.basename(value),
                    job_dir / 'aux' / value,
                    self.config.work_dir / 'shared_inputs' / 'aux' / value,
                    self.config.work_dir / 'shared_inputs' / 'aux' / os.path.basename(value),
                    repo_root / value,
                ]
                if value.startswith('aux/'):
                    candidates.append(repo_root / value[len('aux/'):])
                for candidate in candidates:
                    try:
                        if candidate.exists() and candidate.is_file():
                            return candidate.resolve()
                    except OSError:
                        continue
                shared_aux_root = self.config.work_dir / 'shared_inputs' / 'aux'
                base_name = os.path.basename(value)
                if shared_aux_root.exists() and shared_aux_root.is_dir() and base_name:
                    try:
                        for candidate in shared_aux_root.rglob(base_name):
                            if candidate.exists() and candidate.is_file():
                                return candidate.resolve()
                    except OSError:
                        pass
                return None

            try:
                rewritten_lines = []
                with open(corr_cfg_path, 'r') as corr_file:
                    for raw_line in corr_file:
                        line = raw_line.rstrip('\n')
                        if not line.strip() or line.lstrip().startswith('#'):
                            rewritten_lines.append(raw_line)
                            continue

                        def _replace_file_ref(match: re.Match) -> str:
                            prefix = match.group(1)
                            value = match.group(2).strip()
                            src = _resolve_corr_payload(value)
                            if src is not None:
                                dst = job_dir / src.name
                                try:
                                    shutil.copy2(str(src), str(dst))
                                except Exception:
                                    pass
                                return f"{prefix}file={src.name}"
                            return f"{prefix}file={os.path.basename(value)}"

                        rewritten = re.sub(r'(^|\s)file=([^\s#]+)', _replace_file_ref, line)
                        rewritten_lines.append(rewritten + '\n')

                with open(corr_cfg_path, 'w') as corr_file:
                    corr_file.writelines(rewritten_lines)
            except Exception:
                return

            # Ensure every `file=` payload reference exists at job top-level.
            try:
                shared_aux_root = self.config.work_dir / 'shared_inputs' / 'aux'
                with open(corr_cfg_path, 'r') as corr_file:
                    for raw_line in corr_file:
                        line = raw_line.split('#', 1)[0].strip()
                        if not line:
                            continue
                        match = re.search(r'(^|\s)file=([^\s#]+)', line)
                        if not match:
                            continue
                        fname = os.path.basename(match.group(2).strip())
                        if not fname:
                            continue
                        dst = job_dir / fname
                        if dst.exists() and dst.is_file():
                            continue

                        src = None
                        if shared_aux_root.exists() and shared_aux_root.is_dir():
                            try:
                                for candidate in shared_aux_root.rglob(fname):
                                    if candidate.exists() and candidate.is_file():
                                        src = candidate
                                        break
                            except OSError:
                                src = None

                        if src is None:
                            repo_candidate = repo_root / fname
                            if repo_candidate.exists() and repo_candidate.is_file():
                                src = repo_candidate

                        if src is not None:
                            try:
                                shutil.copy2(str(src), str(dst))
                            except Exception:
                                pass
            except Exception:
                pass

        for job_id in job_ids:
            job_dir = self.config.work_dir / f"job_{job_id}"
            if not job_dir.exists() or not job_dir.is_dir():
                continue

            legacy_aux = job_dir / 'aux'
            if legacy_aux.exists() and legacy_aux.is_dir():
                shutil.rmtree(legacy_aux, ignore_errors=True)

            _normalize_job_correction_config(job_dir)

            # Safety net: ensure corrections.txt file payloads exist at top-level.
            corr_cfg_path = job_dir / 'corrections.txt'
            shared_aux_root = self.config.work_dir / 'shared_inputs' / 'aux'
            if corr_cfg_path.exists() and corr_cfg_path.is_file() and shared_aux_root.exists() and shared_aux_root.is_dir():
                try:
                    with open(corr_cfg_path, 'r') as corr_file:
                        for raw_line in corr_file:
                            line = raw_line.split('#', 1)[0].strip()
                            if not line:
                                continue
                            m = re.search(r'(^|\s)file=([^\s#]+)', line)
                            if not m:
                                continue
                            fname = os.path.basename(m.group(2).strip())
                            if not fname:
                                continue
                            dst = job_dir / fname
                            if dst.exists() and dst.is_file():
                                continue
                            src = None
                            for candidate in shared_aux_root.rglob(fname):
                                if candidate.exists() and candidate.is_file():
                                    src = candidate
                                    break
                            if src is not None:
                                shutil.copy2(str(src), str(dst))
                except Exception:
                    pass

            # Move/copy all non-unique job files into shared_inputs.
            # Keep only job_config.txt, floats.txt, and ints.txt as per-job files.
            keep_per_job = {'job_config.txt', 'floats.txt', 'ints.txt'}
            try:
                for job_file in sorted(job_dir.iterdir()):
                    if not job_file.exists() or not job_file.is_file():
                        continue
                    if job_file.name in keep_per_job:
                        continue

                    shared_file = shared_dir / job_file.name
                    if not shared_file.exists() or shared_file.stat().st_mtime != job_file.stat().st_mtime or shared_file.stat().st_size != job_file.stat().st_size:
                        shutil.copy2(str(job_file), str(shared_file))
                    try:
                        job_file.unlink()
                    except OSError:
                        pass
            except Exception:
                pass

        # Generate condor submission files
        submit_path = write_submit_files(
            str(self.config.work_dir.absolute()),
            len(job_ids),
            os.path.basename(str(self.config.exe_path)),
            self.config.stage_inputs,
            self.config.stage_outputs,
            self.config.root_setup,
            x509loc=str(self.config.x509) if self.config.x509 else None,
            max_runtime=self.config.max_runtime,
            config_file="job_config.txt",
            eos_sched=self.config.eos_sched,
            shared_dir_name="shared_inputs",
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
        """Submit jobs via DASK with C++ wrapper support"""
        try:
            from dask.distributed import Client, LocalCluster
            from dask_jobqueue import HTCondorCluster
        except ImportError:
            logger.error("DASK not installed. Install with: pip install dask distributed dask-jobqueue")
            return 0

        if dry_run:
            logger.info(f"Dry run: would submit {len(job_ids)} jobs via DASK")
            return len(job_ids)

        # Require a valid executable for submission
        if not self.config.exe_path or not Path(self.config.exe_path).is_file():
            logger.error("Executable not configured or not found — cannot submit via DASK")
            return 0

        try:
            # Prepare shared libraries
            self._prepare_shared_libraries()

            # Create DASK cluster with file transfers
            transfer_files: List[str] = []

            # Ensure shared inputs (exe, x509, lib) are prepared in work_dir/shared_inputs
            self._prepare_shared_inputs()
            lib_dir = self.config.work_dir / "shared_inputs" / "lib"

            # Transfer the executable only if it exists as a file
            if self.config.exe_path and Path(self.config.exe_path).is_file():
                transfer_files.append(str(self.config.exe_path))

            # Transfer specific files inside lib/ (do not transfer the directory)
            if lib_dir.exists() and lib_dir.is_dir():
                for p in sorted(lib_dir.iterdir()):
                    if p.is_file():
                        transfer_files.append(str(p))

            # Transfer x509 proxy if provided (prefer copy in shared_inputs)
            if self.config.x509:
                x509_basename = Path(self.config.x509).name
                shared_x509 = Path(self.config.work_dir) / 'shared_inputs' / x509_basename
                if shared_x509.exists() and shared_x509.is_file():
                    transfer_files.append(str(shared_x509))
                elif Path(self.config.x509).exists() and Path(self.config.x509).is_file():
                    transfer_files.append(str(self.config.x509))
            
            cluster = HTCondorCluster(
                cores=1,
                memory="2GB",
                disk="2GB",
                job_extra={
                    "+MaxRuntime": "3600",
                },
                # Transfer executable and libraries
                transfer_input_files=transfer_files if transfer_files else None,
            )
            cluster.scale(jobs=len(job_ids))
            
            client = Client(cluster)
            
            # Submit jobs using Python wrapper for C++ executables
            def run_cpp_job(job_id: int, exe_path: str, config_path: str) -> dict:
                """Run a C++ job with proper wrapper and staging
                
                Note: Input/output staging is handled by DASK cluster configuration
                and the HTCondor submission, not within this function.
                """
                import sys
                import os
                from pathlib import Path
                
                # Add cpp_wrapper to path
                wrapper_dir = Path(__file__).parent
                if str(wrapper_dir) not in sys.path:
                    sys.path.insert(0, str(wrapper_dir))
                
                try:
                    from cpp_wrapper import run_cpp_executable
                    
                    # Setup working directory
                    work_dir = os.getcwd()
                    
                    # Run with wrapper
                    result = run_cpp_executable(
                        exe_path=exe_path,
                        config_path=config_path,
                        working_dir=work_dir,
                        setup_libs=True
                    )
                    
                    return {
                        'job_id': job_id,
                        'returncode': result['returncode'],
                        'stdout': result['stdout'],
                        'stderr': result['stderr'],
                        'success': result['success']
                    }
                    
                except ImportError:
                    # Fallback to direct execution
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
                        'success': result.returncode == 0
                    }
                
            # Submit all jobs
            futures = []
            for job_id in job_ids:
                job = self.jobs[job_id]
                future = client.submit(
                    run_cpp_job,
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
        choices=["create", "submit", "status", "validate", "resubmit", "test", "create-test"],
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
        "-x", "--x509",
        type=str,
        help="Path to x509 proxy"
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
        exe_path=Path(args.exe) if args.exe else None,
        stage_inputs=args.stage_inputs,
        stage_outputs=args.stage_outputs,
        x509=args.x509,
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

    elif args.command == "create-test":
        if args.job_id is None:
            logger.error("--job-id required for create-test command")
            sys.exit(1)
        try:
            test_dir = manager.create_test_job(args.job_id)
            print(f"Test job created at: {test_dir}")
        except Exception as e:
            logger.error(f"Failed to create test job: {e}")
            sys.exit(1)

    else:
        logger.error(f"Unknown command: {args.command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
