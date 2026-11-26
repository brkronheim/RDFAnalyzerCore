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


def _copy_native_asset(key, source, directory):
    from native_config_io import legacy_aux_kind, read_legacy_aux, write_config
    kind = legacy_aux_kind(key, source)
    target = directory / source.name
    if kind:
        target = target.with_suffix(".yaml")
        write_config(read_legacy_aux(source, kind), target)
    else:
        shutil.copy2(source, target)
    return target


def _stage_yaml_corrections(path, job_dir, resolve):
    from native_config_io import read_config_records, write_config_yaml, runtime_yaml_path
    records = read_config_records(path, strict_legacy=True)
    if not isinstance(records, list):
        raise ValueError(f"{path}: correctionConfig must contain a list of mappings")
    for number, record in enumerate(records, 1):
        if not isinstance(record, dict):
            raise ValueError(f"{path}: correction entry {number} must be a mapping")
        value = record.get("file")
        if not value:
            continue
        source = resolve(value) or resolve(value.removeprefix("aux/"))
        if not source or not source.is_file():
            raise ValueError(f"{path}: missing correction payload {value!r} in entry {number}")
        target = job_dir / source.name
        if target.resolve() != source.resolve():
            shutil.copy2(source, target)
        record["file"] = target.name
    target = runtime_yaml_path(path)
    write_config_yaml(records, target)
    return target.name


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

            from native_config_io import read_config_records
            for entry in read_config_records(corr_cfg_src):
                value = entry.get("file")
                if not value:
                    continue
                src = _resolve_path(value) or _resolve_path(value.removeprefix("aux/"))
                if src and src.is_file():
                    shutil.copy2(src, job_dir / src.name)
                    shutil.copy2(src, shared_aux_dir / src.name)

        def _rewrite_correction_config_to_top_level(job_dir: Path, cfg_name: str) -> None:
            if not cfg_name:
                return
            corr_cfg_path = job_dir / os.path.basename(cfg_name)
            if not corr_cfg_path.exists() or not corr_cfg_path.is_file():
                return

            return _stage_yaml_corrections(corr_cfg_path, job_dir, _resolve_path)

        for i, (file_list, extra_config) in enumerate(zip(file_lists, job_configs)):
            job_id = len(self.jobs)
            
            # Create job directory
            job_dir = self.config.work_dir / f"job_{job_id}"
            job_dir.mkdir(parents=True, exist_ok=True)
            
            # Generate job config
            config_name = "job_config.yaml"
            config_path = job_dir / config_name
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

            # Handle special inline float/int config content.
            # If `extra_config` provides `floatConfig`/`intConfig` as inline content
            # (contains '\n' or an '=' sign and is not a filename), write that content
            # into a file in the job directory and set the job config to reference the
            # filename.  This allows callers to pass normalisation lines (e.g.
            # ``normScale=3.14``) directly rather than via a pre-written file on disk.
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
                    copied_cfg = _copy_native_asset(cfg_key, src_cfg, job_dir)
                    base_config[cfg_key] = copied_cfg.name

            from native_config_io import parse_config_text
            for key, stem in (("floatConfig", "floats"), ("intConfig", "ints")):
                values = {}
                base_value = base_config.get(key)
                if base_value:
                    source = _resolve_path(base_value)
                    if not source or not source.is_file():
                        raise ValueError(f"{base_config_path}: {key} references missing file {base_value!r}")
                    values.update(read_config(source))
                extra = extra_config.get(key)
                if isinstance(extra, dict):
                    values.update({str(k): str(v) for k, v in extra.items()})
                elif isinstance(extra, str) and _is_inline_content(extra):
                    values.update(parse_config_text(extra.splitlines(), f"job {job_id} {key}"))
                elif extra:
                    source = _resolve_path(str(extra))
                    if not source or not source.is_file():
                        raise ValueError(f"job {job_id}: {key} references missing file {extra!r}")
                    values = read_config(source)
                target = job_dir / (stem + config_path.suffix)
                write_config(values, target)
                base_config[key] = target.name

            # Merge any remaining keys from extra_config (e.g. 'type')
            for k, v in extra_config.items():
                if k in ('floatConfig', 'intConfig'):
                    continue
                base_config[k] = str(v)

            _copy_aux_payload(job_dir)
            correction_name = _rewrite_correction_config_to_top_level(job_dir, base_config.get("correctionConfig", ""))
            if correction_name:
                base_config["correctionConfig"] = correction_name

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
        # Baseline assets can live outside the per-job directory. Per-job
        # versions below take precedence when they carry dataset overrides.
        base_cfg = read_config(self.config.base_config)
        base_inputs = set(get_copy_file_list(base_cfg))
        for key, fname in base_cfg.items():
            if fname in base_inputs:
                source = Path(self.config.base_config).resolve().parent / fname
                if source.is_file():
                    _copy_native_asset(key, source, test_dir)

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

        test_cfg = dict(job_cfg)
        inputs = set(get_copy_file_list(job_cfg))
        copied = {job_config_path.name}
        for key, name in job_cfg.items():
            if name not in inputs:
                continue
            source = job_dir / Path(name).name
            if source.is_file():
                target = _copy_native_asset(key, source, test_dir)
                test_cfg[key] = target.name
                copied.add(source.name)

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
        # Copy all remaining local config files for robustness
        for p in job_dir.iterdir():
            if p.is_file() and p.name not in copied and p.suffix.lower() in ('.txt', '.yaml', '.yml'):
                shutil.copy2(str(p), str(test_dir / p.name))

        test_cfg['fileList'] = first_file or job_cfg.get('fileList', '')
        test_cfg['batch'] = 'False'
        test_cfg.setdefault('threads', '1')
        test_cfg['saveFile'] = 'test_output.root'
        test_cfg['metaFile'] = 'test_output_meta.root'

        # Ensure float/int references are basenames in test dir

        # Normalize all config references to local top-level files in test_job
        for cfg_key in ('sampleConfig', 'saveConfig', 'correctionConfig', 'floatConfig', 'intConfig'):
            if cfg_key in test_cfg and test_cfg[cfg_key]:
                test_cfg[cfg_key] = os.path.basename(test_cfg[cfg_key])

        write_config(test_cfg, str(test_dir / 'submit_config.yaml'))

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

        config_names = {Path(self.jobs[job_id].config_path).name for job_id in job_ids}
        archive_mode = config_names != {"job_config.yaml"} or any(
            Path(read_config(self.jobs[job_id].config_path).get(key, "")).suffix == ".txt"
            for job_id in job_ids for key in ("floatConfig", "intConfig", "correctionConfig")
        )
        config_name = "job_config.runtime.yaml" if archive_mode else "job_config.yaml"

        def _normalize_job_config(job_dir: Path, source: Path) -> None:
            from native_config_io import read_config, write_config
            job_cfg = read_config(source)
            for key, stem in (("floatConfig", "floats"), ("intConfig", "ints")):
                old = job_dir / job_cfg.get(key, stem + ".txt")
                if archive_mode:
                    if key in job_cfg and not old.is_file():
                        raise ValueError(f"{source}: missing {key} input {old}")
                    name = stem + ".runtime.yaml"
                    write_config(read_config(old) if old.is_file() else {}, job_dir / name)
                    job_cfg[key] = name

            corr_cfg_name = os.path.basename(job_cfg.get("correctionConfig", ""))
            corr_cfg_path = job_dir / corr_cfg_name

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

            if corr_cfg_name and corr_cfg_path.is_file():
                job_cfg["correctionConfig"] = _stage_yaml_corrections(corr_cfg_path, job_dir, _resolve_corr_payload)
            if archive_mode or job_cfg != read_config(source):
                write_config(job_cfg, job_dir / config_name)

        for job_id in job_ids:
            job_dir = self.config.work_dir / f"job_{job_id}"
            if not job_dir.exists() or not job_dir.is_dir():
                continue

            legacy_aux = job_dir / 'aux'
            if not archive_mode and legacy_aux.exists() and legacy_aux.is_dir():
                shutil.rmtree(legacy_aux, ignore_errors=True)

            original = Path(self.jobs[job_id].config_path)
            _normalize_job_config(job_dir, original)
            keep_per_job = {config_name, original.name, 'floats.runtime.yaml', 'ints.runtime.yaml', 'floats.yaml', 'ints.yaml', 'floats.txt', 'ints.txt'}
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
                        if not archive_mode:
                            job_file.unlink()
                    except OSError:
                        pass
            except Exception:
                pass

        # Generate condor submission files
        launch_dir = None
        if (self.config.work_dir / "condor_submit.sub").is_file():
            import tempfile
            retries = self.config.work_dir / "resubmissions"
            retries.mkdir(exist_ok=True)
            launch_dir = tempfile.mkdtemp(prefix="submit-", dir=retries)
        submit_path = write_submit_files(
            str(self.config.work_dir.absolute()),
            len(job_ids),
            os.path.basename(str(self.config.exe_path)),
            self.config.stage_inputs,
            self.config.stage_outputs,
            self.config.root_setup,
            x509loc=str(self.config.x509) if self.config.x509 else None,
            max_runtime=self.config.max_runtime,
            config_file=config_name,
            eos_sched=self.config.eos_sched,
            shared_dir_name="shared_inputs",
            job_indices=job_ids if job_ids != list(range(len(job_ids))) else None,
            launch_dir=launch_dir,
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
            for proc_id, job_id in enumerate(job_ids):
                job = self.jobs[job_id]
                job.status = JobStatus.SUBMITTED
                job.submit_time = submit_time
                job.attempts += 1
                if cluster_id:
                    job.condor_job_id = f"{cluster_id}.{proc_id}"
                    
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
        logger.error("Use the Snakemake campaign (examples/snakemake/Snakefile) to create a production.")
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
