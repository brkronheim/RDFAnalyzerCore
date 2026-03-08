#!/usr/bin/env python3
"""
Enhanced submission script with ProductionManager integration.

This script bridges the existing submission infrastructure with the new
ProductionManager for unified job lifecycle management.
"""

import argparse
import os
import sys
from pathlib import Path

from production_manager import ProductionManager, ProductionConfig, Job, JobStatus
from submission_backend import read_config
from generateSubmissionFilesNANO import (
    getSampleList, queryRucio, get_rucio_client, get_proxy_path
)
from validate_config import validate_submit_config


def _resolve_cli_path(path_value: str, repo_root: Path) -> Path:
    path_obj = Path(path_value)
    if path_obj.is_absolute():
        return path_obj
    cwd_candidate = (Path.cwd() / path_obj)
    if cwd_candidate.exists():
        return cwd_candidate.resolve()
    repo_candidate = (repo_root / path_obj)
    if repo_candidate.exists():
        return repo_candidate.resolve()
    return cwd_candidate.resolve()


def generate_production_from_nano(
    manager: ProductionManager,
    sample_config: str,
    file_split_gb: int = 30,
    threads: int = 4,
) -> int:
    """
    Generate jobs for a production using NANO/Rucio data discovery.

    This implementation parallelizes work at two levels, mirroring
    generateSubmissionFilesNANO.py:
      - samples are processed in parallel (up to `threads` workers)
      - DAS/Rucio entries for a single sample are queried in parallel

    Args:
        manager: ProductionManager instance
        sample_config: Path to sample configuration file
        file_split_gb: GB of data per job
        threads: Number of parallel threads for Rucio queries

    Returns:
        Number of jobs created
    """
    # Read sample list and topology
    sample_list, base_dirs, lumi, WL, BL = getSampleList(sample_config)

    # Thread-safe accumulators
    file_lists: list = []
    job_configs: list = []

    def _process_sample(sample_name, sample_data):
        """Process a single sample (runs in a worker thread)."""
        try:
            client_local = get_rucio_client()
        except Exception as e:
            print(f"Warning: cannot create Rucio client for sample {sample_name}: {e}")
            return []

        das = sample_data['das']
        xsec = float(sample_data.get('xsec', 0.0))
        typ = sample_data['type']
        norm = float(sample_data.get('norm', 1.0))
        kfac = float(sample_data.get('kfac', 1.0))
        site = sample_data.get('site', '')
        extra_scale = float(sample_data.get('extraScale', 1.0))

        das_entries = [d.strip() for d in das.split(',') if d and d.strip()]
        if not das_entries:
            return []

        # If there are multiple DAS entries, query them in parallel
        if len(das_entries) == 1:
            groups_result = queryRucio(das_entries[0], file_split_gb, WL, BL, site, client_local)
        else:
            workers = max(1, min(len(das_entries), threads))
            from concurrent.futures import ThreadPoolExecutor, as_completed
            all_files = []
            with ThreadPoolExecutor(max_workers=workers) as das_executor:
                das_futures = {das_executor.submit(queryRucio, e, file_split_gb, WL, BL, site, client_local): e for e in das_entries}
                for fut in as_completed(das_futures):
                    try:
                        res = fut.result()
                    except Exception:
                        continue
                    if not res:
                        continue
                    for g in sorted(res.keys()):
                        grp = res[g]
                        parts = [p.strip() for p in grp.split(',') if p.strip()]
                        all_files.extend(parts)

            # deduplicate while preserving order
            seen = set()
            combined = []
            for f in all_files:
                if f in seen:
                    continue
                seen.add(f)
                combined.append(f)
            groups_result = {0: ",".join(combined)} if combined else {}

        # Convert groups_result into (file_list, job_config) entries
        entries = []
        for group_id, file_list in groups_result.items():
            norm_scale = extra_scale * kfac * lumi * xsec / norm
            job_config = {
                'type': typ,
                'floatConfig': f'normScale={norm_scale}\nsampleNorm={norm}',
                'intConfig': f'type={typ}',
            }
            entries.append((file_list, job_config))

        return entries

    # Process samples in parallel
    from concurrent.futures import ThreadPoolExecutor, as_completed
    max_workers = max(1, threads)
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for sample_name, sample_data in sample_list.items():
            futures.append(executor.submit(_process_sample, sample_name, sample_data))

        for fut in as_completed(futures):
            res = fut.result()
            if not res:
                continue
            for file_list, job_cfg in res:
                file_lists.append(file_list)
                job_configs.append(job_cfg)

    # Generate jobs in manager
    return manager.generate_jobs(file_lists, job_configs)


def main():
    repo_root = Path(__file__).resolve().parents[2]

    parser = argparse.ArgumentParser(
        description="Submit RDFAnalyzerCore production with ProductionManager"
    )
    
    # Production settings
    parser.add_argument(
        '--name', '-n',
        required=True,
        help='Production name'
    )
    parser.add_argument(
        '--config', '-c',
        required=True,
        help='Base analysis configuration file'
    )
    parser.add_argument(
        '--sample-config',
        required=True,
        help='Sample configuration file (for NANO mode)'
    )
    parser.add_argument(
        '--exe', '-e',
        required=True,
        help='Path to analysis executable'
    )
    parser.add_argument(
        '-x', '--x509',
        type=str,
        default="",
        help='Path to x509 proxy (optional)'
    )
    parser.add_argument(
        '--output-dir', '-o',
        help='Output directory (default: based on config saveDirectory)'
    )
    parser.add_argument(
        '--work-dir', '-w',
        help='Working directory for production (default: condorSub_<name>)'
    )
    
    # Job splitting settings
    parser.add_argument(
        '--size', '-s',
        type=int,
        default=30,
        help='GB of data per job (default: 30)'
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='Number of threads for Rucio queries (default: 4)'
    )
    parser.add_argument(
        '--root-setup',
        type=str,
        default="",
        help="command to setup ROOT (e.g., 'source /path/to/thisroot.sh')"
    )
    parser.add_argument(
        '--max-runtime',
        type=int,
        default=3600,
        help='Max runtime (seconds) for Condor jobs (default: 3600)'
    )
    parser.add_argument(
        '--make-test-job',
        action='store_true',
        help='create a local test job for the first generated job (writes test_job/submit_config.txt)'
    )
    
    # Submission settings
    parser.add_argument(
        '--backend',
        choices=['htcondor', 'dask'],
        default='htcondor',
        help='Batch submission backend (default: htcondor)'
    )
    parser.add_argument(
        '--stage-inputs',
        action='store_true',
        help='Stage input files to worker nodes'
    )
    parser.add_argument(
        '--stage-outputs',
        action='store_true',
        help='Stage output files from worker nodes'
    )
    parser.add_argument(
        '--max-retries',
        type=int,
        default=3,
        help='Maximum number of retry attempts per job (default: 3)'
    )
    parser.add_argument(
        '--eos-sched',
        action='store_true',
        help='Use EOS scheduling'
    )
    
    # Action settings
    parser.add_argument(
        '--submit',
        action='store_true',
        help='Submit jobs after generation'
    )
    parser.add_argument(
        '--no-validate',
        action='store_true',
        help='Skip configuration validation'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Generate but do not submit jobs'
    )
    
    args = parser.parse_args()
    
    args.config = str(_resolve_cli_path(args.config, repo_root))
    args.sample_config = str(_resolve_cli_path(args.sample_config, repo_root))
    args.exe = str(_resolve_cli_path(args.exe, repo_root))
    if args.x509:
        args.x509 = str(_resolve_cli_path(args.x509, repo_root))

    # Validate configuration
    if not args.no_validate:
        print("Validating configuration...")
        errors, warnings = validate_submit_config(args.config, mode="nano")
        
        if warnings:
            print("Configuration warnings:")
            for w in warnings:
                print(f"  - {w}")
                
        if errors:
            print("Configuration validation failed:")
            for e in errors:
                print(f"  - {e}")
            sys.exit(1)
            
        print("Configuration validated successfully")
    
    # Read base config to get output directory
    base_config = read_config(args.config)
    if args.output_dir:
        output_dir = Path(_resolve_cli_path(args.output_dir, repo_root))
    else:
        output_dir = Path(base_config.get('saveDirectory', f'outputs_{args.name}'))
    
    # Set work directory
    if args.work_dir:
        work_dir = Path(_resolve_cli_path(args.work_dir, repo_root))
    else:
        work_dir = Path(f'condorSub_{args.name}')
    
    # Check if production already exists
    state_file = work_dir / "production_state.json"
    if state_file.exists():
        print(f"Warning: Production {args.name} already exists at {work_dir}")
        response = input("Continue and load existing state? [y/N]: ")
        if response.lower() not in ('y', 'yes'):
            print("Aborted")
            sys.exit(1)
    
    # Create production config
    prod_config = ProductionConfig(
        name=args.name,
        work_dir=work_dir,
        exe_path=Path(args.exe),
        base_config=args.config,
        output_dir=output_dir,
        backend=args.backend,
        stage_inputs=args.stage_inputs,
        stage_outputs=args.stage_outputs,
        max_retries=args.max_retries,
        eos_sched=args.eos_sched,
        root_setup=args.root_setup,
        x509=args.x509 or None,
        max_runtime=args.max_runtime,
    )
    
    # Create manager
    manager = ProductionManager(prod_config)
    
    # Generate jobs if this is a new production
    if not manager.jobs:
        print("Generating jobs from NANO/Rucio...")
        try:
            # prefer explicit proxy passed on CLI; otherwise use current proxy
            if args.x509:
                proxy = str(Path(args.x509))
                if not Path(proxy).exists():
                    raise Exception(f"x509 proxy not found: {proxy}")
            else:
                proxy = get_proxy_path()

            # ensure manager knows which proxy to stage
            manager.config.x509 = str(proxy)
            print(f"Using proxy: {proxy}")
        except Exception as e:
            print(f"Error: {e}")
            sys.exit(1)
            
        num_jobs = generate_production_from_nano(
            manager,
            args.sample_config,
            file_split_gb=args.size,
            threads=args.threads,
        )
        
        print(f"Generated {num_jobs} jobs")
    else:
        print(f"Loaded existing production with {len(manager.jobs)} jobs")
    
    # Print current status
    manager.print_progress()

    # Optionally create a local test job for quick verification
    if args.make_test_job:
        if manager.jobs:
            try:
                test_dir = manager.create_test_job(0)
                print(f"\nTest job created at: {test_dir}")
                print(f"Run locally: cd {test_dir} && ./{os.path.basename(prod_config.exe_path)} submit_config.txt")
            except Exception as e:
                print(f"Failed to create test job: {e}")
        else:
            print("No jobs available to create a test job")

    # Submit if requested
    if args.submit and not args.dry_run:
        print("\nSubmitting jobs...")
        count = manager.submit_jobs()
        print(f"Submitted {count} jobs")
        
        # Print updated status
        manager.update_status()
        manager.print_progress()
    elif args.dry_run:
        print("\nDry run - jobs not submitted")
        print("To submit, run:")
        print(f"  python production_manager.py submit --name {args.name} --work-dir {work_dir}")
    else:
        print("\nJobs generated but not submitted")
        print("To submit, run:")
        print(f"  python production_manager.py submit --name {args.name} --work-dir {work_dir}")
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
