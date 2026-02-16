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


def generate_production_from_nano(
    manager: ProductionManager,
    sample_config: str,
    file_split_gb: int = 30,
    threads: int = 4,
) -> int:
    """
    Generate jobs for a production using NANO/Rucio data discovery.
    
    Args:
        manager: ProductionManager instance
        sample_config: Path to sample configuration file
        file_split_gb: GB of data per job
        threads: Number of parallel threads for Rucio queries
        
    Returns:
        Number of jobs created
    """
    # Get samples from config
    sample_list, base_dirs, lumi, WL, BL = getSampleList(sample_config)
    
    # Get Rucio client
    client = get_rucio_client()
    
    # Process each sample
    file_lists = []
    job_configs = []
    
    for sample_name, sample_data in sample_list.items():
        print(f"Processing sample: {sample_name}")
        
        das = sample_data['das']
        xsec = float(sample_data['xsec'])
        typ = sample_data['type']
        norm = float(sample_data.get('norm', 1.0))
        kfac = float(sample_data.get('kfac', 1.0))
        site = sample_data.get('site', '')
        extra_scale = float(sample_data.get('extraScale', 1.0))
        
        # Query Rucio for files
        das_entries = [d.strip() for d in das.split(',') if d and d.strip()]
        
        for das_entry in das_entries:
            groups = queryRucio(das_entry, file_split_gb, WL, BL, site, client)
            
            # Create a job for each group
            for group_id, file_list in groups.items():
                norm_scale = extra_scale * kfac * lumi * xsec / norm
                
                job_config = {
                    'type': typ,
                    'floatConfig': f'normScale={norm_scale}\nsampleNorm={norm}',
                    'intConfig': f'type={typ}',
                }
                
                file_lists.append(file_list)
                job_configs.append(job_config)
    
    # Generate jobs in manager
    return manager.generate_jobs(file_lists, job_configs)


def main():
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
        output_dir = Path(args.output_dir)
    else:
        output_dir = Path(base_config.get('saveDirectory', f'outputs_{args.name}'))
    
    # Set work directory
    if args.work_dir:
        work_dir = Path(args.work_dir)
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
    )
    
    # Create manager
    manager = ProductionManager(prod_config)
    
    # Generate jobs if this is a new production
    if not manager.jobs:
        print("Generating jobs from NANO/Rucio...")
        try:
            proxy = get_proxy_path()
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
