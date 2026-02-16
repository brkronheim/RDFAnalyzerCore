#!/usr/bin/env python3
"""
Example usage of the Production Manager.

This example shows how to programmatically create and manage a production
using the ProductionManager API.
"""

import sys
from pathlib import Path

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "core" / "python"))

from production_manager import (
    ProductionManager,
    ProductionConfig,
    JobStatus
)


def example_basic_usage():
    """Example: Basic production creation and management"""
    print("=== Example: Basic Production Usage ===\n")
    
    # Step 1: Create production configuration
    print("Step 1: Creating production configuration...")
    config = ProductionConfig(
        name="example_production",
        work_dir=Path("example_production_work"),
        exe_path=Path("./my_analyzer"),  # Path to your analysis executable
        base_config="example_config.txt",  # Base configuration file
        output_dir=Path("example_outputs"),
        backend="htcondor",
        stage_inputs=False,
        stage_outputs=False,
        max_retries=3,
    )
    print(f"  Production name: {config.name}")
    print(f"  Work directory: {config.work_dir}")
    print(f"  Backend: {config.backend}\n")
    
    # Step 2: Create production manager
    print("Step 2: Creating production manager...")
    manager = ProductionManager(config)
    print(f"  Manager created with {len(manager.jobs)} existing jobs\n")
    
    # Step 3: Generate jobs
    print("Step 3: Generating jobs...")
    
    # In a real production, these file lists would come from Rucio or data discovery
    file_lists = [
        "root://xrootd.site/store/data/file1.root,root://xrootd.site/store/data/file2.root",
        "root://xrootd.site/store/data/file3.root,root://xrootd.site/store/data/file4.root",
        "root://xrootd.site/store/data/file5.root",
    ]
    
    # Optional: Add per-job configuration overrides
    job_configs = [
        {"type": "0"},  # Data
        {"type": "1"},  # MC sample 1
        {"type": "1"},  # MC sample 2
    ]
    
    num_jobs = manager.generate_jobs(file_lists, job_configs)
    print(f"  Generated {num_jobs} jobs\n")
    
    # Step 4: Submit jobs (dry run)
    print("Step 4: Submitting jobs (dry run)...")
    count = manager.submit_jobs(dry_run=True)
    print(f"  Would submit {count} jobs\n")
    
    # Step 5: Check progress
    print("Step 5: Checking progress...")
    manager.print_progress()
    
    # Step 6: Save state
    print("Step 6: State saved to:", config.state_file)
    print("\nProduction created successfully!")
    print("To submit for real, set dry_run=False in submit_jobs()")


def example_monitoring():
    """Example: Monitoring an existing production"""
    print("\n=== Example: Monitoring Production ===\n")
    
    # Load existing production
    config = ProductionConfig(
        name="example_production",
        work_dir=Path("example_production_work"),
        exe_path=Path("./my_analyzer"),
        base_config="example_config.txt",
        output_dir=Path("example_outputs"),
    )
    
    manager = ProductionManager(config)
    print(f"Loaded production: {config.name}")
    print(f"Total jobs: {len(manager.jobs)}\n")
    
    # Update status from batch system
    print("Updating job statuses...")
    status_counts = manager.update_status()
    
    # Print progress
    manager.print_progress()
    
    # Get detailed progress
    progress = manager.get_progress()
    print(f"\nDetailed progress:")
    print(f"  Completion rate: {100*progress['completion_rate']:.1f}%")
    print(f"  Validation rate: {100*progress['validation_rate']:.1f}%")
    print(f"  Failure rate: {100*progress['failure_rate']:.1f}%")


def example_validation_and_recovery():
    """Example: Validating outputs and recovering from failures"""
    print("\n=== Example: Validation and Recovery ===\n")
    
    # Load production
    config = ProductionConfig(
        name="example_production",
        work_dir=Path("example_production_work"),
        exe_path=Path("./my_analyzer"),
        base_config="example_config.txt",
        output_dir=Path("example_outputs"),
    )
    
    manager = ProductionManager(config)
    
    # Validate outputs
    print("Validating job outputs...")
    results = manager.validate_outputs()
    valid_count = sum(1 for v in results.values() if v)
    print(f"  {valid_count}/{len(results)} jobs have valid outputs\n")
    
    # Check for failed jobs
    failed_jobs = [
        job_id for job_id, job in manager.jobs.items()
        if job.status in (JobStatus.FAILED, JobStatus.MISSING_OUTPUT)
    ]
    
    if failed_jobs:
        print(f"Found {len(failed_jobs)} failed jobs")
        print(f"Failed job IDs: {failed_jobs}\n")
        
        # Resubmit failed jobs
        print("Resubmitting failed jobs...")
        count = manager.resubmit_failed()
        print(f"  Resubmitted {count} jobs\n")
    else:
        print("No failed jobs to resubmit\n")
    
    # Print final status
    manager.print_progress()


def example_advanced_api():
    """Example: Advanced API usage"""
    print("\n=== Example: Advanced API Usage ===\n")
    
    config = ProductionConfig(
        name="advanced_production",
        work_dir=Path("advanced_production_work"),
        exe_path=Path("./my_analyzer"),
        base_config="config.txt",
        output_dir=Path("advanced_outputs"),
        backend="dask",  # Use DASK backend
        stage_inputs=True,
        stage_outputs=True,
    )
    
    manager = ProductionManager(config)
    
    # Generate jobs with custom configurations
    print("Generating jobs with custom configurations...")
    file_lists = ["file1.root"] * 5
    job_configs = [
        {"systematic": "nominal"},
        {"systematic": "JES_up"},
        {"systematic": "JES_down"},
        {"systematic": "JER_up"},
        {"systematic": "JER_down"},
    ]
    
    manager.generate_jobs(file_lists, job_configs)
    print(f"  Generated {len(manager.jobs)} jobs for systematic variations\n")
    
    # Test a single job locally before submission
    print("Testing job 0 locally...")
    success = manager.test_job(0, local=True)
    print(f"  Test {'succeeded' if success else 'failed'}\n")
    
    # Submit only specific jobs
    print("Submitting specific jobs...")
    job_ids = [0, 1, 2]  # Submit only first 3 jobs
    count = manager.submit_jobs(job_ids=job_ids, dry_run=True)
    print(f"  Would submit {count} jobs\n")
    
    # Access individual job information
    print("Job details:")
    for job_id in [0, 1]:
        job = manager.jobs[job_id]
        print(f"  Job {job_id}:")
        print(f"    Status: {job.status.value}")
        print(f"    Config: {job.config_path}")
        print(f"    Output: {job.output_path}")
        print(f"    Attempts: {job.attempts}")


def main():
    """Run all examples"""
    print("Production Manager Examples")
    print("=" * 60)
    print()
    
    try:
        # Run examples
        example_basic_usage()
        
        # Uncomment to run other examples if you have an existing production
        # example_monitoring()
        # example_validation_and_recovery()
        # example_advanced_api()
        
        print("\n" + "=" * 60)
        print("Examples completed successfully!")
        print("\nNext steps:")
        print("1. Modify the configuration for your analysis")
        print("2. Use production_submit.py for automatic data discovery")
        print("3. Use production_monitor.py for real-time monitoring")
        print("4. See docs/PRODUCTION_MANAGER.md for complete documentation")
        
    except Exception as e:
        print(f"\nError running examples: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
