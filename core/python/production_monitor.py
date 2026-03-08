#!/usr/bin/env python3
"""
Production monitoring and management CLI.

Provides a continuous monitoring interface and management commands for
productions managed by ProductionManager.
"""

import argparse
import curses
import sys
import time
from pathlib import Path
from typing import Optional

from production_manager import ProductionManager, ProductionConfig, JobStatus


def format_duration(seconds: float) -> str:
    """Format duration in seconds to human-readable string"""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    elif seconds < 86400:
        return f"{seconds/3600:.1f}h"
    else:
        return f"{seconds/86400:.1f}d"


def _condor_paths_for_job(job, work_dir: Path):
    """Return condor stdout/stderr/log paths for a job based on its condor_job_id.

    Expected layout (created by submission_backend):
      <work_dir>/condor_logs/log_<Cluster>_<Process>.stdout
      <work_dir>/condor_logs/log_<Cluster>_<Process>.stderr
      <work_dir>/condor_logs/log_<Cluster>.log
    """
    if not getattr(job, "condor_job_id", None):
        return None
    try:
        cluster, proc = str(job.condor_job_id).split('.')
    except Exception:
        return None

    base = Path(work_dir) / "condor_logs"
    return {
        'stdout': base / f"log_{cluster}_{proc}.stdout",
        'stderr': base / f"log_{cluster}_{proc}.stderr",
        'log': base / f"log_{cluster}.log",
    }


def _parse_condor_log_for_abort_reason(log_path: Path) -> Optional[str]:
    """Scan a condor cluster log for lines indicating abort/hold/remove/exit reasons.

    Returns a short, human-readable reason string or None if nothing obvious found.
    """
    if not log_path or not log_path.exists():
        return None

    import re

    try:
        with open(log_path, 'r', errors='ignore') as f:
            lines = f.readlines()[-500:]

        # Look from the end for the most relevant messages
        patterns = [
            r'Hold reason[:=]\s*(.*)',
            r'Job was aborted',
            r'Aborted by signal',
            r'Exited with status\s*(\d+)',
            r'ExitCode\s*=\s*(\d+)',
            r'Removed',
            r'Job was held',
            r'Job terminated',
            r'Abnormal termination',
        ]

        for ln in reversed(lines):
            for p in patterns:
                m = re.search(p, ln, re.IGNORECASE)
                if m:
                    # Prefer captured group if present
                    if m.groups():
                        return m.group(1).strip()
                    return ln.strip()

        return None
    except Exception:
        return None


def _parse_stderr_for_failure(stderr_path: Path, max_lines: int = 40) -> Optional[str]:
    """Return a short excerpt from the stderr file that likely indicates the failure."""
    if not stderr_path or not stderr_path.exists():
        return None

    try:
        with open(stderr_path, 'r', errors='ignore') as f:
            lines = [l.rstrip('\n') for l in f.readlines()]

        # Prefer last non-empty lines (where errors usually appear)
        non_empty = [l for l in lines if l.strip()]
        if not non_empty:
            return None

        excerpt_lines = non_empty[-max_lines:]
        excerpt = '\n'.join(excerpt_lines)
        # Return first meaningful sentence or the last line if short
        last_line = excerpt_lines[-1]
        if len(last_line) > 300:
            return last_line[:300] + '...'
        if '\n' in excerpt and len(excerpt) > 500:
            return excerpt[-500:]
        return excerpt
    except Exception:
        return None


def _inspect_and_record_failure(manager: ProductionManager, job) -> Optional[str]:
    """Inspect condor log and stderr for a single job and record job.last_error.

    Returns the discovered reason (or None). If a reason is found it is stored in
    the Job.last_error field and the manager state is saved.
    """
    # If already recorded, return it
    if getattr(job, 'last_error', None):
        return job.last_error

    paths = _condor_paths_for_job(job, manager.config.work_dir)
    if not paths:
        return None

    reasons = []
    # Check condor cluster log first (may contain hold/remove/abort messages)
    log_reason = _parse_condor_log_for_abort_reason(paths.get('log'))
    if log_reason:
        reasons.append(f"condor-log: {log_reason}")

    # Check stderr for application-level errors
    stderr_excerpt = _parse_stderr_for_failure(paths.get('stderr'))
    if stderr_excerpt:
        # Shorten to a one-line summary for quick display
        first_line = stderr_excerpt.splitlines()[-1]
        reasons.append(f"stderr: {first_line}")

    if reasons:
        combined = " | ".join(reasons)
        # store a reasonably sized message
        job.last_error = combined if len(combined) < 3000 else combined[:3000] + '...'
        # persist state so the reason is available on next run
        try:
            manager._save_state()
        except Exception:
            pass
        return job.last_error

    return None


def monitor_continuous(manager: ProductionManager, refresh_interval: int = 30):
    """
    Continuous monitoring with curses interface.
    
    Args:
        manager: ProductionManager instance
        refresh_interval: Seconds between updates
    """
    def draw_screen(stdscr):
        """Draw the monitoring screen"""
        curses.curs_set(0)  # Hide cursor
        stdscr.nodelay(True)  # Non-blocking input
        
        while True:
            stdscr.clear()
            height, width = stdscr.getmaxyx()
            
            # Update status
            manager.update_status()
            progress = manager.get_progress()
            
            # Header
            title = f"Production Monitor: {manager.config.name}"
            stdscr.addstr(0, (width - len(title)) // 2, title, curses.A_BOLD)
            stdscr.addstr(1, 0, "=" * width)
            
            # Summary
            row = 3
            stdscr.addstr(row, 0, f"Total Jobs: {progress['total_jobs']}")
            row += 2
            
            # Status breakdown
            stdscr.addstr(row, 0, "Status Breakdown:", curses.A_BOLD)
            row += 1
            
            for status, count in progress['status_counts'].items():
                if count > 0:
                    pct = 100.0 * count / progress['total_jobs']
                    line = f"  {status:20s}: {count:5d} ({pct:5.1f}%)"
                    stdscr.addstr(row, 0, line)
                    row += 1
            
            row += 1
            
            # Progress bars
            completion_pct = int(100 * progress['completion_rate'])
            validation_pct = int(100 * progress['validation_rate'])
            failure_pct = int(100 * progress['failure_rate'])
            
            bar_width = min(50, width - 40)
            
            stdscr.addstr(row, 0, "Completion: ")
            stdscr.addstr(row, 12, "[")
            filled = int(bar_width * completion_pct / 100)
            if filled > 0:
                stdscr.addstr(row, 13, "=" * filled, curses.A_REVERSE)
            stdscr.addstr(row, 13 + bar_width, "]")
            stdscr.addstr(row, 15 + bar_width, f"{completion_pct}%")
            row += 1
            
            stdscr.addstr(row, 0, "Validation: ")
            stdscr.addstr(row, 12, "[")
            filled = int(bar_width * validation_pct / 100)
            if filled > 0:
                stdscr.addstr(row, 13, "=" * filled, curses.A_REVERSE)
            stdscr.addstr(row, 13 + bar_width, "]")
            stdscr.addstr(row, 15 + bar_width, f"{validation_pct}%")
            row += 1
            
            stdscr.addstr(row, 0, "Failures:   ")
            stdscr.addstr(row, 12, "[")
            filled = int(bar_width * failure_pct / 100)
            if filled > 0:
                stdscr.addstr(row, 13, "=" * filled, curses.A_REVERSE)
            stdscr.addstr(row, 13 + bar_width, "]")
            stdscr.addstr(row, 15 + bar_width, f"{failure_pct}%")
            row += 2
            
            # Recent jobs
            stdscr.addstr(row, 0, "Recent Jobs:", curses.A_BOLD)
            row += 1

            # Show last 10 jobs with their status
            recent_jobs = sorted(
                manager.jobs.items(),
                key=lambda x: x[1].submit_time or 0,
                reverse=True
            )[:10]

            for job_id, job in recent_jobs:
                runtime = ""
                if job.submit_time:
                    if job.completion_time:
                        duration = job.completion_time - job.submit_time
                    else:
                        duration = time.time() - job.submit_time
                    runtime = f" ({format_duration(duration)})"

                # If job recently failed/was held and we haven't recorded an error, inspect files
                if job.status == JobStatus.FAILED and not getattr(job, 'last_error', None):
                    _inspect_and_record_failure(manager, job)

                # Build display line and append a short error summary when available
                base_line = f"  Job {job_id:4d}: {job.status.value:15s}{runtime}"
                error_summary = ''
                if getattr(job, 'last_error', None):
                    # show a short, single-line summary in the UI
                    one_line = str(job.last_error).splitlines()[0]
                    # truncate to keep screen tidy
                    max_err_len = max(10, width - len(base_line) - 6)
                    if len(one_line) > max_err_len:
                        one_line = one_line[:max_err_len-3] + '...'
                    error_summary = f"  -> {one_line}"

                line = base_line + (" " + error_summary if error_summary else "")

                # Ensure we don't try to write past the screen width
                if len(line) > width:
                    line = line[:width-1]

                stdscr.addstr(row, 0, line)
                row += 1

                if row >= height - 3:
                    break
            
            # Footer
            if height > 2:
                stdscr.addstr(height - 2, 0, "=" * width)
                footer = f"Press 'q' to quit | Refresh: {refresh_interval}s | Last update: {time.strftime('%H:%M:%S')}"
                stdscr.addstr(height - 1, 0, footer)
            
            stdscr.refresh()
            
            # Check for quit command
            try:
                key = stdscr.getch()
                if key == ord('q') or key == ord('Q'):
                    break
            except:
                pass
            
            # Sleep
            time.sleep(refresh_interval)
    
    try:
        curses.wrapper(draw_screen)
    except KeyboardInterrupt:
        pass


def monitor_simple(manager: ProductionManager, refresh_interval: int = 30, max_iterations: Optional[int] = None):
    """
    Simple text-based monitoring (no curses).
    
    Args:
        manager: ProductionManager instance
        refresh_interval: Seconds between updates
        max_iterations: Maximum number of iterations (None for infinite)
    """
    iteration = 0
    
    try:
        while max_iterations is None or iteration < max_iterations:
            print(f"\n{time.strftime('%Y-%m-%d %H:%M:%S')}")
            manager.update_status()

            # Inspect newly failed/held jobs and record concise reasons
            for job_id, job in manager.jobs.items():
                if job.status == JobStatus.FAILED and not getattr(job, 'last_error', None):
                    _inspect_and_record_failure(manager, job)

            manager.print_progress()

            # Print detailed failure reasons for any failed jobs (useful for --once/status)
            failed_jobs = [j for j in manager.jobs.values() if j.status == JobStatus.FAILED]
            if failed_jobs:
                print("Failed jobs details:")
                for j in failed_jobs:
                    reason = getattr(j, 'last_error', None)
                    print(f"  Job {j.job_id}: {j.condor_job_id or 'no-condor-id'}")
                    if reason:
                        print(f"    Reason: {reason}")
                    else:
                        # try to show stderr tail if available
                        paths = _condor_paths_for_job(j, manager.config.work_dir)
                        stderr_excerpt = None
                        if paths and paths.get('stderr'):
                            stderr_excerpt = _parse_stderr_for_failure(paths.get('stderr'), max_lines=10)
                        if stderr_excerpt:
                            print("    Stderr (tail):")
                            for ln in stderr_excerpt.splitlines()[-10:]:
                                print(f"      {ln}")
                        else:
                            print("    No stderr/condor-log available yet.")

            iteration += 1

            if max_iterations is None or iteration < max_iterations:
                time.sleep(refresh_interval)
                
    except KeyboardInterrupt:
        print("\nMonitoring interrupted")


def list_productions(work_dir: Path = Path(".")):
    """List all productions in a directory"""
    print("Available productions:")
    print("-" * 60)
    
    # Find all production_state.json files
    state_files = list(work_dir.glob("**/production_state.json"))
    
    if not state_files:
        print("No productions found")
        return
    
    for state_file in state_files:
        prod_dir = state_file.parent
        
        # Create a temporary manager to read state
        temp_config = ProductionConfig(
            name=prod_dir.name,
            work_dir=prod_dir,
            exe_path=Path("dummy"),
            base_config="dummy",
            output_dir=Path("dummy"),
        )
        
        try:
            manager = ProductionManager(temp_config)
            progress = manager.get_progress()
            
            print(f"\n{prod_dir.name}:")
            print(f"  Location: {prod_dir}")
            print(f"  Total jobs: {progress['total_jobs']}")
            print(f"  Completed: {progress['status_counts']['completed']}")
            print(f"  Validated: {progress['status_counts']['validated']}")
            print(f"  Failed: {progress['status_counts']['failed'] + progress['status_counts']['missing_output']}")
            
        except Exception as e:
            print(f"\n{prod_dir.name}:")
            print(f"  Error loading state: {e}")


def main():
    parser = argparse.ArgumentParser(
        description="Monitor and manage RDFAnalyzerCore productions"
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # Monitor command
    monitor_parser = subparsers.add_parser('monitor', help='Monitor production progress')
    monitor_parser.add_argument(
        '--name', '-n',
        help='Production name'
    )
    monitor_parser.add_argument(
        '--work-dir', '-w',
        help='Production work directory'
    )
    monitor_parser.add_argument(
        '--refresh', '-r',
        type=int,
        default=30,
        help='Refresh interval in seconds (default: 30)'
    )
    monitor_parser.add_argument(
        '--simple',
        action='store_true',
        help='Use simple text output instead of curses'
    )
    monitor_parser.add_argument(
        '--once',
        action='store_true',
        help='Run once and exit'
    )
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Print production status')
    status_parser.add_argument(
        '--name', '-n',
        help='Production name'
    )
    status_parser.add_argument(
        '--work-dir', '-w',
        help='Production work directory'
    )
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all productions')
    list_parser.add_argument(
        '--dir', '-d',
        default='.',
        help='Directory to search for productions (default: current)'
    )
    
    # Validate command
    validate_parser = subparsers.add_parser('validate', help='Validate job outputs')
    validate_parser.add_argument(
        '--name', '-n',
        help='Production name'
    )
    validate_parser.add_argument(
        '--work-dir', '-w',
        help='Production work directory'
    )
    
    # Resubmit command
    resubmit_parser = subparsers.add_parser('resubmit', help='Resubmit failed jobs')
    resubmit_parser.add_argument(
        '--name', '-n',
        help='Production name'
    )
    resubmit_parser.add_argument(
        '--work-dir', '-w',
        help='Production work directory'
    )
    resubmit_parser.add_argument(
        '--max-attempts',
        type=int,
        help='Maximum retry attempts per job'
    )
    
    args = parser.parse_args()
    
    if args.command is None:
        parser.print_help()
        return 1
    
    if args.command == 'list':
        list_productions(Path(args.dir))
        return 0
    
    # For other commands, we need production info
    if not args.name and not args.work_dir:
        print("Error: either --name or --work-dir must be specified")
        return 1
    
    # Determine work directory
    if args.work_dir:
        work_dir = Path(args.work_dir)
    else:
        work_dir = Path(f"condorSub_{args.name}")
    
    if not work_dir.exists():
        print(f"Error: production directory not found: {work_dir}")
        return 1
    
    # Create production config
    prod_config = ProductionConfig(
        name=args.name or work_dir.name,
        work_dir=work_dir,
        exe_path=Path("dummy"),  # Not needed for monitoring
        base_config="dummy",
        output_dir=Path("dummy"),
    )
    
    # Create manager
    manager = ProductionManager(prod_config)
    
    # Execute command
    if args.command == 'monitor':
        if args.once:
            monitor_simple(manager, args.refresh, max_iterations=1)
        elif args.simple:
            monitor_simple(manager, args.refresh)
        else:
            monitor_continuous(manager, args.refresh)
            
    elif args.command == 'status':
        manager.update_status()

        # Inspect failed/held jobs and record reasons
        for job_id, job in manager.jobs.items():
            if job.status == JobStatus.FAILED and not getattr(job, 'last_error', None):
                _inspect_and_record_failure(manager, job)

        manager.print_progress()

        # Print per-job failure details
        failed_jobs = [j for j in manager.jobs.values() if j.status == JobStatus.FAILED]
        if failed_jobs:
            print("\nFailed jobs details:")
            for j in failed_jobs:
                print(f"  Job {j.job_id}: condor_id={j.condor_job_id or 'N/A'}")
                if getattr(j, 'last_error', None):
                    print(f"    Reason: {j.last_error}")
                else:
                    paths = _condor_paths_for_job(j, manager.config.work_dir)
                    if paths and paths.get('stderr') and paths['stderr'].exists():
                        print(f"    Stderr (tail):")
                        stderr_excerpt = _parse_stderr_for_failure(paths['stderr'], max_lines=20)
                        for ln in (stderr_excerpt or '').splitlines()[-20:]:
                            print(f"      {ln}")
                    else:
                        print("    No additional logs available yet.")
        
    elif args.command == 'validate':
        print("Validating job outputs...")
        results = manager.validate_outputs()
        valid_count = sum(1 for v in results.values() if v)
        print(f"Validated {valid_count}/{len(results)} jobs successfully")
        manager.print_progress()
        
    elif args.command == 'resubmit':
        print("Resubmitting failed jobs...")
        count = manager.resubmit_failed(max_attempts=args.max_attempts)
        print(f"Resubmitted {count} jobs")
        manager.update_status()
        manager.print_progress()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
