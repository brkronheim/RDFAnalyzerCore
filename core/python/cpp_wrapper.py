#!/usr/bin/env python3
"""
Python wrapper for C++ executables to enable DASK distributed execution.

This wrapper handles:
- Execution environment setup
- Shared library discovery and staging
- Input/output staging
- Error handling and logging
"""

import os
import subprocess
import sys
import shutil
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# System library paths to exclude from staging (available on all nodes)
SYSTEM_LIB_PATHS = ['/lib64/', '/usr/lib64/', '/lib/', '/usr/lib/']


def find_shared_libraries(executable_path: str) -> list:
    """
    Find all shared libraries required by an executable.
    
    Args:
        executable_path: Path to the executable
        
    Returns:
        List of absolute paths to required .so files
    """
    try:
        # Use ldd to find shared library dependencies
        result = subprocess.run(
            ['ldd', executable_path],
            capture_output=True,
            text=True,
            timeout=30
        )
        
        if result.returncode != 0:
            logger.warning(f"ldd failed for {executable_path}: {result.stderr}")
            return []
        
        so_files = []
        for line in result.stdout.split('\n'):
            line = line.strip()
            if '=>' in line:
                # Format: libname.so => /path/to/libname.so (address)
                parts = line.split('=>')
                if len(parts) >= 2:
                    path_part = parts[1].strip().split()[0]
                    if path_part and path_part != '(0x' and os.path.exists(path_part):
                        so_files.append(path_part)
            elif line.startswith('/'):
                # Format: /path/to/libname.so (address)
                path_part = line.split()[0]
                if os.path.exists(path_part):
                    so_files.append(path_part)
        
        return so_files
        
    except subprocess.TimeoutExpired:
        logger.error(f"ldd timed out for {executable_path}")
        return []
    except Exception as e:
        logger.error(f"Error finding shared libraries: {e}")
        return []


def stage_shared_libraries(executable_path: str, target_dir: str) -> list:
    """
    Stage shared libraries to a target directory.
    
    Args:
        executable_path: Path to the executable
        target_dir: Directory to stage libraries to
        
    Returns:
        List of staged library paths (relative to target_dir)
    """
    so_files = find_shared_libraries(executable_path)
    
    if not so_files:
        logger.info("No additional shared libraries found (or executable is statically linked)")
        return []
    
    # Create lib directory
    lib_dir = Path(target_dir) / "lib"
    lib_dir.mkdir(parents=True, exist_ok=True)
    
    staged_files = []
    for so_path in so_files:
        try:
            # Skip system libraries that should be available on all nodes
            if any(so_path.startswith(prefix) for prefix in SYSTEM_LIB_PATHS):
                continue
            
            # Copy library to lib directory
            lib_name = os.path.basename(so_path)
            target_path = lib_dir / lib_name
            
            if not target_path.exists():
                shutil.copy2(so_path, target_path)
                logger.info(f"Staged library: {lib_name}")
            
            staged_files.append(f"lib/{lib_name}")
            
        except Exception as e:
            logger.warning(f"Failed to stage library {so_path}: {e}")
    
    return staged_files


def setup_library_path(lib_dir: str) -> None:
    """
    Setup LD_LIBRARY_PATH to include staged libraries.
    
    Args:
        lib_dir: Directory containing staged libraries
    """
    if not os.path.exists(lib_dir):
        return
    
    abs_lib_dir = os.path.abspath(lib_dir)
    
    current_ld_path = os.environ.get('LD_LIBRARY_PATH', '')
    if current_ld_path:
        os.environ['LD_LIBRARY_PATH'] = f"{abs_lib_dir}:{current_ld_path}"
    else:
        os.environ['LD_LIBRARY_PATH'] = abs_lib_dir
    
    logger.info(f"LD_LIBRARY_PATH: {os.environ['LD_LIBRARY_PATH']}")


def run_cpp_executable(
    exe_path: str,
    config_path: str,
    working_dir: str = ".",
    setup_libs: bool = True
) -> dict:
    """
    Run a C++ executable with proper environment setup.
    
    Args:
        exe_path: Path to the executable
        config_path: Path to the configuration file
        working_dir: Working directory for execution
        setup_libs: Whether to setup library paths
        
    Returns:
        Dictionary with execution results
    """
    # Change to working directory
    original_dir = os.getcwd()
    os.chdir(working_dir)
    
    try:
        # Setup library path if needed
        if setup_libs:
            # Use os.getcwd() after chdir to avoid double-prefixing relative
            # working_dir paths (e.g. "tmp" → chdir into "tmp", then look for
            # "lib" inside the current directory, not "tmp/lib" inside "tmp").
            lib_dir = os.path.join(os.getcwd(), "lib")
            if os.path.exists(lib_dir):
                setup_library_path(lib_dir)
        
        # Make executable if needed
        if os.path.exists(exe_path):
            os.chmod(exe_path, 0o755)
        
        # Run the executable
        logger.info(f"Running: {exe_path} {config_path}")
        result = subprocess.run(
            [exe_path, config_path],
            capture_output=True,
            text=True,
            timeout=7200  # 2 hour timeout
        )
        
        logger.info(f"Execution completed with return code: {result.returncode}")
        
        return {
            'returncode': result.returncode,
            'stdout': result.stdout,
            'stderr': result.stderr,
            'success': result.returncode == 0
        }
        
    except subprocess.TimeoutExpired:
        logger.error("Execution timed out after 2 hours")
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': 'Execution timed out',
            'success': False
        }
    except Exception as e:
        logger.error(f"Execution failed: {e}")
        return {
            'returncode': -1,
            'stdout': '',
            'stderr': str(e),
            'success': False
        }
    finally:
        os.chdir(original_dir)


def main():
    """Main entry point for wrapper script"""
    if len(sys.argv) < 3:
        print("Usage: cpp_wrapper.py <executable> <config_file> [working_dir]")
        sys.exit(1)
    
    exe_path = sys.argv[1]
    config_path = sys.argv[2]
    working_dir = sys.argv[3] if len(sys.argv) > 3 else "."
    
    result = run_cpp_executable(exe_path, config_path, working_dir)
    
    # Print output
    if result['stdout']:
        print(result['stdout'])
    if result['stderr']:
        print(result['stderr'], file=sys.stderr)
    
    sys.exit(result['returncode'])


if __name__ == "__main__":
    main()
