"""Version information utilities for output artifact versioning.

This module provides functions to retrieve git commit hashes for the main
framework and user analysis repositories, as well as configuration file
modification timestamps. This information is embedded in the output artifacts
(submit_config files) produced by the submission scripts, allowing workflow
managers to detect when jobs need to be rerun.
"""
import datetime
import json
import os
import subprocess


def get_git_hash(path):
    """Get the full git commit hash for the repository at the given path.

    Parameters
    ----------
    path : str or None
        Path inside (or to the root of) a git repository.  Passing None is
        safe and simply returns None.

    Returns
    -------
    str or None
        The full 40-character git commit hash, or None if *path* is None, the
        path is not inside a git repository, or git is not available.
    """
    if path is None:
        return None
    try:
        result = subprocess.run(
            ["git", "-C", path, "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_git_root(path):
    """Get the root directory of the git repository containing the given path.

    Parameters
    ----------
    path : str
        A file or directory path.

    Returns
    -------
    str or None
        The absolute path to the repository root, or None if not in a git
        repository or git is not available.
    """
    search_dir = (
        os.path.dirname(os.path.abspath(path)) if os.path.isfile(path) else os.path.abspath(path)
    )
    try:
        result = subprocess.run(
            ["git", "-C", search_dir, "rev-parse", "--show-toplevel"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return None


def get_config_mtime(config_file):
    """Get the modification time of a configuration file as an ISO 8601 string.

    Parameters
    ----------
    config_file : str
        Path to the configuration file.

    Returns
    -------
    str or None
        The UTC modification time in ISO 8601 format (e.g.
        ``"2024-01-01T12:00:00+00:00"``), or None if the file does not exist
        or its mtime cannot be read.
    """
    try:
        mtime = os.path.getmtime(config_file)
        return datetime.datetime.fromtimestamp(
            mtime, tz=datetime.timezone.utc
        ).isoformat()
    except OSError:
        return None


def get_version_info(config_file, framework_path=None):
    """Collect versioning information for output artifact labelling.

    Retrieves the git commit hash of the main RDFAnalyzerCore framework, the
    git commit hash of the user analysis repository that owns *config_file*,
    and the modification timestamp of *config_file*.

    If the framework and user repositories share the same git root (e.g. when
    analyses live inside the RDFAnalyzerCore tree), both hash fields will have
    the same value.

    Parameters
    ----------
    config_file : str
        Path to the main job submission configuration file.
    framework_path : str, optional
        Path to a file or directory inside the RDFAnalyzerCore repository.
        Defaults to the directory that contains this module (``version_info.py``).

    Returns
    -------
    dict
        A dictionary with the following keys:

        ``framework_hash``
            Full git commit hash of the RDFAnalyzerCore framework, or None.
        ``user_repo_hash``
            Full git commit hash of the user analysis repository, or None.
        ``config_mtime``
            UTC modification time of *config_file* in ISO 8601 format, or None.
    """
    if framework_path is None:
        framework_path = os.path.dirname(os.path.abspath(__file__))

    framework_root = get_git_root(framework_path)
    framework_hash = get_git_hash(framework_root)

    user_repo_root = get_git_root(config_file)
    user_repo_hash = get_git_hash(user_repo_root)

    config_mtime = get_config_mtime(config_file)

    return {
        "framework_hash": framework_hash,
        "user_repo_hash": user_repo_hash,
        "config_mtime": config_mtime,
    }


def write_version_info_json(output_path, version_info):
    """Write version information to a JSON file.

    Parameters
    ----------
    output_path : str
        Destination file path (will be created or overwritten).
    version_info : dict
        Version info dict as returned by :func:`get_version_info`.
    """
    with open(output_path, "w") as f:
        json.dump(version_info, f, indent=2)
        f.write("\n")
