"""Resolve run directories, worker settings, and partition profiles."""

from __future__ import annotations

from pathlib import Path
from typing import Optional
import json
import os
import shutil

from job_sizing import select_sizing
from job_sizing import sizing_context
from optional_extensions import render_environment_setup
from submission_backend import read_config
from . import staging as _job_staging
from . import job_config as _job_config

from .. import REPOSITORY_ROOT


WORKSPACE = str(REPOSITORY_ROOT)


class AnalysisPaths(object):
    @property
    def _jobs_dir(self) -> str:
        """Directory tree holding per-dataset job subdirectories."""
        return os.path.join(self._run_dir, "jobs")

    @property
    def _outputs_dir(self) -> str:
        """Directory tree holding per-dataset analysis output ROOT files."""
        return os.path.join(self._run_dir, "outputs")

    @property
    def _job_outputs_dir(self) -> str:
        """Directory holding per-branch ``.done`` marker files."""
        return os.path.join(self._run_dir, "job_outputs")


class SkimPaths(AnalysisPaths):
    def _job_sizing_context(self) -> dict:
        config = dict(read_config(self.submit_config), **_job_config._stitch_job_overrides(self))
        # Input inventories and output locations do not describe computational
        # work. Calibration/selection assets, executable and runtime do.
        workload_config = {k: v for k, v in config.items()
                           if k not in {"sampleConfig", "fileList", "saveFile", "metaFile", "saveDirectory"}}
        return sizing_context(config, self.exe,
            payloads=_job_staging._analysis_config_payload_sources(self.submit_config, workload_config),
            runtime=_job_staging._local_shared_libs_with_signature(os.path.abspath(self.exe))[1],
            environment=json.dumps({"root_setup": self._root_setup_content if self.root_setup else "",
                                    "container": self.container_setup, "python_env": self.python_env}, sort_keys=True))

    def _sizing_selection(self, freeze=False) -> dict:
        receipt = Path(self._run_dir) / "job_sizing_selection.json"
        if receipt.is_file() and not freeze:
            return json.loads(receipt.read_text())
        if not freeze:
            return {}
        profile, seed = self.partition_profile, self.input_metadata_cache
        if self.sizing_pilots:
            pilot_result = Path(self._run_dir) / 'sizing_pilots/result.json'
            if not pilot_result.is_file():
                raise RuntimeError('Sizing pilots must finish before main preparation')
            result = json.loads(pilot_result.read_text())
            profile, seed = result['profile'], result['metadata_seed']
        result = select_sizing(self._run_dir, self._job_sizing_context(), profile=profile,
                               metadata_seed=seed,
                               target_seconds=self.sizing_target_seconds,
                               pilot_target_seconds=self.sizing_pilot_target_seconds,
                               allow_refresh=not (Path(self._run_dir) / "execution_identity.json").is_file())
        if result.get("warning"):
            self.publish_message("WARNING: " + result["warning"])
        return result

    def _planning_profile(self) -> str:
        if self.partition not in {"auto", "events", "entry_range"}:
            # Preserve the existing error for an incompatible explicit profile.
            return self.partition_profile if self.partition_profile != "none" else ""
        if self.partition == "auto" and str(read_config(self.submit_config).get("threads", "1")) != "1":
            # Automatic reuse must not introduce unsupported ranged execution
            # into a previously valid multi-threaded workflow.
            return ""
        return str(self._sizing_selection(freeze=True).get("profile", ""))

    def _planning_metadata_cache(self) -> str:
        """Keep mutable discovered counts isolated from the user's seed snapshot."""
        if self.partition not in {"auto", "events", "entry_range"}:
            return ""
        path = os.path.join(self._run_dir, "input_metadata.json")
        seed = self._sizing_selection(freeze=True).get("metadata_seed", "")
        if seed and not os.path.exists(path):
            Path(path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(seed, path)
        return path

    def _resolve_skim(self, path_value: str) -> str:
        """Resolve *path_value* relative to the submit-config's parent directory."""
        return _job_staging._resolve_analysis_config_path(self.submit_config, path_value)

    @property
    def _shared_dir(self) -> str:
        """Shared inputs directory containing the exe, libs, and x509 proxy."""
        return os.path.join(self._run_dir, "shared_inputs")

    @property
    def _shared_archive_path(self) -> str:
        """Tarball containing the worker runtime cfg/ and aux/ layout."""
        return os.path.join(self._run_dir, _job_staging._shared_inputs_archive_name())

    @property
    def _exe_relpath(self) -> str:
        """Basename of the compiled analysis executable."""
        return os.path.basename(os.path.abspath(self.exe))

    @property
    def _root_setup_content(self) -> str:
        """Return portable setup text with enabled extension hooks embedded."""
        if not self.root_setup:
            return ""
        path = self._resolve_skim(self.root_setup)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"root_setup must point to an existing file, got: {self.root_setup!r}"
            )
        with open(path) as fh:
            return render_environment_setup(fh.read()).rstrip("\n")

    def _python_env_path(self) -> Optional[str]:
        """Return the resolved path to the Python environment tarball, or None."""
        if not self.python_env:
            return None
        path = self._resolve_skim(self.python_env)
        if not path or not os.path.isfile(path):
            raise RuntimeError(
                f"python_env must point to an existing tarball, "
                f"got: {self.python_env!r}\n"
                "Create a worker environment archive before preparing production."
            )
        return path

    def _file_list_dir(self) -> str:
        """Return the directory containing per-dataset file-list JSONs."""
        if not self.file_source:
            return ""
        from ..discovery import file_list_directory
        return str(file_list_directory(self._workspace, self._effective_file_source_name, self.file_source))

    @property
    def _effective_file_source_name(self) -> str:
        """Return the configured file-source name or fall back to the run name."""
        return str(self.file_source_name or self.name)

    @property
    def _test_job_dir(self) -> str:
        """Directory for the local pre-submission test job."""
        return os.path.join(self._run_dir, "test_job")

    @property
    def _test_jobs_manifest_path(self) -> str:
        """Manifest of per-dtype configurations used by the local preflight."""
        return os.path.join(self._test_job_dir, "test_jobs.json")
