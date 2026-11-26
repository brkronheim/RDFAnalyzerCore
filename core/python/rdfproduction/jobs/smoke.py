"""Run and validate representative samples before dispatch."""

from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Any
import hashlib
import json
import os
import shutil
import tempfile

from dataset_manifest import DatasetManifest
from ..performance import PerformanceRecorder
from submission_backend import read_config
from run_bundle import file_digest
from .paths import SkimPaths
from . import staging as _job_staging
from . import job_config as _job_config
import rdfproduction.executors as _executors


class SmokeOperations(SkimPaths):
    @property
    def smoke_path(self):
        return Path(self._test_job_dir) / "test_passed.txt"

    def smoke_complete(self):
        """Require passing preflight markers for every configured sample type."""
        if not self.smoke_path.is_file():
            return False

        exe_path = os.path.abspath(self.exe)
        staged_exe = os.path.join(self._test_job_dir, self._exe_relpath)
        if not _job_staging._staged_copy_is_current(exe_path, staged_exe):
            return False
        local_libs, library_signature = _job_staging._local_shared_libs_with_signature(exe_path)
        if not library_signature or any(
            not _job_staging._staged_copy_is_current(
                lib_src, os.path.join(self._test_job_dir, lib_name)
            )
            for lib_name, lib_src in local_libs.items()
        ):
            return False
        try:
            with open(self._test_jobs_manifest_path, encoding="utf-8") as jobs_fh:
                jobs_manifest = json.load(jobs_fh)
            if (
                not isinstance(jobs_manifest, dict)
                or jobs_manifest.get("schema_version") != 1
                or not isinstance(jobs_manifest.get("jobs"), list)
            ):
                return False
            manifest_hash = hashlib.sha256(
                Path(self._test_jobs_manifest_path).read_bytes()
            ).hexdigest()
            expected_types = sorted(
                {
                    str(job.get("dtype", "")).lower()
                    for job in jobs_manifest.get("jobs", [])
                    if isinstance(job, dict) and job.get("dtype")
                }
            )
            if not expected_types:
                return False
            exe_stat = os.stat(exe_path)
            python_env = self._python_env_path()
            python_env_sha256 = file_digest(python_env) if python_env else ""
            with open(self.smoke_path) as marker_fh:
                marker = {}
                for line in marker_fh:
                    key, sep, value = line.strip().partition("=")
                    if sep:
                        marker[key] = value
        except (OSError, json.JSONDecodeError):
            return False
        try:
            return (
                marker.get("status") == "passed"
                and marker.get("test_schema_version") == "3"
                and sorted(marker.get("test_types", "").split(",")) == expected_types
                and marker.get("test_jobs_sha256") == manifest_hash
                and marker.get("runtime_libs_sha256") == library_signature
                and marker.get("root_setup_sha256") == hashlib.sha256(self._root_setup_content.encode()).hexdigest()
                and marker.get("python_env_sha256", "") == python_env_sha256
                and int(marker.get("exe_size", "-1")) == exe_stat.st_size
                and int(marker.get("exe_mtime_ns", "-1")) == exe_stat.st_mtime_ns
            )
        except ValueError:
            return False

    def _refresh_test_input_url(self, config_path: str) -> None:
        """Refresh a reused file-source test config from its prepared job."""
        if not self.file_source:
            return

        config = read_config(config_path)
        input_url = str(config.get("fileList", "")).split(",", 1)[0].strip()
        if not input_url:
            return

        prep_path = os.path.join(self._run_dir, "prep_submission.json")
        if not os.path.isfile(prep_path):
            return
        try:
            with open(prep_path) as fh:
                prepared_jobs = json.load(fh)
        except (OSError, json.JSONDecodeError):
            return

        input_lfn = _job_config._extract_lfn_for_redirect(input_url)
        for entry in prepared_jobs:
            if not isinstance(entry, dict):
                continue
            source_job_dir = str(entry.get("job_dir", ""))
            if not source_job_dir:
                continue
            source_redirectors = os.path.join(
                source_job_dir, "site_redirectors.json"
            )
            if not os.path.isfile(source_redirectors):
                continue
            try:
                with open(source_redirectors) as fh:
                    redirector_map = json.load(fh)
            except (OSError, json.JSONDecodeError):
                continue
            if input_lfn not in redirector_map:
                continue

            config["fileList"] = _job_config._select_test_input_url(input_url, source_job_dir)
            _job_config._write_config_file(config_path, config)
            return

    def smoke(self):
        test_dir = self._test_job_dir
        if not os.path.isfile(self._test_jobs_manifest_path):
            if self.file_source:
                raise RuntimeError(
                    f"Preflight job manifest is missing: {self._test_jobs_manifest_path}. "
                    "PrepareSkimJobs must create one test per available data/MC type."
                )
            # Standard manifest mode has no PrepareSkimJobs dependency, so
            # create the local preflight configs directly from its manifest.
            self._prepare_standard_test_job(test_dir)

        try:
            with open(self._test_jobs_manifest_path, encoding="utf-8") as jobs_fh:
                jobs_manifest = json.load(jobs_fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Could not read preflight job manifest: {self._test_jobs_manifest_path}"
            ) from exc
        if (
            not isinstance(jobs_manifest, dict)
            or jobs_manifest.get("schema_version") != 1
        ):
            raise RuntimeError(
                f"Invalid preflight job manifest: {self._test_jobs_manifest_path}"
            )
        test_jobs = jobs_manifest.get("jobs")
        if not isinstance(test_jobs, list) or not test_jobs:
            raise RuntimeError(
                f"No preflight inputs were recorded in {self._test_jobs_manifest_path}. "
                "Ensure the manifest has at least one available data or MC sample."
            )

        exe_relpath = self._exe_relpath
        exe_in_test = os.path.join(test_dir, exe_relpath)
        current_exe = os.path.abspath(self.exe)
        if os.path.isfile(current_exe) and not _job_staging._staged_copy_is_current(
            current_exe, exe_in_test
        ):
            shutil.copy2(current_exe, exe_in_test)
        if not os.path.isfile(exe_in_test):
            # Try the absolute path directly (in case shared_inputs is on PATH)
            exe_in_test = os.path.abspath(self.exe)

        if not os.path.isfile(exe_in_test):
            raise RuntimeError(
                f"Executable not found for test job: {exe_in_test!r}. "
                "Build the analysis before running RunSkimTestJob."
            )

        local_libs, library_signature = _job_staging._local_shared_libs_with_signature(
            current_exe
        )
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(test_dir, lib_name)
            if not _job_staging._staged_copy_is_current(lib_src, lib_dst):
                shutil.copy2(lib_src, lib_dst)

        self.publish_message(
            f"[RunSkimTestJob] Running {len(test_jobs)} sample-type preflight job(s) "
            f"in {test_dir} …"
        )

        python_env = self._python_env_path()
        python_env_sha256 = file_digest(python_env) if python_env else ""
        with PerformanceRecorder("RunSkimTestJob") as rec, ExitStack() as cleanup:
            root_setup = self._root_setup_content
            if python_env:
                env_dir = cleanup.enter_context(
                    tempfile.TemporaryDirectory(prefix=".preflight-python-", dir=test_dir)
                )
                _job_staging._extract_tar_archive(python_env, env_dir)
                if file_digest(python_env) != python_env_sha256:
                    raise RuntimeError("Python environment archive changed during preflight setup")
                root_setup = _job_staging._python_environment_setup(root_setup, env_dir)
            results = []
            tested_types = []
            tested_samples = []
            for test_job in test_jobs:
                if not isinstance(test_job, dict):
                    raise RuntimeError("Invalid preflight job record")
                config_name = str(test_job.get("config", ""))
                config_path = os.path.join(test_dir, config_name)
                if not config_name or not os.path.isfile(config_path):
                    raise RuntimeError(f"Preflight config not found: {config_path!r}")
                from native_config_io import materialize_runtime_config
                config_path = str(materialize_runtime_config(config_path))
                config_name = os.path.basename(config_path)
                test_config = read_config(config_path)
                _job_staging._remove_mc_generator_config_for_data(test_config)
                _job_config._write_config_file(config_path, test_config)
                self._refresh_test_input_url(config_path)
                dtype = str(test_job.get("dtype", "unknown")).lower()
                sample = str(test_job.get("dataset", "unknown"))
                self.publish_message(
                    f"[RunSkimTestJob] Testing {dtype} sample {sample} …"
                )
                result = _executors._run_analysis_job(
                    exe_path=exe_in_test,
                    job_dir=test_dir,
                    root_setup=root_setup,
                    container_setup="",
                    config_relpath=config_name,
                    stream_output=_job_staging._stream_analysis_output(),
                    worker_number=0,
                )
                results.append(result)
                tested_types.append(dtype)
                tested_samples.append(sample)
        rec.save(os.path.join(test_dir, "test_job.perf.json"))

        self.publish_message(
            f"[RunSkimTestJob] Preflight passed for {', '.join(tested_types)}: "
            f"{'; '.join(results)}"
        )

        exe_stat = os.stat(current_exe)
        with self.smoke_path.open("w") as fh:
            fh.write(
                f"status=passed\n"
                f"test_schema_version=3\n"
                f"test_types={','.join(sorted(tested_types))}\n"
                f"test_samples={','.join(tested_samples)}\n"
                f"test_jobs_sha256={hashlib.sha256(Path(self._test_jobs_manifest_path).read_bytes()).hexdigest()}\n"
                f"runtime_libs_sha256={library_signature}\n"
                f"root_setup_sha256={hashlib.sha256(self._root_setup_content.encode()).hexdigest()}\n"
                f"python_env_sha256={python_env_sha256}\n"
                f"exe_size={exe_stat.st_size}\n"
                f"exe_mtime_ns={exe_stat.st_mtime_ns}\n"
                f"{';'.join(results)}\n"
            )

    def _prepare_standard_test_job(self, test_dir: str) -> None:
        """Create a minimal test job directory in standard (non-file-source) mode.

        Selects one dataset with files for each available data/MC type, then
        writes a minimal config for every selected one.
        """
        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running RunSkimTestJob."
            )

        manifest = DatasetManifest.load(self.dataset_manifest)
        if not manifest.datasets:
            raise RuntimeError(
                f"Dataset manifest '{self.dataset_manifest}' contains no datasets."
            )

        candidates: dict[str, tuple[Any, str]] = {}
        for dataset in manifest.datasets:
            first_file = dataset.files[0] if dataset.files else dataset.das
            if not first_file:
                continue
            dtype = str(dataset.dtype or "").strip().lower()
            if dtype not in {"data", "mc"}:
                dtype = "other"
            candidates.setdefault(dtype, (dataset, str(first_file)))

        if not candidates:
            raise RuntimeError(
                f"Dataset manifest '{self.dataset_manifest}' has no dataset with "
                "a file or DAS identifier for preflight."
            )
        test_types = [dtype for dtype in ("data", "mc") if dtype in candidates]
        if not test_types:
            test_types = [next(iter(candidates))]

        Path(test_dir).mkdir(parents=True, exist_ok=True)

        # Copy executable
        exe_relpath = self._exe_relpath
        dst_exe = os.path.join(test_dir, exe_relpath)
        if not os.path.exists(dst_exe):
            shutil.copy2(exe_path, dst_exe)

        # Copy local shared libraries
        local_libs, _ = _job_staging._local_shared_libs_with_signature(exe_path)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(test_dir, lib_name)
            if not _job_staging._staged_copy_is_current(lib_src, lib_dst):
                shutil.copy2(lib_src, lib_dst)

        # Copy auxiliary config files referenced in template
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _job_staging._copy_analysis_config_files(self.submit_config, template_config, test_dir)
            _job_staging._copy_analysis_aux_dir(self.submit_config, test_dir)
        else:
            template_config = {}

        test_jobs = []
        for dtype in test_types:
            dataset, first_file = candidates[dtype]
            test_config = dict(template_config)
            test_config.update(_job_config._dataset_job_metadata(dataset))
            test_config.update(_job_config._stitch_job_overrides(self))
            test_config["fileList"] = first_file
            test_config["saveFile"] = f"test_output_{dtype}.root"
            test_config["metaFile"] = f"test_output_{dtype}_meta.root"
            test_config["saveDirectory"] = test_dir
            test_config["lumi"] = str(
                manifest.lumi_for(dataset.year, dataset.era)
            )
            test_config["threads"] = "1"
            _job_staging._remove_mc_generator_config_for_data(test_config)
            for key in ("__orig_saveFile", "__orig_metaFile", "__orig_fileList"):
                test_config.pop(key, None)

            if os.path.isfile(self.submit_config):
                _job_staging._ensure_local_analysis_runtime_layout(
                    self.submit_config,
                    test_config,
                    test_dir,
                )

            for key, value in list(test_config.items()):
                if key.startswith("__") or not isinstance(value, str):
                    continue
                if value.endswith((".txt", ".yaml", ".yml", ".json", ".json.gz", ".onnx")):
                    basename = _job_staging._asset_name(value, test_config)
                    cfg_path = os.path.join(test_dir, "cfg", basename)
                    if os.path.exists(cfg_path):
                        test_config[key] = os.path.join("cfg", basename)
                    elif os.path.exists(os.path.join(test_dir, basename)):
                        test_config[key] = basename

            config_name = f"submit_config_{dtype}.yaml"
            config_path = os.path.join(test_dir, config_name)
            _job_config._write_config_file(config_path, test_config)
            test_jobs.append(
                {
                    "dtype": dtype,
                    "dataset": dataset.name,
                    "config": config_name,
                    "input": first_file,
                    "config_sha256": hashlib.sha256(
                        Path(config_path).read_bytes()
                    ).hexdigest(),
                }
            )

        primary_config = os.path.join(test_dir, test_jobs[0]["config"])
        shutil.copy2(primary_config, os.path.join(test_dir, "submit_config.yaml"))
        with open(self._test_jobs_manifest_path, "w", encoding="utf-8") as manifest_fh:
            json.dump({"schema_version": 1, "jobs": test_jobs}, manifest_fh, indent=2)

        self.publish_message(
            f"[RunSkimTestJob] Test jobs prepared in {test_dir}/ for "
            f"{', '.join(test_types)}.\n"
            f"  Inputs: "
            + "; ".join(
                f"{job['dataset']} ({job['dtype']}): {job['input']}"
                for job in test_jobs
            )
        )
