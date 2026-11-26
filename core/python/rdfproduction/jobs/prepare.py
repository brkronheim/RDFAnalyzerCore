"""Materialize the partition plan, job directories, and shared worker inputs."""

from __future__ import annotations

from native_config_io import job_config_path
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import Optional
import hashlib
import json
import os
import shutil
import time

from dataset_manifest import DatasetEntry
from dataset_manifest import DatasetManifest
from ..performance import PerformanceRecorder
from submission_backend import read_config
from run_bundle import execution_identity
from run_bundle import file_digest
from run_bundle import record_execution
from run_bundle import verify_config_bundle
from .paths import SkimPaths
from . import staging as _job_staging
from . import job_config as _job_config


class PreparationOperations(SkimPaths):
    @property
    def preparation_path(self):
        return Path(self._run_dir) / "prep_submission.json"

    @property
    def _prepared_config_hash_path(self) -> str:
        return os.path.join(self._run_dir, "prep_submission.config.sha256")

    def _submit_config_hash(self) -> str:
        digest = hashlib.sha256()
        with open(self.submit_config, "rb") as config_fh:
            for chunk in iter(lambda: config_fh.read(1024 * 1024), b""):
                digest.update(chunk)
        for key in ("partition_profile", "input_metadata_cache"):
            path = getattr(self, key, "")
            if path and path != "none":
                digest.update(key.encode())
                digest.update(file_digest(path).encode())
        selected = self._sizing_selection()
        if self.partition in {"auto", "events", "entry_range"}:
            for key in ("profile", "metadata_seed"):
                if selected.get(key):
                    digest.update(file_digest(selected[key]).encode())
        return digest.hexdigest()

    def _bundle_execution_identity(self, bundle_record):
        exe_path = os.path.abspath(self.exe)
        discovery = {}
        if self.file_source:
            discovery = {str(p.relative_to(self._file_list_dir())): file_digest(p) for p in sorted(Path(self._file_list_dir()).rglob("*"))
                         if p.is_file()}
        partition_options = {"files_per_job": self.files_per_job,
                             "rucio_group_size_gb": self.rucio_group_size_gb,
                             "partition": self.partition, "entries_per_job": self.entries_per_job}
        # Preserve identities of existing/default campaigns. New controls are
        # explicitly sealed when used, before any staged worker is rewritten.
        for key in ("partition_profile", "input_metadata_cache"):
            path = getattr(self, key, "")
            if path and path != "none":
                partition_options[key + "_sha256"] = file_digest(path)
        selected = self._sizing_selection()
        if selected.get("profile") and self.partition in {"auto", "events", "entry_range"}:
            partition_options["selected_profile_sha256"] = file_digest(selected["profile"])
        if selected.get("metadata_seed") and self.partition in {"auto", "events", "entry_range"}:
            partition_options["selected_metadata_sha256"] = file_digest(selected["metadata_seed"])
        if self.entry_range_mode != "filter":
            partition_options["entry_range_mode"] = self.entry_range_mode
        if self.sizing_pilots:
            partition_options['sizing_pilots'] = dict(parallel=self.sizing_pilot_parallel,
                prefix_entries=self.sizing_pilot_prefix_entries, safety_factor=self.sizing_pilot_safety_factor)
        return execution_identity(bundle_record["bundle_id"],
            inputs={"manifest_sha256": file_digest(self.dataset_manifest), "discovery": discovery},
            partitions=partition_options,
            runtime={"executable": file_digest(exe_path),
                     "libraries": _job_staging._local_shared_libs_with_signature(exe_path)[1]},
            options={"output_directory": self.output_directory,
                         "root_setup": hashlib.sha256(self._root_setup_content.encode()).hexdigest() if self.root_setup else "",
                         "container_setup": self.container_setup})

    def preparation_complete(self):
        """Invalidate prepared inputs when their config, runtime, or proxy changes."""
        bundle_record = verify_config_bundle(self.submit_config)
        identity_path = Path(self._run_dir) / "execution_identity.json"
        if bundle_record and identity_path.is_file():
            if json.loads(identity_path.read_text()) != self._bundle_execution_identity(bundle_record):
                return False
        if not self.preparation_path.is_file():
            return False

        if self.x509 and not _job_staging._staged_copy_is_current(
            os.path.abspath(self.x509), os.path.join(self._shared_dir, "x509")
        ):
            return False
        if self.python_env:
            try:
                python_env = self._python_env_path()
            except RuntimeError:
                return False
            if not _job_staging._staged_copy_is_current(
                python_env, os.path.join(self._shared_dir, os.path.basename(python_env))
            ):
                return False

        try:
            with open(self._prepared_config_hash_path) as hash_fh:
                if hash_fh.read().strip() != self._submit_config_hash():
                    return False
        except OSError:
            # Older preparation outputs did not record their source config.
            # Rebuild once so staged job configs cannot silently omit settings.
            return False

        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            return False

        staged_exe_paths = [os.path.join(self._shared_dir, self._exe_relpath)]
        local_libs, _ = _job_staging._local_shared_libs_with_signature(exe_path)
        try:
            exe_mtime_ns = os.stat(exe_path).st_mtime_ns
            archive_mtime_ns = os.stat(self._shared_archive_path).st_mtime_ns
        except OSError:
            return False
        if archive_mtime_ns < exe_mtime_ns:
            return False
        for lib_name, lib_src in local_libs.items():
            try:
                if os.stat(lib_src).st_mtime_ns > archive_mtime_ns:
                    return False
            except OSError:
                return False
            if not _job_staging._staged_copy_is_current(
                lib_src, os.path.join(self._shared_dir, lib_name)
            ):
                return False

        test_config_path = str(job_config_path(self._test_job_dir))
        if self.make_test_job and not os.path.isfile(self._test_jobs_manifest_path):
            return False
        if self.make_test_job and os.path.isfile(test_config_path):
            staged_exe_paths.append(
                os.path.join(self._test_job_dir, self._exe_relpath)
            )
            if any(
                not _job_staging._staged_copy_is_current(
                    lib_src, os.path.join(self._test_job_dir, lib_name)
                )
                for lib_name, lib_src in local_libs.items()
            ):
                return False

        if not all(
            _job_staging._staged_copy_is_current(exe_path, staged_path)
            for staged_path in staged_exe_paths
        ):
            return False
        try:
            _job_staging._validate_staged_skim_executable(self._shared_dir, exe_path)
        except RuntimeError:
            return False
        return True

    def prepare(self):
        with PerformanceRecorder("PrepareSkimJobs") as rec:
            self._run_impl()
        rec.save(os.path.join(self._run_dir, "prep_submission.perf.json"))

    def _run_impl(self):  # noqa: C901 - intentionally comprehensive
        bundle_record = verify_config_bundle(self.submit_config)
        exe_path = os.path.abspath(self.exe)
        if not os.path.isfile(exe_path):
            raise RuntimeError(
                f"Analysis executable not found: {exe_path!r}. "
                "Build the analysis before running PrepareSkimJobs."
            )
        if self.prepare_workers < 1:
            raise ValueError("--prepare-workers must be at least 1")

        manifest = DatasetManifest.load(self.dataset_manifest)
        template_config = read_config(self.submit_config)
        self._sizing_selection(freeze=True)
        if bundle_record:
            # Lock source identity before modifying staged workers. Discovery
            # and the partition plan are recorded once preparation is complete.
            identity = self._bundle_execution_identity(bundle_record)
            record_execution(Path(self._run_dir) / "execution_identity.json", identity)
        base_extra_overrides = _job_config._stitch_job_overrides(self)
        base_extra_overrides["metadataExecutableSha256"] = file_digest(exe_path)
        if bundle_record:
            base_extra_overrides["metadataExecutionId"] = identity["execution_id"]

        # These references depend on the template, optional stitching override,
        # and whether generator-only settings are removed for data. Resolve them
        # once per dtype instead of rescanning the config paths for every job.
        runtime_payload_sources_by_dtype: dict[str, list[str]] = {}
        # Filled while building the archive, before preparation threads start.
        # Keep this cache local to this invocation so a later preparation cannot
        # reuse stale payloads. Jobs receive independent files with identical bytes.
        prepared_payloads: Optional[dict[str, bytes]] = {} if bundle_record else None
        dataset_dtypes = {
            str(entry.dtype or "mc").lower() for entry in manifest.datasets
        } | {"mc"}
        for dtype in dataset_dtypes:
            config_for_dtype = dict(template_config)
            config_for_dtype["dtype"] = dtype
            _job_staging._remove_mc_generator_config_for_data(config_for_dtype)
            config_for_dtype.update(base_extra_overrides)
            runtime_payload_sources_by_dtype[dtype] = _job_staging._analysis_config_payload_sources(
                self.submit_config, config_for_dtype
            )

        def write_job_entry(
            *,
            dataset: DatasetEntry,
            job_dir: str,
            out_dir: str,
            manifest_lumi: Optional[float],
            extra_overrides: dict[str, str],
            site_redirectors: dict,
        ) -> dict[str, str]:
            dtype = str(dataset.dtype or "mc").lower()
            runtime_payload_sources = runtime_payload_sources_by_dtype.get(
                dtype, runtime_payload_sources_by_dtype["mc"]
            )
            _job_config._write_job_config(
                job_dir=job_dir,
                template_config_path=self.submit_config,
                dataset=dataset,
                output_dir=out_dir,
                manifest_lumi=manifest_lumi,
                extra_overrides=extra_overrides,
                stage_shared_inputs_locally=False,
                template_config=template_config,
                runtime_payload_sources=runtime_payload_sources,
                prepared_payloads=prepared_payloads,
            )

            # Write the per-job site redirector list so Condor workers can
            # probe replicas from all available sites.
            with open(os.path.join(job_dir, "site_redirectors.json"), "w") as sr_fh:
                json.dump(site_redirectors, sr_fh)

            return {
                "dataset_name": dataset.name,
                "job_dir": job_dir,
                "config_path": str(job_config_path(job_dir)),
                "out_dir": out_dir,
            }

        def prepare_in_parallel(items, prepare_one) -> list[dict[str, str]]:
            if not items:
                return []

            worker_count = min(self.prepare_workers, len(items))
            prepared: list[dict[str, str]] = []
            start_time = time.monotonic()
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                for count, job_entry in enumerate(
                    executor.map(prepare_one, items), start=1
                ):
                    prepared.append(job_entry)
                    if count % 500 == 0 or count == len(items):
                        elapsed = time.monotonic() - start_time
                        self.publish_message(
                            f"PrepareSkimJobs: prepared {count}/{len(items)} job "
                            f"configs in {elapsed:.1f}s ({worker_count} thread(s))."
                        )
            return prepared

        # ---- set up shared_inputs/ ----------------------------------------
        shared_inputs_start = time.monotonic()
        shared_dir = self._shared_dir
        Path(shared_dir).mkdir(parents=True, exist_ok=True)

        # Copy executable
        shutil.copy2(exe_path, os.path.join(shared_dir, self._exe_relpath))

        # Copy local shared libraries
        local_libs, _ = _job_staging._local_shared_libs_with_signature(exe_path)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(shared_dir, lib_name)
            if not _job_staging._staged_copy_is_current(lib_src, lib_dst):
                shutil.copy2(lib_src, lib_dst)
        if local_libs:
            self.publish_message(
                f"Staged {len(local_libs)} local shared lib(s) into {shared_dir}/"
            )

        # Copy x509 proxy
        if self.x509:
            x509_src = os.path.abspath(self.x509)
            if not os.path.isfile(x509_src):
                raise RuntimeError(f"X509 proxy file not found: {x509_src!r}")
            shutil.copy2(x509_src, os.path.join(shared_dir, "x509"))
        if self.x509 or os.path.lexists(os.path.join(shared_dir, "x509")):
            _job_staging._validate_x509_proxy(os.path.join(shared_dir, "x509"))

        # Copy Python env tarball
        python_env_src = self._python_env_path()
        if python_env_src:
            tarball_name = os.path.basename(python_env_src)
            shutil.copy2(python_env_src, os.path.join(shared_dir, tarball_name))

        # Bundle shared configs and aux once for remote worker materialization.
        _job_staging._bundle_analysis_config_files(
            self.submit_config, template_config, shared_dir,
            prepared_payloads=prepared_payloads,
        )
        _job_staging._bundle_analysis_aux_dir(self.submit_config, shared_dir)
        _job_staging._build_shared_inputs_archive(shared_dir, self._shared_archive_path)
        self.publish_message(
            "PrepareSkimJobs: shared inputs and archive ready in "
            f"{time.monotonic() - shared_inputs_start:.1f}s."
        )

        # ---- determine datasets and file groups ---------------------------
        job_entries: list = []

        if self.file_source:
            fl_dir = self._file_list_dir()
            if not os.path.isdir(fl_dir):
                raise RuntimeError(
                    f"File list directory not found: {fl_dir!r}.\n"
                    f"Discover {self.file_source} inputs for {self._effective_file_source_name!r} before preparation."
                )

            planning_start = time.monotonic()
            metadata_cache = self._planning_metadata_cache()
            planned_jobs = _job_config._plan_file_source_jobs(
                dataset_manifest_path=self.dataset_manifest,
                file_list_dir=fl_dir,
                partition=self.partition,
                files_per_job=self.files_per_job,
                entries_per_job=self.entries_per_job,
                jobs_dir=self._jobs_dir,
                outputs_dir=self._outputs_dir,
                manifest=manifest,
                partition_profile=self._planning_profile(),
                input_metadata_cache=metadata_cache,
                tree_name=str(template_config.get("treeList", "Events")),
            )
            self.publish_message(
                f"PrepareSkimJobs: planned {len(planned_jobs)} job(s) in "
                f"{time.monotonic() - planning_start:.1f}s."
            )

            manifest_entries = {entry.name: entry for entry in manifest.datasets}

            def prepare_file_source_plan(plan: dict[str, object]) -> dict[str, str]:
                job_dir = str(plan["job_dir"])
                out_dir = str(plan["out_dir"])
                dataset_name = str(plan["dataset_name"])

                # Plans must retain manifest identity, even when a caller
                # supplies or caches the plan separately from this invocation.
                if dataset_name not in manifest_entries:
                    raise RuntimeError(f"Planned dataset is absent from the manifest: {dataset_name!r}")
                transient = DatasetEntry.from_dict(manifest_entries[dataset_name].to_dict())
                transient.files = [
                    f.strip()
                    for f in str(plan["files"]).split(",")
                    if f.strip()
                ]
                transient.das = None

                extra_overrides = dict(base_extra_overrides)
                if plan.get("planned_entries") is not None:
                    extra_overrides["plannedInputEntries"] = str(plan["planned_entries"])
                if int(plan.get("last_entry", 0)) > 0:
                    extra_overrides["firstEntry"] = str(plan["first_entry"])
                    extra_overrides["lastEntry"] = str(plan["last_entry"])
                    extra_overrides["entryRangeMode"] = self.entry_range_mode

                manifest_lumi = (
                    float(plan["lumi"])
                    if plan.get("lumi") is not None
                    else manifest.lumi_for(transient.year, transient.era)
                )
                return write_job_entry(
                    dataset=transient,
                    job_dir=job_dir,
                    out_dir=out_dir,
                    manifest_lumi=manifest_lumi,
                    extra_overrides=extra_overrides,
                    site_redirectors=plan.get("site_redirectors", {}),
                )

            job_entries = prepare_in_parallel(planned_jobs, prepare_file_source_plan)

        else:
            # Standard manifest mode: one job dir per dataset entry
            def prepare_manifest_entry(entry: DatasetEntry) -> dict[str, str]:
                job_dir = os.path.join(self._jobs_dir, entry.name)
                out_dir = os.path.join(self._outputs_dir, entry.name)
                return write_job_entry(
                    dataset=entry,
                    job_dir=job_dir,
                    out_dir=out_dir,
                    manifest_lumi=manifest.lumi_for(entry.year, entry.era),
                    extra_overrides=dict(base_extra_overrides),
                    site_redirectors={},
                )

            job_entries = prepare_in_parallel(
                manifest.datasets, prepare_manifest_entry
            )

        # ---- write the branch-map manifest --------------------------------
        Path(self._run_dir).mkdir(parents=True, exist_ok=True)
        with open(self.preparation_path, "w") as fh:
            json.dump(job_entries, fh, indent=2)
        if bundle_record:
            record_execution(Path(self._run_dir) / "execution_plan.json", execution_identity(
                bundle_record["bundle_id"], inputs=identity["inputs"],
                partitions=[{"dataset": entry["dataset_name"],
                             "config_sha256": file_digest(entry["config_path"])} for entry in job_entries],
                runtime=identity["runtime"], options=identity["options"]))

        self.publish_message(
            f"PrepareSkimJobs: {len(job_entries)} job dir(s) prepared.\n"
            f"  shared_inputs/ ready at {shared_dir}"
        )

        # ---- create local sample-type preflight configs --------------------
        if self.make_test_job:
            self._create_test_job(exe_path, job_entries)

        with open(self._prepared_config_hash_path, "w") as hash_fh:
            hash_fh.write(self._submit_config_hash() + "\n")

    def _create_test_job(self, exe_path: str, job_entries: list) -> None:
        """Create one local test configuration for each available data/MC type.

        Each test uses the first available file from one representative prepared
        job. Outputs are written under ``test_job/`` rather than to remote storage.

        Parameters
        ----------
        exe_path:
            Absolute path to the analysis executable.
        job_entries:
            List of ``{"dataset_name", "job_dir", "out_dir"}`` dicts produced by
            :meth:`_run_impl`.
        """
        test_dir = self._test_job_dir
        Path(test_dir).mkdir(parents=True, exist_ok=True)

        exe_relpath = self._exe_relpath

        # Keep one representative input job for every sample type that has a
        # prepared file. This checks both the data and MC configuration paths
        # when both are present in the manifest.
        candidates: dict[str, dict[str, Any]] = {}
        for entry in job_entries:
            jcfg_path = str(job_config_path(entry["job_dir"]))
            if os.path.exists(jcfg_path):
                jcfg = read_config(jcfg_path)
                file_list = jcfg.get("__orig_fileList", jcfg.get("fileList", ""))
                if not file_list:
                    continue
                dtype = str(jcfg.get("dtype", "")).strip().lower()
                if dtype not in {"data", "mc"}:
                    dtype = "other"
                candidates.setdefault(
                    dtype,
                    {
                        "entry": entry,
                        "config": jcfg,
                        "input": file_list.split(",")[0].strip(),
                    },
                )

        if not candidates:
            raise RuntimeError(
                "Cannot create the requested preflight: no prepared job has an input file."
            )

        test_types = [dtype for dtype in ("data", "mc") if dtype in candidates]
        if not test_types:
            test_types = [next(iter(candidates))]
        test_jobs = []

        # Copy the analysis executable into the test job dir
        dst_exe = os.path.join(test_dir, exe_relpath)
        if not _job_staging._staged_copy_is_current(exe_path, dst_exe):
            shutil.copy2(exe_path, dst_exe)

        # Copy local shared libraries
        local_libs, _ = _job_staging._local_shared_libs_with_signature(exe_path)
        for lib_name, lib_src in sorted(local_libs.items()):
            lib_dst = os.path.join(test_dir, lib_name)
            if not _job_staging._staged_copy_is_current(lib_src, lib_dst):
                shutil.copy2(lib_src, lib_dst)

        # Copy auxiliary config files referenced in the template
        template_config: dict[str, str] = {}
        if os.path.isfile(self.submit_config):
            template_config = read_config(self.submit_config)
            _job_staging._copy_analysis_config_files(self.submit_config, template_config, test_dir)
            # Keep a cfg/ copy as well as the loose files above. Prepared
            # file-source jobs already refer to staged payloads as cfg/foo;
            # this makes the local preflight layout match the worker archive.
            _job_staging._ensure_local_analysis_runtime_layout(
                self.submit_config,
                template_config,
                test_dir,
                include_aux=False,
            )
            _job_staging._copy_analysis_aux_dir(self.submit_config, test_dir)

        for dtype in test_types:
            candidate = candidates[dtype]
            job_dir = candidate["entry"]["job_dir"]
            test_config = dict(template_config)
            test_config.update(candidate["config"])
            # A produced correction is outside the sealed template bundle.
            # Resolve the original source before staging the test's own cfg/.
            test_config.update(_job_config._stitch_job_overrides(self))
            _job_staging._remove_mc_generator_config_for_data(test_config)
            test_config["fileList"] = _job_config._select_test_input_url(
                candidate["input"], job_dir
            )
            test_config["saveFile"] = f"test_output_{dtype}.root"
            test_config["metaFile"] = f"test_output_{dtype}_meta.root"
            test_config["saveDirectory"] = test_dir
            test_config["threads"] = "1"
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

            for key, stem in (("floatConfig", "floats"), ("intConfig", "ints")):
                original = candidate["config"].get(key)
                if original and (Path(job_dir) / original).is_file():
                    local = Path(test_dir) / f"{stem}_{dtype}.yaml"
                    _job_config._write_config_file(local, read_config(Path(job_dir) / original))
                    test_config[key] = local.name

            config_name = f"submit_config_{dtype}.yaml"
            config_path = os.path.join(test_dir, config_name)
            _job_config._write_config_file(config_path, test_config)
            test_jobs.append(
                {
                    "dtype": dtype,
                    "dataset": str(candidate["entry"].get("dataset_name", "")),
                    "config": config_name,
                    "input": test_config["fileList"],
                    "config_sha256": hashlib.sha256(
                        Path(config_path).read_bytes()
                    ).hexdigest(),
                }
            )

        # Keep the historic config filename for manual inspection and older
        # tooling; RunSkimTestJob uses the explicit per-type manifest above.
        primary_config = os.path.join(test_dir, test_jobs[0]["config"])
        shutil.copy2(primary_config, os.path.join(test_dir, "submit_config.yaml"))
        with open(self._test_jobs_manifest_path, "w", encoding="utf-8") as manifest_fh:
            json.dump({"schema_version": 1, "jobs": test_jobs}, manifest_fh, indent=2)

        self.publish_message(
            f"[PrepareSkimJobs] Test jobs created in {test_dir}/ for "
            f"{', '.join(test_types)}.\n"
            f"  Inputs: "
            + "; ".join(f"{job['dataset']} ({job['dtype']}): {job['input']}" for job in test_jobs)
        )
