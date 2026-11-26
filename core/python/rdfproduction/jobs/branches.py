"""Execute and verify prepared local or Dask partitions."""

from __future__ import annotations

from native_config_io import job_config_path
from pathlib import Path
from typing import cast
import json
import os
import shutil
import tempfile

from dataset_manifest import DatasetEntry
from dataset_manifest import DatasetManifest
from output_schema import ArtifactResolutionStatus
from output_schema import check_cache_validity
from output_schema import write_cache_sidecar
from ..performance import PerformanceRecorder
from ..performance import perf_path_for
from submission_backend import read_config
from .paths import SkimPaths
from . import provenance as _job_provenance
from . import staging as _job_staging
from . import job_config as _job_config
import rdfproduction.executors as _executors
from . import output_validation as _job_output_validation


class BranchOperations(SkimPaths):
    def _fallback_branch_entries(self) -> list[dict[str, str]]:
        """Return deterministic branch metadata before prep_submission.json exists."""
        if self.file_source:
            fl_dir = self._file_list_dir()
            if os.path.isdir(fl_dir):
                # Workflow completion can request this preview before upstream
                # preparation runs. Never freeze/consult mutable learned history
                # here. The branch map resets to authoritative prepared entries.
                selection = self._sizing_selection()
                profile = selection.get("profile", self.partition_profile)
                if profile == "none":
                    profile = ""
                if self.partition == "auto" and str(read_config(self.submit_config).get("threads", "1")) != "1":
                    profile = ""
                seed = os.path.join(self._run_dir, "input_metadata.json")
                if not os.path.isfile(seed):
                    seed = selection.get("metadata_seed", self.input_metadata_cache)
                # The preview may query missing counts for explicit entry modes;
                # its scratch cache must not mutate an input seed or prepared run.
                with tempfile.TemporaryDirectory(prefix="rdf_sizing_preview_") as scratch:
                    metadata = os.path.join(scratch, "entries.json")
                    if seed:
                        shutil.copy2(seed, metadata)
                    plans = _job_config._plan_file_source_jobs(
                        dataset_manifest_path=self.dataset_manifest,
                        file_list_dir=fl_dir, partition=self.partition,
                        files_per_job=self.files_per_job, entries_per_job=self.entries_per_job,
                        jobs_dir=self._jobs_dir, outputs_dir=self._outputs_dir,
                        partition_profile=profile, input_metadata_cache=metadata,
                        tree_name=str(read_config(self.submit_config).get("treeList", "Events")))
                return [
                    {
                        "dataset_name": str(entry["dataset_name"]),
                        "job_dir": str(entry["job_dir"]),
                        "config_path": str(job_config_path(str(entry["job_dir"]))),
                        "out_dir": str(entry["out_dir"]),
                    }
                    for entry in plans
                ]

            manifest = DatasetManifest.load(self.dataset_manifest)
            return [
                {
                    "dataset_name": entry.name,
                    "job_dir": os.path.join(self._jobs_dir, entry.name, "job_0"),
                    "config_path": str(job_config_path(os.path.join(self._jobs_dir, entry.name, "job_0"))),
                    "out_dir": os.path.join(self._outputs_dir, entry.name, "job_0"),
                }
                for entry in manifest.datasets
            ]

        manifest = DatasetManifest.load(self.dataset_manifest)
        return [
            {
                "dataset_name": entry.name,
                "job_dir": os.path.join(self._jobs_dir, entry.name),
                "config_path": str(job_config_path(os.path.join(self._jobs_dir, entry.name))),
                "out_dir": os.path.join(self._outputs_dir, entry.name),
            }
            for entry in manifest.datasets
        ]

    def _prepared_branch_entries(self) -> list[dict[str, str]]:
        """Return branch metadata from PrepareSkimJobs, or a deterministic fallback."""
        prep_json = os.path.join(self._run_dir, "prep_submission.json")
        if os.path.isfile(prep_json):
            with open(prep_json) as fh:
                payload = json.load(fh)
            if isinstance(payload, list):
                return [
                    {
                        "dataset_name": str(entry["dataset_name"]),
                        "job_dir": str(entry["job_dir"]),
                        "config_path": str(entry.get("config_path", str(job_config_path(str(entry["job_dir"]))))),
                        "out_dir": str(entry["out_dir"]),
                    }
                    for entry in payload
                    if isinstance(entry, dict)
                ]
        return self._fallback_branch_entries()

    def branch_output_path(self):
        data = self.branch_data
        if isinstance(data, DatasetEntry):
            name = data.name
        else:
            name = str(data["dataset_name"])
        suffix = f"_b{self.branch}" if self.file_source and not isinstance(data, DatasetEntry) else ""
        return Path(self._job_outputs_dir) / f"{name}{suffix}.done"

    def branch_complete(self):
        """Require the done marker and the complete set of readable ROOT outputs.

        Cache validity is checked only in standard manifest mode (one job per
        dataset). File-source branches still require their own skim and
        metadata ROOT outputs, but do not write a single-dataset cache sidecar.
        """
        if not self.branch_output_path().is_file():
            return False
        branch_data = self.branch_data
        if isinstance(branch_data, DatasetEntry):
            dataset = branch_data
            dataset_name = dataset.name
            out_dir = os.path.join(self._outputs_dir, dataset.name)
        else:
            branch_entry = cast(dict[str, str], branch_data)
            dataset_name = str(branch_entry["dataset_name"])
            out_dir = str(branch_entry["out_dir"])
            dataset = _job_staging._find_dataset_entry(self.dataset_manifest, dataset_name)
            if dataset is None:
                return False

        skim_file = os.path.join(out_dir, "skim.root")
        meta_file = os.path.join(out_dir, "meta.root")
        config_path = (
            str(branch_data.get("config_path", ""))
            if isinstance(branch_data, dict)
            else ""
        )
        config = read_config(config_path) if config_path and os.path.isfile(config_path) else {}
        tree_name = str(config.get("saveTree", "Events") or "Events")
        require_skim = str(config.get("enableSkim", "")).strip().lower() in {"1", "true", "yes", "on"}
        output_errors = _job_output_validation._skim_job_output_errors(
            dataset_name=dataset_name,
            dtype=dataset.dtype,
            save_path=skim_file,
            meta_path=meta_file,
            tree_name=tree_name,
            require_skim=require_skim,
        )
        if output_errors:
            done_path = str(self.branch_output_path())
            if os.path.exists(done_path):
                os.remove(done_path)
            self.publish_message(
                f"[SkimTask] Outputs for '{dataset_name}' are incomplete: "
                + "; ".join(output_errors)
            )
            return False

        if self.file_source or not require_skim:
            return True

        effective_config_path = config_path or str(job_config_path(os.path.join(self._jobs_dir, dataset.name)))
        current_prov = _job_provenance._build_task_provenance(
            submit_config_path=effective_config_path,
            dataset_manifest_path=self.dataset_manifest,
            extra_input_paths=[self.stitch_weights] if self.stitch_weights else None,
        )
        status = check_cache_validity(
            skim_file, current_provenance=current_prov, strict=True
        )

        if status == ArtifactResolutionStatus.COMPATIBLE:
            return True

        done_path = str(self.branch_output_path())
        if os.path.exists(done_path):
            os.remove(done_path)
        self.publish_message(
            f"[SkimTask] Cache invalidated for '{dataset_name}' "
            f"(status={status.value}): the .done marker has been removed "
            "and the artifact will be regenerated."
        )
        return False

    def run_branch(self):
        branch_data = self.branch_data

        if isinstance(branch_data, DatasetEntry):
            dataset = branch_data
            exe_path = os.path.abspath(self.exe)
            if not os.path.isfile(exe_path):
                raise RuntimeError(
                    f"Analysis executable not found: {exe_path!r}. "
                    "Build the analysis before running SkimTask."
                )
            if not dataset.files and not dataset.das:
                raise RuntimeError(
                    f"Dataset '{dataset.name}' has no files or DAS path defined "
                    "in the dataset manifest."
                )

            job_dir = os.path.join(self._jobs_dir, dataset.name)
            Path(job_dir).mkdir(parents=True, exist_ok=True)
            out_dir = os.path.join(self._outputs_dir, dataset.name)
            _job_config._write_job_config(
                job_dir=job_dir,
                template_config_path=self.submit_config,
                dataset=dataset,
                output_dir=out_dir,
                manifest_lumi=_job_staging._manifest_lumi_for_dataset(self.dataset_manifest, dataset),
                extra_overrides=_job_config._stitch_job_overrides(self),
            )

            task_label = f"SkimTask[{dataset.name}]"
            with PerformanceRecorder(task_label) as rec:
                result = _executors._run_analysis_job(
                    exe_path=exe_path,
                    job_dir=job_dir,
                    stream_output=_job_staging._stream_analysis_output(),
                    worker_number=self.branch,
                )

            Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
            rec.save(perf_path_for(str(self.branch_output_path())))

            skim_file = os.path.join(out_dir, "skim.root")
            if os.path.isfile(skim_file):
                mf = _job_provenance._build_skim_manifest(
                    dataset=dataset,
                    submit_config_path=str(job_config_path(job_dir)),
                    dataset_manifest_path=self.dataset_manifest,
                    skim_output_file=skim_file,
                    extra_input_paths=[self.stitch_weights] if self.stitch_weights else None,
                )
                write_cache_sidecar(skim_file, mf)
                self.publish_message(
                    f"[SkimTask] Cache sidecar written for '{dataset.name}': "
                    f"{skim_file}"
                )

            self.publish_message(f"Skim done for '{dataset.name}': {result}")
            with self.branch_output_path().open("w") as fh:
                fh.write(f"status=done\ndataset={dataset.name}\n{result}\n")

        else:
            branch_entry = cast(dict[str, str], branch_data)
            dataset_name = str(branch_entry["dataset_name"])
            job_dir = str(branch_entry["job_dir"])
            config_path = str(branch_entry.get("config_path", str(job_config_path(job_dir))))
            out_dir = str(branch_entry["out_dir"])

            if not os.path.isdir(self._shared_dir):
                raise RuntimeError(
                    f"Shared inputs directory not found: {self._shared_dir!r}.\n"
                    "Ensure PrepareSkimJobs has completed successfully."
                )
            if not os.path.isdir(job_dir):
                raise RuntimeError(
                    f"Job directory not found: {job_dir!r}.\n"
                    "Ensure PrepareSkimJobs has completed successfully."
                )

            if not self.file_source:
                dataset = _job_staging._find_dataset_entry(self.dataset_manifest, dataset_name)
                if dataset is not None and not dataset.files and not dataset.das:
                    raise RuntimeError(
                        f"Dataset '{dataset_name}' has no files or DAS path defined "
                        "in the dataset manifest."
                    )

            task_label = f"SkimTask[{dataset_name}/b{self.branch}]"
            with PerformanceRecorder(task_label) as rec:
                result = _job_staging._run_prepared_skim_job(
                    shared_dir=self._shared_dir,
                    source_job_dir=job_dir,
                    exe_relpath=self._exe_relpath,
                    root_setup=self._root_setup_content,
                    container_setup=self.container_setup or "",
                    source_config_path=config_path,
                    shared_archive_path=self._shared_archive_path,
                    stream_output=_job_staging._stream_analysis_output(),
                    worker_number=self.branch,
                    python_env_name=os.path.basename(self._python_env_path() or ""),
                )

            Path(self._job_outputs_dir).mkdir(parents=True, exist_ok=True)
            rec.save(perf_path_for(str(self.branch_output_path())))

            if not self.file_source:
                dataset = _job_staging._find_dataset_entry(self.dataset_manifest, dataset_name)
                skim_file = os.path.join(out_dir, "skim.root")
                if dataset is not None and os.path.isfile(skim_file):
                    mf = _job_provenance._build_skim_manifest(
                        dataset=dataset,
                        submit_config_path=config_path,
                        dataset_manifest_path=self.dataset_manifest,
                        skim_output_file=skim_file,
                        extra_input_paths=[self.stitch_weights] if self.stitch_weights else None,
                    )
                    write_cache_sidecar(skim_file, mf)
                    self.publish_message(
                        f"[SkimTask] Cache sidecar written for '{dataset_name}': "
                        f"{skim_file}"
                    )

            self.publish_message(
                f"Skim done for '{dataset_name}' branch {self.branch}: {result}"
            )
            with self.branch_output_path().open("w") as fh:
                fh.write(
                    f"status=done\ndataset={dataset_name}\n"
                    f"branch={self.branch}\n{result}\n"
                )

    def get_dask_work(self, branch_num: int, branch_data) -> tuple:
        """Return the (callable, args, kwargs) triple for Dask dispatch.

        Dict-style branch data runs via the same prepared-job sandbox used by
        local and HTCondor execution.  DatasetEntry support is retained for
        unit tests that patch branch_data directly.
        """
        if isinstance(branch_data, DatasetEntry):
            exe_path = os.path.abspath(self.exe)
            job_dir = os.path.join(self._jobs_dir, branch_data.name)
            config_path = str(job_config_path(job_dir))
            config = read_config(config_path) if os.path.isfile(config_path) else {}
            out_dir = os.path.join(self._outputs_dir, branch_data.name)
            return (
                _job_output_validation._run_skim_job_with_output_contract,
                [
                    _executors._run_analysis_job,
                    [exe_path, job_dir, self._root_setup_content,
                     self.container_setup or ""],
                    {"worker_number": branch_num},
                    self.dataset_manifest,
                    config_path,
                    branch_data.name,
                    os.path.join(out_dir, "skim.root"),
                    os.path.join(out_dir, "meta.root"),
                    str(config.get("saveTree", "Events") or "Events"),
                    True,
                    [self.stitch_weights] if self.stitch_weights else None,
                ],
                {},
            )
        branch_entry = cast(dict[str, str], branch_data)
        config_path = str(
            branch_entry.get(
                "config_path",
                str(job_config_path(str(branch_entry["job_dir"]))),
            )
        )
        config = read_config(config_path) if os.path.isfile(config_path) else {}
        out_dir = str(branch_entry["out_dir"])
        return (
            _job_output_validation._run_skim_job_with_output_contract,
            [
                _job_staging._run_prepared_skim_job,
                [
                    self._shared_dir,
                    str(branch_entry["job_dir"]),
                    self._exe_relpath,
                    self._root_setup_content,
                    self.container_setup or "",
                    config_path,
                    self._shared_archive_path,
                ],
                {"worker_number": branch_num, "python_env_name": os.path.basename(self._python_env_path() or "")},
                self.dataset_manifest,
                config_path,
                str(branch_entry["dataset_name"]),
                os.path.join(out_dir, "skim.root"),
                os.path.join(out_dir, "meta.root"),
                str(config.get("saveTree", "Events") or "Events"),
                not bool(self.file_source),
                [self.stitch_weights] if self.stitch_weights else None,
            ],
            {},
        )
