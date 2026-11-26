"""Build per-dataset Condor submit files and publish submission receipts."""

from __future__ import annotations

from native_config_io import job_config_path
from pathlib import Path
import json
import os
import shutil

from ..performance import PerformanceRecorder
from ..performance import perf_path_for
from submission_backend import read_config
from submission_backend import write_submit_files
from .paths import SkimPaths
from . import staging as _job_staging
from . import condor_files as _job_condor_files
from . import job_config as _job_config
import rdfproduction.condor_submission as _condor_submission


class SubmissionOperations(SkimPaths):
    @property
    def submission_manifest_path(self):
        return Path(self._run_dir) / "submission_manifest.json"

    @property
    def submitted_path(self):
        return Path(self._run_dir) / "submitted.json"

    def build_submission(self):
        with PerformanceRecorder("BuildSkimSubmission") as rec:
            self._build_submission_impl()
        rec.save(os.path.join(self._run_dir, "build_submission.perf.json"))

    def _build_submission_impl(self) -> None:
        prep_manifest = os.path.join(self._run_dir, "prep_submission.json")
        if not os.path.isfile(prep_manifest):
            raise RuntimeError(
                f"Prepared skim manifest not found: {prep_manifest!r}. "
                "Run PrepareSkimJobs first."
            )

        with open(prep_manifest) as fh:
            payload = json.load(fh)
        if not isinstance(payload, list) or not payload:
            raise RuntimeError("No prepared skim jobs found in prep_submission.json.")

        grouped_jobs = _job_condor_files._group_prepared_skim_jobs_by_dataset(payload)
        if not grouped_jobs:
            raise RuntimeError("No prepared skim job directories were discovered.")

        template_config = read_config(self.submit_config)
        final_output_base = _job_config._resolve_output_destination_base(
            self.submit_config, template_config, self.output_directory
        )
        bookkeeping_outputs_dir = os.path.join(self._run_dir, "outputs")

        Path(self._run_dir).mkdir(parents=True, exist_ok=True)

        shared_dir = self._shared_dir
        if not os.path.isdir(shared_dir):
            raise RuntimeError(
                f"Shared inputs directory not found: {shared_dir!r}. "
                "Run PrepareSkimJobs first."
            )

        _job_staging._validate_staged_skim_executable(shared_dir, self.exe)
        python_env_staged = None
        python_env_src = self._python_env_path()
        if python_env_src:
            python_env_staged = os.path.join(shared_dir, os.path.basename(python_env_src))
            if not _job_staging._staged_copy_is_current(python_env_src, python_env_staged):
                raise RuntimeError(
                    f"Staged Python environment is missing or out of date: {python_env_staged!r}. "
                    "Run PrepareSkimJobs first."
                )

        stitch_weights_path = str(getattr(self, "stitch_weights", "") or "").strip()
        if stitch_weights_path and not os.path.isfile(stitch_weights_path):
            raise RuntimeError(
                f"Stitching weights file not found: {stitch_weights_path!r}"
            )
        if stitch_weights_path:
            # Stitch weights are identical for every job.  Add them to the
            # run-level archive instead of copying one JSON into each job.
            shutil.copy2(
                stitch_weights_path,
                os.path.join(shared_dir, "stitch_weights.json"),
            )
            _job_staging._build_shared_inputs_archive(shared_dir, self._shared_archive_path)

        shared_transfer_files = _job_staging._skim_shared_input_files(shared_dir)
        _job_staging._validate_staged_skim_proxy(shared_dir, required=bool(self.x509))
        shared_archive_file_names = _job_staging._shared_archive_top_level_files(
            self._shared_archive_path
        )
        if (python_env_staged and os.path.isfile(self._shared_archive_path)
                and os.path.basename(python_env_staged) not in shared_archive_file_names):
            raise RuntimeError(
                f"Shared inputs archive is missing the Python environment: {python_env_staged!r}. "
                "Run PrepareSkimJobs first."
            )

        submission_records: list[dict[str, object]] = []
        for dataset_name, dataset_jobs in grouped_jobs.items():
            submission_dir = _job_condor_files._dataset_submission_dir(self._run_dir, dataset_name)
            if os.path.exists(submission_dir):
                shutil.rmtree(submission_dir)
            Path(submission_dir).mkdir(parents=True, exist_ok=True)

            jobs_payload: list[dict[str, object]] = []
            for local_index, (global_index, entry) in enumerate(dataset_jobs):
                job_dir_abs = str(entry["job_dir"])
                staged_job_dir = os.path.join(submission_dir, f"job_{local_index}")
                Path(staged_job_dir).mkdir(parents=True, exist_ok=True)
                _job_staging._copy_job_submission_files(
                    job_dir_abs,
                    staged_job_dir,
                    shared_archive_file_names,
                )
                if stitch_weights_path:
                    staged_config_path = str(job_config_path(staged_job_dir))
                    staged_config = read_config(staged_config_path)
                    staged_config["stitchWeights"] = "stitch_weights.json"
                    _job_config._write_config_file(staged_config_path, staged_config)
                if final_output_base and entry.get("out_dir"):
                    final_output_dir = os.path.join(final_output_base, dataset_name)
                    _job_config._rewrite_job_output_destinations(
                        str(job_config_path(staged_job_dir)),
                        final_output_dir,
                    )

                jobs_payload.append({
                    "job_index": local_index,
                    "global_index": global_index,
                    "source_job_dir": job_dir_abs,
                    "job_dir": staged_job_dir,
                    "out_dir": str(entry.get("out_dir", "")),
                })

            submission_transfer_files = list(shared_transfer_files)

            config_names = {job_config_path(job["job_dir"]).name for job in jobs_payload}
            if len(config_names) != 1:
                raise ValueError(f"Prepared jobs for {dataset_name} mix worker config filenames; regenerate this dataset preparation")
            config_name = config_names.pop()
            submit_path = write_submit_files(
                submission_dir,
                len(dataset_jobs),
                self._exe_relpath,
                stage_inputs=self.stage_in,
                stage_outputs=True,
                root_setup=self._root_setup_content,
                x509loc=None,
                max_runtime=str(self.max_runtime),
                request_memory=self.request_memory,
                request_cpus=1,
                request_disk=20000,
                extra_transfer_files=submission_transfer_files,
                include_aux=False,
                shared_dir_name=None,
                eos_sched=False,
                config_file=config_name,
                container_setup=self.container_setup,
                want_os=self.want_os,
                python_env_tarball=python_env_staged,
                shared_archive_name=os.path.basename(self._shared_archive_path),
                runtime_config_relpath=os.path.join("cfg", config_name),
                sample_name=dataset_name,
                run_name=self.name,
                sizing_pilot_entries=self.sizing_pilot_prefix_entries if self.sizing_pilot_child else 0,
            )
            _job_staging._rewrite_submit_transfer_files(
                submit_path,
                banned_exact_paths=[os.path.join(submission_dir, "shared_inputs")],
            )

            submission_records.append({
                "dataset_name": dataset_name,
                "submission_dir": submission_dir,
                "submit_path": submit_path,
                "runscript_path": os.path.join(submission_dir, "condor_runscript.sh"),
                "job_count": len(dataset_jobs),
                "shared_transfer_files": list(shared_transfer_files),
                "jobs": jobs_payload,
            })

        with self.submission_manifest_path.open("w") as fh:
            json.dump({"datasets": submission_records}, fh, indent=2)

        self.publish_message(
            f"Skim condor submission ready.\n"
            f"  {len(payload)} job(s) prepared across {len(submission_records)} dataset submission(s).\n"
            f"  Submission manifest: {self.submission_manifest_path}"
        )

    def submit(self, manifest_path=None):
        with PerformanceRecorder("SubmitSkimJobs") as rec:
            def validate_worker():
                _job_staging._validate_staged_skim_proxy(self._shared_dir, required=bool(self.x509))
                _job_staging._validate_staged_skim_executable(self._shared_dir, self.exe)

            _condor_submission.submit_skim_batches(
                manifest_path or self.submission_manifest_path,
                self.submitted_path,
                validate=validate_worker,
                publish=self.publish_message,
            )

        rec.save(perf_path_for(str(self.submitted_path)))
