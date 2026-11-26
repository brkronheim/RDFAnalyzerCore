"""Monitor Condor state, verify stage-out, and retry or split failed jobs."""

from __future__ import annotations

from native_config_io import job_config_path
import fcntl
import json
import os
from pathlib import Path
import tempfile
import time
from typing import Optional

from dataset_manifest import DatasetManifest
import io_policy as _io_policy
import job_sizing as _job_sizing
from job_sizing import atomic_json
from run_bundle import file_digest
from submission_backend import read_config
import xrd_site_rankings as _xrd_site_rankings

from .. import transport as _transport
from . import condor_files as _job_condor_files
from . import output_validation as _job_output_validation
from . import staging as _job_staging
from .submit import SubmissionOperations


class MonitorOperations(SubmissionOperations):
    @property
    def monitor_path(self):
        return Path(self._run_dir) / "all_outputs_verified.txt"

    def monitoring_complete(self, *, allow_partial=False):
        """Require completed jobs and valid output artifacts under the selected policy."""
        if not self.monitor_path.is_file():
            return False

        try:
            summary: dict[str, int] = {}
            with open(self.monitor_path) as fh:
                for line in fh:
                    key, separator, value = line.strip().partition("=")
                    if separator and key in {"done", "perm_fail", "total"}:
                        summary[key] = int(value)
            total = summary.get("total", 0)
            failed = summary.get("perm_fail", 0)
            if (
                total <= 0
                or {"done", "perm_fail", "total"} - summary.keys()
                or summary.get("done", 0) + failed != total
                or (failed and not allow_partial)
            ):
                return False

            with open(self._state_file) as fh:
                state = json.load(fh)

            dataset_states = state.get("datasets")
            if not isinstance(dataset_states, dict) or not dataset_states:
                return False

            manifest = DatasetManifest.load(self.dataset_manifest)
            checked_jobs = 0
            failed_jobs = 0
            jobs_to_check = {}
            job_states = {}
            for dataset_name, dataset_state in dataset_states.items():
                dataset = manifest.by_name(dataset_name)
                if dataset is None:
                    return False
                jobs = dataset_state.get("jobs")
                if not isinstance(jobs, dict) or not jobs:
                    return False
                if not any(job.get("status") == "done" for job in jobs.values()):
                    return False
                for index, job_state in jobs.items():
                    if (allow_partial and job_state.get("status") == "perm_fail"
                            and int(job_state.get("retries", 0)) >= self.max_retries):
                        failed_jobs += 1
                        continue
                    if not isinstance(job_state, dict) or job_state.get("status") != "done":
                        return False
                    job_dir = job_state.get("job_dir")
                    job_config = str(job_config_path(job_dir)) if job_dir else self.submit_config
                    require_skim = (
                        _job_output_validation._config_enables_skim(job_config)
                        if os.path.isfile(job_config)
                        else _job_output_validation._config_enables_skim(self.submit_config)
                    )
                    output_roles = ("save", "meta") if require_skim else ("meta",)
                    job_output_paths = [
                        str(job_state.get(role, "")) for role in output_roles
                    ]
                    if any(not path for path in job_output_paths):
                        return False
                    key = (dataset_name, index)
                    jobs_to_check[key] = (dict(dir=job_state.get("job_dir", ""),
                        save=job_state.get("save", ""), meta=job_state.get("meta", ""),
                        tree_name=job_state.get("tree_name", "Events")), output_roles)
                    job_states[key] = job_state
                    checked_jobs += 1

            if failed_jobs != failed or checked_jobs + failed_jobs != total:
                return False
            return not any(self._validate_output_jobs(
                jobs_to_check, job_states, manifest).values())
        except Exception:  # completion checks fail closed on corrupt persisted records
            return False

    @property
    def _validation_cache_path(self) -> Path:
        return Path(self._run_dir) / "output_validation.json"

    def _read_validation_cache(self) -> dict:
        try:
            cache = json.loads(self._validation_cache_path.read_text())
            if cache.get("schema_version") == 1 and isinstance(cache.get("jobs"), dict):
                return cache["jobs"]
        except (OSError, ValueError, AttributeError):
            pass
        return {}

    def _save_validation_updates(self, updates: dict) -> None:
        """Merge receipts atomically without rewriting the monitor's job state."""
        if not updates:
            return
        path = self._validation_cache_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_suffix(".lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX)
            current = self._read_validation_cache()
            for key, receipt in updates.items():
                if receipt is None:
                    current.pop(key, None)
                else:
                    current[key] = receipt
            atomic_json(path, dict(schema_version=1, jobs=current))

    def _validate_output_jobs(self, jobs: dict, job_states: dict,
                              manifest: DatasetManifest) -> dict:
        """Share fresh batched path checks and persistent integrity receipts.

        Receipts are only valid for the same job attempt, configuration, output
        paths and fresh storage signatures. Old done states alone are not proof
        of integrity. Existence-only checks never populate this cache.
        """
        paths = list(dict.fromkeys(str(info.get(role, ""))
            for info, roles in jobs.values() for role in roles if info.get(role)))
        signatures = {}
        if self.output_validation == "integrity":
            signatures = _job_output_validation._output_file_info_batch(paths)
            exists = {path: info is not None for path, info in signatures.items()}
        else:
            exists = _job_output_validation._output_files_exist_batch(paths)
        cache = self._read_validation_cache() if signatures else {}
        updates = {}
        errors_by_job = {}
        last_checkpoint = time.monotonic()
        checked = reused = 0
        cache_writable = True
        def checkpoint():
            nonlocal cache_writable
            if cache_writable:
                try:
                    self._save_validation_updates(updates)
                except OSError as error:
                    cache_writable = False
                    self.publish_message(f"WARNING: output validation receipts could not be "
                        f"saved; full validation results remain valid: {error}")
            updates.clear()
        manifest_sha = file_digest(self.dataset_manifest) if signatures else ""
        identity = Path(self._run_dir) / "execution_identity.json"
        execution_sha = file_digest(identity) if signatures and identity.is_file() else ""
        self.publish_message(f"Checking {len(jobs)} output pair(s): batched storage metadata, "
                             "then uncached integrity checks.")
        try:
            for key, (info, roles) in jobs.items():
                dataset_name, index = key
                dataset = manifest.by_name(dataset_name)
                errors = []
                job_paths = [str(info.get(role, "")) for role in roles]
                for role, path in zip(roles, job_paths):
                    if not path:
                        errors.append(f"{role} output path is not configured")
                    elif not exists.get(path, False):
                        errors.append(f"{role} output {path!r} is absent or unreadable")
                if dataset is None:
                    errors.append(f"dataset {dataset_name!r} is missing from the dataset manifest")
                cache_key = json.dumps([dataset_name, str(index)])
                if not errors and self.output_validation == "integrity":
                    state = job_states[key]
                    job_dir = info.get("dir")
                    config = job_config_path(job_dir) if job_dir else Path(self.submit_config)
                    if not config.is_file():
                        config = Path(self.submit_config)
                    contract = dict(version=1, dataset=dataset_name, dtype=dataset.dtype,
                        tree_name=info.get("tree_name", "Events"), roles=list(roles),
                        config_sha256=file_digest(config) if config.is_file() else None,
                        manifest_sha256=manifest_sha, execution_sha256=execution_sha,
                        cluster=state.get("cluster"), proc=state.get("proc"),
                        retries=state.get("retries", 0))
                    files = {path: signatures.get(path) for path in job_paths}
                    receipt = dict(contract=contract, files=files)
                    cacheable = all(files.values())
                    if cacheable and cache.get(cache_key) == receipt:
                        reused += 1
                    else:
                        errors.extend(_job_output_validation._skim_job_output_errors(dataset_name=dataset_name,
                            dtype=dataset.dtype, save_path=str(info.get("save", "")),
                            meta_path=str(info.get("meta", "")),
                            tree_name=str(info.get("tree_name", "Events")),
                            require_skim="save" in roles))
                        checked += 1
                        # Unknown metadata (e.g. stat fallback) cannot establish
                        # file identity, even after successful full validation.
                        updates[cache_key] = receipt if not errors and cacheable else None
                elif errors and self.output_validation == "integrity":
                    updates[cache_key] = None
                errors_by_job[key] = errors
                now = time.monotonic()
                if updates and (len(updates) >= 25 or now - last_checkpoint >= 10):
                    checkpoint()
                    self.publish_message(f"Output validation: {len(errors_by_job)}/{len(jobs)} "
                        f"checked, {checked} ROOT checks, {reused} receipts reused.")
                    last_checkpoint = now
        finally:
            checkpoint()
        self.publish_message(f"Output validation finished: {len(jobs)} checked, "
                             f"{checked} ROOT checks, {reused} receipts reused.")
        return errors_by_job

    @property
    def _state_file(self) -> str:
        return os.path.join(self._run_dir, "monitor_state.json")

    def _load_state(self) -> dict:
        if not os.path.exists(self._state_file):
            return {"datasets": {}}
        try:
            with open(self._state_file) as fh:
                state = json.load(fh)
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Cannot read monitor state {self._state_file!r}; preserve and "
                "inspect the file before resuming to avoid losing retry history."
            ) from exc
        if not isinstance(state, dict) or not isinstance(state.get("datasets"), dict):
            raise RuntimeError(
                f"Monitor state {self._state_file!r} has an invalid structure; "
                "expected a JSON object containing a datasets object."
            )
        return state

    def _save_state(self, state: dict) -> None:
        parent = os.path.dirname(self._state_file)
        Path(parent).mkdir(parents=True, exist_ok=True)
        fd, temporary_path = tempfile.mkstemp(
            prefix=".monitor_state.", suffix=".tmp", dir=parent
        )
        try:
            with os.fdopen(fd, "w") as fh:
                json.dump(state, fh, indent=2)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(temporary_path, self._state_file)
        except Exception:
            try:
                os.unlink(temporary_path)
            except OSError:
                pass
            raise

    def _discover_jobs(self, submission_dir: str) -> dict[int, dict[str, str]]:
        return _job_condor_files._discover_jobs_in_submission_dir(submission_dir)

    def _status_summary(self, state: dict) -> dict[str, int]:
        counts: dict[str, int] = {}
        for dataset_state in state.get("datasets", {}).values():
            for job_state in dataset_state.get("jobs", {}).values():
                status = job_state["status"]
                counts[status] = counts.get(status, 0) + 1
        return counts

    def _reactivate_retryable_failures(self, state: dict) -> int:
        """Make prior permanent failures retryable when the retry budget grew."""
        reactivated = 0
        for dataset_state in state.get("datasets", {}).values():
            for job_state in dataset_state.get("jobs", {}).values():
                if job_state.get("status") != "perm_fail":
                    continue
                retries = int(job_state.get("retries", 0))
                if retries < self.max_retries:
                    job_state["status"] = "submitted"
                    reactivated += 1
        return reactivated

    def monitor(self, submitted_payload=None):
        if submitted_payload is None:
            with self.submitted_path.open("r") as fh:
                submitted_payload = json.load(fh)

        dataset_records = list(submitted_payload.get("datasets", []))
        if not dataset_records:
            raise RuntimeError("No submitted dataset clusters found in submitted.json")
        manifest = DatasetManifest.load(self.dataset_manifest)

        state = self._load_state()
        dataset_job_info: dict[str, dict[int, dict[str, str]]] = {}
        for record in dataset_records:
            dataset_name = str(record["dataset_name"])
            submission_dir = str(record["submission_dir"])
            initial_cluster = str(record.get("cluster_id", ""))
            if not initial_cluster:
                raise RuntimeError(f"No cluster_id found for dataset {dataset_name}")

            job_info = self._discover_jobs(submission_dir)
            if not job_info:
                raise RuntimeError(
                    f"No job_N directories found in {submission_dir}. "
                    "Run BuildSkimSubmission first."
                )

            dataset_job_info[dataset_name] = job_info
            dataset_state = state.setdefault("datasets", {}).setdefault(dataset_name, {
                "submission_dir": submission_dir,
                "shared_transfer_files": list(record.get("shared_transfer_files", [])),
                "jobs": {},
            })
            dataset_state["submission_dir"] = submission_dir
            dataset_state["shared_transfer_files"] = list(record.get("shared_transfer_files", []))

            for idx in job_info:
                key = str(idx)
                if key not in dataset_state["jobs"]:
                    dataset_state["jobs"][key] = {
                        "status": "submitted",
                        "retries": 0,
                        "cluster": initial_cluster,
                        "proc": idx,
                    }

            for key, persisted_job in dataset_state["jobs"].items():
                if not isinstance(persisted_job, dict):
                    continue
                try:
                    persisted_idx = int(key)
                except (TypeError, ValueError):
                    continue
                if not persisted_job.get("job_dir"):
                    continue
                persisted_job_dir = str(persisted_job["job_dir"])
                persisted_config = str(job_config_path(persisted_job_dir))
                tree_name = persisted_job.get("tree_name")
                if not tree_name and os.path.isfile(persisted_config):
                    try:
                        tree_name = read_config(persisted_config).get("saveTree")
                    except (OSError, ValueError):
                        tree_name = None
                job_info[persisted_idx] = {
                    "dir": persisted_job_dir,
                    "save": str(persisted_job.get("save", "")),
                    "meta": str(persisted_job.get("meta", "")),
                    "tree_name": str(tree_name or "Events"),
                }

            for idx, info in job_info.items():
                job_state = dataset_state["jobs"].setdefault(str(idx), {})
                job_state.setdefault("job_dir", info.get("dir", ""))
                job_state.setdefault("save", info.get("save", ""))
                job_state.setdefault("meta", info.get("meta", ""))
                job_state.setdefault("tree_name", info.get("tree_name", "Events"))

        reactivated = self._reactivate_retryable_failures(state)
        if reactivated:
            self.publish_message(
                f"Reactivated {reactivated} prior permanent failure(s) "
                f"for retry (limit={self.max_retries})."
            )
        self._save_state(state)

        self.publish_message(
            f"Monitoring {sum(len(job_info) for job_info in dataset_job_info.values())} skim job(s) across {len(dataset_job_info)} dataset cluster(s)."
        )
        self.publish_message(f"Output validation: {self.output_validation}")

        event_log_cache: dict = {}
        while True:
            active_clusters = set()
            for dataset_state in state.get("datasets", {}).values():
                for job_state in dataset_state.get("jobs", {}).values():
                    if job_state["status"] in ("submitted", "running", "resubmitting"):
                        active_clusters.add(str(job_state["cluster"]))

            live_status: dict[tuple[str, int], int] = {}
            live_ads: dict[tuple[str, int], dict] = {}
            live_hold_reason: dict[tuple[str, int], str] = {}
            history_exit: dict[tuple[str, int], int] = {}
            for cluster_id in active_clusters:
                for proc, ad in _transport.condor_q_ads(cluster_id).items():
                    if "JobStatus" in ad:
                        live_status[(cluster_id, proc)] = ad["JobStatus"]
                        live_ads[(cluster_id, proc)] = ad
                    if ad.get("HoldReason"):
                        live_hold_reason[(cluster_id, proc)] = str(ad.get("HoldReason"))
                for proc, exit_code in _transport.condor_history_exit(cluster_id).items():
                    history_exit[(cluster_id, proc)] = exit_code

            # History can expire independently of the retained local event log.
            # Recover terminal evidence only for absent jobs with no history;
            # successful exit never bypasses output integrity/provenance checks.
            for dataset_name, job_info in dataset_job_info.items():
                dataset_state = state["datasets"][dataset_name]
                for idx, info in job_info.items():
                    job_state = dataset_state["jobs"][str(idx)]
                    key = (str(job_state["cluster"]), job_state["proc"])
                    if (job_state["status"] in ("done", "perm_fail") or
                            key in live_status or key in history_exit):
                        continue
                    evidence = _transport.condor_event_log_exit(
                        dataset_state["submission_dir"], key[0], key[1], idx,
                        str(info.get("dir", "")), event_log_cache,
                    )
                    if evidence is not None:
                        history_exit[key] = evidence[0]
                        job_state["completion_evidence"] = {
                            "source": "condor_event_log", "path": evidence[1],
                            "cluster": key[0], "proc": key[1], "exit_code": evidence[0],
                        }

            successful_jobs: dict[tuple[str, int], tuple[dict, tuple[str, ...]]] = {}
            for dataset_name, job_info in dataset_job_info.items():
                dataset_state = state["datasets"][dataset_name]
                for idx in sorted(list(job_info.keys())):
                    job_state = dataset_state["jobs"][str(idx)]
                    if job_state["status"] == "perm_fail":
                        continue
                    if job_state["status"] == "done":
                        info = job_info[idx]
                        job_config = str(job_config_path(str(info.get("dir", ""))))
                        require_skim = (
                            _job_output_validation._config_enables_skim(job_config)
                            if os.path.isfile(job_config)
                            else _job_output_validation._config_enables_skim(self.submit_config)
                        )
                        output_roles = ("save", "meta") if require_skim else ("meta",)
                        successful_jobs[(dataset_name, idx)] = (info, output_roles)
                        continue
                    cluster_id = str(job_state["cluster"])
                    proc = job_state["proc"]
                    if live_status.get((cluster_id, proc)) is not None:
                        continue
                    if history_exit.get((cluster_id, proc)) != 0:
                        continue

                    info = job_info[idx]
                    job_config = str(job_config_path(str(info.get("dir", ""))))
                    require_skim = (
                        _job_output_validation._config_enables_skim(job_config)
                        if os.path.isfile(job_config)
                        else _job_output_validation._config_enables_skim(self.submit_config)
                    )
                    output_roles = ("save", "meta") if require_skim else ("meta",)
                    successful_jobs[(dataset_name, idx)] = (info, output_roles)
            job_states = {key: state["datasets"][key[0]]["jobs"][str(key[1])]
                          for key in successful_jobs}
            output_errors = self._validate_output_jobs(successful_jobs, job_states, manifest)

            for dataset_name, job_info in dataset_job_info.items():
                dataset_state = state["datasets"][dataset_name]
                submission_dir = str(dataset_state["submission_dir"])
                for idx in sorted(list(job_info.keys())):
                    key = str(idx)
                    job_state = dataset_state["jobs"][key]
                    cluster_id = str(job_state["cluster"])
                    proc = job_state["proc"]
                    status = job_state["status"]
                    if status == "perm_fail":
                        continue

                    condor_status = live_status.get((cluster_id, proc))
                    exit_code = history_exit.get((cluster_id, proc))

                    if status == "done":
                        errors = output_errors.get((dataset_name, idx), [])
                        if errors:
                            self._try_resubmit(
                                dataset_name,
                                idx,
                                dataset_state,
                                job_state,
                                reason="; ".join(errors),
                                job_info=job_info,
                                monitor_state=state,
                            )
                        continue

                    if condor_status == 2:
                        job_state["status"] = "running"
                        condor_ad = live_ads.get((cluster_id, proc), {})
                        if _job_condor_files._condor_job_exceeded_runtime(condor_ad, self.max_runtime):
                            assigned_runtime = float(condor_ad.get("MaxRuntime", 0) or 0)
                            if assigned_runtime <= 0:
                                assigned_runtime = self.max_runtime
                            elapsed_runtime = _job_condor_files._condor_job_elapsed_runtime(condor_ad)
                            removed = _transport.condor_rm_job(cluster_id, proc)
                            if removed:
                                self.publish_message(
                                    f"[{dataset_name}] Job {idx} exceeded Condor runtime "
                                    f"({elapsed_runtime}s > 1.5 * {assigned_runtime}s); "
                                    "removed from queue."
                                )
                                self._try_resubmit(
                                    dataset_name,
                                    idx,
                                    dataset_state,
                                    job_state,
                                    reason=(
                                        f"runtime {elapsed_runtime}s exceeded "
                                        f"1.5 * assigned runtime {assigned_runtime}s"
                                    ),
                                    job_info=job_info,
                                    monitor_state=state,
                                )
                            else:
                                self.publish_message(
                                    f"[{dataset_name}] Job {idx} exceeded Condor runtime "
                                    "but could not be removed; leaving it running."
                                )
                        continue

                    if condor_status == 5:
                        hold_reason = live_hold_reason.get((cluster_id, proc), "held without reported reason")
                        removed = _transport.condor_rm_job(cluster_id, proc)
                        if removed:
                            self.publish_message(f"[{dataset_name}] Job {idx} held and removed from queue: {hold_reason}")
                        else:
                            self.publish_message(f"[{dataset_name}] Job {idx} held (could not confirm condor_rm): {hold_reason}")
                            continue
                        self._try_resubmit(
                            dataset_name,
                            idx,
                            dataset_state,
                            job_state,
                            reason=f"held: {hold_reason}",
                            job_info=job_info,
                            monitor_state=state,
                        )
                        continue

                    if condor_status is None and exit_code is not None and exit_code != 0:
                        error_tail = _transport.read_failed_job_error(submission_dir, cluster_id, proc, idx)
                        if error_tail:
                            self.publish_message(
                                f"[{dataset_name}] Job {idx} failed with exit code {exit_code}. stderr tail:\n{error_tail}"
                            )
                        self._try_resubmit(
                            dataset_name,
                            idx,
                            dataset_state,
                            job_state,
                            reason=f"exit code {exit_code}",
                            job_info=job_info,
                            monitor_state=state,
                        )
                        continue

                    if condor_status is None and exit_code == 0:
                        errors = output_errors.get((dataset_name, idx), [])
                        if not errors:
                            job_state["status"] = "done"
                            self.publish_message(
                                f"[{dataset_name}] Job {idx}: successful exit and "
                                "expected output paths verified."
                            )
                        else:
                            self._try_resubmit(
                                dataset_name,
                                idx,
                                dataset_state,
                                job_state,
                                reason="; ".join(errors),
                                job_info=job_info,
                                monitor_state=state,
                            )

            self._save_state(state)

            summary = self._status_summary(state)
            done = summary.get("done", 0)
            perm_fail = summary.get("perm_fail", 0)
            total = sum(len(job_info) for job_info in dataset_job_info.values())
            self.publish_message(
                f"Poll: {done}/{total} verified, {perm_fail} permanently failed, "
                f"{total - done - perm_fail} in-flight."
            )

            if done + perm_fail == total:
                break

            time.sleep(self.poll_interval)

        summary = self._status_summary(state)
        done = summary.get("done", 0)
        perm_fail = summary.get("perm_fail", 0)
        if not perm_fail and done:
            self._update_site_rankings()
            self._update_job_sizing()
        with self.monitor_path.open("w") as fh:
            fh.write(f"done={done}\n")
            fh.write(f"perm_fail={perm_fail}\n")
            fh.write(f"total={sum(len(job_info) for job_info in dataset_job_info.values())}\n")
            if perm_fail:
                failed = [
                    f"{dataset_name}:{key}"
                    for dataset_name, dataset_state in state.get("datasets", {}).items()
                    for key, job_state in dataset_state.get("jobs", {}).items()
                    if job_state["status"] == "perm_fail"
                ]
                fh.write("perm_failed_jobs=" + ",".join(failed) + "\n")

        if perm_fail:
            self.publish_message(
                f"WARNING: {perm_fail} skim job(s) permanently failed. Details in {self._state_file}"
            )
        else:
            self.publish_message(f"All {done} skim output(s) verified. Monitoring complete.")

    def _update_site_rankings(self) -> None:
        """Ancillary learning must not turn verified physics outputs into a retry."""
        try:
            result = _xrd_site_rankings.update_after_run(self._run_dir, seed=_io_policy.SITE_RANKING_FILE)
        except Exception as exc:
            result = {"status": "error", "reason": str(exc)}
            self.publish_message(f"WARNING: XRootD site ranking update failed: {exc}")
        try:
            path = Path(self._run_dir) / "xrd_site_rankings_update.json"
            path.write_text(json.dumps(result, indent=2) + "\n")
        except OSError as exc:
            self.publish_message(f"WARNING: Could not save XRootD ranking update receipt: {exc}")
        if result.get("status") == "updated":
            self.publish_message(
                f"Updated XRootD site rankings from {result['completed_runs']} completed run(s): "
                f"{result['cache']}"
            )

    def _update_job_sizing(self) -> None:
        """Learning is ancillary to verified outputs; never resubmit on failure."""
        try:
            selection = self._sizing_selection()
            if self.sizing_pilot_child:
                result = {"status": "skipped", "reason": "paired pilots publish provisional rates separately"}
            elif not selection:
                result = {"status": "skipped", "reason": "no preparation workload identity"}
            else:
                result = _job_sizing.update_sizing_after_run(self._run_dir, selection["context"],
                                            target_seconds=self.sizing_target_seconds)
        except Exception as exc:
            result = {"status": "error", "reason": str(exc)}
            self.publish_message(f"WARNING: Job-sizing profile update failed: {exc}")
        try:
            (Path(self._run_dir) / "job_sizing_update.json").write_text(json.dumps(result, indent=2) + "\n")
        except OSError as exc:
            self.publish_message(f"WARNING: Could not save job-sizing update receipt: {exc}")
        if result.get("status") == "updated":
            self.publish_message(f"Updated job-sizing history: {result['datasets']} trained dataset(s), {result['cache']}")

    def _try_resubmit(
        self,
        dataset_name: str,
        idx: int,
        dataset_state: dict,
        job_state: dict,
        reason: str,
        job_info: Optional[dict[int, dict[str, str]]] = None,
        monitor_state: Optional[dict] = None,
    ) -> None:
        retries = int(job_state.get("retries", 0))
        if retries >= self.max_retries:
            self.publish_message(
                f"[{dataset_name}] Job {idx}: permanently failed ({reason}, {retries} retries exhausted)."
            )
            job_state["status"] = "perm_fail"
            if monitor_state is not None:
                self._save_state(monitor_state)
            return

        _job_staging._validate_staged_skim_proxy(self._shared_dir, required=bool(self.x509))
        _job_staging._validate_staged_skim_executable(self._shared_dir, self.exe)
        self.publish_message(
            f"[{dataset_name}] Job {idx} failed ({reason}) - resubmitting "
            f"(attempt {retries + 1}/{self.max_retries})."
        )

        submission_dir = str(dataset_state["submission_dir"])
        tracked_job_info = job_info if job_info is not None else {}
        source_info = dict(tracked_job_info.get(idx, {}))
        source_job_dir = str(
            source_info.get("dir") or os.path.join(submission_dir, f"job_{idx}")
        )
        file_groups = _job_condor_files._split_skim_partition(
            source_job_dir, os.path.join(self._run_dir, "input_metadata.json"))
        retry_attempt = retries + 1
        submitted_parts: list[tuple[dict[str, str], str]] = []

        for part_index, partition in enumerate(file_groups):
            part_files = partition["files"]
            part_info = source_info
            log_label = None
            if len(file_groups) > 1:
                try:
                    part_info = _job_condor_files._prepare_split_skim_job(
                        source_job_dir=source_job_dir,
                        submission_dir=submission_dir,
                        job_index=idx,
                        retry_attempt=retry_attempt,
                        part_index=part_index,
                        file_list=part_files,
                        first_entry=partition.get("first_entry"),
                        last_entry=partition.get("last_entry"),
                    )
                except (OSError, ValueError) as exc:
                    self.publish_message(
                        f"[{dataset_name}] Job {idx}: could not prepare split retry "
                        f"part {part_index}: {exc}"
                    )
                    continue
                log_label = f"resub_{idx}_retry{retry_attempt}_part{part_index}"

            new_cluster = _job_condor_files._submit_single_skim_job(
                idx,
                submission_dir,
                self.max_runtime,
                self.request_memory,
                extra_transfer_files=list(dataset_state.get("shared_transfer_files", [])),
                config_file=job_config_path(part_info.get("dir") or source_job_dir).name,
                source_job_dir=part_info.get("dir") or source_job_dir,
                log_label=log_label,
            )
            if new_cluster:
                submitted_parts.append((part_info, new_cluster))

        if len(submitted_parts) != len(file_groups):
            for _, cluster_id in submitted_parts:
                _transport.condor_rm_job(cluster_id, 0)
            self.publish_message(
                f"[{dataset_name}] Job {idx}: retry submission did not return "
                "all expected clusters; leaving it retryable."
            )
            job_state["status"] = "submitted"
            if monitor_state is not None:
                self._save_state(monitor_state)
            return

        def _record_part(
            target_idx: int,
            part_info: dict[str, str],
            cluster_id: str,
        ) -> None:
            tracked_job_info[target_idx] = {
                "dir": str(part_info.get("dir", "")),
                "save": str(part_info.get("save", "")),
                "meta": str(part_info.get("meta", "")),
                "tree_name": str(part_info.get("tree_name", "Events")),
            }
            target_state = (
                job_state
                if target_idx == idx
                else dataset_state.setdefault("jobs", {}).setdefault(str(target_idx), {})
            )
            target_state.pop("completion_evidence", None)
            target_state.update(
                cluster=cluster_id,
                proc=0,
                retries=retry_attempt,
                status="resubmitting",
                job_dir=str(part_info.get("dir", "")),
                save=str(part_info.get("save", "")),
                meta=str(part_info.get("meta", "")),
                tree_name=str(part_info.get("tree_name", "Events")),
            )

        first_part, first_cluster = submitted_parts[0]
        _record_part(idx, first_part, first_cluster)
        next_idx = max(tracked_job_info.keys(), default=idx) + 1
        for part_info, cluster_id in submitted_parts[1:]:
            while str(next_idx) in dataset_state.get("jobs", {}):
                next_idx += 1
            _record_part(next_idx, part_info, cluster_id)
            next_idx += 1

        # Persist new cluster identifiers before processing another job. This
        # narrows the restart window after Condor accepts a retry; the external
        # submit itself cannot be made transactional with this local JSON file.
        if monitor_state is not None:
            self._save_state(monitor_state)
