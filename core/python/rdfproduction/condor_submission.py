"""Journal initial Condor batches independently of the workflow engine."""

import fcntl
import json
import os
from pathlib import Path
import pwd
import re
import socket
import subprocess

from run_bundle import content_digest, file_digest, write_json


_BATCH_ATTRIBUTE = "RDFSubmissionId"
_CONFIG_SELECTORS = (
    "FULL_HOSTNAME", "SCHEDD_HOST", "SCHEDD_NAME", "MASTER_NAME",
    "SCHEDD_ADDRESS_FILE", "COLLECTOR_HOST", "SPOOL",
)


def _submission_context():
    """Capture the selectors used by the default local Condor tools."""
    result = subprocess.run(
        ["condor_config_val", "-dump", "-expanded", f"^({'|'.join(_CONFIG_SELECTORS)})$"],
        capture_output=True, text=True, check=True, timeout=60,
    )
    selectors = {}
    for line in result.stdout.splitlines():
        key, separator, value = line.partition("=")
        if separator and key.strip() in _CONFIG_SELECTORS:
            selectors[key.strip()] = value.strip()
    if not selectors.get("FULL_HOSTNAME"):
        raise RuntimeError("Condor did not report FULL_HOSTNAME for the submission journal.")
    return {
        "owner": pwd.getpwuid(os.getuid()).pw_name,
        "host": socket.gethostname(),
        "condor": selectors,
    }


def _write_submission_record(path, record):
    write_json(path, record)
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
    directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _submission_plan(manifest_path):
    manifest = json.loads(Path(manifest_path).read_text())
    records = manifest.get("datasets")
    if not records or not isinstance(records, list):
        raise RuntimeError("No dataset submissions found in submission manifest.")

    submit_digests = {}
    for record in records:
        dataset = record["dataset_name"]
        if not isinstance(dataset, str) or not dataset or dataset in submit_digests:
            raise RuntimeError(f"Invalid or duplicate dataset name in submission manifest: {dataset!r}")
        job_count = record["job_count"]
        if type(job_count) is not int or job_count < 1:
            raise RuntimeError(f"Invalid Condor job count for {dataset}: {job_count!r}")
        submit_file = Path(record["submit_path"])
        if not submit_file.is_file():
            raise RuntimeError(f"Condor submit file not found for {dataset}: {submit_file}")
        submit_digests[dataset] = file_digest(submit_file)
    execution_plan = Path(manifest_path).with_name("execution_plan.json")
    identity = {
        "datasets": records,
        "submit_digests": submit_digests,
        "execution_plan": file_digest(execution_plan) if execution_plan.is_file() else None,
    }
    return records, content_digest(identity)


def _query_batch(command, constraint):
    result = subprocess.run(
        [command, "-constraint", constraint, "-json", "-attributes",
         f"{_BATCH_ATTRIBUTE},Owner,ClusterId,ProcId,GlobalJobId"],
        capture_output=True, text=True, check=True, timeout=120,
    )
    ads = json.loads(result.stdout.strip() or "[]")
    if not isinstance(ads, list) or any(not isinstance(ad, dict) for ad in ads):
        raise RuntimeError(f"Invalid {command} ClassAd response.")
    return ads


def _reconcile_batch(batch_id, owner, job_count):
    constraint = f'{_BATCH_ATTRIBUTE} =?= "{batch_id}" && Owner =?= {json.dumps(owner)}'
    # A job can leave condor_q between reads. Query history second and merge
    # both views using GlobalJobId, which includes the submitting schedd.
    ads = _query_batch("condor_q", constraint) + _query_batch("condor_history", constraint)
    jobs = {}
    clusters = set()
    for ad in ads:
        cluster, proc, global_id = (ad.get(key) for key in ("ClusterId", "ProcId", "GlobalJobId"))
        if (ad.get(_BATCH_ATTRIBUTE) != batch_id or ad.get("Owner") != owner
                or type(cluster) is not int or cluster < 1 or type(proc) is not int
                or not isinstance(global_id, str)):
            raise RuntimeError("Condor returned an invalid submission identity.")
        schedd, separator, job_id = global_id.partition("#")
        if not schedd or not separator or job_id.split("#", 1)[0] != f"{cluster}.{proc}":
            raise RuntimeError("Condor returned an invalid GlobalJobId.")
        clusters.add((schedd, cluster))
        if jobs.setdefault(proc, global_id) != global_id:
            raise RuntimeError("Condor returned multiple attempts for one job in the submission.")
    if len(clusters) != 1 or jobs.keys() != set(range(job_count)):
        raise RuntimeError("Condor did not report one cluster with all expected jobs.")
    schedd, cluster = clusters.pop()
    return str(cluster), f"Reconciled from Condor queue/history on {schedd}"


def _submit_batch(record, batch_id):
    job_count = record["job_count"]
    result = subprocess.run(
        ["condor_submit", "-terse", "-single-cluster", "-maxjobs", str(job_count),
         "-append", f'+{_BATCH_ATTRIBUTE} = "{batch_id}"', record["submit_path"]],
        capture_output=True, text=True, check=True,
    )
    stdout = result.stdout.strip()
    match = re.fullmatch(r"([1-9]\d*)\.0\s*-\s*\1\.(\d+)", stdout)
    if not match or int(match[2]) != job_count - 1:
        raise RuntimeError(f"Unexpected Condor submit receipt: {stdout!r}")
    return match[1], stdout


def _submit_skim_batches(manifest_path, receipt_path, validate, publish):
    # Completed legacy receipts retain their original monitor format and owner.
    if receipt_path.is_file():
        return
    journal_path = receipt_path.with_name("submission_journal.json")
    records, plan_id = _submission_plan(manifest_path)
    scheduler = _submission_context()
    identity = {"version": 1, "plan_id": plan_id, "scheduler": scheduler}
    batches = {}
    if journal_path.exists():
        journal = json.loads(journal_path.read_text())
        if journal.get("identity") != identity:
            raise RuntimeError(f"Submission plan or scheduler differs from {journal_path}.")
        batches = journal["batches"]
    elif receipt_path.with_name("monitor_state.json").exists():
        raise RuntimeError("An existing monitor lacks its submission receipt. Restore the receipt before resuming.")
    else:
        journal = {"identity": identity, "batches": batches}

    workers_validated = False
    submitted = []
    for record in records:
        dataset = record["dataset_name"]
        batch_id = content_digest({"run": str(receipt_path.parent.resolve()), "dataset": dataset,
                                   "identity": identity})
        prior = batches.get(dataset)
        if prior is None:
            if not workers_validated:
                validate()
                workers_validated = True
            batches[dataset] = {"id": batch_id, "state": "pending"}
            _write_submission_record(journal_path, journal)
        elif prior.get("id") != batch_id or prior.get("state") not in {"pending", "accepted"}:
            raise RuntimeError(f"Invalid submission journal for dataset {dataset} in {journal_path}.")
        if prior is None or prior["state"] == "pending":
            try:
                if prior is None:
                    cluster, stdout = _submit_batch(record, batch_id)
                else:
                    cluster, stdout = _reconcile_batch(batch_id, scheduler["owner"], record["job_count"])
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
                details = getattr(exc, "stderr", None) or str(exc)
                raise RuntimeError(
                    f"Submission acceptance is unresolved for {dataset} (ID {batch_id}). "
                    f"The pending intent remains in {journal_path}. {details.strip()}"
                ) from exc
            batches[dataset] = {"id": batch_id, "state": "accepted", "cluster_id": cluster, "stdout": stdout}
            _write_submission_record(journal_path, journal)
            publish(f"Submitted skim dataset {dataset} in cluster {cluster} with {record['job_count']} job(s).")
        submitted.append(dict(record, cluster_id=batches[dataset]["cluster_id"],
                              stdout=batches[dataset]["stdout"], submission_id=batch_id))
    _write_submission_record(receipt_path, {"datasets": submitted})


def submit_skim_batches(manifest_path, receipt_path, *, validate, publish):
    """Submit one cluster per dataset and recover only from positive Condor evidence."""
    receipt_path = Path(receipt_path)
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    with receipt_path.with_name(".submission.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Another controller owns batch submission in {receipt_path.parent}.") from exc
        _submit_skim_batches(manifest_path, receipt_path, validate, publish)
