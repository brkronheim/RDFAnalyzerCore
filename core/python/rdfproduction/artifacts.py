"""Read-only validation for production and merge receipts."""

import json
from pathlib import Path

from run_bundle import content_digest, file_digest, verify_artifact
from .output_index import load_verified_output_index, verify_output_signatures


def verify_outputs(output_index, merge_receipt=None, *, check_signatures=None):
    """Verify local receipt contents and, when requested, fresh storage metadata."""
    index = load_verified_output_index(output_index)
    inventory = index["datasets"]
    result = {
        "index_id": index["index_id"],
        "datasets": len(inventory),
        "verified_partitions": sum(len(dataset["jobs"]) for dataset in inventory.values()),
        "storage_checked": check_signatures is not None,
    }
    if check_signatures is not None:
        verify_output_signatures(index, check_signatures)
    if merge_receipt is None:
        return result

    receipt = Path(merge_receipt).resolve()
    data = json.loads(receipt.read_text())
    if not isinstance(data, dict):
        raise ValueError(f"invalid merge receipt: {receipt}")
    identity = data.pop("receipt_id", None)
    if data.get("schema") != "rdf-verified-merge-v1" or identity != content_digest(data):
        raise ValueError(f"invalid merge receipt: {receipt}")
    if data.get("index_id") != index["index_id"]:
        raise ValueError(f"merge receipt belongs to another output index: {receipt}")
    snapshots = data.get("snapshots")
    if not isinstance(snapshots, dict) or len(snapshots) != len(inventory):
        raise ValueError(f"merge receipt does not cover all datasets: {receipt}")
    root = Path(data["destination"]).resolve()
    expected = set(inventory)
    for filename, digest in snapshots.items():
        snapshot = Path(filename).resolve()
        if snapshot.parent != root or file_digest(snapshot) != digest:
            raise ValueError(f"merge snapshot is outside the destination or changed: {snapshot}")
        artifact = verify_artifact(snapshot)
        configuration = artifact.get("configuration", {})
        dataset = configuration.get("dataset")
        if (artifact.get("kind") != "verified-merge"
                or artifact.get("parents") != {"verified_output_index": index["index_id"]}
                or dataset not in expected):
            raise ValueError(f"merge snapshot belongs to another dataset or output index: {snapshot}")
        jobs = {key: entry["outputs"]["meta"] for key, entry in inventory[dataset]["jobs"].items()}
        if configuration.get("jobs") != jobs:
            raise ValueError(f"merge snapshot does not cover the verified jobs for {dataset}: {snapshot}")
        expected.remove(dataset)
    return {**result, "merge_receipt_id": identity, "merged_datasets": len(snapshots)}
