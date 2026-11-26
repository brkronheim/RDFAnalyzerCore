"""Reusable diagnostic execution and identities for saved statistical studies."""
import json
from pathlib import Path
import subprocess

from run_bundle import file_digest
from .errors import FitError


def read_json(path):
    path = Path(path)
    try:
        value = json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        raise FitError(f"cannot read JSON {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise FitError(f"expected a JSON object in {path}")
    return value


def run_logged(command, *, cwd, log_path, environment):
    log_path = Path(log_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with log_path.open("w") as log:
            result = subprocess.run(command, cwd=cwd, stdout=log, stderr=subprocess.STDOUT,
                                    env=environment, check=False)
    except OSError as exc:
        raise FitError(f"could not run {command[0]}: {exc}") from exc
    if result.returncode:
        tail = "\n".join(log_path.read_text(errors="replace").splitlines()[-45:])
        raise FitError(f"command failed with exit status {result.returncode}; log: {log_path}\n{tail}")


def diagnostic_manifest(path, fit_result, workspace, *, schema="rdf-fit-diagnostics-v1"):
    """Prevent diagnostic ensembles from mixing changed fits or workspaces."""
    identity = {"fit_result": str(Path(fit_result).resolve()),
                "fit_result_sha256": file_digest(fit_result),
                "workspace": str(Path(workspace).resolve()),
                "workspace_sha256": file_digest(workspace)}
    if Path(path).is_file():
        record = read_json(path)
        for key, value in identity.items():
            if key.endswith("_sha256") and key not in record:
                # Upgrade legacy mtime records only when their old identity
                # still agrees. Future checks always compare file contents.
                original = "fit_result" if key.startswith("fit_result") else "workspace"
                file = fit_result if original == "fit_result" else workspace
                if record.get(original+"_mtime_ns") != Path(file).stat().st_mtime_ns:
                    raise FitError("diagnostic inputs changed; choose a fresh output directory")
            elif record.get(key) != value:
                raise FitError("diagnostic inputs changed; choose a fresh output directory")
        record.update(identity)
    else:
        record = {"schema": schema, **identity, "algorithms": {}}
    record.setdefault("algorithms", {})
    return record
