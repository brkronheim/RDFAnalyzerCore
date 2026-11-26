"""Validate recorded job outputs and report storage signatures."""

from __future__ import annotations

from typing import Optional
import json
import os
import stat

from output_schema import write_cache_sidecar
from submission_backend import ensure_xrootd_redirector
from submission_backend import read_config
from . import provenance as _job_provenance
import rdfproduction.transport as _transport
from . import staging as _job_staging
import derivation_correctionlib as _derivation_correctionlib


def _output_files_exist_batch(output_paths: list[str]) -> dict[str, bool]:
    """Check a mixed list of local and XRootD output paths."""
    result: dict[str, bool] = {}
    eos_paths: list[str] = []

    for path in output_paths:
        if not path:
            result[path] = False
        elif path.startswith("root://") or path.startswith("/eos/") or path.startswith("/store/"):
            eos_paths.append(path)
        else:
            result[path] = os.path.exists(path)

    if eos_paths:
        result.update(_transport.eos_files_exist_batch(eos_paths))

    return result


def _output_file_info_batch(output_paths: list[str]) -> dict[str, dict | None]:
    """Cheap fresh signatures; unknown remote signatures must not be cached."""
    result: dict[str, dict | None] = {}
    remote = []
    for path in dict.fromkeys(output_paths):
        if path.startswith(("root://", "/eos/", "/store/")):
            remote.append(path)
        else:
            try:
                info = os.stat(path)
                result[path] = dict(size=info.st_size, mtime_ns=info.st_mtime_ns,
                    ctime_ns=info.st_ctime_ns, inode=info.st_ino, device=info.st_dev) \
                    if stat.S_ISREG(info.st_mode) else None
            except OSError:
                result[path] = None
    if remote:
        result.update(_transport.eos_file_info_batch(remote))
    return result


def _root_file_tree_error(path: str, tree_name: Optional[str]) -> Optional[str]:
    """Check a ROOT file, optionally requiring a named tree inside it."""
    if not path:
        return "output path is not configured"

    is_url = path.startswith("root://")
    is_remote_storage_path = path.startswith(("/eos/", "/store/"))
    if not is_url and not is_remote_storage_path:
        try:
            if not os.path.isfile(path):
                return "file is absent or is not a regular file"
            if os.path.getsize(path) <= 0:
                return "file is empty"
        except OSError as exc:
            return f"file is unreadable: {exc}"

    open_paths = [
        ensure_xrootd_redirector(path, "root://eosuser.cern.ch/")
        if is_url or is_remote_storage_path
        else path
    ]
    if is_remote_storage_path:
        open_paths.insert(0, path)

    errors: list[str] = []
    try:
        import uproot  # type: ignore
    except ImportError:
        uproot = None  # type: ignore[assignment]

    if uproot is not None:
        for open_path in dict.fromkeys(open_paths):
            try:
                with uproot.open(open_path) as root_file:
                    if tree_name is None:
                        root_file.keys()
                        # Compact metadata is part of the artifact contract.
                        # Validate while the file is already open; legacy files
                        # without the versioned keys retain their old contract.
                        from metadata_io import JOB_KEY, REFERENCE_KEY, load_metadata_records
                        if JOB_KEY in root_file or REFERENCE_KEY in root_file:
                            try:
                                load_metadata_records(open_path)
                            except Exception as exc:
                                return f"invalid compact metadata: {exc}"
                        return None
                    obj = root_file[tree_name]
                    classname = getattr(obj, "classname", None)
                    if classname is not None and classname not in ("TTree", "ROOT::RNTuple"):
                        return f"object '{tree_name}' is {classname}, not a tree"
                    return None
            except Exception as exc:  # uproot uses several backend-specific exceptions
                errors.append(f"uproot could not read {open_path!r}: {exc}")

    try:
        import ROOT  # type: ignore
    except Exception as exc:  # broken optional PyROOT installations can fail at import time
        errors.append(f"PyROOT could not be imported: {exc}")
        ROOT = None  # type: ignore[assignment]

    if ROOT is not None:
        for open_path in dict.fromkeys(open_paths):
            root_file = None
            try:
                root_file = ROOT.TFile.Open(open_path, "READ")
                if not root_file or root_file.IsZombie():
                    raise OSError("ROOT reported a zombie or unopened file")
                if tree_name is None:
                    from metadata_io import JOB_KEY, REFERENCE_KEY, _normalize_record
                    obj = root_file.Get(JOB_KEY)
                    if obj:
                        try:
                            _normalize_record(json.loads(str(obj.GetString().Data())), open_path)
                        except Exception as exc:
                            return f"invalid compact metadata: {exc}"
                    if root_file.Get(REFERENCE_KEY):
                        return "merged metadata references require uproot for integrity validation"
                    return None
                obj = root_file.Get(tree_name)
                if not obj or not obj.InheritsFrom("TTree"):
                    return f"object '{tree_name}' is missing or is not a TTree"
                return None
            except Exception as exc:  # ROOT can raise framework-specific exceptions
                errors.append(f"PyROOT could not read {open_path!r}: {exc}")
            finally:
                if root_file:
                    root_file.Close()

    detail = "; ".join(errors) if errors else "neither uproot nor PyROOT is available"
    if tree_name is None:
        return f"cannot validate ROOT file {path!r}: {detail}"
    return f"cannot validate ROOT tree '{tree_name}' in {path!r}: {detail}"


def _config_enables_skim(config_path: str) -> bool:
    """Read the Analyzer skim switch from a staged or template config."""
    if not config_path or not os.path.isfile(config_path):
        return False
    value = read_config(config_path).get("enableSkim", False)
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _skim_job_output_errors(
    *,
    dataset_name: str,
    dtype: str,
    save_path: str,
    meta_path: str,
    tree_name: str = "Events",
    require_skim: bool = True,
) -> list[str]:
    """Validate a skim when enabled and always validate the metadata output."""
    errors: list[str] = []
    if require_skim:
        if not save_path:
            errors.append("skim output path is not configured")
        else:
            reason = _root_file_tree_error(save_path, tree_name)
            if reason:
                errors.append(f"skim output {save_path!r}: {reason}")

    if not meta_path:
        errors.append("metadata output path is not configured")
    else:
        reason = _root_file_tree_error(meta_path, None)
        if reason:
            errors.append(f"metadata output {meta_path!r}: {reason}")

    if dtype.lower() == "mc" and meta_path and not any(
        error.startswith("metadata output") for error in errors
    ):
        for description, reader in (
            ("raw generator-weight counter", _derivation_correctionlib._read_weight_sum),
            ("generator-weight sign counter", _derivation_correctionlib._read_weight_sign_sum),
        ):
            try:
                reader(meta_path, dataset_name)
            except Exception as exc:  # malformed ROOT metadata can raise backend-specific errors
                errors.append(
                    f"metadata output {meta_path!r} has invalid {description}: {exc}"
                )
    return errors


def _run_skim_job_with_output_contract(
    job_runner,
    job_args: list,
    job_kwargs: dict,
    dataset_manifest_path: str,
    submit_config_path: str,
    dataset_name: str,
    save_path: str,
    meta_path: str,
    tree_name: str,
    write_cache: bool,
    extra_input_paths: Optional[list[str]] = None,
) -> str:
    """Run a Dask skim and validate outputs before the proxy writes its marker."""
    result = job_runner(*job_args, **job_kwargs)
    dataset = _job_staging._find_dataset_entry(dataset_manifest_path, dataset_name)
    if dataset is None:
        raise RuntimeError(
            f"Skim output contract cannot find dataset {dataset_name!r} in "
            f"{dataset_manifest_path!r}."
        )

    require_skim = _config_enables_skim(submit_config_path)
    errors = _skim_job_output_errors(
        dataset_name=dataset_name,
        dtype=dataset.dtype,
        save_path=save_path,
        meta_path=meta_path,
        tree_name=tree_name,
        require_skim=require_skim,
    )
    if errors:
        raise RuntimeError(
            f"Skim output contract failed for dataset {dataset_name!r}: "
            + "; ".join(errors)
        )

    if write_cache and require_skim:
        manifest = _job_provenance._build_skim_manifest(
            dataset=dataset,
            submit_config_path=submit_config_path,
            dataset_manifest_path=dataset_manifest_path,
            skim_output_file=save_path,
            extra_input_paths=extra_input_paths,
        )
        write_cache_sidecar(save_path, manifest)
    return result
