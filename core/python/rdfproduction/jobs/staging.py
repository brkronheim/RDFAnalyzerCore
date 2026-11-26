"""Stage runtime files, configuration payloads, credentials, and worker archives."""

from __future__ import annotations

from native_config_io import job_config_path
from pathlib import Path
from typing import Optional
import hashlib
import json
import os
import re
import shlex
import shutil
import subprocess
import tarfile
import tempfile

from dataset_manifest import DatasetEntry
from dataset_manifest import DatasetManifest
from submission_backend import get_copy_file_list
from submission_backend import read_config
from submission_backend import write_config
from run_bundle import file_digest
from run_bundle import find_bundle
from run_bundle import rewrite_references
import rdfproduction.transport as _transport
import rdfproduction.executors as _executors

from .. import REPOSITORY_ROOT


def _stream_analysis_output() -> bool:
    """Return whether local analysis subprocess logs should be shown live."""
    return os.environ.get("RDF_VERBOSE", os.environ.get("RDF_VH_VERBOSE", "")).strip().lower() in {
        "1", "true", "yes", "on",
    }


_MC_GENERATOR_CONFIG_KEYS = (
    "counterWeightBranch",
    "genWeightMode",
    "genWeightBaseWeightColumn",
    "genWeightNominalColumn",
    "genWeightLheScaleBranch",
    "genWeightPsBranch",
    "genWeightPdfBranch",
    "genWeightEnableLheScale",
    "genWeightEnablePs",
    "genWeightEnableIsr",
    "genWeightEnableFsr",
    "genWeightEnablePdf",
    "genWeightEnablePdfAs",
)


def _remove_mc_generator_config_for_data(config: dict[str, str]) -> None:
    """Remove generator-weight settings from data job configurations."""
    if str(config.get("dtype", "")).lower() != "data":
        return
    for key in _MC_GENERATOR_CONFIG_KEYS:
        config.pop(key, None)


def _find_dataset_entry(
    dataset_manifest_path: str, name: str
) -> Optional[DatasetEntry]:
    """Return the :class:`~dataset_manifest.DatasetEntry` named *name*, or ``None``."""
    if not dataset_manifest_path or not os.path.isfile(dataset_manifest_path):
        return None
    try:
        manifest = DatasetManifest.load(dataset_manifest_path)
        for entry in manifest.datasets:
            if entry.name == name:
                return entry
    except Exception:
        pass
    return None


def _manifest_lumi_for_dataset(
    dataset_manifest_path: str, dataset: DatasetEntry
) -> Optional[float]:
    """Return manifest luminosity when the manifest is available."""
    if not dataset_manifest_path or not os.path.isfile(dataset_manifest_path):
        return None
    try:
        manifest = DatasetManifest.load(dataset_manifest_path)
    except (OSError, ValueError, TypeError):
        return None
    return manifest.lumi_for(dataset.year, dataset.era)


def _resolve_analysis_config_path(
    template_config_path: str,
    path_value: str,
) -> str:
    """Resolve config references relative to the analysis cfg or its parent.

    Analysis submit-config templates often live under ``analysis/cfg/`` while
    referenced payload files are written as ``cfg/foo.yaml`` relative to the
    analysis root. Materialized job configs can also contain the staging-only
    ``cfg/`` prefix; retry without it when resolving against the source tree.
    """
    if not path_value:
        return path_value
    bundle = find_bundle(template_config_path)
    if bundle:
        record = json.loads((bundle / "bundle_manifest.json").read_text())
        if path_value in record["files"]:
            return str(bundle / path_value)
        if path_value in record["assets"]:
            return str(bundle / record["assets"][path_value])
        if path_value.startswith("cfg/"):
            members = [name for name in record["files"] if Path(name).name == Path(path_value).name]
            if len(members) == 1:
                return str(bundle / members[0])
        if os.path.isabs(path_value):
            return path_value
        raise FileNotFoundError(f"undeclared bundle dependency {path_value!r} in {bundle}")
    if os.path.isabs(path_value):
        return path_value

    template_dir = os.path.dirname(os.path.abspath(template_config_path))

    def _candidates(value: str) -> list[str]:
        paths = [os.path.abspath(value)]
        current_dir = template_dir
        visited: set[str] = set()
        while current_dir not in visited:
            visited.add(current_dir)
            paths.append(os.path.join(current_dir, value))

            parent_dir = os.path.dirname(current_dir)
            if parent_dir == current_dir:
                break
            current_dir = parent_dir
        return paths

    for candidate in _candidates(path_value):
        if os.path.exists(candidate):
            return os.path.abspath(candidate)

    # Job configs are normalized to cfg/<basename> after their inputs have
    # been materialized. A later layout pass may see that runtime path while
    # resolving against the original template directory, so retry the
    # corresponding source reference without the staging-only cfg/ prefix.
    if path_value.startswith("cfg/"):
        for candidate in _candidates(path_value[len("cfg/"):]):
            if os.path.exists(candidate):
                return os.path.abspath(candidate)

    return os.path.abspath(os.path.join(template_dir, path_value))


def _analysis_config_payload_sources(
    template_config_path: str,
    config_dict: dict[str, str],
    staged_config_dir: Optional[str] = None,
) -> list[str]:
    """Resolve payload references, including paths already staged under cfg/."""
    sources: list[str] = []
    basename_sources: dict[str, str] = {}
    paths = list(get_copy_file_list(config_dict))
    bundle = find_bundle(template_config_path)
    if bundle:
        declared = json.loads((bundle / "bundle_manifest.json").read_text())
        paths.extend(str(bundle / value) for value in declared["assets"].values())
    for path_value in paths:
        if staged_config_dir and path_value.startswith("cfg/"):
            staged_path = os.path.join(staged_config_dir, path_value)
            if os.path.isfile(staged_path):
                # Repeated layout passes should be idempotent. This payload
                # was resolved and staged on the earlier pass.
                continue

        src = _resolve_analysis_config_path(template_config_path, path_value)
        if not os.path.isfile(src):
            raise FileNotFoundError(
                f"Configured analysis input {path_value!r} from "
                f"'{template_config_path}' does not resolve to a file: {src}"
            )

        basename = _asset_name(src, config_dict)
        previous = basename_sources.get(basename)
        if previous is not None and previous != src:
            raise ValueError(
                f"Configured analysis inputs '{previous}' and '{src}' both "
                f"stage as '{basename}'; give them distinct basenames"
            )
        basename_sources[basename] = src
        if src not in sources:
            sources.append(src)
    return sources


def _legacy_asset_kind(source, config):
    from native_config_io import legacy_aux_kind
    if Path(source).suffix not in {".txt", ".cfg"}:
        return None
    name = os.path.basename(source)
    for key, value in config.items():
        if isinstance(value, str) and os.path.basename(value) == name:
            kind = legacy_aux_kind(key, source)
            if kind:
                return kind
    return None


def _asset_name(source, config):
    name = os.path.basename(source)
    return Path(name).with_suffix(".yaml").name if _legacy_asset_kind(source, config) else name


def _link_archived_asset_paths(template, config, working_dir, config_dir):
    """Keep opaque recipes from sealed legacy bundles readable without rewriting their bytes."""
    from native_config_io import legacy_aux_kind
    if not any(isinstance(value, str) and key.endswith("Config") and value.endswith((".txt", ".cfg"))
               and legacy_aux_kind(key, value) is None for key, value in config.items()):
        return
    bundle = find_bundle(template)
    if not bundle:
        return
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    # Historical recipe rewriters emitted paths under assets/ relative to the
    # sealed bundle. Resolve those names both from the worker and alongside its config.
    for relative in manifest["assets"].values():
        if Path(relative).parent != Path("assets"):
            continue
        staged = Path(config_dir) / _asset_name(relative, config)
        for base in {Path(working_dir), Path(config_dir)}:
            link = base / relative
            link.parent.mkdir(exist_ok=True)
            if not link.exists() and not link.is_symlink():
                link.symlink_to(os.path.relpath(staged, link.parent))


def _stage_config_asset(template, source, directory, config):
    from native_config_io import read_legacy_aux, write_config_yaml
    kind = _legacy_asset_kind(source, config)
    target = Path(directory) / _asset_name(source, config)
    if kind:
        write_config_yaml(read_legacy_aux(source, kind), target)
    else:
        shutil.copy2(source, target)
    _rewrite_staged_analysis_config(template_config_path=template, source_config_path=source, staged_config_path=str(target))
    return target


def _copy_analysis_config_files(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
) -> set[str]:
    """Copy referenced config payload files into *destination_dir*.

    Returns the basenames that were staged locally so callers can rewrite
    config values to point at the copied files.
    """
    copied: set[str] = set()
    Path(destination_dir).mkdir(parents=True, exist_ok=True)
    for src in _analysis_config_payload_sources(template_config_path, config_dict):
        staged = _stage_config_asset(template_config_path, src, destination_dir, config_dict)
        copied.add(staged.name)
    _link_archived_asset_paths(template_config_path, config_dict, destination_dir, destination_dir)
    return copied


def _analysis_aux_dir(template_config_path: str) -> str:
    """Return the analysis-local aux directory for a submit-config template."""
    bundle = find_bundle(template_config_path)
    if bundle:
        return str(bundle / "aux")
    return _resolve_analysis_config_path(template_config_path, "aux")


def _copy_analysis_aux_dir(template_config_path: str, destination_dir: str) -> bool:
    """Copy the analysis aux directory into *destination_dir*/aux when present."""
    aux_src = _analysis_aux_dir(template_config_path)
    if not aux_src or not os.path.isdir(aux_src):
        return False

    aux_dst = os.path.join(destination_dir, "aux")
    Path(aux_dst).mkdir(parents=True, exist_ok=True)
    shutil.copytree(aux_src, aux_dst, dirs_exist_ok=True)
    return True


def _bundle_analysis_aux_dir(template_config_path: str, destination_dir: str) -> Optional[str]:
    """Create a tar.gz archive containing the analysis aux/ directory."""
    aux_src = _analysis_aux_dir(template_config_path)
    if not aux_src or not os.path.isdir(aux_src):
        return None

    archive_path = os.path.join(destination_dir, "aux_bundle.tar.gz")
    with tarfile.open(archive_path, "w:gz") as archive:
        archive.add(aux_src, arcname="aux")
    return archive_path


def _bundle_analysis_config_files(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
    prepared_payloads: Optional[dict[str, bytes]] = None,
) -> Optional[str]:
    """Archive cfg/ payloads, optionally retaining rewritten text for local jobs."""
    payload_paths = _analysis_config_payload_sources(template_config_path, config_dict)
    if not payload_paths:
        return None

    archive_path = os.path.join(destination_dir, "cfg_bundle.tar.gz")
    with tempfile.TemporaryDirectory(prefix="skim_cfg_bundle_") as tmpdir:
        cfg_stage_dir = os.path.join(tmpdir, "cfg")
        Path(cfg_stage_dir).mkdir(parents=True, exist_ok=True)
        for src in payload_paths:
            staged_path = _stage_config_asset(template_config_path, src, cfg_stage_dir, config_dict)
            if prepared_payloads is not None and Path(src).suffix in {".yaml", ".yml", ".txt"}:
                prepared_payloads[src] = Path(staged_path).read_bytes()
        _link_archived_asset_paths(template_config_path, config_dict, tmpdir, cfg_stage_dir)
        with tarfile.open(archive_path, "w:gz") as archive:
            archive.add(cfg_stage_dir, arcname="cfg")
            if (Path(tmpdir) / "assets").is_dir():
                archive.add(Path(tmpdir) / "assets", arcname="assets")
    return archive_path


def _symlink_or_copy_path(src_path: str, dst_path: str) -> None:
    """Create *dst_path* from *src_path*, preferring symlinks and falling back to copies."""
    if os.path.exists(dst_path) or os.path.islink(dst_path):
        return

    src_abs = os.path.abspath(src_path)
    try:
        os.symlink(src_abs, dst_path)
    except OSError:
        if os.path.isdir(src_abs):
            shutil.copytree(src_abs, dst_path, dirs_exist_ok=True)
        else:
            Path(os.path.dirname(dst_path)).mkdir(parents=True, exist_ok=True)
            shutil.copy2(src_abs, dst_path)


def _staged_copy_is_current(source_path: str, staged_path: str) -> bool:
    """Return whether a staged file matches source contents, including same-stat edits."""
    try:
        source_stat = os.stat(source_path)
        staged_stat = os.stat(staged_path)
    except OSError:
        return False
    return source_stat.st_size == staged_stat.st_size and file_digest(source_path) == file_digest(staged_path)


def _local_shared_libs_with_signature(exe_path: str) -> tuple[dict[str, str], str]:
    """Collect local runtime libraries and fingerprint their source metadata."""
    # A Python launcher uses the selected interpreter and environment archive.
    # Running ldd on it cannot discover the extensions loaded by that interpreter.
    local_libs = {} if Path(exe_path).suffix == ".pyz" else _transport.collect_local_shared_libs(exe_path, str(REPOSITORY_ROOT))
    digest = hashlib.sha256()
    for name, source_path in sorted(local_libs.items()):
        try:
            source_stat = os.stat(source_path)
        except OSError:
            return local_libs, ""
        record = f"{name}\0{file_digest(source_path)}\n"
        digest.update(record.encode())
    return local_libs, digest.hexdigest()


def _ensure_local_analysis_runtime_layout(
    template_config_path: str,
    config_dict: dict[str, str],
    destination_dir: str,
    include_aux: bool = True,
    payload_sources: Optional[list[str]] = None,
    prepared_payloads: Optional[dict[str, bytes]] = None,
) -> None:
    """Populate local job directories with referenced cfg/ and aux payloads."""
    cfg_dir = os.path.join(destination_dir, "cfg")
    Path(cfg_dir).mkdir(parents=True, exist_ok=True)

    sources = payload_sources
    if sources is None:
        sources = _analysis_config_payload_sources(
            template_config_path,
            config_dict,
            staged_config_dir=destination_dir,
        )
    sealed = find_bundle(template_config_path) is not None
    for src in sources:
        dst = os.path.join(cfg_dir, _asset_name(src, config_dict))
        if _legacy_asset_kind(src, config_dict) or sealed and Path(src).suffix in {".yaml", ".yml", ".txt"}:
            # Nested references differ in the worker layout. Never rewrite a
            # symlink into the immutable source bundle.
            if os.path.islink(dst):
                os.unlink(dst)
            if prepared_payloads is not None and src in prepared_payloads:
                Path(dst).write_bytes(prepared_payloads[src])
            else:
                _stage_config_asset(template_config_path, src, cfg_dir, config_dict)
        else:
            _symlink_or_copy_path(src, dst)

    _link_archived_asset_paths(template_config_path, config_dict, destination_dir, cfg_dir)

    if include_aux:
        aux_src = _analysis_aux_dir(template_config_path)
        if aux_src and os.path.isdir(aux_src):
            aux_dst = os.path.join(destination_dir, "aux")
            _symlink_or_copy_path(aux_src, aux_dst)


def _shared_inputs_archive_name() -> str:
    """Return the canonical filename for the skim shared-input archive."""
    return "shared_inputs.tar.gz"


def _validate_staged_skim_executable(shared_dir: str, exe_path: str) -> None:
    """Require the worker executable to match the current configured binary."""
    exe_path = os.path.abspath(exe_path)
    exe_name = os.path.basename(exe_path)
    archive_path = os.path.join(os.path.dirname(shared_dir), _shared_inputs_archive_name())
    staged_path = os.path.join(shared_dir, exe_name)
    remedy = (
        "Rerun PrepareSkimJobs to restage the executable and rebuild "
        "shared_inputs.tar.gz before submitting; use a new run name if the "
        "sealed bundle's execution identity has changed."
    )
    try:
        if not os.path.isfile(exe_path):
            raise RuntimeError(f"Current analysis executable not found: {exe_path!r}. {remedy}")
        expected_digest = file_digest(exe_path)
        if os.path.isfile(archive_path):
            staged_path = f"{archive_path}:{exe_name}"
            with tarfile.open(archive_path, "r:gz") as archive:
                member = next(
                    (entry for entry in archive if entry.name in (exe_name, f"./{exe_name}")),
                    None,
                )
                if member is None or not member.isfile():
                    raise RuntimeError(
                        f"Staged executable {staged_path!r} is missing or is not a regular file. {remedy}"
                    )
                digest = hashlib.sha256()
                with archive.extractfile(member) as source:
                    for chunk in iter(lambda: source.read(1024 * 1024), b""):
                        digest.update(chunk)
                staged_digest = digest.hexdigest()
        else:
            if not os.path.isfile(staged_path):
                raise RuntimeError(f"Staged executable not found: {staged_path!r}. {remedy}")
            staged_digest = file_digest(staged_path)
        if staged_digest != expected_digest:
            raise RuntimeError(
                f"Staged executable {staged_path!r} does not match the current "
                f"--exe binary {exe_path!r} (SHA-256 differs). {remedy}"
            )
    except (OSError, tarfile.TarError) as exc:
        raise RuntimeError(
            f"Cannot verify staged executable {staged_path!r} against {exe_path!r}: {exc}. {remedy}"
        ) from exc


def _validate_x509_proxy(proxy_path: str, *, label: Optional[str] = None) -> None:
    """Require at least two days of validity on a staged X509 proxy."""
    description = label or proxy_path
    remedy = (
        "Renew the proxy (for CMS: voms-proxy-init -voms cms -rfc --valid 168:0) "
        "and rerun PrepareSkimJobs to restage it and rebuild shared_inputs.tar.gz "
        "before submitting."
    )
    try:
        result = subprocess.run(
            ["voms-proxy-info", "-file", proxy_path, "-timeleft"],
            capture_output=True, text=True, check=True, timeout=30,
        )
        seconds_left = int(result.stdout.strip())
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        raise RuntimeError(
            f"Cannot verify staged X509 proxy {description!r}; "
            f"at least 48 hours (172800 seconds) of validity are required. {remedy}"
        ) from exc
    if seconds_left < 172800:
        raise RuntimeError(
            f"Staged X509 proxy {description!r} has {seconds_left} seconds remaining; "
            f"at least 48 hours (172800 seconds) are required. {remedy}"
        )


def _validate_staged_skim_proxy(shared_dir: str, *, required: bool = False) -> None:
    """Check the credential workers receive, preferring the archived copy."""
    proxy_path = os.path.join(shared_dir, "x509")
    archive_path = os.path.join(os.path.dirname(shared_dir), _shared_inputs_archive_name())
    if not os.path.isfile(archive_path):
        if required or os.path.lexists(proxy_path):
            _validate_x509_proxy(proxy_path)
        return

    try:
        with tarfile.open(archive_path, "r:gz") as archive:
            member = next(
                (entry for entry in archive if entry.name in ("x509", "./x509")),
                None,
            )
            if member is None:
                if required or os.path.lexists(proxy_path):
                    raise RuntimeError(
                        f"Staged X509 proxy is missing from {archive_path!r}. "
                        "Rerun PrepareSkimJobs with --x509 to rebuild the archive "
                        "with a proxy valid for at least 48 hours."
                    )
                return
            if not member.isfile():
                raise RuntimeError(f"Staged X509 proxy in {archive_path!r} is not a regular file.")
            # Do not extract archive paths or retain a second credential copy.
            with archive.extractfile(member) as source, tempfile.TemporaryDirectory(
                prefix="skim_proxy_check_"
            ) as tmpdir:
                check_path = os.path.join(tmpdir, "x509")
                with open(check_path, "xb", opener=lambda path, flags: os.open(path, flags, 0o600)) as target:
                    shutil.copyfileobj(source, target)
                _validate_x509_proxy(check_path, label=f"{archive_path}:x509")
    except (OSError, tarfile.TarError) as exc:
        raise RuntimeError(f"Cannot inspect staged proxy in {archive_path!r}: {exc}") from exc


def _build_shared_inputs_archive(shared_dir: str, archive_path: str) -> str:
    """Create a tar.gz archive with materialized cfg/aux and shared runtime files."""
    cfg_bundle = os.path.join(shared_dir, "cfg_bundle.tar.gz")
    aux_bundle = os.path.join(shared_dir, "aux_bundle.tar.gz")
    excluded_files = {
        "cfg_bundle.tar.gz",
        "aux_bundle.tar.gz",
        os.path.basename(archive_path),
    }

    with tempfile.TemporaryDirectory(prefix="skim_shared_inputs_") as tmpdir:
        Path(os.path.join(tmpdir, "cfg")).mkdir(parents=True, exist_ok=True)
        Path(os.path.join(tmpdir, "aux")).mkdir(parents=True, exist_ok=True)

        if os.path.isfile(cfg_bundle):
            _extract_tar_archive(cfg_bundle, tmpdir)
        if os.path.isfile(aux_bundle):
            _extract_tar_archive(aux_bundle, tmpdir)

        # Publish only a closed archive. Readers and retries can keep using
        # the previous generation if copying any member fails.
        with tempfile.TemporaryDirectory(dir=Path(archive_path).parent) as staging:
            pending = Path(staging) / "shared_inputs.tar.gz"
            with tarfile.open(pending, "w:gz") as archive:
                for entry_name in ("cfg", "aux", "assets"):
                    entry_path = os.path.join(tmpdir, entry_name)
                    if os.path.isdir(entry_path):
                        archive.add(entry_path, arcname=entry_name)
                for entry in sorted(Path(shared_dir).iterdir()):
                    if not entry.is_file() or entry.name in excluded_files:
                        continue
                    archive.add(str(entry), arcname=entry.name)
            os.replace(pending, archive_path)

    return archive_path


def _normalize_shared_config_references(
    template_config_path: str,
    job_config: dict[str, str],
    staged_payload_names: Optional[set[str]] = None,
) -> None:
    """Rewrite referenced payload files to the runtime cfg/ layout."""
    staged_payloads = set(staged_payload_names or set())
    if staged_payload_names is None:
        for path_value in get_copy_file_list(job_config):
            if not path_value:
                continue
            source_path = _resolve_analysis_config_path(template_config_path, path_value)
            if os.path.isfile(source_path):
                staged_payloads.add(_asset_name(source_path, job_config))
    for key, val in list(job_config.items()):
        if key.startswith("__"):
            continue
        if isinstance(val, str) and (
            val.endswith(".txt")
            or val.endswith(".yaml")
            or val.endswith(".yml")
            or val.endswith(".json")
            or val.endswith(".json.gz")
            or val.endswith(".onnx")
        ):
            basename = _asset_name(val, job_config)
            if basename in staged_payloads:
                job_config[key] = os.path.join("cfg", basename)


def _resolve_runtime_transfer_path(path_value: str) -> str:
    """Resolve a runtime path, preferring local originals before transferred copies."""
    if path_value and os.path.exists(path_value):
        return path_value
    local_candidate = os.path.join(os.getcwd(), os.path.basename(path_value))
    if os.path.exists(local_candidate):
        return local_candidate
    return path_value


def _runtime_loose_cfg_files(exclude_names: Optional[set[str]] = None) -> list[str]:
    """Discover loose config payload files transferred into the current work dir."""
    exclude = set(exclude_names or set())
    discovered: list[str] = []
    for entry_name in sorted(os.listdir(os.getcwd())):
        if entry_name in exclude:
            continue
        entry_path = os.path.join(os.getcwd(), entry_name)
        if not os.path.isfile(entry_path):
            continue
        if not entry_name.lower().endswith((".txt", ".yaml", ".yml")):
            continue
        discovered.append(entry_path)
    return discovered


def _copy_runtime_loose_cfg_files(destination_cfg_dir: str, exclude_names: Optional[set[str]] = None) -> None:
    """Copy transferred loose config files into the runtime cfg/ directory."""
    for src_path in _runtime_loose_cfg_files(exclude_names=exclude_names):
        dst_path = os.path.join(destination_cfg_dir, os.path.basename(src_path))
        if os.path.exists(dst_path):
            continue
        shutil.copy2(src_path, dst_path)


def _materialize_runtime_staged_inputs(runtime_cfg_path: str, sandbox_dir: str) -> None:
    """Link staged worker input files into the runtime sandbox when present."""
    config_dict = read_config(runtime_cfg_path)
    file_list = str(config_dict.get("fileList", "") or "")
    if not file_list:
        return

    staged_names = [
        entry.strip()
        for entry in file_list.split(",")
        if re.fullmatch(r"input_\d+\.root", entry.strip())
    ]
    if not staged_names:
        return

    missing: list[str] = []
    for staged_name in staged_names:
        src_path = os.path.join(os.getcwd(), staged_name)
        dst_path = os.path.join(sandbox_dir, staged_name)
        if os.path.exists(dst_path):
            continue
        if not os.path.exists(src_path):
            missing.append(staged_name)
            continue
        try:
            os.symlink(src_path, dst_path)
        except OSError:
            shutil.copy2(src_path, dst_path)

    if not missing:
        return

    original_file_list = str(config_dict.get("__orig_fileList", "") or "")
    if original_file_list:
        config_dict["fileList"] = original_file_list
        write_config(config_dict, runtime_cfg_path)


def _skim_shared_loose_input_files(shared_dir: str) -> list[str]:
    """Return shared input files that should be transferred individually."""
    excluded = {
        "cfg_bundle.tar.gz",
        "aux_bundle.tar.gz",
        _shared_inputs_archive_name(),
    }
    return sorted(
        os.path.join(shared_dir, entry)
        for entry in os.listdir(shared_dir)
        if os.path.isfile(os.path.join(shared_dir, entry)) and entry not in excluded
    )


def _shared_archive_top_level_files(archive_path: str) -> set[str]:
    """Return files the archive supplies at the worker sandbox root."""
    if not os.path.isfile(archive_path):
        return set()

    with tarfile.open(archive_path, "r:gz") as archive:
        return {
            Path(member.name).name
            for member in archive.getmembers()
            if member.isfile() and Path(member.name).parent == Path(".")
        }


def _copy_job_submission_files(
    source_dir: str,
    destination_dir: str,
    shared_archive_file_names: Optional[set[str]] = None,
) -> None:
    """Copy per-job payloads without expanding shared configuration overlays.

    ``PrepareSkimJobs`` keeps a local ``cfg/`` overlay so local execution can
    use the same prepared job directory.  Remote skim jobs receive the
    equivalent payload through ``shared_inputs.tar.gz`` instead, so copying
    that overlay into every concrete Condor job would duplicate potentially
    large correction/configuration files.  Keep job-specific files (the
    submit config, redirector map, and any future per-job payloads) while
    omitting directories and files represented by the shared archive.
    """
    shared_names = set(shared_archive_file_names or set())
    per_job_names = {"submit_config.txt", "submit_config.yaml", "site_redirectors.json"}
    original = job_config_path(source_dir)
    for entry in sorted(Path(source_dir).iterdir()):
        if entry.name in {"submit_config.txt", "submit_config.yaml"}:
            if entry == original:
                write_config(read_config(entry), Path(destination_dir) / "submit_config.yaml")
            continue
        if entry.name in {"cfg", "aux"}:
            continue
        if entry.name in shared_names and entry.name not in per_job_names:
            continue

        destination = os.path.join(destination_dir, entry.name)
        if entry.is_dir():
            shutil.copytree(str(entry), destination, dirs_exist_ok=True)
        elif entry.is_file():
            shutil.copy2(str(entry), destination)


def _copy_tree_contents(src_dir: str, dst_dir: str) -> None:
    """Copy all files/directories under src_dir into dst_dir."""
    if not os.path.isdir(src_dir):
        return
    Path(dst_dir).mkdir(parents=True, exist_ok=True)
    for entry in sorted(Path(src_dir).iterdir()):
        dst_path = os.path.join(dst_dir, entry.name)
        if entry.is_dir():
            shutil.copytree(str(entry), dst_path, dirs_exist_ok=True)
        elif entry.is_file():
            shutil.copy2(str(entry), dst_path)


def _extract_tar_archive(archive_path: str, destination_dir: str) -> None:
    """Extract a staged archive with the standard library's data filter."""
    data_filter = getattr(tarfile, "data_filter", None)
    if not callable(data_filter):
        raise RuntimeError(
            "Worker archive extraction requires tarfile.data_filter. Use Python 3.12 "
            "or newer, or an interpreter with the extraction-filter backport."
        )
    with tarfile.open(archive_path, "r:gz") as archive:
        archive.extractall(destination_dir, filter=data_filter)


def _python_environment_setup(root_setup: str, directory: str) -> str:
    path = shlex.quote(os.path.abspath(directory))
    return "\n".join((
        root_setup,
        f'export PYTHONPATH={path}:${{PYTHONPATH:-}}',
        f'export PATH={path}/bin:${{PATH:-}}',
        f'export LD_LIBRARY_PATH={path}:${{LD_LIBRARY_PATH:-}}',
        f'export ROOT_INCLUDE_PATH={path}${{ROOT_INCLUDE_PATH:+:$ROOT_INCLUDE_PATH}}',
    ))


def _materialize_file_source_runtime_job(
    *,
    shared_dir: str,
    source_job_dir: str,
    exe_relpath: str,
    source_config_path: Optional[str] = None,
    shared_archive_path: Optional[str] = None,
) -> tuple[str, str]:
    """Create a worker-local runtime directory with cfg/ and aux/ materialized."""
    runtime_shared_dir = _resolve_runtime_transfer_path(shared_dir)
    runtime_job_dir = _resolve_runtime_transfer_path(source_job_dir)
    runtime_shared_archive = ""
    if shared_archive_path:
        runtime_shared_archive = _resolve_runtime_transfer_path(shared_archive_path)

    if not os.path.isdir(runtime_shared_dir) and not (
        runtime_shared_archive and os.path.isfile(runtime_shared_archive)
    ):
        raise RuntimeError(
            "Shared inputs are not available at runtime. Expected either a shared "
            f"directory {runtime_shared_dir!r} or archive {runtime_shared_archive!r}."
        )
    if source_job_dir and not os.path.isdir(runtime_job_dir):
        runtime_job_dir = ""

    sandbox_dir = tempfile.mkdtemp(prefix="skim_runtime_")

    if runtime_shared_archive and os.path.isfile(runtime_shared_archive):
        _extract_tar_archive(runtime_shared_archive, sandbox_dir)

    if os.path.isdir(runtime_shared_dir):
        cfg_bundle = os.path.join(runtime_shared_dir, "cfg_bundle.tar.gz")
        if os.path.isfile(cfg_bundle):
            _extract_tar_archive(cfg_bundle, sandbox_dir)

        aux_bundle = os.path.join(runtime_shared_dir, "aux_bundle.tar.gz")
        if os.path.isfile(aux_bundle):
            _extract_tar_archive(aux_bundle, sandbox_dir)

    runtime_cfg_dir = os.path.join(sandbox_dir, "cfg")
    Path(runtime_cfg_dir).mkdir(parents=True, exist_ok=True)

    submit_config_src = ""
    if source_config_path:
        submit_config_src = _resolve_runtime_transfer_path(source_config_path)
    elif runtime_job_dir:
        submit_config_src = str(job_config_path(runtime_job_dir))

    if not os.path.isfile(submit_config_src):
        raise RuntimeError(
            f"Per-job submit config not found at runtime: {submit_config_src!r}."
        )
    runtime_submit_config = os.path.join(runtime_cfg_dir, "submit_config.yaml")

    if runtime_job_dir:
        job_cfg_overlay = os.path.join(runtime_job_dir, "cfg")
        if os.path.isdir(job_cfg_overlay):
            _copy_tree_contents(job_cfg_overlay, runtime_cfg_dir)

    _copy_runtime_loose_cfg_files(
        runtime_cfg_dir,
        exclude_names={os.path.basename(submit_config_src), "submit_config.yaml"},
    )

    write_config(read_config(submit_config_src), runtime_submit_config)
    _materialize_runtime_staged_inputs(runtime_submit_config, sandbox_dir)

    if os.path.isdir(runtime_shared_dir):
        for src_path in _skim_shared_loose_input_files(runtime_shared_dir):
            runtime_src_path = _resolve_runtime_transfer_path(src_path)
            if os.path.isfile(runtime_src_path):
                shutil.copy2(
                    runtime_src_path,
                    os.path.join(sandbox_dir, os.path.basename(src_path)),
                )

    exe_path = os.path.join(sandbox_dir, exe_relpath)
    if not os.path.isfile(exe_path):
        raise RuntimeError(
            f"Executable not found in runtime sandbox: {exe_path!r}."
        )
    return sandbox_dir, exe_path


def _run_prepared_skim_job(
    shared_dir: str,
    source_job_dir: str,
    exe_relpath: str,
    root_setup: str = "",
    container_setup: str = "",
    source_config_path: Optional[str] = None,
    shared_archive_path: Optional[str] = None,
    stream_output: bool = False,
    worker_number: Optional[int] = None,
    python_env_name: str = "",
) -> str:
    """Run a prepared skim job after materializing a worker-local sandbox."""
    runtime_job_dir = None
    try:
        runtime_job_dir, exe_path = _materialize_file_source_runtime_job(
            shared_dir=shared_dir,
            source_job_dir=source_job_dir,
            exe_relpath=exe_relpath,
            source_config_path=source_config_path,
            shared_archive_path=shared_archive_path,
        )
        if python_env_name:
            destination = os.path.join(runtime_job_dir, "_python_env")
            _extract_tar_archive(os.path.join(runtime_job_dir, python_env_name), destination)
            root_setup = _python_environment_setup(root_setup, destination)
        return _executors._run_analysis_job(
            exe_path=exe_path,
            job_dir=runtime_job_dir,
            root_setup=root_setup,
            container_setup=container_setup,
            config_relpath=os.path.join("cfg", job_config_path(os.path.join(runtime_job_dir, "cfg")).name),
            stream_output=stream_output,
            worker_number=worker_number,
        )
    finally:
        if runtime_job_dir and os.path.isdir(runtime_job_dir):
            shutil.rmtree(runtime_job_dir, ignore_errors=True)


def _rewrite_submit_transfer_files(
    submit_path: str,
    banned_suffixes: Optional[list[str]] = None,
    banned_exact_paths: Optional[list[str]] = None,
    appended_paths: Optional[list[str]] = None,
) -> None:
    """Rewrite transfer-input entries in a generated Condor submit file."""
    banned_suffixes = banned_suffixes or []
    banned_exact = set(banned_exact_paths or [])
    appended_paths = appended_paths or []

    with open(submit_path) as fh:
        lines = fh.readlines()

    rewritten: list[str] = []
    for line in lines:
        stripped = line.lstrip()
        if not stripped.lower().startswith("transfer_input_files ="):
            rewritten.append(line)
            continue

        prefix, value = line.split("=", 1)
        items = [item.strip() for item in value.strip().split(",") if item.strip()]
        kept = [
            item
            for item in items
            if item not in banned_exact
            and not any(item.endswith(suffix) for suffix in banned_suffixes)
        ]
        for path in appended_paths:
            if path not in kept:
                kept.append(path)
        rewritten.append(f"{prefix}= {','.join(kept)}\n")

    with open(submit_path, "w") as fh:
        fh.writelines(rewritten)


def _skim_shared_input_files(shared_dir: str) -> list[str]:
    """Return concrete shared-input files to transfer one-by-one for skim jobs."""
    archive_path = os.path.join(os.path.dirname(shared_dir), _shared_inputs_archive_name())
    if os.path.isfile(archive_path):
        return [archive_path]
    return _skim_shared_loose_input_files(shared_dir)


def _rewrite_staged_analysis_config(
    template_config_path: str,
    source_config_path: str,
    staged_config_path: str,
) -> None:
    """Rewrite staged YAML payload references to the worker's ``aux/`` tree.

    Payloads may be nested below ``aux/`` (for example
    ``aux/payloads/run3_2022_preEE/...``). Preserve that relative layout when
    rewriting paths; using only the basename can both miss nested files and
    select the wrong era when basenames are repeated.
    """
    bundle = find_bundle(template_config_path)
    if bundle:
        record = json.loads((bundle / "bundle_manifest.json").read_text())
        replacements = {}
        prefix = "cfg/" if Path(staged_config_path).parent.name == "cfg" else ""
        for source, relative in record["assets"].items():
            runtime = prefix + Path(relative).name
            replacements.update({source: runtime, relative: runtime,
                                 str(bundle / relative): runtime})
        path = Path(staged_config_path)
        if path.suffix in {".yaml", ".yml"}:
            from native_config_io import load_yaml_document, write_config_yaml
            value = load_yaml_document(path)
            rewritten = rewrite_references(value, replacements)
            if rewritten != value:
                write_config_yaml(rewritten, path)
        return
    if not staged_config_path.lower().endswith((".yaml", ".yml")):
        return

    aux_src = _analysis_aux_dir(template_config_path)
    if not aux_src or not os.path.isdir(aux_src):
        return
    aux_root = Path(aux_src).resolve()
    aux_files_by_name: dict[str, list[Path]] = {}
    for aux_entry in aux_root.rglob("*"):
        if aux_entry.is_file():
            aux_files_by_name.setdefault(aux_entry.name, []).append(aux_entry)

    from native_config_io import load_yaml_document, write_config_yaml
    payload = load_yaml_document(staged_config_path)

    def _worker_aux_path(value: str) -> Optional[str]:
        value = value.strip()
        if not value or value.startswith(("root://", "http://", "https://")):
            return None

        candidate = Path(value).expanduser()
        if not candidate.is_absolute():
            resolved_value = _resolve_analysis_config_path(source_config_path, value)
            candidate = Path(resolved_value).expanduser()
        try:
            resolved_candidate = candidate.resolve()
            relative = resolved_candidate.relative_to(aux_root)
        except (OSError, RuntimeError, ValueError):
            relative = None
        if relative is not None and resolved_candidate.is_file():
            return (Path("aux") / relative).as_posix()

        # Keep support for configs that name a payload by basename. Only
        # rewrite it when that basename uniquely identifies a file in aux.
        if os.path.isabs(value) or os.path.sep in value:
            return None
        matches = aux_files_by_name.get(value, [])
        if len(matches) == 1:
            return (Path("aux") / matches[0].relative_to(aux_root)).as_posix()
        return None

    def _rewrite(node):
        if isinstance(node, list):
            return [_rewrite(item) for item in node]
        if isinstance(node, dict):
            rewritten: dict[object, object] = {}
            for key, value in node.items():
                if isinstance(key, str) and key.lower().endswith("file") and isinstance(value, str):
                    worker_path = _worker_aux_path(value)
                    if worker_path is not None:
                        rewritten[key] = worker_path
                        continue
                rewritten[key] = _rewrite(value)
            return rewritten
        return node

    rewritten_payload = _rewrite(payload)
    if rewritten_payload == payload:
        return

    write_config_yaml(rewritten_payload, staged_config_path)
