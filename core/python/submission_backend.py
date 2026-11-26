import base64
import hashlib
import os
import shlex
import warnings
from pathlib import Path

import yaml

if __package__:
    from . import io_policy as _io_policy
else:
    import io_policy as _io_policy


if __package__:
    from . import native_config_io as _config_io
else:
    import native_config_io as _config_io

# Preserve the public submission_backend API used by existing scripts.
read_config = _config_io.read_config
read_config_text = _config_io.read_config_text
read_config_yaml = _config_io.read_config_yaml
parse_config_text = _config_io.parse_config_text
write_config = _config_io.write_config
write_config_yaml = _config_io.write_config_yaml
get_config_extension = _config_io.get_config_extension


def _worker_config_io():
    """Embed the same decoder in workers which run without a source checkout."""
    return Path(_config_io.__file__).read_text(encoding="utf-8")


def get_copy_file_list(config_dict):
    transfer_list = []
    for key in config_dict:
        value = config_dict[key]
        if (".txt" in value or ".yaml" in value or ".yml" in value
                or ".json" in value or ".onnx" in value
                or (key == "xrdSiteRankingFile" and value not in ("auto", "none", ""))):
            transfer_list.append(value)
    return transfer_list


def ensure_xrootd_redirector(uri, redirector="root://eospublic.cern.ch/"):
    """Ensure *uri* is addressed via an XRootD redirector.

    If the URI already starts with ``root://`` it is returned unchanged.
    A bare EOS path (starting with ``/``) is prefixed with *redirector* so
    that the file can be accessed from any worker node regardless of whether
    the EOS file system is locally mounted.  This decouples the storage
    location from the job running location.

    Args:
        uri:        File URI as returned by the CERN Open Data API or Rucio.
        redirector: XRootD redirector to prepend when the URI has no scheme.
                    Defaults to ``root://eospublic.cern.ch/``.

    Returns:
        URI guaranteed to start with ``root://``, or the original value if
        it has no leading ``/`` and is not a ``root://`` URI.
    """
    if not uri:
        return uri
    if uri.startswith("root://"):
        # Normalize root:// URLs to always use a double-slash after the host.
        # Some XRootD pools require the "//store/..." form, and os.path-like
        # operations can accidentally collapse it into "/store/...".
        parts = uri.split("/", 3)
        if len(parts) >= 4 and not parts[3].startswith("/"):
            # root://host/store/foo -> root://host//store/foo
            return f"{parts[0]}//{parts[2]}//{parts[3]}"
        return uri

    if uri.startswith("/"):
        # Preserve local absolute paths for local-mode workflows. Only EOS-like
        # storage paths should be normalized through an XRootD redirector.
        if not (uri.startswith("/store/") or uri.startswith("/eos/")):
            return uri

        # Ensure we produce a double-slash after the redirector
        # (e.g. root://host//store/...) while avoiding a triple-slash artifact.
        if not uri.startswith("//"):
            uri = "//" + uri.lstrip("/")
        return redirector.rstrip("/") + uri

    # If the URI looks like an EOS path without a leading slash, treat it as such.
    if uri.startswith("store/") or uri.startswith("eos/"):
        uri = "/" + uri
        return redirector.rstrip("/") + "//" + uri.lstrip("/")

    print(uri)
    return uri


def ensure_symlink(src, dst):
    if os.path.islink(dst) or os.path.exists(dst):
        return
    os.symlink(src, dst)


def _worker_input_block(config_file, stage_inputs):
    """Resolve public input URLs without launching worker-side site probes."""
    return f"""
run_helper_python - << 'RDFINPUT'
import os
import shutil
import subprocess
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode

GLOBAL_REDIRECTOR = {_io_policy.GLOBAL_REDIRECTOR!r}
PUBLIC_STORAGE_PREFIXES = {_io_policy.PUBLIC_STORAGE_PREFIXES!r}
SITE_REDIRECTOR_MARKER = {_io_policy.SITE_REDIRECTOR_MARKER!r}
STAGE_INPUTS = {stage_inputs!r}
STAGE_COPY_TIMEOUT = 60
MAX_ATTEMPTS = 3
cfg_path = {config_file!r}

{_worker_config_io()}
cfg = read_config(cfg_path)


def global_url(value):
    parsed = urlsplit(value)
    if parsed.scheme not in ('', 'root') or (not parsed.scheme and not value.startswith('/store/')):
        return value
    path = parsed.path
    marker_at = path.find(SITE_REDIRECTOR_MARKER)
    if marker_at >= 0:
        site_end = path.find('/', marker_at + len(SITE_REDIRECTOR_MARKER))
        if site_end >= 0:
            path = path[site_end:]
    path = '/' + path.lstrip('/')
    if not GLOBAL_REDIRECTOR or not any(path.startswith(prefix) for prefix in PUBLIC_STORAGE_PREFIXES):
        return value
    endpoint = urlsplit(GLOBAL_REDIRECTOR)
    query = urlencode([(key, val) for key, val in parse_qsl(parsed.query, keep_blank_values=True)
                       if key not in ('tried', 'triedrc')])
    return urlunsplit((endpoint.scheme, endpoint.netloc, '/' + path, query, parsed.fragment))

raw_files = cfg.get('fileList', '')
if raw_files.startswith('[') and raw_files.endswith(']'):
    raw_files = raw_files[1:-1]
files = raw_files.split(',')
urls = [global_url(str(value).strip()) for value in files if str(value).strip()]
if STAGE_INPUTS:
    print('Staging input files with xrdcp (no redirector probing)')
    local_paths = []
    for index, url in enumerate(urls):
        local_name = 'input_{{}}.root'.format(index)
        if not urlsplit(url).scheme and os.path.isfile(url):
            if os.path.abspath(url) != os.path.abspath(local_name):
                shutil.copyfile(url, local_name)
        else:
            for attempt in range(1, MAX_ATTEMPTS + 1):
                try:
                    subprocess.run(['xrdcp', '-f', '--nopbar', '--streams',
                                    os.environ.get('XRDCP_STREAMS', '4'), url, local_name],
                                   check=True, timeout=STAGE_COPY_TIMEOUT)
                    break
                except (subprocess.CalledProcessError, subprocess.TimeoutExpired):
                    if attempt == MAX_ATTEMPTS:
                        raise
        local_paths.append(local_name)
    urls = local_paths
    print('[stage-in] {{}} file(s) staged successfully'.format(len(urls)))
else:
    print('[input] Using global redirector for public inputs; no worker site probing')
if urls:
    cfg['fileList'] = ','.join(urls)
    write_config(cfg, runtime_yaml_path(cfg_path))
RDFINPUT
"""


def _warn_ignored_option(value, name):
    if value:
        warnings.warn(
            f"{name} is deprecated and has no effect; remove this argument. "
            "Nondefault values will be rejected at a future compatibility boundary.",
            FutureWarning,
            stacklevel=3,
        )


def stage_inputs_block(eos_sched=False, config_file="submit_config.yaml"):
    _warn_ignored_option(eos_sched, 'eos_sched')
    return _worker_input_block(config_file, stage_inputs=True)


def stage_outputs_blocks(eos_sched=False, config_file="submit_config.yaml"):
    _warn_ignored_option(eos_sched, 'eos_sched')
    pre_block = f"""
run_helper_python - << 'PY'
import os

{_worker_config_io()}
cfg = read_config("{config_file}")

save_file = cfg.get("saveFile", "")
meta_file = cfg.get("metaFile", "")

if "__orig_saveFile" in cfg and cfg.get("__orig_saveFile"):
    cfg["saveFile"] = os.path.basename(cfg["__orig_saveFile"])
elif save_file:
    cfg["__orig_saveFile"] = save_file
    cfg["saveFile"] = os.path.basename(save_file)

if "__orig_metaFile" in cfg and cfg.get("__orig_metaFile"):
    cfg["metaFile"] = os.path.basename(cfg["__orig_metaFile"])
elif meta_file:
    cfg["__orig_metaFile"] = meta_file
    cfg["metaFile"] = os.path.basename(meta_file)

proc_id = os.environ.get("CONDOR_PROC", "")
if proc_id and cfg.get("saveFile"):
    base, ext = os.path.splitext(cfg["saveFile"])
    cfg["saveFile"] = f"{{base}}_{{proc_id}}{{ext}}"
if proc_id and cfg.get("metaFile"):
    base, ext = os.path.splitext(cfg["metaFile"])
    cfg["metaFile"] = f"{{base}}_{{proc_id}}{{ext}}"

write_config(cfg, runtime_yaml_path("{config_file}"))
PY
"""

    post_block = f"""
run_helper_python - << 'PY'
import os
import subprocess
import time

XRDFS_TIMEOUT = int(os.environ.get("XRDFS_TIMEOUT", "30"))
XRDFS_VERIFY_RETRIES = int(os.environ.get("XRDFS_VERIFY_RETRIES", "4"))
XRDFS_VERIFY_DELAY = float(os.environ.get("XRDFS_VERIFY_DELAY", "5"))
XRDCP_TIMEOUT_FLOOR = int(os.environ.get("XRDCP_TIMEOUT_FLOOR", "300"))
XRDCP_TIMEOUT_CEILING = int(os.environ.get("XRDCP_TIMEOUT_CEILING", "1800"))
XRDCP_TIMEOUT_BYTES_PER_SEC = int(
    os.environ.get("XRDCP_TIMEOUT_BYTES_PER_SEC", str(20 * 1024 * 1024))
)

{_worker_config_io()}
cfg = read_config(runtime_yaml_path("{config_file}"))

def normalize_dest(dest):
    if not dest:
        return dest
    if dest.startswith("root://"):
        return dest
    return "root://eosuser.cern.ch/" + dest

def split_xrootd_dest(dest):
    normalized = normalize_dest(dest)
    if not normalized.startswith("root://"):
        raise RuntimeError(f"Unsupported stage-out destination: {{dest}}")
    without_scheme = normalized[len("root://"):]
    host, _, remainder = without_scheme.partition("/")
    if not host:
        raise RuntimeError(f"Could not parse XRootD host from destination: {{normalized}}")
    path = "/" + remainder.lstrip("/")
    return normalized, f"root://{{host}}/", path

def eos_dir_from_dest(dest):
    if dest.startswith("root://"):
        _, _, path_part = split_xrootd_dest(dest)
    else:
        path_part = dest
    return os.path.dirname("/" + path_part.lstrip("/"))

def xrdfs_ok(host_url, *args, timeout=XRDFS_TIMEOUT):
    try:
        result = subprocess.run(
            ["xrdfs", host_url, *args],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            timeout=timeout,
        )
        return result.returncode == 0, result.stdout.strip() or result.stderr.strip()
    except Exception as exc:
        return False, str(exc)

_attempted_output_dirs = set()

def ensure_output_endpoint_ready(dest):
    normalized_dest, host_url, _ = split_xrootd_dest(dest)
    eos_dir = eos_dir_from_dest(normalized_dest)
    if not eos_dir or eos_dir == "/":
        return

    output_dir_key = (host_url, eos_dir)
    if output_dir_key in _attempted_output_dirs:
        return
    _attempted_output_dirs.add(output_dir_key)

    mkdir_ok, mkdir_msg = xrdfs_ok(host_url, "mkdir", "-p", eos_dir)
    if not mkdir_ok:
        print(
            f"Warning: xrdfs mkdir -p {{eos_dir}} failed; "
            f"trying xrdcp stage-out anyway: {{mkdir_msg}}"
        )

def verify_remote_file(dest, retries=XRDFS_VERIFY_RETRIES, delay=XRDFS_VERIFY_DELAY):
    normalized_dest, host_url, remote_path = split_xrootd_dest(dest)
    last_err = ""
    for attempt in range(1, retries + 1):
        ok, msg = xrdfs_ok(host_url, "stat", remote_path)
        if ok:
            return
        last_err = msg or f"xrdfs stat failed for {{remote_path}}"
        print(f"xrdfs remote verification attempt {{attempt}} failed for {{normalized_dest}}: {{last_err}}")
        time.sleep(delay * attempt)

    raise RuntimeError(f"Remote file not visible after stage-out: {{normalized_dest}} ({{last_err}})")

def compute_xrdcp_timeout(local_name):
    size_bytes = os.path.getsize(local_name)
    estimated = XRDCP_TIMEOUT_FLOOR + int(size_bytes / max(XRDCP_TIMEOUT_BYTES_PER_SEC, 1))
    return max(XRDCP_TIMEOUT_FLOOR, min(XRDCP_TIMEOUT_CEILING, estimated))

def xrdcp_if_exists(local_name, dest, retries=3, timeout=120, streams=None):
    if not dest:
        print("no dest")
        return
    if not os.path.exists(local_name):
        print("local path doesn't exist")
        return
    if os.path.getsize(local_name) == 0:
        raise RuntimeError(f"Refusing to stage out empty file: {{local_name}}")

    normalized_dest = normalize_dest(dest)
    effective_timeout = max(timeout, compute_xrdcp_timeout(local_name))
    print(local_name, normalized_dest, retries, effective_timeout, streams)

    if streams is None:
        streams = os.environ.get("XRDCP_STREAMS", "4")

    ensure_output_endpoint_ready(normalized_dest)

    cmd = [
        "xrdcp",
        "-f",
        "--nopbar",
        "--streams",
        str(streams),
        "--retry",
        "3",
        local_name,
        normalized_dest,
    ]
    last_exc = None
    for attempt in range(1, retries + 1):
        print(attempt, cmd)
        try:
            subprocess.run(cmd, check=True, timeout=effective_timeout)
            verify_remote_file(normalized_dest)
            return
        except Exception as exc:
            last_exc = exc
            print(f"xrdcp attempt {{attempt}} failed for {{normalized_dest}}: {{exc}}")
            time.sleep(min(5 * attempt, 30))

    raise RuntimeError(f"xrdcp failed after {{retries}} attempt(s) for {{normalized_dest}}: {{last_exc}}")

orig_save = cfg.get("__orig_saveFile", "")
orig_meta = cfg.get("__orig_metaFile", "")
save_file = cfg.get("saveFile", "")
meta_file = cfg.get("metaFile", "")

if orig_save and save_file:
    xrdcp_if_exists(save_file, orig_save)
if orig_meta and meta_file:
    xrdcp_if_exists(meta_file, orig_meta)
PY
"""

    return pre_block, post_block


def xrootd_optimize_block(config_file="submit_config.yaml", blacklisted_sites=None):
    """Compatibility entry point: global URL routing, without site probing.

    Site quality and blacklisting are handled by the input adaptor. The legacy
    ``blacklisted_sites`` argument is accepted for source compatibility only;
    nonempty requests warn because they have no effect in this helper.
    """
    _warn_ignored_option(blacklisted_sites, 'blacklisted_sites')
    return _worker_input_block(config_file, stage_inputs=False)


def _site_ranking_block(site_ranking_file=None):
    """Freeze a validated ranking in the worker script; no checkout on workers.

    Embedding is deliberate: every submission path already ships its script,
    including shared archives, containers and per-job transfer lists.
    """
    source = site_ranking_file
    if source is None:
        if __package__:
            from .xrd_site_rankings import submission_ranking_source
        else:
            from xrd_site_rankings import submission_ranking_source
        source = submission_ranking_source(_io_policy.SITE_RANKING_FILE)
    if source == "none":
        return "export RDF_XRD_SITE_RANKING_FILE=none\n" + _site_origin_block()
    if not source:
        return _site_origin_block()
    if __package__:
        from .xrd_site_rankings import read_snapshot
    else:
        from xrd_site_rankings import read_snapshot
    read_snapshot(source)
    payload = Path(source).read_bytes()
    if len(payload) > 4 * 1024 * 1024:
        raise ValueError("XRootD site ranking snapshot exceeds 4 MiB")
    encoded = base64.b64encode(payload).decode("ascii")
    digest = hashlib.sha256(payload).hexdigest()
    return f"""
# Immutable snapshot selected on the submission host, materialized after setup.
run_helper_python - <<'RDF_SITE_RANKING'
import base64, hashlib
payload = base64.b64decode({encoded!r})
assert hashlib.sha256(payload).hexdigest() == {digest!r}
with open('rdf_xrd_site_rankings.tsv', 'wb') as stream:
    stream.write(payload)
print('[input] XRootD ranking snapshot sha256={digest}', flush=True)
RDF_SITE_RANKING
export RDF_XRD_SITE_RANKING_FILE="$PWD/rdf_xrd_site_rankings.tsv"
""" + _site_origin_block()


def _site_origin_block():
    # Evaluate site tags on the worker, never freeze the submitter's location.
    variables = " ".join(_io_policy.SITE_ENV_VARS)
    return f"""
if [ -z "${{RDF_XRD_SITE_ORIGIN:-}}" ]; then
  for _rdf_site_variable in {variables}; do
    _rdf_site_value="${{!_rdf_site_variable:-}}"
    if [ -n "$_rdf_site_value" ]; then export RDF_XRD_SITE_ORIGIN="$_rdf_site_value"; break; fi
  done
fi
if [ -z "${{RDF_XRD_SITE_ORIGIN:-}}" ] && [ -n "${{_CONDOR_MACHINE_AD:-}}" ]; then
  RDF_XRD_SITE_ORIGIN="$(run_helper_python - <<'RDF_WORKER_ORIGIN'
import os, re
try:
    with open(os.environ['_CONDOR_MACHINE_AD']) as stream:
        text = stream.read()
    match = re.search(r'^Machine\\s*=\\s*"([^"\\n]+)"', text, re.MULTILINE)
    print(match.group(1) if match else '')
except (OSError, KeyError):
    print('')
RDF_WORKER_ORIGIN
)"
  if [ -n "$RDF_XRD_SITE_ORIGIN" ]; then export RDF_XRD_SITE_ORIGIN; fi
fi
"""


def generate_condor_runscript(
    exe_relpath,
    stage_inputs,
    stage_outputs,
    root_setup,
    x509loc=None,
    pre_setup_lines="",
    use_shared_inputs=False,
    eos_sched=False,
    shared_dir_name=None,
    config_file="submit_config.yaml",
    python_env_tarball=None,
    shared_archive_name=None,
    runtime_config_relpath=None,
    request_cpus=1,
    site_ranking_file=None,
    sizing_pilot_entries=0,
):
    _warn_ignored_option(eos_sched, 'eos_sched')
    request_cpus = int(request_cpus)
    if request_cpus < 1:
        raise ValueError("request_cpus must be positive")
    legacy_conversion = ""
    if get_config_extension(config_file) != ".yaml":
        source_file = config_file
        config_file = str(_config_io.runtime_yaml_path(config_file))
        legacy_conversion = ("run_helper_python - <<'RDF_LEGACY_CONFIG'\n" + _worker_config_io()
                             + "\nmaterialize_runtime_config(" + repr(source_file) + ")\nRDF_LEGACY_CONFIG")
    if runtime_config_relpath:
        runtime_config_relpath = str(_config_io.runtime_yaml_path(runtime_config_relpath))
    stage_block = stage_inputs_block(config_file=config_file) if stage_inputs else ""
    stage_out_pre = ""
    stage_out_post = ""
    if stage_outputs:
        stage_out_pre, stage_out_post = stage_outputs_blocks(config_file=config_file)
    def _relax_ulimit_commands(setup_text):
        if not setup_text:
            return ""
        relaxed_lines = []
        for line in setup_text.splitlines():
            if line.lstrip().startswith("ulimit "):
                indent = line[:len(line) - len(line.lstrip())]
                relaxed_lines.append(f"{indent}({line.strip()}) || true")
            else:
                relaxed_lines.append(line)
        return "\n".join(relaxed_lines)

    def _setup_block(setup_text):
        if not setup_text:
            return ""
        return "set +u\n" + _relax_ulimit_commands(setup_text).rstrip() + "\nset -u\n"

    root_block = _setup_block(root_setup)
    pre_block = pre_setup_lines or ""
    x509_name = os.path.basename(x509loc) if x509loc else ""
    shared_block = ""
    if shared_dir_name:
        shared_block = (
            f"if [ -f {shared_dir_name}/{exe_relpath} ]; then cp -f {shared_dir_name}/{exe_relpath} .; fi\n"
            f"if [ -f {exe_relpath} ]; then chmod +x {exe_relpath}; fi\n"
            f"if [ -d {shared_dir_name}/aux ] && [ ! -e aux ]; then cp -R {shared_dir_name}/aux aux; fi\n"
            f"if [ -d {shared_dir_name}/cfg ] && [ ! -e cfg ]; then cp -R {shared_dir_name}/cfg cfg; fi\n"
            f"if compgen -G \"{shared_dir_name}/*.so*\" > /dev/null; then cp -f {shared_dir_name}/*.so* .; fi\n"
            f"for _shared_f in {shared_dir_name}/*; do [ -f \"$_shared_f\" ] && cp -n \"$_shared_f\" . 2>/dev/null || true; done\n"
            "for _bundle in cfg_bundle.tar.gz aux_bundle.tar.gz; do\n"
            f"  if [ -f {shared_dir_name}/$_bundle ]; then\n"
            f"    cp -n {shared_dir_name}/$_bundle . 2>/dev/null || true\n"
            "  fi\n"
            "  if [ -f \"$_bundle\" ]; then\n"
            "    case \"$_bundle\" in\n"
            "      cfg_bundle.tar.gz) _target_dir=cfg ;;\n"
            "      aux_bundle.tar.gz) _target_dir=aux ;;\n"
            "      *) _target_dir= ;;\n"
            "    esac\n"
            "    if [ -n \"${_target_dir:-}\" ] && [ ! -d \"$_target_dir\" ]; then\n"
            "      tar -xzf \"$_bundle\"\n"
            "    fi\n"
            "  fi\n"
            "done\n"
            f"export LD_LIBRARY_PATH=\"$PWD/{shared_dir_name}:$PWD:${{LD_LIBRARY_PATH:-}}\"\n"
        )
        if x509_name:
            shared_block += f"if [ -f {shared_dir_name}/{x509_name} ]; then cp -f {shared_dir_name}/{x509_name} .; fi\n"
    elif shared_archive_name:
        shared_block = (
            f"if [ -f \"{shared_archive_name}\" ]; then\n"
            f"  tar -xzf \"{shared_archive_name}\"\n"
            f"fi\n"
            f"if [ -f \"{exe_relpath}\" ]; then chmod +x \"{exe_relpath}\"; fi\n"
            "export LD_LIBRARY_PATH=\"$PWD:${LD_LIBRARY_PATH:-}\"\n"
        )

    config_runtime_path = runtime_config_relpath or config_file
    site_ranking_block = _site_ranking_block(site_ranking_file)
    xrd_optimize_block = "" if stage_inputs else xrootd_optimize_block(config_runtime_path)
    config_runtime_dir = os.path.dirname(config_runtime_path)
    materialize_config_block = ""
    if config_runtime_path != config_file:
        materialize_config_block = (
            (f"mkdir -p {shlex.quote(config_runtime_dir)}\n" if config_runtime_dir else "")
            + "for _cfg_payload in *.txt *.yaml *.yml *.json; do\n"
            + "  [ -f \"$_cfg_payload\" ] || continue\n"
            + f"  if [ \"$_cfg_payload\" != \"{config_file}\" ]; then cp -n \"$_cfg_payload\" {shlex.quote((config_runtime_dir or '.') + '/')} 2>/dev/null || true; fi\n"
            + "done\n"
            + f"cp -f {shlex.quote(config_file)} {shlex.quote(config_runtime_path)}\n"
        )

    x509_block = ""
    if x509_name:
        x509_block = (
            f"export X509_USER_PROXY={x509_name}\n"
            "voms-proxy-info -all\n"
            f"voms-proxy-info -all -file {x509_name}\n"
        )
    else:
        x509_block = (
            'if [ -f "x509" ]; then\n'
            '  export X509_USER_PROXY=x509\n'
            '  voms-proxy-info -all || true\n'
            '  voms-proxy-info -all -file x509 || true\n'
            'fi\n'
        )

    python_env_block = ""
    if python_env_tarball:
        tarball_name = os.path.basename(python_env_tarball)
        python_env_block = (
            f"python_env_archive={shlex.quote(tarball_name)}\n"
            f"if [ -f \"$python_env_archive\" ]; then\n"
            f"  echo \"Unpacking Python environment from $python_env_archive...\"\n"
            f"  mkdir -p _python_env\n"
            f"  tar -xzf \"$python_env_archive\" -C _python_env\n"
            f"  export PYTHONPATH=\"$PWD/_python_env:${{PYTHONPATH:-}}\"\n"
            f"  export PATH=\"$PWD/_python_env/bin:${{PATH:-}}\"\n"
            f"  export LD_LIBRARY_PATH=\"$PWD/_python_env:${{LD_LIBRARY_PATH:-}}\"\n"
            f"  export ROOT_INCLUDE_PATH=\"$PWD/_python_env${{ROOT_INCLUDE_PATH:+:$ROOT_INCLUDE_PATH}}\"\n"
            f"  echo \"Python environment ready (PYTHONPATH=$PYTHONPATH)\"\n"
            f"else\n"
            f"  echo \"ERROR: Python environment tarball $python_env_archive not found\" >&2\n"
            f"  exit 1\n"
            f"fi\n"
        )

    root_include_path_block = (
        'export ROOT_INCLUDE_PATH="$PWD${ROOT_INCLUDE_PATH:+:$ROOT_INCLUDE_PATH}"\n'
    )

    analysis_exec = "os.execv(sys.argv[2], sys.argv[2:])"
    pilot_start, pilot_setup_report = "", ""
    if sizing_pilot_entries:
        if int(sizing_pilot_entries) < 1:
            raise ValueError("sizing_pilot_entries must be positive")
        analysis_exec = ("sys.argv = ['rdf-sizing-pilot'] + sys.argv[2:] + [" + repr(str(sizing_pilot_entries)) + "]\n"
                          + _worker_config_io() + "\n" + Path(__file__).with_name('job_sizing_worker.py').read_text().replace(
                             "from native_config_io import read_config, write_config", ""))
        pilot_start = 'rdf_pilot_worker_start=$(date +%s)'
        pilot_setup_report = 'echo "Worker setup time: $((analysis_start - rdf_pilot_worker_start - stage_in_end + stage_in_start))s"'
    run_script = f"""#!/bin/bash
# fail fast on any command error, undefined var, or pipeline failure
set -euo pipefail
trap 'rc=$?; echo "ERROR: wrapper exited with code $rc"; exit $rc' ERR
# log and re-raise SIGTERM so Condor records ExitBySignal (do not swallow the signal)
trap 'echo "Received SIGTERM - likely timed out by scheduler"; trap - SIGTERM; kill -s SIGTERM $$' SIGTERM
{pilot_start}
RDF_ORIG_LD_LIBRARY_PATH="${{LD_LIBRARY_PATH:-}}"
# The helper interpreter is selected before setup scripts adjust PATH. Keep
# embedded Python compatible with the older subprocess API on worker nodes.
RDF_HELPER_PYTHON="$(command -v python3 || command -v python || true)"
RDF_WORKER_YAML="$PWD/rdf_worker_yaml.zip"
run_helper_python() {{
    local helper="${{RDF_HELPER_PYTHON:-python3}}"
    if [ -z "$helper" ]; then
        helper=python3
    fi
    local helper_path="${{PYTHONPATH:-}}"
    if [ -f "$RDF_WORKER_YAML" ]; then
        helper_path="$RDF_WORKER_YAML${{helper_path:+:$helper_path}}"
    fi
    env -u PYTHONHOME PYTHONPATH="$helper_path" LD_LIBRARY_PATH="${{RDF_ORIG_LD_LIBRARY_PATH:-}}" "$helper" "$@"
}}
# Preserve the site's inherited CPU placement, including for transfer helpers.
# request_cpus specifies a count; it does not identify CPUs we may assign.
run_helper_python - {request_cpus} <<'RDF_CPU_REPORT'
import os, sys
count = int(sys.argv[1])
allowed = sorted(os.sched_getaffinity(0))
print('[cpu] worker requested={{}} allowed={{}} policy=inherited'.format(count, allowed), flush=True)
RDF_CPU_REPORT
export OMP_NUM_THREADS={request_cpus} OMP_THREAD_LIMIT={request_cpus}
export OPENBLAS_NUM_THREADS={request_cpus} MKL_NUM_THREADS={request_cpus}
export NUMEXPR_NUM_THREADS={request_cpus} VECLIB_MAXIMUM_THREADS={request_cpus}
{root_block}{pre_block}{shared_block}{x509_block}{python_env_block}{root_include_path_block}ls
stage_in_start=$(date +%s)
{legacy_conversion}
{stage_out_pre}{stage_block}
stage_in_end=$(date +%s)
echo "Stage-in time: $((stage_in_end - stage_in_start))s"
echo "Check file existence"
ls
{materialize_config_block}{site_ranking_block}{xrd_optimize_block}
analysis_start=$(date +%s)
{pilot_setup_report}
echo "Starting Analysis"
chmod +x ./{exe_relpath}
# run the analysis but capture its exit status so we can re-raise a signal
set +e
# Retain inherited affinity before library initialization. Numerical pool
# limits remain tied to request_cpus; they are not an aggregate CPU quota.
# Python exec preserves the analysis exit/signal handling below.
RDF_ANALYSIS_PYTHONPATH="${{PYTHONPATH:-}}" RDF_ANALYSIS_LD_LIBRARY_PATH="${{LD_LIBRARY_PATH:-}}" run_helper_python - {request_cpus} ./{exe_relpath} {config_runtime_path} <<'RDF_CPU_EXEC'
import os, sys
os.environ['LD_LIBRARY_PATH'] = os.environ.pop('RDF_ANALYSIS_LD_LIBRARY_PATH')
os.environ['PYTHONPATH'] = os.environ.pop('RDF_ANALYSIS_PYTHONPATH')
count = int(sys.argv[1])
allowed = sorted(os.sched_getaffinity(0))
for key in ('OMP_NUM_THREADS', 'OPENBLAS_NUM_THREADS', 'MKL_NUM_THREADS',
            'NUMEXPR_NUM_THREADS', 'VECLIB_MAXIMUM_THREADS'):
    os.environ[key] = str(count)
os.environ['OMP_THREAD_LIMIT'] = str(count)
print('[cpu] requested={{}} allowed={{}} policy=inherited'.format(count, allowed), flush=True)
{analysis_exec}
RDF_CPU_EXEC
rc=$?
set -e
if [ "$rc" -ne 0 ]; then
  echo "Analysis failed with exit code $rc"
  # if child was terminated by a signal, rc will be >= 128 (128 + signal)
  if [ "$rc" -ge 128 ]; then
    sig=$((rc - 128))
    echo "Analysis terminated by signal $sig — re-raising to ensure Condor records ExitBySignal"
    kill -s "$sig" $$ || exit "$rc"
  fi
  exit "$rc"
fi
analysis_end=$(date +%s)
echo "Analysis time: $((analysis_end - analysis_start))s"

stage_out_start=$(date +%s)
{stage_out_post}
stage_out_end=$(date +%s)
echo "Stage-out time: $((stage_out_end - stage_out_start))s"
echo "Done!"
"""
    return run_script


def generate_container_wrapper(container_setup, inner_script_name):
    setup = (container_setup or "").strip()
    inner_cmd = f"bash {shlex.quote('./' + inner_script_name)}"

    if "{cmd}" in setup:
        launch_cmd = setup.replace("{cmd}", inner_cmd)
    elif "cmssw-el" in setup and "--command-to-run" not in setup:
        launch_cmd = f'{setup} --command-to-run "{inner_cmd}"'
    elif "--command-to-run" in setup:
        launch_cmd = f'{setup} "{inner_cmd}"'
    else:
        launch_cmd = f"{setup} {inner_cmd}"

    wrapper = f"""#!/bin/bash
set -euo pipefail
trap 'rc=$?; echo "ERROR: wrapper exited with code $rc"; exit $rc' ERR
trap 'echo "Received SIGTERM - likely timed out by scheduler"; trap - SIGTERM; kill -s SIGTERM $$' SIGTERM

if [ ! -f ./{inner_script_name} ]; then
  echo "ERROR: missing inner runscript ./{inner_script_name}"
  exit 2
fi
chmod +x ./{inner_script_name}

{launch_cmd}
"""
    return wrapper


_CONDOR_RESERVED_MACHINES = ("hepcms-henrietta.privnet",)
_CONDOR_EXTRA_BLOCKED_HOSTS_ENV = "RDF_CONDOR_EXTRA_BLOCKED_HOSTS"
_DEFAULT_CONDOR_EXTRA_BLOCKED_MACHINES = ("hepcms-namenode.privnet",)
_CONDOR_BLOCKED_MACHINE_PATTERNS_ENV = "RDF_CONDOR_BLOCKED_MACHINE_PATTERNS"
_DEFAULT_CONDOR_BLOCKED_MACHINE_PATTERNS = (
    # IFCA worker nodes consistently lacked the configured CVMFS ROOT release.
    r"(^|[.])ifca[.]es$",
    # Failures were concentrated on these IIHE worker nodes; other IIHE nodes
    # completed successfully and remain eligible.
    r"^node38-(1|3|5)[.]wn[.]iihe[.]ac[.]be$",
    # All observed jobs on this IHEP worker failed; other IHEP nodes succeeded.
    r"^lwnac008[.]ihep[.]ac[.]cn$",
)


def _condor_blocked_machines():
    """Return reserved hosts plus configurable additional blocked hosts.

    ``RDF_CONDOR_EXTRA_BLOCKED_HOSTS`` is a comma-separated override for the
    additional blocked hosts. When unset, the known EOS-unreachable worker is
    blocked by default; setting it to an empty string allows that worker to
    match again. Reserved machines remain excluded regardless of this setting.
    """
    configured_hosts = os.environ.get(_CONDOR_EXTRA_BLOCKED_HOSTS_ENV)
    if configured_hosts is None:
        extra_hosts = _DEFAULT_CONDOR_EXTRA_BLOCKED_MACHINES
    else:
        extra_hosts = configured_hosts.split(",")

    hosts = [*_CONDOR_RESERVED_MACHINES]
    hosts.extend(host.strip() for host in extra_hosts if host.strip())
    return tuple(dict.fromkeys(hosts))


def _condor_blocked_machine_patterns():
    """Return default site/worker regexes or an environment override."""
    configured_patterns = os.environ.get(_CONDOR_BLOCKED_MACHINE_PATTERNS_ENV)
    if configured_patterns is None:
        patterns = _DEFAULT_CONDOR_BLOCKED_MACHINE_PATTERNS
    else:
        patterns = configured_patterns.split(",")

    return tuple(
        dict.fromkeys(pattern.strip() for pattern in patterns if pattern.strip())
    )


def _classad_string(value):
    """Quote a Python string as an HTCondor ClassAd string literal."""
    return '"' + value.replace("\\", "\\\\").replace('"', '\\"') + '"'


def condor_config_return_remap(log_directory, config_file):
    """Keep each returned worker configuration with its Condor attempt logs."""
    filename = f"returned_$(Cluster)_$(Process)_{os.path.basename(config_file)}"
    destination = os.path.abspath(os.path.join(log_directory, filename))
    return "transfer_output_remaps = " + _classad_string(f"{config_file}={destination}")


def _condor_machine_exclusion_expressions():
    """Build exact-host and regex machine veto expressions."""
    expressions = [
        f"(TARGET.Machine != {_classad_string(host)})"
        for host in _condor_blocked_machines()
    ]
    expressions.extend(
        f"!(regexp({_classad_string(pattern)}, TARGET.Machine, \"i\"))"
        for pattern in _condor_blocked_machine_patterns()
    )
    return expressions


def condor_machine_requirements():
    """Return the framework's default Condor machine veto expression."""
    return " && ".join(_condor_machine_exclusion_expressions())


def ensure_condor_machine_exclusion(submit_text):
    """Add default blocked hosts and sites to a Condor submit description."""
    lines = submit_text.splitlines()
    trailing_newline = submit_text.endswith("\n")
    exclusions = _condor_machine_exclusion_expressions()

    for index, line in enumerate(lines):
        attribute, separator, expression = line.partition("=")
        if not separator or attribute.strip().lower() != "requirements":
            continue
        requirement, comment_marker, comment = expression.partition("#")
        missing_exclusions = [
            exclusion for exclusion in exclusions if exclusion not in requirement
        ]
        if not missing_exclusions:
            return submit_text
        comment_suffix = f" #{comment}" if comment_marker else ""
        exclusion_expr = " && ".join(missing_exclusions)
        lines[index] = (
            f"{attribute}= ({requirement.strip()}) && "
            f"{exclusion_expr}{comment_suffix}"
        )
        return "\n".join(lines) + ("\n" if trailing_newline else "")

    queue_index = next(
        (
            index
            for index, line in enumerate(lines)
            if line.lstrip().lower().startswith("queue")
        ),
        len(lines),
    )
    exclusion_expr = " && ".join(exclusions)
    lines.insert(queue_index, f"Requirements = {exclusion_expr}")
    return "\n".join(lines) + ("\n" if trailing_newline else "")


def generate_condor_submit(
    main_dir,
    jobs,
    exe_relpath,
    x509loc=None,
    want_os="",
    max_runtime="3600",
    request_memory=2000,
    request_cpus=1,
    request_disk=20000,
    extra_transfer_files=None,
    use_shared_inputs=False,
    stream_logs=False,
    eos_sched=False,
    include_aux=True,
    shared_dir_name=None,
    shared_archive_name=None,
    config_file="submit_config.yaml",
    container_image="",
    sample_name="",
    run_name="",
    job_indices=None,
    runscript_path=None,
):
    _warn_ignored_option(eos_sched, 'eos_sched')
    Path(main_dir + "/condor_logs").mkdir(parents=True, exist_ok=True)
    job_selector = "$(Process)"
    queue = f"queue {jobs}"
    if job_indices is not None:
        indices = tuple(job_indices)
        if len(indices) != jobs or len(set(indices)) != jobs or any(type(i) is not int or i < 0 for i in indices):
            raise ValueError("job_indices must identify each queued job exactly once with a nonnegative integer")
        job_selector = "$(RDFJobIndex)"
        queue = "queue RDFJobIndex in " + ",".join(map(str, indices))
    worker_dir = f"{main_dir}/job_{job_selector}"
    transfer_files = [f"{worker_dir}/{config_file}"]
    # ProductionManager workers have no per-job redirector manifest.
    if config_file not in {"job_config.yaml", "job_config.runtime.yaml", "job_config.txt"}:
        transfer_files.append(f"{worker_dir}/site_redirectors.json")
    if get_config_extension(config_file) == ".txt" or config_file in {"job_config.yaml", "job_config.runtime.yaml"}:
        extension = ".runtime.yaml" if config_file == "job_config.runtime.yaml" else get_config_extension(config_file)
        transfer_files.extend(f"{worker_dir}/{key}{extension}" for key in ("floats", "ints"))
    if shared_dir_name:
        transfer_files.append(f"{main_dir}/{shared_dir_name}")
    elif shared_archive_name:
        archive_path = os.path.join(main_dir, shared_archive_name)
        if os.path.exists(archive_path):
            transfer_files.append(archive_path)
    else:
        transfer_files.append(f"{worker_dir}/{exe_relpath}")
        if include_aux:
            transfer_files.append(f"{worker_dir}/aux")
        if x509loc:
            x509_name = os.path.basename(x509loc)
            transfer_files.append(f"{worker_dir}/{x509_name}")
    if extra_transfer_files:
        transfer_files.extend(extra_transfer_files)
    filtered_files = []
    for path in transfer_files:
        if "$(" in path:
            filtered_files.append(path)
            continue
        if os.path.exists(path):
            filtered_files.append(path)
    transfer_input_files = ",".join(filtered_files)

    stream_block = ""
    #if stream_logs:
    #    stream_block = "stream_output = True\nstream_error = True\n"

    log_name = "log_$(Cluster).log"
    want_os_block = f'MY.WantOS = "{want_os}"\n' if want_os else ""
    container_block = f'MY.SingularityImage = "{container_image}"\n' if container_image else ""
    # Put the sample first so it remains visible when condor_q truncates
    # BATCH_NAME. Including the run keeps separate productions distinct.
    labels_block = ""
    if sample_name:
        batch_name = f"{sample_name} [{run_name}]" if run_name else sample_name
        labels_block += f"+JobBatchName = {_classad_string(batch_name)}\n"
        labels_block += f"+RDFSample = {_classad_string(sample_name)}\n"
    if run_name:
        labels_block += f"+RDFRun = {_classad_string(run_name)}\n"
    requirements_block = (
        'requirements = (TARGET.Arch =?= "X86_64") && '
        '((TARGET.Microarch =!= UNDEFINED && regexp("^x86_64-v([2-9]|[1-9][0-9]+)$", TARGET.Microarch)) '
        '|| (TARGET.Has_sse4_1 =?= True && TARGET.Has_sse4_2 =?= True && TARGET.has_ssse3 =?= True))\n'
    )

    submit_file = f"""universe = vanilla
Executable     =  {runscript_path or os.path.join(main_dir, "condor_runscript.sh")}
Should_Transfer_Files     = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification     = never
{labels_block}transfer_input_files = {transfer_input_files}
environment = "CONDOR_PROC={job_selector} CONDOR_CLUSTER=$(Cluster)"
{requirements_block}{stream_block}{want_os_block}{container_block}+RequestMemory={request_memory}
+RequestCpus={request_cpus}
+RequestDisk={request_disk}
+MaxRuntime={max_runtime}
max_transfer_input_mb = 10000
WhenToTransferOutput=On_Exit
transfer_output_files = {_config_io.runtime_yaml_path(config_file)}
{condor_config_return_remap(os.path.join(main_dir, "condor_logs"), str(_config_io.runtime_yaml_path(config_file)))}

Output     = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stdout
Error      = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stderr
Log        = {main_dir}/condor_logs/{log_name}
{queue}
"""
    return ensure_condor_machine_exclusion(submit_file)


def _package_worker_yaml(directory):
    """Stage the installed pure Python YAML reader for the pre-setup worker interpreter."""
    from importlib.metadata import distribution
    import tempfile
    import zipfile

    target = Path(directory) / "rdf_worker_yaml.zip"
    # Include the distribution's license alongside the portable Python modules.
    dist = distribution("PyYAML")
    sources = [(path, "yaml/" + path.name) for path in sorted(Path(yaml.__file__).parent.glob("*.py"))]
    sources += [(dist.locate_file(path), str(path)) for path in dist.files or ()
                if "license" in path.name.lower() and path.name.lower() != "licenses"]
    with tempfile.NamedTemporaryFile(dir=directory, suffix=".zip", delete=False) as temp:
        temporary = Path(temp.name)
    try:
        with zipfile.ZipFile(temporary, "w", zipfile.ZIP_DEFLATED) as archive:
            for source, name in sources:
                archive.write(source, name)
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return str(target.resolve())


def materialize_retry_submit(content, submission_dir, config_path):
    """Give a new retry its own YAML and wrapper, preserving historical receipts and launchers."""
    from tempfile import mkdtemp
    import re
    directory = Path(submission_dir).resolve()
    transfer = re.search(r"(?mi)^([ \t]*transfer_input_files\s*=\s*)([^\n]+)", content)
    if not transfer:
        raise ValueError("Retry needs an explicit transfer_input_files list")
    inputs = [item.strip() for item in transfer[2].split(",") if item.strip()]
    names = {Path(item).name for item in inputs}
    legacy_name = next((name for name in ("submit_config.txt", "job_config.txt") if name in names), None)
    if legacy_name is None:
        if any(Path(item).name == "rdf_worker_yaml.zip" and (directory / item).is_file() for item in inputs):
            return content
        package = directory / "rdf_worker_yaml.zip"
        if not package.is_file():
            _package_worker_yaml(directory)
        inputs = [item for item in inputs if Path(item).name != package.name]
        inputs.append(str(package))
        return content[:transfer.start(2)] + ",".join(inputs) + content[transfer.end(2):]

    source = Path(config_path)
    executable = re.search(r"(?mi)^\s*executable\s*=\s*(.+?)\s*$", content)
    if not executable:
        raise ValueError("Legacy retry has no executable in its Condor submit description")
    wrapper = directory / executable[1].strip().strip('"')
    if not wrapper.is_file():
        raise ValueError(f"Legacy retry wrapper is unavailable: {wrapper}. Prepare a new YAML campaign from the original spec.")
    scripts = {wrapper}
    for item in inputs:
        if Path(item).name == "condor_runscript_inner.sh":
            scripts.add(directory / item)
    scripts = {p: p.read_text() for p in scripts}
    if not any("run_helper_python" in text and legacy_name in text for text in scripts.values()):
        raise ValueError(f"Unrecognized legacy worker {wrapper}. Prepare a new YAML campaign from its execution settings.")

    configs_dir = directory / "resubmissions"
    configs_dir.mkdir(exist_ok=True)
    retry = Path(mkdtemp(prefix="yaml-", dir=configs_dir))
    target = retry / Path(legacy_name).with_suffix(".yaml").name
    config = read_config(source)
    old_constants, new_constants = set(), []
    for key, stem in (("floatConfig", "floats"), ("intConfig", "ints")):
        ref = config.get(key)
        if not ref or get_config_extension(ref) == ".yaml":
            continue
        candidates = [source.parent / ref, source.parent / Path(ref).name]
        for input_path in inputs:
            entry = directory / input_path
            if entry.is_dir():
                candidates.extend((entry / ref, entry / Path(ref).name))
            elif entry.name == Path(ref).name:
                candidates.append(entry)
        constant = next((candidate for candidate in candidates if candidate.is_file()), None)
        if constant is None:
            # The reference may live in a sealed input archive. It remains a
            # read-only dependency and is resolved by the original worker layout.
            continue
        converted = retry / (stem + ".yaml")
        write_config(read_config(constant), converted)
        config[key] = converted.name
        old_constants.add(constant.name)
        new_constants.append(str(converted))
    write_config(config, target)
    reader = _package_worker_yaml(retry)

    for path, script in scripts.items():
        # The original worker can also have a separate container launcher.
        if "run_helper_python" in script:
            script = re.sub(r"(?ms)^run_helper_python - <<'RDF_LEGACY_CONFIG'\n.*?^RDF_LEGACY_CONFIG\n", "", script)
            script = script.replace(str(_config_io.runtime_yaml_path(legacy_name)), target.name).replace(legacy_name, target.name)
            # Replace the known generated input and output helpers. The remainder
            # of the original wrapper owns its interpreter, environment and executable.
            if __package__:
                from .refresh_xrootd_runtime import patch_probe_functions
            else:
                from refresh_xrootd_runtime import patch_probe_functions
            script = patch_probe_functions(script)
            def update_output(match):
                body = match[1]
                path_arg = re.search(r"""(?m)^cfg = read_config\(["']([^"']+)["']\)""", body)
                if not path_arg:
                    return match[0]
                pre, post = stage_outputs_blocks(config_file=path_arg[1])
                if "def normalize_dest" in body:
                    return post.strip("\n")
                if "__orig_saveFile" in body and "save_file = cfg.get(" in body:
                    return pre.strip("\n")
                return match[0]
            script = re.sub(r"(?ms)^run_helper_python - << 'PY'\n(.*?)^PY$", update_output, script)
            # Scope the dependency to the helper interpreter, preserving the analysis environment.
            script = script.replace('env -u PYTHONHOME LD_LIBRARY_PATH=',
                                    'env -u PYTHONHOME PYTHONPATH="$PWD/rdf_worker_yaml.zip${PYTHONPATH:+:$PYTHONPATH}" LD_LIBRARY_PATH=')
        cloned = retry / path.name
        cloned.write_text(script)
        cloned.chmod(path.stat().st_mode & 0o777)

    updated = []
    for item in inputs:
        if Path(item).name == legacy_name:
            updated.append(str(target))
        elif Path(item).name in {"floats.txt", "ints.txt"} and not (directory / item).is_file():
            # Older generated descriptors listed both even for jobs without constants.
            continue
        elif Path(item).name == "rdf_worker_yaml.zip" or Path(item).name in old_constants:
            continue
        elif directory / item in scripts:
            updated.append(str(retry / Path(item).name))
        else:
            updated.append(item)
    updated.extend(new_constants)
    updated.append(reader)
    content = content[:transfer.start(2)] + ",".join(updated) + content[transfer.end(2):]
    content = re.sub(r"(?mi)^([ \t]*executable\s*=\s*)[^\n]+",
                     lambda m: m[1] + str(retry / wrapper.name), content)
    content = content.replace(str(_config_io.runtime_yaml_path(legacy_name)), target.name).replace(legacy_name, target.name)
    remap = condor_config_return_remap(str(directory / "condor_logs"), target.name)
    current = re.search(r'(?mi)^([ \t]*transfer_output_remaps\s*=\s*)"([^"\n]*)"[ \t]*$', content)
    if current:
        mappings = [item for item in current[2].split(";") if item and item.partition("=")[0] != target.name]
        mappings.append(remap.split('"', 2)[1])
        content = content[:current.start()] + current[1] + '"' + ";".join(mappings) + '"' + content[current.end():]
    else:
        content = re.sub(r"(?mi)^([ \t]*queue\b)", lambda m: remap + "\n" + m[1], content)
    return content


def write_submit_files(
    main_dir,
    jobs,
    exe_relpath,
    stage_inputs,
    stage_outputs,
    root_setup,
    x509loc=None,
    pre_setup_lines="",
    want_os="",
    max_runtime="3600",
    request_memory=2000,
    request_cpus=1,
    request_disk=20000,
    extra_transfer_files=None,
    use_shared_inputs=False,
    stream_logs=False,
    eos_sched=False,
    include_aux=True,
    shared_dir_name=None,
    config_file=None,
    container_setup="",
    python_env_tarball=None,
    shared_archive_name=None,
    runtime_config_relpath=None,
    sample_name="",
    run_name="",
    sizing_pilot_entries=0,
    job_indices=None,
    launch_dir=None,
):
    _warn_ignored_option(eos_sched, 'eos_sched')
    config_file = config_file or _config_io.job_config_path(Path(main_dir) / "job_0").name
    launch_dir = launch_dir or main_dir
    submit_path = os.path.join(launch_dir, "condor_submit.sub")
    runscript_path = os.path.join(launch_dir, "condor_runscript.sh")

    submit_extra_transfer_files = list(extra_transfer_files) if extra_transfer_files else []
    submit_extra_transfer_files.append(_package_worker_yaml(launch_dir))
    inner_script_name = "condor_runscript_inner.sh"
    inner_runscript_path = os.path.join(launch_dir, inner_script_name)

    if container_setup:
        submit_extra_transfer_files.append(inner_runscript_path)

    payload_script = generate_condor_runscript(
        exe_relpath,
        stage_inputs,
        stage_outputs,
        root_setup,
        x509loc=x509loc,
        pre_setup_lines=pre_setup_lines,
        use_shared_inputs=use_shared_inputs,
        shared_dir_name=shared_dir_name,
        config_file=config_file,
        python_env_tarball=python_env_tarball,
        shared_archive_name=shared_archive_name,
        runtime_config_relpath=runtime_config_relpath,
        request_cpus=request_cpus,
        sizing_pilot_entries=sizing_pilot_entries,
    )

    if container_setup:
        with open(inner_runscript_path, "w") as condor_sub:
            condor_sub.write(payload_script)

    with open(runscript_path, "w") as condor_sub:
        condor_sub.write(payload_script)

    with open(submit_path, "w") as condor_sub:
        condor_sub.write(
            generate_condor_submit(
                main_dir,
                jobs,
                exe_relpath,
                x509loc=x509loc,
                want_os=want_os,
                max_runtime=max_runtime,
                request_memory=request_memory,
                request_cpus=request_cpus,
                request_disk=request_disk,
                extra_transfer_files=submit_extra_transfer_files,
                use_shared_inputs=use_shared_inputs,
                stream_logs=stream_logs,
                include_aux=include_aux,
                shared_dir_name=shared_dir_name,
                shared_archive_name=shared_archive_name,
                config_file=config_file,
                container_image=(container_setup or "").strip(),
                sample_name=sample_name,
                run_name=run_name,
                job_indices=job_indices,
                runscript_path=runscript_path,
            )
        )
    return submit_path
