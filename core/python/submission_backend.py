import os
import shlex
from pathlib import Path
import yaml


_EMBEDDED_ROOT_PROBE_MACRO_TMPL = """\\
void __FUNCNAME__() {
    const char* url = \"__URL__\";
    Long64_t nbytes = __NBYTES__LL;
    TStopwatch sw;
    sw.Start();
    TFile* f = TFile::Open(url, \"READ\");
    if (!f || f->IsZombie()) {
        printf(\"PROBE_RESULT:FAILED\\n\");
        if (f) { f->Close(); delete f; }
        return;
    }
    Int_t to_read = (nbytes > 0 && nbytes < (Long64_t)kMaxInt) ? (Int_t)nbytes : kMaxInt;
    if (to_read <= 0) {
        f->Close();
        delete f;
        printf(\"PROBE_RESULT:FAILED\\n\");
        return;
    }
    char* buf = new char[to_read];
    Bool_t ok = f->ReadBuffer(buf, 0LL, to_read);
    delete[] buf;
    sw.Stop();
    f->Close();
    delete f;
    if (ok) { printf(\"PROBE_RESULT:FAILED\\n\"); return; }
    double elapsed = sw.RealTime();
    if (elapsed > 0)
        printf(\"PROBE_RESULT:%.6f:%.0f\\n\", elapsed, (double)to_read);
    else
        printf(\"PROBE_RESULT:FAILED\\n\");
}
"""


def read_config(config_file):
    """
    Read config file in either text or YAML format.
    Auto-detects format based on file extension.
    """
    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        return read_config_yaml(config_file)
    else:
        return read_config_text(config_file)


def read_config_text(config_file):
    """Read config file in text format (key=value)."""
    config_dict = {}
    with open(config_file) as file:
        for line in file:
            line = line.split("#")[0]
            line = line.strip().split("=")
            if len(line) == 2:
                config_dict[line[0]] = line[1]
    return config_dict


def read_config_yaml(config_file):
    """Read config file in YAML format."""
    with open(config_file) as file:
        config_dict = yaml.safe_load(file)
    # Ensure all values are strings for consistency
    return {k: str(v) for k, v in config_dict.items()}


def write_config(config_dict, output_file):
    """
    Write config dict to file in either text or YAML format.
    Auto-detects format based on file extension.
    """
    if output_file.endswith('.yaml') or output_file.endswith('.yml'):
        write_config_yaml(config_dict, output_file)
    else:
        write_config_text(config_dict, output_file)


def write_config_text(config_dict, output_file):
    """Write config dict to text file (key=value format)."""
    with open(output_file, "w") as file:
        for key in config_dict.keys():
            file.write(str(key) + "=" + config_dict[key] + "\n")


def write_config_yaml(config_dict, output_file):
    """Write config dict to YAML file."""
    with open(output_file, "w") as file:
        yaml.dump(config_dict, file, default_flow_style=False, sort_keys=False)


def get_config_extension(config_file):
    """Get the extension of a config file."""
    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        return '.yaml'
    else:
        return '.txt'


def get_copy_file_list(config_dict):
    transfer_list = []
    for key in config_dict:
        value = config_dict[key]
        if ".txt" in value or ".yaml" in value or ".yml" in value:
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
        # Ensure we produce a double-slash after the redirector (e.g. root://host//store/...)
        # while avoiding a triple-slash artifact.
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


def stage_inputs_block(eos_sched=False, config_file="submit_config.txt"):
    return f"""
echo "Staging input files with xrdcp"
run_helper_python - << 'PY'
import os
import re
import socket
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

# ---------------------------------------------------------------------------
# Inline helpers shared with the XRootD optimiser (no external Python packages)
# ---------------------------------------------------------------------------

SITE_REDIRECTOR_MARKER = "/store/test/xrootd/"
GENERIC_REDIRECTOR_HOSTS = {
    "cmsxrootd.fnal.gov",
    "xrootd-cms.infn.it",
    "cms-xrd-global.cern.ch",
}
LOCAL_SITE_BONUS = 1.25
PROBE_TIMEOUT = 15.0
# Extra seconds added to the per-probe timeout when waiting for the thread
# pool to finish — gives in-flight probes time to complete gracefully.
PROBE_POOL_TIMEOUT_BUFFER = 2.0
PROBE_BYTES = 1 * 1024 * 1024  # 1 MiB – realistic but still small
STAGE_COPY_TIMEOUT = 60        # per-xrdcp timeout in seconds
# Total attempts per file: try up to MAX_ATTEMPTS site-specific redirectors
# ranked by measured read throughput.
MAX_ATTEMPTS = 3

# ROOT macro template (TFile::Open + TFile::ReadBuffer).
# TFile::ReadBuffer: kFALSE (0) = success, kTRUE (1) = failure.
ROOT_MACRO_TMPL = {_EMBEDDED_ROOT_PROBE_MACRO_TMPL!r}


def read_config(cfg_path):
    if cfg_path.endswith('.yaml') or cfg_path.endswith('.yml'):
        import yaml
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        return {{k: str(v) for k, v in cfg.items()}}
    cfg = {{}}
    with open(cfg_path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg


def write_config(cfg, cfg_path):
    if cfg_path.endswith('.yaml') or cfg_path.endswith('.yml'):
        import yaml
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    else:
        with open(cfg_path, "w") as f:
            for k, v in cfg.items():
                f.write(f"{{k}}={{v}}\\n")


def extract_lfn(url):
    if url.startswith("root://"):
        m = re.match(r"root://[^/]+/(.*)", url)
        if m:
            return "/" + m.group(1).lstrip("/")
    return url


def build_url(lfn, redirector):
    # Always use // between redirector and LFN: required for both regular
    # redirectors (root://cmsxrootd.fnal.gov/) and site-specific test
    # redirectors (root://xrootd-cms.infn.it//store/test/xrootd/<site>/).
    return redirector.rstrip("/") + "//" + lfn.lstrip("/")


def is_site_specific_redirector(redirector):
    return SITE_REDIRECTOR_MARKER in redirector


def is_direct_xrootd_candidate(candidate):
    if not candidate.startswith("root://") or is_site_specific_redirector(candidate):
        return False
    match = re.match(r"root://([^/:]+)", candidate)
    host = match.group(1).lower() if match else ""
    return host not in GENERIC_REDIRECTOR_HOSTS


def filter_site_redirectors(redirectors):
    filtered = []
    seen = set()
    for redirector in redirectors:
        if not (is_site_specific_redirector(redirector) or is_direct_xrootd_candidate(redirector)):
            continue
        if redirector in seen:
            continue
        filtered.append(redirector)
        seen.add(redirector)
    return filtered


def candidate_url(lfn, redirector):
    if is_direct_xrootd_candidate(redirector):
        return redirector
    return build_url(lfn, redirector)


def probe_via_root_macro(url, read_bytes, timeout):
    # Probe via ROOT macro (TFile::Open + ReadBuffer).
    macro_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".C", delete=False, prefix="xrd_probe_"
        ) as tmp:
            funcname = os.path.splitext(os.path.basename(tmp.name))[0]
            macro_src = (
                ROOT_MACRO_TMPL
                .replace("__FUNCNAME__", funcname)
                .replace("__URL__", url)
                .replace("__NBYTES__", str(read_bytes))
            )
            tmp.write(macro_src)
            macro_path = tmp.name
        result = subprocess.run(
            ["root", "-b", "-q", "-l", macro_path],
            capture_output=True, timeout=timeout, text=True,
        )
        for line in result.stdout.splitlines():
            if line.startswith("PROBE_RESULT:"):
                parts = line.split(":")
                if len(parts) >= 2 and parts[1] == "FAILED":
                    return None
                if len(parts) >= 3:
                    try:
                        elapsed, to_read = float(parts[1]), float(parts[2])
                        if elapsed > 0 and to_read > 0:
                            return to_read / (1024.0 * 1024.0) / elapsed
                    except ValueError:
                        pass
        return None
    except FileNotFoundError:
        print("ROOT executable not found. Please ensure ROOT is installed and in PATH.")
        return None
    except subprocess.TimeoutExpired:
        print(f"ROOT macro execution timed out after {{timeout}} seconds.")
        return None
    except Exception as e:
        import traceback
        print(f"An unexpected error occurred: {{e}}")
        print(traceback.format_exc())
        return None
    finally:
        if macro_path:
            try:
                os.unlink(macro_path)
            except OSError:
                pass


def probe_one_redirector(lfn, redirector, read_bytes=PROBE_BYTES, timeout=PROBE_TIMEOUT):
    url = candidate_url(lfn, redirector)
    return probe_via_root_macro(url, read_bytes, timeout)


def rank_redirectors_parallel(lfn, redirectors, local_site=""):
    # Map site name prefix to preferred redirector domain for local bonus.
    site_domain_map = {{
        "T1_US_FNAL": "fnal.gov",
        "T0_CH_CERN": "cern.ch",
        "T2_US_": "fnal.gov",
        "T2_DE_": "infn.it", "T2_IT_": "infn.it", "T2_FR_": "infn.it",
        "T2_UK_": "infn.it", "T2_ES_": "infn.it", "T2_CH_": "cern.ch",
        "T3_US_": "fnal.gov",
    }}
    def site_matches(site, redir):
        redir_l = redir.lower()
        for prefix, domain in site_domain_map.items():
            if site.startswith(prefix) and domain in redir_l:
                return True
        return site in redir

    results = []
    processed = set()
    with ThreadPoolExecutor(max_workers=max(1, min(len(redirectors), 4))) as pool:
        futures = {{pool.submit(probe_one_redirector, lfn, r): r for r in redirectors}}
        try:
            for future in as_completed(futures, timeout=PROBE_TIMEOUT + PROBE_POOL_TIMEOUT_BUFFER):
                processed.add(future)
                try:
                    tput = future.result()
                except Exception:
                    continue
                if tput is None:
                    continue
                redir = futures[future]
                if local_site and site_matches(local_site, redir):
                    tput *= LOCAL_SITE_BONUS
                results.append((redir, tput))
        except TimeoutError:
            pass

        for future, redir in futures.items():
            if future in processed or not future.done():
                continue
            try:
                tput = future.result()
            except Exception:
                continue
            if tput is None:
                continue
            if local_site and site_matches(local_site, redir):
                tput *= LOCAL_SITE_BONUS
            results.append((redir, tput))
    results.sort(key=lambda x: x[1], reverse=True)
    return results


def detect_local_site():
    for var in ("CMS_LOCAL_SITE", "GLIDEIN_CMSSite", "OSG_SITE_NAME", "SITE_NAME"):
        val = os.environ.get(var, "").strip()
        if val:
            return val
    try:
        hn = socket.getfqdn().lower()
        if hn.endswith(".fnal.gov") or hn == "fnal.gov":
            return "T1_US_FNAL"
        if hn.endswith(".cern.ch") or hn == "cern.ch":
            return "T0_CH_CERN"
    except Exception:
        pass
    return ""


# normalize site-specific test redirectors into the generic store path
def _normalize_test_redirector(u):
    m = re.search(r"(root://[^/]+)//store/test/xrootd/[^/]+//(store/.+)$", u)
    if m:
        return m.group(1) + "//" + m.group(2)
    return u


# ---------------------------------------------------------------------------
# Main stage-in logic
# ---------------------------------------------------------------------------

cfg = read_config("{config_file}")

file_list = cfg.get("__orig_fileList", "") or cfg.get("fileList", "")
if not file_list:
    raise SystemExit("fileList not found in {config_file}")

local_site = detect_local_site()
if local_site:
    print(f"[stage-in] Local site: {{local_site}}")
else:
    print("[stage-in] Local site not detected; using throughput ranking only")

# Load per-file redirector list produced by _query_rucio during job preparation.
# Only site-specific redirectors are considered; generic redirectors are
# intentionally ignored.
import json as _json
_site_redirectors_file = "site_redirectors.json"
_site_redirectors: dict = {{}}
if os.path.exists(_site_redirectors_file):
    try:
        with open(_site_redirectors_file) as _sr_fh:
            _site_redirectors = _json.load(_sr_fh)
        if _site_redirectors:
            print(f"[stage-in] Loaded site redirectors for {{len(_site_redirectors)}} file(s)")
    except Exception as _sr_exc:
        print(f"[stage-in] Warning: could not read site_redirectors.json: {{_sr_exc}}")

streams = os.environ.get("XRDCP_STREAMS", "4")
urls = [u.strip() for u in file_list.split(",") if u.strip()]

# Probe all XRootD files in parallel to find the best redirector for each.
def _best_redirector_for(url):
    lfn = extract_lfn(url)
    if not (lfn.startswith("/store/") or lfn.startswith("/eos/")):
        return url, url  # non-XRootD: keep original, no ranking
    file_redirectors = filter_site_redirectors(_site_redirectors.get(lfn) or [])
    if not file_redirectors:
        raise RuntimeError(f"no site-specific redirectors available for {{lfn}}")
    ranked = rank_redirectors_parallel(lfn, file_redirectors, local_site)
    if not ranked:
        raise RuntimeError(f"all probes failed for {{lfn}}")
    best_redir = ranked[0][0]
    print(f"  [stage-in] {{lfn}} -> {{best_redir}} ({{ranked[0][1]:.2f}} MB/s)")
    return url, ranked

n_redirectors = len(filter_site_redirectors(next(iter(_site_redirectors.values()), []))) if _site_redirectors else 0
print(f"[stage-in] Probing up to {{n_redirectors}} redirector(s) per file for {{len(urls)}} file(s) in parallel...")
best_redirector_rankings = {{}}
redirector_failures = []
with ThreadPoolExecutor(max_workers=max(1, min(len(urls), 3))) as pool:
    futures = {{pool.submit(_best_redirector_for, u): u for u in urls}}
    for fut in as_completed(futures):
        try:
            orig, ranked = fut.result()
            best_redirector_rankings[orig] = ranked
        except Exception as exc:
            orig = futures[fut]
            redirector_failures.append(f"{{extract_lfn(orig)}} :: {{exc}}")

if redirector_failures:
    print("[stage-in] ERROR: redirector selection failed for one or more files:")
    for failure in redirector_failures:
        print(f"  [stage-in] {{failure}}")
    sys.exit(42)

# Stage each file with xrdcp using the ranked best URL.
local_paths = []
for i, url in enumerate(urls):
    local_name = f"input_{{i}}.root"
    ranked_redirectors = best_redirector_rankings.get(url, [])
    if not ranked_redirectors:
        raise RuntimeError(f"missing ranked redirectors for {{url}}")

    success = False
    for attempt, (redirector, _) in enumerate(ranked_redirectors[:MAX_ATTEMPTS], start=1):
        cur_url = candidate_url(extract_lfn(url), redirector)
        print(f"[stage-in] attempt {{attempt}}/{{MAX_ATTEMPTS}} xrdcp {{cur_url}} -> {{local_name}}")
        try:
            subprocess.run(
                ["xrdcp", "-f", "--nopbar", "--streams", streams, cur_url, local_name],
                check=True,
                timeout=STAGE_COPY_TIMEOUT,
            )
            local_paths.append(local_name)
            success = True
            break
        except Exception as exc:
            print(f"[stage-in] attempt {{attempt}} failed: {{exc}}")

    if not success:
        raise RuntimeError(f"xrdcp failed after {{MAX_ATTEMPTS}} attempt(s) for {{url}}")

cfg["fileList"] = ",".join(local_paths)
write_config(cfg, "{config_file}")
print(f"[stage-in] {{len(local_paths)}} file(s) staged successfully")
PY
"""


def stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt"):
    pre_block = f"""
run_helper_python - << 'PY'
import os
import subprocess
import time

XRDFS_TIMEOUT = int(os.environ.get("XRDFS_TIMEOUT", "30"))
XRDFS_VERIFY_RETRIES = int(os.environ.get("XRDFS_VERIFY_RETRIES", "4"))
XRDFS_VERIFY_DELAY = float(os.environ.get("XRDFS_VERIFY_DELAY", "5"))

def read_config(config_file):
    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        import yaml
        with open(config_file) as f:
            cfg = yaml.safe_load(f)
        return {{k: str(v) for k, v in cfg.items()}}
    else:
        cfg = {{}}
        with open(config_file) as f:
            for line in f:
                line = line.split("#")[0].strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
        return cfg

def write_config(cfg, config_file):
    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        import yaml
        with open(config_file, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    else:
        with open(config_file, "w") as f:
            for k, v in cfg.items():
                f.write(f"{{k}}={{v}}\\n")

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
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0, result.stdout.strip() or result.stderr.strip()
    except Exception as exc:
        return False, str(exc)

def ensure_output_endpoint_ready(dest, retries=XRDFS_VERIFY_RETRIES, delay=XRDFS_VERIFY_DELAY):
    normalized_dest, host_url, _ = split_xrootd_dest(dest)
    eos_dir = eos_dir_from_dest(normalized_dest)
    if not eos_dir or eos_dir == "/":
        return

    mkdir_ok, mkdir_msg = xrdfs_ok(host_url, "mkdir", "-p", eos_dir)
    if not mkdir_ok:
        print(f"Warning: xrdfs mkdir -p {{eos_dir}} failed (may already exist): {{mkdir_msg}}")

    last_err = ""
    for attempt in range(1, retries + 1):
        stat_ok, stat_msg = xrdfs_ok(host_url, "stat", eos_dir)
        if stat_ok:
            ls_ok, ls_msg = xrdfs_ok(host_url, "ls", eos_dir)
            if ls_ok:
                return
            last_err = ls_msg or f"xrdfs ls failed for {{eos_dir}}"
        else:
            last_err = stat_msg or f"xrdfs stat failed for {{eos_dir}}"
        print(f"xrdfs endpoint check attempt {{attempt}} failed for {{eos_dir}}: {{last_err}}")
        time.sleep(delay * attempt)

    raise RuntimeError(f"Output endpoint check failed for {{normalized_dest}}: {{last_err}}")

def check_output_endpoint(dest):
    if dest:
        ensure_output_endpoint_ready(dest)

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

check_output_endpoint(cfg.get("__orig_saveFile", ""))
check_output_endpoint(cfg.get("__orig_metaFile", ""))

proc_id = os.environ.get("CONDOR_PROC", "")
if proc_id and cfg.get("saveFile"):
    base, ext = os.path.splitext(cfg["saveFile"])
    cfg["saveFile"] = f"{{base}}_{{proc_id}}{{ext}}"
if proc_id and cfg.get("metaFile"):
    base, ext = os.path.splitext(cfg["metaFile"])
    cfg["metaFile"] = f"{{base}}_{{proc_id}}{{ext}}"

write_config(cfg, "{config_file}")
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

def read_config(config_file):
    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        import yaml
        with open(config_file) as f:
            cfg = yaml.safe_load(f)
        return {{k: str(v) for k, v in cfg.items()}}
    else:
        cfg = {{}}
        with open(config_file) as f:
            for line in f:
                line = line.split("#")[0].strip()
                if not line or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
        return cfg

cfg = read_config("{config_file}")

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
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        return result.returncode == 0, result.stdout.strip() or result.stderr.strip()
    except Exception as exc:
        return False, str(exc)

def ensure_output_endpoint_ready(dest, retries=XRDFS_VERIFY_RETRIES, delay=XRDFS_VERIFY_DELAY):
    normalized_dest, host_url, _ = split_xrootd_dest(dest)
    eos_dir = eos_dir_from_dest(normalized_dest)
    if not eos_dir or eos_dir == "/":
        return

    mkdir_ok, mkdir_msg = xrdfs_ok(host_url, "mkdir", "-p", eos_dir)
    if not mkdir_ok:
        print(f"Warning: xrdfs mkdir -p {{eos_dir}} failed (may already exist): {{mkdir_msg}}")

    last_err = ""
    for attempt in range(1, retries + 1):
        stat_ok, stat_msg = xrdfs_ok(host_url, "stat", eos_dir)
        if stat_ok:
            ls_ok, ls_msg = xrdfs_ok(host_url, "ls", eos_dir)
            if ls_ok:
                return
            last_err = ls_msg or f"xrdfs ls failed for {{eos_dir}}"
        else:
            last_err = stat_msg or f"xrdfs stat failed for {{eos_dir}}"
        print(f"xrdfs endpoint check attempt {{attempt}} failed for {{eos_dir}}: {{last_err}}")
        time.sleep(delay * attempt)

    raise RuntimeError(f"Output endpoint check failed for {{normalized_dest}}: {{last_err}}")

def check_output_endpoint(dest):
    normalized_dest = normalize_dest(dest)
    ensure_output_endpoint_ready(normalized_dest)

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


def xrootd_optimize_block(config_file="submit_config.txt", blacklisted_sites=None):
    """Return a shell heredoc that selects the fastest XRootD redirector.

    This block is embedded in the Condor worker runscript when stage-in is
    **not** used.  It reads the ``fileList`` from the job config, probes all
    well-known CMS XRootD redirectors from the worker node using a ROOT macro,
    and rewrites the config with the site-specific URLs that delivered the
    best throughput.

    The local CMS site is detected from common grid environment variables
    (``GLIDEIN_CMSSite``, ``CMS_LOCAL_SITE``, etc.) and preferred when its
    measured performance is within 20% of the global best.  Known-problematic
    sites can be excluded via *blacklisted_sites* or via the ``xrdBlacklist``
    key in the job config (comma-separated list of site name / hostname
    patterns).

    Args:
        config_file: Path to the per-job submit config passed to the
            analysis executable.
        blacklisted_sites: Optional list of site/hostname patterns to exclude
            from probing.  These are merged with any ``xrdBlacklist`` entry
            found in the config file at runtime.

    Returns:
        A multi-line bash string (heredoc) safe to embed in a generated
        runscript.
    """
    # Encode the compile-time blacklist as a Python list literal.
    _bl_literal = repr(list(blacklisted_sites) if blacklisted_sites else [])

    return f"""
echo "Optimizing XRootD redirectors for fastest site"
run_helper_python - << 'XRDPY'
import os
import re
import socket
import subprocess
import sys
import tempfile
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed

# ---------------------------------------------------------------------------
# Inline helpers (stdlib + ROOT via subprocess; no external Python packages)
# ---------------------------------------------------------------------------

SITE_REDIRECTOR_MARKER = "/store/test/xrootd/"
GENERIC_REDIRECTOR_HOSTS = {
    "cmsxrootd.fnal.gov",
    "xrootd-cms.infn.it",
    "cms-xrd-global.cern.ch",
}
LOCAL_SITE_BONUS = 1.25
PROBE_TIMEOUT = 10.0
# Extra seconds added to PROBE_TIMEOUT when waiting for the thread pool.
PROBE_POOL_TIMEOUT_BUFFER = 2.0
PROBE_BYTES = 1 * 1024 * 1024

# Compile-time blacklist (merged with runtime xrdBlacklist config key below).
STATIC_BLACKLIST = {_bl_literal}

# ROOT macro template – measures TFile::Open + TFile::ReadBuffer throughput.
# NOTE: TFile::ReadBuffer uses kFALSE (0) = success, kTRUE (1) = failure.
# NOTE: Keep this in sync with _ROOT_PROBE_MACRO_TMPL in xrootd_site_selector.py.
ROOT_MACRO_TMPL = {_EMBEDDED_ROOT_PROBE_MACRO_TMPL!r}


def read_config(cfg_path):
    if cfg_path.endswith('.yaml') or cfg_path.endswith('.yml'):
        import yaml
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)
        return {{k: str(v) for k, v in cfg.items()}}
    cfg = {{}}
    with open(cfg_path) as f:
        for line in f:
            line = line.split("#")[0].strip()
            if not line or "=" not in line:
                continue
            k, v = line.split("=", 1)
            cfg[k.strip()] = v.strip()
    return cfg


def write_config(cfg, cfg_path):
    if cfg_path.endswith('.yaml') or cfg_path.endswith('.yml'):
        import yaml
        with open(cfg_path, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    else:
        with open(cfg_path, "w") as f:
            for k, v in cfg.items():
                f.write(f"{{k}}={{v}}\\n")


def detect_local_site():
    for var in ("CMS_LOCAL_SITE", "GLIDEIN_CMSSite", "OSG_SITE_NAME", "SITE_NAME"):
        val = os.environ.get(var, "").strip()
        if val:
            return val
    try:
        hn = socket.getfqdn().lower()
        if hn.endswith(".fnal.gov") or hn == "fnal.gov":
            return "T1_US_FNAL"
        if hn.endswith(".cern.ch") or hn == "cern.ch":
            return "T0_CH_CERN"
    except Exception:
        pass
    return ""


def is_blacklisted(redirector, blacklist):
    redir_l = redirector.lower()
    return any(p.lower() in redir_l for p in blacklist)


def extract_lfn(url):
    if url.startswith("root://"):
        m = re.match(r"root://[^/]+/(.*)", url)
        if m:
            return "/" + m.group(1).lstrip("/")
    return url


def build_url(lfn, redirector):
    # Always use // between redirector and LFN: required for both regular
    # redirectors (root://cmsxrootd.fnal.gov/) and site-specific test
    # redirectors (root://xrootd-cms.infn.it//store/test/xrootd/<site>/).
    return redirector.rstrip("/") + "//" + lfn.lstrip("/")


def is_site_specific_redirector(redirector):
    return SITE_REDIRECTOR_MARKER in redirector


def is_direct_xrootd_candidate(candidate):
    if not candidate.startswith("root://") or is_site_specific_redirector(candidate):
        return False
    match = re.match(r"root://([^/:]+)", candidate)
    host = match.group(1).lower() if match else ""
    return host not in GENERIC_REDIRECTOR_HOSTS


def filter_site_redirectors(redirectors, blacklist=None):
    _blacklist = blacklist or []
    filtered = []
    seen = set()
    for redirector in redirectors:
        if not (is_site_specific_redirector(redirector) or is_direct_xrootd_candidate(redirector)):
            continue
        if is_blacklisted(redirector, _blacklist):
            continue
        if redirector in seen:
            continue
        filtered.append(redirector)
        seen.add(redirector)
    return filtered


def candidate_url(lfn, redirector):
    if is_direct_xrootd_candidate(redirector):
        return redirector
    return build_url(lfn, redirector)


def probe_via_root_macro(url, read_bytes, timeout):
    # Probe url using a ROOT macro (TFile::Open + ReadBuffer).
    macro_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".C", delete=False, prefix="xrd_probe_"
        ) as tmp:
            funcname = os.path.splitext(os.path.basename(tmp.name))[0]
            macro_src = (
                ROOT_MACRO_TMPL
                .replace("__FUNCNAME__", funcname)
                .replace("__URL__", url)
                .replace("__NBYTES__", str(read_bytes))
            )
            tmp.write(macro_src)
            macro_path = tmp.name
        result = subprocess.run(
            ["root", "-b", "-q", "-l", macro_path],
            capture_output=True, timeout=timeout, text=True,
        )
        for line in result.stdout.splitlines():
            if line.startswith("PROBE_RESULT:"):
                parts = line.split(":")
                if len(parts) >= 2 and parts[1] == "FAILED":
                    return None
                if len(parts) >= 3:
                    try:
                        elapsed, to_read = float(parts[1]), float(parts[2])
                        if elapsed > 0 and to_read > 0:
                            return to_read / (1024.0 * 1024.0) / elapsed
                    except ValueError:
                        pass
        return None
    except FileNotFoundError:
        print("ROOT executable not found. Please ensure ROOT is installed and in PATH.")
        return None
    except subprocess.TimeoutExpired:
        print(f"ROOT macro execution timed out after {{timeout}} seconds.")
        return None
    except Exception as e:
        import traceback
        print(f"An unexpected error occurred: {{e}}")
        print(traceback.format_exc())
        return None
    finally:
        if macro_path:
            try:
                os.unlink(macro_path)
            except OSError:
                pass


def probe_redirector(lfn, redirector, read_bytes=PROBE_BYTES, timeout=PROBE_TIMEOUT):
    url = candidate_url(lfn, redirector)
    return probe_via_root_macro(url, read_bytes, timeout)


def select_best_url(url, redirectors, local_site="", blacklist=None):
    lfn = extract_lfn(url)
    if not (lfn.startswith("/store/") or lfn.startswith("/eos/")):
        return url

    active = filter_site_redirectors(redirectors, blacklist)
    if not active:
        raise RuntimeError(f"no site-specific redirectors available for {{lfn}}")

    # Mapping from site name prefix to preferred redirector domain.
    site_domain_map = {{
        "T1_US_FNAL": "fnal.gov",
        "T0_CH_CERN": "cern.ch",
        "T2_US_": "fnal.gov",
        "T2_DE_": "infn.it", "T2_IT_": "infn.it", "T2_FR_": "infn.it",
        "T2_UK_": "infn.it", "T2_ES_": "infn.it", "T2_CH_": "cern.ch",
        "T3_US_": "fnal.gov",
    }}

    def site_matches_redir(site, redir):
        redir_l = redir.lower()
        for prefix, domain in site_domain_map.items():
            if site.startswith(prefix) and domain in redir_l:
                return True
        return site in redir

    results = []
    processed = set()
    with ThreadPoolExecutor(max_workers=max(1, min(len(active), 4))) as pool:
        futures = {{pool.submit(probe_redirector, lfn, r): r for r in active}}
        try:
            for future in as_completed(futures, timeout=PROBE_TIMEOUT + PROBE_POOL_TIMEOUT_BUFFER):
                processed.add(future)
                try:
                    tput = future.result()
                except Exception:
                    continue
                if tput is None:
                    continue
                redir = futures[future]
                if local_site and site_matches_redir(local_site, redir):
                    tput *= LOCAL_SITE_BONUS
                results.append((redir, tput))
        except TimeoutError:
            pass

        for future, redir in futures.items():
            if future in processed or not future.done():
                continue
            try:
                tput = future.result()
            except Exception:
                continue
            if tput is None:
                continue
            if local_site and site_matches_redir(local_site, redir):
                tput *= LOCAL_SITE_BONUS
            results.append((redir, tput))

    if not results:
        raise RuntimeError(f"all probes failed for {{lfn}}")

    results.sort(key=lambda x: x[1], reverse=True)
    best_redir, best_tput = results[0]
    new_url = candidate_url(lfn, best_redir)
    print(f"  [xrd-opt] {{lfn}} -> {{best_redir}} ({{best_tput:.2f}} MB/s)")
    return new_url


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

cfg_path = "{config_file}"
if not os.path.exists(cfg_path):
    print(f"[xrd-opt] Config not found at {{cfg_path}}; skipping site optimisation")
    sys.exit(0)

cfg = read_config(cfg_path)
file_list_raw = cfg.get("fileList", "")
if not file_list_raw:
    print("[xrd-opt] No fileList in config; skipping")
    sys.exit(0)

files = [f.strip() for f in file_list_raw.split(",") if f.strip()]
xrd_files = [f for f in files if f.startswith("root://") or f.startswith("/store/")]
if not xrd_files:
    print("[xrd-opt] No XRootD files in fileList; skipping")
    sys.exit(0)

# Merge compile-time blacklist with runtime xrdBlacklist from config.
runtime_bl = [s.strip() for s in cfg.get("xrdBlacklist", "").split(",") if s.strip()]
blacklist = list(STATIC_BLACKLIST) + runtime_bl
if blacklist:
    print(f"[xrd-opt] Blacklisted sites/patterns: {{blacklist}}")

local_site = detect_local_site()
if local_site:
    print(f"[xrd-opt] Local site: {{local_site}}")
else:
    print("[xrd-opt] Local site not detected; using throughput ranking only")

# Load per-file redirector list produced by _query_rucio during job preparation.
import json as _json
_site_redirectors_file = "site_redirectors.json"
_site_redirectors: dict = {{}}
if os.path.exists(_site_redirectors_file):
    try:
        with open(_site_redirectors_file) as _sr_fh:
            _site_redirectors = _json.load(_sr_fh)
        if _site_redirectors:
            print(f"[xrd-opt] Loaded site redirectors for {{len(_site_redirectors)}} file(s)")
    except Exception as _sr_exc:
        print(f"[xrd-opt] Warning: could not read site_redirectors.json: {{_sr_exc}}")

n_redirectors = len(filter_site_redirectors(next(iter(_site_redirectors.values()), []), blacklist)) if _site_redirectors else 0
print(f"[xrd-opt] Probing up to {{n_redirectors}} redirector(s) per file for {{len(xrd_files)}} file(s)...")
optimized = []
redirector_failures = []
for f in files:
    if f.startswith("root://") or f.startswith("/store/"):
        lfn = extract_lfn(f)
        file_redirectors = _site_redirectors.get(lfn) or []
        try:
            optimized.append(select_best_url(f, file_redirectors, local_site, blacklist))
        except Exception as exc:
            redirector_failures.append(f"{{lfn}} :: {{exc}}")
    else:
        optimized.append(f)

if redirector_failures:
    print("[xrd-opt] ERROR: redirector selection failed for one or more files:")
    for failure in redirector_failures:
        print(f"  [xrd-opt] {{failure}}")
    sys.exit(42)

cfg["fileList"] = ",".join(optimized)
write_config(cfg, cfg_path)
print(f"[xrd-opt] Config updated with optimized file list")
XRDPY
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
    config_file="submit_config.txt",
    python_env_tarball=None,
    shared_archive_name=None,
    runtime_config_relpath=None,
):
    stage_block = stage_inputs_block(eos_sched, config_file) if stage_inputs else ""
    stage_out_pre = ""
    stage_out_post = ""
    if stage_outputs:
        stage_out_pre, stage_out_post = stage_outputs_blocks(eos_sched, config_file)
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
    xrd_optimize_block = "" if stage_inputs else xrootd_optimize_block(config_runtime_path)
    config_runtime_dir = os.path.dirname(config_runtime_path)
    materialize_config_block = ""
    if config_runtime_path != config_file:
        materialize_config_block = (
            (f"mkdir -p {shlex.quote(config_runtime_dir)}\n" if config_runtime_dir else "")
            + "for _cfg_payload in *.txt *.yaml *.yml; do\n"
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
            f"if [ -f \"{tarball_name}\" ]; then\n"
            f"  echo \"Unpacking Python environment from {tarball_name}...\"\n"
            f"  mkdir -p _python_env\n"
            f"  tar -xzf \"{tarball_name}\" -C _python_env\n"
            f"  export PYTHONPATH=\"$PWD/_python_env:${{PYTHONPATH:-}}\"\n"
            f"  export PATH=\"$PWD/_python_env/bin:${{PATH:-}}\"\n"
            f"  export LD_LIBRARY_PATH=\"$PWD/_python_env:${{LD_LIBRARY_PATH:-}}\"\n"
            f"  echo \"Python environment ready (PYTHONPATH=$PYTHONPATH)\"\n"
            f"else\n"
            f"  echo \"WARNING: Python environment tarball {tarball_name} not found\"\n"
            f"fi\n"
        )

    run_script = f"""#!/bin/bash
# fail fast on any command error, undefined var, or pipeline failure
set -euo pipefail
trap 'rc=$?; echo "ERROR: wrapper exited with code $rc"; exit $rc' ERR
# log and re-raise SIGTERM so Condor records ExitBySignal (do not swallow the signal)
trap 'echo "Received SIGTERM - likely timed out by scheduler"; trap - SIGTERM; kill -s SIGTERM $$' SIGTERM
RDF_ORIG_LD_LIBRARY_PATH="${{LD_LIBRARY_PATH:-}}"
RDF_HELPER_PYTHON="$(command -v python3 || command -v python || true)"
run_helper_python() {{
    local helper="${{RDF_HELPER_PYTHON:-python3}}"
    if [ -z "$helper" ]; then
        helper=python3
    fi
    env -u PYTHONHOME LD_LIBRARY_PATH="${{RDF_ORIG_LD_LIBRARY_PATH:-}}" "$helper" "$@"
}}
{root_block}{pre_block}{shared_block}{x509_block}{python_env_block}ls
stage_in_start=$(date +%s)
{stage_out_pre}{stage_block}
stage_in_end=$(date +%s)
echo "Stage-in time: $((stage_in_end - stage_in_start))s"
echo "Check file existence"
ls
{materialize_config_block}{xrd_optimize_block}
analysis_start=$(date +%s)
echo "Starting Analysis"
chmod +x ./{exe_relpath}
# run the analysis but capture its exit status so we can re-raise a signal
set +e
./{exe_relpath} {config_runtime_path}
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
    config_file="submit_config.txt",
    container_image="",
):
    Path(main_dir + "/condor_logs").mkdir(parents=True, exist_ok=True)
    transfer_files = [
        f"{main_dir}/job_$(Process)/{config_file}",
        f"{main_dir}/job_$(Process)/floats.txt",
        f"{main_dir}/job_$(Process)/ints.txt",
        f"{main_dir}/job_$(Process)/site_redirectors.json",
    ]
    if shared_dir_name:
        transfer_files.append(f"{main_dir}/{shared_dir_name}")
    elif shared_archive_name:
        archive_path = os.path.join(main_dir, shared_archive_name)
        if os.path.exists(archive_path):
            transfer_files.append(archive_path)
    else:
        transfer_files.append(f"{main_dir}/job_$(Process)/{exe_relpath}")
        if include_aux:
            transfer_files.append(f"{main_dir}/job_$(Process)/aux")
        if x509loc:
            x509_name = os.path.basename(x509loc)
            transfer_files.append(f"{main_dir}/job_$(Process)/{x509_name}")
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
    requirements_block = (
        'requirements = (TARGET.Arch =?= "X86_64") && '
        '((TARGET.Microarch =!= UNDEFINED && regexp("^x86_64-v([2-9]|[1-9][0-9]+)$", TARGET.Microarch)) '
        '|| (TARGET.Has_sse4_1 =?= True && TARGET.Has_sse4_2 =?= True && TARGET.has_ssse3 =?= True))\n'
    )

    submit_file = f"""universe = vanilla
Executable     =  {main_dir}/condor_runscript.sh
Should_Transfer_Files     = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification     = never
transfer_input_files = {transfer_input_files}
environment = "CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)"
{requirements_block}{stream_block}{want_os_block}{container_block}+RequestMemory={request_memory}
+RequestCpus={request_cpus}
+RequestDisk={request_disk}
+MaxRuntime={max_runtime}
max_transfer_input_mb = 10000
WhenToTransferOutput=On_Exit
transfer_output_files = {config_file}

Output     = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stdout
Error      = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stderr
Log        = {main_dir}/condor_logs/{log_name}
queue {jobs}
"""
    return submit_file


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
    config_file="submit_config.txt",
    container_setup="",
    python_env_tarball=None,
    shared_archive_name=None,
    runtime_config_relpath=None,
):
    submit_path = os.path.join(main_dir, "condor_submit.sub")
    runscript_path = os.path.join(main_dir, "condor_runscript.sh")

    submit_extra_transfer_files = list(extra_transfer_files) if extra_transfer_files else []
    inner_script_name = "condor_runscript_inner.sh"
    inner_runscript_path = os.path.join(main_dir, inner_script_name)

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
        eos_sched=eos_sched,
        shared_dir_name=shared_dir_name,
        config_file=config_file,
        python_env_tarball=python_env_tarball,
        shared_archive_name=shared_archive_name,
        runtime_config_relpath=runtime_config_relpath,
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
                eos_sched=eos_sched,
                include_aux=include_aux,
                shared_dir_name=shared_dir_name,
                shared_archive_name=shared_archive_name,
                config_file=config_file,
                container_image=(container_setup or "").strip(),
            )
        )
    return submit_path