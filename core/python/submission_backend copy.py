import os
from pathlib import Path
import yaml


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


def ensure_symlink(src, dst):
    if os.path.islink(dst) or os.path.exists(dst):
        return
    os.symlink(src, dst)


def stage_inputs_block(eos_sched=False, config_file="submit_config.txt"):
    return f"""
echo "Staging input files with xrdcp"
python3 - << 'PY'
import subprocess
import os

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

cfg = read_config("{config_file}")

file_list = cfg.get("__orig_fileList", "") or cfg.get("fileList", "")
if not file_list:
    raise SystemExit("fileList not found in {config_file}")

local_paths = []
streams = os.environ.get("XRDCP_STREAMS", "4")

# normalize site-specific test redirectors into the generic store path
def _normalize_test_redirector(u):
    m = __import__('re').search(r"(root://[^/]+)//store/test/xrootd/[^/]+//(store/.+)$", u)
    if m:
        return m.group(1) + "//" + m.group(2)
    return u

for i, url in enumerate(file_list.split(",")):
    url = url.strip()
    if not url:
        continue
    local_name = f"input_{{i}}.root"

    max_attempts = 10
    attempt = 0
    last_exc = None
    tried_normalized = False
    while attempt < max_attempts:
        attempt += 1
        cur_url = url if not tried_normalized else _normalize_test_redirector(url)
        print("xrdcp attempt", attempt, "->", cur_url)
        try:
            subprocess.run(["xrdcp", "-f", "--nopbar", "--streams", streams, cur_url, local_name], check=True, timeout=120+attempt*30)
            local_paths.append(local_name)
            break
        except Exception as exc:
            last_exc = exc
            # if this is a site-specific test redirector and we haven't tried the normalized form yet, try it
            if ("/store/test/xrootd/" in cur_url or "/store/test/xrootd/" in url) and not tried_normalized:
                alt = _normalize_test_redirector(url)
                if alt != url:
                    # outer f-string would try to interpolate {{alt}} at definition time — escape braces
                    print(f"Warning: xrdcp failed for site-specific redirector; retrying with generic path: {{alt}}")
                    tried_normalized = True
                    __import__('time').sleep(2)
                    continue
            print(f"xrdcp attempt {{attempt}} failed for {{cur_url}}: {{exc}}")
            __import__('time').sleep(min(5 * attempt, 30))
    else:
        raise RuntimeError(f"xrdcp failed after {{max_attempts}} attempts for {{url}}: {{last_exc}}")

cfg["fileList"] = ",".join(local_paths)
write_config(cfg, "{config_file}")
PY
"""


def stage_outputs_blocks(eos_sched=False, config_file="submit_config.txt"):
    pre_block = f"""
python3 - << 'PY'
import os

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

def write_config(config_file):

    cfg = read_config(config_file)

    save_file = cfg.get("saveFile", "")
    meta_file = cfg.get("metaFile", "")


    if config_file.endswith('.yaml') or config_file.endswith('.yml'):
        import yaml
        with open(config_file, "w") as f:
            yaml.dump(cfg, f, default_flow_style=False, sort_keys=False)
    else:
        with open(config_file, "w") as f:
            for k, v in cfg.items():
                f.write(f"{{k}}={{v}}\\n")


    # append Condor process id to local save/meta filenames when available
    proc = os.environ.get("CONDOR_PROC", "")

    if "__orig_saveFile" in cfg and cfg.get("__orig_saveFile"):
        base = os.path.basename(cfg["__orig_saveFile"])
    elif save_file:
        cfg["__orig_saveFile"] = save_file
        base = os.path.basename(save_file)
    else:
        base = ""

    if base:
        if proc:
            name, ext = os.path.splitext(base)
            cfg["saveFile"] = f"{{name}}_{{proc}}{{ext}}"
        else:
            cfg["saveFile"] = base

    if "__orig_metaFile" in cfg and cfg.get("__orig_metaFile"):
        base_meta = os.path.basename(cfg["__orig_metaFile"])
    elif meta_file:
        cfg["__orig_metaFile"] = meta_file
        base_meta = os.path.basename(meta_file)
    else:
        base_meta = ""

    if base_meta:
        if proc:
            namem, extm = os.path.splitext(base_meta)
            cfg["metaFile"] = f"{{namem}}_{{proc}}{{extm}}"
        else:
            cfg["metaFile"] = base_meta

write_config("{config_file}")
PY
"""

    post_block = f"""
python3 - << 'PY'
import os
import subprocess
import time

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

def xrdcp_if_exists(local_name, dest, retries=3, timeout=120, streams=None):
    dest = "root://eosuser.cern.ch/" + dest
    print(local_name, dest, retries, timeout, streams)
    if not dest:
        print("no dest")
        return
    if not os.path.exists(local_name):
        print("local path doesn't exist")
        return
    if os.path.getsize(local_name) == 0:
        raise RuntimeError(f"Refusing to stage out empty file: {{local_name}}")

    if streams is None:
        streams = os.environ.get("XRDCP_STREAMS", "4")
    cmd = [
        "xrdcp",
        "-f",
        "--nopbar",
        "--streams",
        str(streams),
        "--retry",
        "3",
        local_name,
        dest,
    ]
    last_exc = None
    for attempt in range(1, retries + 1):
        print(attempt, cmd)
        try:
            subprocess.run(cmd, check=True, timeout=timeout + 60)
            return
        except Exception as exc:
            last_exc = exc
            time.sleep(min(10 * attempt, 30))
    raise RuntimeError(f"xrdcp failed after {{retries}} attempts for {{local_name}} -> {{dest}}: {{last_exc}}")

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
):
    stage_block = stage_inputs_block(eos_sched, config_file) if stage_inputs else ""
    stage_out_pre = ""
    stage_out_post = ""
    if stage_outputs:
        stage_out_pre, stage_out_post = stage_outputs_blocks(eos_sched, config_file)
    # Wrap user-provided setup commands so 'unbound variable' (nounset)
    # in sourced scripts doesn't cause the wrapper to exit immediately.
    # The wrapper sets 'set -euo pipefail' — many site-provided setup scripts
    # (LCG, module views) reference variables that may be unset. Temporarily
    # disable 'nounset' for the setup block and then re-enable it.
    if root_setup:
        root_block = "set +u\n" + root_setup.rstrip() + "\nset -u\n"
    else:
        root_block = ""
    pre_block = pre_setup_lines or ""
    x509_name = os.path.basename(x509loc) if x509loc else ""
    shared_block = ""
    if shared_dir_name:
        shared_block = (
            f"if [ -f {shared_dir_name}/{exe_relpath} ]; then cp -f {shared_dir_name}/{exe_relpath} .; chmod +x {exe_relpath}; fi\n"
            f"if [ -d {shared_dir_name} ]; then find {shared_dir_name} -maxdepth 1 -type f -exec cp -f {{}} . \\;; fi\n"
            f"if [ -d {shared_dir_name}/aux ] && [ ! -e aux ]; then cp -R {shared_dir_name}/aux aux; fi\n"
        )
        shared_block += (
            f"if [ -d {shared_dir_name}/aux ]; then "
            f"find {shared_dir_name}/aux -type f -exec cp -f {{}} . \\;; fi\n"
        )
        shared_block += (
            f"if [ -d {shared_dir_name}/lib ] && [ ! -e lib ]; then cp -R {shared_dir_name}/lib lib; fi\n"
        )
        if x509_name:
            shared_block += f"if [ -f {shared_dir_name}/{x509_name} ]; then cp -f {shared_dir_name}/{x509_name} .; fi\n"

    x509_block = ""
    if x509_name:
        x509_block = (
            f"export X509_USER_PROXY={x509_name}\n"
            "voms-proxy-info -all\n"
            f"voms-proxy-info -all -file {x509_name}\n"
        )

    run_script = f"""#!/bin/bash
# fail fast on any command error, undefined var, or pipeline failure
set -euo pipefail
trap 'rc=$?; echo "ERROR: wrapper exited with code $rc"; exit $rc' ERR
# log and re-raise SIGTERM so Condor records ExitBySignal (do not swallow the signal)
trap 'echo "Received SIGTERM - likely timed out by scheduler"; trap - SIGTERM; kill -s SIGTERM $$' SIGTERM
{root_block}{pre_block}{shared_block}{x509_block}ls

# Setup LD_LIBRARY_PATH for staged/shared libraries
# Prefer a staged lib/ directory (if present), and also include the
# current working directory when individual .so files were transferred
# by Condor into the job's base directory.
if [ -d "lib" ]; then
  export LD_LIBRARY_PATH="$PWD/lib:${{LD_LIBRARY_PATH:-}}"
  echo "LD_LIBRARY_PATH set to: $LD_LIBRARY_PATH"
fi
# If there are any .so files in the base directory, add $PWD as well
# (Condor may transfer individual .so files rather than a lib/ directory).
if compgen -G "*.so" > /dev/null; then
  export LD_LIBRARY_PATH="$PWD:${{LD_LIBRARY_PATH:-}}"
  echo "Added $PWD to LD_LIBRARY_PATH (found local .so files)"
fi

stage_in_start=$(date +%s)
{stage_out_pre}{stage_block}
stage_in_end=$(date +%s)
echo "Stage-in time: $((stage_in_end - stage_in_start))s"
echo "Check file existence"
ls

analysis_start=$(date +%s)
echo "Starting Analysis"
# run the analysis but capture its exit status so we can re-raise a signal
set +e
./{exe_relpath} {config_file}
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


def generate_condor_submit(
    main_dir,
    jobs,
    exe_relpath,
    x509loc=None,
    want_os="el9",
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
):
    Path(main_dir + "/condor_logs").mkdir(parents=True, exist_ok=True)
    transfer_files = []
    main_dir_path = Path(main_dir)

    # always consider per-job config files (use job_$(Process) patterns only if
    # at least one job dir actually contains the file)
    job_root = Path(main_dir)
    job_dirs = [p for p in job_root.iterdir() if p.is_dir() and p.name.startswith('job_')]

    def _collect_files_under(directory: Path, relative_to: Path = None) -> list:
        files = []
        if not directory.exists() or not directory.is_dir():
            return files
        for p in sorted(directory.rglob('*')):
            if p.is_file():
                if relative_to is not None:
                    try:
                        files.append(p.relative_to(relative_to).as_posix())
                    except ValueError:
                        files.append(str(p))
                else:
                    files.append(str(p))
        return files

    def _any_job_has(relpath: str) -> bool:
        for jd in job_dirs:
            if (jd / relpath).exists():
                return True
        return False

    # job-scoped files in transfer_input_files should be relative to main_dir
    # and include the job_$(Process) prefix.
    def _job_scoped(name: str) -> str:
        return f"job_$(Process)/{name}"

    # Per-job unique files only
    if _any_job_has(config_file):
        transfer_files.append(_job_scoped(config_file))
    if _any_job_has('floats.txt'):
        transfer_files.append(_job_scoped('floats.txt'))
    if _any_job_has('ints.txt'):
        transfer_files.append(_job_scoped('ints.txt'))

    # Shared/common files come from shared_inputs only
    if shared_dir_name:
        shared_dir = main_dir_path / shared_dir_name
        if shared_dir.exists() and shared_dir.is_dir():
            transfer_files.extend(_collect_files_under(shared_dir, relative_to=main_dir_path))

    if extra_transfer_files:
        transfer_files.extend(extra_transfer_files)

    # filter out plain files/dirs by checking existence in at least one job dir
    filtered_files = []
    for path in transfer_files:
        if '$(' in path:
            filtered_files.append(path)
            continue
        p = Path(path)
        if p.is_absolute():
            if p.exists():
                filtered_files.append(path)
            continue
        if p.exists():
            filtered_files.append(path)
            continue
        if (main_dir_path / p).exists():
            filtered_files.append(path)
            continue
        if _any_job_has(path):
            filtered_files.append(path)

    # remove duplicates while preserving order
    seen = set()
    deduped = []
    for item in filtered_files:
        if item in seen:
            continue
        seen.add(item)
        deduped.append(item)
    transfer_input_files = ",".join(deduped)

    stream_block = ""
    #if stream_logs:
    #    stream_block = "stream_output = True\nstream_error = True\n"

    log_name = "log_$(Cluster).log"

    submit_file = f"""universe = vanilla
Executable     =  {main_dir}/condor_runscript.sh
Should_Transfer_Files     = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification     = never
initialdir = {main_dir}
environment = CONDOR_PROC=$(Process) CONDOR_CLUSTER=$(Cluster)
transfer_input_files = {transfer_input_files}
{stream_block}MY.WantOS = "{want_os}"
+RequestMemory={request_memory}
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
    want_os="el9",
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
):
    submit_path = os.path.join(main_dir, "condor_submit.sub")
    runscript_path = os.path.join(main_dir, "condor_runscript.sh")
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
                extra_transfer_files=extra_transfer_files,
                use_shared_inputs=use_shared_inputs,
                stream_logs=stream_logs,
                eos_sched=eos_sched,
                include_aux=include_aux,
                shared_dir_name=shared_dir_name,
                config_file=config_file,
            )
        )
    with open(runscript_path, "w") as condor_sub:
        condor_sub.write(
            generate_condor_runscript(
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
            )
        )
    return submit_path
