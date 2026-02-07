import os
from pathlib import Path


def read_config(config_file):
    config_dict = {}
    with open(config_file) as file:
        for line in file:
            line = line.split("#")[0]
            line = line.strip().split("=")
            if len(line) == 2:
                config_dict[line[0]] = line[1]
    return config_dict


def get_copy_file_list(config_dict):
    transfer_list = []
    for key in config_dict:
        if ".txt" in config_dict[key]:
            transfer_list.append(config_dict[key])
    return transfer_list


def ensure_symlink(src, dst):
    if os.path.islink(dst) or os.path.exists(dst):
        return
    os.symlink(src, dst)


def stage_inputs_block():
    return """
echo "Staging input files with xrdcp"
python3 - << 'PY'
import subprocess
import os
cfg = {}
with open("cfg/submit_config.txt") as f:
    for line in f:
        line = line.split("#")[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip()

file_list = cfg.get("__orig_fileList", "") or cfg.get("fileList", "")
if not file_list:
    raise SystemExit("fileList not found in cfg/submit_config.txt")

local_paths = []
streams = os.environ.get("XRDCP_STREAMS", "4")
for i, url in enumerate(file_list.split(",")):
    url = url.strip()
    if not url:
        continue
    local_name = f"input_{i}.root"
    print("xrdcp", "-f", "--nopbar", "--streams", streams, url, local_name)
    subprocess.run(["xrdcp", "-f", "--nopbar", "--streams", streams, url, local_name], check=True)
    local_paths.append(local_name)

cfg["fileList"] = ",".join(local_paths)
with open("cfg/submit_config.txt", "w") as f:
    for k, v in cfg.items():
        f.write(f"{k}={v}\\n")
PY
"""


def stage_outputs_blocks():
    pre_block = """
python3 - << 'PY'
import os
cfg = {}
with open("cfg/submit_config.txt") as f:
    for line in f:
        line = line.split("#")[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip()

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

with open("cfg/submit_config.txt", "w") as f:
    for k, v in cfg.items():
        f.write(f"{k}={v}\\n")
PY
"""

    post_block = """
python3 - << 'PY'
import os
import subprocess
import time

cfg = {}
with open("cfg/submit_config.txt") as f:
    for line in f:
        line = line.split("#")[0].strip()
        if not line or "=" not in line:
            continue
        k, v = line.split("=", 1)
        cfg[k.strip()] = v.strip()

def xrdcp_if_exists(local_name, dest, retries=3, timeout=600, streams=None):
    dest = "root://eosuser.cern.ch/" + dest
    print(local_name, dest, retries, timeout, streams)
    if not dest:
        print("no dest")
        return
    if not os.path.exists(local_name):
        print("local path doesn't exist")
        return
    if os.path.getsize(local_name) == 0:
        raise RuntimeError(f"Refusing to stage out empty file: {local_name}")

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
    raise RuntimeError(f"xrdcp failed after {retries} attempts for {local_name} -> {dest}: {last_exc}")

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
):
    stage_block = stage_inputs_block() if stage_inputs else ""
    stage_out_pre = ""
    stage_out_post = ""
    if stage_outputs:
        stage_out_pre, stage_out_post = stage_outputs_blocks()
    root_block = (root_setup + "\n") if root_setup else ""
    pre_block = pre_setup_lines or ""
    x509_block = ""
    if x509loc:
        x509_block = (
            f"export X509_USER_PROXY={x509loc}\n"
            "voms-proxy-info -all\n"
            f"voms-proxy-info -all -file {x509loc}\n"
        )

    shared_block = ""
    if use_shared_inputs:
        shared_block = (
            "# Link shared inputs into working directory\n"
            f"if [ -f shared/{exe_relpath} ] && [ ! -e {exe_relpath} ]; then ln -s shared/{exe_relpath} {exe_relpath}; fi\n"
            "if [ -d shared/aux ] && [ ! -e aux ]; then ln -s shared/aux aux; fi\n"
            f"if [ -f {exe_relpath} ]; then chmod +x {exe_relpath}; fi\n"
        )

    run_script = f"""#!/bin/bash
{root_block}{pre_block}{x509_block}{shared_block}ls
ls cfg
{stage_out_pre}{stage_block}
echo "Check file existence"
ls

echo "Starting Analysis"
./{exe_relpath} cfg/submit_config.txt
{stage_out_post}
echo "Done!"
"""
    return run_script


def generate_condor_submit(
    main_dir,
    jobs,
    exe_relpath,
    x509loc=None,
    want_os="el9",
    max_runtime="1200",
    request_memory=2000,
    request_cpus=1,
    request_disk=20000,
    extra_transfer_files=None,
    use_shared_inputs=False,
    stream_logs=False,
):
    Path(main_dir + "/condor_logs").mkdir(parents=True, exist_ok=True)
    if use_shared_inputs:
        transfer_files = [
            f"{main_dir}/job_$(Process)/cfg",
            f"{main_dir}/shared/{exe_relpath}",
            f"{main_dir}/shared/aux",
        ]
    else:
        transfer_files = [
            f"{main_dir}/job_$(Process)/{exe_relpath}",
            f"{main_dir}/job_$(Process)/cfg",
            f"{main_dir}/job_$(Process)/aux",
        ]
    if x509loc:
        transfer_files.append(x509loc)
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

    submit_file = f"""universe = vanilla
Executable     =  {main_dir}/condor_runscript.sh
Should_Transfer_Files     = YES
on_exit_hold = (ExitBySignal == True) || (ExitCode != 0)
Notification     = never
transfer_input_files = {transfer_input_files}
{stream_block}MY.WantOS = "{want_os}"
+RequestMemory={request_memory}
+RequestCpus={request_cpus}
+RequestDisk={request_disk}
+MaxRuntime={max_runtime}
max_transfer_input_mb = 10000
WhenToTransferOutput=On_Exit
transfer_output_files = 

Output     = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stdout
Error      = {main_dir}/condor_logs/log_$(Cluster)_$(Process).stderr
Log        = {main_dir}/condor_logs/log_$(Cluster)_$(Process).log
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
    max_runtime="1200",
    request_memory=2000,
    request_cpus=1,
    request_disk=20000,
    extra_transfer_files=None,
    use_shared_inputs=False,
    stream_logs=False,
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
            )
        )
    return submit_path
