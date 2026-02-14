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
for i, url in enumerate(file_list.split(",")):
    url = url.strip()
    if not url:
        continue
    local_name = f"input_{{i}}.root"
    print("xrdcp", "-f", "--nopbar", "--streams", streams, url, local_name)
    subprocess.run(["xrdcp", "-f", "--nopbar", "--streams", streams, url, local_name], check=True)
    local_paths.append(local_name)

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

write_config(cfg, "{config_file}")
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
    root_block = (root_setup + "\n") if root_setup else ""
    pre_block = pre_setup_lines or ""
    x509_name = os.path.basename(x509loc) if x509loc else ""
    shared_block = ""
    if shared_dir_name:
        shared_block = (
            f"if [ -f {shared_dir_name}/{exe_relpath} ]; then cp -f {shared_dir_name}/{exe_relpath} .; chmod +x {exe_relpath}; fi\n"
            f"if [ -d {shared_dir_name}/aux ] && [ ! -e aux ]; then cp -R {shared_dir_name}/aux aux; fi\n"
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
{root_block}{pre_block}{shared_block}{x509_block}ls
stage_in_start=$(date +%s)
{stage_out_pre}{stage_block}
stage_in_end=$(date +%s)
echo "Stage-in time: $((stage_in_end - stage_in_start))s"
echo "Check file existence"
ls

analysis_start=$(date +%s)
echo "Starting Analysis"
./{exe_relpath} {config_file}
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
    max_runtime="1200",
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
    transfer_files = [
        f"{main_dir}/job_$(Process)/{config_file}",
        f"{main_dir}/job_$(Process)/floats.txt",
        f"{main_dir}/job_$(Process)/ints.txt",
    ]
    if shared_dir_name:
        transfer_files.append(f"{main_dir}/{shared_dir_name}/{exe_relpath}")
        if include_aux:
            transfer_files.append(f"{main_dir}/{shared_dir_name}/aux")
        if x509loc:
            x509_name = os.path.basename(x509loc)
            transfer_files.append(f"{main_dir}/{shared_dir_name}/{x509_name}")
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
    max_runtime="1200",
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
