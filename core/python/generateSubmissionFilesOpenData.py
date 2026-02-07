import json
import subprocess
import argparse
import glob
import shutil
import requests
import os

from pathlib import Path
from distutils.dir_util import copy_tree

from submission_backend import (
    read_config,
    get_copy_file_list,
    ensure_symlink,
    write_submit_files,
)
from validate_config import validate_submit_config


def _load_file_indices(recid):
    client_error = None
    if shutil.which("cernopendata-client"):
        result = subprocess.run(
            [
                "cernopendata-client",
                "get-metadata",
                "--recid",
                str(recid),
                "--output-value=_file_indices",
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()
        if result.returncode == 0 and stdout:
            try:
                return json.loads(stdout)
            except json.JSONDecodeError as exc:
                client_error = f"Invalid JSON from cernopendata-client: {exc}"
        else:
            client_error = stderr or "Empty output from cernopendata-client"
    else:
        client_error = "cernopendata-client not found on PATH"

    api_error = None
    try:
        response = requests.get(
            f"https://opendata.cern.ch/api/records/{recid}", timeout=30
        )
    except Exception as exc:
        api_error = f"HTTP request failed: {exc}"
    else:
        if response.ok:
            try:
                payload = response.json()
            except ValueError as exc:
                api_error = f"Invalid JSON from CERN Open Data API: {exc}"
            else:
                metadata = payload.get("metadata", {})
                file_indices = metadata.get("_file_indices")
                if file_indices:
                    return file_indices
                api_error = "CERN Open Data API response missing metadata._file_indices"
        else:
            api_error = f"CERN Open Data API returned HTTP {response.status_code}"

    raise RuntimeError(
        f"Failed to fetch metadata for recid {recid}. "
        f"Client error: {client_error}. API error: {api_error}."
    )


def _link_or_copy_file(src, dst, use_symlink=True):
    if os.path.islink(dst):
        current = os.readlink(dst)
        if os.path.abspath(current) == os.path.abspath(src):
            return
        os.unlink(dst)
    elif os.path.exists(dst):
        if use_symlink:
            os.remove(dst)
        else:
            same_size = os.path.getsize(dst) == os.path.getsize(src)
            same_mtime = int(os.path.getmtime(dst)) == int(os.path.getmtime(src))
            if same_size and same_mtime:
                return
            os.remove(dst)
    if use_symlink:
        ensure_symlink(src, dst)
    else:
        shutil.copy2(src, dst)


def _link_or_copy_dir(src, dst, use_symlink=True):
    if os.path.islink(dst):
        current = os.readlink(dst)
        if os.path.abspath(current) == os.path.abspath(src):
            return
        os.unlink(dst)
    elif os.path.exists(dst):
        if use_symlink:
            shutil.rmtree(dst)
        else:
            return
    if use_symlink:
        ensure_symlink(src, dst)
    else:
        copy_tree(src, dst)


def _ensure_spool_transfer(main_dir, submit_path, runscript_path, marker="condor_spool.out"):
    with open(submit_path) as submit_file:
        lines = submit_file.read().splitlines()

    if not any(line.strip().startswith("transfer_output_files") for line in lines):
        insert_after = None
        for i, line in enumerate(lines):
            if line.strip().startswith("WhenToTransferOutput"):
                insert_after = i
                break
        if insert_after is None:
            for i, line in enumerate(lines):
                if line.strip().startswith("transfer_input_files"):
                    insert_after = i
                    break
        if insert_after is None:
            insert_after = 0
        lines.insert(insert_after + 1, f"transfer_output_files = {marker}")
        with open(submit_path, "w") as submit_file:
            submit_file.write("\n".join(lines) + "\n")

    with open(runscript_path) as runscript_file:
        run_lines = runscript_file.read().splitlines()

    if not any(marker in line for line in run_lines):
        touch_line = f"touch {marker}"
        insert_idx = None
        for i, line in enumerate(run_lines):
            if line.strip() == 'echo "Done!"':
                insert_idx = i
                break
        if insert_idx is None:
            run_lines.append(touch_line)
        else:
            run_lines.insert(insert_idx, touch_line)
        with open(runscript_path, "w") as runscript_file:
            runscript_file.write("\n".join(run_lines) + "\n")


def processMetaData(recid, sampleNames):
    result = _load_file_indices(recid)
    fileDict = dict()
    for entry in result:
        for data in entry["files"]:
            uri = data["uri"]
            key = data["key"].split("_file_index")[0]
            if key not in sampleNames:
                break
            key = sampleNames[key]
            if key in fileDict:
                fileDict[key].append(uri)
            else:
                fileDict[key] = [uri]
    return fileDict


def main():
    parser = argparse.ArgumentParser("Generate files for condor submission")
    parser.add_argument("-c", "--config", type=str, required=True, help="config file to process")
    parser.add_argument("-n", "--name", type=str, required=True, help="submissionName")
    parser.add_argument("-a", "--aux", action="store_true", help="Copy aux into sub folders")
    parser.add_argument("--no-validate", action="store_true", help="skip config validation")
    parser.add_argument("--make-test-job", action="store_true", help="create a local test job using one file")
    parser.add_argument(
        "-f",
        "--files",
        type=int,
        required=False,
        default=30,
        help="number of root files per job, default is 30",
    )
    parser.add_argument("-e", "--exe", type=str, required=True, help="path to the C++ executable to run")
    parser.add_argument(
        "--stage-inputs",
        action="store_true",
        help="xrdcp input files to the worker node before running",
    )
    parser.add_argument(
        "--stage-outputs",
        action="store_true",
        help="xrdcp outputs to final destination after running",
    )
    parser.add_argument("--root-setup", type=str, default="", help="command to setup ROOT (e.g., 'source /path/to/thisroot.sh')")
    parser.add_argument("-x", "--x509", type=str, default="", help="path to x509 proxy (optional)")
    parser.add_argument(
        "--spool",
        action="store_true",
        help="prepare for condor_submit -spool (copy exe/aux instead of symlinks)",
    )

    args = parser.parse_args()

    if not args.no_validate:
        errors, warnings = validate_submit_config(args.config, mode="opendata")
        if warnings:
            print("Config validation warnings:")
            for w in warnings:
                print(f"- {w}")
        if errors:
            print("Config validation failed:")
            for err in errors:
                print(f"- {err}")
            raise SystemExit(1)

    config_path = os.path.abspath(args.config)
    base_dir = os.path.abspath(os.path.join(os.path.dirname(config_path), ".."))

    def resolve_path(path_value):
        if not path_value:
            return path_value
        if os.path.isabs(path_value):
            return path_value
        # Prefer provided relative path if it exists in the current working directory
        if os.path.exists(path_value):
            return os.path.abspath(path_value)
        # Otherwise resolve relative to the config's base directory
        return os.path.abspath(os.path.join(base_dir, path_value))

    configDict = read_config(args.config)
    print(configDict.keys())
    fileSplit = args.files
    configFile = resolve_path(configDict["sampleConfig"])
    saveDirectory = resolve_path(configDict["saveDirectory"])
    exe_path = resolve_path(args.exe)
    exe_relpath = os.path.basename(exe_path)
    if not os.path.exists(exe_path):
        raise SystemExit(f"Executable not found: {exe_path}. Provide a valid path with -e/--exe")

    Path(saveDirectory).mkdir(parents=True, exist_ok=True)
    copyList = get_copy_file_list(configDict)
    lumi = 1
    recids = []
    sampleNames = dict()
    fileDict = dict()

    with open(configFile) as file:
        for line in file:
            storeLine = line[:-1]
            line = line.split("#")[0]
            line = line.strip().split()
            innerDict = dict()
            for pair in line:
                pair = pair.strip().split("=")
                if len(pair) == 2:
                    innerDict[pair[0]] = pair[1]
            if "lumi" in innerDict:
                lumi = float(innerDict["lumi"])
            if "recids" in innerDict:
                recids += innerDict["recids"].strip().split(",")
            if "name" in innerDict and "das" in innerDict:
                sampleNames[innerDict["das"]] = innerDict["name"]

    for recid in recids:
        fileDict.update(processMetaData(recid, sampleNames))

    mainDir = f"condorSub_{args.name}/"
    index = 0
    test_job_created = False
    use_symlink = not args.spool

    shared_dir = os.path.join(mainDir, "shared")
    Path(shared_dir).mkdir(parents=True, exist_ok=True)

    aux_src = resolve_path("aux")
    if aux_src and os.path.exists(aux_src):
        if args.aux or not use_symlink:
            _link_or_copy_dir(aux_src, os.path.join(shared_dir, "aux"), use_symlink=False)
        else:
            _link_or_copy_dir(aux_src, os.path.join(shared_dir, "aux"), use_symlink=True)
    else:
        print(f"Warning: 'aux' directory not found at '{aux_src}'; skipping aux link/copy")
    _link_or_copy_file(exe_path, os.path.join(shared_dir, exe_relpath), use_symlink=use_symlink)
    with open(configFile) as file:
        for line in file:
            line = line.split("#")[0]
            line = line.strip().split()
            innerDict = dict()
            for pair in line:
                pair = pair.strip().split("=")
                if len(pair) == 2:
                    innerDict[pair[0]] = pair[1]

            if "name" in innerDict:
                name = innerDict["name"]
                xsec = float(innerDict["xsec"]) if "xsec" in innerDict else 1.0
                typ = str(innerDict["type"]) if "type" in innerDict else "1"
                norm = float(innerDict["norm"]) if "norm" in innerDict else 1.0
                kfac = float(innerDict["kfac"]) if "kfac" in innerDict else 1.0
                extraScale = float(innerDict["extraScale"]) if "extraScale" in innerDict else 1.0

                fileListFlat = fileDict[name]

                if args.make_test_job and not test_job_created and fileListFlat:
                    test_dir = os.path.join(mainDir, "test_job")
                    Path(test_dir).mkdir(parents=True, exist_ok=True)
                    Path(os.path.join(test_dir, "cfg")).mkdir(parents=True, exist_ok=True)

                    for file in copyList:
                        src = resolve_path(file)
                        dst = os.path.join(test_dir, file)
                        Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src, dst)

                    aux_src = resolve_path("aux")
                    if aux_src and os.path.exists(aux_src):
                        if args.aux or not use_symlink:
                            _link_or_copy_dir(aux_src, os.path.join(test_dir, "aux"), use_symlink=False)
                        else:
                            _link_or_copy_dir(aux_src, os.path.join(test_dir, "aux"), use_symlink=True)
                    else:
                        print(f"Warning: 'aux' directory not found at '{aux_src}'; skipping aux link/copy")

                    _link_or_copy_file(exe_path, os.path.join(test_dir, exe_relpath), use_symlink=use_symlink)

                    test_config = dict(configDict)
                    test_config["fileList"] = fileListFlat[0]
                    test_config["batch"] = "False"
                    test_config.setdefault("threads", "1")
                    test_config["type"] = typ
                    test_config["saveFile"] = "test_output.root"
                    test_config["metaFile"] = "test_output_meta.root"

                    normScale = str(extraScale * kfac * lumi * xsec / norm)

                    if "floatConfig" in test_config.keys():
                        with open(os.path.join(test_dir, test_config["floatConfig"]), "a") as floatFile:
                            floatFile.write("\nnormScale=" + normScale + "\n")
                            floatFile.write("\nsampleNorm=" + str(norm) + "\n")
                    else:
                        with open(os.path.join(test_dir, "cfg/floats.txt"), "w") as floatFile:
                            floatFile.write("\nnormScale=" + normScale + "\n")
                            floatFile.write("\nsampleNorm=" + str(norm) + "\n")
                            test_config["floatConfig"] = "cfg/floats.txt"

                    if "intConfig" in test_config.keys():
                        with open(os.path.join(test_dir, test_config["intConfig"]), "a") as intFile:
                            intFile.write("\ntype=" + typ + "\n")
                    else:
                        with open(os.path.join(test_dir, "cfg/ints.txt"), "w") as intFile:
                            intFile.write("\ntype=" + typ + "\n")
                            test_config["intConfig"] = "cfg/ints.txt"

                    with open(os.path.join(test_dir, "cfg/submit_config.txt"), "w") as file:
                        for key in test_config.keys():
                            file.write(str(key) + "=" + test_config[key] + "\n")

                    print("Test job created. Run locally with:")
                    print(f"cd {test_dir} && ./{exe_relpath} cfg/submit_config.txt")
                    test_job_created = True

                fileList = dict()
                startIndex = 0
                endIndex = min(fileSplit, len(fileListFlat))
                counter = 0
                while startIndex != endIndex:
                    fileList[str(counter)] = ",".join(fileListFlat[startIndex:endIndex])
                    startIndex = endIndex
                    endIndex = min(endIndex + fileSplit, len(fileListFlat))
                    counter += 1
                    print("Made", len(fileList.keys()), "jobs for this sample")

                sampleIndex = 0
                for subDir in fileList:
                    Path(mainDir + "job_" + str(index)).mkdir(parents=True, exist_ok=True)
                    Path(mainDir + "job_" + str(index) + "/cfg").mkdir(parents=True, exist_ok=True)

                    for file in copyList:
                        src = resolve_path(file)
                        dst = os.path.join(mainDir + "job_" + str(index), file)
                        Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src, dst)

                    # aux and executable are provided via shared inputs

                    outputFileName = saveDirectory + "/" + name + "_" + str(sampleIndex) + ".root"
                    configDict["saveFile"] = outputFileName
                    configDict["metaFile"] = saveDirectory + "/" + name + "_" + str(sampleIndex) + "_meta.root"
                    configDict["fileList"] = fileList[subDir]
                    configDict["batch"] = "True"
                    configDict["type"] = typ
                    if args.stage_inputs:
                        original_list = configDict["fileList"]
                        local_inputs = [
                            f"input_{i}.root"
                            for i, item in enumerate(original_list.split(","))
                            if item.strip()
                        ]
                        configDict["__orig_fileList"] = original_list
                        configDict["fileList"] = ",".join(local_inputs)
                    if args.stage_outputs:
                        configDict["__orig_saveFile"] = configDict["saveFile"]
                        configDict["__orig_metaFile"] = configDict["metaFile"]
                        configDict["saveFile"] = os.path.basename(configDict["saveFile"])
                        configDict["metaFile"] = os.path.basename(configDict["metaFile"])
                    normScale = str(extraScale * kfac * lumi * xsec / norm)

                    if "floatConfig" in configDict.keys():
                        with open(mainDir + "job_" + str(index) + "/" + configDict["floatConfig"], "a") as floatFile:
                            floatFile.write("\nnormScale=" + normScale + "\n")
                            floatFile.write("\nsampleNorm=" + str(norm) + "\n")
                    else:
                        with open(mainDir + "job_" + str(index) + "/cfg/floats.txt", "w") as floatFile:
                            floatFile.write("\nnormScale=" + normScale + "\n")
                            floatFile.write("\nsampleNorm=" + str(norm) + "\n")
                            configDict["floatConfig"] = "cfg/floats.txt"

                    if "intConfig" in configDict.keys():
                        with open(mainDir + "job_" + str(index) + "/" + configDict["intConfig"], "a") as intFile:
                            intFile.write("\ntype=" + typ + "\n")
                    else:
                        with open(mainDir + "job_" + str(index) + "/cfg/ints.txt", "w") as intFile:
                            intFile.write("\ntype=" + typ + "\n")
                            configDict["intConfig"] = "cfg/ints.txt"

                    with open(mainDir + "job_" + str(index) + "/cfg/submit_config.txt", "w") as file:
                        for key in configDict.keys():
                            file.write(str(key) + "=" + configDict[key] + "\n")

                    index += 1
                    sampleIndex += 1

    submit_path = write_submit_files(
        mainDir,
        index,
        exe_relpath,
        args.stage_inputs,
        args.stage_outputs,
        args.root_setup,
        x509loc=args.x509 if args.x509 else None,
        pre_setup_lines="ulimit -n 10000\ncmssw-el8\nsource /cvmfs/sft.cern.ch/lcg/views/LCG_104a/x86_64-centos8-gcc11-opt/setup.sh\n",
        want_os="el8",
        max_runtime="3600*2",
        request_memory=2000,
        request_cpus=1,
        request_disk=20000,
        use_shared_inputs=True,
        stream_logs=args.spool,
    )
    if args.spool:
        runscript_path = os.path.join(mainDir, "condor_runscript.sh")
        _ensure_spool_transfer(mainDir, submit_path, runscript_path)

    if index == 1:
        print(index, "job created")
    else:
        print(index, "jobs created")
    print("use the following command to submit:")
    if args.spool:
        print("condor_submit -spool", submit_path)
    else:
        print("condor_submit", submit_path)
    index += 1


if __name__ == "__main__":
    main()