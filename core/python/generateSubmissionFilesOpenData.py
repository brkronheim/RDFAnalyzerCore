import json
import argparse
import os
import shutil
import subprocess

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

from pathlib import Path
from distutils.dir_util import copy_tree

from submission_backend import (
    read_config, 
    get_copy_file_list, 
    write_submit_files,
    write_config,
    get_config_extension,
    ensure_xrootd_redirector,
)
from validate_config import validate_submit_config
from dataset_manifest import DatasetManifest


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
        os.symlink(src, dst)
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
        os.symlink(src, dst)
    else:
        copy_tree(src, dst)


def normalize_config_paths(config_dict):
    normalized = dict(config_dict)
    for key, value in config_dict.items():
        if isinstance(value, str) and ".txt" in value and os.path.sep in value:
            normalized[key] = os.path.basename(value)
    return normalized


def _append_unique_lines(file_path, lines):
    """Ensure the given `lines` appear exactly once in `file_path`.

    If the file exists, remove duplicate identical lines while preserving the
    original order of the first occurrence. Then add any missing lines from
    `lines`. The result overwrites the file so pre-existing duplicate keys are
    eliminated.
    """
    existing_lines = []
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            existing_lines = [ln.rstrip("\n") for ln in f]
    os.makedirs(os.path.dirname(file_path) or ".", exist_ok=True)

    seen = set()
    deduped = []
    for ln in existing_lines:
        if ln in seen:
            continue
        seen.add(ln)
        deduped.append(ln)

    for line in lines:
        if not line:
            continue
        if line in seen:
            continue
        seen.add(line)
        deduped.append(line)

    with open(file_path, "w") as f:
        for ln in deduped:
            f.write(ln + "\n")


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
        lines.insert(insert_after + 1, "transfer_output_files = ")
        with open(submit_path, "w") as submit_file:
            submit_file.write("\n".join(lines) + "\n")


def processMetaData(recid, sampleNames):
    result = _load_file_indices(recid)
    fileDict = dict()
    for entry in result:
        for data in entry["files"]:
            uri = ensure_xrootd_redirector(data["uri"])
            key = data["key"].split("_file_index")[0]
            if key not in sampleNames:
                break
            key = sampleNames[key]
            if key in fileDict:
                fileDict[key].append(uri)
            else:
                fileDict[key] = [uri]
    return fileDict


def _parse_opendata_config(config_file):
    """Parse a sample config file for Open Data submissions.

    Accepts both the legacy key=value text format **and** the new YAML manifest
    format (detected by ``.yaml`` / ``.yml`` extension).  When a YAML manifest
    is provided, ``recids`` are collected from the ``das`` field of each entry
    (comma-separated record IDs stored there by convention for open-data
    samples).

    Returns:
        samples    dict[str, dict]  – {name: legacy_dict}
        recids     list[str]        – record IDs to query
        lumi       float            – luminosity
    """
    ext = os.path.splitext(config_file)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(config_file)
        samples = manifest.to_legacy_sample_dict()
        recids = []
        for entry in manifest.datasets:
            if entry.das:
                for r in entry.das.split(","):
                    r = r.strip()
                    if r and r not in recids:
                        recids.append(r)
        return samples, recids, manifest.lumi

    samples = {}
    recids = []
    lumi = 1.0

    with open(config_file) as fh:
        for line in fh:
            line = line.split("#")[0].strip()
            if not line:
                continue
            parts = line.split()
            inner = {}
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    inner[k.strip()] = v.strip()
            if "lumi" in inner:
                lumi = float(inner["lumi"])
            if "recids" in inner:
                recids += [r.strip() for r in inner["recids"].split(",") if r.strip()]
            if "name" in inner:
                samples[inner["name"]] = inner

    return samples, recids, lumi


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
    parser.add_argument('--max-runtime', type=int, default=3600, help='Max runtime (seconds) for Condor jobs (default: 3600)')
    parser.add_argument("-x", "--x509", type=str, default="", help="path to x509 proxy (optional)")
    parser.add_argument(
        "--eos-sched",
        action="store_true",
        help="use EOS scheduling",
    )
    parser.add_argument(
        "--threads",
        type=int,
        default=4,
        help="number of threads for remote metadata / metadata-fetch and job splitting (default: 4)",
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
    config_ext = get_config_extension(args.config)
    submit_config_name = f"submit_config{config_ext}"
    fileSplit = args.files
    configFile = resolve_path(configDict["sampleConfig"])
    saveDirectory = resolve_path(configDict["saveDirectory"])
    exe_path = resolve_path(args.exe)
    exe_relpath = os.path.basename(exe_path)
    if not os.path.exists(exe_path):
        raise SystemExit(f"Executable not found: {exe_path}. Provide a valid path with -e/--exe")

    Path(saveDirectory).mkdir(parents=True, exist_ok=True)
    copyList = get_copy_file_list(configDict)

    # Parse the sample config (supports both legacy text and YAML manifest)
    samples, recids, lumi = _parse_opendata_config(configFile)

    # Build das-key → sample-name mapping for processMetaData
    sampleNames = {
        sample_dict["das"]: name
        for name, sample_dict in samples.items()
        if sample_dict.get("das")
    }
    fileDict = {}

    # Fetch metadata for recids in parallel to speed up slow network/API calls
    if recids:
        with ThreadPoolExecutor(max_workers=max(1, args.threads)) as executor:
            futures = {executor.submit(processMetaData, recid, sampleNames): recid for recid in recids}
            for fut in as_completed(futures):
                recid = futures[fut]
                try:
                    result = fut.result()
                except Exception as e:
                    print(f"Warning: failed to fetch metadata for recid {recid}: {e}")
                    continue
                # processMetaData returns a dict we can safely merge
                fileDict.update(result)

    mainDir = f"condorSub_{args.name}/"
    index = 0
    test_job_created = False

    if args.eos_sched:
        mainDir = os.path.join("/eos/user/b/bkronhei/RDFAnalyzerCore", mainDir)
        # Inform the user how to enable the EOS condor submission environment
        print("Note: using EOS scheduling — before submitting run: module load lxbatch/eossubmit")

    # Abort if the submission directory already exists to avoid accidental reuse/overwrite
    if os.path.exists(mainDir):
        raise SystemExit(f"Submission directory already exists: {mainDir!r}. Remove it or choose a different --name.")

    x509loc = "x509" if args.x509 else None
    x509_src = resolve_path(args.x509) if args.x509 else None

    aux_src = resolve_path("aux")
    aux_exists = bool(aux_src and os.path.exists(aux_src))
    if not aux_exists:
        print(f"Warning: 'aux' directory not found at '{aux_src}'; skipping aux copy")

    shared_dir_name = "shared_inputs"
    shared_dir = os.path.join(mainDir, shared_dir_name)
    Path(shared_dir).mkdir(parents=True, exist_ok=True)
    _link_or_copy_file(exe_path, os.path.join(shared_dir, exe_relpath), use_symlink=False)
    if aux_exists:
        _link_or_copy_dir(aux_src, os.path.join(shared_dir, "aux"), use_symlink=False)
    if x509_src:
        shutil.copy2(x509_src, os.path.join(shared_dir, x509loc))

    # Copy common config files to shared_inputs/ once (transferred as a directory
    # to all workers, eliminating per-job duplicates and reducing storage footprint).
    for _cfg_file in copyList:
        _src = resolve_path(_cfg_file)
        if _src and os.path.exists(_src):
            _dst = os.path.join(shared_dir, os.path.basename(_src))
            if not os.path.exists(_dst):
                shutil.copy2(_src, _dst)

    # shared_inputs/ directory is transferred as a whole; no individual file entries needed.
    extra_transfer_files = []
    # Thread-safe counter and single-shot event for test-job creation
    index_lock = threading.Lock()
    index_container = {"value": 0}
    def allocate_index():
        with index_lock:
            v = index_container["value"]
            index_container["value"] += 1
            return v

    test_job_event = threading.Event()

    # build the list of sample dicts to process (already parsed by _parse_opendata_config)
    samples_to_process = list(samples.values())

    def process_sample(innerDict):
        nonlocal index_container
        name = innerDict["name"]
        xsec = float(innerDict.get("xsec", 1.0))
        typ = str(innerDict.get("type", "1"))
        norm = float(innerDict.get("norm", 1.0))
        kfac = float(innerDict.get("kfac", 1.0))
        extraScale = float(innerDict.get("extraScale", 1.0))

        if name not in fileDict:
            print(f"Warning: no files found for sample '{name}' (skipping)")
            return 0

        fileListFlat = fileDict[name]

        # create one test job once (thread-safe)
        if args.make_test_job and not test_job_event.is_set() and fileListFlat:
            with index_lock:
                if not test_job_event.is_set():
                    test_dir = os.path.join(mainDir, "test_job")
                    Path(test_dir).mkdir(parents=True, exist_ok=True)

                    for file in copyList:
                        src = resolve_path(file)
                        dst = os.path.join(test_dir, os.path.basename(file))
                        Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                        shutil.copyfile(src, dst)

                    aux_src = resolve_path("aux")
                    if aux_src and os.path.exists(aux_src):
                        _link_or_copy_dir(aux_src, os.path.join(test_dir, "aux"), use_symlink=False)
                    else:
                        print(f"Warning: 'aux' directory not found at '{aux_src}'; skipping aux link/copy")

                    _link_or_copy_file(exe_path, os.path.join(test_dir, exe_relpath), use_symlink=False)

                    test_config = normalize_config_paths(dict(configDict))
                    test_config["fileList"] = fileListFlat[0]
                    test_config["batch"] = "False"
                    test_config.setdefault("threads", "1")
                    test_config["type"] = typ
                    test_config["saveFile"] = "test_output.root"
                    test_config["metaFile"] = "test_output_meta.root"
                    test_config["sampleConfig"] = os.path.basename(configFile)
                    test_config["floatConfig"] = "floats.txt"
                    test_config["intConfig"] = "ints.txt"

                    normScale = str(extraScale * kfac * lumi * xsec / norm)

                    float_file = os.path.join(test_dir, test_config.get("floatConfig", "floats.txt"))
                    _append_unique_lines(float_file, ["normScale=" + normScale, "sampleNorm=" + str(norm)])
                    test_config["floatConfig"] = os.path.basename(float_file)

                    int_file = os.path.join(test_dir, test_config.get("intConfig", "ints.txt"))
                    _append_unique_lines(int_file, ["type=" + typ])
                    test_config["intConfig"] = os.path.basename(int_file)

                    with open(os.path.join(test_dir, "submit_config.txt"), "w") as f:
                        for key in test_config.keys():
                            f.write(str(key) + "=" + test_config[key] + "\n")

                    print("Test job created. Run locally with:")
                    print(f"cd {test_dir} && ./{exe_relpath} submit_config.txt")
                    test_job_event.set()

        # split fileListFlat into per-job groups
        fileList = dict()
        startIndex = 0
        endIndex = min(fileSplit, len(fileListFlat))
        counter = 0
        while startIndex != endIndex:
            fileList[str(counter)] = ",".join(fileListFlat[startIndex:endIndex])
            startIndex = endIndex
            endIndex = min(endIndex + fileSplit, len(fileListFlat))
            counter += 1
        print("Made", len(fileList.keys()), "jobs for sample", name)

        sampleIndex = 0
        jobs_created = 0
        for subDir in fileList:
            my_index = allocate_index()
            job_dir = os.path.join(mainDir, f"job_{my_index}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            outputFileName = saveDirectory + "/" + name + "_" + str(sampleIndex) + ".root"
            job_config = normalize_config_paths(dict(configDict))
            job_config["saveFile"] = outputFileName
            job_config["metaFile"] = saveDirectory + "/" + name + "_" + str(sampleIndex) + "_meta.root"
            job_config["fileList"] = fileList[subDir]
            job_config["batch"] = "True"
            job_config["type"] = typ
            job_config["sampleConfig"] = os.path.basename(configFile)
            job_config["floatConfig"] = "floats.txt"
            job_config["intConfig"] = "ints.txt"
            if args.stage_inputs:
                original_list = job_config["fileList"]
                local_inputs = [
                    f"input_{i}.root"
                    for i, item in enumerate(original_list.split(","))
                    if item.strip()
                ]
                job_config["__orig_fileList"] = original_list
                job_config["fileList"] = ",".join(local_inputs)
            if args.stage_outputs:
                # remote destination should include the job index (process) suffix
                base, ext = os.path.splitext(job_config["saveFile"])
                job_config["__orig_saveFile"] = f"{base}_{sampleIndex}{ext}"
                basem, extm = os.path.splitext(job_config["metaFile"])
                job_config["__orig_metaFile"] = f"{basem}_{sampleIndex}{extm}"
                # keep local basenames as before; the runscript will append CONDOR_PROC
                job_config["saveFile"] = os.path.basename(job_config["saveFile"])
                job_config["metaFile"] = os.path.basename(job_config["metaFile"])

            normScale = str(extraScale * kfac * lumi * xsec / norm)

            float_file = os.path.join(job_dir, job_config.get("floatConfig", "floats.txt"))
            _append_unique_lines(float_file, ["normScale=" + normScale, "sampleNorm=" + str(norm)])
            job_config["floatConfig"] = os.path.basename(float_file)

            int_file = os.path.join(job_dir, job_config.get("intConfig", "ints.txt"))
            _append_unique_lines(int_file, ["type=" + typ])
            job_config["intConfig"] = os.path.basename(int_file)

            with open(os.path.join(job_dir, "submit_config.txt"), "w") as f:
                for key in job_config.keys():
                    f.write(str(key) + "=" + job_config[key] + "\n")

            jobs_created += 1
            sampleIndex += 1

        return jobs_created

    # process samples in parallel
    total_futures = []
    with ThreadPoolExecutor(max_workers=max(1, args.threads)) as executor:
        for innerDict in samples_to_process:
            total_futures.append(executor.submit(process_sample, innerDict))

        # propagate exceptions
        for fut in as_completed(total_futures):
            exc = fut.exception()
            if exc:
                raise exc

    # total job count is the counter value
    index = index_container["value"]

    submit_path = write_submit_files(
        mainDir,
        index,
        exe_relpath,
        args.stage_inputs,
        args.stage_outputs,
        args.root_setup,
        x509loc=x509loc,
        pre_setup_lines="ulimit -n 10000\ncmssw-el8\nsource /cvmfs/sft.cern.ch/lcg/views/LCG_104a/x86_64-centos8-gcc11-opt/setup.sh\n",
        want_os="el8",
        max_runtime="1200",
        request_memory=2000,
        request_cpus=1,
        request_disk=20000,
        extra_transfer_files=extra_transfer_files,
        include_aux=aux_exists,
        shared_dir_name=shared_dir_name,
        eos_sched=args.eos_sched,
        config_file=submit_config_name,
    )
    if index == 1:
        print(index, "job created")
    else:
        print(index, "jobs created")
    print("use the following command to submit:")
    print("condor_submit", submit_path)
    index += 1


if __name__ == "__main__":
    main()