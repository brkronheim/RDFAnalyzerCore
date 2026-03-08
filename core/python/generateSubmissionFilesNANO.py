import argparse
import os
import re
import shutil
import subprocess
from pathlib import Path
import time

import requests
from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3
from rucio.client import Client
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

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


def get_proxy_path() -> str:
    '''
    Checks if the VOMS proxy exists and if it is valid
    for at least 1 hour.
    If it exists, returns the path of it'''
    try:
        subprocess.run("voms-proxy-info -exists -valid 0:20", shell=True, check=True)
    except subprocess.CalledProcessError:
        raise Exception(
            "VOMS proxy expirend or non-existing: please run `voms-proxy-init -voms cms -rfc --valid 168:0`"
        )

    # Now get the path of the certificate
    proxy = subprocess.check_output(
        "voms-proxy-info -path", shell=True, text=True
    ).strip()
    return proxy


# Rucio needs the default configuration --> taken from CMS cvmfs defaults
if "RUCIO_HOME" not in os.environ:
    os.environ["RUCIO_HOME"] = "/cvmfs/cms.cern.ch/rucio/current"


def get_rucio_client(proxy=None) -> Client:
    """
    Open a client to the CMS rucio server using x509 proxy.

    Parameters
    ----------
        proxy : str, optional
            Use the provided proxy file if given, if not use `voms-proxy-info` to get the current active one.

    Returns
    -------
        nativeClient: rucio.Client
            Rucio client
    """
    try:
        if not proxy:
            proxy = get_proxy_path()
        nativeClient = Client()
        return nativeClient

    except Exception as e:
        print("Wrong Rucio configuration, impossible to create client")
        raise e


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
        shutil.copytree(src, dst, dirs_exist_ok=True)


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


def _collect_local_shared_libs(exe_path, repo_root):
    """Collect shared libraries required by `exe_path` that live under `repo_root`.

    Returns a mapping of staged filename -> source absolute path.
    """
    staged = {}
    try:
        ldd_output = subprocess.check_output(["ldd", exe_path], text=True, stderr=subprocess.STDOUT)
    except Exception as err:
        print(f"Warning: failed to inspect shared-library deps via ldd for '{exe_path}': {err}")
        return staged

    repo_prefix = os.path.abspath(repo_root)
    if not repo_prefix.endswith(os.path.sep):
        repo_prefix += os.path.sep

    for line in ldd_output.splitlines():
        line = line.strip()
        if not line or "=> not found" in line:
            continue

        soname = None
        resolved = None

        match_arrow = re.match(r"^(\S+)\s*=>\s*(\S+)", line)
        if match_arrow:
            soname = os.path.basename(match_arrow.group(1))
            resolved = match_arrow.group(2)
        else:
            match_abs = re.match(r"^(\/\S+)\s+\(", line)
            if match_abs:
                resolved = match_abs.group(1)
                soname = os.path.basename(resolved)

        if not resolved or not os.path.isabs(resolved):
            continue

        real_path = os.path.realpath(resolved)
        if not os.path.exists(real_path):
            continue
        if not real_path.startswith(repo_prefix):
            continue

        real_name = os.path.basename(real_path)
        if ".so" not in real_name:
            continue

        staged[real_name] = real_path
        if soname and ".so" in soname:
            staged[soname] = real_path

    return staged




def queryRucio(directory, fileSplit, WL, BL, siteOverride, client):
    runningSize = 0

    groups = dict()
    filesFound = 0
    group = 0
    groupCount = dict()
    groupSizes = dict()
    print("checking Rucio for", directory)

    # Basic validation of the DAS/Rucio path
    if not directory or not isinstance(directory, str) or not directory.startswith("/"):
        print(f"Warning: invalid DAS/Rucio path: '{directory}' - skipping")
        return {}

    # Robustly call Rucio with retries/backoff to handle intermittent stream errors
    max_retries = 5
    backoff = 0.5
    rucioOutput = None
    for attempt in range(1, max_retries + 1):
        try:
            rucio_gen = client.list_replicas([{"scope": "cms", "name": directory}])
            rucioOutput = list(rucio_gen)
            break
        except (ChunkedEncodingError, RequestException, urllib3.exceptions.ProtocolError) as e:
            if attempt < max_retries:
                print(f"Warning: Rucio request failed (attempt {attempt}/{max_retries}): {e}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue
            else:
                print(f"Error: failed to list replicas for '{directory}' after {max_retries} attempts: {e}")
                return {}
        except Exception as e:
            print(f"Error: unexpected error while listing replicas for '{directory}': {e}")
            return {}

    if rucioOutput is None:
        print(f"Error: no response from Rucio for '{directory}'")
        return {}

    if not rucioOutput:
        print(f"Warning: no files found for '{directory}'")
        return {}

    #print("Output received")
    #print(len(rucioOutput), "files found")
    for filedata in rucioOutput:
         file = filedata["name"]
         states = filedata.get("states", {})
         size = filedata.get("bytes", 0)*1e-9
         redirector="root://xrootd-cms.infn.it/"
         filesFound += 1
         runningSize += size
         if(runningSize > fileSplit):
             group += 1
             runningSize = size

         for site in states:
             
             if(states[site]=="AVAILABLE" and "Tape" not in site):
                 site = site.replace("_Disk", "")
                 if(site in WL):
                     #print("Good", site)
                     redirector = "root://xrootd-cms.infn.it/" +  "/store/test/xrootd/"+site+"/"
                     break
                 """
                 elif(site in BL or "T3" in site or "Tape" in site):
                     continue
                     #print("Bad", site)
                 else:
                     #
                     #print("Neutral", site)
                     redirector =  "root://xrootd-cms.infn.it/" + "/store/test/xrootd/"+site+"/"
                """
                 
         if(group in groups):
             groups[group] +=","+ensure_xrootd_redirector(file, redirector)
             groupCount[group]+=1
             groupSizes[group]+=round(size,1)
         else:
             groups[group] = ensure_xrootd_redirector(file, redirector)
             groupCount[group]=1
             groupSizes[group]=round(size,1)
    #for x in range(len(groups)):
    #    print("Group", x, "has", groupCount[x], "files with total size", groupSizes[x], "GB")
    #print("groupCounts:", groupCount)
    #print("groupSizes:", groupSizes)
    #print(len(groups), "groups found and", filesFound, "files found")
    return(groups)

def getSampleList(configFile):
    """Parse a sample config file and return ``(sampleDict, baseDirectoryList, lumi, WL, BL)``.

    Accepts both the legacy key=value text format **and** the new YAML manifest
    format (detected by ``.yaml`` / ``.yml`` extension).  When a YAML manifest
    is supplied the returned ``sampleDict`` contains all :class:`DatasetEntry`
    objects converted to legacy dicts, and ``lumi`` / ``WL`` / ``BL`` are
    taken from the manifest's global settings.
    """
    ext = os.path.splitext(configFile)[1].lower()
    if ext in (".yaml", ".yml"):
        manifest = DatasetManifest.load_yaml(configFile)
        return (
            manifest.to_legacy_sample_dict(),
            [],
            manifest.lumi,
            manifest.whitelist,
            manifest.blacklist,
        )

    configDict = dict()
    baseDirectoryList = []
    lumi = 1
    WL = []
    BL = []
    with open(configFile) as file:
        for line in file:
            line = line.split("#")[0] 
            line = line.strip().split()
            innerDict=dict()
            for pair in line:
                pair = pair.strip().split("=")
                if(len(pair)==2):
                    innerDict[pair[0]] = pair[1]
            if("name" in innerDict):
                configDict[innerDict["name"]] = innerDict
            elif("prefix_cern" in innerDict):
                baseDirectoryList.append(innerDict["prefix_cern"])
            elif("lumi" in innerDict):
                lumi = float(innerDict["lumi"])
            elif("WL" in innerDict):
                WL = innerDict["WL"].split(",")
            elif("BL" in innerDict):
                BL = innerDict["BL"].split(",")
    return(configDict, baseDirectoryList, lumi, WL, BL)

def main():
    parser = argparse.ArgumentParser("Generate files for condor submission")
    parser.add_argument('-c', '--config', type=str, required=True, help="config file to process")
    parser.add_argument('-n', '--name', type=str, required=True, help="submissionName")
    parser.add_argument('-s', '--size', type=int, required=False, default=10, help="GB of data to process per job, default is 10")
    parser.add_argument('-t', '--time', type=int, required=False, default=3600, help="max runtime per job in seconds, default is 3600 (1 hour)")
    parser.add_argument('-x', '--x509', type=str, required=True, help="location of x509 proxy to use (such as /afs/cern.ch/user/u/username/private/x509)")
    parser.add_argument('-e', '--exe', type=str, required=True, help="path to the C++ executable to run")
    parser.add_argument('--stage-inputs', action='store_true', help="xrdcp input files to the worker node before running")
    parser.add_argument('--stage-outputs', action='store_true', help="xrdcp outputs to final destination after running")
    parser.add_argument('--root-setup', type=str, default="", help="path to a setup script file; file contents are embedded into the inner worker runscript")
    parser.add_argument('--container-setup', type=str, default="", help="container setup command, script path, or @script path to run before the main executable")
    parser.add_argument('--no-validate', action='store_true', help="skip config validation")
    parser.add_argument('--make-test-job', action='store_true', help="create a local test job using one file")
    parser.add_argument(
        '--eos-sched',
        action='store_true',
        help='use EOS scheduling',
    )
    parser.add_argument(
        '--threads',
        type=int,
        default=4,
        help='number of threads for DAS/Rucio queries and job splitting (default: 4)',
    )

    args = parser.parse_args()

    if not args.no_validate:
        errors, warnings = validate_submit_config(args.config, mode="nano")
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

    def read_setup_file(file_value):
        if not file_value:
            return ""
        candidate_path = resolve_path(file_value)
        if not candidate_path or not os.path.isfile(candidate_path):
            raise SystemExit(f"--root-setup must point to an existing file. Not found: {file_value}")
        with open(candidate_path, "r") as setup_file:
            return setup_file.read().rstrip("\n")

    def resolve_setup_block(setup_value):
        if not setup_value:
            return ""
        candidate = setup_value[1:] if setup_value.startswith("@") else setup_value
        candidate_path = resolve_path(candidate)
        if candidate_path and os.path.isfile(candidate_path):
            with open(candidate_path, "r") as setup_file:
                return setup_file.read().rstrip("\n")
        return setup_value.strip()

    configDict = read_config(args.config)
    root_setup_block = read_setup_file(args.root_setup)
    container_setup_block = args.container_setup
    #print(container_setup_block)
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    #print(configDict.keys())
    fileSplit = args.size
    # x509loc will be set later
    exe_path = resolve_path(args.exe)
    exe_relpath = os.path.basename(exe_path)
    if not os.path.exists(exe_path):
        raise SystemExit(f"Executable not found: {exe_path}. Provide a valid path with -e/--exe")
    config = resolve_path(configDict['sampleConfig'])
    saveDirectory = resolve_path(configDict['saveDirectory'])
    try:
        Path(saveDirectory).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(f"Cannot create saveDirectory at {saveDirectory}: {e}")
    copyList = get_copy_file_list(configDict)



    sampleList, baseDirectoryList, lumi, WL, BL = getSampleList(config)
    #print("WL: ", WL)
    #print("BL: ", BL)

    # Create a thread-safe index and a one-time event for the test job
    index_lock = threading.Lock()
    index_container = {"value": 0}
    def allocate_index():
        with index_lock:
            v = index_container["value"]
            index_container["value"] += 1
            return v

    test_job_event = threading.Event()

    mainDir = f"condorSub_{args.name}/"

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

    local_shared_libs = _collect_local_shared_libs(exe_path, repo_root)
    for staged_name, src_path in sorted(local_shared_libs.items()):
        _link_or_copy_file(src_path, os.path.join(shared_dir, staged_name), use_symlink=False)
    if local_shared_libs:
        print(f"Staged {len(local_shared_libs)} local shared library file(s) into '{shared_dir_name}'")

    aux_files = []
    if aux_exists:
        for aux_file in sorted(Path(aux_src).iterdir()):
            if aux_file.is_file():
                _link_or_copy_file(str(aux_file), os.path.join(shared_dir, aux_file.name), use_symlink=False)
                aux_files.append(aux_file.name)
    if x509_src:
        shutil.copy2(x509_src, os.path.join(shared_dir, x509loc))

    copy_basenames = sorted({os.path.basename(path) for path in copyList})
    skip_transfer = {"floats.txt", "ints.txt", "submit_config.txt"}
    extra_transfer_files = [
        os.path.join(mainDir, "job_$(Process)", name)
        for name in copy_basenames
        if name not in skip_transfer
    ]
    extra_transfer_files.extend(
        os.path.join(mainDir, shared_dir_name, staged_name)
        for staged_name in sorted(local_shared_libs.keys())
    )
    extra_transfer_files.extend(
        os.path.join(mainDir, shared_dir_name, aux_fname)
        for aux_fname in aux_files
    )

    # worker that processes a single sample (can run in parallel)
    def process_sample(key, sample):
        # create a per-thread Rucio client to avoid client thread-safety issues
        try:
            client_local = get_rucio_client()
        except Exception as e:
            print(f"Warning: cannot create Rucio client for sample {key}: {e}")
            return 0

        #print("Checking ", key)
        name = sample['name']
        das = sample['das']
        xsec = float(sample['xsec'])
        typ = sample['type']
        norm = float(sample['norm']) if 'norm' in sample else 1.0
        kfac = float(sample['kfac']) if 'kfac' in sample else 1.0
        site = sample.get('site', '')
        extraScale = float(sample['extraScale']) if 'extraScale' in sample else 1.0

        sampleIndex = 0

        das_entries = [d.strip() for d in das.split(',') if d and d.strip()]
        # parallelize queries across DAS entries for this sample
        groups_result = {}
        if not das_entries:
            groups_result = {}
        elif len(das_entries) == 1:
            groups_result = queryRucio(das_entries[0], fileSplit, WL, BL, site, client_local)
        else:
            # ensure ThreadPoolExecutor gets at least 1 worker
            workers = max(1, min(len(das_entries), args.threads))
            with ThreadPoolExecutor(max_workers=workers) as das_executor:
                das_futures = {das_executor.submit(queryRucio, das_entry, fileSplit, WL, BL, site, client_local): das_entry for das_entry in das_entries}
                # collect returned groups per DAS entry (preserve group boundaries)
                all_groups = []
                for fut in as_completed(das_futures):
                    res = fut.result()
                    if not res:
                        continue
                    for g in sorted(res.keys()):
                        grp = res[g]
                        if not grp:
                            continue
                        parts = [p.strip() for p in grp.split(',') if p.strip()]
                        all_groups.append(parts)

                # preserve per-dataset groups but remove duplicate files across groups
                seen = set()
                groups_result = {}
                idx = 0
                for parts in all_groups:
                    uniq_parts = []
                    for f in parts:
                        if f in seen:
                            continue
                        seen.add(f)
                        uniq_parts.append(f)
                    if uniq_parts:
                        groups_result[idx] = ",".join(uniq_parts)
                        idx += 1

        # Create a single local test job (race-protected)
        if args.make_test_job and not test_job_event.is_set() and groups_result:
            first_group = next(iter(groups_result.values()))
            first_file = first_group.split(",")[0] if first_group else ""
            if first_file:
                with index_lock:
                    if not test_job_event.is_set():
                        test_dir = os.path.join(mainDir, "test_job")
                        Path(test_dir).mkdir(parents=True, exist_ok=True)

                        for file in copyList:
                            src = resolve_path(file)
                            dst = os.path.join(test_dir, os.path.basename(file))
                            Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                            shutil.copyfile(src, dst)

                        aux_src_local = resolve_path("aux")
                        if aux_src_local and os.path.exists(aux_src_local):
                            for aux_file in sorted(Path(aux_src_local).iterdir()):
                                if aux_file.is_file():
                                    _link_or_copy_file(str(aux_file), os.path.join(test_dir, aux_file.name), use_symlink=False)
                        else:
                            print(f"Warning: 'aux' directory not found at '{aux_src_local}'; skipping aux link/copy")
                        _link_or_copy_file(exe_path, os.path.join(test_dir, exe_relpath), use_symlink=False)

                        test_config = normalize_config_paths(dict(configDict))
                        test_config["fileList"] = first_file
                        test_config["batch"] = "False"
                        test_config.setdefault("threads", "1")
                        test_config["type"] = typ
                        test_config["saveFile"] = "test_output.root"
                        test_config["metaFile"] = "test_output_meta.root"
                        test_config["sampleConfig"] = os.path.basename(config)
                        test_config["floatConfig"] = "floats.txt"
                        test_config["intConfig"] = "ints.txt"

                        normScale = str(extraScale * kfac * lumi * xsec / norm)

                        float_file = os.path.join(test_dir, test_config.get("floatConfig", "floats.txt"))
                        _append_unique_lines(float_file, ["normScale="+normScale, "sampleNorm="+str(norm)])
                        test_config["floatConfig"] = os.path.basename(float_file)

                        int_file = os.path.join(test_dir, test_config.get("intConfig", "ints.txt"))
                        _append_unique_lines(int_file, ["type="+typ])
                        test_config["intConfig"] = os.path.basename(int_file)

                        with open(os.path.join(test_dir, "submit_config.txt"), "w") as f:
                            for cfg_key in test_config.keys():
                                f.write(str(cfg_key)+"="+test_config[cfg_key]+"\n")

                        print("Test job created. Run locally with:")
                        print(f"cd {test_dir} && ./{exe_relpath} submit_config.txt")
                        test_job_event.set()

        # create per-job folders for this sample
        jobs_created = 0
        for subDir in groups_result:
            my_index = allocate_index()
            job_dir = os.path.join(mainDir, f"job_{my_index}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            for file in copyList:
                src = resolve_path(file)
                dst = os.path.join(job_dir, os.path.basename(file))
                Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)

            outputFileName = saveDirectory + "/" + name + "_" + str(sampleIndex) + ".root"
            job_config = normalize_config_paths(dict(configDict))
            job_config["saveFile"] = outputFileName
            job_config["metaFile"] = saveDirectory + "/" + name + "_" + str(sampleIndex) + "_meta.root"
            job_config["fileList"] = groups_result[subDir]
            job_config["batch"] = "True"
            job_config["type"] = typ
            job_config["sampleConfig"] = os.path.basename(config)
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
                job_config["__orig_saveFile"] = job_config["saveFile"]
                job_config["__orig_metaFile"] = job_config["metaFile"]
                job_config["saveFile"] = os.path.basename(job_config["saveFile"])
                job_config["metaFile"] = os.path.basename(job_config["metaFile"])

            normScale = str(extraScale * kfac * lumi * xsec / norm)

            float_file = os.path.join(job_dir, job_config.get("floatConfig", "floats.txt"))
            _append_unique_lines(float_file, ["normScale="+normScale, "sampleNorm="+str(norm)])
            job_config["floatConfig"] = os.path.basename(float_file)

            int_file = os.path.join(job_dir, job_config.get("intConfig", "ints.txt"))
            _append_unique_lines(int_file, ["type="+typ])
            job_config["intConfig"] = os.path.basename(int_file)

            with open(os.path.join(job_dir, "submit_config.txt"), "w") as f:
                for key in job_config.keys():
                    f.write(str(key)+"="+job_config[key]+"\n")

            jobs_created += 1
            sampleIndex += 1

        return jobs_created

    # run sample processing in parallel using a thread pool
    max_workers = max(1, args.threads)
    futures = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for key, sample in sampleList.items():
            futures.append(executor.submit(process_sample, key, sample))

        # propagate exceptions from worker threads
        for fut in as_completed(futures):
            exc = fut.exception()
            if exc:
                raise exc

    # total jobs created
    index = index_container["value"]

    submit_path = write_submit_files(
        mainDir,
        index,
        exe_relpath,
        args.stage_inputs,
        args.stage_outputs,
        root_setup_block,
        x509loc=x509loc,
        max_runtime=str(args.time),
        request_memory=2000,
        request_cpus=1,
        request_disk=20000,
        extra_transfer_files=extra_transfer_files,
        include_aux=False,
        shared_dir_name=shared_dir_name,
        eos_sched=args.eos_sched,
        config_file="submit_config.txt",
        container_setup=container_setup_block,
    )
    if(index==1):
        print(index, "job created")
    else:
        print(index, "jobs created")
    print("use the following command to submit:")
    print("condor_submit", submit_path)
            

if(__name__=="__main__"):
    main()