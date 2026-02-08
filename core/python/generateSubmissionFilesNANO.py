import argparse
import os
import shutil
import subprocess
from pathlib import Path
import time

import numpy as np
import requests
from requests.exceptions import ChunkedEncodingError, RequestException
import urllib3
from rucio.client import Client

from submission_backend import read_config, get_copy_file_list, write_submit_files
from validate_config import validate_submit_config


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

    print("Output received")
    print(len(rucioOutput), "files found")
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
                 elif(site in BL or "T3" in site or "Tape" in site):
                     continue
                     #print("Bad", site)
                 else:
                     #
                     #print("Neutral", site)
                     redirector =  "root://xrootd-cms.infn.it/" + "/store/test/xrootd/"+site+"/"

         if(group in groups):
             groups[group] +=","+redirector+file
             groupCount[group]+=1
             groupSizes[group]+=np.round(size,1)
         else:
             groups[group] = redirector+file
             groupCount[group]=1
             groupSizes[group]=np.round(size,1)
    print("groupCounts:", groupCount)
    print("groupSizes:", groupSizes)
    print(len(groups), "groups found and", filesFound, "files found")
    return(groups)

def getSampleList(configFile):
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
    parser.add_argument('-s', '--size', type=int, required=False, default=30, help="GB of data to process per job, default is 30")
    parser.add_argument('-x', '--x509', type=str, required=True, help="location of x509 proxy to use (such as /afs/cern.ch/user/u/username/private/x509)")
    parser.add_argument('-e', '--exe', type=str, required=True, help="path to the C++ executable to run")
    parser.add_argument('--stage-inputs', action='store_true', help="xrdcp input files to the worker node before running")
    parser.add_argument('--stage-outputs', action='store_true', help="xrdcp outputs to final destination after running")
    parser.add_argument('--root-setup', type=str, default="", help="command to setup ROOT (e.g., 'source /path/to/thisroot.sh')")
    parser.add_argument('--no-validate', action='store_true', help="skip config validation")
    parser.add_argument('--make-test-job', action='store_true', help="create a local test job using one file")
    parser.add_argument(
        '--eos-sched',
        action='store_true',
        help='use EOS scheduling',
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

    configDict = read_config(args.config)
    print(configDict.keys())
    fileSplit = args.size
    # x509loc will be set later
    exe_path = resolve_path(args.exe)
    exe_relpath = os.path.basename(exe_path)
    if not os.path.exists(exe_path):
        raise SystemExit(f"Executable not found: {exe_path}. Provide a valid path with -e/--exe")
    config = resolve_path(configDict['sampleConfig'])
    saveDirectory = resolve_path(configDict['saveDirectory'])
    Path(saveDirectory).mkdir(parents=True, exist_ok=True)
    copyList = get_copy_file_list(configDict)



    sampleList, baseDirectoryList, lumi, WL, BL = getSampleList(config)
    print("WL: ", WL)
    print("BL: ", BL)

    client = get_rucio_client()
    index = 0
    mainDir = f"condorSub_{args.name}/"
    test_job_created = False

    if args.eos_sched:
        mainDir = os.path.join("/eos/user/b/bkronhei/RDFAnalyzerCore", mainDir)

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

    copy_basenames = sorted({os.path.basename(path) for path in copyList})
    skip_transfer = {"floats.txt", "ints.txt", "submit_config.txt"}
    extra_transfer_files = [
        os.path.join(mainDir, "job_$(Process)", name)
        for name in copy_basenames
        if name not in skip_transfer
    ]
    for key in sampleList:
        print("Checking ", key)
        # get the general information about this sample
        sample = sampleList[key]
        name = sample['name']
        das = sample['das']
        xsec = float(sample['xsec'])
        typ = sample['type']
        if('norm' not in sample):
            norm = 1.0
        else:
            norm = float(sample['norm'])
        kfac  = float(sample['kfac']) if 'kfac' in sample else 1.0
        site  = sample['site'] if 'site' in sample else ''

        extraScale  = float(sample['extraScale']) if 'extraScale' in sample else 1.0
        
        # Find all the sub directories with root files
        #subDirs = []
        #for baseDir in baseDirectoryList:
        #    subDirs += findSubFolders(baseDir+"/"+directory)
        # generate submission files for each sub directory
        sampleIndex = 0

        fileList = queryRucio(das, fileSplit, WL, BL, site, client)
        if args.make_test_job and not test_job_created and fileList:
            first_group = next(iter(fileList.values()))
            first_file = first_group.split(",")[0] if first_group else ""
            if first_file:
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
                test_config["fileList"] = first_file
                test_config["batch"] = "False"
                test_config.setdefault("threads", "1")
                test_config["type"] = typ
                test_config["saveFile"] = "test_output.root"
                test_config["metaFile"] = "test_output_meta.root"
                test_config["sampleConfig"] = os.path.basename(config)
                test_config["floatConfig"] = "floats.txt"
                test_config["intConfig"] = "ints.txt"

                normScale = str(extraScale*kfac*lumi*xsec/norm)

                # Write float keys uniquely (avoid duplicates if file already contains them)
                float_file = os.path.join(test_dir, test_config.get("floatConfig", "floats.txt"))
                _append_unique_lines(float_file, ["normScale="+normScale, "sampleNorm="+str(norm)])
                test_config["floatConfig"] = os.path.basename(float_file)

                int_file = os.path.join(test_dir, test_config.get("intConfig", "ints.txt"))
                _append_unique_lines(int_file, ["type="+typ])
                test_config["intConfig"] = os.path.basename(int_file)

                with open(os.path.join(test_dir, "submit_config.txt"),"w") as file:
                    for cfg_key in test_config.keys():
                        file.write(str(cfg_key)+"="+test_config[cfg_key]+"\n")

                print("Test job created. Run locally with:")
                print(f"cd {test_dir} && ./{exe_relpath} submit_config.txt")
                test_job_created = True
        for subDir in fileList:
            job_dir = os.path.join(mainDir, f"job_{index}")
            Path(job_dir).mkdir(parents=True, exist_ok=True)

            # copy config files into per-job folder
            for file in copyList:
                src = resolve_path(file)
                dst = os.path.join(job_dir, os.path.basename(file))
                Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)

            # aux/exe/x509 are provided via shared inputs
            #print("linking", os.getcwd()+"/aux", mainDir+"job_"+str(index)+"/aux")
            # aux and executable are provided via shared inputs

            # determine name of output file
            outputFileName = saveDirectory+"/"+name+"_"+str(sampleIndex)+".root"
            job_config = normalize_config_paths(dict(configDict))
            job_config["saveFile"] = outputFileName
            job_config["metaFile"] = saveDirectory + "/" + name + "_" + str(sampleIndex) + "_meta.root"
            job_config["fileList"] = fileList[subDir]
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
            # determine normalization scale
            normScale = str(extraScale*kfac*lumi*xsec/norm)
            
            # augment float and int config files
            # Write float keys uniquely (avoid duplicates if file already contains them)
            float_file = os.path.join(job_dir, job_config.get("floatConfig", "floats.txt"))
            _append_unique_lines(float_file, ["normScale="+normScale, "sampleNorm="+str(norm)])
            job_config["floatConfig"] = os.path.basename(float_file)

            int_file = os.path.join(job_dir, job_config.get("intConfig", "ints.txt"))
            _append_unique_lines(int_file, ["type="+typ])
            job_config["intConfig"] = os.path.basename(int_file)
            # make the main config file and update or add the savefile
            with open(os.path.join(job_dir, "submit_config.txt"),"w") as file:
                for key in job_config.keys():
                    file.write(str(key)+"="+job_config[key]+"\n")
            index+=1
            sampleIndex+=1

    submit_path = write_submit_files(
        mainDir,
        index,
        exe_relpath,
        args.stage_inputs,
        args.stage_outputs,
        args.root_setup,
        x509loc=x509loc,
        want_os="el9",
        max_runtime="1200",
        request_memory=2000,
        request_cpus=1,
        request_disk=20000,
        extra_transfer_files=extra_transfer_files,
        include_aux=aux_exists,
        shared_dir_name=shared_dir_name,
        eos_sched=args.eos_sched,
    )
    if(index==1):
        print(index, "job created")
    else:
        print(index, "jobs created")
    print("use the following command to submit:")
    print("condor_submit", submit_path)
    index+=1
            

if(__name__=="__main__"):
    main()