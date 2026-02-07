import subprocess
import argparse
import glob
import shutil

import os

from pathlib import Path


from rucio.client import Client


import socket
import os
import subprocess

import numpy as np

from submission_backend import (
    read_config,
    get_copy_file_list,
    ensure_symlink,
    write_submit_files,
)
from validate_config import validate_submit_config

def check_port(port):
    import socket

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("0.0.0.0", port))
        available = True
    except Exception:
        available = False
    sock.close()
    return available


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
        shutil.copytree(src, dst, dirs_exist_ok=True)




from rucio.client import Client

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

# Recursively find all lowest level folders with root files
def findSubFolders(mainDirectory):
    subDirs = glob.glob(mainDirectory+"/*")
    segments = set()
    for subDir in subDirs:
        if(".root" in subDir and "fail" not in subDir.lower()):
            segments.add(mainDirectory+"/")
        segments.update(findSubFolders(subDir))
    return(segments)



def queryRucio(directory, fileSplit, WL, BL, siteOverride, client):
    runningSize = 0

    groups = dict()
    filesFound = 0
    group = 0
    groupCount = dict()
    groupSizes = dict()
    print("checking Rucio for", directory)
    rucioOutput = client.list_replicas([{"scope": "cms", "name": directory}])
    print("Output received")
    rucioOutput = list(rucioOutput)
    print(len(rucioOutput), "files found")
    for filedata in rucioOutput:
         file = filedata["name"]
         states = filedata["states"]
         size = filedata["bytes"]*1e-9
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
        '--spool',
        action='store_true',
        help='prepare for condor_submit -spool (copy exe/aux instead of symlinks)',
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
    x509loc = args.x509
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
                test_config["fileList"] = first_file
                test_config["batch"] = "False"
                test_config.setdefault("threads", "1")
                test_config["type"] = typ
                test_config["saveFile"] = os.path.join(test_dir, "test_output.root")
                test_config["metaFile"] = os.path.join(test_dir, "test_output_meta.root")

                normScale = str(extraScale*kfac*lumi*xsec/norm)

                if('floatConfig' in test_config.keys()):
                    with open(os.path.join(test_dir, test_config['floatConfig']), "a") as floatFile:
                        floatFile.write("\nnormScale="+normScale+"\n")
                        floatFile.write("\nsampleNorm="+str(norm)+"\n")
                else:
                    with open(os.path.join(test_dir, "cfg/floats.txt"), "w") as floatFile:
                        floatFile.write("\nnormScale="+normScale+"\n")
                        floatFile.write("\nsampleNorm="+str(norm)+"\n")
                        test_config['floatConfig'] = 'cfg/floats.txt'

                if('intConfig' in test_config.keys()):
                    with open(os.path.join(test_dir, test_config['intConfig']), "a") as intFile:
                        intFile.write("\ntype="+typ+"\n")
                else:
                    with open(os.path.join(test_dir, "cfg/ints.txt"), "w") as intFile:
                        intFile.write("\ntype="+typ+"\n")
                        test_config['intConfig'] = 'cfg/ints.txt'

                with open(os.path.join(test_dir, "cfg/submit_config.txt"),"w") as file:
                    for cfg_key in test_config.keys():
                        file.write(str(cfg_key)+"="+test_config[cfg_key]+"\n")

                print("Test job created. Run locally with:")
                print(f"cd {test_dir} && ./{exe_relpath} cfg/submit_config.txt")
                test_job_created = True
        for subDir in fileList:
            
            # make the submission directory and its cfg sub folder
            Path(mainDir+"job_"+str(index)).mkdir(parents=True, exist_ok=True)
            Path(mainDir+"job_"+str(index)+"/cfg").mkdir(parents=True, exist_ok=True)
            
            # copy config files into condor submission folder
            for file in copyList:
                src = resolve_path(file)
                dst = os.path.join(mainDir+"job_"+str(index), file)
                Path(os.path.dirname(dst)).mkdir(parents=True, exist_ok=True)
                shutil.copyfile(src, dst)
            #print("linking", os.getcwd()+"/aux", mainDir+"job_"+str(index)+"/aux")
            # aux and executable are provided via shared inputs

            # determine name of output file
            outputFileName = saveDirectory+"/"+name+"_"+str(sampleIndex)+".root"
            configDict["saveFile"] = outputFileName
            configDict["metaFile"] = saveDirectory + "/" + name + "_" + str(sampleIndex) + "_meta.root"
            configDict["fileList"] = fileList[subDir]
            #print(subDir, fileList[subDir], configDict["fileList"])
            configDict["batch"] = "True"
            configDict["type"]=typ
            # determine normalization scale
            normScale = str(extraScale*kfac*lumi*xsec/norm)
            
            # augment float and int config files
            if('floatConfig' in configDict.keys()):
                with open(mainDir+"job_"+str(index)+"/"+configDict['floatConfig'], "a") as floatFile:
                    floatFile.write("\nnormScale="+normScale+"\n")
                    floatFile.write("\nsampleNorm="+str(norm)+"\n")
            else:
                with open(mainDir+"job_"+str(index)+"/cfg/floats.txt", "w") as floatFile:
                    floatFile.write("\nnormScale="+normScale+"\n")
                    floatFile.write("\nsampleNorm="+str(norm)+"\n")
                    configDict['floatConfig'] = 'cfg/floats.txt'

            if('intConfig' in configDict.keys()):
                with open(mainDir+"job_"+str(index)+"/"+configDict['intConfig'], "a") as intFile:
                    intFile.write("\ntype="+typ+"\n")
            else:
                with open(mainDir+"job_"+str(index)+"/cfg/ints.txt", "w") as intFile:
                    intFile.write("\ntype="+typ+"\n")
                    configDict['intConfig'] = 'cfg/ints.txt'
            # make the main config file and update or add the savefile
            with open(mainDir+"job_"+str(index)+"/cfg/submit_config.txt","w") as file:
                for key in configDict.keys():
                    file.write(str(key)+"="+configDict[key]+"\n")
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
        max_runtime="3600*4",
        request_memory=2000,
        request_cpus=1,
        request_disk=20000,
        use_shared_inputs=True,
    )
    if(index==1):
        print(index, "job created")
    else:
        print(index, "jobs created")
    print("use the following command to submit:")
    if args.spool:
        print("condor_submit -spool", submit_path)
    else:
        print("condor_submit", submit_path)
    index+=1
            

if(__name__=="__main__"):
    main()