# LAW Workflow for NANO Submission

This folder contains a LAW-based workflow that mirrors `core/python/generateSubmissionFilesNANO.py`.

Main implementation: [nano_tasks.py](nano_tasks.py)

## What it does

The workflow is split into 4 tasks:

1. `PrepareNANOSample`
   - one branch per sample
   - queries Rucio and creates per-job directories
2. `BuildNANOSubmission`
   - builds `condor_submit.sub`, `condor_runscript.sh`, `condor_runscript_inner.sh`
   - stages shared inputs (`exe`, `x509`, optional `aux`)
   - stages local `.so` dependencies from this repository into `shared_inputs`
3. `SubmitNANOJobs`
   - runs `condor_submit`
4. `MonitorNANOJobs`
   - monitors jobs and can resubmit failed/held jobs

## Important behavior

- `root_setup` is treated as a **file path**; file contents are embedded into the inner runscript.
- `container_setup` is passed to the wrapper runscript and is used to launch the inner runscript (e.g. `cmssw-el9`).
- Condor submit files are **not OS-pinned** by default (`MY.WantOS` is omitted).
- Local shared libraries required by the executable (resolved by `ldd` and located under this repo) are staged and transferred.

## Prerequisites

From repo root:

```bash
source env.sh
pip install --user law
```

From `law/` directory, use:

```bash
export PYTHONPATH=$PWD
export LAW_CONFIG_FILE=law.cfg
law index --modules nano_tasks
```

## Common parameters

- `--submit-config`: submit config file (e.g. `../analyses/rdfAnalyzerStitch/cfg/test_cfg_22.txt`)
- `--name`: submission name (`condorSub_<name>`)
- `--x509`: proxy file path (e.g. `../x509`)
- `--exe`: executable path (e.g. `../build/analyses/rdfAnalyzerStitch/vjetStitch`)
- `--root-setup`: setup script file (e.g. `../env.sh`)
- `--container-setup`: container command (e.g. `cmssw-el9`)

## Run up to submission file generation (no condor submit)

```bash
cd law
export PYTHONPATH=$PWD
export LAW_CONFIG_FILE=law.cfg
law index --modules nano_tasks

law run PrepareNANOSample --module nano_tasks --workers 1 \
  --submit-config ../analyses/rdfAnalyzerStitch/cfg/test_cfg_22.txt \
  --name testLaw --x509 ../x509 \
  --exe ../build/analyses/rdfAnalyzerStitch/vjetStitch \
  --root-setup ../env.sh --container-setup '/cvmfs/cms.cern.ch/common/cmssw-el9' \
  --stage-in --size 10

law run BuildNANOSubmission --module nano_tasks --workers 1 \
  --submit-config ../analyses/rdfAnalyzerStitch/cfg/test_cfg_22.txt \
  --name testLaw --x509 ../x509 \
  --exe ../build/analyses/rdfAnalyzerStitch/vjetStitch \
  --root-setup ../env.sh --container-setup '/cvmfs/cms.cern.ch/common/cmssw-el9'\
  --stage-in --size 10
```

This creates:

- `../condorSub_testLaw/condor_submit.sub`
- `../condorSub_testLaw/condor_runscript.sh`
- `../condorSub_testLaw/condor_runscript_inner.sh`

## Submit and monitor

```bash
law run SubmitNANOJobs --module nano_tasks --local-scheduler --workers 1 \
  --submit-config ../analyses/rdfAnalyzerStitch/cfg/test_cfg_22.txt \
  --name testLaw --x509 ../x509 \
  --exe ../build/analyses/rdfAnalyzerStitch/vjetStitch \
  --root-setup ../env.sh --container-setup '/cvmfs/cms.cern.ch/common/cmssw-el9'\
  --stage-in --size 10

law run MonitorNANOJobs --module nano_tasks --local-scheduler --workers 1 \
  --submit-config ../analyses/rdfAnalyzerStitch/cfg/test_cfg_22.txt \
  --name testLaw --x509 ../x509 \
  --exe ../build/analyses/rdfAnalyzerStitch/vjetStitch \
  --root-setup ../env.sh --container-setup '/cvmfs/cms.cern.ch/common/cmssw-el9'\
  --stage-in --size 10
```

## Notes

- If your `saveDirectory` points to EOS, ensure EOS path permissions are valid.
- For local dry tests, use a copy of submit config with a writable local `saveDirectory`.
- If task discovery fails, rerun:

```bash
export PYTHONPATH=$PWD
export LAW_CONFIG_FILE=law.cfg
law index --modules nano_tasks
```
