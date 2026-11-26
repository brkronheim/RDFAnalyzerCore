CI runner Dockerfile for self-hosted GitHub Actions runner

This repository includes a Dockerfile for the self-hosted GitHub Actions runner used
in CI/testing. The image purpose is to provide a reproducible environment with
preinstalled dependencies (including Python, numpy and numba) required by the
Python bindings and other tests.

File
- `docker/gh-runner.Dockerfile` — base image used for the self-hosted runner.

Key features
- Ubuntu 22.04 base
- Preinstalled: ROOT (binary distribution), CMake, Boost, TBB, build-essential
- Python 3.x + pip, with `numpy`, `numba`, and `pybind11` installed via pip
- System libraries useful for building Python packages: OpenBLAS, LAPACK, gfortran
- XRootD client headers and libraries for the default adaptor, plus GSL and xxHash for ROOT
- GitHub Actions runner bundle included for convenience

Why numpy/numba are included
- The Python bindings tests import and use `numpy` and `numba` (numba for
  DefineFromPointer integration). Installing them in the runner image avoids
  test-time network installs and ensures consistent test behavior.

## Build and start the runners

Run these commands from the repository root on the Docker host. Enable the
system Docker service so it starts at boot, then check for existing runners.

{% raw %}
```bash
sudo systemctl enable --now docker.service
sudo docker ps -a --format 'table {{.Names}}\t{{.Image}}\t{{.Status}}'
sudo docker build --pull -t gh-runner -f docker/gh-runner.Dockerfile docker
```
{% endraw %}

Set `GITHUB_PAT` to a token with repository Administration write permission
before creating containers. Enter the token locally using `read` to keep it
out of shell history. The launcher defaults to four runners. Pass the previous
runner count explicitly if it differs.

```bash
read -rsp 'GitHub runner PAT: ' GITHUB_PAT
printf '\n'
export GITHUB_PAT
sudo --preserve-env=GITHUB_PAT bash docker/startupContainer.sh 4
unset GITHUB_PAT
```

To deploy the rebuilt image over existing `gh-runner-N` containers, confirm the
runners are idle and pass `--force` before the count in the launcher command.
This removes and recreates the matching containers. A regular launch updates
the restart policy and starts existing matching containers.

Docker's `always` restart policy brings the containers back at boot and after
an ephemeral runner finishes a job. Each container requests a new registration
token on startup, so its PAT must remain valid. Once started, check container
logs and confirm the runners appear online in the repository's Actions settings.

{% raw %}
```bash
systemctl is-enabled docker.service
sudo docker inspect --format '{{.HostConfig.RestartPolicy.Name}}' gh-runner-1
sudo docker logs --tail 30 gh-runner-1
```
{% endraw %}

## Image notes

- The image installs `numpy` and `numba` via pip; using the Ubuntu-provided
  Python ensures the rest of the CI toolchain behaves like the runner host.
- The Dockerfile sets `PYTHONPATH=$ROOTSYS/lib` so the system `python3` can
  import PyROOT from the ROOT binary distribution. A build-time smoke test
  verifies `import ROOT` during image build.
- If you need a different Python version or additional Python packages, update
  the `RUN python3 -m pip install ...` line in the Dockerfile.
- The top-level CMake now enables `POSITION_INDEPENDENT_CODE` so static libs
  can be safely linked into the Python extension module in CI.
