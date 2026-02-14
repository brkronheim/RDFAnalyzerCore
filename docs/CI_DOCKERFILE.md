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
- GitHub Actions runner bundle included for convenience

Why numpy/numba are included
- The Python bindings tests import and use `numpy` and `numba` (numba for
  DefineFromPointer integration). Installing them in the runner image avoids
  test-time network installs and ensures consistent test behavior.

Build & run

1. Build the image locally

   docker build -t rdfanalyzer/gh-runner -f docker/gh-runner.Dockerfile .

2. Run the container (example)

   docker run -d --name gh-runner \
     -e GITHUB_PAT=<redacted> \
     -e GITHUB_OWNER=<owner> \
     -e GITHUB_REPO=<repo> \
     --tmpfs /tmp \
     --tmpfs /home/runner/_work:rw,uid=1000,gid=1000,exec \
     rdfanalyzer/gh-runner

Notes
- The image installs `numpy` and `numba` via pip; using the Ubuntu-provided
  Python ensures the rest of the CI toolchain behaves like the runner host.
- The Dockerfile sets `PYTHONPATH=$ROOTSYS/lib` so the system `python3` can
  import PyROOT from the ROOT binary distribution. A build-time smoke test
  verifies `import ROOT` during image build.
- If you need a different Python version or additional Python packages, update
  the `RUN python3 -m pip install ...` line in the Dockerfile.
- The top-level CMake now enables `POSITION_INDEPENDENT_CODE` so static libs
  can be safely linked into the Python extension module in CI.
