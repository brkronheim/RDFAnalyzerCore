FROM ubuntu:22.04

# Install build tools, ROOT dependencies and Python (pip)
RUN apt-get update && apt-get install -y \
    curl \
    git \
    ca-certificates \
    build-essential \
    sudo \
    jq \
    libicu70 \
    cmake \
    zlib1g-dev \
    wget \
    libtbb12 \
    libtbb-dev \
    libboost-all-dev \
    # Python + build deps for numpy/numba
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    python3-distutils \
    libopenblas-dev \
    liblapack-dev \
    gfortran \
    && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python packages required by tests
RUN python3 -m pip install --upgrade pip setuptools wheel \
    && python3 -m pip install --no-cache-dir numpy numba pybind11

# Create runner user
RUN useradd -m runner && echo "runner ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers

WORKDIR /home/runner

# GitHub Actions runner
RUN curl -L https://github.com/actions/runner/releases/download/v2.331.0/actions-runner-linux-x64-2.331.0.tar.gz \
    | tar xz

# ROOT binary distribution
RUN curl -L https://root.cern/download/root_v6.36.08.Linux-ubuntu22.04-x86_64-gcc11.4.tar.gz \
    | tar xz

# Persist ROOT environment
ENV ROOTSYS=/home/runner/root
ENV PATH=$ROOTSYS/bin:$PATH
ENV LD_LIBRARY_PATH=$ROOTSYS/lib:$LD_LIBRARY_PATH
# Make PyROOT importable by the system Python used in this image.  ROOT's
# Python extension modules live under $ROOTSYS/lib and must be on PYTHONPATH.
ENV PYTHONPATH=$ROOTSYS/lib:$PYTHONPATH

# Verify PyROOT at build time (smoke test) to fail fast if Python/ROOT mismatch
RUN python3 -c "import ROOT; print('PyROOT OK —', ROOT.gROOT.GetVersion())"

# Startup script
COPY start.sh .

# Permissions before dropping root
RUN chown -R runner:runner /home/runner && chmod +x start.sh

USER runner

ENTRYPOINT ["./start.sh"]
