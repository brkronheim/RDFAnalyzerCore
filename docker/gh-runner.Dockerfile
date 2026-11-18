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
    libgif-dev \
    libgsl-dev \
    libssl-dev \
    libx11-dev \
    libxext-dev \
    libxft-dev \
    libxpm-dev \
    libxrootd-client-dev \
    libxxhash-dev \
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
RUN curl -fsSL -o runner.tar.gz https://github.com/actions/runner/releases/download/v2.338.2/actions-runner-linux-x64-2.338.2.tar.gz \
    && echo 'baebf204307bac6193480852732f0aba2ae97b6c5597c244f4d5558390147c40  runner.tar.gz' | sha256sum -c - \
    && tar xzf runner.tar.gz \
    && rm runner.tar.gz

# ROOT binary distribution
RUN curl -fsSL -o root.tar.gz https://root.cern/download/root_v6.40.04.Linux-ubuntu22.04-x86_64-gcc11.4.tar.gz \
    && tar xzf root.tar.gz \
    && rm root.tar.gz

# Persist ROOT environment
ENV ROOTSYS=/home/runner/root
ENV PATH=$ROOTSYS/bin:$PATH
ENV LD_LIBRARY_PATH=$ROOTSYS/lib
# Make PyROOT importable by the system Python used in this image.  ROOT's
# Python extension modules live under $ROOTSYS/lib and must be on PYTHONPATH.
ENV PYTHONPATH=$ROOTSYS/lib

# Verify PyROOT at build time (smoke test) to fail fast if Python/ROOT mismatch
RUN python3 -c "import ROOT; print('PyROOT OK —', ROOT.gROOT.GetVersion())"

# Startup script
COPY start.sh .

# Permissions before dropping root
RUN chown -R runner:runner /home/runner && chmod +x start.sh

USER runner

RUN ./bin/Runner.Listener --version

ENTRYPOINT ["./start.sh"]
