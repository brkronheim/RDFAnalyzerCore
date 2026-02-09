# Getting Started with RDFAnalyzerCore

This guide will help you get up and running with RDFAnalyzerCore quickly.

## Prerequisites

- ROOT 6.30/02 or later (progress bar support was added around 6.28)
- CMake 3.19.0 or later
- C++17 compatible compiler
- Git

## Quick Start

### 1. Clone the Repository

```bash
git clone git@github.com:brkronheim/RDFAnalyzerCore.git
cd RDFAnalyzerCore
```

### 2. Set Up Environment

On lxplus (CERN computing):
```bash
source env.sh
```

This script sets up ROOT and other required dependencies from CVMFS.

For local installations, ensure ROOT is available in your PATH and environment.

### 3. Build the Framework

```bash
source build.sh
```

This will:
- Configure CMake with the appropriate presets
- Download ONNX Runtime automatically
- Build the core framework
- Discover and build any analyses in the `analyses/` directory
- Run tests to verify the installation

The build artifacts will be placed in the `build/` directory.

### 4. Run the Example Analysis

```bash
cd analyses/ExampleAnalysis
./run_analysis cfg.txt
```

This runs a simple Z→μμ analysis on ATLAS Open Data.

## Understanding the Output

The framework produces two types of output:

1. **Skim Output** (`saveFile`): Event-level ROOT file with selected branches
2. **Metadata/Histogram Output** (`metaFile`): Histograms, counters, and analysis metadata

Output locations are specified in your configuration file.

## Next Steps

- **Learn about configurations**: See [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)
- **Build your own analysis**: See [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)
- **Understand the architecture**: See [ARCHITECTURE.md](ARCHITECTURE.md)
- **Add new features**: See [PLUGIN_DEVELOPMENT.md](PLUGIN_DEVELOPMENT.md)

## Common Issues

### Build Fails with ROOT Not Found

Ensure ROOT is properly sourced:
```bash
source env.sh  # On lxplus
# OR
source /path/to/root/bin/thisroot.sh  # Local installation
```

### ONNX Runtime Download Fails

The build automatically downloads ONNX Runtime. If this fails:
- Check your internet connection
- Verify you can access GitHub releases
- Manual download may be needed in restricted environments

### Analysis Not Found During Build

Analyses must:
- Be placed in the `analyses/` directory
- Contain a valid `CMakeLists.txt` file
- Be committed to Git (or tracked via .gitmodules for submodules)

Run `source cleanBuild.sh` to reconfigure and rebuild everything.

## File Organization

After building, your directory structure will look like:

```
RDFAnalyzerCore/
├── core/                 # Framework source code
│   ├── interface/       # Public headers
│   ├── src/            # Implementation files
│   ├── plugins/        # Plugin implementations
│   └── test/           # Unit tests
├── analyses/            # Analysis-specific code
│   └── ExampleAnalysis/
├── build/              # Build artifacts (CMake, binaries)
├── docs/               # Documentation
├── cmake/              # CMake modules
└── README.md          # Main documentation
```

## Development Workflow

1. **Create or clone an analysis repository**
   ```bash
   cd analyses
   git clone <your-analysis-repo>
   ```

2. **Build the analysis**
   ```bash
   cd ../
   source build.sh
   ```

3. **Run your analysis**
   ```bash
   cd build/analyses/YourAnalysis
   ./your_executable config.txt
   ```

4. **Iterate**: Make changes to your analysis code and rebuild incrementally

## Testing

Run the test suite to verify your installation:

```bash
source test.sh
```

Or run specific tests:

```bash
cd build
ctest -R TestName -V  # Verbose output for specific test
```

## Getting Help

- **Documentation**: Check the `docs/` directory for detailed guides
- **Examples**: Look at `analyses/ExampleAnalysis/` for a working example
- **Issues**: Open an issue on GitHub if you encounter problems
- **README**: The main [README.md](../README.md) has comprehensive technical documentation

## What's Next?

Now that you have the framework running, you're ready to:

1. **Understand the config system**: Learn how to configure analyses in [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)
2. **Build an analysis**: Follow the step-by-step guide in [ANALYSIS_GUIDE.md](ANALYSIS_GUIDE.md)
3. **Use machine learning**: Integrate BDTs or neural networks with OnnxManager
4. **Apply corrections**: Use CorrectionManager for scale factors
5. **Handle systematics**: Track and propagate systematic uncertainties

Happy analyzing!
