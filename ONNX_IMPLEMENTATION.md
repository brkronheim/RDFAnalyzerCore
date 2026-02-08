# ONNX Manager Implementation Summary

## Overview
This implementation adds support for ONNX (Open Neural Network Exchange) model evaluation to RDFAnalyzerCore, similar to the existing BDTManager functionality. ONNX is an open standard for machine learning models that enables interoperability between different ML frameworks.

## What Was Implemented

### 1. ONNX Runtime Integration (CMake)
- **File**: `cmake/SetupOnnxRuntime.cmake`
- Automatically downloads ONNX Runtime binaries from official Microsoft releases during CMake configuration
- Supports Linux (x64, aarch64) and macOS (x64, arm64)
- Version: 1.20.0
- No manual installation or building required
- Sets up proper RPATH for runtime library loading

### 2. OnnxManager Plugin
- **Files**: 
  - `core/plugins/OnnxManager/OnnxManager.h`
  - `core/plugins/OnnxManager/OnnxManager.cc`
  - `core/plugins/OnnxManager/CMakeLists.txt`

**Features**:
- Inherits from `NamedObjectManager<std::shared_ptr<Ort::Session>>`
- Loads ONNX models from configuration files
- Applies models to ROOT RDataFrame with conditional execution
- Returns -1.0 when runVar is false (to skip unnecessary computation)
- Thread-safe for use with ROOT's ImplicitMT
- Follows the same patterns as BDTManager for consistency

**Key Methods**:
- `applyModel(modelName)`: Apply a specific model to the dataframe
- `applyAllModels()`: Apply all configured models
- `getModel(key)`: Retrieve ONNX session object
- `getModelFeatures(key)`: Get input feature names
- `getRunVar(modelName)`: Get conditional run variable name
- `getAllModelNames()`: List all loaded models

### 3. Comprehensive Unit Tests
- **File**: `core/test/testOnnxManager.cc`
- **Test Configuration**: `core/test/cfg/onnx_models.txt`
- **Test Models**: `core/test/cfg/test_model.onnx`, `core/test/cfg/test_model2.onnx`

**Test Coverage**:
- Constructor and manager creation
- Model retrieval (valid and invalid)
- Feature retrieval (valid and invalid)
- RunVar retrieval (valid and invalid)
- Base class interface methods
- Model application with valid inputs
- Model application with runVar=false
- Multiple model support
- Thread safety with ROOT ImplicitMT
- Const correctness

### 4. Documentation
- **File**: `README.md`

**Additions**:
- Overview of OnnxManager in the plugins section
- Detailed usage guide with configuration file format
- C++ API examples and code snippets
- Instructions for creating ONNX models from:
  - scikit-learn (using skl2onnx)
  - PyTorch (using torch.onnx.export)
  - TensorFlow/Keras (using tf2onnx)

## Configuration Format

Create a configuration file (e.g., `cfg/onnx_models.txt`):
```
file=path/to/model.onnx name=model_output inputVariables=var1,var2,var3 runVar=should_run
```

Add to main config:
```
onnxConfig=cfg/onnx_models.txt
```

## Usage Example

```cpp
// Create and configure OnnxManager
auto onnxManager = std::make_unique<OnnxManager>(*configProvider);
ManagerContext ctx{*configProvider, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
onnxManager->setContext(ctx);

// Apply models
onnxManager->applyAllModels();

// Access results in dataframe
auto df = dataManager->getDataFrame();
auto predictions = df.Take<float>("model_output");
```

## Testing Notes

### Build Requirements
- ROOT 6.30.02 or later
- CMake 3.19.0 or later
- C++17 compatible compiler
- Internet connection during build (to download ONNX Runtime)

### Running Tests
The implementation includes comprehensive unit tests that can be run with:
```bash
source env.sh
source build.sh
cd build
ctest -R OnnxManagerTest
```

**Note**: Tests require a ROOT environment with CVMFS or local ROOT installation.

## Design Decisions

1. **Binary Distribution**: ONNX Runtime is downloaded as pre-built binaries rather than built from source to:
   - Avoid long build times
   - Reduce build complexity
   - Ensure consistent behavior across platforms
   - Follow the problem statement requirement

2. **Conditional Execution**: Like BDTManager, models return -1.0 when runVar is false to:
   - Skip expensive inference when not needed
   - Maintain compatibility with existing analysis patterns
   - Enable clear identification of skipped events

3. **Code Structure**: Follows BDTManager patterns to:
   - Maintain consistency across the codebase
   - Leverage existing NamedObjectManager infrastructure
   - Make the API familiar to existing users

4. **Thread Safety**: Configured with single-threaded ONNX inference per event to:
   - Allow ROOT's ImplicitMT to handle parallelism at the event level
   - Avoid thread contention within ONNX Runtime
   - Maintain consistent behavior with other managers

## Security Considerations

- ONNX Runtime binaries are downloaded from official Microsoft GitHub releases
- Version is pinned to 1.20.0 for reproducibility
- CMake verifies download success before proceeding
- No external code execution during model loading (only model inference)

## Future Enhancements (Not Implemented)

Potential improvements for future work:
1. Support for multiple output tensors from models
2. Batch inference for improved performance
3. Model caching for faster repeated evaluations
4. Support for different ONNX Runtime execution providers (CPU, CUDA, TensorRT)
5. Model quantization support
6. Integration with ONNX Model Zoo

## Compatibility

- **Platform Support**: Linux (x64, aarch64), macOS (x64, arm64)
- **ONNX Opset**: Compatible with ONNX opset 12+
- **Model Types**: Linear models, tree ensembles, neural networks, etc.
- **ML Frameworks**: Any framework that can export to ONNX format

## Limitations

1. **Single Output**: Currently assumes models have a single output value
2. **Float Input**: Input features are converted to float32
3. **1D Input**: Models should expect (1, N) shaped input tensors
4. **CPU Only**: ONNX Runtime is configured for CPU inference only

## References

- [ONNX Runtime Documentation](https://onnxruntime.ai/docs/)
- [ONNX Format Specification](https://onnx.ai/)
- [ROOT RDataFrame Guide](https://root.cern/doc/master/classROOT_1_1RDataFrame.html)
