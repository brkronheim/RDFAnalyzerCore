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
- **Supports multiple outputs** (e.g., ParticleTransformer with bootstrapped models)
- **Deferred execution**: Models loaded but NOT applied until explicitly invoked

**Key Methods**:
- `applyModel(modelName, outputSuffix="")`: Apply a specific model to the dataframe
- `applyAllModels(outputSuffix="")`: Apply all configured models
- `getModel(key)`: Retrieve ONNX session object
- `getModelFeatures(key)`: Get input feature names
- `getRunVar(modelName)`: Get conditional run variable name
- `getAllModelNames()`: List all loaded models
- `getModelInputNames(modelName)`: Get ONNX input tensor names
- `getModelOutputNames(modelName)`: Get ONNX output tensor names

### 3. Comprehensive Unit Tests
- **File**: `core/test/testOnnxManager.cc`
- **Test Configuration**: `core/test/cfg/onnx_models.txt`
- **Test Models**: 
  - `core/test/cfg/test_model.onnx` - Single output model
  - `core/test/cfg/test_model2.onnx` - Single output model
  - `core/test/cfg/test_model_multi_output.onnx` - Multi-output model

**Test Coverage**:
- Constructor and manager creation
- Model retrieval (valid and invalid)
- Feature retrieval (valid and invalid)
- RunVar retrieval (valid and invalid)
- Base class interface methods
- Model application with valid inputs
- Model application with runVar=false
- Multiple model support
- Multi-output model support
- Thread safety with ROOT ImplicitMT
- Const correctness
- Input/output name retrieval

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

### Single Output Model
```cpp
// Create and configure OnnxManager
auto onnxManager = std::make_unique<OnnxManager>(*configProvider);
ManagerContext ctx{*configProvider, *dataManager, *systematicManager, *logger, *skimSink, *metaSink};
onnxManager->setContext(ctx);

// Define input features first
dataManager->Define("pt", ...);
dataManager->Define("eta", ...);

// Then apply models
onnxManager->applyAllModels();

// Access results in dataframe
auto df = dataManager->getDataFrame();
auto predictions = df.Take<float>("model_output");
```

### Multi-Output Model (ParticleTransformer Style)
```cpp
// Define inputs
dataManager->Define("jet_pt", ...);
dataManager->Define("jet_eta", ...);
dataManager->Define("jet_phi", ...);

// Apply multi-output model
onnxManager->applyModel("particle_transformer");

// Access individual outputs
auto df = dataManager->getDataFrame();
auto output0 = df.Take<float>("particle_transformer_output0");  // First output
auto output1 = df.Take<float>("particle_transformer_output1");  // Second output
auto output2 = df.Take<float>("particle_transformer_output2");  // Third output
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

5. **Deferred Execution**: Models are loaded during construction but NOT applied automatically to:
   - Allow users to define all required input features first
   - Prevent errors from missing DataFrame columns
   - Give users full control over when inference happens
   - Support complex analysis workflows with dependencies

6. **Multiple Outputs**: Full support for models with multiple output tensors to:
   - Enable ParticleTransformer-style models with bootstrapped outputs
   - Support ensemble models that return multiple predictions
   - Create individual DataFrame columns for each output for easy access

## Security Considerations

- ONNX Runtime binaries are downloaded from official Microsoft GitHub releases
- Version is pinned to 1.20.0 for reproducibility
- CMake verifies download success before proceeding
- No external code execution during model loading (only model inference)

## Future Enhancements

Potential improvements for future work:
1. Support for multiple output tensors from models - **Implemented** ✅
2. Support for variable-shaped inputs (dynamic batch sizes, ragged arrays)
3. Batch inference for improved performance
4. Model caching for faster repeated evaluations
5. Support for different ONNX Runtime execution providers (CPU, CUDA, TensorRT)
6. Model quantization support
7. Integration with ONNX Model Zoo

## Compatibility

- **Platform Support**: Linux (x64, aarch64), macOS (x64, arm64)
- **ONNX Opset**: Compatible with ONNX opset 12+
- **Model Types**: Linear models, tree ensembles, neural networks, transformers, etc.
- **ML Frameworks**: Any framework that can export to ONNX format
- **Multiple Outputs**: Fully supported (e.g., ParticleTransformer with bootstrapped models)

## Limitations

1. **Float Input**: Input features are converted to float32
2. **Fixed Shape Input**: Currently expects (1, N) shaped input tensors for single input models
3. **CPU Only**: ONNX Runtime is configured for CPU inference only
4. **Single Scalar Output Per Tensor**: Each output tensor must produce a single scalar value

## References

- [ONNX Runtime Documentation](https://onnxruntime.ai/docs/)
- [ONNX Format Specification](https://onnx.ai/)
- [ROOT RDataFrame Guide](https://root.cern/doc/master/classROOT_1_1RDataFrame.html)
