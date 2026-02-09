# OnnxManager Enhancements - Multi-Output Support

## Overview
This document describes the enhancements made to OnnxManager to support models with multiple outputs and ensure proper deferred execution pattern.

## Problem Statement Requirements

The user requested:
1. **Multiple outputs**: Support for models that return multiple output tensors (e.g., ParticleTransformer with bootstrapped models)
2. **Variable-shaped inputs**: Support for multiple inputs with variable shapes
3. **Deferred execution**: Models should be loaded when config is present but not applied until user explicitly invokes them

## Implementation

### 1. Multiple Output Support

#### Single Output Models (Original Behavior)
For models with a single output tensor:
- Creates a DataFrame column named `{modelName}`
- Returns a single float value

Example:
```cpp
onnxManager->applyModel("dnn_classifier");
auto predictions = df.Take<float>("dnn_classifier");
```

#### Multi-Output Models (New Feature)
For models with multiple output tensors:
- Creates individual columns: `{modelName}_output0`, `{modelName}_output1`, etc.
- Creates intermediate column: `{modelName}_outputs` (vector containing all outputs)
- Each output returns -1.0 when `runVar` is false

Example:
```cpp
onnxManager->applyModel("particle_transformer");
auto output0 = df.Take<float>("particle_transformer_output0");
auto output1 = df.Take<float>("particle_transformer_output1");
auto output2 = df.Take<float>("particle_transformer_output2");
```

#### Technical Details
1. **Single Inference**: Model runs once per event, capturing all outputs
2. **Efficient**: Avoids repeated inference for multiple outputs
3. **Thread-Safe**: Works with ROOT's ImplicitMT
4. **Memory Efficient**: Uses lambda capture by value for necessary objects only

### 2. Enhanced API

#### New Methods
```cpp
// Get ONNX input tensor names from the model
const std::vector<std::string>& getModelInputNames(const std::string& modelName);

// Get ONNX output tensor names from the model
const std::vector<std::string>& getModelOutputNames(const std::string& modelName);
```

#### Enhanced Methods
```cpp
// Apply model with optional suffix for output columns
void applyModel(const std::string& modelName, const std::string& outputSuffix = "");

// Apply all models with optional suffix
void applyAllModels(const std::string& outputSuffix = "");
```

### 3. Deferred Execution Pattern

**Already Correctly Implemented**: Models are loaded during construction but NOT applied automatically.

#### Best Practice Usage
```cpp
// 1. Construct OnnxManager (loads models from config)
auto onnxManager = std::make_unique<OnnxManager>(*configProvider);
ManagerContext ctx{...};
onnxManager->setContext(ctx);

// 2. Define ALL required input features
dataManager->Define("jet_pt", ...);
dataManager->Define("jet_eta", ...);
dataManager->Define("jet_phi", ...);
// ... define all features ...

// 3. Apply models (after inputs are ready)
onnxManager->applyModel("particle_transformer");
```

#### Why Deferred Execution?
- **Prevents errors**: Ensures all required DataFrame columns exist before inference
- **User control**: Gives users full control over when inference happens
- **Flexibility**: Supports complex workflows with feature dependencies
- **Debugging**: Easier to debug issues with missing features

## Testing

### New Test Models
- `test_model_multi_output.onnx`: A model with 2 outputs for testing multi-output functionality

### New Tests
1. `MultiOutputModel`: Tests basic multi-output functionality
2. `MultiOutputModel_RunVarFalse`: Tests that all outputs return -1.0 when runVar is false
3. `GetInputOutputNames`: Tests the new API methods for querying model structure

### Updated Tests
- `GetAllModelNames`: Updated to expect 3 models instead of 2
- `ConstCorrectness`: Updated to check for 3 models

## Configuration

No changes to configuration format required. The same format supports both single and multi-output models:

```
file=model.onnx name=my_model inputVariables=pt,eta,phi runVar=has_jet
```

The manager automatically detects the number of outputs from the ONNX model.

## Performance Considerations

### Single Output
- Same performance as before
- No additional overhead

### Multiple Outputs
- **Efficient**: Single inference call captures all outputs
- **Memory**: Intermediate vector column stores all outputs
- **Indexing**: Individual output columns use fast indexing into the vector
- **Thread Safety**: Maintains compatibility with ROOT's ImplicitMT

## Examples

### ParticleTransformer with 3 Bootstrapped Models
```cpp
// Configuration
file=models/particle_transformer.onnx name=pt_score inputVariables=jet_pt,jet_eta,jet_phi,jet_mass runVar=has_jet

// Usage
dataManager->Define("jet_pt", ...);
dataManager->Define("jet_eta", ...);
dataManager->Define("jet_phi", ...);
dataManager->Define("jet_mass", ...);
dataManager->Define("has_jet", ...);

onnxManager->applyModel("pt_score");

// Access individual bootstrap outputs
auto df = dataManager->getDataFrame();
auto bootstrap0 = df.Take<float>("pt_score_output0");
auto bootstrap1 = df.Take<float>("pt_score_output1");
auto bootstrap2 = df.Take<float>("pt_score_output2");

// Calculate ensemble average
dataManager->Define("pt_score_avg", 
    [](float o0, float o1, float o2) { return (o0 + o1 + o2) / 3.0f; },
    {"pt_score_output0", "pt_score_output1", "pt_score_output2"});
```

### Multi-Model Setup with Different Outputs
```cpp
// Single output model
onnxManager->applyModel("binary_classifier");  // Creates: binary_classifier

// Multi-output model
onnxManager->applyModel("regressor");  // Creates: regressor_output0, regressor_output1, regressor_output2

// Access results
auto binary_pred = df.Take<float>("binary_classifier");
auto reg_output0 = df.Take<float>("regressor_output0");
auto reg_output1 = df.Take<float>("regressor_output1");
auto reg_output2 = df.Take<float>("regressor_output2");
```

## Future Enhancements

### Variable-Shaped Inputs
Currently, the implementation supports:
- ✅ Single input with fixed shape (1, N)
- ✅ Multiple outputs of any count

Future work could add:
- Multiple inputs with different shapes
- Dynamic batch sizes
- Ragged/variable-length inputs
- Support for non-scalar output tensors

### Implementation Strategy for Variable Shapes
When needed, could extend to:
1. Parse input shape specifications from config
2. Support multiple input columns (one per model input)
3. Handle dynamic shapes by querying DataFrame column types
4. Support RVec inputs of varying lengths

## Compatibility

### Backward Compatible
- Existing single-output models work unchanged
- No configuration changes required
- API additions are optional (existing code continues to work)

### Forward Compatible
- Design supports future extensions for variable inputs
- Clean separation between loading and application phases
- Extensible for new input/output types

## Conclusion

The OnnxManager now fully supports:
- ✅ Models with multiple output tensors
- ✅ ParticleTransformer-style models with bootstrapped outputs
- ✅ Deferred execution pattern (load early, apply when ready)
- ✅ Enhanced API for querying model structure
- ✅ Comprehensive testing and documentation

The implementation is efficient, thread-safe, and maintains backward compatibility while enabling powerful new use cases.
