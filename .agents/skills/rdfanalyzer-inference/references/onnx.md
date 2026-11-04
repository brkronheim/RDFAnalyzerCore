# ONNX inference

Read [OnnxManager.h](../../../../core/plugins/OnnxManager/OnnxManager.h), [implementation](../../../../core/plugins/OnnxManager/OnnxManager.cc), [ONNX_IMPLEMENTATION](../../../../docs/ONNX_IMPLEMENTATION.md), and [ONNX_MULTI_OUTPUT](../../../../docs/ONNX_MULTI_OUTPUT.md).

The parser requires `file`, `name`, `inputVariables`, and `runVar` per `onnxConfig` record. Inspect optional `paddingSize`, input shapes/names, provider options such as `useCuda`/`cudaDeviceId`, and all current packing options in `loadModelsFromConfig`. Different exported models can expect one feature vector or multiple tensor inputs. Infer dimensions from the actual graph and exported metadata; never assume one flattened layout fits both.

Follow `applyModel`/`applyAllModels` after the analysis has defined all inputs. Resolve output suffixes, multiple output nodes, scalar/vector outputs, and disabled-event handling from the implementation. Check actual output columns against the consumer; a node name, an output index, and a classifier class index are distinct concepts.

CUDA inference support is controlled by the ONNX Runtime build/provider, including `ONNXRUNTIME_USE_CUDA`; it is separate from the kinematic fitter's `USE_CUDA`. The current manager throws when `useCuda=true` is requested without CUDA support. Verify the selected runtime/library and actual provider before reporting GPU execution. Inspect per-slot/session/thread ownership before increasing ROOT or intra-op concurrency.

Tests: [OnnxManager](../../../../core/tests/cpp/testOnnxManager.cc) and [GPU](../../../../core/tests/cpp/testOnnxManagerGpu.cc). For batching or padding changes compare full and partial batches, gates/sentinels, output shapes, and numerical predictions; use a representative model rather than only timing tensor allocation. Reweighting export/preprocessing is covered in the [project reference](../../rdfanalyzer-analysis-projects/references/reweighting.md).
