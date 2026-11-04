---
name: rdfanalyzer-inference
description: Configure, apply, or modify ONNX, FastForest BDT, and ROOT SOFIE inference managers, including feature ordering, model outputs, batching, and execution providers.
---

# Model inference

Read only the backend reference needed:

- [ONNX](references/onnx.md): runtime model/session configuration, tensor packing, outputs, batches, and GPU selection.
- [BDT and SOFIE](references/bdt-sofie.md): FastForest text models and compiled callable registration.

Trace the model config from the analysis's `bdtConfig`, `onnxConfig`, or `sofieConfig` into the manager's parser and explicit `apply*` call. Resource loading/registration does not prove model columns are defined or consumed. Check that all input and `runVar` columns exist at application time and preserve the trained feature order, preprocessing, shape, and scalar/vector types.

Match the intended output tensor/node and class index to the column used in the selection or histogram. Verify the disabled-event sentinel and data/MC or train/eval routing from code. For GPU or batching changes, inspect the actual provider/build availability and compare numerical predictions and throughput on the requested workload. A requested GPU setting is not evidence that execution used a GPU.

Manager lifecycle uses [NamedObjectManager](../../../core/plugins/NamedObjectManager/NamedObjectManager.h); see [plugin lifecycle](../rdfanalyzer-plugin-lifecycle/SKILL.md) for wiring and [build](../rdfanalyzer-build/SKILL.md) for ONNX/ROOT dependencies. Reweighting-specific preprocessing/export contracts are in the [analysis project references](../rdfanalyzer-analysis-projects/SKILL.md).
