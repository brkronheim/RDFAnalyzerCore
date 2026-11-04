# FastForest BDT and ROOT SOFIE

[BDTManager](../../../../core/plugins/BDTManager/BDTManager.cc) parses `bdtConfig` records requiring `file`, `name`, `inputVariables`, and `runVar`. It loads FastForest text resources and defines outputs through `applyBDT`/`applyAllBDTs`. Inspect feature order, supported scalar types, gating, and the disabled-event sentinel in the actual implementation. An ONNX file or TMVA XML is not automatically the FastForest text representation expected here.

[SofieManager](../../../../core/plugins/SofieManager/SofieManager.cc) reads optional `sofieConfig` records with `name`, `inputVariables`, and `runVar`; compiled inference callables must be supplied with `registerModel`. Config parsing does not load a runtime model file. `applyModel`/`applyAllModels` connects the callable to the graph. Inspect the callable/session lifetime and vector/shape contract in [SofieManager.h](../../../../core/plugins/SofieManager/SofieManager.h), especially under ROOT multithreading.

The current SOFIE optional-config parser catches configuration exceptions. An initialized manager or metadata listing is therefore insufficient evidence that the expected config/callable exists. For a missing output inspect parsed feature/runVar maps, registered inference functions, and the explicit application call.

Tests: [BDTManager](../../../../core/tests/cpp/testBDTManager.cc) and [SofieManager](../../../../core/tests/cpp/testSofieManager.cc). For SOFIE generation/build examples read [SOFIE_IMPLEMENTATION](../../../../docs/SOFIE_IMPLEMENTATION.md). Compare a known prediction, feature order, and gate behavior after changes. Use [build](../../rdfanalyzer-build/SKILL.md) for compiled resource/ROOT dependencies.
