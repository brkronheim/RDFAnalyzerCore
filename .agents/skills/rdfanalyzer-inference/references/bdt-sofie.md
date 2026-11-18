# FastForest BDT and ROOT SOFIE

[BDTManager](../../../../core/plugins/BDTManager/BDTManager.cc) parses `bdtConfig` records requiring `file`, `name`, `inputVariables`, and `runVar`. It loads FastForest text resources and defines outputs through `applyBDT`/`applyAllBDTs`. Inspect feature order, supported scalar types, gating, and the disabled-event sentinel in the actual implementation. An ONNX file or TMVA XML is not automatically the FastForest text representation expected here.

Loaded named BDTs are reused during lifecycle setup; setup does not reload an
already registered name. FastForest text dumps store a raw base margin. For
XGBoost `binary:logistic`, convert the probability prior to its logit rather
than appending it unchanged.

`bdtCombineModels=true` is an opt-in `applyAllBDTs` path for groups of exactly
two models with identical ordered features and nominal scalar-bool gates. It
evaluates one shared feature pack and projects each gated score, retaining
`input_<model>` helpers and paired/explicit input mappings. Both, either or
neither gate may pass. Larger groups, varied gates and caller-provided input
packs use the individual path; separate `applyBDT` calls stay unchanged.
The default is false. Measure the actual workload before enabling it.

[SofieManager](../../../../core/plugins/SofieManager/SofieManager.cc) reads optional `sofieConfig` records with `name`, `inputVariables`, and `runVar`; compiled inference callables must be supplied with `registerModel`. Config parsing does not load a runtime model file. `applyModel`/`applyAllModels` connects the callable to the graph. Inspect the callable/session lifetime and vector/shape contract in [SofieManager.h](../../../../core/plugins/SofieManager/SofieManager.h), especially under ROOT multithreading.

The current SOFIE optional-config parser catches configuration exceptions. An initialized manager or metadata listing is therefore insufficient evidence that the expected config/callable exists. For a missing output inspect parsed feature/runVar maps, registered inference functions, and the explicit application call.

Tests: [BDTManager](../../../../core/tests/cpp/testBDTManager.cc) and [SofieManager](../../../../core/tests/cpp/testSofieManager.cc). For SOFIE generation/build examples read [SOFIE_IMPLEMENTATION](../../../../docs/SOFIE_IMPLEMENTATION.md). Compare a known prediction, feature order, and gate behavior after changes. Use [build](../../rdfanalyzer-build/SKILL.md) for compiled resource/ROOT dependencies.
