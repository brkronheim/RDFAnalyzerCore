# FastForest BDT and standalone SOFIE

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

For ONNX generation, install the [PyPI `sofie` package](https://pypi.org/project/sofie/)
with `python -m pip install sofie`. Use `sofie.onnx.parse(path)`, then
`model.generate()` and `model.output_generated(header)`. Inspect the emitted
namespace, constructor, input shape and `infer` signature. Framework targets
linked to `coreAll` or SDK `RDFAnalyzer::Core` inherit available LP64 OpenBLAS
by default. Use `RDF_SOFIE_BLAS_LIBRARY` to override it. Stage any separate
weights and verify the provider is available on workers.

[SofieManager](../../../../core/plugins/SofieManager/SofieManager.cc) reads
optional `sofieConfig` records with `name`, `inputVariables` and `runVar`.
Supply each compiled inference callable with C++ `registerModel`. The
Python bindings expose application and lookup. Existing feature and gate
metadata for a name take precedence when registering the callable.
`applyModel` and `applyAllModels` connect the callable to the graph.
The output is a scalar `Float_t` from vector element zero, with `-1.0f`
for a false bool gate. Use independent generated sessions under ROOT
multithreading, and verify feature order and a known prediction.

SOFIE propagates paired and explicit feature shifts through `DefineVector`
and gate shifts through the score definition. Its registered callable consumes
one enabled variation at a time. ONNX's optional `systematicBundle` path
batches eligible variation rows from one event. See the
[SOFIE systematic execution guide](../../../../docs/SOFIE_IMPLEMENTATION.md#systematic-variations).

The current SOFIE optional-config parser catches configuration exceptions. An initialized manager or metadata listing is therefore insufficient evidence that the expected config/callable exists. For a missing output inspect parsed feature/runVar maps, registered inference functions, and the explicit application call.

Tests: [BDTManager](../../../../core/tests/cpp/testBDTManager.cc) and [SofieManager](../../../../core/tests/cpp/testSofieManager.cc). For SOFIE generation/build examples read [SOFIE_IMPLEMENTATION](../../../../docs/SOFIE_IMPLEMENTATION.md). Compare a known prediction, feature order, and gate behavior after changes. Use [build](../../rdfanalyzer-build/SKILL.md) for compiled resource/ROOT dependencies.
