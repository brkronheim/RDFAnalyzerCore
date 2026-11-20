# Correctionlib and energy managers

| Family | Implementation and closest tests |
|---|---|
| Scalar/vector correctionlib | [CorrectionManager.cc](../../../../core/plugins/CorrectionManager/CorrectionManager.cc); [testCorrectionManager](../../../../core/tests/cpp/testCorrectionManager.cc) |
| Shared scale/smear behavior | [ObjectEnergyManagerBase.cc](../../../../core/plugins/ObjectEnergyManagerBase/ObjectEnergyManagerBase.cc) |

Inspect each manager's public header beside its implementation before calling it. CorrectionManager's config key is `correctionConfig` with `correctionlibConfig` as a fallback; read parser-required fields and registered features. Scalar evaluation and vector evaluation have different type/broadcasting contracts. Compound corrections and ordinary correction objects can take different schemas. Trace how a varied string input or input column is constructed and when the result is attached to the graph.

Concrete CMS energy/smearing contracts and their tests live in the
[CMS object-correction skill](../../../../extensions/cms/.agents/skills/rdfanalyzer-cms-object-corrections/SKILL.md).
`ObjectEnergyManagerBase` provides reusable scale/systematic operations; it does
not prescribe a payload schema. Check immediate versus deferred evaluation,
input/output types, variation propagation and at least one numerical case.
For selected object indices, read [collections](collections.md).

Python can explicitly use [correction batches](../../../../docs/CORRECTION_BATCHES.md)
through `Analysis.correction_batch()` or the raw native batch bindings. Default
propagation is preserved; explicit per-output false flags disable automatic input
variation expansion. Scalar precision follows manager configuration and vector
output remains Float_t. Sharing is call-local; readers are fresh after later
Redefine operations. Check the loaded binding's `correction-batch` capability;
source additions are unavailable in an older module until rebuilt. This adds no
mandatory native interface or complete declaration-level plugin graph metadata.

Correction input packing broadcasts scalars only: every per-object array must
have the same length, including empty arrays. The generic `PackingLayout::Rows`
API retains its explicit repeat-last contract; correction evaluation uses
`AlignedRows`. Range/category exceptions retain unity and emit every fallback
with the payload name, ordered inputs and error. Invalid numeric inputs/results
are fatal. `applySelectedCorrectionsVec` dispatches by an aligned case-index
array before evaluating one payload per object; `applyConditionalCorrection`
skips scalar evaluations for inactive events. Both preserve the ordinary
systematic propagation contract, with an explicit nominal-only option.

For multiple selected outputs, use `applySelectedCorrectionsVecBatch` to share
one numeric pack per input variation. `applyConditionalCorrections` shares
scalar packs and consumes bool conditions directly without selector/result
boxing. Both keep sharing local to the public call, preserve nominal-only
output flags, and create fresh readers following `Redefine`.

The conditional batch overload accepts an input-preparation callback to fuse
numeric preparation into gated evaluation. It runs only for active events on
a private pack copy, must preserve input count/order, and must support RDF
slot concurrency. RDF still evaluates the declared raw input dependencies.
