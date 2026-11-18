# Prevent silent systematic collisions and avoid unnecessary graph work

Status: concrete implementation plan, 2026-11-15. The analysis-level recoil repair is implemented separately; the framework changes below are proposed, not implemented or deployed. Public C++ contract changes need review before implementation. Current production retains its sealed executable and calibration behavior.

## What the recent analysis work exposed

| Observation | Underlying problem | Appropriate owner |
| --- | --- | --- |
| W-background recoil variations could use propagated nominal functions instead of the calibrated uncertainty functions | `Define` creates suffixed columns; a subsequent `DefineNominal` with the same name silently returns. The mapping can therefore point to a valid column with the wrong computation. | Framework definition/mapping contracts; immediate analysis repair |
| MET-dependent channel input was used by otherwise MET-independent jet cleaning | The dependency inventory conveys both numerical dependencies and selection context. Unchanged jet masks inherit many recoil families, expanding preparation and downstream graphs. | Analysis dependency decomposition; framework provenance and planner |
| DY spends substantial time on MET/recoil before rejection | Necessary lepton and cleaned-jet conditions were evaluated too late on the fit branch. A cheap condition can reject events before expensive dependencies are requested. | Analysis first; bounded framework selection planning later |
| Shared jet cleaning improved execution, while broader tuple grouping did not | The useful optimization depends on actual dependencies and workload. Grouping by itself can compute unused fields or duplicate work. | Demand-aware planner and measured applicability |

Source evidence: [IDataFrameProvider](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/api/IDataFrameProvider.h), [DefineVariations](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/detail/DefineVariations.h), [SystematicManager](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/src/SystematicManager.cc), [Analyzer filtering](https://github.com/brkronheim/RDFAnalyzerCore/blob/main/core/interface/analyzer.h).

The immediate repair uses one analysis helper for the main recoil path and background path. Explicit calibrated shifts have a separate namespace and are mapped before downstream MET definitions. Duplicate calibrated names throw. This deliberately changes affected uncertainty templates; it is not eligible for an output-equivalent production executable swap.

## Target guarantees and limits

ROOT already evaluates demanded definitions lazily and caches a result for an event/slot. The objective is to avoid introducing redundant demanded computations and unnecessary variation nodes, and to reject impossible events before requesting expensive inputs. Removing unused definitions can also save setup, JIT and graph memory even when their event functions never executed.

A framework cannot infer the minimal physics algorithm or prove two arbitrary C++/Python functions equivalent. Nor may it discard work observed by another action, a counter, an output branch, a side effect or an exception contract. Promise structural guarantees for declared, pure operations and conservative behavior elsewhere. Do not deduplicate results merely because a small sample happens to give equal numbers.

## Ordered implementation work

### 1. Make definition ownership and collisions explicit

Implement this before a graph optimizer. First inventory typed C++, Python, expression, vector and manager definitions, including deliberate define-if-missing uses. Record logical output, concrete physical column, producer, inputs, definition mode and systematic label.

Proposed contract for review:

- A new definition of an existing physical name fails with both producer sites and input lists. A definition is not silently accepted because its name already exists.
- Intentional ensure/define-if-missing is a distinct operation. Keep existing public methods compatible during migration, emit actionable diagnostics for ambiguous collisions, migrate real callers, then decide strict-default timing. Do not change the old idempotent C++ behavior globally without this audit.
- An explicit variation definition creates the terminal computation and installs its `(logical output, label)` mapping as one checked operation. Reserve names and validate the full requested family before attaching ROOT nodes.
- A second conflicting mapping fails. An exact repeat of the same mapping may be idempotent. Replacing an automatically propagated mapping with an explicit calibrated one is an intentional, recorded operation, not a silent map assignment. Replacement after consumers have been lowered fails; already-created consumers cannot be repaired retrospectively.

Implementation surfaces: `IDataFrameProvider`, `defineWithVariations`, `SystematicManager`, Analyzer adapters and Python bindings. Preserve paired and explicit-label semantics. Diagnostics should name the output, family/label, old/new producer and resolution, and whether descendants already exist.

Acceptance: reproduce the W collision and fail at construction, retain intentional ensure uses, reject duplicate pairs and replicas consistently in Python/C++, and verify independent explicit producer registration reaches histogram values and selection migrations. No event loop should be needed to diagnose a name collision.

### 2. Add a graph explanation and preflight audit

Extend existing Python `describe`/`explain` and native systematic reporting rather than creating an unrelated reporting stack. Include definitions, true inputs, resolved variation inputs, mappings, selection masks, booked consumers, source locations where available, and incomplete coverage from raw ROOT access.

Report each expensive preparation's consumers, how many distinct resolved input tuples it has, why each label reaches it, unused generated columns, and mapping replacements. Distinguish a registered family from a family actually evaluated by an action. Add structural diagnostics for identity/pass-through dependencies and large variation fan-out; do not assert they are physically redundant without a declared contract.

Acceptance: the W test visibly shows the inherited producer and explicit calibrated producer; the DY graph identifies the jet-cleaning/channel dependency and MET/recoil work downstream of early gates. Reports remain bounded for PDF banks and do not allocate dense representations of large sparse axes.

### 3. Resolve variation producers before generating derived columns

Use one internal resolver shared by typed C++ and Python. Build a finite plan for one definition/family first, then lower it. A later bounded region can extend the planning scope; no immediate requirement to record an entire legacy analysis.

Resolve physical input identities including versions after `Redefine`, aliases, dtype, binding scope and systematic label. If several labels resolve to exactly the same inputs for the same declared-pure operation, define the computation once and map the labels to it. If inputs are unchanged, reuse nominal. Explicit calibrated producers take their intended mapping and are not replaced by automatic propagation.

Do not treat different functions with matching input names as equivalent. Callable/capture identity, precision and effect contracts are part of the identity. Do not introduce Cartesian products of independent nuisance families unless explicitly requested. Preserve existing paired/replica inventory and correlated label semantics.

Acceptance: callable-invocation counts show one evaluation per distinct demanded producer/input tuple per event, across paired and explicit labels. Validate aliases and redefinitions with independent numerical oracles. Check the same behavior with IMT and C++/Python authoring. Compare preflight graph size and setup time as well as event-loop time.

### 4. Separate numerical dependencies from acceptance context

Refactor jet cleaning into geometry/object inputs only. Track which channel or region consumes its result separately, through selection masks and region context. A MET-only shift must not create a new jet-cleaning computation unless an actual cleaning input changes. A JEC/JER or lepton change that affects the mask must continue to propagate.

Start with an analysis-level control after the recoil fix, then design the smallest shared representation needed by multiple callers. Do not globally delete existing channel dependencies or classify nuisances as weight-only by their names. The v7 experiment proved that doing so can expose existing producer bugs and change outputs.

Acceptance: clean-jet values, W loose-electron handling, reco top inputs and migration masks match an independent oracle. MET/recoil-only labels reuse geometry. Genuine jet/lepton variations retain expected differences. Corrected recoil uncertainties, rather than the historical collision, are the reference for this work.

### 5. Place cheap necessary gates before expensive work, within legal branches

Provide a bounded planning path for pure selection recipes with known dependencies. Begin with explicit necessary gates, whose relationship to the complete selection is declared and tested; do not attempt automatic logical inference from arbitrary lambdas. Construct the OR union over all relevant variation masks, retaining the individual masks for consumers.

Only move a gate within the action branch and scope it protects. Independent tag-and-probe, preselection histograms, signed-weight normalization, cutflow and counters keep their original populations and booking positions. Respect filter/action boundaries, missing-input checks, user side effects, failures and retained raw `RNode` handles. If legality is unknown, retain the existing order and explain why.

Acceptance: both nominal-fail/variation-pass and nominal-pass/variation-fail events survive correctly. Counters, flows, Sumw2 and TnP remain exact. Instrumented call counts show that rejected fit-branch events do not execute MET/recoil/TT reconstruction while independent actions still do their required work.

### 6. Prune and share only demanded private intermediates

Extend the existing bounded Python Stage/typed planning work after steps 1–5, using the same producer and effect model. Trace roots from every action, variation/mask mapping and output contract; omit unused private operations before ROOT lowering. Share common expensive preparation within compatible branches. Do not force all fields of a tuple or bundle when a consumer needs only one expensive field.

Keep public columns, wildcard/full snapshots, raw-node observations and plugin-created actions observable. Initially treat unknown plugin/native operations as planning barriers. Preserve an unfused lowering as an independent control. Use the existing fusion plan; this proposal adds correctness and demand prerequisites, not a second runtime or compiler project.

Acceptance: no unused private producer is lowered in a fully declared region; demanded values execute once where legal. Unknown regions remain correct with visible fallback. Fusion is retained only if it improves a measured scope without duplicating demand or erasing supported observability.

### 7. Validate defaults and migrate real production callers

Use a small collision/alias/migration matrix first, then real DY0J/DY2J, W, Gamma, TTbar and data controls. Retain signed weights, complete bin coordinates/values/flows/Sumw2, raw PDFs, region memberships and systematic inventories. Include Run3 configurations, independent TnP/counters, IMT, pure C++ and Python-authored paths, plus conflicting and intentionally repeated definitions.

Use deterministic invocation-count assertions to verify avoided work. For performance, separate graph construction, JIT, verified event loop, export, whole worker and memory; measure paired/counterbalanced equal workloads. Sustained performance acceptance requires the repository's >=300-second worker gates and resource admission, not a short pilot. Default-enable only a validated deterministic policy; retire the superseded supported path after migration rather than accumulating public tuning switches.

Publish source/build identities and explain which defaults ordinary users receive. Fixing a wrong systematic is assessed against an independent physics oracle; it cannot pass an equivalence test that requires keeping the wrong historical template. Other unaffected outputs still require exact agreement. New physics-changing bundles need a new version; frozen campaign artifacts are not overwritten.

## Concrete next tranche

1. Finish the analysis recoil regression and a private executable comparison; preserve current production.
2. Inventory duplicate definitions and variation-map overwrites, and write the concrete strict-definition/explicit-variation API proposal with migration examples. Review this public C++ change before coding it.
3. Implement the producer/mapping preflight and shared collision diagnostics; prove the original bug is rejected in both authoring modes.
4. Add resolved-input deduplication in the shared variation planner, then test the MET-independent cleaning control.
5. Add early-gate/demand planning only in declared bounded regions, followed by supported-default acceptance.

No remote submission, replacement production executable or broad public C++ API change is authorized merely by this plan. The user has requested future-source recoil repair and this plan; a corrected-systematic production run is a separate decision after its impact is measured.
