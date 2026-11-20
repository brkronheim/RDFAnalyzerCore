# Apply native correction variants together in Python

The optional `Analysis.correction_batch()` facade exposes existing native
`CorrectionManager::applyCorrections` and `applyCorrectionsVec` operations. No
public C++ manager/Analyzer interface or output precision changes. Register the
manager and payload explicitly through the existing interface:

```python
analysis.native.AddPlugin('corr', 'CorrectionManager')
analysis.native.registerCorrection('corr', 'sf', 'payload.json', 'sf', ['pt'])
analysis.correction_batch('corr', 'sf', [['nominal'], ['up'], ['down']],
    outputs=['sf', 'sf_up', 'sf_down'])
```

Use the chosen payload's actual ordered schema/keys. This example assumes one
categorical string and one numeric feature. Numeric inputs default to registered
features; `inputs=[...]` overrides them. Each `string_arguments` entry is one
variant's ordered string-input list. Empty inner lists support corrections with
no categorical inputs.

Outputs are explicit, fresh and unique, one per variant. Structure/types are checked
before native setup. Native payload/column/type failures after lowering starts
are nontransactional; start a fresh Analysis. Batches must precede execution.

## Propagation and sharing

Empty/default propagation flags preserve native all-propagating behavior. Supply
one boolean per output to disable automatic input-variation expansion for chosen
terminal outputs:

```python
analysis.correction_batch('corr', 'sf', [['up'], ['down']],
    outputs=['terminal_up', 'terminal_down'],
    propagate_systematics=[False, False])
```

Choose this according to the nuisance contract. It does not undo an explicitly
shifted input column or register weight nuisances/products. Use existing variation
and `weights()` methods to declare them. Nominal corrections that must respond to
shifted inputs should retain propagation. No shape-by-weight Cartesian policy is
inferred.

Sharing lasts only for the explicit call. Calls creating new outputs obtain fresh
readers, preserving native Redefine/stage semantics. The facade rejects existing
output columns instead of silently reusing them; use new output names after
changing inputs. Raw native methods retain their existing idempotent behavior.
Native plugin definitions remain
opaque to the declaration recorder; columns/types/results stay inspectable.

## Vector corrections

```python
analysis.correction_batch('corr', 'jet_sf', [['up'], ['down']],
    inputs=['Jet_pt', 'rho'], outputs=['jet_sf_up', 'jet_sf_down'], vector=True)
```

The payload must accept those inputs. Scalars broadcast, per-object arrays must
be aligned/equal-length, and empty collections remain empty. Vector output stays
native `RVec<Float_t>` even when scalar output is configured as double. The scalar
path honors `correctionScalarOutputType`; the facade inserts no cast.

Expert callers can use raw `applyCorrections` / `applyCorrectionsVec` bindings
with native default output naming. The facade's explicit outputs improve Python
inspection. Both reuse compiled core evaluation and add no Python event callback.
Shared packing can reduce repeated graph work; this interface slice makes no
new throughput claim or default change.
