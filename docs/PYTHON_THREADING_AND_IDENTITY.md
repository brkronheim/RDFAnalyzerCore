# Python runtime threading and stable row identity

`AnalysisConfig.threads` records what was requested. ROOT may limit its pool to
the CPUs visible in process affinity, and its pool is process-global. The
read-only `analysis.runtime_threading` snapshot separates configuration intent
from observations:

```python
print(analysis.runtime_threading)
# {'requested_threads': 2, 'observed': True,
#  'implicit_mt_enabled': True, 'pool_size': 1, 'dataframe_slots': 1}
```

A request for two threads on a process pinned to one CPU can produce this result.
The configuration remains unchanged. `pool_size` is ROOT's current worker-pool
capacity; `dataframe_slots` is this graph's booked slot capacity. They can differ
if another analysis changes the process-global pool. Neither field counts
threads that actually executed events, establishes exclusive CPU ownership, or
measures scaling. Serial operation normally reports no IMT pool (`pool_size=0`)
and one dataframe slot. Avoid changing ROOT threading while graphs are active.

`analysis.describe().as_dict()['details']['threading']` and a successful
`analysis.result().details['threading']` retain the same observation. Inspection
does not run events; native inspection during execution is rejected. Older
native modules report `observed=False` and `None` for unavailable measurements.
They do not substitute the requested count. The `runtime-threading` capability
identifies native observation support. Configuration-only construction still
needs no ROOT import or file opening.

## Physical source identity under ROOT IMT

ROOT documents that `rdfentry_` is not a stable TChain input-entry index during
multithreaded processing. Snapshot row order can also change.
[ROOT6.40 RDataFrame documentation](https://root.cern.ch/doc/v640/classROOT_1_1RDataFrame.html).
Do not use that ordinal as a physical input-row identity, physics seed, or
cross-framework join key under IMT. Changing only the snapshot sort order does
not repair the identity.

Use persisted event/source keys with their actual uniqueness contract. For
example, run/luminosityBlock/event may need dataset and file identity as well;
MC event numbers are not universally unique. If a stable file/global entry index
is required, persist it before IMT in an input branch or an aligned provenance
friend. Validate positional alignment and every input's coverage explicitly.

The existing friend-tree configuration can attach provenance without rewriting
physics inputs. Python expression wrappers accept C++ identifiers; give qualified
friend fields unambiguous aliases through the existing `aliasConfig`:

```yaml
friends:
  - alias: audit
    treeName: Events
    fileList:
      - /path/to/provenance-for-first-input.root
      - /path/to/provenance-for-second-input.root
```

```text
existingName=audit.stable_source_entry newName=audit_source_entry
existingName=audit.source_event newName=audit_source_event
```

```python
config = AnalysisConfig(
    files=physics_inputs, output='selected.root', threads=2,
    options={'friendConfig': 'friends.yaml', 'aliasConfig': 'aliases.txt'},
)
analysis = Analysis(config)
analysis.define_nominal('source_entry', 'audit_source_entry', ['audit_source_entry'])
```

The sidecar must have the exact input order/entry alignment; names alone are not
proof. A mismatched friend can silently associate the wrong rows, so compare
independent persisted source/event keys before filters and retain manifest,
entry-count and byte evidence. Add run/lumi/file keys when event alone does not
identify the expected row.
