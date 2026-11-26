# Measuring and tuning an RDFAnalyzer job

## Measure event processing

Use the same executable, ROOT build, input files, entry range, variation list, thread count and requested outputs in each comparison. Compare local reads separately from remote reads. Record the event population and output parity before interpreting rates.

From a build-matched environment, measure the full process with its normal entry point.

```sh
/usr/bin/time -v ./analysis analysis.yaml
```

Record input events per elapsed second, user and system CPU seconds, peak resident set size, selected events, output bytes and RDF loop count. Benchmark both run orders and repeat. A short process can spend more time opening inputs or compiling expressions than evaluating events.

For a paired comparison of two builds, run the full worker with the same
staged configuration. Pass the independently verified number of input entries,
including entries rejected by cuts. Use a new output directory and point
`--cwd` at the original working directory if the job uses relative input paths.

```sh
python scripts/benchmark_worker.py /staged/job/worker.yaml \
  --variant baseline=/build/before/analysis --variant candidate=/build/after/analysis \
  --events 281121 --repeats 3 --output /tmp/worker-comparison --check-products
```

The script alternates the binaries in fresh processes and records wall time,
CPU time and peak RSS. `--check-products` compares metadata and skim products
after the timed runs. Use deterministic entry order for exact skim comparisons.

To time a lazy RDF graph independently, materialize an action consuming the computed column. A Count alone may skip an unused Define.

```python
from time import perf_counter
import ROOT

start = perf_counter()
df = ROOT.RDataFrame(1_000_000).Define("value", "double(rdfentry_) * 2")
total = df.Sum("value")
booked = perf_counter()
assert total.GetValue() == 999_999_000_000
finished = perf_counter()
print("booking s", booked - start, "execution s", finished - booked,
      "events/s", 1_000_000 / (finished - booked), "loops", df.GetNRuns())
```

With the Analyzer Python interface, book all counts, sums, histograms and the skim before calling `analysis.run()`. Inspect `analysis.native.event_loops` afterwards. Retain the owner of any buffer passed to `DefineFromVector` through execution. `DefineFromPointer` accepts a compiled function address.

## Choose the next experiment

| Observation | Comparison to run |
|---|---|
| High setup time | Separate library loading, input discovery, expression JIT and graph booking. Repeat with a process-cold input and a warmed file cache. |
| CPU-bound event loop | Profile the worker on real event multiplicities. Share repeatedly demanded columns, count predicate evaluations and inspect costly inputs consumed only by a small fraction of accepted events. Preserve selection semantics for every variation. |
| High memory with many slots | Compare identical outputs with one slot and the intended worker slot count. Record histogram axes, action count, variations, nonempty bins and peak RSS. Include file finalization. |
| Slow remote reads | Compare the same source staged locally, logging ROOT reads and transferred bytes in each case. A different compression codec changes CPU as well as storage traffic. |
| Large skims | Set `saveConfig` to a YAML column list. Record output size, selected entry identities and write CPU for each compression setting. |
| Python analysis appears slower | Compare the same graph and inputs in C++ and Python. Time startup, booking, first materialization and output separately. Compiled column functions run inside ROOT for both interfaces. |

Production worker slots should fit the scheduler CPU and memory allocation. Scale one workload at a time and compare whole-worker throughput. Validate exact counts and integer branches, histogram contents including flow bins, sum of weights squared, and agreed floating-point tolerances.

[Monitoring](PERFORMANCE_MONITORING.md) · [Python binding ownership](PYTHON_BINDINGS_DEEP.md) · [Reusable native functions](FUNCTION_CATALOG.md)
