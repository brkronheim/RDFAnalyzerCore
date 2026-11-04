"""Run a real TTree through the native API in either extension configuration."""
import gc
from pathlib import Path
import sys
import tempfile

import numpy as np
import uproot

sys.path.insert(0, sys.argv[1])
import rdfanalyzer

enabled = set(filter(None, sys.argv[2].split(";")))
assert set(rdfanalyzer.enabled_extensions) == enabled
assert all(rdfanalyzer.has_extension(name) for name in enabled)
assert not rdfanalyzer.has_extension("__missing_extension__")
with tempfile.TemporaryDirectory(prefix="rdf-extension-smoke-") as temporary:
    work = Path(temporary)
    with uproot.recreate(work / "input.root") as output:
        output.mktree("Events", {"pt": np.array([10., 30., 50.], dtype=np.float64)})
    (work / "columns.txt").write_text("pt\nscaled\n")
    (work / "config.txt").write_text(
        f"fileList={work}/input.root\nsaveFile={work}/output.root\n"
        f"metaFile={work}/meta.root\nsaveConfig={work}/columns.txt\n"
        "saveTree=Events\nenableSkim=true\nthreads=1\n")
    analysis = rdfanalyzer.Analyzer(str(work / "config.txt"))
    analysis.Define("scaled", "pt * 2.0", ["pt"])
    analysis.Filter("selected", "scaled > 50.0", ["scaled"])
    analysis.save()
    del analysis
    gc.collect()
    with uproot.open(work / "output.root", handler=uproot.source.file.MemmapSource, decompression_executor=uproot.source.futures.TrivialExecutor(), interpretation_executor=uproot.source.futures.TrivialExecutor()) as output:
        values = output["Events"]["scaled"].array(library="np")
        np.testing.assert_array_equal(values, [60., 100.])
    with uproot.open(work / "meta.root", handler=uproot.source.file.MemmapSource) as metadata:
        value = metadata["provenance/extensions.enabled"].member("fTitle")
        assert set(filter(None, value.split(";"))) == enabled
        for name in enabled:
            revision = metadata[f"provenance/extensions.{name}.git_hash"].member("fTitle")
            assert len(revision) == 40 or revision == "unknown"
print(f"Native extension build smoke test passed (extensions={sorted(enabled)})")
