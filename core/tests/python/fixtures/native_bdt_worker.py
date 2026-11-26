"""Apply the framework tutorial BDT to an already-produced skim."""

import os
from pathlib import Path
import sys

import yaml
from rdfanalysis import Analysis, AnalysisConfig


cfg = yaml.safe_load(Path(sys.argv[1]).read_text())
sources = cfg["fileList"]
if isinstance(sources, str):
    sources = sources.split(",")
metadata = Path(cfg["metaFile"]).resolve()
analysis = Analysis(AnalysisConfig(files=sources, output=str(metadata.with_name("scores.root")),
                                   skim=False, persist_histograms=True, options={"bdtConfig": cfg["bdtConfig"]}))
analysis.define_nominal("ml_feature", "static_cast<float>(x)")
analysis.define_nominal("run_model", "true")
analysis.native.AddPlugin("bdt", "BDTManager")
analysis.native.applyAllBDTs()
analysis.histogram("native_score", "score", 2, 0., 1.)
analysis.run()
os.replace(metadata.with_name("scores.metadata.root"), metadata)
