"""Small local counter/application executable used by the two-era DAG test."""

import json
from pathlib import Path
import sys

import numpy as np
import uproot
import yaml


cfg = yaml.safe_load(Path(sys.argv[1]).read_text())
sources = cfg["fileList"]
if isinstance(sources, str):
    sources = sources.split(",")
with uproot.open(sources[0]) as src:
    bins = src["Events"]["bin"].array(library="np")
sample = cfg["sample"]
edges = np.array([0., 1., 2.])
counts, _ = np.histogram(bins, bins=edges)
with uproot.recreate(cfg["metaFile"]) as output:
    output[f"counter_intWeightSignSum_{sample}"] = (counts.astype(float), edges)
    output[f"counter_weightSum_{sample}"] = (np.array([float(len(bins))]), np.array([0., 1.]))
    output[f"counter_weightSignSum_{sample}"] = (np.array([float(len(bins))]), np.array([0., 1.]))
    output["observable"] = (counts.astype(float), edges)
    if cfg["stitchMode"] == "correctionlib":
        from correctionlib import CorrectionSet
        correct = CorrectionSet.from_file(cfg["stitchWeights"])["toy"]
        weights = np.array([correct.evaluate(sample, int(b)) for b in bins])
        output["observable_stitched"] = np.histogram(bins, bins=edges, weights=weights)
    output["__rdf_job_metadata_v1"] = json.dumps({
        "schema_version": 1, "format": "rdf-job-metadata",
        "provenance": {"job.logical_id": sample, "service.counter.sample": sample,
                       "framework.git_hash": "fixture", "config.hash": "fixture"},
        "regions": {}, "normalizations": {}, "weight_audits": {}})
