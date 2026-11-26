#!/usr/bin/env python3
"""Check exact single-slot histogram, metadata and skim parity outside timing."""

import argparse
from array import array
import hashlib
import json
from pathlib import Path


def axis_signature(axis):
    # A packed categorical coordinate can have 100 million uniform bins
    # with only a few thousand occupied. Inspect the stored representation.
    edges = axis.GetXbins()
    labels = axis.GetLabels()
    return (axis.GetNbins(), axis.GetXmin(), axis.GetXmax(),
            tuple(edges[i] for i in range(edges.GetSize())),
            sorted((label.GetUniqueID(), str(label.GetString())) for label in labels)
            if labels else [])


def histogram_signature(hist):
    if hist.InheritsFrom("THnBase"):
        axes = [axis_signature(hist.GetAxis(i)) for i in range(hist.GetNdimensions())]
        stats = (hist.GetSumw(), hist.GetSumw2())
        coord = array("i", [0] * len(axes))
        cells = {}
        for i in range(hist.GetNbins()):
            content = hist.GetBinContent(i, coord)
            cells[tuple(coord)] = content, hist.GetBinError2(i)
    else:
        axes = [axis_signature(axis) for axis in
                (hist.GetXaxis(), hist.GetYaxis(), hist.GetZaxis())[:hist.GetDimension()]]
        stats = array("d", [0.] * 13)  # TH1::kNstat, including all TH3 moments
        hist.GetStats(stats)
        sumw2 = hist.GetSumw2() if hist.GetSumw2N() else None
        cells = [(hist.GetBinContent(i), hist.GetBinError(i), sumw2[i] if sumw2 else None)
                 for i in range(hist.GetNcells())]
    return axes, hist.GetEntries(), stats, cells


def compare_directory(left, right, prefix=""):
    keys = {k.GetName(): k.GetClassName() for k in left.GetListOfKeys()}
    if keys != {k.GetName(): k.GetClassName() for k in right.GetListOfKeys()}:
        raise ValueError(f"ROOT objects or classes differ under {prefix or '/'}")
    for name, kind in keys.items():
        path = prefix + name
        a, b = left.Get(name), right.Get(name)
        if a.InheritsFrom("TDirectory"):
            compare_directory(a, b, path + "/")
            continue
        if a.InheritsFrom("TH1") or a.InheritsFrom("THnBase"):
            equal = histogram_signature(a) == histogram_signature(b)
        elif kind == "TObjString":
            left_text, right_text = str(a.GetString()), str(b.GetString())
            if name == "__rdf_job_metadata_v1":
                # Run identity includes timing, executable identity and output paths.
                ldoc, rdoc = json.loads(left_text), json.loads(right_text)
                ldoc.pop("provenance", None)
                rdoc.pop("provenance", None)
                equal = ldoc == rdoc
            else:
                equal = left_text == right_text
        else:
            raise ValueError(f"unsupported metadata object {path} ({kind}); use a workload-specific check")
        if not equal:
            raise ValueError(f"physics output differs at {path}")


def skim_signature(path):
    import awkward as ak
    import uproot

    result = {}
    # Worker products are local files. Use a binary stream to read their
    # baskets without dispatching through Uproot's asynchronous filesystem source.
    with path.open("rb") as stream, uproot.open(stream) as root:
        for name in root.keys(cycle=False, recursive=True):
            tree = root[name]
            if isinstance(tree, uproot.ReadOnlyDirectory):
                continue
            if not hasattr(tree, "iterate"):
                raise ValueError(f"unsupported skim object {name} in {path}")
            digest = hashlib.sha256()
            for batch in tree.iterate(step_size=40_000, library="ak"):
                for field in sorted(batch.fields):
                    form, length, buffers = ak.to_buffers(ak.to_packed(batch[field]))
                    for value in (field, form.to_json(), str(length)):
                        digest.update(value.encode() + b"\0")
                    for buffer in buffers.values():
                        digest.update(buffer.tobytes())
            result[name] = tree.typenames(), tree.num_entries, digest.hexdigest()
    return result


def compare(reference, candidate):
    files = [name for name in ("meta.root", "skim.root") if (reference / name).is_file()]
    if not files or files != [name for name in ("meta.root", "skim.root")
                              if (candidate / name).is_file()]:
        raise ValueError("missing or mismatched worker products")
    for name in files:
        left, right = reference / name, candidate / name
        if name == "skim.root":
            if skim_signature(left) != skim_signature(right):
                raise ValueError(f"skim schema, entry order or values differ in {candidate}")
        else:
            import ROOT
            with ROOT.TFile.Open(str(left)) as a, ROOT.TFile.Open(str(right)) as b:
                compare_directory(a, b)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("reference", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()
    compare(args.reference, args.candidate)
