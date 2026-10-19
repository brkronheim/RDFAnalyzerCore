"""Tests for the sparse-first histogram metadata and extraction utility."""

from __future__ import annotations

import importlib.util
import json
import os
import sys

import pytest

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CORE_PY = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PY not in sys.path:
    sys.path.insert(0, _CORE_PY)

from extract_nd_histograms import (  # noqa: E402
    histogram_schema_signature,
    read_embedded_histogram_schema,
    validate_compatible_histogram_schemas,
)


def _schema(sample_label: str = "inclusive") -> dict:
    return {
        "schema_version": 1,
        "directory": "nd_histograms_v1",
        "histograms": [
            {
                "key": "pt__nominal",
                "name": "h_pt",
                "variable": "pt",
                "label": "Jet pT",
                "axes": [
                    {"role": "channel", "bins": 1, "labels": ["signal"], "edges": [0, 1]},
                    {"role": "control", "bins": 1, "labels": ["default"], "edges": [0, 1]},
                    {"role": "sample", "bins": 1, "labels": [sample_label], "edges": [0, 1]},
                    {"role": "systematic", "bins": 2, "labels": ["Nominal", "jesUp"], "edges": [0, 1, 2]},
                    {"role": "observable", "bins": 2, "labels": [], "edges": [0, 10, 30]},
                ],
            }
        ],
    }


def test_read_embedded_schema_from_tobjstring(tmp_path):
    uproot = pytest.importorskip("uproot")
    path = tmp_path / "histograms.root"
    with uproot.recreate(path) as root_file:
        root_file["__rdf_histogram_schema_v1"] = json.dumps(_schema())

    assert read_embedded_histogram_schema(str(path)) == _schema()


def test_sparse_schema_requires_matching_axis_labels():
    reference = _schema()
    incompatible = _schema("wh")

    with pytest.raises(ValueError, match="category labels must match"):
        validate_compatible_histogram_schemas(
            [("job0.root", reference), ("job1.root", incompatible)]
        )


def test_sparse_schema_signature_ignores_histogram_order():
    first = _schema()
    second = _schema()
    second["histograms"].append(
        {
            "key": "eta__nominal",
            "name": "h_eta",
            "variable": "eta",
            "label": "Jet eta",
            "axes": first["histograms"][0]["axes"],
        }
    )
    reversed_schema = dict(second)
    reversed_schema["histograms"] = list(reversed(second["histograms"]))

    assert histogram_schema_signature(second) == histogram_schema_signature(reversed_schema)


@pytest.mark.skipif(importlib.util.find_spec("ROOT") is None, reason="PyROOT unavailable")
def test_extraction_preserves_sparse_data_and_variable_binning(tmp_path):
    import ROOT  # type: ignore
    from array import array

    from extract_nd_histograms import extract_nd_histograms

    input_path = tmp_path / "merged.root"
    output_path = tmp_path / "projected.root"
    nbins = array("i", [1, 1, 1, 2, 2])
    lower = array("d", [0.5, 0.5, 0.5, 0.0, 0.0])
    upper = array("d", [1.5, 1.5, 1.5, 2.0, 30.0])
    coordinates = array("d", [1.0, 1.0, 1.0, 0.5, 5.0])

    root_file = ROOT.TFile.Open(str(input_path), "RECREATE")
    directory = root_file.mkdir("nd_histograms_v1")
    directory.cd()
    sparse = ROOT.THnSparseF("pt__nominal", "h_pt", 5, nbins, lower, upper)
    sparse.Sumw2()
    sparse.SetBinEdges(4, array("d", [0.0, 10.0, 30.0]))
    sparse.Fill(coordinates, 2.0)
    sparse.Fill(coordinates, -1.0)
    labels = ["signal", "default", "inclusive", "Nominal"]
    for index, label in enumerate(labels):
        sparse.GetAxis(index).SetBinLabel(1, label)
    sparse.GetAxis(3).SetBinLabel(2, "jesUp")
    sparse.Write()
    root_file.cd()
    root_file.WriteObject(
        ROOT.TObjString(json.dumps(_schema())), "__rdf_histogram_schema_v1"
    )
    root_file.Close()

    assert extract_nd_histograms(str(input_path), str(output_path)) == 2
    result_file = ROOT.TFile.Open(str(output_path), "READ")
    try:
        assert result_file.Get("nd_histograms_v1/pt__nominal")
        projected = result_file.Get("signal_default/inclusive/h_pt")
        assert projected
        assert projected.GetBinContent(projected.FindBin(5.0)) == pytest.approx(1.0)
        assert projected.GetBinError(projected.FindBin(5.0)) == pytest.approx(5.0**0.5)
        assert projected.GetXaxis().GetBinUpEdge(1) == pytest.approx(10.0)
        assert projected.GetXaxis().GetBinUpEdge(2) == pytest.approx(30.0)
    finally:
        result_file.Close()
