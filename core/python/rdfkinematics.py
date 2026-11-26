"""Variation-aware ROOT graph operations. All angles are in radians."""
from functools import lru_cache
from pathlib import Path
import re


@lru_cache(maxsize=1)
def declare_analysis_math():
    """Expose rdf::AnalysisMath functions to ROOT expressions in this process."""
    import ROOT
    here = Path(__file__).resolve().parent
    for directory in (here, here.parent / "include/rdfanalyzer", here.parent / "interface"):
        if (directory / "AnalysisMath.h").is_file():
            ROOT.gInterpreter.AddIncludePath(str(directory))
            break
    if not ROOT.gInterpreter.Declare('#include "AnalysisMath.h"'):
        raise RuntimeError("Cannot load AnalysisMath.h from the RDFAnalyzer runtime")


def _define(analysis, name, function, columns):
    if any(not isinstance(col, str) or not re.fullmatch(r"[A-Za-z_]\w*", col, re.ASCII)
           for col in columns):
        raise ValueError("Pass simple column names, or define an alias first")
    declare_analysis_math()
    return analysis.define(name, f"rdf::{function}({', '.join(columns)})", columns)


def signed_delta_phi(analysis, name, first, second):
    """Book first minus second azimuth in [-pi, pi), including wrapped inputs."""
    return _define(analysis, name, "signedDeltaPhi", (first, second))


def abs_delta_phi(analysis, name, first, second):
    """Book the unsigned azimuthal distance in [0, pi]."""
    return _define(analysis, name, "absDeltaPhi", (first, second))


def delta_r2(analysis, name, eta1, phi1, eta2, phi2):
    """Book the squared eta-phi distance for each event and input variation."""
    return _define(analysis, name, "deltaR2", (eta1, phi1, eta2, phi2))


def indexed_pt_eta_phi_m(analysis, name, pt, eta, phi, mass, index):
    """Book a PtEtaPhiMVector using a source-index column. All arrays must align."""
    return _define(analysis, name, "indexedPtEtaPhiM", (pt, eta, phi, mass, index))


def indexed_value(analysis, name, values, index, fallback):
    """Look up a source index with an explicit fallback column of the element type."""
    return _define(analysis, name, "indexedValue", (values, index, fallback))


def selected_transverse_sum(analysis, name, mask, pt, phi):
    """Book the selected (px, py) pair. Access its .first and .second in C++ expressions."""
    return _define(analysis, name, "selectedTransverseSum", (mask, pt, phi))
