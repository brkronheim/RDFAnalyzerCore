"""Compare the translated likelihood against an actual Combine workspace.

RDF_TEST_RABBIT_COMBINE=1 enables native tests. Run with the repository ROOT
environment, PyROOT on PYTHONPATH, and the Rabbit interpreter. TensorFlow is
isolated in subprocesses; the reference process evaluates native RooFit code.
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys

import numpy as np
import pytest

from statistical_inference import FitError
from statistical_inference.parameters import restore_parameters
from statistical_inference.rabbit import add_rabbit_options, options_from_args, run_rabbit
from test_statistical_inference import make_card


def test_compatible_options_are_explicit_and_reject_conflicting_policies(tmp_path):
    parser = argparse.ArgumentParser()
    add_rabbit_options(parser)
    assert options_from_args(parser.parse_args([]))["likelihood"] == "native"
    assert options_from_args(parser.parse_args(["--rabbit-likelihood", "combine"]))["likelihood"] == "combine"
    assert options_from_args(parser.parse_args([]))["covariance"] == "auto"
    assert options_from_args(parser.parse_args(["--rabbit-covariance", "analytic"]))["covariance"] == "analytic"
    for options in (dict(mc_stat="full"), dict(zero_shape_policy="native")):
        with pytest.raises(FitError, match="compatibility requires"):
            run_rabbit({}, tmp_path, likelihood="combine", **options)
    with pytest.raises(FitError, match="compatible likelihood"):
        run_rabbit({}, tmp_path, covariance="combine")


def test_compatible_input_contract_rejects_numeric_rates_and_mixed_mc_rules(tmp_path):
    card = make_card(tmp_path/"numeric")
    card.write_text(card.read_text().replace("rate -1 -1", "rate 10 20"))
    with pytest.raises(FitError, match="template-derived rates"):
        run_rabbit(dict(datacard=str(card)), tmp_path/"numeric_fit", likelihood="combine")
    a = make_card(tmp_path/"a", name="a", mcstat=True)
    b = make_card(tmp_path/"b", name="b", mcstat=False)
    with pytest.raises(FitError, match="same autoMCStats policy"):
        run_rabbit(dict(datacards=[str(a), str(b)]), tmp_path/"mixed_fit", likelihood="combine")


def test_bounded_nuisance_covariance_uses_the_physical_jacobian():
    model = dict(parameters=[], likelihood="combine")
    raw = np.array([1., -2.])
    covariance = np.array([[2., .3], [.3, 4.]])
    names, values, cov = restore_parameters(["jes", "jer"], raw, covariance, model)
    jacobian = 1.-np.tanh(raw/7.)**2
    assert names == ["jes", "jer"]
    np.testing.assert_allclose(values, 7.*np.tanh(raw/7.))
    np.testing.assert_allclose(cov, covariance*np.outer(jacobian, jacobian))


def test_native_stencil_diagnostic_reads_nodes_and_rejects_incomplete_traces():
    from statistical_inference.compat_stencils import extract_stencils
    model = dict(identity="toy", parameters=[dict(parameter="mu")], effects=[])
    log = ("[Bisect] 0 cmin = 1, cmax = 0.5, trial = 0.75 dNll = 0.2\n"
           "[Bisect] 1 cmin = 1, cmax = 0.75, trial = 0.875 dNll = 0.1\n"
           "[Bisect] 0 cmin = 1, cmax = 1.5, trial = 1.25 dNll = 0.1\n"
           "mu 1.000 | 0.875 0.100 1 | 1.250 (0.100) 1\n")
    assert extract_stencils(log, model)["stencils"] == {"mu": [-.125, 0., .25]}
    with pytest.raises(FitError, match="lacks parameters"):
        extract_stencils("", model)
    with pytest.raises(FitError, match="both unique"):
        extract_stencils("mu 1.000 | 0.875 0.100 1 | 1.250 (0.100) 1\n", model)


@pytest.mark.skipif(os.environ.get("RDF_TEST_RABBIT_COMBINE") != "1", reason="optional native Combine/Rabbit validation")
@pytest.mark.parametrize("edge,mcstat,multichannel", [(False, True, False), (True, True, False),
                                                    (True, False, False), (False, True, True)])
def test_native_workspace_predictions_profiles_and_derivatives(tmp_path, edge, mcstat, multichannel):
    from statistical_inference.combine import _find_combine, _combine_runtime_environment
    from statistical_inference.model import read_model
    from statistical_inference.compat_reference import CombineReference
    from create_datacards import DatacardGenerator, Histogram1D
    import uproot

    card = make_card(tmp_path, mcstat=mcstat)
    executable = Path(_find_combine(None))
    runtime = _combine_runtime_environment(str(executable))[0]
    combine_python = os.environ.get("RDF_COMBINE_PYTHON", str(Path(__file__).resolve().parents[3]/".venv/bin/python"))
    if multichannel:
        second = make_card(tmp_path, name="short", mcstat=mcstat)
        with uproot.open(tmp_path/"short.root") as handle:
            shapes = {name.split(";")[0]: (h.values()[:16], h.variances()[:16], h.axis().edges()[:17])
                      for name, h in handle.items()}
        generator = object.__new__(DatacardGenerator)
        with uproot.recreate(tmp_path/"short.root") as handle:
            for name, (values, var, bins) in shapes.items():
                handle[name] = generator._histogram_to_uproot_dict(Histogram1D(values, bins, name, var), name)
        merged = subprocess.run([combine_python, str(executable.with_name("combineCards.py")),
                                 f"long={card}", f"short={second}"], cwd=tmp_path, env=runtime,
                                check=True, capture_output=True, text=True)
        card = tmp_path/"combined.txt"
        lines = ["# "+line if line.startswith("Combination of ") else line
                 for line in merged.stdout.splitlines() if "autoMCStats" not in line]
        # combineCards expands the same wildcard MC-stat rule per channel;
        # retain its equivalent supported all-channel spelling in this card.
        lines.append("* autoMCStats 0 1 1")
        card.write_text("\n".join(lines)+"\n")
    if edge:
        with uproot.open(tmp_path/"ch.root") as handle:
            shapes = {name.split(";")[0]: (h.values(), h.variances(), h.axis().edges())
                      for name, h in handle.items()}
        signal, variance, edges = shapes["signal"]
        up, down = signal*1.2, signal*.8
        up[2], down[5], signal[8], up[8] = 0., 0., 0., 15.
        shapes.update(signal=(signal, variance, edges), signal_shapeUp=(up, variance, edges),
                      signal_shapeDown=(down, variance, edges),
                      signal_otherUp=(signal*np.linspace(.8, 1.3, len(signal)), variance, edges),
                      signal_otherDown=(signal*np.linspace(1.25, .7, len(signal)), variance, edges))
        generator = object.__new__(DatacardGenerator)
        with uproot.recreate(tmp_path/"ch.root") as handle:
            for name, (values, var, bins) in shapes.items():
                handle[name] = generator._histogram_to_uproot_dict(Histogram1D(values, bins, name, var), name)
        card.write_text(card.read_text().replace("shape shape 1 -", "shape shape 0.5 -\nother shape 1 -")
                        .replace("lumi lnN 1.025 1.025", "lumi lnN 0.8/1.3 1.025"))
    model = read_model([card])
    model["likelihood"] = "combine"
    (tmp_path/"model.json").write_text(json.dumps(model))
    points = [{}, dict(mu=1.2, beta=.8, shape=.4, lumi=-.2), dict(mu=.8, beta=1.3, shape=-1.4, lumi=.7)]
    if edge:
        points[1]["other"], points[2]["other"] = -.3, 1.2
    (tmp_path/"points.json").write_text(json.dumps(points))
    environment = dict(os.environ, CUDA_VISIBLE_DEVICES="-1", TF_NUM_INTRAOP_THREADS="2", TF_NUM_INTEROP_THREADS="1")
    script = ("import json; from pathlib import Path; from statistical_inference.tensor import write_tensor; "
              "p=Path(__import__('sys').argv[1]); "
              "write_tensor(json.loads((p/'model.json').read_text()),p/'input.hdf5',sparse=False,likelihood='combine')")
    subprocess.run([sys.executable, "-c", script, str(tmp_path)], env=environment, check=True, capture_output=True)
    subprocess.run([sys.executable, "-m", "statistical_inference.compat_probe", str(tmp_path),
                    str(tmp_path/"points.json"), str(tmp_path/"probe.json"), "--hessian"],
                   env=environment, check=True, capture_output=True)
    subprocess.run([combine_python, str(executable.with_name("text2workspace.py")), str(card), "-m", "125", "-o", str(tmp_path/"w.root")],
                   env=runtime, check=True, capture_output=True)
    probe = json.loads((tmp_path/"probe.json").read_text())
    reference = CombineReference(tmp_path/"w.root", model, probe, executable.parent.parent/"lib/libHiggsAnalysisCombinedLimit.so")
    for point in probe["points"]:
        result = reference.evaluate(point["parameters"])
        for key in ("expected", "variance", "profiled", "nll"):
            np.testing.assert_allclose(point[key], result[key], rtol=2e-12, atol=3e-10)
        # Check a displaced point: zero nominal bins can make the nominal
        # second derivative non-regular under clipping in both engines.
        if point["parameters"]["shape"] != .4:
            continue
        h = 1e-4
        gradient = []
        for name in probe["parameter_order"]:
            plus, minus = dict(point["parameters"]), dict(point["parameters"])
            plus[name] += h
            minus[name] -= h
            gradient.append((reference.evaluate(plus)["nll"]-reference.evaluate(minus)["nll"])/(2*h))
        np.testing.assert_allclose(point["gradient"], gradient, rtol=2e-6, atol=2e-6)
        # A native finite-difference Hessian is independent of TF autodiff.
        names = probe["parameter_order"]
        h = 8e-4
        finite = np.empty((len(names), len(names)))
        for i, a in enumerate(names):
            for j, b in enumerate(names):
                terms = []
                for sa, sb in ((1, 1), (1, -1), (-1, 1), (-1, -1)):
                    p = dict(point["parameters"])
                    p[a] += sa*h
                    p[b] += sb*h
                    terms.append(reference.evaluate(p)["nll"])
                finite[i, j] = (terms[0]-terms[1]-terms[2]+terms[3])/(4*h*h)
        np.testing.assert_allclose(point["hessian"], finite, rtol=2e-4, atol=2e-4)
    if not edge and not multichannel:
        # Exercise the finite-stencil estimator and its MC-stat Schur block,
        # independently comparing with native robustHesse on the same toy.
        fit_script = ("import json,sys; from statistical_inference.rabbit import run_rabbit; "
                      "from pathlib import Path; p=Path(sys.argv[1]); "
                      "r=run_rabbit({'datacard':str(p/'ch.txt')},p/'finite',asimov=True,"
                      "device='cpu',likelihood='combine'); "
                      "(p/'finite.json').write_text(json.dumps(r))")
        subprocess.run([sys.executable, "-c", fit_script, str(tmp_path)], env=environment,
                       check=True, capture_output=True)
        command = [str(executable), "-M", "MultiDimFit", str(tmp_path/"w.root"),
                   "--algo", "none", "--robustHesse", "1", "--redefineSignalPOIs", "mu,beta",
                   "--freezeParameters", "r", "--setParameters", "r=1,mu=1,beta=1",
                   "-t", "-1", "--expectSignal", "1", "-n", "compatibility", "-v", "2"]
        subprocess.run(command, cwd=tmp_path, env=runtime, check=True, capture_output=True)
        import ROOT
        handle = ROOT.TFile.Open(str(tmp_path/"robustHessecompatibility.root"))
        histogram = handle.Get("h_covariance")
        labels = [histogram.GetXaxis().GetBinLabel(i) for i in range(1, histogram.GetNbinsX()+1)]
        fit = json.loads((tmp_path/"finite.json").read_text())
        indices = [labels.index(n)+1 for n in fit["full_covariance_order"]]
        native_cov = np.array([[histogram.GetBinContent(i, j) for j in indices] for i in indices])
        rabbit_cov = np.asarray(fit["full_covariance"])
        np.testing.assert_allclose(np.sqrt(np.diag(rabbit_cov)), np.sqrt(np.diag(native_cov)), rtol=1e-3)
        scale = np.sqrt(np.outer(np.diag(native_cov), np.diag(native_cov)))
        assert np.max(np.abs(rabbit_cov-native_cov)/scale) < 2e-3
        assert Path(fit["model"]).with_name("combine_covariance.json").is_file()
        handle.Close()
