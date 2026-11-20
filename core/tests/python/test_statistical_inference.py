"""Physics-contract checks for portable binned models and backend result readers."""
import json
from pathlib import Path
import sys
import tempfile
import unittest

import numpy as np
import uproot

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "python"))
from statistical_inference import FitError, fit_section
from statistical_inference.model import read_model
from statistical_inference.parameters import encode, decode, restore_parameters
from create_datacards import DatacardGenerator, Histogram1D


def make_card(directory, *, name="ch", mcstat=True, nuisances=True):
    """Two identifiable shared rate parameters, weighted MC, noninteger Asimov."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    x = np.linspace(0., 1., 32)
    signal = 30. + 70. * x
    background = 100. - 70. * x
    edges = np.arange(len(x) + 1, dtype=float)
    generator = object.__new__(DatacardGenerator)
    shapes = {"signal": (signal, signal * 1.7), "bkg": (background, background * 2.3),
              "data_obs": (1.1 * signal + .9 * background, 1.1 * signal + .9 * background)}
    if nuisances:
        shapes.update(signal_shapeUp=(signal * (1.02 + .03 * x), signal * 1.7),
                      signal_shapeDown=(signal * (.97 - .01 * x), signal * 1.7))
    with uproot.recreate(directory / f"{name}.root") as handle:
        for key, (values, variances) in shapes.items():
            handle[key] = generator._histogram_to_uproot_dict(Histogram1D(values, edges, key, variances), key)
    lines = ["imax 1", "jmax 1", "kmax *",
             f"shapes * * {name}.root $PROCESS $PROCESS_$SYSTEMATIC",
             f"bin {name}", "observation -1", f"bin {name} {name}",
             "process signal bkg", "process 0 1", "rate -1 -1",
             "mu rateParam * signal 1 [0.05,5]", "beta rateParam * bkg 1 [0.05,5]"]
    if nuisances:
        lines += ["lumi lnN 1.025 1.025", "shape shape 1 -", "detector group = lumi shape"]
    if mcstat:
        lines += ["* autoMCStats 0 1 1"]
    card = directory / f"{name}.txt"
    card.write_text("\n".join(lines) + "\n")
    return card


def make_polynomial_card(directory):
    """A normalized V+jets shape and ttbar coefficient with saved vjet metadata."""
    import shutil
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    x = np.linspace(0., 1., 32)
    lo = 1 + 40 * np.exp(-((x - .25) / .13) ** 2)
    hi = 2 + 35 * np.exp(-((x - .7) / .17) ** 2)
    bkg = 20 + 15 * x
    edges = np.arange(33)
    generator = object.__new__(DatacardGenerator)
    with uproot.recreate(directory / "shapes.root") as handle:
        for name, values in dict(low=lo, high=hi, ttbar_pt0_mass0=bkg, data_obs=lo + hi + bkg).items():
            handle[f"ch/{name}"] = generator._histogram_to_uproot_dict(Histogram1D(values, edges, name, values * .3), name)
    parameters = [dict(parameter=n, initial=1., min=.05, max=5., is_poi=n == "mu_DYJets", constant=n == "c0")
                  for n in ("mu_DYJets", "c0", "c1")]
    processes = {name: dict(parameters=["c0", "c1"], weights=weights, normalization="mu_DYJets",
                           normalization_group="Zto2L", truth_definition=dict(gen_decay="Zto2L"))
                 for name, weights in (("low", [1., 0.]), ("high", [0., 1.]))}
    poly = dict(schema="vjet-njetpt-polynomial-v2", parameters=parameters, processes=processes,
                normalizations={"Zto2L": dict(reference_total=float(lo.sum() + hi.sum()),
                    reference_terms=[])})
    poly["normalizations"]["Zto2L"]["reference_terms"] = [
        {"process": "low", "yield": float(lo.sum())}, {"process": "high", "yield": float(hi.sum())}]
    tpar = dict(parameter="ttbar_c_0_0", initial=1., min=.05, max=5., is_poi=False)
    ttbar = dict(schema="vjet-ttbar-polynomial-v1", parameters=[tpar], definition="constant ttbar validation correction",
                 config=dict(pt_lower_edges_gev=[0.], mass_lower_edges_gev=[400.],
                     pt_max_gev=600., mass_max_gev=1500., tail_pt_gev=600., tail_mass_gev=1500.,
                     degree_pt=0, degree_mass=0, coefficient_min=.05, coefficient_max=5.),
                 processes={"ttbar_pt0_mass0": dict(parameters=["ttbar_c_0_0"], weights=[1.])})
    model_path = directory / "polynomial_model.json"
    model_path.write_text(json.dumps(poly))
    plugin = Path(__file__).resolve().parents[3] / "analyses/vjet-njetpt/aux/VJetPolynomialModel.py"
    if plugin.is_file():
        shutil.copyfile(plugin, directory / plugin.name)
    poi = dict(fit_model="polynomial", polynomial=poly, ttbar_polynomial=ttbar,
               active=[parameters[0]], floating_parameters=[parameters[0], parameters[2], tpar],
               fit_range=dict(min_strength=.05, max_strength=5.), response_rank=3, n_floating_parameters=3,
               channels=[], reco_pt_binning={}, systematics={},
               physics_model=dict(module=str(directory / "VJetPolynomialModel.py"),
                                  config=str(model_path), plugin="VJetPolynomialModel:polynomialModel"))
    (directory / "poi_map.json").write_text(json.dumps(poi))
    (directory / "fit_inputs.json").write_text(json.dumps(dict(asimov=True, fit_range=poi["fit_range"])))
    card = directory / "datacard_vjet_response.txt"
    card.write_text("\n".join(["imax 1", "jmax *", "kmax *",
        "shapes * * shapes.root $CHANNEL/$PROCESS $CHANNEL/$PROCESS_$SYSTEMATIC",
        "bin ch", "observation -1", "bin ch ch ch", "process low high ttbar_pt0_mass0",
        "process 0 -1 1", "rate -1 -1 -1", "c1 flatParam",
        "ttbar_c_0_0 extArg 1 [0.05,5]", "ttbar_c_0_0 flatParam",
        "scale_ttbar_pt0_mass0 rateParam * ttbar_pt0_mass0 (1*@0) ttbar_c_0_0"]) + "\n")
    return card


class ModelTest(unittest.TestCase):
    def test_generator_writes_observed_data_separately_from_processes(self):
        import yaml
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            edges = np.arange(4, dtype=float)
            with uproot.recreate(root / "inputs.root") as handle:
                handle["obs"] = (np.array([12., 17., 23.]), edges)
                handle["sig"] = (np.array([3., 4., 6.]), edges)
            config = dict(output_dir=str(root / "cards"),
                          input_files={"data": dict(path=str(root / "inputs.root"), type="data"),
                                       "signal": dict(path=str(root / "inputs.root"), type="signal")},
                          processes={"signal": dict(samples=["signal"])},
                          control_regions={"sr": dict(observable="sig", processes=["signal"],
                                                       signal_processes=["signal"], data_process="data")})
            # Histogram aliases use the normal public in-memory preparation path.
            path = root / "config.yaml"
            path.write_text(yaml.safe_dump(config))
            generator = DatacardGenerator(str(path))
            generator.histograms = {"signal": {"sig": Histogram1D([3., 4., 6.], edges)},
                                    "data": {"sig": Histogram1D([12., 17., 23.], edges)}}
            generator.generate_datacard("sr")
            model = read_model([root / "cards/datacard_sr.txt"])
            self.assertEqual(model["channels"][0]["data"]["values"], [12., 17., 23.])
            self.assertEqual([p["name"] for p in model["channels"][0]["processes"]], ["signal"])

    def test_weighted_variance_asymmetry_and_correlations_survive(self):
        with tempfile.TemporaryDirectory() as directory:
            a = make_card(directory, name="A")
            b = make_card(directory, name="B")
            model = read_model([a, b])
            self.assertEqual([p["parameter"] for p in model["parameters"]], ["mu", "beta"])
            self.assertEqual(len(model["channels"]), 2)
            self.assertEqual(model["groups"]["detector"], ["lumi", "shape", "lumi", "shape"])
            h = model["channels"][0]["processes"][0]
            np.testing.assert_allclose(h["variances"], np.array(h["values"]) * 1.7)
            shape = next(e for e in model["effects"] if e["kind"] == "shape")
            self.assertNotEqual(shape["up"]["values"][0] - h["values"][0],
                                h["values"][0] - shape["down"]["values"][0])
            self.assertNotEqual(model["channels"][0]["data"]["values"], h["values"])

    def test_shared_strength_for_multiple_signal_processes(self):
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory)
            text = card.read_text().replace("process 0 1", "process 0 -1")
            text = "\n".join(line for line in text.splitlines() if "rateParam" not in line)
            card.write_text(text)
            model = read_model([card])
            self.assertEqual([p["parameter"] for p in model["parameters"]], ["r"])
            self.assertEqual((model["parameters"][0]["min"], model["parameters"][0]["max"]), (0., 20.))
            self.assertEqual([s["parameter"] for s in model["scales"]], ["r", "r"])

    def test_unknown_directive_and_duplicate_channel_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory)
            with self.assertRaisesRegex(FitError, "duplicate channel"):
                read_model([card, card])
            card.write_text(card.read_text() + "constraint param 0 1\n")
            with self.assertRaisesRegex(FitError, "unsupported card directive"):
                read_model([card])

    def test_model_identity_changes_with_template(self):
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory)
            before = read_model([card])["identity"]
            with uproot.update(card.with_suffix(".root")) as handle:
                handle["signal"] = (np.full(32, 50.), np.arange(33))
            self.assertNotEqual(before, read_model([card])["identity"])

    def test_cache_revalidates_source_contents(self):
        from statistical_inference.model import cached_model
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory)
            cache = Path(directory) / "cache.json"
            first = cached_model([card], cache)
            self.assertEqual(first["identity"], cached_model([card], cache)["identity"])
            card.write_text(card.read_text().replace("lumi lnN 1.025", "lumi lnN 1.050"))
            self.assertNotEqual(first["identity"], cached_model([card], cache)["identity"])

    def test_bounds_and_covariance_restore(self):
        records = [dict(parameter="mu", initial=1., min=.05, max=5., is_poi=True),
                   dict(parameter="beta", initial=2., min=0., max=None, is_poi=False)]
        raw = [encode(p["initial"], p) for p in records]
        np.testing.assert_allclose([decode(x, p)[0] for x, p in zip(raw, records)], [1., 2.])
        names, values, cov = restore_parameters(["rdf_internal_0", "rdf_internal_1", "lumi"],
                                                [*raw, .1], np.ones((3, 3)), {"parameters": records})
        self.assertEqual(names, ["mu", "beta", "lumi"])
        jac = np.array([decode(raw[0], records[0])[1], 2., 1.])
        np.testing.assert_allclose(cov, np.outer(jac, jac))

    def test_backend_selection_does_not_mislabel_rabbit(self):
        result = dict(backend="rabbit", rabbit={"status": "rabbit_complete"}, combine={"status": "old"})
        self.assertEqual(fit_section(result)["status"], "rabbit_complete")

    def test_normalized_polynomial_and_ttbar_are_structured(self):
        with tempfile.TemporaryDirectory() as directory:
            card = make_polynomial_card(directory)
            model = read_model([card], poi_map=card.parent / "poi_map.json")
            self.assertEqual([p["parameter"] for p in model["parameters"] if p["is_poi"]], ["mu_DYJets"])
            self.assertEqual([s["kind"] for s in model["scales"]], ["polynomial", "polynomial", "linear"])
            card.write_text(card.read_text().replace("(1*@0)", "(2*@0)"))
            with self.assertRaisesRegex(FitError, "unsupported formula"):
                read_model([card], poi_map=card.parent / "poi_map.json")


if __name__ == "__main__":
    unittest.main()
