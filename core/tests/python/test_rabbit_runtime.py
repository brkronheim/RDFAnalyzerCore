"""Opt-in native fits, including GPU placement and cross-device covariance.

RDF_TEST_RABBIT=1 enables CPU. RDF_TEST_RABBIT_GPUS=1,4 also runs one and
four GPUs; invoke outside device-restricted sandboxes. Each fit uses its own
subprocess and directory, so TensorFlow device initialization cannot leak.
"""
import json
import os
from pathlib import Path
import tempfile
import unittest

import numpy as np
from test_statistical_inference import make_card, make_polynomial_card
from statistical_inference.rabbit import run_rabbit


@unittest.skipUnless(os.environ.get("RDF_TEST_RABBIT") == "1", "set RDF_TEST_RABBIT=1 for native runtime fits")
class RabbitRuntimeTest(unittest.TestCase):
    def test_bfgs_recovers_rates_and_observed_hessian(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            card = make_card(directory, mcstat=False, nuisances=False)
            result = run_rabbit({"datacard":str(card)}, directory/"bfgs", device="cpu",
                               histogram_errors=False, hvp_batch=1, minimizer_method="BFGS", minimizer_gtol=1e-6)
            np.testing.assert_allclose([result["poi_results"][n]["value"] for n in ("mu","beta")],
                                       [1.1,.9], rtol=1e-6, atol=1e-8)
            self.assertIn("BFGS", result["native_arguments"])
            self.assertEqual(result["native_arguments"][result["native_arguments"].index("--minimizerGtol")+1], "1e-06")
            self.assertLess(result["edm"], 1e-7)
            self.assertTrue(np.all(np.linalg.eigvalsh(result["full_covariance"]) > 0))

    def test_omitting_bin_diagnostics_retains_full_parameter_covariance(self):
        from statistical_inference.results import read_fit_histograms
        from statistical_inference.errors import FitError
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            card = make_card(directory, mcstat=False, nuisances=False)
            fit = run_rabbit({"datacard": str(card)}, directory/"fit", device="cpu",
                             histogram_errors=False, hvp_batch=1)
            x = np.linspace(0., 1., 32)
            design = np.column_stack((30.+70.*x, 100.-70.*x))
            expected = design @ [1.1, .9]
            covariance = np.linalg.inv(design.T @ (design/expected[:, None]))
            np.testing.assert_allclose(fit["full_covariance"], covariance, rtol=1e-6, atol=1e-9)
            self.assertFalse(fit["histogram_errors_computed"])
            self.assertFalse(fit["histogram_chi2_computed"])
            self.assertIn("--noChi2", fit["native_arguments"])
            self.assertNotIn("--computeHistErrors", fit["native_arguments"])
            result = dict(backend="rabbit", rabbit=fit)
            with self.assertRaisesRegex(FitError, "histogram errors were disabled"):
                read_fit_histograms(result)
            _, histograms = read_fit_histograms(result, require_uncertainties=False)
            np.testing.assert_allclose(histograms["ch"]["postfit"], expected, rtol=1e-7)

    def test_normalized_polynomial_and_ttbar_covariance(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            card = make_polynomial_card(directory)
            count = 1 if os.environ.get("RDF_TEST_RABBIT_GPUS") else 0
            fit = run_rabbit({"datacard": str(card), "poi_map": str(directory / "poi_map.json")},
                             directory, asimov=True, device="gpu" if count else "cpu",
                             devices=[0] if count else None, hvp_batch=1)
            self.assertTrue(fit["asimov_minimized"])
            self.assertIsNotNone(fit["minimizer_status"])
            x = np.linspace(0., 1., 32)
            lo = 1 + 40 * np.exp(-((x - .25) / .13) ** 2)
            hi = 2 + 35 * np.exp(-((x - .7) / .17) ** 2)
            bkg = 20 + 15 * x
            total_signal = lo + hi
            derivative = hi - total_signal * hi.sum() / total_signal.sum()
            design = np.column_stack((total_signal, derivative, bkg))
            covariance = np.linalg.inv(design.T @ (design / (total_signal + bkg)[:, None]))
            np.testing.assert_allclose(fit["fit_parameter_covariance"], covariance, rtol=1e-6, atol=1e-9)

    def test_weighted_mcstat_and_asymmetric_nuisances_across_devices(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            card = make_card(directory)
            info = {"datacard": str(card)}
            cpu = run_rabbit(info, directory / "cpu", asimov=True, device="cpu")
            if os.environ.get("RDF_TEST_RABBIT_GPUS"):
                count = max(int(n) for n in os.environ["RDF_TEST_RABBIT_GPUS"].split(","))
                gpu = run_rabbit(info, directory / "gpu", asimov=True, device="gpu",
                                 devices=list(range(count)), n_devices=count)
                self.assertEqual(cpu["full_covariance_order"], gpu["full_covariance_order"])
                np.testing.assert_allclose(cpu["full_covariance"], gpu["full_covariance"], rtol=1e-7, atol=1e-10)
            self.assertEqual(cpu["mc_stat"], dict(enabled=True, mode="lite", distribution="gamma"))

    def test_native_likelihood_and_covariance_on_requested_devices(self):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            card = make_card(directory, mcstat=False, nuisances=False)
            results = [run_rabbit({"datacard": str(card)}, directory / "cpu", device="cpu")]
            x = np.linspace(0., 1., 32)
            design = np.column_stack((30. + 70. * x, 100. - 70. * x))
            expected = design @ [1.1, .9]
            analytic_cov = np.linalg.inv(design.T @ (design / expected[:, None]))
            for count in [int(n) for n in os.environ.get("RDF_TEST_RABBIT_GPUS", "").split(",") if n]:
                results.append(run_rabbit({"datacard": str(card)}, directory / f"gpu{count}",
                                          device="gpu", n_devices=count, devices=list(range(count))))
            for result in results:
                self.assertEqual(result["status"], "rabbit_complete")
                np.testing.assert_allclose([result["poi_results"][n]["value"] for n in ("mu", "beta")],
                                           [1.1, .9], rtol=1e-7, atol=1e-8)
                np.testing.assert_allclose(result["covariance"], analytic_cov, rtol=1e-6, atol=1e-9)
                self.assertLess(result["edm"], 1e-7)
                self.assertEqual(len(result["logical_gpus"]), result["n_devices"] if result["device"] == "gpu" else 0)
                hist = json.loads(Path(result["histograms"]).read_text())["channels"]["ch"]
                np.testing.assert_allclose(hist["postfit"], expected, rtol=1e-7)
