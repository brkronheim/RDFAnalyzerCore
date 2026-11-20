"""The default C++ reader preserves the portable likelihood byte for byte."""
import json
import argparse
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

import numpy as np

sys.path[:0] = [str(Path(__file__).resolve().parents[2]/"python"), str(Path(__file__).resolve().parent)]
from statistical_inference.model import read_model, cached_model
from statistical_inference.errors import FitError
from statistical_inference.rabbit import add_rabbit_options, options_from_args
from test_statistical_inference import make_card


class TemplateReaderTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            import ROOT
        except ImportError:
            raise unittest.SkipTest("PyROOT required for template-reader equivalence checks")

    def test_weighted_shapes_and_cache_identity_match(self):
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory)
            expected = read_model([card], template_reader="uproot")
            self.assertEqual(read_model([card]), expected)
            cache = Path(directory)/"cache.json"
            self.assertEqual(cached_model([card], cache), expected)
            with patch("statistical_inference.model._RootTemplateFile.__init__", side_effect=AssertionError("cache miss")):
                self.assertEqual(cached_model([card], cache, template_reader="uproot"), expected)

    def test_float_histogram_and_unweighted_variable_edges(self):
        import ROOT
        from array import array
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            f = ROOT.TFile.Open(str(root/"shapes.root"), "RECREATE")
            h = ROOT.TH1F("signal", "", 3, array("d", [0., .3, 1.7, 5.]))
            h.Sumw2()
            for i, (value, error) in enumerate(zip([0., 2.3, 7.1], [0., 3.5, 1.3]), 1):
                h.SetBinContent(i, value)
                h.SetBinError(i, error)
            h.Write()
            d = ROOT.TH1D("data_obs", "", 3, array("d", [0., .3, 1.7, 5.]))
            d.SetBinContent(2, 3.)
            d.SetBinContent(3, 8.)
            d.Write()
            f.Close()
            card = root/"card.txt"
            card.write_text("\n".join(["imax 1", "jmax 0", "kmax 0",
                "shapes * * shapes.root $PROCESS", "bin c", "observation -1",
                "bin c", "process signal", "process 0", "rate -1"]) + "\n")
            expected = read_model([card], template_reader="uproot")
            actual = read_model([card])
            self.assertEqual(actual, expected)
            self.assertEqual(actual["channels"][0]["data"]["variances"], [0., 3., 8.])
            self.assertEqual(actual["channels"][0]["data"]["edges"], [0., .3, 1.7, 5.])

    def test_negative_content_is_rejected(self):
        import ROOT
        with tempfile.TemporaryDirectory() as directory:
            card = make_card(directory, nuisances=False)
            f = ROOT.TFile.Open(str(Path(directory)/"ch.root"), "UPDATE")
            h = f.Get("signal")
            h.SetBinContent(1, -1.)
            h.Write("signal", ROOT.TObject.kOverwrite)
            f.Close()
            for reader in ("root", "uproot"):
                with self.subTest(reader=reader), self.assertRaises(FitError):
                    read_model([card], template_reader=reader)

    def test_cli_defaults_to_root_with_explicit_uproot_override(self):
        parser = argparse.ArgumentParser()
        add_rabbit_options(parser)
        self.assertEqual(options_from_args(parser.parse_args([]))["template_reader"], "root")
        self.assertEqual(options_from_args(parser.parse_args(
            ["--rabbit-template-reader", "uproot"]))["template_reader"], "uproot")


if __name__ == "__main__":
    unittest.main()
