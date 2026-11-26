from pathlib import Path
import tempfile
import unittest
from analysis_inputs import dataset_files, normalization_record
from category_layout import CategoryLayout
from statistical_inference.covariance import covariance_modes


class AnalysisInputTests(unittest.TestCase):
    def test_one_representation_per_dataset(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            for name in ("DY_0.root","DY_partial.root","DY.root"):
                (root/name).touch()
            self.assertEqual(dataset_files(root,"DY"),[root/"DY.root"])
            (root/"DY.root").unlink()
            self.assertEqual(dataset_files(root,"DY"),[root/"DY_partial.root"])

    def test_normalization_retains_mode_and_all_factors(self):
        record=normalization_record(dict(name="DY",xsec=3,filter_efficiency=.5,kfac=1.2,extra_scale=2),20,2)
        self.assertAlmostEqual(record["scale"],360)
        self.assertEqual(record["denominator_name"],"sum_weights")
        with self.assertRaises(ValueError):
            normalization_record(dict(xsec=1),0,1)

    def test_named_layout_round_trip(self):
        layout=CategoryLayout([dict(name="truth",size=4),dict(name="reco",size=3)],base=1)
        layout=CategoryLayout.from_metadata(layout.metadata())
        for truth in range(4):
            for reco in range(3):
                self.assertEqual(layout.decode(layout.encode([truth,reco])),dict(truth=truth,reco=reco))

    def test_covariance_modes_reject_indefinite_uncertainties(self):
        self.assertEqual(len(covariance_modes([[1,0],[0,0]])),1)
        with self.assertRaises(ValueError):
            covariance_modes([[1,2],[2,1]])


if __name__ == "__main__":
    unittest.main()
