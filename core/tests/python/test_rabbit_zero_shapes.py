"""Native tensor conversion must make its handling of zero shifts explicit."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

from test_statistical_inference import make_card
from statistical_inference import FitError
from statistical_inference.model import read_model
from statistical_inference.tensor import write_tensor


@unittest.skipUnless(importlib.util.find_spec("rabbit"), "Rabbit runtime is optional")
class ZeroShapeTest(unittest.TestCase):
    def test_zero_shift_requires_explicit_policy_and_retains_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            model = read_model([make_card(root)])
            effect = next(e for e in model["effects"] if e["kind"] == "shape")
            effect["down"]["values"][0] = 0.
            with self.assertRaisesRegex(FitError, "zero varied yield"):
                write_tensor(model, root / "strict.hdf5", sparse=False)
            output = write_tensor(model, root / "native.hdf5", sparse=False, zero_shape_policy="native")
            import h5py
            import numpy as np
            with h5py.File(output) as handle:
                self.assertTrue(np.isfinite(handle["hlogk"][...]).all())
            audit = json.loads(output.with_suffix(".metadata.json").read_text())
            self.assertEqual(audit["n_zero_varied_bins"], 1)
            self.assertEqual(audit["by_nuisance"], {"shape": 1})
            self.assertAlmostEqual(audit["relative_floor"], .001)
            self.assertGreater(audit["summed_affected_nominal"], 0.)
            model["channels"][0]["processes"][0]["values"][0] = 0.
            with self.assertRaisesRegex(FitError, "support from zero nominal"):
                write_tensor(model, root / "unsupported.hdf5", zero_shape_policy="native")
