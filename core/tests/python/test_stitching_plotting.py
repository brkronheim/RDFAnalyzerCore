import os
import sys
import tempfile
import unittest
from pathlib import Path

import numpy as np

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", ".."))
_CORE_PYTHON = os.path.join(_REPO_ROOT, "core", "python")
if _CORE_PYTHON not in sys.path:
    sys.path.insert(0, _CORE_PYTHON)

from stitching_plotting import (
    plot_ratio_comparison,
    pull_with_uncertainty,
    ratio_plot_range,
    ratio_with_uncertainty,
)


class TestStitchingPlotting(unittest.TestCase):
    def test_ratio_plot_range_uses_default_when_data_reaches_bounds(self):
        self.assertEqual(
            ratio_plot_range([0.7, 1.3]),
            (0.8, 1.2),
        )

    def test_ratio_plot_range_tightens_around_points_without_error_bars(self):
        np.testing.assert_allclose(
            ratio_plot_range([0.95, 1.05]),
            (0.93, 1.07),
        )

    def test_ratio_plot_range_tightens_around_points_and_error_bars(self):
        np.testing.assert_allclose(
            ratio_plot_range([0.95, 1.05], [0.01, 0.01]),
            (0.92, 1.08),
        )

    def test_ratio_plot_range_keeps_default_when_error_bars_reach_outside(self):
        self.assertEqual(
            ratio_plot_range([0.95, 1.05], [0.2, 0.2]),
            (0.8, 1.2),
        )

    def test_pull_with_uncertainty_combines_both_mc_variances(self):
        pull = pull_with_uncertainty(
            [12.0, 8.0],
            [10.0, 10.0],
            [4.0, 9.0],
            [9.0, 16.0],
        )

        np.testing.assert_allclose(
            pull,
            [2.0 / np.sqrt(13.0), -2.0 / np.sqrt(25.0)],
        )

    def test_ratio_with_uncertainty_handles_empty_denominator_bins(self):
        ratio, uncertainty = ratio_with_uncertainty(
            [2.0, 0.0],
            [1.0, 0.0],
            [0.25, 0.25],
            [0.25, 0.25],
        )

        self.assertEqual(ratio[0], 2.0)
        self.assertTrue(np.isnan(ratio[1]))
        self.assertAlmostEqual(uncertainty[0], np.sqrt(1.25))
        self.assertTrue(np.isnan(uncertainty[1]))

    def test_ratio_with_uncertainty_handles_zero_numerator(self):
        ratio, uncertainty = ratio_with_uncertainty(
            [0.0],
            [2.0],
            [4.0],
            [1.0],
        )

        self.assertEqual(ratio[0], 0.0)
        self.assertEqual(uncertainty[0], 1.0)

    def test_plot_ratio_comparison_writes_image(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "plots" / "comparison.png"
            ratio, uncertainty = plot_ratio_comparison(
                [10.0, 20.0],
                [9.0, 22.0],
                [0.0, 1.0, 2.0],
                output_path,
                title="Stitching validation",
                xlabel="Observable",
                boundaries=[1.0],
                inclusive_variance=[1.0, 4.0],
                stitched_variance=[1.0, 4.0],
            )
            self.assertTrue(output_path.is_file())
            self.assertEqual(ratio.tolist(), [0.9, 1.1])
            self.assertTrue(np.all(np.isfinite(uncertainty)))

    def test_plot_ratio_comparison_supports_signed_values_on_linear_scale(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ratio, uncertainty = plot_ratio_comparison(
                [-2.0, 4.0],
                [-1.0, 5.0],
                [0.0, 1.0, 2.0],
                Path(tmpdir) / "signed.png",
                title="Signed stitching validation",
                xlabel="Observable",
                log_y=False,
                inclusive_variance=[1.0, 4.0],
                stitched_variance=[1.0, 4.0],
            )

        self.assertEqual(ratio.tolist(), [0.5, 1.25])
        self.assertTrue(np.all(np.isfinite(uncertainty)))


if __name__ == "__main__":
    unittest.main()