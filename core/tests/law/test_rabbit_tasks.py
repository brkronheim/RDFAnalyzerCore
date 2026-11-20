"""Backend selection, dependency propagation and stale-result completion checks."""
import json
from pathlib import Path
import sys
import tempfile
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / "core/python"), str(ROOT / "core/python/law"), str(ROOT / "core/tests/python")]
pytest.importorskip("law")
pytest.importorskip("luigi")
from combine_tasks import ManifestFitTask
from dag_tasks import FullAnalysisDAG
from statistical_inference.model import read_model
from test_statistical_inference import make_card


@pytest.mark.parametrize("likelihood,covariance,mc_stat,zero_policy",
                         [("native", "auto", "full", "native"), ("combine", "combine", "card", "error")])
def test_dag_forwards_gpu_and_coverage_configuration(likelihood, covariance, mc_stat, zero_policy):
    task = FullAnalysisDAG(name="rabbit_dag_contract", datacard_config="fit.yaml",
                           manifest_path="manifest.yaml", skip_merge=True, skip_plots=True,
                           fitting_backend="rabbit", rabbit_device="gpu", rabbit_devices="0,1,2,3",
                           rabbit_n_devices=4, rabbit_python="/runtime/python", strict_coverage=True,
                           rabbit_mc_stat=mc_stat, rabbit_zero_shape_policy=zero_policy, rabbit_hvp_batch=1,
                           rabbit_likelihood=likelihood,
                           rabbit_covariance=covariance,
                           fit_dataset="asimov")
    fit = next(t for t in task.requires() if isinstance(t, ManifestFitTask))
    assert (fit.rabbit_device, fit.rabbit_devices, fit.rabbit_n_devices) == ("gpu", "0,1,2,3", 4)
    assert fit.rabbit_python == "/runtime/python"
    assert fit.rabbit_mc_stat == mc_stat
    assert fit.rabbit_zero_shape_policy == zero_policy
    assert fit.rabbit_hvp_batch == 1
    assert fit.rabbit_likelihood == likelihood
    assert fit.rabbit_covariance == covariance
    assert fit.fit_dataset == "asimov"
    assert fit.requires().strict_coverage


def test_completion_rejects_changed_inputs_missing_outputs_and_failed_runs(tmp_path):
    cards = tmp_path / "cards"
    card = make_card(cards)
    outputs = tmp_path / "outputs"
    identity = read_model([card])["identity"]
    native = tmp_path / "fitresults.hdf5"

    def fake_fit(*args, **kwargs):
        native.write_bytes(b"native output placeholder")
        return dict(model_identity=identity, hdf5_output=str(native), status="rabbit_complete")

    with patch.object(ManifestFitTask, "_manifest_fit_dir", property(lambda self: str(outputs))):
        task = ManifestFitTask(name="rabbit_completion_contract", datacard_config="fit.yaml",
                               datacard_dir=str(cards), fitting_backend="rabbit", rabbit_device="cpu")
        outputs.mkdir()
        assert not task.complete()  # An existing directory cannot complete the fit.
        with patch("statistical_inference.rabbit.run_rabbit", side_effect=fake_fit):
            task.run()
        assert task.complete()
        native.unlink()
        assert not task.complete()
        native.write_bytes(b"native output placeholder")
        card.write_text(card.read_text().replace("mu rateParam * signal 1", "mu rateParam * signal 1.1"))
        assert not task.complete()
        with patch("statistical_inference.rabbit.run_rabbit", side_effect=RuntimeError("failed fit")):
            with pytest.raises(RuntimeError, match="failed fit"):
                task.run()
        assert not Path(task.output().path).exists()
