from __future__ import annotations

from core.python.dataset_manifest import DatasetEntry
from core.python.law.analysis_tasks import _write_job_config
from core.python.submission_backend import read_config


def test_write_job_config_prefers_explicit_redirector(tmp_path):
    template = tmp_path / "submit_config.txt"
    template.write_text(
        "\n".join(
            [
                "fileList=input.root",
                "saveFile=output.root",
                "metaFile=meta.root",
                "saveDirectory=output",
                "xrootdRedirector=root://cmsxrootd.fnal.gov/",
            ]
        )
    )

    dataset = DatasetEntry(
        name="sampleA",
        year=2022,
        dtype="mc",
        process="zh",
        group="signal",
        files=["/store/mc/foo.root"],
    )
    job_dir = tmp_path / "job"
    output_dir = tmp_path / "out"
    _write_job_config(
        job_dir=str(job_dir),
        template_config_path=str(template),
        dataset=dataset,
        output_dir=str(output_dir),
    )

    cfg = read_config(str(job_dir / "submit_config.txt"))
    assert cfg["fileList"].startswith("root://cmsxrootd.fnal.gov/")
