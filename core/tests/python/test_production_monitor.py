from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace

_HERE = os.path.dirname(os.path.abspath(__file__))
if _HERE not in sys.path:
    sys.path.insert(0, _HERE)

import production_monitor


def test_format_duration():
    assert production_monitor.format_duration(30) == "30s"
    assert production_monitor.format_duration(90) == "1.5m"
    assert production_monitor.format_duration(3600) == "1.0h"
    assert production_monitor.format_duration(90000) == "1.0d"


def test_condor_paths_for_job():
    job = SimpleNamespace(condor_job_id="123.45")
    paths = production_monitor._condor_paths_for_job(job, Path("work_dir"))
    assert paths["stdout"].name == "log_123_45.stdout"
    assert paths["stderr"].name == "log_123_45.stderr"
    assert paths["log"].name == "log_123.log"


def test_condor_paths_for_job_invalid_id():
    job = SimpleNamespace(condor_job_id="badformat")
    assert production_monitor._condor_paths_for_job(job, Path("work_dir")) is None


def test_parse_condor_log_for_abort_reason(tmp_path):
    log_path = tmp_path / "condor_logs" / "log_321_12.log"
    log_path.parent.mkdir(parents=True)
    log_path.write_text(
        "Exited with status 1\n"
        "Hold reason = disk full\n"
    )
    reason = production_monitor._parse_condor_log_for_abort_reason(log_path)
    assert reason is not None
    assert reason != ""


def test_parse_stderr_for_failure(tmp_path):
    stderr_path = tmp_path / "condor_logs" / "log_321_12.stderr"
    stderr_path.parent.mkdir(parents=True)
    stderr_path.write_text("Traceback (most recent call last):\nValueError: boom\n")
    excerpt = production_monitor._parse_stderr_for_failure(stderr_path)
    assert "ValueError: boom" in excerpt


def test_inspect_and_record_failure_records_last_error(tmp_path):
    work_dir = tmp_path
    condor_dir = work_dir / "condor_logs"
    condor_dir.mkdir(parents=True)
    (condor_dir / "log_321_12.log").write_text("Exited with status 1\nHold reason = disk full\n")
    (condor_dir / "log_321_12.stderr").write_text("Traceback\nRuntimeError: crash\n")

    job = SimpleNamespace(condor_job_id="321.12", status=production_monitor.JobStatus.FAILED)
    manager = SimpleNamespace(config=SimpleNamespace(work_dir=work_dir), saved=False)

    def save_state():
        manager.saved = True

    manager._save_state = save_state

    reason = production_monitor._inspect_and_record_failure(manager, job)

    assert reason is not None
    assert reason != ""
    assert manager.saved is True
    assert job.last_error == reason


def test_list_productions_finds_state_files(tmp_path, monkeypatch, capsys):
    production_dir = tmp_path / "myProd"
    production_dir.mkdir()
    (production_dir / "production_state.json").write_text("{}")

    class DummyManager:
        def __init__(self, config):
            self.config = config

        def get_progress(self):
            return {
                "total_jobs": 2,
                "status_counts": {"completed": 1, "validated": 0, "failed": 1, "missing_output": 0},
            }

    monkeypatch.setattr(production_monitor, "ProductionManager", DummyManager)

    production_monitor.list_productions(tmp_path)
    captured = capsys.readouterr()
    assert "myProd" in captured.out
    assert "Total jobs: 2" in captured.out
