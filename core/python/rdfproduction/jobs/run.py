"""Collect the settings and operations for one named production run."""

from __future__ import annotations

from dataclasses import dataclass, replace
import os

from .branches import BranchOperations
from .monitor import MonitorOperations
from .paths import WORKSPACE
from .pilots import PilotOperations
from .prepare import PreparationOperations
from .smoke import SmokeOperations


@dataclass
class ProductionRun(PreparationOperations, PilotOperations, SmokeOperations,
                    MonitorOperations, BranchOperations):
    """Settings and operations for one prepared production run."""

    submit_config: str
    exe: str
    name: str
    dataset_manifest: str
    file_source: str = ''
    file_source_name: str = ''
    files_per_job: int = 50
    rucio_group_size_gb: float = 10.0
    partition: str = 'auto'
    entries_per_job: int = 100000
    partition_profile: str = ''
    input_metadata_cache: str = ''
    entry_range_mode: str = 'filter'
    sizing_pilots: bool = False
    sizing_target_seconds: int = 3600
    sizing_pilot_target_seconds: int = 1800
    sizing_pilot_parallel: int = 4
    sizing_pilot_prefix_entries: int = 10000
    sizing_pilot_safety_factor: float = 1.5
    sizing_pilot_child: bool = False
    prepare_workers: int = 4
    output_validation: str = 'integrity'
    x509: str = ''
    stage_in: bool = False
    root_setup: str = ''
    output_directory: str = ''
    container_setup: str = ''
    want_os: str = ''
    python_env: str = ''
    make_test_job: bool = True
    stitch_mode: str = ''
    stitch_weights: str = ''
    dask_scheduler: str = ''
    dask_workers: int = 1
    max_runtime: int = 7200
    request_memory: int = 2000
    max_retries: int = 50
    poll_interval: int = 120

    xrdfs_server: str = ""
    xrdfs_pattern: str = "*.root"
    xrdfs_recursive: bool = True
    dask_threads_per_worker: int = 1
    workspace: str = WORKSPACE
    branch: int = 0
    branch_data: dict | None = None

    @property
    def _workspace(self):
        return self.workspace

    @property
    def _run_dir(self):
        return os.path.join(self.workspace, f"skimRun_{self.name}")

    @staticmethod
    def publish_message(message):
        print(message, flush=True)

    def for_branch(self, index, entry):
        return replace(self, branch=index, branch_data=entry)

    def pilot_run(self, batch):
        return replace(self, name=batch["name"], dataset_manifest=batch["manifest"],
                       file_source="", file_source_name="", partition="file_group",
                       partition_profile="none", input_metadata_cache="", make_test_job=False,
                       output_directory=batch["output_directory"], sizing_pilots=False,
                       sizing_pilot_child=True, max_retries=0, poll_interval=15)
