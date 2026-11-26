"""Client-construction controls only: no Dask cluster, scheduler or submission."""
import sys
from pathlib import Path
from types import ModuleType,SimpleNamespace
import unittest
from unittest.mock import MagicMock,patch

REPO=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(REPO/'core/python'))
sys.path.insert(0,str(REPO/'core/python/law'))
try:
    import workflow_executors as executors
except ImportError:
    executors=None


@unittest.skipIf(executors is None,'LAW/Luigi unavailable')
class LocalWorkerCount(unittest.TestCase):
    @staticmethod
    def pending_task(scheduler):
        task = SimpleNamespace(dask_scheduler=scheduler, branch_map={0: "test branch"},
                               publish_message=MagicMock(), req=MagicMock(),
                               get_dask_work=MagicMock(return_value=(str, ["test branch"], {})))
        task.req.return_value.complete.return_value = False
        return task

    def invoke(self,scheduler='',workers=1,missing=False):
        distributed=ModuleType('distributed');distributed.Client=MagicMock();distributed.as_completed=MagicMock()
        task = self.pending_task(scheduler)
        if not missing:task.dask_workers=workers
        with patch.dict(sys.modules,{'distributed':distributed}):
            executors.DaskWorkflowProxy.run(SimpleNamespace(task=task))
        return distributed,task

    def test_local_default_and_explicit_count_reach_client(self):
        for workers in (1,3):
            distributed,task=self.invoke(workers=workers)
            distributed.Client.assert_called_once_with(n_workers=workers)
            distributed.Client.return_value.close.assert_called_once_with()
            distributed.Client.return_value.submit.assert_called_once_with(str, "test branch", pure=False)

    def test_older_task_without_parameter_uses_documented_default(self):
        distributed,task=self.invoke(missing=True)
        distributed.Client.assert_called_once_with(n_workers=1)

    def test_invalid_local_count_fails_before_client_creation(self):
        for workers in (0,-1,True,1.5,'3'):
            distributed=ModuleType('distributed');distributed.Client=MagicMock();distributed.as_completed=MagicMock()
            task = self.pending_task("")
            task.dask_workers = workers
            with patch.dict(sys.modules,{'distributed':distributed}),self.assertRaisesRegex(ValueError,'positive integer'):
                executors.DaskWorkflowProxy.run(SimpleNamespace(task=task))
            distributed.Client.assert_not_called()

    def test_external_scheduler_keeps_worker_ownership_and_explains_inactive_option(self):
        distributed,task=self.invoke(scheduler='tcp://example.invalid:8786',workers=3)
        distributed.Client.assert_called_once_with('tcp://example.invalid:8786')
        self.assertTrue(any('external scheduler controls' in str(call)for call in task.publish_message.call_args_list))
        distributed.Client.return_value.close.assert_called_once_with()


if __name__=='__main__':unittest.main()
